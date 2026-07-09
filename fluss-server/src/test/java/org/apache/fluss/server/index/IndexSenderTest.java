/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.MapUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexSender} per-bucket in-flight muting and at-least-once retry. */
public class IndexSenderTest {

    /** Gateway that records every {@code putKv} call and lets the test control completion. */
    private static final class RecordingGateway extends TestTabletServerGateway {
        private final List<CompletableFuture<PutKvResponse>> pending = new CopyOnWriteArrayList<>();
        private final Set<Integer> failBuckets =
                Collections.newSetFromMap(MapUtils.newConcurrentMap());
        private volatile boolean failNext;
        private volatile boolean autoCompleteSuccess;

        RecordingGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            CompletableFuture<PutKvResponse> future = new CompletableFuture<>();
            pending.add(future);
            if (failNext) {
                future.completeExceptionally(new RuntimeException("injected failure"));
            } else if (autoCompleteSuccess) {
                future.complete(responseFor(request));
            }
            return future;
        }

        /**
         * Builds a per-bucket response mirroring the request: buckets listed in {@link
         * #failBuckets} carry an error code, the rest are acked. This matches the real server
         * contract where PutKv reports success/failure per bucket rather than per request.
         */
        private PutKvResponse responseFor(PutKvRequest request) {
            PutKvResponse response = new PutKvResponse();
            for (PbPutKvReqForBucket bucketReq : request.getBucketsReqsList()) {
                PbPutKvRespForBucket bucketResp =
                        response.addBucketsResp().setBucketId(bucketReq.getBucketId());
                if (failBuckets.contains(bucketReq.getBucketId())) {
                    bucketResp.setError(
                            Errors.UNKNOWN_SERVER_ERROR.code(), "injected per-bucket failure");
                }
            }
            return response;
        }
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window) {
        byte[] bytes = new byte[] {1, 2, 3};
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, window);
    }

    /** A per-bucket success response acking exactly the given bucket ids (no error set). */
    private static PutKvResponse ackResponse(int... bucketIds) {
        PutKvResponse response = new PutKvResponse();
        for (int bucketId : bucketIds) {
            response.addBucketsResp().setBucketId(bucketId);
        }
        return response;
    }

    private static IndexReplicator owner(IndexAccumulator accumulator) {
        return new IndexReplicator(
                null, Collections.emptyList(), accumulator, null, 0L, 1024, (sync, all) -> {});
    }

    private static void await(BooleanSupplier condition) {
        waitUntil(
                condition::getAsBoolean,
                Duration.ofSeconds(5),
                "Condition was not met within timeout");
    }

    @Test
    void senderDrainsBatchesThatExistedBeforeStartup() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket bucket = new TableBucket(90L, 0);
        IndexReplicator owner = owner(accumulator);
        accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            await(() -> gateway.pending.size() == 1);
            gateway.pending.get(0).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void inFlightMutingSerializesSendsForSameBucket() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(100L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));
            accumulator.append(batch(bucket, new IndexWindow("idx", 20L, 1, owner)));

            // Only the first batch is dispatched; the bucket is muted until its ack.
            await(() -> gateway.pending.size() == 1);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);
            assertThat(accumulator.hasPending(bucket))
                    .as("second batch must stay queued while the first is in flight")
                    .isTrue();
            assertThat(gateway.pending.size())
                    .as("second batch must not be sent while the first is in flight")
                    .isEqualTo(1);

            // Ack the first send -> bucket unmuted -> second batch dispatched.
            gateway.pending.get(0).complete(ackResponse(0));
            await(() -> gateway.pending.size() == 2);
            assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(10L);

            // Ack the second send -> the second window advances the pushed offset.
            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 20L);
        } finally {
            sender.close();
        }
    }

    @Test
    void failedSendIsRetriedWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.failNext = true;
        gateway.autoCompleteSuccess = true;
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(200L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            // Failure re-enqueues for retry: more than one send attempt is observed.
            await(() -> gateway.pending.size() >= 2);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("offset must not advance while sends keep failing")
                    .isEqualTo(0L);

            // Recover: the next attempt auto-succeeds and the offset finally advances.
            gateway.failNext = false;
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void failedHeadBatchRetriesBeforeLaterBatchInSameBucket() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            TableBucket bucket = new TableBucket(250L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));
            accumulator.append(batch(bucket, new IndexWindow("idx", 20L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            gateway.pending.get(0).completeExceptionally(new RuntimeException("first failed"));

            await(() -> gateway.pending.size() == 2);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);
            assertThat(accumulator.hasPending(bucket))
                    .as("later batch must stay queued while the failed head is being retried")
                    .isTrue();
            assertThat(gateway.pending.size())
                    .as("later batch must not be sent while the failed head is being retried")
                    .isEqualTo(2);

            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            await(() -> gateway.pending.size() == 3);

            gateway.pending.get(2).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 20L);
        } finally {
            sender.close();
        }
    }

    @Test
    void partialBucketFailureRetriesOnlyFailedBucketAndHoldsOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.autoCompleteSuccess = true;
        gateway.failBuckets.add(1); // bucket 1's index push keeps failing
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            IndexReplicator owner = owner(accumulator);
            // One window spanning two index buckets of the same table; it completes only once both
            // buckets are acked.
            IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
            accumulator.append(batch(new TableBucket(300L, 0), window));
            accumulator.append(batch(new TableBucket(300L, 1), window));

            // Bucket 1 keeps failing, so the window never completes and the offset never advances
            // even though bucket 0 was acked. A buggy "RPC-ok == whole-batch-acked" sender would
            // wrongly advance the offset here.
            await(() -> gateway.pending.size() >= 3);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("a per-bucket failure must not advance the pushed offset")
                    .isEqualTo(0L);

            // Recover bucket 1: the window now completes and the offset advances to the window end.
            gateway.failBuckets.clear();
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void stuckPutKvTimesOutAndRetriesWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        50L);
        try {
            TableBucket bucket = new TableBucket(400L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);

            // The first future is never completed by the gateway. The sender-side timeout must
            // unmute and retry the bucket instead of leaving the window stuck forever.
            await(() -> gateway.pending.size() >= 2);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("timeout must not ack or advance the window")
                    .isEqualTo(0L);

            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            await(() -> sender.inFlightRequestCount() == 0);
        } finally {
            sender.close();
        }
    }

    @Test
    void ackAfterOwnerClosedReleasesBatchWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(500L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(accumulator.pendingBytes()).isEqualTo(3L);

            owner.close();
            gateway.pending.get(0).complete(ackResponse(0));

            await(() -> sender.inFlightRequestCount() == 0);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("a closed owner must not be advanced by a late ack")
                    .isEqualTo(0L);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void failureAfterOwnerClosedReleasesBatchWithoutRetryLoop() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(600L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(accumulator.pendingBytes()).isEqualTo(3L);

            owner.close();
            gateway.pending.get(0).completeExceptionally(new RuntimeException("late failure"));

            await(() -> sender.inFlightRequestCount() == 0);
            assertThat(gateway.pending)
                    .as("closed-owner batch must not be re-enqueued for retry")
                    .hasSize(1);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }
}
