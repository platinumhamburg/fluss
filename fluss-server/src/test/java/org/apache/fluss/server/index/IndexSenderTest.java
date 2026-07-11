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
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.MapUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexSender} per-bucket in-flight muting and at-least-once retry. */
public class IndexSenderTest {

    private enum LifecyclePoint {
        PUT_KV_INVOCATION,
        PUT_KV_COMPLETION
    }

    private static final class BlockingLifecycleHooks implements IndexSender.LifecycleHooks {
        private final LifecyclePoint point;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();

        private BlockingLifecycleHooks(LifecyclePoint point) {
            this.point = point;
        }

        @Override
        public void beforePutKvInvocation() {
            blockAt(LifecyclePoint.PUT_KV_INVOCATION);
        }

        @Override
        public void beforePutKvCompletion() {
            blockAt(LifecyclePoint.PUT_KV_COMPLETION);
        }

        private void blockAt(LifecyclePoint candidate) {
            if (candidate != point || !blocked.compareAndSet(false, true)) {
                return;
            }
            reached.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void awaitReached() throws InterruptedException {
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void release() {
            release.countDown();
        }
    }

    /** Gateway that records every {@code putKv} call and lets the test control completion. */
    private static final class RecordingGateway extends TestTabletServerGateway {
        private final List<CompletableFuture<PutKvResponse>> pending = new CopyOnWriteArrayList<>();
        private final List<PutKvRequest> requests = new CopyOnWriteArrayList<>();
        private final Set<Integer> failBuckets =
                Collections.newSetFromMap(MapUtils.newConcurrentMap());
        private volatile boolean failNext;
        private volatile boolean autoCompleteSuccess;
        private volatile short putKvMaxVersion = 2;
        private volatile int apiVersionsCalls;
        private volatile CompletableFuture<ApiVersionsResponse> pendingApiVersions;

        RecordingGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            requests.add(request);
            CompletableFuture<PutKvResponse> future = new CompletableFuture<>();
            pending.add(future);
            if (failNext) {
                future.completeExceptionally(new RuntimeException("injected failure"));
            } else if (autoCompleteSuccess) {
                future.complete(responseFor(request));
            }
            return future;
        }

        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            apiVersionsCalls++;
            if (pendingApiVersions != null) {
                return pendingApiVersions;
            }
            return CompletableFuture.completedFuture(apiVersionsResponse());
        }

        private ApiVersionsResponse apiVersionsResponse() {
            ApiVersionsResponse response = new ApiVersionsResponse();
            response.addAllApiVersions(
                    Collections.singletonList(
                            new PbApiVersion()
                                    .setApiKey(ApiKeys.PUT_KV.id)
                                    .setMinVersion(0)
                                    .setMaxVersion(putKvMaxVersion)));
            return response;
        }

        private void completeApiVersions(boolean success) {
            CompletableFuture<ApiVersionsResponse> future = pendingApiVersions;
            if (success) {
                future.complete(apiVersionsResponse());
            } else {
                future.completeExceptionally(new RuntimeException("injected capability failure"));
            }
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeFencesLateCapabilityCallback(boolean success) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.pendingApiVersions = new CompletableFuture<>();
        TableBucket bucket = new TableBucket(40L, 0);
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

        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            await(() -> gateway.apiVersionsCalls == 1);
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosedForTesting);
            assertThat(close.isDone()).isFalse();

            gateway.completeApiVersions(success);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).isEmpty();
            assertThat(sender.inFlightRequestCount()).isZero();
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            gateway.completeApiVersions(success);
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @Test
    void closeLinearizesBeforePutKvInvocationAndWaitsForProbeCleanup() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.pendingApiVersions = new CompletableFuture<>();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_INVOCATION);
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(44L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.apiVersionsCalls == 1);

            Future<?> probeCompletion =
                    lifecycleExecutor.submit(() -> gateway.completeApiVersions(true));
            hooks.awaitReached();
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosedForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            probeCompletion.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).isEmpty();
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            gateway.completeApiVersions(true);
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeWaitsForLatePutKvCallbackAndFencesItsSideEffects(boolean success) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_COMPLETION);
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(45L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.pending.size() == 1);

            Future<?> requestCompletion =
                    lifecycleExecutor.submit(
                            () -> completePutKv(gateway, 0, success));
            hooks.awaitReached();
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosedForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            requestCompletion.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).hasSize(1);
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            if (!gateway.pending.isEmpty()) {
                completePutKv(gateway, 0, success);
            }
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @ParameterizedTest(name = "gatewayReplacement={0}")
    @ValueSource(booleans = {false, true})
    void targetReplacementBeforePutKvInvocationRetriesWithoutSendingToOldTarget(
            boolean gatewayReplacement) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        RecordingGateway[] current = {first};
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_INVOCATION);
        IndexSender sender =
                sender(
                        accumulator,
                        serverId -> serverId == 1 ? current[0] : second,
                        ignored -> leader.get(),
                        hooks);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(47L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            hooks.awaitReached();
            if (gatewayReplacement) {
                current[0] = second;
            } else {
                leader.set(2);
            }
            hooks.release();

            await(() -> second.requests.size() == 1);
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(first.requests).isEmpty();
            assertThat(second.requests).hasSize(1);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            hooks.release();
            sender.close();
        }
    }

    @ParameterizedTest(name = "gatewayReplacement={0}, success={1}")
    @MethodSource("requestReplacementCases")
    void targetReplacementFencesLatePutKvCompletionAndRetriesExactlyOnce(
            boolean gatewayReplacement, boolean success) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        RecordingGateway[] current = {first};
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_COMPLETION);
        IndexSender sender =
                sender(
                        accumulator,
                        serverId -> serverId == 1 ? current[0] : second,
                        ignored -> leader.get(),
                        hooks);
        ExecutorService completionExecutor = Executors.newSingleThreadExecutor();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(46L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.pending.size() == 1);

            Future<?> completion =
                    completionExecutor.submit(() -> completePutKv(first, 0, success));
            hooks.awaitReached();
            if (gatewayReplacement) {
                current[0] = second;
            } else {
                leader.set(2);
            }
            hooks.release();
            completion.get(5, TimeUnit.SECONDS);

            await(() -> second.requests.size() == 1);
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(first.requests).hasSize(1);
            assertThat(second.requests).hasSize(1);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            hooks.release();
            sender.close();
            completionExecutor.shutdownNow();
        }
    }

    private static Stream<Arguments> requestReplacementCases() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leaderChangeFencesLateCapabilityCallbackAndRetriesNewTarget(boolean success)
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        first.pendingApiVersions = new CompletableFuture<>();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(leader.get()),
                        serverId -> serverId == 1 ? first : second,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(41L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.apiVersionsCalls == 1);

            leader.set(2);
            first.completeApiVersions(success);

            await(() -> second.requests.size() == 1);
            assertThat(first.requests).isEmpty();
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void gatewayReplacementFencesLateCapabilityCallbackAndRetriesNewGateway(boolean success)
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        first.pendingApiVersions = new CompletableFuture<>();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        RecordingGateway[] current = {first};
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(1),
                        serverId -> current[0],
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(42L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.apiVersionsCalls == 1);

            current[0] = second;
            first.completeApiVersions(success);

            await(() -> second.requests.size() == 1);
            assertThat(first.requests).isEmpty();
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void gatesExactV1BytesOnConcreteGatewayCapabilityWithoutFallback() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.putKvMaxVersion = 1;
        gateway.autoCompleteSuccess = true;
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        10L,
                        10L,
                        1024L * 1024L,
                        5_000L);
        try {
            TableBucket bucket = new TableBucket(42L, 0);
            IndexReplicator owner = owner(accumulator);
            IndexBatch encodedBatch =
                    v1Batch(bucket, new IndexWindow("idx", 10L, 1, owner));
            byte[] expected = bytes(encodedBatch.encoded());
            KvRecordBatch parsed =
                    KvRecordBatchReader.pointToByteBuffer(
                            encodedBatch.encoded().getByteBuf().nioBuffer());
            assertThat(parsed.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V1);
            assertThat(parsed.fencedWriterKey()).isEqualTo(new WriterKey(9L, 3L));
            assertThat(parsed.fencedSequence()).isEqualTo(10L);
            accumulator.append(encodedBatch);

            await(() -> gateway.apiVersionsCalls == 1);
            assertThat(gateway.requests).isEmpty();

            gateway.putKvMaxVersion = 2;
            await(() -> gateway.apiVersionsCalls >= 2);
            await(() -> gateway.requests.size() == 1);

            PutKvRequest request = gateway.requests.get(0);
            assertThat(request.getAcks()).isEqualTo(-1);
            assertThat(request.getAggMode()).isEqualTo(MergeMode.OVERWRITE.getProtoValue());
            org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf records =
                    request.getBucketsReqsList().get(0).getRecordsSlice();
            byte[] sent = new byte[records.readableBytes()];
            records.getBytes(records.readerIndex(), sent);
            assertThat(sent).containsExactly(expected);
        } finally {
            sender.close();
        }
    }

    @Test
    void gatewayReplacementInvalidatesPositiveCapability() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        first.autoCompleteSuccess = true;
        RecordingGateway second = new RecordingGateway();
        second.putKvMaxVersion = 1;
        second.autoCompleteSuccess = true;
        RecordingGateway[] current = {first};
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> current[0],
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        10L,
                        10L,
                        1024L * 1024L,
                        5_000L);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(43L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.requests.size() == 1);

            current[0] = second;
            accumulator.append(
                    batch(
                            new TableBucket(43L, 1),
                            new IndexWindow("idx", 20L, 1, owner)));
            await(() -> second.apiVersionsCalls == 1);
            assertThat(second.requests).isEmpty();
        } finally {
            sender.close();
        }
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window) {
        byte[] bytes = new byte[] {1, 2, 3};
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, window);
    }

    private static IndexBatch v1Batch(TableBucket targetBucket, IndexWindow window)
            throws Exception {
        FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        1024,
                        new UnmanagedPagedOutputView(128),
                        KvFormat.COMPACTED);
        builder.append(new byte[] {7, 8, 9}, null);
        builder.setWriterState(new WriterKey(9L, 3L), 10L);
        return new IndexBatch(targetBucket, builder.build(), window);
    }

    private static byte[] bytes(BytesView view) {
        byte[] bytes = new byte[view.getBytesLength()];
        view.getByteBuf().getBytes(view.getByteBuf().readerIndex(), bytes);
        return bytes;
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

    private static IndexSender sender(
            IndexAccumulator accumulator,
            java.util.function.Function<Integer, RecordingGateway> gatewayFactory,
            java.util.function.IntUnaryOperator leader,
            IndexSender.LifecycleHooks hooks) {
        return new IndexSender(
                accumulator,
                (tableId, bucket) -> OptionalInt.of(leader.applyAsInt(bucket)),
                gatewayFactory::apply,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                1,
                5L,
                1L,
                1L,
                1024L * 1024L,
                5_000L,
                hooks);
    }

    private static void completePutKv(RecordingGateway gateway, int requestIndex, boolean success) {
        CompletableFuture<PutKvResponse> future = gateway.pending.get(requestIndex);
        if (success) {
            future.complete(gateway.responseFor(gateway.requests.get(requestIndex)));
        } else {
            future.completeExceptionally(new RuntimeException("injected request failure"));
        }
    }

    private static void assertClosedAndDrained(
            IndexSender sender, IndexAccumulator accumulator) {
        assertThat(sender.inFlightRequestCount()).isZero();
        assertThat(sender.outstandingAsyncOperationCount()).isZero();
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.hasUnsent()).isFalse();
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
