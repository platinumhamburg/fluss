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

import org.apache.fluss.metadata.IndexMutation;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexPushSender}. */
class IndexPushSenderTest {

    private ScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void testGroupByTargetAndDispatchProducesOnePutKvPerTarget() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(3);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        // 3 mutations across 2 targets (1:0, 1:0, 2:0).
        sender.send(
                Arrays.asList(
                        IndexMutation.upsert(1L, 0, k("a"), v("va"), 10L),
                        IndexMutation.upsert(1L, 0, k("b"), v("vb"), 11L),
                        IndexMutation.upsert(2L, 0, k("c"), v("vc"), 12L)));

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // One PutKvRequest per target.
        assertThat(gw.received).hasSize(2);
        // Each request carries exactly one bucket-req (target == bucket).
        for (PutKvRequest req : gw.received) {
            assertThat(req.getBucketsReqsCount()).isEqualTo(1);
        }
        sender.close();
    }

    @Test
    void testSuccessfulAckTriggersAckCallback() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(2);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        sender.send(
                Arrays.asList(
                        IndexMutation.upsert(1L, 0, k("a"), v("va"), 100L),
                        IndexMutation.upsert(1L, 0, k("b"), v("vb"), 101L)));

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(acks).hasSize(2);
        // Each (sourceOffset, indexTableId, indexBucket) tuple appears once.
        assertThat(acks).anySatisfy(a -> assertThat(a).containsExactly(100L, 1L, 0L));
        assertThat(acks).anySatisfy(a -> assertThat(a).containsExactly(101L, 1L, 0L));
        sender.close();
    }

    @Test
    void testFailureRetriesWithBackoff() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        // First response is a per-bucket error; second is success.
        gw.responses.add(failureResponse());
        // After that, default success applies.

        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(1);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        long t0 = System.currentTimeMillis();
        sender.send(
                Collections.singletonList(
                        IndexMutation.upsert(1L, 0, k("a"), v("va"), 50L)));

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        long elapsed = System.currentTimeMillis() - t0;
        // Initial 200ms backoff should have elapsed before the retry succeeded.
        assertThat(elapsed).isGreaterThanOrEqualTo(150L);
        assertThat(gw.received.size()).isGreaterThanOrEqualTo(2);
        assertThat(acks).hasSize(1);
        assertThat(acks.get(0)).containsExactly(50L, 1L, 0L);
        sender.close();
    }

    @Test
    void testRespectsMaxBatchSize() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        List<long[]> acks = new CopyOnWriteArrayList<>();
        // 300 mutations, max 256 → 2 batches → 2 PutKv requests.
        int n = 300;
        CountDownLatch ackLatch = new CountDownLatch(n);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        List<IndexMutation> mutations = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            mutations.add(IndexMutation.upsert(7L, 3, k("k" + i), v("v" + i), i));
        }
        sender.send(mutations);

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(gw.received).hasSize(2);

        int totalRecords = 0;
        for (PutKvRequest req : gw.received) {
            assertThat(req.getBucketsReqsCount()).isEqualTo(1);
            byte[] records = req.getBucketsReqAt(0).getRecords();
            totalRecords += recordCount(records);
        }
        assertThat(totalRecords).isEqualTo(n);
        sender.close();
    }

    @Test
    void testDeleteMutationProducesPutKvWithNullValue() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(1);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        sender.send(
                Collections.singletonList(IndexMutation.delete(1L, 0, k("deadKey"), 42L)));

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(gw.received).hasSize(1);
        PbPutKvReqForBucket pb = gw.received.get(0).getBucketsReqAt(0);
        assertThat(pb.getBucketId()).isEqualTo(0);
        List<ParsedRecord> recs = parseRecords(pb.getRecords());
        assertThat(recs).hasSize(1);
        assertThat(recs.get(0).key).isEqualTo("deadKey".getBytes());
        assertThat(recs.get(0).value).isNull();
        sender.close();
    }

    @Test
    void testPerTargetOrderingIsPreservedAcrossRetries() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        // Pre-stage three controlled futures:
        // attempt #1 -> batch_1 first try (we will fail it via per-bucket errorCode).
        // attempt #2 -> batch_1 retry (we will succeed).
        // attempt #3 -> batch_2 dispatch after batch_1's retry succeeds.
        CompletableFuture<PutKvResponse> attempt1 = new CompletableFuture<>();
        CompletableFuture<PutKvResponse> attempt2 = new CompletableFuture<>();
        CompletableFuture<PutKvResponse> attempt3 = new CompletableFuture<>();
        gw.responses.add(attempt1);
        gw.responses.add(attempt2);
        gw.responses.add(attempt3);

        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(2);
        // maxBatchSize == 1 so each mutation becomes its own batch.
        IndexPushSender sender = newSender(gw, acks, ackLatch, 1);

        // Two mutations targeting the SAME (indexTableId=1, indexBucket=0).
        sender.send(
                Arrays.asList(
                        IndexMutation.upsert(1L, 0, k("k1"), v("v1"), 100L),
                        IndexMutation.upsert(1L, 0, k("k2"), v("v2"), 101L)));

        // Wait until first batch dispatched, then assert exactly one is in flight
        // (batch_2 must wait behind batch_1 in the per-target FIFO).
        long deadline = System.currentTimeMillis() + 2000;
        while (gw.received.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(gw.received).hasSize(1);

        // Fail batch_1's first attempt with a per-bucket error response.
        PutKvResponse fail = new PutKvResponse();
        PbPutKvRespForBucket fb = fail.addBucketsResp();
        fb.setBucketId(0);
        fb.setErrorCode(1);
        attempt1.complete(fail);

        // Give a brief moment to confirm batch_2 is NOT dispatched immediately
        // after batch_1 failed (it must wait for batch_1 to succeed in retry).
        Thread.sleep(50);
        assertThat(gw.received).hasSize(1);

        // The 200ms backoff retry should fire and dispatch batch_1 AGAIN.
        deadline = System.currentTimeMillis() + 2000;
        while (gw.received.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(gw.received).hasSize(2);
        // batch_2 still not dispatched because batch_1's retry is still pending.

        // Succeed batch_1's retry.
        attempt2.complete(successResponse());

        // Now batch_2 should be dispatched.
        deadline = System.currentTimeMillis() + 2000;
        while (gw.received.size() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(gw.received).hasSize(3);

        // Succeed batch_2.
        attempt3.complete(successResponse());

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // Acks must arrive in source-offset order: 100 first, then 101.
        assertThat(acks).hasSize(2);
        assertThat(acks.get(0)[0]).isEqualTo(100L);
        assertThat(acks.get(1)[0]).isEqualTo(101L);
        sender.close();
    }

    @Test
    void testCloseDrainsInFlightBeforeReturning() throws Exception {
        CapturingGateway gw = new CapturingGateway();
        // Pre-stage a manually controlled future so we can hold the request in-flight.
        CompletableFuture<PutKvResponse> blocked = new CompletableFuture<>();
        gw.responses.add(blocked);

        List<long[]> acks = new CopyOnWriteArrayList<>();
        CountDownLatch ackLatch = new CountDownLatch(1);
        IndexPushSender sender = newSender(gw, acks, ackLatch, 256);

        sender.send(
                Collections.singletonList(
                        IndexMutation.upsert(1L, 0, k("a"), v("va"), 1L)));

        // The request has been dispatched but not completed.
        // Wait until the gateway has actually been called.
        long deadline = System.currentTimeMillis() + 2000;
        while (gw.received.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(gw.received).hasSize(1);

        // Close on a worker; it must NOT return while the future is unresolved.
        CompletableFuture<Void> closeFuture =
                CompletableFuture.runAsync(sender::close);
        try {
            closeFuture.get(300, TimeUnit.MILLISECONDS);
            // If get() did not throw, close returned too early.
            throw new AssertionError("close() returned before in-flight request completed");
        } catch (TimeoutException expected) {
            // expected — close is blocking waiting for the future
        }

        // Complete the in-flight future; close should now return.
        blocked.complete(successResponse());
        closeFuture.get(5, TimeUnit.SECONDS);
        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private IndexPushSender newSender(
            CapturingGateway gw, List<long[]> acks, CountDownLatch ackLatch, int maxBatchSize) {
        return new IndexPushSender(
                serverId -> gw,
                (tid, b) -> 1,
                executor,
                (off, t) -> {
                    acks.add(new long[] {off, t.getIndexTableId(), t.getIndexBucket()});
                    ackLatch.countDown();
                },
                maxBatchSize);
    }

    private static byte[] k(String s) {
        return s.getBytes();
    }

    private static byte[] v(String s) {
        return s.getBytes();
    }

    private static PutKvResponse successResponse() {
        PutKvResponse r = new PutKvResponse();
        r.addBucketsResp().setBucketId(0);
        return r;
    }

    private static CompletableFuture<PutKvResponse> failureResponse() {
        PutKvResponse r = new PutKvResponse();
        PbPutKvRespForBucket b = r.addBucketsResp();
        b.setBucketId(0);
        b.setErrorCode(1);
        return CompletableFuture.completedFuture(r);
    }

    /** Test-only parser that reconstructs (key, value) tuples without invoking a RowDecoder. */
    private static final class ParsedRecord {
        final byte[] key;
        final byte[] value;

        ParsedRecord(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private static int recordCount(byte[] batch) {
        // RECORDS_COUNT_OFFSET is 24 (LENGTH=4 + MAGIC=1 + CRC=4 + SCHEMA_ID=2 + ATTRIBUTES=1
        // + WRITER_ID=8 + BATCH_SEQUENCE=4 = 24).
        return readIntLE(batch, 24);
    }

    private static List<ParsedRecord> parseRecords(byte[] batch) {
        // Header is 28 bytes; records follow.
        int count = recordCount(batch);
        int pos = 28;
        List<ParsedRecord> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int recordSize = readIntLE(batch, pos);
            int recordEnd = pos + 4 + recordSize;
            pos += 4;
            int[] kv = readUnsignedVarInt(batch, pos);
            int keyLen = kv[0];
            pos += kv[1];
            byte[] key = Arrays.copyOfRange(batch, pos, pos + keyLen);
            pos += keyLen;
            byte[] value = null;
            if (pos < recordEnd) {
                value = Arrays.copyOfRange(batch, pos, recordEnd);
            }
            out.add(new ParsedRecord(key, value));
            pos = recordEnd;
        }
        return out;
    }

    private static int readIntLE(byte[] b, int off) {
        return (b[off] & 0xFF)
                | ((b[off + 1] & 0xFF) << 8)
                | ((b[off + 2] & 0xFF) << 16)
                | ((b[off + 3] & 0xFF) << 24);
    }

    private static int[] readUnsignedVarInt(byte[] b, int off) {
        int value = 0;
        int shift = 0;
        int p = off;
        while (true) {
            int bt = b[p++] & 0xFF;
            value |= (bt & 0x7F) << shift;
            if ((bt & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return new int[] {value, p - off};
    }

    /** Records every PutKvRequest received and replays a queue of pre-staged responses. */
    private static final class CapturingGateway extends TestingTabletGatewayService {
        final List<PutKvRequest> received = new CopyOnWriteArrayList<>();
        final java.util.concurrent.BlockingQueue<CompletableFuture<PutKvResponse>> responses =
                new java.util.concurrent.LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            received.add(request);
            CompletableFuture<PutKvResponse> next = responses.poll();
            if (next != null) {
                return next;
            }
            // Default to success.
            return CompletableFuture.completedFuture(successResponse());
        }
    }

}
