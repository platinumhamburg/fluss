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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.index.IndexPushTracker.Target;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link IndexPushScheduler}. */
class IndexPushSchedulerTest {

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
    void testSubmitWithNoMutationsRegistersEmptyTargetSet() throws Exception {
        // Empty plan list → extractor returns no mutations.
        IndexMutationExtractor.Context ctx = emptyContext();
        IndexPushTracker tracker = new IndexPushTracker();
        BlockingGateway gw = new BlockingGateway();
        List<Long> callbackValues = new CopyOnWriteArrayList<>();
        AtomicReference<BiConsumer<Long, Target>> capturedAck = new AtomicReference<>();

        IndexPushScheduler scheduler =
                new IndexPushScheduler(
                        ctx,
                        tracker,
                        ackCb -> {
                            capturedAck.set(ackCb);
                            return newSender(gw, ackCb);
                        },
                        callbackValues::add);

        scheduler.submit(10L, null, nonNullRow());

        // No mutations → no PutKv dispatched at all.
        Thread.sleep(50);
        assertThat(gw.received).isEmpty();
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(callbackValues).isEmpty();

        // Trigger an ack via the bridged callback (any target works because the registered set
        // for offset 10 is empty — advanceLocked will publish 10 unconditionally).
        capturedAck.get().accept(10L, new Target(99L, 0));

        assertThat(tracker.getIndexPushedOffset()).isEqualTo(10L);
        assertThat(callbackValues).containsExactly(10L);

        scheduler.close();
    }

    @Test
    void testSubmitWithMutationsRegistersTargetsAndDispatches() throws Exception {
        // Two plans → two mutations per row, each to a distinct (indexTableId, bucket) target.
        IndexMutationExtractor.Context ctx = twoIndexContext();
        IndexPushTracker tracker = new IndexPushTracker();
        BlockingGateway gw = new BlockingGateway();
        List<Long> callbackValues = new CopyOnWriteArrayList<>();

        IndexPushScheduler scheduler =
                new IndexPushScheduler(
                        ctx,
                        tracker,
                        ackCb -> newSender(gw, ackCb),
                        callbackValues::add);

        scheduler.submit(7L, null, nonNullRow());

        // Two distinct targets → two PutKv requests, each carrying one bucket entry.
        long deadline = System.currentTimeMillis() + 5_000L;
        while (gw.received.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(gw.received).hasSize(2);
        Set<Long> tableIds = new HashSet<>();
        for (PutKvRequest req : gw.received) {
            tableIds.add(req.getTableId());
            assertThat(req.getBucketsReqsCount()).isEqualTo(1);
        }
        assertThat(tableIds).containsExactlyInAnyOrder(1L, 2L);

        // Watermark stays unadvanced because the gateway futures are still blocked.
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(callbackValues).isEmpty();

        // Complete the blocked futures so close() can drain.
        gw.completeAllPending(successResponse());
        scheduler.close();
    }

    @Test
    void testAckCallbackAdvancesTrackerAndFiresCallbackOnce() throws Exception {
        IndexMutationExtractor.Context ctx = twoIndexContext();
        IndexPushTracker tracker = new IndexPushTracker();
        BlockingGateway gw = new BlockingGateway();
        List<Long> callbackValues = new CopyOnWriteArrayList<>();
        AtomicReference<BiConsumer<Long, Target>> capturedAck = new AtomicReference<>();

        IndexPushScheduler scheduler =
                new IndexPushScheduler(
                        ctx,
                        tracker,
                        ackCb -> {
                            capturedAck.set(ackCb);
                            return newSender(gw, ackCb);
                        },
                        callbackValues::add);

        scheduler.submit(5L, null, nonNullRow());

        Target t1 = new Target(1L, 0);
        Target t2 = new Target(2L, 0);

        // First ack does not advance — t2 is still pending.
        capturedAck.get().accept(5L, t1);
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(callbackValues).isEmpty();

        // Second ack closes out the set → offset advances to 5; callback fires exactly once.
        capturedAck.get().accept(5L, t2);
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(5L);
        assertThat(callbackValues).containsExactly(5L);

        gw.completeAllPending(successResponse());
        scheduler.close();
    }

    @Test
    void testAckCallbackDoesNotFireIfOffsetUnchanged() throws Exception {
        IndexMutationExtractor.Context ctx = oneIndexContext();
        IndexPushTracker tracker = new IndexPushTracker();
        BlockingGateway gw = new BlockingGateway();
        List<Long> callbackValues = new CopyOnWriteArrayList<>();
        AtomicReference<BiConsumer<Long, Target>> capturedAck = new AtomicReference<>();

        IndexPushScheduler scheduler =
                new IndexPushScheduler(
                        ctx,
                        tracker,
                        ackCb -> {
                            capturedAck.set(ackCb);
                            return newSender(gw, ackCb);
                        },
                        callbackValues::add);

        scheduler.submit(5L, null, nonNullRow());
        scheduler.submit(6L, null, nonNullRow());

        Target t1 = new Target(1L, 0);

        // Ack the LATER offset first — head 5 is still pending so no advancement happens.
        capturedAck.get().accept(6L, t1);
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(callbackValues).isEmpty();

        // Now ack the earlier offset — both cascade and the callback fires once with the final
        // value 6, never with the intermediate 5.
        capturedAck.get().accept(5L, t1);
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(6L);
        assertThat(callbackValues).containsExactly(6L);

        gw.completeAllPending(successResponse());
        scheduler.close();
    }

    @Test
    void testCloseClosesSender() {
        IndexMutationExtractor.Context ctx = oneIndexContext();
        IndexPushTracker tracker = new IndexPushTracker();
        BlockingGateway gw = new BlockingGateway();
        List<Long> callbackValues = new CopyOnWriteArrayList<>();

        IndexPushScheduler scheduler =
                new IndexPushScheduler(
                        ctx,
                        tracker,
                        ackCb -> newSender(gw, ackCb),
                        callbackValues::add);

        scheduler.close();

        // close() cascaded to the sender → any further submit must surface the sender's
        // "closed" guard rather than silently dispatching.
        assertThatThrownBy(() -> scheduler.submit(1L, null, nonNullRow()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private IndexPushSender newSender(BlockingGateway gw, BiConsumer<Long, Target> ackCb) {
        return new IndexPushSender(serverId -> gw, (tid, b) -> 1, executor, ackCb, 256);
    }

    private static IndexMutationExtractor.Context emptyContext() {
        return new IndexMutationExtractor.Context(
                Collections.emptyList(), new int[] {0}, false, null);
    }

    private static IndexMutationExtractor.Context oneIndexContext() {
        IndexMutationExtractor.IndexPlan p =
                new IndexMutationExtractor.IndexPlan(
                        /* indexTableId= */ 1L,
                        new int[] {0},
                        fixedKey((byte) 1),
                        fixedValue((byte) 2),
                        fixedBucket(0));
        return new IndexMutationExtractor.Context(
                Collections.singletonList(p), new int[] {0}, false, null);
    }

    private static IndexMutationExtractor.Context twoIndexContext() {
        IndexMutationExtractor.IndexPlan p1 =
                new IndexMutationExtractor.IndexPlan(
                        /* indexTableId= */ 1L,
                        new int[] {0},
                        fixedKey((byte) 1),
                        fixedValue((byte) 0xA),
                        fixedBucket(0));
        IndexMutationExtractor.IndexPlan p2 =
                new IndexMutationExtractor.IndexPlan(
                        /* indexTableId= */ 2L,
                        new int[] {0},
                        fixedKey((byte) 2),
                        fixedValue((byte) 0xB),
                        fixedBucket(0));
        return new IndexMutationExtractor.Context(
                Arrays.asList(p1, p2), new int[] {0}, false, null);
    }

    private static IndexMutationExtractor.KeyEncoder fixedKey(final byte b) {
        return (row, idx, pk) -> new byte[] {b};
    }

    private static IndexMutationExtractor.ValueEncoder fixedValue(final byte b) {
        return (row, idx, pk, pid) -> new byte[] {b};
    }

    private static IndexMutationExtractor.BucketAssigner fixedBucket(final int b) {
        return (key, row) -> b;
    }

    private static PutKvResponse successResponse() {
        PutKvResponse r = new PutKvResponse();
        r.addBucketsResp().setBucketId(0);
        return r;
    }

    /**
     * Row whose only meaningful method is {@link InternalRow#isNullAt(int)} — always returns false
     * so {@link IndexMutationExtractor} treats every idx column as non-null.
     */
    private static InternalRow nonNullRow() {
        return new InternalRow() {
            @Override
            public int getFieldCount() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean isNullAt(int pos) {
                return false;
            }

            @Override
            public boolean getBoolean(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte getByte(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public short getShort(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public float getFloat(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getDouble(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BinaryString getChar(int pos, int length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BinaryString getString(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Decimal getDecimal(int pos, int precision, int scale) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TimestampNtz getTimestampNtz(int pos, int precision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TimestampLtz getTimestampLtz(int pos, int precision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getBinary(int pos, int length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getBytes(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalArray getArray(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalMap getMap(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalRow getRow(int pos, int numFields) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Gateway that records every {@link PutKvRequest} and returns a CompletableFuture that NEVER
     * completes by default. This lets a test observe dispatch without the real sender's
     * success-path firing back into the scheduler's ack bridge. Tests that need acks invoke the
     * captured ack callback directly. When tearing the test down, {@link #completeAllPending}
     * unblocks the sender so {@code scheduler.close()} can return.
     */
    private static final class BlockingGateway extends TestingTabletGatewayService {
        final List<PutKvRequest> received = new CopyOnWriteArrayList<>();
        final LinkedBlockingQueue<CompletableFuture<PutKvResponse>> pending =
                new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            received.add(request);
            CompletableFuture<PutKvResponse> f = new CompletableFuture<>();
            pending.add(f);
            return f;
        }

        void completeAllPending(PutKvResponse resp) {
            CompletableFuture<PutKvResponse> f;
            while ((f = pending.poll()) != null) {
                f.complete(resp);
            }
        }
    }
}
