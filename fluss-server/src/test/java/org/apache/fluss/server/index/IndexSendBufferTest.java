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

import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link IndexSendBuffer} per-bucket queue, re-enqueue ordering and listener. */
public class IndexSendBufferTest {

    private static IndexBatch batch(TableBucket targetBucket) {
        return batch(targetBucket, new IndexReplicationWindow("idx", 1L, 1, replicator(new IndexSendBuffer())));
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexReplicationWindow window) {
        return batch(targetBucket, window, 3);
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexReplicationWindow window, int size) {
        return batch(targetBucket, window, size, size);
    }

    private static IndexBatch batch(
            TableBucket targetBucket, IndexReplicationWindow window, int size, long retainedBytes) {
        byte[] bytes = new byte[size];
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, retainedBytes, window);
    }

    private static IndexReplicator replicator(IndexSendBuffer sendBuffer) {
        return realReplicator(sendBuffer);
    }

    private static IndexReplicator realReplicator(IndexSendBuffer sendBuffer) {
        return IndexReplicator.forTesting(
                StubSourceWals.unreadable(),
                Collections.emptyList(),
                sendBuffer,
                null,
                0L,
                1024,
                1024,
                (sync, all) -> {});
    }

    @Test
    void capacityLimitsMustBePositiveAndAllConstructorsRemainAvailable() {
        assertThatCode(() -> new IndexSendBuffer()).doesNotThrowAnyException();
        assertThatCode(() -> new IndexSendBuffer(1L)).doesNotThrowAnyException();
        assertThatCode(() -> new IndexSendBuffer(1L, 2L)).doesNotThrowAnyException();

        assertThatThrownBy(() -> new IndexSendBuffer(0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IndexSendBuffer(1L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IndexSendBuffer(-1L, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void wholeWindowAdmissionIsAllOrNoneAtTotalCapacity() {
        long totalLimit = 10L;
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, totalLimit);
        AtomicInteger wakeups = new AtomicInteger();
        sendBuffer.setAppendListener(ignored -> wakeups.incrementAndGet());
        IndexReplicator ownerA = replicator(sendBuffer);
        IndexReplicationWindow ownerAWindow = new IndexReplicationWindow("idx", 10L, 1, ownerA);
        IndexBatch ownerABatch = batch(new TableBucket(710L, 0), ownerAWindow, 1, totalLimit);
        List<IndexBatch> ownerAWindowBatches = Collections.singletonList(ownerABatch);
        IndexReplicator ownerB = replicator(sendBuffer);
        IndexReplicationWindow ownerBWindow = new IndexReplicationWindow("idx", 20L, 2, ownerB);
        IndexBatch ownerBFirst = batch(new TableBucket(711L, 0), ownerBWindow, 1, 3L);
        IndexBatch ownerBSecond = batch(new TableBucket(711L, 1), ownerBWindow, 1, 7L);
        List<IndexBatch> ownerBWindowBatches = Arrays.asList(ownerBFirst, ownerBSecond);

        assertThat(sendBuffer.tryAppendWindow(ownerAWindowBatches)).isTrue();
        assertThat(sendBuffer.pendingBytes()).isEqualTo(totalLimit);

        assertThat(sendBuffer.tryAppendWindow(ownerBWindowBatches)).isFalse();
        assertThat(sendBuffer.pendingBytes(ownerB.sourceBucket())).isZero();
        assertThat(sendBuffer.hasPending(ownerBFirst.targetBucket())).isFalse();
        assertThat(sendBuffer.hasPending(ownerBSecond.targetBucket())).isFalse();
        assertThat(ownerBWindow.isAdmitted()).isFalse();
        assertThat(wakeups).hasValue(ownerAWindowBatches.size());

        sendBuffer.remove(ownerABatch);
        sendBuffer.release(ownerABatch);
    }

    @Test
    void indivisibleWindowMayCrossPerOwnerSoftThreshold() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(5L, 10L);
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 1, owner);
        IndexBatch admitted = batch(new TableBucket(712L, 0), window, 1, 7L);

        assertThat(sendBuffer.tryAppendWindow(Collections.singletonList(admitted))).isTrue();
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isEqualTo(7L);
        assertThat(sendBuffer.isFull(owner.sourceBucket())).isTrue();
        assertThat(sendBuffer.isFull()).isFalse();

        sendBuffer.remove(admitted);
        sendBuffer.release(admitted);
    }

    @Test
    void concurrentWholeWindowAdmissionNeverExceedsTotalCapacity() throws Exception {
        long totalLimit = 6L;
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, totalLimit);
        IndexReplicator ownerA = replicator(sendBuffer);
        IndexReplicationWindow windowA = new IndexReplicationWindow("idx", 10L, 2, ownerA);
        List<IndexBatch> batchesA =
                Arrays.asList(
                        batch(new TableBucket(713L, 0), windowA, 1, 3L),
                        batch(new TableBucket(713L, 1), windowA, 1, 3L));
        IndexReplicator ownerB = replicator(sendBuffer);
        IndexReplicationWindow windowB = new IndexReplicationWindow("idx", 20L, 2, ownerB);
        List<IndexBatch> batchesB =
                Arrays.asList(
                        batch(new TableBucket(714L, 0), windowB, 1, 3L),
                        batch(new TableBucket(714L, 1), windowB, 1, 3L));
        CyclicBarrier start = new CyclicBarrier(3);
        AtomicLong maximumObserved = new AtomicLong();
        AtomicInteger wakeups = new AtomicInteger();
        sendBuffer.setAfterAppendAdmissionHook(
                () -> maximumObserved.accumulateAndGet(sendBuffer.pendingBytes(), Math::max));
        sendBuffer.setAppendListener(ignored -> wakeups.incrementAndGet());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> admittedA =
                    executor.submit(
                            () -> {
                                start.await();
                                return sendBuffer.tryAppendWindow(batchesA);
                            });
            Future<Boolean> admittedB =
                    executor.submit(
                            () -> {
                                start.await();
                                return sendBuffer.tryAppendWindow(batchesB);
                            });
            start.await();

            boolean resultA = admittedA.get(5, TimeUnit.SECONDS);
            boolean resultB = admittedB.get(5, TimeUnit.SECONDS);

            assertThat(resultA).isNotEqualTo(resultB);
            assertThat(sendBuffer.pendingBytes()).isEqualTo(totalLimit);
            assertThat(maximumObserved).hasValue(totalLimit);
            assertThat(wakeups).hasValue(2);
            assertThat(windowA.isAdmitted()).isEqualTo(resultA);
            assertThat(windowB.isAdmitted()).isEqualTo(resultB);
            List<IndexBatch> rejected = resultA ? batchesB : batchesA;
            for (IndexBatch batch : rejected) {
                assertThat(sendBuffer.hasPending(batch.targetBucket())).isFalse();
            }
            for (IndexBatch batch : resultA ? batchesA : batchesB) {
                sendBuffer.remove(batch);
                sendBuffer.release(batch);
            }
            assertThat(sendBuffer.pendingBytes()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void totalCapacityUsesRetainedPagesRatherThanLogicalPayload() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, 4096L);
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 2, owner);
        IndexBatch first = batch(new TableBucket(715L, 0), window, 1, 4096L);
        IndexBatch second = batch(new TableBucket(715L, 1), window, 1, 4096L);

        assertThat(first.encoded().getBytesLength() + second.encoded().getBytesLength())
                .isLessThan(4096);
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Arrays.asList(first, second)))
                .isInstanceOf(RecordTooLargeException.class);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isZero();
        assertThat(sendBuffer.hasPending(first.targetBucket())).isFalse();
        assertThat(sendBuffer.hasPending(second.targetBucket())).isFalse();
        assertThat(window.isAdmitted()).isFalse();
    }

    @Test
    void retainedByteOverflowIsRejectedWithoutAccounting() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(Long.MAX_VALUE, Long.MAX_VALUE);
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 2, owner);
        IndexBatch first = batch(new TableBucket(716L, 0), window, 0, Long.MAX_VALUE);
        IndexBatch second = batch(new TableBucket(716L, 1), window, 0, 1L);

        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Arrays.asList(first, second)))
                .isInstanceOf(RecordTooLargeException.class);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isZero();
    }

    @Test
    void retryDoesNotReserveAgainAndAckReleasesExactlyOnce() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(5L, 5L);
        IndexReplicator owner = realReplicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 1, owner);
        IndexBatch batch = batch(new TableBucket(717L, 0), window, 1, 5L);

        assertThat(sendBuffer.tryAppendWindow(Collections.singletonList(batch))).isTrue();
        assertThat(sendBuffer.pollFirst(batch.targetBucket())).isSameAs(batch);
        assertThat(sendBuffer.reEnqueueIfActive(batch, 0L)).isTrue();
        assertThat(sendBuffer.pendingBytes()).isEqualTo(5L);
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isEqualTo(5L);
        assertThat(sendBuffer.pollFirst(batch.targetBucket())).isSameAs(batch);

        sendBuffer.release(batch);
        sendBuffer.release(batch);
        window.onBatchAcked(batch);
        window.onBatchAcked(batch);

        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(10L);
    }

    @Test
    void terminalFailureAndOwnerCloseEachReleaseWholeWindowExactlyOnce() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, 10L);
        IndexReplicator failedOwner = realReplicator(sendBuffer);
        IndexReplicationWindow failedWindow = new IndexReplicationWindow("idx", 10L, 2, failedOwner);
        List<IndexBatch> failedBatches =
                Arrays.asList(
                        batch(new TableBucket(718L, 0), failedWindow, 1, 2L),
                        batch(new TableBucket(718L, 1), failedWindow, 1, 3L));
        assertThat(sendBuffer.tryAppendWindow(failedBatches)).isTrue();

        RuntimeException failure = new RuntimeException("terminal");
        List<IndexBatch> drained = failedWindow.tryFailAndDrain(failure);
        assertThat(drained).containsExactlyInAnyOrderElementsOf(failedBatches);
        sendBuffer.dropBatches(drained);
        sendBuffer.dropBatches(drained);
        assertThat(failedOwner.terminalFailure()).isSameAs(failure);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(failedOwner.sourceBucket())).isZero();

        IndexReplicator closedOwner = realReplicator(sendBuffer);
        IndexReplicationWindow closedWindow = new IndexReplicationWindow("idx", 20L, 2, closedOwner);
        List<IndexBatch> closedBatches =
                Arrays.asList(
                        batch(new TableBucket(719L, 0), closedWindow, 1, 4L),
                        batch(new TableBucket(719L, 1), closedWindow, 1, 6L));
        assertThat(sendBuffer.tryAppendWindow(closedBatches)).isTrue();

        closedOwner.close();
        closedOwner.close();
        assertThat(sendBuffer.dropForSource(closedOwner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(closedOwner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
    }

    @Test
    void ownerCloseAfterReservationRollsBackEveryTarget() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, 10L);
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 2, owner);
        IndexBatch first = batch(new TableBucket(720L, 0), window, 1, 4L);
        IndexBatch second = batch(new TableBucket(720L, 1), window, 1, 6L);
        List<IndexBatch> batches = Arrays.asList(first, second);
        CountDownLatch reserved = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicInteger wakeups = new AtomicInteger();
        sendBuffer.setAppendListener(ignored -> wakeups.incrementAndGet());
        sendBuffer.setAfterAppendAdmissionHook(
                () -> {
                    reserved.countDown();
                    await(resume);
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> admission = executor.submit(() -> sendBuffer.tryAppendWindow(batches));
            assertThat(reserved.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(sendBuffer.pendingBytes()).isEqualTo(10L);

            owner.close();
            resume.countDown();

            assertThat(admission.get(5, TimeUnit.SECONDS)).isFalse();
            assertThat(sendBuffer.pendingBytes()).isZero();
            assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isZero();
            assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
            assertThat(sendBuffer.hasPending(first.targetBucket())).isFalse();
            assertThat(sendBuffer.hasPending(second.targetBucket())).isFalse();
            assertThat(window.isAdmitted()).isFalse();
            assertThat(wakeups).hasValue(0);
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void wholeWindowInputMustBeCompleteExactAndUnique() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(100L, 100L);
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 2, owner);
        IndexBatch first = batch(new TableBucket(721L, 0), window);
        IndexBatch second = batch(new TableBucket(721L, 1), window);

        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Collections.singletonList(first)))
                .isInstanceOf(IllegalArgumentException.class);

        IndexReplicationWindow otherWindow = new IndexReplicationWindow("idx", 20L, 1, owner);
        IndexBatch other = batch(new TableBucket(722L, 0), otherWindow);
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Arrays.asList(first, other)))
                .isInstanceOf(IllegalArgumentException.class);

        IndexReplicationWindow duplicateWindow = new IndexReplicationWindow("idx", 30L, 2, owner);
        IndexBatch duplicateFirst = batch(new TableBucket(723L, 0), duplicateWindow);
        IndexBatch duplicateSecond = batch(new TableBucket(723L, 0), duplicateWindow);
        assertThatThrownBy(
                        () ->
                                sendBuffer.tryAppendWindow(
                                        Arrays.asList(duplicateFirst, duplicateSecond)))
                .isInstanceOf(IllegalArgumentException.class);

        IndexReplicationWindow overRegisteredWindow = new IndexReplicationWindow("idx", 40L, 2, owner);
        IndexBatch registeredFirst = batch(new TableBucket(725L, 0), overRegisteredWindow);
        IndexBatch registeredSecond = batch(new TableBucket(725L, 1), overRegisteredWindow);
        batch(new TableBucket(725L, 2), overRegisteredWindow);
        assertThatThrownBy(
                        () ->
                                sendBuffer.tryAppendWindow(
                                        Arrays.asList(registeredFirst, registeredSecond)))
                .isInstanceOf(IllegalArgumentException.class);

        IndexReplicationWindow releasedWindow = new IndexReplicationWindow("idx", 50L, 1, owner);
        IndexBatch released = batch(new TableBucket(726L, 0), releasedWindow);
        assertThat(released.markReleased()).isTrue();
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Collections.singletonList(released)))
                .isInstanceOf(IllegalArgumentException.class);

        IndexReplicationWindow accountedWindow = new IndexReplicationWindow("idx", 60L, 1, owner);
        IndexBatch accounted = batch(new TableBucket(727L, 0), accountedWindow);
        assertThat(accounted.markAccounted()).isTrue();
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Collections.singletonList(accounted)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> sendBuffer.append(first))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(window.isAdmitted()).isFalse();

        assertThat(sendBuffer.tryAppendWindow(Arrays.asList(first, second))).isTrue();
        assertThatThrownBy(() -> sendBuffer.tryAppendWindow(Arrays.asList(first, second)))
                .isInstanceOf(IllegalArgumentException.class);
        for (IndexBatch admitted : Arrays.asList(first, second)) {
            sendBuffer.remove(admitted);
            sendBuffer.release(admitted);
        }
    }

    @Test
    void senderPollingLeavesStagedUnadmittedHeadInPlace() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 1, owner);
        IndexBatch batch = batch(new TableBucket(724L, 0), window);
        sendBuffer.publishStagedForTesting(batch);

        assertThat(sendBuffer.pollFirst(batch.targetBucket())).isNull();
        assertThat(sendBuffer.pollFirstReady(batch.targetBucket(), Long.MAX_VALUE)).isNull();
        assertThat(sendBuffer.hasPending(batch.targetBucket())).isTrue();

        window.markAdmitted();
        assertThat(sendBuffer.pollFirstReady(batch.targetBucket(), Long.MAX_VALUE))
                .isSameAs(batch);
        assertThat(sendBuffer.hasPending(batch.targetBucket())).isFalse();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    void pendingAccountingUsesRetainedBytes() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator owner = replicator(sendBuffer);
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 10L, 1, owner);
        IndexBatch batch = batch(new TableBucket(700L, 0), window, 3, 4096L);

        sendBuffer.append(batch);

        assertThat(batch.encoded().getBytesLength()).isEqualTo(3);
        assertThat(batch.retainedBytes()).isEqualTo(4096L);
        assertThat(window.registeredPayloadBytes()).isEqualTo(3L);
        assertThat(sendBuffer.pendingBytes()).isEqualTo(4096L);
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isEqualTo(4096L);

        sendBuffer.release(sendBuffer.pollFirst(batch.targetBucket()));
        assertThat(sendBuffer.pendingBytes()).isZero();
    }

    @Test
    void retainedBytesMustCoverEncodedPayload() {
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 1L, 1, replicator(new IndexSendBuffer()));

        assertThatThrownBy(() -> batch(new TableBucket(701L, 0), window, 3, 2L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retainedBytes must cover the encoded payload");
    }

    @Test
    void retainedBytesMustNotBeNegative() {
        IndexReplicationWindow window = new IndexReplicationWindow("idx", 1L, 1, replicator(new IndexSendBuffer()));

        assertThatThrownBy(() -> batch(new TableBucket(702L, 0), window, 0, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retainedBytes must cover the encoded payload");
    }

    @Test
    void reEnqueuePreservesPerBucketOrder() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        TableBucket bucket = new TableBucket(1L, 0);
        IndexBatch first = batch(bucket);
        IndexBatch second = batch(bucket);

        sendBuffer.append(first);
        sendBuffer.append(second);

        // Poll the head, then a failed send re-enqueues it to the front.
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(first);
        assertThat(sendBuffer.reEnqueueIfActive(first, 0L)).isTrue();

        // Re-enqueued batch must come back before the rest, preserving order.
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(first);
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(second);
        assertThat(sendBuffer.pollFirst(bucket)).isNull();

        // The re-enqueue bumps the retry counter.
        assertThat(first.attempts()).isEqualTo(1);
    }

    @Test
    void appendListenerFiresOnEveryAppend() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        AtomicInteger wakeups = new AtomicInteger(0);
        sendBuffer.setAppendListener(bucket -> wakeups.incrementAndGet());

        sendBuffer.append(batch(new TableBucket(1L, 0)));
        sendBuffer.append(batch(new TableBucket(1L, 1)));

        assertThat(wakeups.get()).isEqualTo(2);
    }

    @Test
    void hasUnsentReflectsQueueState() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        TableBucket bucket = new TableBucket(2L, 0);
        assertThat(sendBuffer.hasUnsent()).isFalse();

        sendBuffer.append(batch(bucket));
        assertThat(sendBuffer.hasUnsent()).isTrue();
        assertThat(sendBuffer.buckets()).contains(bucket);

        sendBuffer.pollFirst(bucket);
        assertThat(sendBuffer.hasUnsent()).isFalse();
    }

    @Test
    void removeExactQueuedBatchDoesNotRemoveSibling() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        TableBucket bucket = new TableBucket(3L, 0);
        IndexBatch first = batch(bucket);
        IndexBatch second = batch(bucket);
        sendBuffer.append(first);
        sendBuffer.append(second);

        assertThat(sendBuffer.remove(first)).isTrue();
        sendBuffer.release(first);
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(second);
        sendBuffer.release(second);
        assertThat(sendBuffer.remove(first)).isFalse();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        assertThat(sendBuffer.pendingBytes()).isZero();
    }

    @Test
    void pollFirstOnUnknownBucketReturnsNull() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        assertThat(sendBuffer.pollFirst(new TableBucket(9L, 9))).isNull();
    }

    @Test
    void dropForReplicatorRemovesOnlyThatReplicatorsBatches() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator ownerA = replicator(sendBuffer);
        IndexReplicator ownerB = replicator(sendBuffer);

        // Two source replicators feed the same index bucket; cleanup must be scoped per producing
        // replicator, not per index table, otherwise it would wrongly drop a sibling bucket's
        // still-deliverable batches.
        TableBucket shared = new TableBucket(500L, 0);
        sendBuffer.append(batch(shared, new IndexReplicationWindow("idx", 1L, 1, ownerA)));
        sendBuffer.append(batch(shared, new IndexReplicationWindow("idx", 2L, 1, ownerB)));
        TableBucket otherBucket = new TableBucket(500L, 1);
        sendBuffer.append(batch(otherBucket, new IndexReplicationWindow("idx", 3L, 1, ownerA)));

        int dropped = sendBuffer.dropForSource(ownerA.sourceBucket());

        assertThat(dropped).isEqualTo(2);
        // ownerB's batch on the shared bucket survives and is still pollable.
        IndexBatch surviving = sendBuffer.pollFirst(shared);
        assertThat(surviving).isNotNull();
        sendBuffer.release(surviving);
        assertThat(sendBuffer.pollFirst(shared)).isNull();
        // ownerA's batch on the other bucket is gone; once ownerB's is drained nothing remains.
        assertThat(sendBuffer.pollFirst(otherBucket)).isNull();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        assertThat(sendBuffer.pendingBytes()).isZero();
    }

    @Test
    void dropForReplicatorContinuesAfterDropListenerFailure() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator owner = replicator(sendBuffer);
        IndexBatch first = batch(new TableBucket(501L, 0), new IndexReplicationWindow("idx", 1L, 1, owner));
        IndexBatch second = batch(new TableBucket(501L, 1), new IndexReplicationWindow("idx", 2L, 1, owner));
        sendBuffer.append(first);
        sendBuffer.append(second);

        List<IndexBatch> observed = new ArrayList<>();
        AtomicInteger callbacks = new AtomicInteger();
        sendBuffer.setDropListener(
                batch -> {
                    observed.add(batch);
                    if (callbacks.getAndIncrement() == 0) {
                        throw new RuntimeException("expected listener failure");
                    }
                });

        AtomicInteger dropped = new AtomicInteger(-1);
        assertThatCode(() -> dropped.set(sendBuffer.dropForSource(owner.sourceBucket())))
                .doesNotThrowAnyException();

        assertThat(dropped.get()).isEqualTo(2);
        assertThat(observed).containsExactlyInAnyOrder(first, second);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(owner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
    }

    @Test
    void appendListenerFailureMarkersRearmAndDeduplicate() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator owner = realReplicator(sendBuffer);
        TableBucket bucket = new TableBucket(504L, 0);
        IndexBatch first = batch(bucket, new IndexReplicationWindow("idx", 10L, 1, owner));
        IndexBatch second = batch(bucket, new IndexReplicationWindow("idx", 20L, 1, owner));
        IndexBatch third = batch(bucket, new IndexReplicationWindow("idx", 30L, 1, owner));
        sendBuffer.setAppendListener(
                ignored -> {
                    throw new RuntimeException("injected append listener failure");
                });

        sendBuffer.append(first);
        assertThat(sendBuffer.missedAppendNotificationCountForTesting()).isEqualTo(1);
        assertThat(sendBuffer.pollMissedAppendNotification()).isEqualTo(bucket);
        assertThat(sendBuffer.missedAppendNotificationCountForTesting()).isZero();

        sendBuffer.append(second);
        sendBuffer.append(third);
        assertThat(sendBuffer.missedAppendNotificationCountForTesting())
                .as("later repeated failures must install exactly one new marker")
                .isEqualTo(1);
        assertThat(sendBuffer.pollMissedAppendNotification()).isEqualTo(bucket);
        assertThat(sendBuffer.pollMissedAppendNotification()).isNull();

        for (IndexBatch batch : new IndexBatch[] {first, second, third}) {
            assertThat(sendBuffer.pollFirst(bucket)).isSameAs(batch);
            sendBuffer.release(batch);
            batch.window().onBatchAcked(batch);
        }
        assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(30L);
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(registrySize(sendBuffer)).isZero();
    }

    @Test
    void ownerCleanupCannotDetachConcurrentInitialAppend() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator stoppingOwner = realReplicator(sendBuffer);
        IndexReplicator publishingOwner = realReplicator(sendBuffer);
        TableBucket bucket = new TableBucket(502L, 0);
        IndexBatch seed = batch(bucket, new IndexReplicationWindow("idx", 1L, 1, stoppingOwner), 5);
        sendBuffer.append(seed);
        Deque<IndexBatch> deque = queue(sendBuffer, bucket);
        IndexBatch published = batch(bucket, new IndexReplicationWindow("idx", 10L, 1, publishingOwner), 7);
        AtomicInteger dropped = new AtomicInteger(-1);
        AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
        Thread cleanupThread =
                new Thread(
                        () -> {
                            try {
                                stoppingOwner.close();
                                dropped.set(sendBuffer.dropForSource(stoppingOwner.sourceBucket()));
                            } catch (Throwable t) {
                                cleanupFailure.set(t);
                            }
                        },
                        "index-owner-cleanup");
        AtomicReference<Throwable> appendFailure = new AtomicReference<>();
        Thread appendThread =
                new Thread(
                        () -> {
                            try {
                                sendBuffer.append(published);
                            } catch (Throwable t) {
                                appendFailure.set(t);
                            }
                        },
                        "index-initial-publisher");

        synchronized (deque) {
            cleanupThread.start();
            awaitBlocked(cleanupThread);
            appendThread.start();
            awaitBlocked(appendThread);
        }
        cleanupThread.join(5_000L);
        appendThread.join(5_000L);

        assertThat(cleanupThread.isAlive()).isFalse();
        assertThat(appendThread.isAlive()).isFalse();
        assertThat(cleanupFailure.get()).isNull();
        assertThat(appendFailure.get()).isNull();
        assertThat(dropped).hasValue(0);
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(published);
        sendBuffer.release(published);
        published.window().onBatchAcked(published);
        assertThat(publishingOwner.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(published.window().registeredBatchCount()).isZero();
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(publishingOwner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        assertThat(registrySize(sendBuffer)).isZero();
    }

    @Test
    void ownerCleanupCannotDetachConcurrentRetryPublication() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator stoppingOwner = realReplicator(sendBuffer);
        IndexReplicator retryingOwner = realReplicator(sendBuffer);
        TableBucket bucket = new TableBucket(503L, 0);
        IndexBatch retry = batch(bucket, new IndexReplicationWindow("idx", 10L, 1, retryingOwner), 11);
        IndexBatch seed = batch(bucket, new IndexReplicationWindow("idx", 1L, 1, stoppingOwner), 5);
        sendBuffer.append(retry);
        sendBuffer.append(seed);
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(retry);

        Deque<IndexBatch> deque = queue(sendBuffer, bucket);
        AtomicReference<Boolean> reEnqueued = new AtomicReference<>();
        AtomicReference<Throwable> retryFailure = new AtomicReference<>();
        Thread retryThread =
                new Thread(
                        () -> {
                            try {
                                reEnqueued.set(sendBuffer.reEnqueueIfActive(retry, 0L));
                            } catch (Throwable t) {
                                retryFailure.set(t);
                            }
                        },
                        "index-retry-publisher");
        AtomicInteger dropped = new AtomicInteger(-1);
        AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
        Thread cleanupThread =
                new Thread(
                        () -> {
                            try {
                                stoppingOwner.close();
                                dropped.set(sendBuffer.dropForSource(stoppingOwner.sourceBucket()));
                            } catch (Throwable t) {
                                cleanupFailure.set(t);
                            }
                        },
                        "index-owner-cleanup");

        synchronized (deque) {
            retryThread.start();
            awaitBlocked(retryThread);
            cleanupThread.start();
            awaitBlocked(cleanupThread);
        }
        retryThread.join(5_000L);
        cleanupThread.join(5_000L);

        assertThat(retryThread.isAlive()).isFalse();
        assertThat(cleanupThread.isAlive()).isFalse();
        assertThat(retryFailure.get()).isNull();
        assertThat(cleanupFailure.get()).isNull();
        assertThat(reEnqueued.get()).isTrue();
        assertThat(dropped).hasValue(0);
        assertThat(retry.attempts()).isEqualTo(1);
        assertThat(sendBuffer.pollFirst(bucket)).isSameAs(retry);
        sendBuffer.release(retry);
        retry.window().onBatchAcked(retry);
        assertThat(retryingOwner.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(retry.window().registeredBatchCount()).isZero();
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingBytes(retryingOwner.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        assertThat(registrySize(sendBuffer)).isZero();
    }

    @Test
    void repeatedBucketChurnReclaimsQueueRegistry() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator owner = replicator(sendBuffer);

        for (int i = 0; i < 100; i++) {
            TableBucket bucket = new TableBucket(600L + i, i);
            IndexBatch batch = batch(bucket, new IndexReplicationWindow("idx", i + 1L, 1, owner), 3);
            sendBuffer.append(batch);
            assertThat(sendBuffer.pollFirst(bucket)).isSameAs(batch);
            sendBuffer.release(batch);
            batch.window().onBatchAcked(batch);
            assertThat(registrySize(sendBuffer)).isZero();
        }

        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
    }

    @Test
    void backPressureIsScopedToProducingReplicator() {
        IndexSendBuffer sendBuffer = new IndexSendBuffer(3);
        IndexReplicator ownerA = replicator(sendBuffer);
        IndexReplicator ownerB = replicator(sendBuffer);

        sendBuffer.append(
                batch(new TableBucket(10L, 0), new IndexReplicationWindow("idx", 1L, 1, ownerA), 3));

        assertThat(sendBuffer.isFull()).isTrue();
        assertThat(sendBuffer.isFull(ownerA.sourceBucket())).isTrue();
        assertThat(sendBuffer.pendingBytes(ownerA.sourceBucket())).isEqualTo(3L);
        assertThat(sendBuffer.isFull(ownerB.sourceBucket())).isFalse();
        assertThat(sendBuffer.pendingBytes(ownerB.sourceBucket())).isZero();

        IndexBatch batch = sendBuffer.pollFirst(new TableBucket(10L, 0));
        assertThat(batch).isNotNull();
        sendBuffer.release(batch);

        assertThat(sendBuffer.isFull(ownerA.sourceBucket())).isFalse();
        assertThat(sendBuffer.pendingBytes(ownerA.sourceBucket())).isZero();
        assertThat(sendBuffer.pendingBytes()).isZero();
        assertThat(sendBuffer.pendingOwnerCountForTesting()).isZero();
    }

    private static void awaitBlocked(Thread thread) {
        waitUntil(
                () -> thread.getState() == Thread.State.BLOCKED,
                Duration.ofSeconds(5),
                "Publisher did not block on the target deque");
    }

    private static Deque<IndexBatch> queue(IndexSendBuffer sendBuffer, TableBucket bucket) {
        return sendBuffer.queuedBatchesForTesting(bucket);
    }

    private static int registrySize(IndexSendBuffer sendBuffer) {
        return sendBuffer.queuedBucketCountForTesting();
    }
}
