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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Unit tests for {@link IndexAccumulator} per-bucket queue, re-enqueue ordering and listener. */
public class IndexAccumulatorTest {

    private static IndexBatch batch(TableBucket targetBucket) {
        return batch(
                targetBucket, new IndexWindow("idx", 1L, 1, replicator(new IndexAccumulator())));
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window) {
        return batch(targetBucket, window, 3);
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window, int size) {
        byte[] bytes = new byte[size];
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, window);
    }

    private static IndexReplicator replicator(IndexAccumulator accumulator) {
        return new IndexReplicator(
                null, Collections.emptyList(), accumulator, null, 0L, 1024, (sync, all) -> {});
    }

    @Test
    void reEnqueuePreservesPerBucketOrder() {
        IndexAccumulator accumulator = new IndexAccumulator();
        TableBucket bucket = new TableBucket(1L, 0);
        IndexBatch first = batch(bucket);
        IndexBatch second = batch(bucket);

        accumulator.append(first);
        accumulator.append(second);

        // Poll the head, then a failed send re-enqueues it to the front.
        assertThat(accumulator.pollFirst(bucket)).isSameAs(first);
        assertThat(accumulator.reEnqueueIfActive(first, 0L)).isTrue();

        // Re-enqueued batch must come back before the rest, preserving order.
        assertThat(accumulator.pollFirst(bucket)).isSameAs(first);
        assertThat(accumulator.pollFirst(bucket)).isSameAs(second);
        assertThat(accumulator.pollFirst(bucket)).isNull();

        // The re-enqueue bumps the retry counter.
        assertThat(first.attempts()).isEqualTo(1);
    }

    @Test
    void appendListenerFiresOnEveryAppend() {
        IndexAccumulator accumulator = new IndexAccumulator();
        AtomicInteger wakeups = new AtomicInteger(0);
        accumulator.setAppendListener(bucket -> wakeups.incrementAndGet());

        accumulator.append(batch(new TableBucket(1L, 0)));
        accumulator.append(batch(new TableBucket(1L, 1)));

        assertThat(wakeups.get()).isEqualTo(2);
    }

    @Test
    void hasUnsentReflectsQueueState() {
        IndexAccumulator accumulator = new IndexAccumulator();
        TableBucket bucket = new TableBucket(2L, 0);
        assertThat(accumulator.hasUnsent()).isFalse();

        accumulator.append(batch(bucket));
        assertThat(accumulator.hasUnsent()).isTrue();
        assertThat(accumulator.buckets()).contains(bucket);

        accumulator.pollFirst(bucket);
        assertThat(accumulator.hasUnsent()).isFalse();
    }

    @Test
    void removeExactQueuedBatchDoesNotRemoveSibling() {
        IndexAccumulator accumulator = new IndexAccumulator();
        TableBucket bucket = new TableBucket(3L, 0);
        IndexBatch first = batch(bucket);
        IndexBatch second = batch(bucket);
        accumulator.append(first);
        accumulator.append(second);

        assertThat(accumulator.remove(first)).isTrue();
        accumulator.release(first);
        assertThat(accumulator.pollFirst(bucket)).isSameAs(second);
        accumulator.release(second);
        assertThat(accumulator.remove(first)).isFalse();
        assertThat(accumulator.hasUnsent()).isFalse();
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    void pollFirstOnUnknownBucketReturnsNull() {
        IndexAccumulator accumulator = new IndexAccumulator();
        assertThat(accumulator.pollFirst(new TableBucket(9L, 9))).isNull();
    }

    @Test
    void dropForReplicatorRemovesOnlyThatReplicatorsBatches() {
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator ownerA = replicator(accumulator);
        IndexReplicator ownerB = replicator(accumulator);

        // Two source replicators feed the same index bucket; cleanup must be scoped per producing
        // replicator, not per index table, otherwise it would wrongly drop a sibling bucket's
        // still-deliverable batches.
        TableBucket shared = new TableBucket(500L, 0);
        accumulator.append(batch(shared, new IndexWindow("idx", 1L, 1, ownerA)));
        accumulator.append(batch(shared, new IndexWindow("idx", 2L, 1, ownerB)));
        TableBucket otherBucket = new TableBucket(500L, 1);
        accumulator.append(batch(otherBucket, new IndexWindow("idx", 3L, 1, ownerA)));

        int dropped = accumulator.dropForReplicator(ownerA);

        assertThat(dropped).isEqualTo(2);
        // ownerB's batch on the shared bucket survives and is still pollable.
        IndexBatch surviving = accumulator.pollFirst(shared);
        assertThat(surviving).isNotNull();
        accumulator.release(surviving);
        assertThat(accumulator.pollFirst(shared)).isNull();
        // ownerA's batch on the other bucket is gone; once ownerB's is drained nothing remains.
        assertThat(accumulator.pollFirst(otherBucket)).isNull();
        assertThat(accumulator.hasUnsent()).isFalse();
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    void dropForReplicatorContinuesAfterDropListenerFailure() {
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator owner = replicator(accumulator);
        IndexBatch first =
                batch(new TableBucket(501L, 0), new IndexWindow("idx", 1L, 1, owner));
        IndexBatch second =
                batch(new TableBucket(501L, 1), new IndexWindow("idx", 2L, 1, owner));
        accumulator.append(first);
        accumulator.append(second);

        List<IndexBatch> observed = new ArrayList<>();
        AtomicInteger callbacks = new AtomicInteger();
        accumulator.setDropListener(
                batch -> {
                    observed.add(batch);
                    if (callbacks.getAndIncrement() == 0) {
                        throw new RuntimeException("expected listener failure");
                    }
                });

        AtomicInteger dropped = new AtomicInteger(-1);
        assertThatCode(() -> dropped.set(accumulator.dropForReplicator(owner)))
                .doesNotThrowAnyException();

        assertThat(dropped.get()).isEqualTo(2);
        assertThat(observed).containsExactlyInAnyOrder(first, second);
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.pendingBytes(owner)).isZero();
        assertThat(accumulator.pendingOwnerCountForTesting()).isZero();
        assertThat(accumulator.hasUnsent()).isFalse();
    }

    @Test
    void ownerCleanupCannotDetachConcurrentInitialAppend() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator stoppingOwner = replicator(accumulator);
        IndexReplicator publishingOwner = replicator(accumulator);
        TableBucket bucket = new TableBucket(502L, 0);
        IndexBatch seed =
                batch(bucket, new IndexWindow("idx", 1L, 1, stoppingOwner), 5);
        accumulator.append(seed);
        assertThat(accumulator.pollFirst(bucket)).isSameAs(seed);
        accumulator.release(seed);
        seed.window().onBatchAcked(seed);

        Deque<IndexBatch> deque = queue(accumulator, bucket);
        IndexBatch published =
                batch(bucket, new IndexWindow("idx", 10L, 1, publishingOwner), 7);
        AtomicReference<Throwable> appendFailure = new AtomicReference<>();
        Thread appendThread =
                new Thread(
                        () -> {
                            try {
                                accumulator.append(published);
                            } catch (Throwable t) {
                                appendFailure.set(t);
                            }
                        },
                        "index-initial-publisher");

        synchronized (deque) {
            appendThread.start();
            awaitBlocked(appendThread);
            stoppingOwner.close();
            assertThat(accumulator.dropForReplicator(stoppingOwner)).isZero();
        }
        appendThread.join(5_000L);

        assertThat(appendThread.isAlive()).isFalse();
        assertThat(appendFailure.get()).isNull();
        assertThat(accumulator.pollFirst(bucket)).isSameAs(published);
        accumulator.release(published);
        published.window().onBatchAcked(published);
        assertThat(publishingOwner.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(published.window().registeredBatchCount()).isZero();
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.pendingBytes(publishingOwner)).isZero();
        assertThat(accumulator.pendingOwnerCountForTesting()).isZero();
        assertThat(accumulator.hasUnsent()).isFalse();
    }

    @Test
    void ownerCleanupCannotDetachConcurrentRetryPublication() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator stoppingOwner = replicator(accumulator);
        IndexReplicator retryingOwner = replicator(accumulator);
        TableBucket bucket = new TableBucket(503L, 0);
        IndexBatch retry =
                batch(bucket, new IndexWindow("idx", 10L, 1, retryingOwner), 11);
        accumulator.append(retry);
        assertThat(accumulator.pollFirst(bucket)).isSameAs(retry);

        Deque<IndexBatch> deque = queue(accumulator, bucket);
        AtomicReference<Boolean> reEnqueued = new AtomicReference<>();
        AtomicReference<Throwable> retryFailure = new AtomicReference<>();
        Thread retryThread =
                new Thread(
                        () -> {
                            try {
                                reEnqueued.set(accumulator.reEnqueueIfActive(retry, 0L));
                            } catch (Throwable t) {
                                retryFailure.set(t);
                            }
                        },
                        "index-retry-publisher");

        synchronized (deque) {
            retryThread.start();
            awaitBlocked(retryThread);
            stoppingOwner.close();
            assertThat(accumulator.dropForReplicator(stoppingOwner)).isZero();
        }
        retryThread.join(5_000L);

        assertThat(retryThread.isAlive()).isFalse();
        assertThat(retryFailure.get()).isNull();
        assertThat(reEnqueued.get()).isTrue();
        assertThat(retry.attempts()).isEqualTo(1);
        assertThat(accumulator.pollFirst(bucket)).isSameAs(retry);
        accumulator.release(retry);
        retry.window().onBatchAcked(retry);
        assertThat(retryingOwner.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(retry.window().registeredBatchCount()).isZero();
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.pendingBytes(retryingOwner)).isZero();
        assertThat(accumulator.pendingOwnerCountForTesting()).isZero();
        assertThat(accumulator.hasUnsent()).isFalse();
    }

    @Test
    void backPressureIsScopedToProducingReplicator() {
        IndexAccumulator accumulator = new IndexAccumulator(3);
        IndexReplicator ownerA = replicator(accumulator);
        IndexReplicator ownerB = replicator(accumulator);

        accumulator.append(
                batch(
                        new TableBucket(10L, 0),
                        new IndexWindow("idx", 1L, 1, ownerA),
                        3));

        assertThat(accumulator.isFull()).isTrue();
        assertThat(accumulator.isFull(ownerA)).isTrue();
        assertThat(accumulator.pendingBytes(ownerA)).isEqualTo(3L);
        assertThat(accumulator.isFull(ownerB)).isFalse();
        assertThat(accumulator.pendingBytes(ownerB)).isZero();

        IndexBatch batch = accumulator.pollFirst(new TableBucket(10L, 0));
        assertThat(batch).isNotNull();
        accumulator.release(batch);

        assertThat(accumulator.isFull(ownerA)).isFalse();
        assertThat(accumulator.pendingBytes(ownerA)).isZero();
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.pendingOwnerCountForTesting()).isZero();
    }

    private static void awaitBlocked(Thread thread) {
        waitUntil(
                () -> thread.getState() == Thread.State.BLOCKED,
                Duration.ofSeconds(5),
                "Publisher did not block on the target deque");
    }

    @SuppressWarnings("unchecked")
    private static Deque<IndexBatch> queue(IndexAccumulator accumulator, TableBucket bucket)
            throws Exception {
        Field field = IndexAccumulator.class.getDeclaredField("batches");
        field.setAccessible(true);
        Map<TableBucket, Deque<IndexBatch>> batches =
                (Map<TableBucket, Deque<IndexBatch>>) field.get(accumulator);
        return batches.get(bucket);
    }
}
