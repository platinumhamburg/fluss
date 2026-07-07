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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexAccumulator} per-bucket queue, re-enqueue ordering and listener. */
public class IndexAccumulatorTest {

    private static IndexBatch batch(TableBucket targetBucket) {
        return batch(targetBucket, new IndexWindow(1L, 1, replicator(new IndexAccumulator())));
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window) {
        byte[] bytes = new byte[] {1, 2, 3};
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, window);
    }

    private static IndexReplicator replicator(IndexAccumulator accumulator) {
        return new IndexReplicator(
                null, Collections.emptyList(), accumulator, null, 0L, 1024, off -> {});
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
        accumulator.reEnqueue(first);

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
        accumulator.append(batch(shared, new IndexWindow(1L, 1, ownerA)));
        accumulator.append(batch(shared, new IndexWindow(2L, 1, ownerB)));
        TableBucket otherBucket = new TableBucket(500L, 1);
        accumulator.append(batch(otherBucket, new IndexWindow(3L, 1, ownerA)));

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
}
