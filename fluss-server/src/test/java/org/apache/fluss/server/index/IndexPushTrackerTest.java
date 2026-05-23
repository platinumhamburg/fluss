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

import org.apache.fluss.server.index.IndexPushTracker.Target;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link IndexPushTracker}. */
class IndexPushTrackerTest {

    @Test
    void testInitialIndexPushedOffsetIsMinusOne() {
        IndexPushTracker tracker = new IndexPushTracker();
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
    }

    @Test
    void testAdvanceReturnsHighestContiguousAckedOffset() {
        IndexPushTracker tracker = new IndexPushTracker();
        Target t1 = new Target(1L, 0);
        Target t2 = new Target(1L, 1);
        Target t3 = new Target(2L, 0);

        tracker.register(10L, set(t1, t2));
        tracker.register(11L, set(t3));
        tracker.register(12L, set(t1));

        assertThat(tracker.ack(10L, t1)).isEqualTo(-1L); // 10 still has t2 pending
        assertThat(tracker.ack(10L, t2)).isEqualTo(10L); // 10 completed
        assertThat(tracker.ack(12L, t1)).isEqualTo(10L); // 11 blocks 12 from advancing
        assertThat(tracker.ack(11L, t3)).isEqualTo(12L); // 11 + 12 cascade
    }

    @Test
    void testAdvanceStallsWhenLowerOffsetHasUnackedTarget() {
        IndexPushTracker tracker = new IndexPushTracker();
        Target t1 = new Target(1L, 0);
        Target t2 = new Target(2L, 0);
        tracker.register(10L, set(t1, t2));
        tracker.register(11L, set(t1));

        assertThat(tracker.ack(11L, t1)).isEqualTo(-1L);
        assertThat(tracker.ack(10L, t1)).isEqualTo(-1L); // 10 still has t2 pending
        assertThat(tracker.ack(10L, t2)).isEqualTo(11L);
    }

    @Test
    void testRepeatedAckOfSameTargetIsIdempotent() {
        IndexPushTracker tracker = new IndexPushTracker();
        Target t1 = new Target(1L, 0);
        tracker.register(10L, set(t1));
        assertThat(tracker.ack(10L, t1)).isEqualTo(10L);
        // ack again — already advanced past, no effect, returns current
        assertThat(tracker.ack(10L, t1)).isEqualTo(10L);
    }

    @Test
    void testAckOfUnknownOffsetIsNoOp() {
        IndexPushTracker tracker = new IndexPushTracker();
        Target t = new Target(1L, 0);
        assertThat(tracker.ack(999L, t)).isEqualTo(-1L);
    }

    @Test
    void testRegisterEmptyTargetSetCompletesImmediately() {
        IndexPushTracker tracker = new IndexPushTracker();
        tracker.register(5L, Collections.emptySet());
        // No ack needed; advancing requires a trigger event. Register itself
        // does not advance, but the next ack call (even a no-op) will.
        assertThat(tracker.getIndexPushedOffset()).isEqualTo(-1L);
        // A trigger ack of an unknown target now advances.
        assertThat(tracker.ack(999L, new Target(0L, 0))).isEqualTo(5L);
    }

    @Test
    void testTargetEqualsAndHashCodeUseBothFields() {
        Target a = new Target(1L, 2);
        Target b = new Target(1L, 2);
        Target c = new Target(1L, 3);
        Target d = new Target(2L, 2);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }

    @Test
    void testTargetRejectsNegativeBucket() {
        assertThatThrownBy(() -> new Target(1L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRegisterRejectsNullTargets() {
        IndexPushTracker tracker = new IndexPushTracker();
        assertThatThrownBy(() -> tracker.register(1L, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThreadSafetyUnderConcurrentAcks() throws Exception {
        IndexPushTracker tracker = new IndexPushTracker();
        int targetsPerOffset = 8;
        int offsets = 64;

        Target[] targets = new Target[targetsPerOffset];
        for (int i = 0; i < targetsPerOffset; i++) {
            targets[i] = new Target(1L, i);
        }

        for (long off = 0; off < offsets; off++) {
            tracker.register(off, set(targets));
        }

        ExecutorService pool = Executors.newFixedThreadPool(targetsPerOffset);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(targetsPerOffset);
            AtomicLong lastObserved = new AtomicLong(-1L);
            for (int t = 0; t < targetsPerOffset; t++) {
                final Target target = targets[t];
                pool.submit(
                        () -> {
                            try {
                                start.await();
                                for (long off = 0; off < offsets; off++) {
                                    long advanced = tracker.ack(off, target);
                                    // monotonic
                                    long prev;
                                    do {
                                        prev = lastObserved.get();
                                        if (advanced <= prev) {
                                            break;
                                        }
                                    } while (!lastObserved.compareAndSet(prev, advanced));
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(tracker.getIndexPushedOffset()).isEqualTo(offsets - 1);
    }

    private static Set<Target> set(Target... ts) {
        Set<Target> s = new HashSet<>();
        for (Target t : ts) {
            s.add(t);
        }
        return s;
    }
}
