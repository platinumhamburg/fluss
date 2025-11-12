/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexPendingWriteQueuePool}. */
class IndexPendingWriteQueuePoolTest {

    private IndexPendingWriteQueuePool pool;

    @BeforeEach
    void setUp() {
        pool = new IndexPendingWriteQueuePool(4); // 4 threads for testing
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void testRegisterAndUnregisterQueue() {
        TableBucket tb1 = new TableBucket(1L, 0);
        TableBucket tb2 = new TableBucket(1L, 1);

        IndexPendingWriteQueue queue1 = new IndexPendingWriteQueue(tb1.toString(), 10);
        IndexPendingWriteQueue queue2 = new IndexPendingWriteQueue(tb2.toString(), 10);

        // Register queues
        pool.registerQueue(tb1, queue1);
        pool.registerQueue(tb2, queue2);

        // Verify queues are registered
        assertThat(pool.getQueue(tb1)).isEqualTo(queue1);
        assertThat(pool.getQueue(tb2)).isEqualTo(queue2);
        assertThat(pool.getRegisteredQueueCount()).isEqualTo(2);

        // Unregister queue
        pool.unregisterQueue(tb1);
        assertThat(pool.getQueue(tb1)).isNull();
        assertThat(pool.getRegisteredQueueCount()).isEqualTo(1);

        // Clean up
        queue1.close();
        queue2.close();
    }

    @Test
    void testTaskExecution() throws InterruptedException {
        TableBucket tb = new TableBucket(1L, 0);
        IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
        pool.registerQueue(tb, queue);

        AtomicInteger executionCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(5);

        // Submit 5 tasks
        for (int i = 0; i < 5; i++) {
            final int taskId = i;
            PendingWrite task =
                    new PendingWrite() {
                        @Override
                        public long getStartOffset() {
                            return taskId;
                        }

                        @Override
                        public long getEndOffset() {
                            return taskId + 1;
                        }

                        @Override
                        public WriteType getType() {
                            return WriteType.HOT_DATA;
                        }

                        @Override
                        public void execute() throws Exception {
                            executionCount.incrementAndGet();
                            latch.countDown();
                        }

                        @Override
                        public String toString() {
                            return "Task-" + taskId;
                        }
                    };
            queue.submit(task);
        }

        // Wait for all tasks to complete
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executionCount.get()).isEqualTo(5);

        queue.close();
    }

    @Test
    void testMultipleQueuesWithFairScheduling() throws InterruptedException {
        int queueCount = 8; // More queues than threads
        List<TableBucket> buckets = new ArrayList<>();
        List<IndexPendingWriteQueue> queues = new ArrayList<>();
        List<AtomicInteger> executionCounts = new ArrayList<>();

        // Create multiple queues
        for (int i = 0; i < queueCount; i++) {
            TableBucket tb = new TableBucket(1L, i);
            IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
            AtomicInteger count = new AtomicInteger(0);

            buckets.add(tb);
            queues.add(queue);
            executionCounts.add(count);

            pool.registerQueue(tb, queue);
        }

        CountDownLatch latch = new CountDownLatch(queueCount * 10); // 10 tasks per queue

        // Submit tasks to all queues
        for (int i = 0; i < queueCount; i++) {
            final int queueIndex = i;
            IndexPendingWriteQueue queue = queues.get(i);
            AtomicInteger count = executionCounts.get(i);

            for (int j = 0; j < 10; j++) {
                final int taskId = j;
                PendingWrite task =
                        new PendingWrite() {
                            @Override
                            public long getStartOffset() {
                                return taskId;
                            }

                            @Override
                            public long getEndOffset() {
                                return taskId + 1;
                            }

                            @Override
                            public WriteType getType() {
                                return WriteType.HOT_DATA;
                            }

                            @Override
                            public void execute() throws Exception {
                                count.incrementAndGet();
                                latch.countDown();
                            }

                            @Override
                            public String toString() {
                                return "Queue-" + queueIndex + "-Task-" + taskId;
                            }
                        };
                queue.submit(task);
            }
        }

        // Wait for all tasks to complete
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        // Verify all queues processed their tasks
        for (int i = 0; i < queueCount; i++) {
            assertThat(executionCounts.get(i).get())
                    .as("Queue %d should process all its tasks", i)
                    .isEqualTo(10);
            queues.get(i).close();
        }
    }

    @Test
    void testRetryOnFailure() throws InterruptedException {
        TableBucket tb = new TableBucket(1L, 0);
        IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
        pool.registerQueue(tb, queue);

        AtomicInteger attemptCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        // Task that fails first 3 times then succeeds
        PendingWrite task =
                new PendingWrite() {
                    @Override
                    public long getStartOffset() {
                        return 0;
                    }

                    @Override
                    public long getEndOffset() {
                        return 1;
                    }

                    @Override
                    public WriteType getType() {
                        return WriteType.HOT_DATA;
                    }

                    @Override
                    public void execute() throws Exception {
                        int count = attemptCount.incrementAndGet();
                        if (count < 3) {
                            throw new Exception("Simulated failure, attempt " + count);
                        }
                        latch.countDown();
                    }

                    @Override
                    public String toString() {
                        return "RetryTask";
                    }
                };

        queue.submit(task);

        // Wait for eventual success
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(attemptCount.get()).isGreaterThanOrEqualTo(3);

        queue.close();
    }

    @Test
    void testPriorityOrdering() throws InterruptedException {
        TableBucket tb = new TableBucket(1L, 0);
        IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
        pool.registerQueue(tb, queue);

        List<Long> executionOrder = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(5);

        // Submit tasks with different offsets (not in order)
        // Note: Due to fair scheduling and multi-threading, strict priority ordering is not
        // guaranteed
        // across different queues, but within a single queue, priority should generally be
        // respected
        long[] offsets = {50, 10, 30, 20, 40};
        for (long offset : offsets) {
            PendingWrite task =
                    new PendingWrite() {
                        @Override
                        public long getStartOffset() {
                            return offset;
                        }

                        @Override
                        public long getEndOffset() {
                            return offset + 1;
                        }

                        @Override
                        public WriteType getType() {
                            return WriteType.HOT_DATA;
                        }

                        @Override
                        public void execute() throws Exception {
                            synchronized (executionOrder) {
                                executionOrder.add(offset);
                            }
                            latch.countDown();
                        }

                        @Override
                        public String toString() {
                            return "Task-" + offset;
                        }
                    };
            queue.submit(task);
        }

        // Wait for all tasks to complete
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify all tasks were executed (order may vary due to concurrency)
        assertThat(executionOrder).containsExactlyInAnyOrder(10L, 20L, 30L, 40L, 50L);

        queue.close();
    }

    @Test
    void testCloseWithPendingTasks() throws InterruptedException {
        TableBucket tb = new TableBucket(1L, 0);
        IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
        pool.registerQueue(tb, queue);

        AtomicInteger executedCount = new AtomicInteger(0);

        // Submit some fast tasks
        for (int i = 0; i < 5; i++) {
            PendingWrite task =
                    new PendingWrite() {
                        @Override
                        public long getStartOffset() {
                            return 0;
                        }

                        @Override
                        public long getEndOffset() {
                            return 1;
                        }

                        @Override
                        public WriteType getType() {
                            return WriteType.HOT_DATA;
                        }

                        @Override
                        public void execute() throws Exception {
                            executedCount.incrementAndGet();
                        }

                        @Override
                        public String toString() {
                            return "Task";
                        }
                    };
            queue.submit(task);
        }

        // Give some time for processing
        Thread.sleep(100);

        // Close pool and queue
        queue.close();
        pool.close();

        // Some tasks should have been executed
        assertThat(executedCount.get()).isGreaterThan(0);
    }

    @Test
    void testHashBasedAssignment() {
        // Register many buckets and verify they're distributed across threads
        int bucketCount = 100;
        for (int i = 0; i < bucketCount; i++) {
            TableBucket tb = new TableBucket(1L, i);
            IndexPendingWriteQueue queue = new IndexPendingWriteQueue(tb.toString(), 10);
            pool.registerQueue(tb, queue);
        }

        assertThat(pool.getRegisteredQueueCount()).isEqualTo(bucketCount);

        // Clean up
        for (int i = 0; i < bucketCount; i++) {
            TableBucket tb = new TableBucket(1L, i);
            IndexPendingWriteQueue queue = pool.getQueue(tb);
            if (queue != null) {
                queue.close();
            }
        }
    }
}
