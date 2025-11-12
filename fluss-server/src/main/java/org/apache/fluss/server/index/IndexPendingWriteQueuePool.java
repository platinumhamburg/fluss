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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.log.FairBucketStatusMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A thread pool that manages multiple {@link IndexPendingWriteQueue}s with fixed-size threads.
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Fixed-size thread pool: avoids thread explosion in multi-table scenarios
 *   <li>Hash-based queue assignment: each TableBucket's queue is consistently assigned to a
 *       specific thread
 *   <li>Fair scheduling: uses {@link FairBucketStatusMap} to ensure fair consumption across queues
 *   <li>Natural backoff: failed writes yield to other queues without sleep, allowing progress
 *   <li>Guaranteed execution: <strong>unlimited retries</strong> to ensure all index write tasks
 *       eventually succeed, preventing index replication gaps
 *   <li>Thread-safe: can be called concurrently from multiple threads
 * </ul>
 *
 * <p><strong>Index Write Reliability:</strong> Index writes must never fail permanently, as this
 * would cause index replication discontinuity. This pool guarantees that failed tasks are re-queued
 * and retried indefinitely until success.
 *
 * <p>Thread Safety: This class is thread-safe and can be called concurrently from multiple threads.
 */
@Internal
public final class IndexPendingWriteQueuePool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexPendingWriteQueuePool.class);

    /**
     * The retry count threshold for logging errors. For example, 100 means log an error every 100
     * retries for a failed task. This helps track persistent failures without flooding logs. Note:
     * This only controls log frequency, NOT retry limits. Index writes will ALWAYS retry
     * indefinitely until success.
     */
    private static final int RETRY_LOG_THRESHOLD = 100;

    private final int threadCount;
    private final ExecutorService executor;
    private final List<QueueConsumer> consumers;
    private final Map<TableBucket, IndexPendingWriteQueue> queueMap;

    private volatile boolean closed = false;

    /**
     * Creates a new IndexPendingWriteQueuePool with fixed-size thread pool.
     *
     * @param threadCount the number of worker threads in the pool
     */
    public IndexPendingWriteQueuePool(int threadCount) {
        this.threadCount = threadCount;
        this.executor =
                Executors.newFixedThreadPool(
                        threadCount, new ExecutorThreadFactory("index-pending-write-pool"));
        this.consumers = new ArrayList<>(threadCount);
        this.queueMap = MapUtils.newConcurrentHashMap();

        // Initialize consumers
        for (int i = 0; i < threadCount; i++) {
            consumers.add(new QueueConsumer(i));
        }

        // Start all consumers
        for (QueueConsumer consumer : consumers) {
            executor.submit(consumer);
        }

        LOG.info(
                "IndexPendingWriteQueuePool initialized with {} threads, retry log threshold: {}",
                threadCount,
                RETRY_LOG_THRESHOLD);
    }

    /**
     * Registers an IndexPendingWriteQueue for a specific TableBucket. The queue will be assigned to
     * a thread based on the hash of the TableBucket.
     *
     * @param tableBucket the table bucket identifier
     * @param queue the pending write queue to register
     */
    public void registerQueue(TableBucket tableBucket, IndexPendingWriteQueue queue) {
        if (closed) {
            LOG.warn(
                    "IndexPendingWriteQueuePool is closed, cannot register queue for {}",
                    tableBucket);
            return;
        }

        IndexPendingWriteQueue existing = queueMap.putIfAbsent(tableBucket, queue);
        if (existing != null) {
            LOG.warn("Queue for {} is already registered, ignoring", tableBucket);
            return;
        }

        int threadIndex = getThreadIndex(tableBucket);
        QueueConsumer consumer = consumers.get(threadIndex);
        consumer.addQueue(tableBucket, queue);

        LOG.info(
                "Registered IndexPendingWriteQueue for {} to thread-{}",
                tableBucket,
                consumer.threadId);
    }

    /**
     * Unregisters an IndexPendingWriteQueue for a specific TableBucket.
     *
     * @param tableBucket the table bucket identifier
     */
    public void unregisterQueue(TableBucket tableBucket) {
        IndexPendingWriteQueue queue = queueMap.remove(tableBucket);
        if (queue == null) {
            return;
        }

        int threadIndex = getThreadIndex(tableBucket);
        QueueConsumer consumer = consumers.get(threadIndex);
        consumer.removeQueue(tableBucket);

        LOG.info(
                "Unregistered IndexPendingWriteQueue for {} from thread-{}",
                tableBucket,
                consumer.threadId);
    }

    /**
     * Gets the queue for a specific TableBucket.
     *
     * @param tableBucket the table bucket identifier
     * @return the pending write queue, or null if not registered
     */
    public IndexPendingWriteQueue getQueue(TableBucket tableBucket) {
        return queueMap.get(tableBucket);
    }

    /**
     * Gets the total number of registered queues.
     *
     * @return the number of registered queues
     */
    public int getRegisteredQueueCount() {
        return queueMap.size();
    }

    private int getThreadIndex(TableBucket tableBucket) {
        // Use consistent hash to assign queue to thread
        int hash = tableBucket.hashCode();
        return Math.abs(hash) % threadCount;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing IndexPendingWriteQueuePool with {} threads", threadCount);
        closed = true;

        // Close all consumers
        for (QueueConsumer consumer : consumers) {
            consumer.close();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn(
                        "IndexPendingWriteQueuePool executor did not terminate within 30 seconds, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn(
                    "Interrupted while waiting for IndexPendingWriteQueuePool executor to terminate");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        queueMap.clear();
        LOG.info("IndexPendingWriteQueuePool closed successfully");
    }

    /** Worker that consumes from multiple queues assigned to it using fair scheduling. */
    private class QueueConsumer implements Runnable {
        private final int threadId;
        private final FairBucketStatusMap<QueueStatus> queueMap;
        private volatile boolean closed = false;

        QueueConsumer(int threadId) {
            this.threadId = threadId;
            this.queueMap = new FairBucketStatusMap<>();
        }

        void addQueue(TableBucket tableBucket, IndexPendingWriteQueue queue) {
            synchronized (queueMap) {
                queueMap.update(tableBucket, new QueueStatus(queue));
            }
        }

        void removeQueue(TableBucket tableBucket) {
            synchronized (queueMap) {
                queueMap.remove(tableBucket);
            }
        }

        void close() {
            closed = true;
        }

        @Override
        public void run() {
            LOG.info("IndexPendingWriteQueuePool consumer thread-{} started", threadId);

            while (!closed || hasRemainingTasks()) {
                try {
                    boolean processed = processNextQueue();
                    if (!processed) {
                        // No queue has tasks, sleep briefly to avoid busy waiting
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    if (!closed) {
                        LOG.warn("Consumer thread-{} interrupted", threadId, e);
                    }
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOG.error("Unexpected error in consumer thread-{}", threadId, e);
                }
            }

            LOG.info(
                    "IndexPendingWriteQueuePool consumer thread-{} stopped, remaining queues: {}",
                    threadId,
                    queueMap.size());
        }

        private boolean processNextQueue() throws InterruptedException {
            List<TableBucket> buckets;
            synchronized (queueMap) {
                buckets = new ArrayList<>(queueMap.bucketSet());
            }

            if (buckets.isEmpty()) {
                return false;
            }

            // Try to process from each queue in fair order
            for (TableBucket tableBucket : buckets) {
                QueueStatus status;
                synchronized (queueMap) {
                    status = queueMap.statusValue(tableBucket);
                }

                if (status == null) {
                    continue;
                }

                IndexPendingWriteQueue queue = status.queue;
                PendingWrite pendingWrite = queue.poll(1, TimeUnit.MILLISECONDS);

                if (pendingWrite != null) {
                    boolean success = executeWrite(tableBucket, queue, pendingWrite);
                    if (success) {
                        // Successfully executed, move queue to end for fairness
                        synchronized (queueMap) {
                            queueMap.moveToEnd(tableBucket);
                        }
                        return true;
                    } else {
                        // Failed execution, re-queue and move to end (natural backoff)
                        // The task will be retried INDEFINITELY when this queue is visited again
                        // This ensures index write reliability and prevents replication gaps
                        queue.offer(pendingWrite);
                        synchronized (queueMap) {
                            queueMap.moveToEnd(tableBucket);
                        }
                        status.incrementRetryCount();

                        // Log error at threshold (for tracking persistent issues only)
                        // Note: This does NOT limit retries - tasks will retry until success
                        if (status.retryCount % RETRY_LOG_THRESHOLD == 0) {
                            LOG.error(
                                    "Failed to execute {} for {} after {} attempts (thread-{}), will continue retrying indefinitely",
                                    pendingWrite,
                                    tableBucket,
                                    status.retryCount,
                                    threadId);
                        }

                        // Continue to try other queues (natural backoff)
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean executeWrite(
                TableBucket tableBucket, IndexPendingWriteQueue queue, PendingWrite pendingWrite) {
            try {
                pendingWrite.execute();
                queue.incrementExecutedTasks();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Successfully executed {} for {} (thread-{})",
                            pendingWrite,
                            tableBucket,
                            threadId);
                }
                return true;
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to execute {} for {} (thread-{}), will retry",
                            pendingWrite,
                            tableBucket,
                            threadId,
                            e);
                }
                return false;
            }
        }

        private boolean hasRemainingTasks() {
            synchronized (queueMap) {
                for (QueueStatus status : queueMap.bucketStatusValues()) {
                    if (status.queue.getQueueSize() > 0) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /** Status tracking for each queue in the fair map. */
    private static class QueueStatus {
        private final IndexPendingWriteQueue queue;
        private int retryCount = 0;

        QueueStatus(IndexPendingWriteQueue queue) {
            this.queue = queue;
        }

        void incrementRetryCount() {
            retryCount++;
        }
    }
}
