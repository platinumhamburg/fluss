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
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A queue that manages pending write operations (both hot data and cold data) for IndexCache.
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Unbounded queue: uses PriorityBlockingQueue with automatic capacity expansion to prevent
 *       task rejection in multi-index table scenarios
 *   <li>Priority-based execution order: processes tasks with smaller startOffset first to help
 *       index fetch progress continuously
 *   <li>Asynchronous processing to avoid blocking main paths
 *   <li>Guaranteed task execution: unlimited retries with fixed 10ms backoff to ensure all tasks
 *       eventually succeed
 *   <li>Graceful shutdown with pending task draining
 * </ul>
 *
 * <p>Thread Safety: This class is thread-safe and can be called concurrently from multiple threads.
 */
@Internal
public final class PendingWriteQueue implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PendingWriteQueue.class);

    private final BlockingQueue<PendingWrite> queue;
    private final ExecutorService executor;
    private final String tableBucketName;
    private final long retryBackoffMs;
    private final int retryLogThreshold;
    private final AtomicLong queuedTasks = new AtomicLong(0);
    private final AtomicLong executedTasks = new AtomicLong(0);

    private volatile boolean closed = false;

    /**
     * Creates a new PendingWriteQueue with unbounded capacity and guaranteed task execution.
     *
     * @param tableBucketName the name of the table bucket for logging
     * @param initialCapacity the initial capacity hint for the queue (actual capacity is unbounded
     *     and will grow as needed)
     * @param retryLogThreshold the retry count threshold for logging errors (e.g., 100 means log
     *     every 100 retries)
     * @param retryBackoffMs the fixed backoff time in milliseconds between retries
     */
    public PendingWriteQueue(
            String tableBucketName,
            int initialCapacity,
            int retryLogThreshold,
            long retryBackoffMs) {
        this.tableBucketName = tableBucketName;
        // Use unbounded PriorityBlockingQueue to prioritize older data (smaller startOffset)
        // The queue will automatically expand as needed, preventing task rejection
        this.queue =
                new PriorityBlockingQueue<>(
                        initialCapacity, Comparator.comparingLong(PendingWrite::getStartOffset));
        this.retryLogThreshold = retryLogThreshold;
        this.retryBackoffMs = retryBackoffMs;
        this.executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("index-pending-write-queue"));

        startConsumer();

        LOG.info(
                "PendingWriteQueue initialized for table bucket {} with unbounded capacity (initial: {}), unlimited retries with {}ms fixed backoff, logging every {} retries",
                tableBucketName,
                initialCapacity,
                retryBackoffMs,
                retryLogThreshold);
    }

    /**
     * Submits a pending write operation to the unbounded queue.
     *
     * @param pendingWrite the pending write operation
     * @return true if successfully submitted, false if queue is closed
     */
    public boolean submit(PendingWrite pendingWrite) {
        if (closed) {
            LOG.warn(
                    "PendingWriteQueue is closed, cannot submit {} for table bucket {}",
                    pendingWrite,
                    tableBucketName);
            return false;
        }

        // PriorityBlockingQueue is unbounded, offer() always succeeds (unless out of memory)
        queue.offer(pendingWrite);
        queuedTasks.incrementAndGet();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Submitted {} to queue for table bucket {}, queue size: {}",
                    pendingWrite,
                    tableBucketName,
                    queue.size());
        }
        return true;
    }

    /**
     * Gets the current queue size.
     *
     * @return the number of pending operations in the queue
     */
    public int getQueueSize() {
        return queue.size();
    }

    private void startConsumer() {
        executor.submit(
                () -> {
                    LOG.info(
                            "PendingWriteQueue consumer started for table bucket {}",
                            tableBucketName);
                    while (!closed || !queue.isEmpty()) {
                        try {
                            PendingWrite pendingWrite = queue.poll(1, TimeUnit.SECONDS);
                            if (pendingWrite != null) {
                                executeWithRetry(pendingWrite);
                            }
                        } catch (InterruptedException e) {
                            if (!closed) {
                                LOG.warn(
                                        "PendingWriteQueue consumer interrupted for table bucket {}",
                                        tableBucketName,
                                        e);
                            }
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception e) {
                            LOG.error(
                                    "Unexpected error in PendingWriteQueue consumer for table bucket {}",
                                    tableBucketName,
                                    e);
                        }
                    }
                    LOG.info(
                            "PendingWriteQueue consumer stopped for table bucket {}, remaining tasks: {}",
                            tableBucketName,
                            queue.size());
                });
    }

    private void executeWithRetry(PendingWrite pendingWrite) {
        int attempt = 0;

        while (!closed) {
            try {
                pendingWrite.execute();
                executedTasks.incrementAndGet();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Successfully executed {} for table bucket {} (attempt {})",
                            pendingWrite,
                            tableBucketName,
                            attempt + 1);
                }
                // Task succeeded, exit retry loop
                return;
            } catch (Exception e) {
                attempt++;

                // Log error at every retryLogThreshold attempts to track persistent failures
                if (attempt % retryLogThreshold == 0) {
                    LOG.error(
                            "Failed to execute {} for table bucket {} after {} attempts, will continue retrying indefinitely",
                            pendingWrite,
                            tableBucketName,
                            attempt,
                            e);
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to execute {} for table bucket {} (attempt {}), retrying in {} ms",
                            pendingWrite,
                            tableBucketName,
                            attempt,
                            retryBackoffMs,
                            e);
                }

                // Fixed backoff before retry
                try {
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn(
                            "Retry backoff interrupted for table bucket {}, re-queuing task {}",
                            tableBucketName,
                            pendingWrite);
                    // Re-queue the task to ensure it gets executed
                    queue.offer(pendingWrite);
                    return;
                }
            }
        }

        // Only reach here if closed=true, re-queue task for graceful shutdown processing
        LOG.warn(
                "PendingWriteQueue closed while retrying {} for table bucket {}, task will be processed during shutdown",
                pendingWrite,
                tableBucketName);
        queue.offer(pendingWrite);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing PendingWriteQueue for table bucket {}", tableBucketName);
        closed = true;

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn(
                        "PendingWriteQueue executor did not terminate within 30 seconds for table bucket {}, forcing shutdown",
                        tableBucketName);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn(
                    "Interrupted while waiting for PendingWriteQueue executor to terminate for table bucket {}",
                    tableBucketName);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOG.info(
                "PendingWriteQueue closed for table bucket {}, stats: queued={}, executed={}",
                tableBucketName,
                queuedTasks.get(),
                executedTasks.get());
    }
}
