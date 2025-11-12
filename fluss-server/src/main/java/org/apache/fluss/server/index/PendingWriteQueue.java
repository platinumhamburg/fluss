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
 *   <li>Priority-based execution order: processes tasks with smaller startOffset first to help
 *       index fetch progress continuously
 *   <li>Asynchronous processing to avoid blocking main paths
 *   <li>Automatic retry on failures with exponential backoff
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
    private final int maxRetries;
    private final long retryBackoffMs;
    private final AtomicLong queuedTasks = new AtomicLong(0);
    private final AtomicLong executedTasks = new AtomicLong(0);
    private final AtomicLong failedTasks = new AtomicLong(0);

    private volatile boolean closed = false;

    /**
     * Creates a new PendingWriteQueue.
     *
     * @param tableBucketName the name of the table bucket for logging
     * @param queueCapacity the maximum capacity of the queue
     * @param maxRetries the maximum number of retries for failed operations
     * @param retryBackoffMs the base backoff time in milliseconds for retries
     */
    public PendingWriteQueue(
            String tableBucketName, int queueCapacity, int maxRetries, long retryBackoffMs) {
        this.tableBucketName = tableBucketName;
        // Use PriorityBlockingQueue to prioritize older data (smaller startOffset)
        this.queue =
                new PriorityBlockingQueue<>(
                        queueCapacity, Comparator.comparingLong(PendingWrite::getStartOffset));
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("index-pending-write-queue"));

        startConsumer();

        LOG.info(
                "PendingWriteQueue initialized for table bucket {} with capacity {}, maxRetries {}, retryBackoffMs {}, priority ordering by startOffset",
                tableBucketName,
                queueCapacity,
                maxRetries,
                retryBackoffMs);
    }

    /**
     * Submits a pending write operation to the queue.
     *
     * @param pendingWrite the pending write operation
     * @return true if successfully submitted, false if queue is full or closed
     */
    public boolean submit(PendingWrite pendingWrite) {
        if (closed) {
            LOG.warn(
                    "PendingWriteQueue is closed, cannot submit {} for table bucket {}",
                    pendingWrite,
                    tableBucketName);
            return false;
        }

        boolean submitted = queue.offer(pendingWrite);
        if (submitted) {
            queuedTasks.incrementAndGet();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Submitted {} to queue for table bucket {}, queue size: {}",
                        pendingWrite,
                        tableBucketName,
                        queue.size());
            }
        } else {
            LOG.warn(
                    "Failed to submit {} to queue for table bucket {}, queue is full (capacity: {})",
                    pendingWrite,
                    tableBucketName,
                    queue.size());
        }
        return submitted;
    }

    /**
     * Gets the current queue size.
     *
     * @return the number of pending operations in the queue
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * Gets the total number of queued tasks.
     *
     * @return the total number of tasks that have been queued
     */
    public long getQueuedTasksCount() {
        return queuedTasks.get();
    }

    /**
     * Gets the total number of executed tasks.
     *
     * @return the total number of tasks that have been executed
     */
    public long getExecutedTasksCount() {
        return executedTasks.get();
    }

    /**
     * Gets the total number of failed tasks.
     *
     * @return the total number of tasks that have failed after all retries
     */
    public long getFailedTasksCount() {
        return failedTasks.get();
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
        Exception lastException = null;

        while (attempt <= maxRetries && !closed) {
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
                return;
            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (attempt <= maxRetries) {
                    long backoffMs = retryBackoffMs * (1L << (attempt - 1));
                    LOG.warn(
                            "Failed to execute {} for table bucket {} (attempt {}), retrying in {} ms",
                            pendingWrite,
                            tableBucketName,
                            attempt,
                            backoffMs,
                            e);

                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Retry backoff interrupted for table bucket {}", tableBucketName);
                        break;
                    }
                }
            }
        }

        failedTasks.incrementAndGet();
        LOG.error(
                "Failed to execute {} for table bucket {} after {} attempts, giving up",
                pendingWrite,
                tableBucketName,
                attempt,
                lastException);
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
                "PendingWriteQueue closed for table bucket {}, stats: queued={}, executed={}, failed={}",
                tableBucketName,
                queuedTasks.get(),
                executedTasks.get(),
                failedTasks.get());
    }
}
