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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
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
 *   <li>External scheduling: designed to be scheduled by {@link IndexPendingWriteQueuePool} with
 *       fair consumption across multiple queues
 *   <li>No blocking retries: failed tasks are re-queued without sleep, allowing natural backoff by
 *       yielding to other queues
 *   <li>Guaranteed execution: works with {@link IndexPendingWriteQueuePool} to ensure
 *       <strong>unlimited retries</strong> until success, preventing index replication gaps
 * </ul>
 *
 * <p>Thread Safety: This class is thread-safe and can be called concurrently from multiple threads.
 */
@Internal
public final class IndexPendingWriteQueue implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexPendingWriteQueue.class);

    private final BlockingQueue<PendingWrite> queue;
    private final String tableBucketName;
    private final AtomicLong queuedTasks = new AtomicLong(0);
    private final AtomicLong executedTasks = new AtomicLong(0);

    private volatile boolean closed = false;

    /**
     * Creates a new IndexPendingWriteQueue with unbounded capacity.
     *
     * @param tableBucketName the name of the table bucket for logging
     * @param initialCapacity the initial capacity hint for the queue (actual capacity is unbounded
     *     and will grow as needed)
     */
    public IndexPendingWriteQueue(String tableBucketName, int initialCapacity) {
        this.tableBucketName = tableBucketName;
        // Use unbounded PriorityBlockingQueue to prioritize older data (smaller startOffset)
        // The queue will automatically expand as needed, preventing task rejection
        this.queue =
                new PriorityBlockingQueue<>(
                        initialCapacity, Comparator.comparingLong(PendingWrite::getStartOffset));

        LOG.info(
                "IndexPendingWriteQueue initialized for table bucket {} with unbounded capacity (initial: {})",
                tableBucketName,
                initialCapacity);
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
                    "IndexPendingWriteQueue is closed, cannot submit {} for table bucket {}",
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
     * Polls a pending write operation from the queue.
     *
     * @param timeout the timeout duration
     * @param unit the timeout unit
     * @return the pending write operation, or null if timeout expires
     * @throws InterruptedException if interrupted while waiting
     */
    @Nullable
    public PendingWrite poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /**
     * Offers a pending write operation back to the queue (used for retry).
     *
     * @param pendingWrite the pending write operation
     */
    void offer(PendingWrite pendingWrite) {
        queue.offer(pendingWrite);
    }

    /**
     * Gets the current queue size.
     *
     * @return the number of pending operations in the queue
     */
    public int getQueueSize() {
        return queue.size();
    }

    /** Increments the executed tasks counter. */
    void incrementExecutedTasks() {
        executedTasks.incrementAndGet();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing IndexPendingWriteQueue for table bucket {}", tableBucketName);
        closed = true;

        LOG.info(
                "IndexPendingWriteQueue closed for table bucket {}, stats: queued={}, executed={}, remaining={}",
                tableBucketName,
                queuedTasks.get(),
                executedTasks.get(),
                queue.size());
    }
}
