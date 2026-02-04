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
 * A queue that manages write tasks (both hot data and cold data) for IndexDataProducer.
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Unbounded queue: uses PriorityBlockingQueue with automatic capacity expansion to prevent
 *       task rejection in multi-index table scenarios
 *   <li>Priority-based execution order: processes tasks with smaller startOffset first to help
 *       index fetch progress continuously
 *   <li>External scheduling: designed to be scheduled by {@link IndexWriteTaskScheduler} with fair
 *       consumption across multiple queues
 *   <li>No blocking retries: failed tasks are re-queued without sleep, allowing natural backoff by
 *       yielding to other queues
 *   <li>Guaranteed execution: works with {@link IndexWriteTaskScheduler} to ensure
 *       <strong>unlimited retries</strong> until success, preventing index replication gaps
 * </ul>
 *
 * <p>Thread Safety: This class is thread-safe and can be called concurrently from multiple threads.
 */
@Internal
public final class IndexWriteTaskQueue implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexWriteTaskQueue.class);

    private final BlockingQueue<IndexWriteTask> queue;
    private final String tableBucketName;
    private final AtomicLong queuedTasks = new AtomicLong(0);
    private final AtomicLong executedTasks = new AtomicLong(0);

    private volatile boolean closed = false;

    /**
     * Creates a new IndexWriteTaskQueue with unbounded capacity.
     *
     * @param tableBucketName the name of the table bucket for logging
     * @param initialCapacity the initial capacity hint for the queue (actual capacity is unbounded
     *     and will grow as needed)
     */
    public IndexWriteTaskQueue(String tableBucketName, int initialCapacity) {
        this.tableBucketName = tableBucketName;
        // Use unbounded PriorityBlockingQueue to prioritize older data (smaller startOffset)
        // The queue will automatically expand as needed, preventing task rejection
        this.queue =
                new PriorityBlockingQueue<>(
                        initialCapacity, Comparator.comparingLong(IndexWriteTask::getStartOffset));

        LOG.info(
                "IndexWriteTaskQueue initialized for table bucket {} with unbounded capacity (initial: {})",
                tableBucketName,
                initialCapacity);
    }

    /**
     * Submits a write task to the unbounded queue.
     *
     * @param task the write task
     * @return true if successfully submitted, false if queue is closed
     */
    public boolean submit(IndexWriteTask task) {
        if (closed) {
            LOG.warn(
                    "IndexWriteTaskQueue is closed, cannot submit {} for table bucket {}",
                    task,
                    tableBucketName);
            return false;
        }

        // PriorityBlockingQueue is unbounded, offer() always succeeds (unless out of memory)
        queue.offer(task);
        queuedTasks.incrementAndGet();
        LOG.debug(
                "Submitted {} to queue for table bucket {}, queue size: {}",
                task,
                tableBucketName,
                queue.size());
        return true;
    }

    /**
     * Polls a write task from the queue.
     *
     * @param timeout the timeout duration
     * @param unit the timeout unit
     * @return the write task, or null if timeout expires
     * @throws InterruptedException if interrupted while waiting
     */
    @Nullable
    public IndexWriteTask poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /**
     * Offers a write task back to the queue (used for retry).
     *
     * @param task the write task
     */
    void offer(IndexWriteTask task) {
        queue.offer(task);
    }

    /**
     * Gets the current queue size.
     *
     * @return the number of pending tasks in the queue
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

        LOG.info("Closing IndexWriteTaskQueue for table bucket {}", tableBucketName);
        closed = true;

        LOG.info(
                "IndexWriteTaskQueue closed for table bucket {}, stats: queued={}, executed={}, remaining={}",
                tableBucketName,
                queuedTasks.get(),
                executedTasks.get(),
                queue.size());
    }
}
