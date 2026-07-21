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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * TabletServer-global staging area that accumulates pre-encoded {@link IndexBatch}es per target
 * index-table bucket. Producers ({@link IndexReplicator}s, running on the read worker pool) append
 * batches; consumers ({@link IndexSender} workers) poll batches front-to-back and re-enqueue failed
 * batches to the front to preserve per-bucket order.
 *
 * <p>This class is a pure per-bucket queue store: it has no knowledge of leaders, RPC, or retry
 * policy. Leader resolution and in-flight muting are the sender's concern. It does, however, track
 * total pending (un-acknowledged) retained bytes as a hard admission bound and tracks
 * per-replicator pending bytes so the read layer can apply soft back-pressure without letting one
 * stalled replicator stop unrelated replicators from deriving new windows.
 */
@Internal
@ThreadSafe
public final class IndexAccumulator {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAccumulator.class);

    private final ConcurrentMap<TableBucket, Deque<IndexBatch>> batches =
            MapUtils.newConcurrentMap();

    /** Upper bound on pending retained bytes for one producing replicator. */
    private final long maxPendingBytes;

    /** Hard upper bound on pending retained bytes across this TabletServer accumulator. */
    private final long maxTotalPendingBytes;

    /** Serializes competing capacity checks and reservations, but never releases. */
    private final Object admissionLock = new Object();

    /** Total retained bytes of all batches currently pending, including queued and in-flight. */
    private final AtomicLong pendingBytes = new AtomicLong(0L);

    /** Pending retained bytes grouped by the leader-side replicator that produced the batches. */
    private final ConcurrentMap<IndexReplicator, Long> pendingBytesByReplicator =
            MapUtils.newConcurrentMap();

    /** Optional callback fired on each append to promptly wake the owning sender worker. */
    @Nullable private volatile Consumer<TableBucket> appendListener;

    /** Deduplicated recovery work for append callbacks that failed before waking a sender. */
    private final ConcurrentMap<TableBucket, Boolean> missedAppendNotifications =
            MapUtils.newConcurrentMap();

    private final ConcurrentLinkedQueue<TableBucket> missedAppendNotificationQueue =
            new ConcurrentLinkedQueue<>();

    /**
     * Optional callback fired after a queued batch is dropped for a stopped replicator, outside
     * deque and accounting locks.
     */
    @Nullable private volatile Consumer<IndexBatch> dropListener;

    @Nullable private volatile Runnable afterAppendAdmissionHook;

    /** Creates an accumulator with no back-pressure bound (primarily for tests). */
    public IndexAccumulator() {
        this(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /** Creates an accumulator using the same bound per producer and in total. */
    public IndexAccumulator(long maxPendingBytes) {
        this(maxPendingBytes, maxPendingBytes);
    }

    /** Creates an accumulator with separate per-producer and TabletServer-wide bounds. */
    public IndexAccumulator(long maxPendingBytes, long maxTotalPendingBytes) {
        checkArgument(maxPendingBytes > 0, "maxPendingBytes must be positive");
        checkArgument(maxTotalPendingBytes > 0, "maxTotalPendingBytes must be positive");
        this.maxPendingBytes = maxPendingBytes;
        this.maxTotalPendingBytes = maxTotalPendingBytes;
    }

    /** Registers a listener invoked for every admitted batch to wake consumers. */
    public void setAppendListener(Consumer<TableBucket> appendListener) {
        this.appendListener = appendListener;
    }

    /**
     * Registers a listener invoked after a queued batch is dropped and its accounting is released.
     */
    public void setDropListener(Consumer<IndexBatch> dropListener) {
        this.dropListener = dropListener;
    }

    @VisibleForTesting
    void setAfterAppendAdmissionHook(Runnable afterAppendAdmissionHook) {
        this.afterAppendAdmissionHook = afterAppendAdmissionHook;
    }

    /**
     * Compatibility helper for single-batch windows. Multi-batch windows must use {@link
     * #tryAppendWindow(List)} so no partial fanout can escape.
     */
    public void append(IndexBatch batch) {
        checkNotNull(batch, "batch");
        checkArgument(
                batch.window().expectedBatchCount() == 1,
                "append only supports single-batch windows");
        tryAppendWindow(Collections.singletonList(batch));
    }

    /**
     * Atomically admits and publishes every target batch of one source window.
     *
     * <p>Capacity reservation is serialized across producers, while sender-visible queue
     * publication is staged behind the window's admission flag. A rejected or failed publication
     * leaves no queue or accounting residue.
     */
    public boolean tryAppendWindow(List<IndexBatch> windowBatches) {
        checkNotNull(windowBatches, "windowBatches");
        checkArgument(!windowBatches.isEmpty(), "windowBatches must not be empty");
        IndexBatch first = checkNotNull(windowBatches.get(0), "window batch");
        IndexWindow window = first.window();

        synchronized (window) {
            validateWindowBatches(windowBatches, window);
            if (!window.isActive() || window.owner().isClosed()) {
                return false;
            }

            long windowBytes = retainedBytes(windowBatches);
            if (windowBytes > maxTotalPendingBytes) {
                throw new RecordTooLargeException(
                        "Index replication window retains "
                                + windowBytes
                                + " bytes, exceeding max total pending bytes="
                                + maxTotalPendingBytes);
            }
            if (!reserve(windowBatches, window, windowBytes)) {
                return false;
            }

            try {
                Runnable hook = afterAppendAdmissionHook;
                if (hook != null) {
                    hook.run();
                }
                if (!window.isActive() || window.owner().isClosed()) {
                    rollbackWindow(windowBatches);
                    return false;
                }

                for (IndexBatch batch : windowBatches) {
                    publishStaged(batch);
                    if (!window.isActive() || window.owner().isClosed()) {
                        rollbackWindow(windowBatches);
                        return false;
                    }
                }
                window.markAdmitted();
            } catch (RuntimeException | Error failure) {
                rollbackWindow(windowBatches);
                throw failure;
            }
        }

        for (IndexBatch batch : windowBatches) {
            notifyAppend(batch);
        }
        return true;
    }

    private void validateWindowBatches(List<IndexBatch> windowBatches, IndexWindow window) {
        checkArgument(!window.isAdmitted(), "window is already admitted");
        checkArgument(
                windowBatches.size() == window.expectedBatchCount(),
                "window batch list does not match its expected batch count");
        Set<TableBucket> targetBuckets = new HashSet<>();
        for (IndexBatch batch : windowBatches) {
            checkNotNull(batch, "window batch");
            checkArgument(batch.window() == window, "all batches must reference the same window");
            checkArgument(
                    targetBuckets.add(batch.targetBucket()),
                    "window contains a duplicate target bucket");
            checkArgument(!batch.isReleased(), "window contains a released batch");
            checkArgument(!batch.wasAccounted(), "window contains an accounted batch");
        }
        checkArgument(
                window.hasExactRegisteredBatches(windowBatches),
                "window batch list does not match its registered batches");
    }

    private long retainedBytes(List<IndexBatch> windowBatches) {
        long bytes = 0L;
        for (IndexBatch batch : windowBatches) {
            try {
                bytes = Math.addExact(bytes, batch.retainedBytes());
            } catch (ArithmeticException overflow) {
                throw new RecordTooLargeException(
                        "Index replication window retained-byte total overflowed", overflow);
            }
        }
        return bytes;
    }

    private boolean reserve(List<IndexBatch> windowBatches, IndexWindow window, long windowBytes) {
        synchronized (admissionLock) {
            if (!window.isActive() || window.owner().isClosed()) {
                return false;
            }

            while (true) {
                long current = pendingBytes.get();
                if (current > maxTotalPendingBytes - windowBytes) {
                    return false;
                }
                if (pendingBytes.compareAndSet(current, current + windowBytes)) {
                    break;
                }
            }

            try {
                pendingBytesByReplicator.compute(
                        window.owner(),
                        (ignoredOwner, ownerBytes) ->
                                ownerBytes == null
                                        ? windowBytes
                                        : Math.addExact(ownerBytes, windowBytes));
            } catch (RuntimeException | Error failure) {
                pendingBytes.addAndGet(-windowBytes);
                throw failure;
            }

            for (IndexBatch batch : windowBatches) {
                if (!batch.markAccounted()) {
                    rollbackReservation(windowBatches, window.owner());
                    return false;
                }
            }
            return true;
        }
    }

    private void rollbackReservation(List<IndexBatch> windowBatches, IndexReplicator owner) {
        for (IndexBatch batch : windowBatches) {
            if (batch.wasAccounted()) {
                release(batch);
            } else {
                releaseReservedBytes(owner, batch.retainedBytes());
            }
        }
    }

    private void publishStaged(IndexBatch batch) {
        batches.compute(
                batch.targetBucket(),
                (ignored, current) -> {
                    Deque<IndexBatch> deque = current == null ? new ArrayDeque<>() : current;
                    synchronized (deque) {
                        deque.addLast(batch);
                    }
                    return deque;
                });
    }

    private void rollbackWindow(List<IndexBatch> windowBatches) {
        for (IndexBatch batch : windowBatches) {
            remove(batch);
        }
        for (IndexBatch batch : windowBatches) {
            release(batch);
        }
    }

    private void notifyAppend(IndexBatch batch) {
        Consumer<TableBucket> listener = this.appendListener;
        if (listener != null) {
            try {
                listener.accept(batch.targetBucket());
            } catch (Throwable t) {
                recordMissedAppendNotification(batch.targetBucket());
                LOG.warn(
                        "Error notifying appended index batch for target bucket {}",
                        batch.targetBucket(),
                        t);
            }
        }
    }

    private void recordMissedAppendNotification(TableBucket bucket) {
        if (missedAppendNotifications.putIfAbsent(bucket, Boolean.TRUE) == null) {
            missedAppendNotificationQueue.add(bucket);
        }
    }

    /** Returns one bucket whose append callback failed, or {@code null} when none remain. */
    @Nullable
    TableBucket pollMissedAppendNotification() {
        TableBucket bucket;
        while ((bucket = missedAppendNotificationQueue.poll()) != null) {
            if (missedAppendNotifications.remove(bucket) != null) {
                return bucket;
            }
        }
        return null;
    }

    @VisibleForTesting
    int missedAppendNotificationCountForTesting() {
        return missedAppendNotifications.size();
    }

    /**
     * Returns {@code true} once the total pending retained bytes reach the configured bound. The
     * read layer consults this before deriving a new window so derivation cannot outrun the send
     * layer.
     */
    public boolean isFull() {
        return pendingBytes.get() >= maxTotalPendingBytes;
    }

    /**
     * Returns {@code true} when the given producing replicator has reached its pending-byte
     * back-pressure bound. The bound is intentionally scoped to the producer so one unhealthy
     * target index bucket cannot stop unrelated main-table buckets from reading WAL.
     */
    public boolean isFull(IndexReplicator owner) {
        return pendingBytes(owner) >= maxPendingBytes;
    }

    /** Total retained bytes currently pending, including queued and in-flight. */
    public long pendingBytes() {
        return pendingBytes.get();
    }

    /** Retained bytes currently pending for the given producing replicator. */
    public long pendingBytes(IndexReplicator owner) {
        Long ownerPendingBytes = pendingBytesByReplicator.get(owner);
        return ownerPendingBytes == null ? 0L : ownerPendingBytes;
    }

    @VisibleForTesting
    int pendingOwnerCountForTesting() {
        return pendingBytesByReplicator.size();
    }

    /** Snapshot of the buckets currently tracked. May include buckets that have just drained. */
    public Set<TableBucket> buckets() {
        return new HashSet<>(batches.keySet());
    }

    /** Returns {@code true} if the bucket currently has at least one queued batch. */
    public boolean hasPending(TableBucket bucket) {
        Deque<IndexBatch> deque = batches.get(bucket);
        if (deque == null) {
            return false;
        }
        synchronized (deque) {
            return !deque.isEmpty();
        }
    }

    /**
     * Remove and return the front batch of the given bucket, or {@code null} if the bucket has no
     * pending batches.
     */
    @Nullable
    public IndexBatch pollFirst(TableBucket bucket) {
        IndexBatch[] result = new IndexBatch[1];
        batches.computeIfPresent(
                bucket,
                (ignored, deque) -> {
                    synchronized (deque) {
                        IndexBatch head = deque.peekFirst();
                        if (head == null || !head.window().isAdmitted()) {
                            return deque;
                        }
                        result[0] = deque.pollFirst();
                        return deque.isEmpty() ? null : deque;
                    }
                });
        return result[0];
    }

    /**
     * Like {@link #pollFirst(TableBucket)} but only returns the front batch if it is eligible for
     * sending at {@code nowMs} (its retry backoff, if any, has elapsed). A batch still in backoff
     * is left at the front of the queue, preserving per-bucket order, and {@code null} is returned.
     */
    @Nullable
    public IndexBatch pollFirstReady(TableBucket bucket, long nowMs) {
        IndexBatch[] result = new IndexBatch[1];
        batches.computeIfPresent(
                bucket,
                (ignored, deque) -> {
                    synchronized (deque) {
                        IndexBatch head = deque.peekFirst();
                        if (head == null) {
                            return null;
                        }
                        if (!head.window().isAdmitted()) {
                            return deque;
                        }
                        if (head.readyAtMs() > nowMs) {
                            return deque;
                        }
                        result[0] = deque.pollFirst();
                        return deque.isEmpty() ? null : deque;
                    }
                });
        return result[0];
    }

    /**
     * Publish a retry only while its window and accounting ownership are active. The final state
     * check, attempt increment, deadline, and deque insertion linearize under the window monitor
     * against {@link IndexWindow#tryFailAndDrain(Throwable)}.
     */
    public boolean reEnqueueIfActive(IndexBatch batch, long readyAtMs) {
        synchronized (batch.window()) {
            synchronized (batch) {
                if (!batch.ownerActive() || batch.isReleased()) {
                    return false;
                }
                batches.compute(
                        batch.targetBucket(),
                        (ignored, current) -> {
                            Deque<IndexBatch> deque =
                                    current == null ? new ArrayDeque<>() : current;
                            synchronized (deque) {
                                batch.setReadyAtMs(readyAtMs);
                                batch.reEnqueued();
                                deque.addFirst(batch);
                                return deque;
                            }
                        });
                return true;
            }
        }
    }

    /** Remove the exact batch from its target queue without changing pending-byte accounting. */
    public boolean remove(IndexBatch batch) {
        boolean[] removed = new boolean[1];
        batches.computeIfPresent(
                batch.targetBucket(),
                (ignored, deque) -> {
                    synchronized (deque) {
                        removed[0] = removeExact(deque, batch);
                        return deque.isEmpty() ? null : deque;
                    }
                });
        return removed[0];
    }

    /** Release pending-byte accounting for a batch that reached a terminal state. */
    public void release(IndexBatch batch) {
        synchronized (batch) {
            if (batch.markReleased() && batch.wasAccounted()) {
                releaseReservedBytes(batch.window().owner(), batch.retainedBytes());
            }
        }
    }

    private void releaseReservedBytes(IndexReplicator owner, long bytes) {
        pendingBytesByReplicator.computeIfPresent(
                owner, (ignored, current) -> current == bytes ? null : current - bytes);
        pendingBytes.addAndGet(-bytes);
    }

    private static boolean removeExact(Deque<IndexBatch> deque, IndexBatch batch) {
        Iterator<IndexBatch> iterator = deque.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() == batch) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /** Returns {@code true} if any bucket still has pending batches. */
    public boolean hasUnsent() {
        for (Deque<IndexBatch> deque : batches.values()) {
            synchronized (deque) {
                if (!deque.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Discard every queued batch produced by {@code owner}, returning how many were dropped. Called
     * when an {@link IndexReplicator} stops (its main-table bucket lost leadership or the table was
     * dropped) so its undelivered batches do not loop forever in the sender's at-least-once retry,
     * pinning memory and holding back-pressure. Cleanup is scoped to the producing replicator:
     * batches from other replicators are left untouched, including other buckets of the same index
     * table that may still have a live leader.
     */
    public int dropForReplicator(IndexReplicator owner) {
        Set<IndexBatch> droppedBatches = new HashSet<>();
        for (TableBucket bucket : new ArrayList<>(batches.keySet())) {
            batches.computeIfPresent(
                    bucket,
                    (ignored, deque) -> {
                        synchronized (deque) {
                            Iterator<IndexBatch> it = deque.iterator();
                            while (it.hasNext()) {
                                IndexBatch batch = it.next();
                                IndexWindow window = batch.window();
                                if (window != null && window.owner() == owner) {
                                    it.remove();
                                    droppedBatches.add(batch);
                                }
                            }
                            return deque.isEmpty() ? null : deque;
                        }
                    });
        }
        releaseAndNotifyDroppedBatches(droppedBatches);
        return droppedBatches.size();
    }

    /** Retires batches that may be queued or sender-owned after their owner fails terminally. */
    void dropBatches(List<IndexBatch> droppedBatches) {
        for (IndexBatch batch : droppedBatches) {
            remove(batch);
        }
        releaseAndNotifyDroppedBatches(droppedBatches);
    }

    private void releaseAndNotifyDroppedBatches(Iterable<IndexBatch> droppedBatches) {
        for (IndexBatch batch : droppedBatches) {
            release(batch);
        }
        Consumer<IndexBatch> listener = this.dropListener;
        if (listener != null) {
            List<Throwable> listenerFailures = new ArrayList<>();
            for (IndexBatch batch : droppedBatches) {
                try {
                    listener.accept(batch);
                } catch (Throwable t) {
                    listenerFailures.add(t);
                }
            }
            for (Throwable failure : listenerFailures) {
                LOG.warn("Error notifying dropped index batch", failure);
            }
        }
    }
}
