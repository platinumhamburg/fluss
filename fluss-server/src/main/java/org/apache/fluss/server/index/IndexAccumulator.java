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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * TabletServer-global staging area that accumulates pre-encoded {@link IndexBatch}es per target
 * index-table bucket. Producers ({@link IndexReplicator}s, running on the read worker pool) append
 * batches; consumers ({@link IndexSender} workers) poll batches front-to-back and re-enqueue failed
 * batches to the front to preserve per-bucket order.
 *
 * <p>This class is a pure per-bucket queue store: it has no knowledge of leaders, RPC, or retry
 * policy. Leader resolution and in-flight muting are the sender's concern. It does, however, track
 * both total pending (un-acknowledged) encoded bytes for observability and per-replicator pending
 * bytes so the read layer can apply back-pressure without letting one stalled replicator stop
 * unrelated replicators from deriving new windows.
 */
@Internal
@ThreadSafe
public final class IndexAccumulator {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAccumulator.class);

    private final ConcurrentMap<TableBucket, Deque<IndexBatch>> batches =
            MapUtils.newConcurrentMap();

    /** Upper bound on pending encoded bytes for one producing replicator. */
    private final long maxPendingBytes;

    /** Total encoded bytes of all batches currently pending, including queued and in-flight. */
    private final AtomicLong pendingBytes = new AtomicLong(0L);

    /** Pending encoded bytes grouped by the leader-side replicator that produced the batches. */
    private final ConcurrentMap<IndexReplicator, AtomicLong> pendingBytesByReplicator =
            MapUtils.newConcurrentMap();

    /** Optional callback fired on each append to promptly wake the owning sender worker. */
    @Nullable private volatile Consumer<TableBucket> appendListener;

    /**
     * Optional callback fired after a queued batch is dropped for a stopped replicator, outside
     * deque and accounting locks.
     */
    @Nullable private volatile Consumer<IndexBatch> dropListener;

    /** Creates an accumulator with no back-pressure bound (primarily for tests). */
    public IndexAccumulator() {
        this(Long.MAX_VALUE);
    }

    /** Creates an accumulator that reports back-pressure once a producer reaches the bound. */
    public IndexAccumulator(long maxPendingBytes) {
        this.maxPendingBytes = maxPendingBytes;
    }

    /** Registers a listener invoked after every {@link #append(IndexBatch)} to wake consumers. */
    public void setAppendListener(Consumer<TableBucket> appendListener) {
        this.appendListener = appendListener;
    }

    /**
     * Registers a listener invoked after a queued batch is dropped and its accounting is released.
     */
    public void setDropListener(Consumer<IndexBatch> dropListener) {
        this.dropListener = dropListener;
    }

    /** Append a batch to the tail of its target bucket's queue. */
    public void append(IndexBatch batch) {
        // Publication order is window -> batch -> deque. Sender lifecycle code never acquires its
        // lifecycle lock while holding any of these monitors.
        synchronized (batch.window()) {
            synchronized (batch) {
                if (!batch.ownerActive() || batch.isReleased()) {
                    return;
                }
                Deque<IndexBatch> deque =
                        batches.computeIfAbsent(batch.targetBucket(), k -> new ArrayDeque<>());
                synchronized (deque) {
                    deque.addLast(batch);
                }
                long bytes = batch.encoded().getBytesLength();
                pendingBytes.addAndGet(bytes);
                pendingBytesByReplicator
                        .computeIfAbsent(batch.window().owner(), ignored -> new AtomicLong())
                        .addAndGet(bytes);
                batch.markAccounted();
            }
        }
        Consumer<TableBucket> listener = this.appendListener;
        if (listener != null) {
            listener.accept(batch.targetBucket());
        }
    }

    /**
     * Returns {@code true} once the total pending encoded bytes reach the configured bound. The
     * read layer consults this before deriving a new window so derivation cannot outrun the send
     * layer.
     */
    public boolean isFull() {
        return pendingBytes.get() >= maxPendingBytes;
    }

    /**
     * Returns {@code true} when the given producing replicator has reached its pending-byte
     * back-pressure bound. The bound is intentionally scoped to the producer so one unhealthy
     * target index bucket cannot stop unrelated main-table buckets from reading WAL.
     */
    public boolean isFull(IndexReplicator owner) {
        return pendingBytes(owner) >= maxPendingBytes;
    }

    /** Total encoded bytes currently pending, including queued and in-flight. */
    public long pendingBytes() {
        return pendingBytes.get();
    }

    /** Encoded bytes currently pending for the given producing replicator. */
    public long pendingBytes(IndexReplicator owner) {
        AtomicLong ownerPendingBytes = pendingBytesByReplicator.get(owner);
        return ownerPendingBytes == null ? 0L : ownerPendingBytes.get();
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
        Deque<IndexBatch> deque = batches.get(bucket);
        if (deque == null) {
            return null;
        }
        IndexBatch batch;
        synchronized (deque) {
            batch = deque.pollFirst();
        }
        return batch;
    }

    /**
     * Like {@link #pollFirst(TableBucket)} but only returns the front batch if it is eligible for
     * sending at {@code nowMs} (its retry backoff, if any, has elapsed). A batch still in backoff
     * is left at the front of the queue, preserving per-bucket order, and {@code null} is returned.
     */
    @Nullable
    public IndexBatch pollFirstReady(TableBucket bucket, long nowMs) {
        Deque<IndexBatch> deque = batches.get(bucket);
        if (deque == null) {
            return null;
        }
        IndexBatch batch;
        synchronized (deque) {
            IndexBatch head = deque.peekFirst();
            if (head == null || head.readyAtMs() > nowMs) {
                return null;
            }
            batch = deque.pollFirst();
        }
        return batch;
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
                Deque<IndexBatch> deque =
                        batches.computeIfAbsent(batch.targetBucket(), k -> new ArrayDeque<>());
                batch.setReadyAtMs(readyAtMs);
                batch.reEnqueued();
                synchronized (deque) {
                    deque.addFirst(batch);
                }
                return true;
            }
        }
    }

    /** Remove the exact batch from its target queue without changing pending-byte accounting. */
    public boolean remove(IndexBatch batch) {
        Deque<IndexBatch> deque = batches.get(batch.targetBucket());
        if (deque == null) {
            return false;
        }
        synchronized (deque) {
            Iterator<IndexBatch> iterator = deque.iterator();
            while (iterator.hasNext()) {
                if (iterator.next() == batch) {
                    iterator.remove();
                    return true;
                }
            }
        }
        return false;
    }

    /** Release pending-byte accounting for a batch that reached a terminal state. */
    public void release(IndexBatch batch) {
        synchronized (batch) {
            if (batch.markReleased() && batch.wasAccounted()) {
                long bytes = batch.encoded().getBytesLength();
                pendingBytes.addAndGet(-bytes);
                AtomicLong ownerPendingBytes =
                        pendingBytesByReplicator.get(batch.window().owner());
                if (ownerPendingBytes != null) {
                    ownerPendingBytes.addAndGet(-bytes);
                }
            }
        }
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
        int dropped = 0;
        Set<IndexBatch> droppedBatches = new HashSet<>();
        Iterator<Map.Entry<TableBucket, Deque<IndexBatch>>> mapIt = batches.entrySet().iterator();
        while (mapIt.hasNext()) {
            Deque<IndexBatch> deque = mapIt.next().getValue();
            synchronized (deque) {
                Iterator<IndexBatch> it = deque.iterator();
                while (it.hasNext()) {
                    IndexBatch batch = it.next();
                    IndexWindow window = batch.window();
                    if (window != null && window.owner() == owner) {
                        it.remove();
                        droppedBatches.add(batch);
                        dropped++;
                    }
                }
                if (deque.isEmpty()) {
                    mapIt.remove();
                }
            }
        }
        for (IndexBatch batch : droppedBatches) {
            release(batch);
        }
        pendingBytesByReplicator.remove(owner);
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
                LOG.warn("Error notifying dropped index batch for replicator {}", owner, failure);
            }
        }
        return dropped;
    }
}
