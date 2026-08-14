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
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A deterministic replication window produced by a single {@link IndexReplicator#poll()} cycle.
 *
 * <p>The offset is a property of the window, not of any individual {@link IndexBatch}. A window
 * covers the half-open WAL offset range {@code [windowStart, windowEndOffset)} for exactly one
 * secondary index and fans out into {@code remaining} index write batches across one or more target
 * index buckets. Only when every one of those batches has been acknowledged does the window advance
 * that index's pushed offset to {@link #windowEndOffset}.
 *
 * <p>Window boundaries may differ after failover. The source advances progress only after every
 * batch in a window completes and replays from its persisted progress after recovery. Together with
 * the target rejecting requests behind its stored writer progress, this makes different source-side
 * window boundaries safe.
 */
@Internal
final class IndexReplicationWindow {

    private final String indexName;
    private final long windowEndOffset;
    private final int expectedBatchCount;
    private int remaining;
    private final IndexReplicator owner;
    private final List<IndexBatch> batches;
    private volatile boolean admitted;
    private long admittedPayloadBytes;
    private boolean terminal;

    IndexReplicationWindow(String indexName, long windowEndOffset, int batchCount, IndexReplicator owner) {
        this.indexName = indexName;
        this.windowEndOffset = windowEndOffset;
        this.expectedBatchCount = batchCount;
        this.remaining = batchCount;
        this.owner = owner;
        this.batches = new ArrayList<>(batchCount);
        this.admitted = false;
    }

    String indexName() {
        return indexName;
    }

    long windowEndOffset() {
        return windowEndOffset;
    }

    /** The source main-table bucket of the producing replicator; the buffer's accounting key. */
    TableBucket sourceBucket() {
        return owner.sourceBucket();
    }

    /** Whether the producing replicator has been closed. */
    boolean isOwnerClosed() {
        return owner.isClosed();
    }

    synchronized void register(IndexBatch batch) {
        if (!terminal) {
            batches.add(batch);
        }
    }

    /**
     * Atomically fail this window and transfer ownership of its registered batches to the caller. A
     * null result means another terminal transition won.
     */
    @Nullable
    List<IndexBatch> tryFailAndDrain(Throwable failure) {
        List<IndexBatch> drained = tryRetireAndDrain();
        if (drained != null) {
            owner.onWindowFailed(indexName, this, failure);
        }
        return drained;
    }

    /** Retires this window without notifying its owner, for owner-wide terminal cleanup. */
    @Nullable
    List<IndexBatch> tryRetireAndDrain() {
        synchronized (this) {
            if (terminal) {
                return null;
            }
            terminal = true;
            List<IndexBatch> drained = new ArrayList<>(batches);
            batches.clear();
            return drained;
        }
    }

    synchronized boolean isActive() {
        return !terminal;
    }

    boolean isAdmitted() {
        return admitted;
    }

    synchronized void markAdmitted() {
        admittedPayloadBytes = registeredPayloadBytes();
        admitted = true;
    }

    int expectedBatchCount() {
        return expectedBatchCount;
    }

    synchronized boolean hasExactRegisteredBatches(List<IndexBatch> expected) {
        return batches.size() == expected.size() && batches.containsAll(expected);
    }

    synchronized int registeredBatchCount() {
        return batches.size();
    }

    synchronized long registeredPayloadBytes() {
        long bytes = 0L;
        for (IndexBatch batch : batches) {
            bytes += batch.encoded().getBytesLength();
        }
        return bytes;
    }

    /**
     * Acknowledge one batch belonging to this window. When the last outstanding batch is
     * acknowledged, the window is complete and the owning replicator's pushed offset is advanced.
     * Removing the exact batch from the registry makes duplicate and late acknowledgements no-ops.
     */
    void onBatchAcked(IndexBatch batch) {
        boolean completed = false;
        long completedBytes = 0L;
        synchronized (this) {
            if (terminal || !batches.remove(batch)) {
                return;
            }
            if (--remaining == 0) {
                completedBytes = admittedPayloadBytes;
                terminal = true;
                batches.clear();
                completed = true;
            }
        }
        if (completed) {
            owner.onWindowComplete(indexName, windowEndOffset, completedBytes);
        }
    }
}
