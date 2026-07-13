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
 * <p>Window boundaries may differ after failover. Correctness relies on the target WriterState
 * fence, whose sequence is this window's exclusive end offset, rather than on reproducing an
 * identical source-side trajectory.
 */
@Internal
final class IndexWindow {

    private final String indexName;
    private final long windowEndOffset;
    private int remaining;
    private final IndexReplicator owner;
    private final List<IndexBatch> batches;
    private boolean terminal;

    IndexWindow(String indexName, long windowEndOffset, int batchCount, IndexReplicator owner) {
        this.indexName = indexName;
        this.windowEndOffset = windowEndOffset;
        this.remaining = batchCount;
        this.owner = owner;
        this.batches = new ArrayList<>(batchCount);
    }

    String indexName() {
        return indexName;
    }

    long windowEndOffset() {
        return windowEndOffset;
    }

    /** The replicator that produced this window; used to scope per-replicator batch cleanup. */
    IndexReplicator owner() {
        return owner;
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
        synchronized (this) {
            if (terminal || !batches.remove(batch)) {
                return;
            }
            if (--remaining == 0) {
                terminal = true;
                batches.clear();
                completed = true;
            }
        }
        if (completed) {
            owner.onWindowComplete(indexName, windowEndOffset);
        }
    }
}
