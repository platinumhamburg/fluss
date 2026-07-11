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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger remaining;
    private final IndexReplicator owner;

    /** One-shot guard for the mutually exclusive completed or failed terminal transition. */
    private final AtomicBoolean terminal = new AtomicBoolean(false);

    IndexWindow(String indexName, long windowEndOffset, int batchCount, IndexReplicator owner) {
        this.indexName = indexName;
        this.windowEndOffset = windowEndOffset;
        this.remaining = new AtomicInteger(batchCount);
        this.owner = owner;
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

    /**
     * Acknowledge one batch belonging to this window. When the last outstanding batch is
     * acknowledged, the window is complete and the owning replicator's pushed offset is advanced.
     * The advance is guarded by a one-shot flag so it fires exactly once even under unexpected
     * over-acknowledgement.
     */
    void onBatchAcked() {
        if (remaining.decrementAndGet() == 0 && terminal.compareAndSet(false, true)) {
            owner.onWindowComplete(indexName, windowEndOffset);
        }
    }

    /** Fail this window terminally without advancing its source pushed offset. */
    void onBatchFailed(Throwable failure) {
        if (terminal.compareAndSet(false, true)) {
            owner.onWindowFailed(failure);
        }
    }
}
