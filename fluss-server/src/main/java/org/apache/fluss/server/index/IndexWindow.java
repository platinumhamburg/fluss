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
 * covers the half-open WAL offset range {@code [windowStart, windowEndOffset)} (aligned to {@code
 * LogRecordBatch} boundaries) and fans out into {@code remaining} index write batches across one or
 * more target index buckets. Only when every one of those batches has been acknowledged does the
 * window advance the owning replicator's pushed offset to {@link #windowEndOffset}.
 *
 * <p>Because window boundaries are derived deterministically from the WAL batch layout plus a fixed
 * maximum window size, a replicator restarting from its pushed offset replays the exact same window
 * trajectory, making the scheme idempotent under at-least-once delivery.
 */
@Internal
final class IndexWindow {

    private final long windowEndOffset;
    private final AtomicInteger remaining;
    private final IndexReplicator owner;

    /** One-shot guard so the owning replicator's pushed offset is advanced at most once. */
    private final AtomicBoolean completed = new AtomicBoolean(false);

    IndexWindow(long windowEndOffset, int batchCount, IndexReplicator owner) {
        this.windowEndOffset = windowEndOffset;
        this.remaining = new AtomicInteger(batchCount);
        this.owner = owner;
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
        if (remaining.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
            owner.onWindowComplete(windowEndOffset);
        }
    }
}
