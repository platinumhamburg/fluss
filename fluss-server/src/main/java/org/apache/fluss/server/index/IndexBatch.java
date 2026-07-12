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
import org.apache.fluss.record.bytesview.BytesView;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A single pre-encoded index write unit targeting one index-table bucket.
 *
 * <p>A batch carries only a reference to the {@link IndexWindow} it belongs to; the WAL offset it
 * advances is a property of that window, not of the batch. On successful acknowledgement the batch
 * notifies its window via {@link IndexWindow#onBatchAcked(IndexBatch)}; on failure it is re-enqueued
 * for unlimited retry without advancing any offset.
 *
 * <p>A batch acknowledges its window at most once: {@link #markAcked()} is a one-shot CAS guard
 * mirroring the client {@code WriteBatch} final-state machine, so a stray duplicate completion can
 * never decrement the window twice and prematurely advance the pushed offset.
 */
@Internal
final class IndexBatch {

    private final TableBucket targetBucket;
    private final BytesView encoded;
    private final IndexWindow window;

    private int attempts;

    /** Earliest wall-clock time this batch may be re-sent; set on retry to enforce backoff. */
    private volatile long readyAtMs;

    /** One-shot guard ensuring the owning window is acknowledged at most once for this batch. */
    private final AtomicBoolean acked = new AtomicBoolean(false);

    /** One-shot guard ensuring pending-byte accounting is released at most once. */
    private final AtomicBoolean released = new AtomicBoolean(false);
    private boolean accounted;

    IndexBatch(TableBucket targetBucket, BytesView encoded, IndexWindow window) {
        this.targetBucket = checkNotNull(targetBucket, "targetBucket");
        this.encoded = checkNotNull(encoded, "encoded");
        this.window = checkNotNull(window, "window");
        this.attempts = 0;
        this.readyAtMs = 0L;
        this.accounted = false;
        window.register(this);
    }

    TableBucket targetBucket() {
        return targetBucket;
    }

    BytesView encoded() {
        return encoded;
    }

    IndexWindow window() {
        return window;
    }

    int attempts() {
        return attempts;
    }

    void reEnqueued() {
        attempts++;
    }

    long readyAtMs() {
        return readyAtMs;
    }

    void setReadyAtMs(long readyAtMs) {
        this.readyAtMs = readyAtMs;
    }

    /**
     * Atomically claim the right to acknowledge the owning window for this batch. Returns {@code
     * true} exactly once over the batch lifetime; subsequent calls return {@code false}.
     */
    boolean markAcked() {
        return acked.compareAndSet(false, true);
    }

    boolean ownerActive() {
        return window.isActive() && !window.owner().isClosed();
    }

    boolean markReleased() {
        return released.compareAndSet(false, true);
    }

    boolean isReleased() {
        return released.get();
    }

    void markAccounted() {
        accounted = true;
    }

    boolean wasAccounted() {
        return accounted;
    }
}
