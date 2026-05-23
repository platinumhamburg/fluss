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

package org.apache.fluss.server.replica;

import org.apache.fluss.annotation.Internal;

/**
 * Thread-safe per-{@link Replica} state holding the {@code indexPushedOffset} watermark and the
 * advance callback. Extracted from {@link Replica} so the watermark logic is unit-testable without
 * a full Replica fixture.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>{@link #advance(long)} is monotonic; a stale (non-greater) value is silently dropped. On
 *       strict increase, the advance callback is fired exactly once.
 *   <li>{@link #seed(long)} shares the monotonic semantics of {@link #advance(long)} but does NOT
 *       fire the callback. It is used for leader-startup checkpoint replay.
 *   <li>{@link #setOnAdvanced(Runnable)} accepts {@code null}, which is normalized to a no-op.
 *   <li>The initial offset is {@code -1L}, matching the "no offset pushed yet" convention.
 * </ul>
 */
@Internal
public final class IndexPushedOffsetState {

    private volatile long indexPushedOffset = -1L;
    private volatile Runnable onAdvanced = () -> {};

    /** Returns the current index-pushed-offset, or {@code -1L} if none has been advanced. */
    public long get() {
        return indexPushedOffset;
    }

    /**
     * Sets the callback invoked after {@link #advance(long)} strictly increases the offset. P2T9
     * wires this to {@code ReplicaManager.completeDelayedOperations}. A {@code null} callback is
     * normalized to a no-op.
     */
    public void setOnAdvanced(Runnable callback) {
        this.onAdvanced = callback == null ? () -> {} : callback;
    }

    /**
     * Monotonically advances the index-pushed-offset. Idempotent; a stale (non-greater) value is
     * silently dropped. On strict increase, fires the advance callback exactly once.
     */
    public void advance(long newOffset) {
        boolean advanced;
        synchronized (this) {
            advanced = newOffset > indexPushedOffset;
            if (advanced) {
                indexPushedOffset = newOffset;
            }
        }
        if (advanced) {
            onAdvanced.run();
        }
    }

    /**
     * Seeds the index-pushed-offset during leader-startup checkpoint replay. Shares the monotonic
     * semantics of {@link #advance(long)} but does NOT fire the callback.
     */
    public void seed(long offset) {
        synchronized (this) {
            if (offset > indexPushedOffset) {
                indexPushedOffset = offset;
            }
        }
    }
}
