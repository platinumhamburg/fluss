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

import org.apache.fluss.annotation.VisibleForTesting;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Tracks how long a source bucket has remained behind without advancing index progress. */
final class IndexReplicationNoProgressTracker {

    private static final long NOT_TRACKING = Long.MIN_VALUE;

    private final LongSupplier nanoTime;

    private long highestPushedOffset;
    private long noProgressStartNanos = NOT_TRACKING;

    IndexReplicationNoProgressTracker(long initialPushedOffset, long initialHighWatermark) {
        this(initialPushedOffset, initialHighWatermark, System::nanoTime);
    }

    @VisibleForTesting
    IndexReplicationNoProgressTracker(
            long initialPushedOffset, long initialHighWatermark, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.highestPushedOffset = initialPushedOffset;
        update(initialPushedOffset, initialHighWatermark);
    }

    synchronized void update(long pushedOffset, long highWatermark) {
        update(pushedOffset, highWatermark, nanoTime.getAsLong());
    }

    synchronized long noProgressTimeMs(long pushedOffset, long highWatermark) {
        long now = nanoTime.getAsLong();
        update(pushedOffset, highWatermark, now);
        if (noProgressStartNanos == NOT_TRACKING) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - noProgressStartNanos));
    }

    private void update(long pushedOffset, long highWatermark, long now) {
        if (pushedOffset >= highWatermark) {
            highestPushedOffset = Math.max(highestPushedOffset, pushedOffset);
            noProgressStartNanos = NOT_TRACKING;
            return;
        }

        if (noProgressStartNanos == NOT_TRACKING || pushedOffset > highestPushedOffset) {
            noProgressStartNanos = now;
        }
        highestPushedOffset = Math.max(highestPushedOffset, pushedOffset);
    }
}
