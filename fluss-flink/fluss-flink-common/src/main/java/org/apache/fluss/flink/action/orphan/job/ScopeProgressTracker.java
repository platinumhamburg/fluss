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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/** Emits cumulative scope progress at a bounded interval from enumeration checkpoints. */
@Internal
final class ScopeProgressTracker {

    private final long intervalMillis;
    private final LongSupplier clock;
    private final BiConsumer<String, ScopePlanStats> logger;
    private long nextLogMillis;

    ScopeProgressTracker(Duration interval, BiConsumer<String, ScopePlanStats> logger) {
        this(interval, System::currentTimeMillis, logger);
    }

    ScopeProgressTracker(
            Duration interval, LongSupplier clock, BiConsumer<String, ScopePlanStats> logger) {
        this.intervalMillis = interval.toMillis();
        this.clock = clock;
        this.logger = logger;
        this.nextLogMillis =
                intervalMillis == 0 ? Long.MAX_VALUE : clock.getAsLong() + intervalMillis;
    }

    void maybeLog(String phase, ScopePlanStats stats) {
        if (intervalMillis == 0) {
            return;
        }
        long now = clock.getAsLong();
        if (now < nextLogMillis) {
            return;
        }
        logger.accept(phase, stats);
        nextLogMillis = now + intervalMillis;
    }
}
