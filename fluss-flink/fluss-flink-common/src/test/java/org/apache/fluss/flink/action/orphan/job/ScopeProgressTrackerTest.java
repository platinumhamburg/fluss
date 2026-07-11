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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ScopeProgressTrackerTest {

    @Test
    void emitsCumulativeProgressAtTheConfiguredInterval() {
        AtomicLong now = new AtomicLong(1_000L);
        List<String> phases = new ArrayList<>();
        ScopeProgressTracker tracker =
                new ScopeProgressTracker(
                        Duration.ofSeconds(30), now::get, (phase, stats) -> phases.add(phase));
        ScopePlanStats stats = new ScopePlanStats();

        tracker.maybeLog("metadata", stats);
        now.set(30_999L);
        tracker.maybeLog("metadata", stats);
        now.set(31_000L);
        tracker.maybeLog("metadata", stats);
        now.set(61_000L);
        tracker.maybeLog("bucket_tasks", stats);

        assertThat(phases).containsExactly("metadata", "bucket_tasks");
    }

    @Test
    void zeroIntervalDisablesProgress() {
        List<String> phases = new ArrayList<>();
        ScopeProgressTracker tracker =
                new ScopeProgressTracker(
                        Duration.ZERO, () -> Long.MAX_VALUE, (phase, stats) -> phases.add(phase));

        tracker.maybeLog("metadata", new ScopePlanStats());

        assertThat(phases).isEmpty();
    }
}
