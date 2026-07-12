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

import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ScanHeartbeatTest {

    @Test
    void zeroIntervalDoesNotCreateScheduler() {
        ScanHeartbeat heartbeat = new ScanHeartbeat(Duration.ZERO, 0, 1, 0, ignored -> {});

        assertThat(heartbeat.isScheduled()).isFalse();
        heartbeat.close();
    }

    @Test
    void reportsBlockedTaskAndCompletedTaskCountersPerSubtask() {
        AtomicLong now = new AtomicLong(1_000L);
        List<ScanHeartbeat.Snapshot> snapshots = new ArrayList<>();
        ScanHeartbeat heartbeat = new ScanHeartbeat(3, 16, 1, now::get, snapshots::add);
        ScopeIdentity first =
                ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 4);

        heartbeat.taskStart(first);
        now.set(6_000L);
        heartbeat.emitNow();

        ScanHeartbeat.Snapshot blocked = snapshots.get(0);
        assertThat(blocked.subtask()).isEqualTo(3);
        assertThat(blocked.parallelism()).isEqualTo(16);
        assertThat(blocked.attempt()).isEqualTo(1);
        assertThat(blocked.tasksCompleted()).isZero();
        assertThat(blocked.currentScope()).isEqualTo(first);
        assertThat(blocked.currentTaskElapsedMillis()).isEqualTo(5_000L);
        assertThat(blocked.counters().scannedFiles()).isZero();
        assertThat(blocked.counters().plannedFiles()).isZero();

        CleanStats stats =
                CleanStats.builder(first)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 2L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 128L)
                        .build();
        heartbeat.taskComplete(stats);
        ScopeIdentity second =
                ScopeIdentity.table("db", "payments", 8L).withPartitionAndBucket(null, 2);
        heartbeat.taskStart(second);
        now.set(9_000L);
        heartbeat.emitNow();

        ScanHeartbeat.Snapshot progressed = snapshots.get(1);
        assertThat(progressed.tasksCompleted()).isEqualTo(1L);
        assertThat(progressed.currentScope()).isEqualTo(second);
        assertThat(progressed.currentTaskElapsedMillis()).isEqualTo(3_000L);
        assertThat(progressed.counters().scannedFiles()).isEqualTo(2L);
        assertThat(progressed.counters().plannedFiles()).isEqualTo(1L);
        assertThat(progressed.counters().plannedBytes()).isEqualTo(128L);
    }

    @Test
    void heartbeatLoggerFailureIsIsolatedFromCleanup() {
        AtomicLong now = new AtomicLong(1_000L);
        ScanHeartbeat heartbeat =
                new ScanHeartbeat(
                        0,
                        1,
                        0,
                        now::get,
                        ignored -> {
                            throw new IllegalStateException("logger unavailable");
                        });
        heartbeat.taskStart(ScopeIdentity.table("db", "orders", 7L));

        assertThatCode(heartbeat::emitSafelyNow).doesNotThrowAnyException();
    }
}
