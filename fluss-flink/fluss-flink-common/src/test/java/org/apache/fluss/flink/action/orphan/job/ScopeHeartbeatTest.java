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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ScopeHeartbeatTest {

    @Test
    void reportsCurrentTargetAndCumulativeProgressWhileTargetIsBlocked() {
        AtomicLong now = new AtomicLong(1_000L);
        ScopePlanStats stats = new ScopePlanStats();
        stats.database();
        stats.table();
        stats.partition();
        List<ScopeHeartbeat.Snapshot> snapshots = new ArrayList<>();
        ScopeHeartbeat heartbeat = new ScopeHeartbeat(stats, now::get, snapshots::add);

        heartbeat.phase("task_planning");
        heartbeat.totalTargets(7L);
        heartbeat.targetStart("db", "orders", 12L, 34L);
        now.set(6_000L);
        heartbeat.emitNow();

        ScopeHeartbeat.Snapshot snapshot = snapshots.get(0);
        assertThat(snapshot.phase()).isEqualTo("task_planning");
        assertThat(snapshot.completedTargets()).isZero();
        assertThat(snapshot.totalTargets()).isEqualTo(7L);
        assertThat(snapshot.database()).isEqualTo("db");
        assertThat(snapshot.table()).isEqualTo("orders");
        assertThat(snapshot.tableId()).isEqualTo(12L);
        assertThat(snapshot.partitionId()).isEqualTo(34L);
        assertThat(snapshot.targetElapsedMillis()).isEqualTo(5_000L);
        assertThat(snapshot.stats().databases()).isEqualTo(1L);

        heartbeat.targetComplete();
        heartbeat.emitNow();
        assertThat(snapshots.get(1).completedTargets()).isEqualTo(1L);
        assertThat(snapshots.get(1).database()).isNull();
        assertThat(snapshots.get(1).targetElapsedMillis()).isZero();
    }
}
