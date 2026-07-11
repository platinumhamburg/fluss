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
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CleanStatsTest {

    @Test
    void emptyHasZeroPlannedAndActualCounters() {
        CleanStats stats = CleanStats.empty();

        assertThat(stats.scannedFiles()).isZero();
        assertThat(stats.plannedFiles()).isZero();
        assertThat(stats.plannedDirs()).isZero();
        assertThat(stats.plannedBytes()).isZero();
        assertThat(stats.deletedFiles()).isZero();
        assertThat(stats.emptyDirsRemoved()).isZero();
        assertThat(stats.deleteFailures()).isZero();
        assertThat(stats.bytesReclaimed()).isZero();
    }

    @Test
    void builderRetainsAllOperationalCountersByObjectTypeAndSkipReason() {
        CleanStats stats =
                CleanStats.builder(ScopeIdentity.table("db", "orders", 7L))
                        .scanned(CleanupObjectType.LOG_SEGMENT, 2L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 100L)
                        .deleteFailed(CleanupObjectType.LOG_SEGMENT, 1L)
                        .plannedDirectory(1L)
                        .removedDirectory(1L)
                        .skipped(SkipReasonCode.KEEP_ACTIVE, 1L)
                        .build();

        assertThat(stats.scannedFiles()).isEqualTo(2L);
        assertThat(stats.plannedDirs()).isEqualTo(1L);
        assertThat(stats.emptyDirsRemoved()).isEqualTo(1L);
        assertThat(stats.deleteFailures()).isEqualTo(1L);
        assertThat(stats.byObjectType().get(CleanupObjectType.LOG_SEGMENT).deleteFailures())
                .isEqualTo(1L);
        assertThat(stats.byObjectType().get(CleanupObjectType.DIRECTORY).plannedDirs())
                .isEqualTo(1L);
        assertThat(stats.bySkipReason()).containsEntry(SkipReasonCode.KEEP_ACTIVE, 1L);
    }
}
