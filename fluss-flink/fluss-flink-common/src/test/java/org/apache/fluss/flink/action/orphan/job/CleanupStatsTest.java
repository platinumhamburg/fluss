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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CleanupStatsTest {

    @Test
    void scopeSnapshotCarriesPlanAndCoverageFacts() {
        ScopeIdentity scope = ScopeIdentity.global();
        Map<SkipReasonCode, Long> skipped = new HashMap<>();
        skipped.put(SkipReasonCode.NO_REMOTE_MANIFEST, 4L);

        CleanupStats stats = CleanupStats.scope(7L, 2L, skipped);
        skipped.put(SkipReasonCode.NO_REMOTE_MANIFEST, 99L);

        assertThat(stats.sourceStage()).isEqualTo(CleanupStats.SourceStage.SCOPE);
        assertThat(stats.scope()).isEqualTo(scope);
        assertThat(stats.tasksPlanned()).isEqualTo(7L);
        assertThat(stats.metadataFailures()).isEqualTo(2L);
        assertThat(stats.skipped()).containsEntry(SkipReasonCode.NO_REMOTE_MANIFEST, 4L);
        assertThatThrownBy(() -> stats.skipped().put(SkipReasonCode.DIRECTORY_LIST_FAILED, 1L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void scanBuilderKeepsCountersAndRuleDimensionsTogether() {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 11L);
        CleanupStats stats =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 17L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(17L)
                                        .add(RuleDecisionCounters.candidate(17L)))
                        .build();

        assertThat(stats.sourceStage()).isEqualTo(CleanupStats.SourceStage.SCAN);
        assertThat(stats.counters().scannedFiles()).isEqualTo(1L);
        assertThat(stats.counters().plannedFiles()).isEqualTo(1L);
        assertThat(stats.counters().plannedBytes()).isEqualTo(17L);
        assertThat(stats.byObjectType().get(CleanupObjectType.LOG_SEGMENT).plannedFiles())
                .isEqualTo(1L);
        assertThat(stats.ruleDecisions().get(CleanupObjectType.LOG_SEGMENT).isConsistent())
                .isTrue();
    }
}
