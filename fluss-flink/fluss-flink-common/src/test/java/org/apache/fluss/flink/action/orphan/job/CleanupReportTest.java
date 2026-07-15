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

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CleanupReportTest {

    @Test
    void keepsTableAndObjectTypeBreakdownsWithoutLogSideAggregation() {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        CleanStats first =
                CleanStats.builder(orders).planned(CleanupObjectType.LOG_SEGMENT, 2L, 300L).build();
        CleanStats second =
                CleanStats.builder(orders)
                        .planned(CleanupObjectType.KV_SHARED_SST, 1L, 700L)
                        .build();

        CleanupReport report =
                CleanupReport.aggregate(
                        Collections.emptyList(), Arrays.asList(first, second), true);

        assertThat(report.tableSummary(orders).plannedFiles()).isEqualTo(3L);
        assertThat(report.tableSummary(orders).plannedBytes()).isEqualTo(1000L);
        assertThat(report.byObjectType().get(CleanupObjectType.KV_SHARED_SST).plannedBytes())
                .isEqualTo(700L);
        assertThat(report.global().plannedBytes()).isEqualTo(1000L);
    }

    @Test
    void rejectsActualDeletionCountersInDryRunReport() {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        CleanStats invalid =
                CleanStats.builder(orders)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 100L)
                        .deleted(CleanupObjectType.LOG_SEGMENT, 1L, 100L)
                        .build();

        assertThatThrownBy(
                        () ->
                                CleanupReport.aggregate(
                                        Collections.emptyList(),
                                        Collections.singletonList(invalid),
                                        true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dry-run report contains actual deletion counters");
    }

    @Test
    void unavailableMtimeIsActionRequiredAndKeepsPlanEmpty() {
        ScopeIdentity scope = ScopeIdentity.table("db", "orders", 7L);
        RuleDecisionCounters decisions =
                RuleDecisionCounters.scanned(256L).add(RuleDecisionCounters.mtimeUnavailable(256L));
        CleanStats stats =
                CleanStats.builder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .skipped(SkipReasonCode.MTIME_UNAVAILABLE, 2L)
                        .ruleDecision(CleanupObjectType.LOG_SEGMENT, decisions)
                        .build();

        CleanupReport report =
                CleanupReport.aggregate(
                        Collections.emptyList(), Collections.singletonList(stats), true);

        assertThat(report.global().plannedFiles()).isZero();
        assertThat(report.global().plannedDirs()).isZero();
        assertThat(report.global().deletedFiles()).isZero();
        assertThat(report.mtimeUnavailableFiles()).isEqualTo(1L);
        assertThat(report.mtimeUnavailableBytes()).isEqualTo(256L);
        assertThat(report.mtimeUnavailableDirs()).isEqualTo(1L);
        assertThat(report.coverageComplete()).isFalse();
        assertThat(report.ruleCountersConsistent()).isTrue();
    }

    @Test
    void unavailableMtimeReasonAndRuleCountersMustReconcile() {
        ScopeIdentity scope = ScopeIdentity.table("db", "orders", 7L);
        CleanStats stats =
                CleanStats.builder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(256L)
                                        .add(RuleDecisionCounters.mtimeUnavailable(256L)))
                        .build();

        CleanupReport report =
                CleanupReport.aggregate(
                        Collections.emptyList(), Collections.singletonList(stats), true);

        assertThat(report.mtimeUnavailableDirs()).isEqualTo(-1L);
        assertThat(report.ruleCountersConsistent()).isFalse();
    }
}
