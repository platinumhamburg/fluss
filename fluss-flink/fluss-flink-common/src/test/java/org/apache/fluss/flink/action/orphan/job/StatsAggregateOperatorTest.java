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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StatsAggregateOperatorTest {

    @Test
    void scopeOnlyInputStillProducesOneCompleteSummary() throws Exception {
        try (Harness harness = new Harness(true)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));
            harness.endInput();

            assertThat(harness.summaries()).hasSize(1);
            CleanupSummary summary = harness.summaries().get(0);
            assertThat(summary.dryRun()).isTrue();
            assertThat(summary.tasksPlanned()).isZero();
            assertThat(summary.coverageComplete()).isTrue();
            assertThat(summary.ruleCountersConsistent()).isTrue();
            assertThat(summary.dryRunCountersConsistent()).isTrue();
        }
    }

    @Test
    void aggregatesScopeAndScanStatsIntoOneSummary() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 12L);
        CleanupStats scan =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 23L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(23L)
                                        .add(RuleDecisionCounters.candidate(23L)))
                        .build();
        try (Harness harness = new Harness(true)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(scan));
            harness.endInput();

            assertThat(harness.summaries())
                    .singleElement()
                    .satisfies(
                            summary -> {
                                assertThat(summary.globalCounters().plannedFiles()).isEqualTo(1L);
                                assertThat(summary.ruleCandidateFiles()).isEqualTo(1L);
                                assertThat(summary.ruleCandidateBytes()).isEqualTo(23L);
                                assertThat(summary.coverageComplete()).isTrue();
                            });
        }
    }

    @Test
    void rejectsMissingOrDuplicateScopeSummary() throws Exception {
        try (Harness missing = new Harness(true)) {
            missing.open();
            assertThatThrownBy(missing::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("scope summary");
        }

        CleanupStats marker = CleanupStats.scope(0L, 0L, Collections.emptyMap());
        try (Harness duplicate = new Harness(true)) {
            duplicate.open();
            duplicate.processElement(new StreamRecord<>(marker));
            assertThatThrownBy(() -> duplicate.processElement(new StreamRecord<>(marker)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Duplicate scope summary");
        }
    }

    @Test
    void inconsistentRuleCountersFailClosed() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 13L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT, RuleDecisionCounters.scanned(10L))
                        .build();

        try (Harness harness = new Harness(true)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));
            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity");
            assertThat(harness.summaries()).isEmpty();
        }
    }

    @Test
    void dryRunActualCountersFailClosed() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 13L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .deleted(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT, RuleDecisionCounters.scanned(10L))
                        .build();

        try (Harness harness = new Harness(true)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));
            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity");
            assertThat(harness.summaries()).isEmpty();
        }
    }

    @Test
    void missingMtimeSkipCounterFailsClosed() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 14L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(10L)
                                        .add(RuleDecisionCounters.mtimeUnavailable(10L)))
                        .build();

        try (Harness harness = new Harness(true)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));
            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity");
            assertThat(harness.summaries()).isEmpty();
        }
    }

    @Test
    void cleanupSummaryContainsNoDetailMaps() {
        assertThat(CleanupSummary.class.getDeclaredFields())
                .extracting(Field::getType)
                .noneMatch(Map.class::isAssignableFrom);
    }

    private static final class Harness
            extends OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> {

        private Harness(boolean dryRun) throws Exception {
            super(new StatsAggregateOperator(dryRun), 1, 1, 0);
        }

        @SuppressWarnings("unchecked")
        private List<CleanupSummary> summaries() {
            return getRecordOutput().stream()
                    .map(record -> ((StreamRecord<CleanupSummary>) record).getValue())
                    .collect(Collectors.toList());
        }
    }
}
