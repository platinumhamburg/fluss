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

import org.apache.fluss.flink.action.orphan.audit.AuditEvent;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory.OpenContextSnapshot;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class StatsAggregateOperatorTest {

    private static final String RUN_ID = "00000000-0000-0000-0000-000000000005";

    @BeforeEach
    void resetReporterProbe() {
        TestingAuditReporterFactory.reset();
    }

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

    @Test
    void statsOpenUsesActualRuntimeIdentity() throws Exception {
        try (Harness harness = new Harness(true, reportingSpec(true))) {
            harness.open();

            assertThat(TestingAuditReporterFactory.openContexts("testing"))
                    .singleElement()
                    .satisfies(context -> assertRuntimeIdentity(context, harness));
        }
    }

    @Test
    void requiredReporterOpenFailureFailsStatsHarnessOpen() throws Exception {
        TestingAuditReporterFactory.fail("testing", "open", "injected-open-failure");
        try (Harness harness = new Harness(true, reportingSpec(true))) {
            assertThatThrownBy(harness::open)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("open")
                    .hasMessageNotContaining("injected-open-failure");
        }
    }

    @Test
    void terminalAuditOrderFlushesBeforeIntegrityFailureAndEmitsNoRecord() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 15L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT, RuleDecisionCounters.scanned(10L))
                        .build();

        try (Harness harness = new Harness(true, reportingSpec(true))) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));

            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity");

            assertThat(TestingAuditReporterFactory.events("testing"))
                    .extracting(AuditEvent::getAction)
                    .containsExactly(
                            "table_rule_summary",
                            "summary_by_rule",
                            "coverage_summary",
                            "audit_integrity",
                            "summary");
            assertThat(TestingAuditReporterFactory.calls())
                    .containsExactly(
                            "testing:validate",
                            "testing:create",
                            "testing:open",
                            "testing:report",
                            "testing:report",
                            "testing:report",
                            "testing:report",
                            "testing:report",
                            "testing:flush");
            assertThat(harness.summaries()).isEmpty();
        }
    }

    @Test
    void requiredFlushFailureFailsValidSummaryBeforeAnyOutputRecord() throws Exception {
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        Harness harness = new Harness(true, reportingSpec(true));
        try {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));

            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("flush")
                    .hasMessageNotContaining("injected-flush-failure");
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .extracting(AuditEvent::getAction)
                    .containsExactly("coverage_summary", "audit_integrity", "summary");
            assertThat(harness.summaries()).isEmpty();
        } finally {
            TestingAuditReporterFactory.reset();
            harness.close();
        }
    }

    @Test
    void integrityFailureRemainsPrimaryWhenRequiredFlushAlsoFails() throws Exception {
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 17L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT, RuleDecisionCounters.scanned(10L))
                        .build();
        Harness harness = new Harness(true, reportingSpec(true));
        try {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));

            assertThatThrownBy(harness::endInput)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .singleElement()
                                            .satisfies(
                                                    suppressed ->
                                                            assertThat(suppressed)
                                                                    .hasMessageContaining("testing")
                                                                    .hasMessageContaining("flush")
                                                                    .hasMessageNotContaining(
                                                                            "injected-flush-failure")));
            assertThat(harness.summaries()).isEmpty();
        } finally {
            TestingAuditReporterFactory.reset();
            harness.close();
        }
    }

    @Test
    void statsProcessingFailureRemainsPrimaryWhenReporterTeardownFails() throws Exception {
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        Harness harness = new Harness(true, reportingSpec(true));
        try {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));

            Throwable processingFailure =
                    catchThrowable(
                            () ->
                                    harness.processElement(
                                            new StreamRecord<>(
                                                    CleanupStats.scope(
                                                            0L, 0L, Collections.emptyMap()))));

            assertThat(processingFailure)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Duplicate scope summary");
            assertThatCode(harness::close).doesNotThrowAnyException();
            assertThat(processingFailure.getSuppressed())
                    .singleElement()
                    .satisfies(
                            lifecycleFailure -> {
                                assertThat(lifecycleFailure)
                                        .hasMessageContaining("testing")
                                        .hasMessageContaining("flush")
                                        .hasMessageNotContaining("injected-flush-failure");
                                assertThat(lifecycleFailure.getSuppressed())
                                        .singleElement()
                                        .satisfies(
                                                closeFailure ->
                                                        assertThat(closeFailure)
                                                                .hasMessageContaining("testing")
                                                                .hasMessageContaining("close")
                                                                .hasMessageNotContaining(
                                                                        "injected-close-failure"));
                            });
            assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
        } finally {
            TestingAuditReporterFactory.reset();
            harness.close();
        }
    }

    @Test
    void statsEndInputFlushesAtMostOnceBeforeClose() throws Exception {
        Harness harness = new Harness(true, reportingSpec(true));
        try {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));
            harness.endInput();
            harness.close();

            assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
        } finally {
            TestingAuditReporterFactory.reset();
            harness.close();
        }
    }

    @Test
    void statsEndInputFailureRemainsPrimaryWhenReporterCloseFails() throws Exception {
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 18L);
        CleanupStats inconsistent =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT, RuleDecisionCounters.scanned(10L))
                        .build();
        Harness harness = new Harness(true, reportingSpec(true));
        try {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(inconsistent));

            Throwable endInputFailure = catchThrowable(harness::endInput);

            assertThat(endInputFailure)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("audit integrity");
            assertThatCode(harness::close).doesNotThrowAnyException();
            assertThat(endInputFailure.getSuppressed())
                    .singleElement()
                    .satisfies(
                            closeFailure ->
                                    assertThat(closeFailure)
                                            .hasMessageContaining("testing")
                                            .hasMessageContaining("close")
                                            .hasMessageNotContaining("injected-close-failure"));
            assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
        } finally {
            TestingAuditReporterFactory.reset();
            harness.close();
        }
    }

    @Test
    void statsCloseFallbackFlushesThenClosesReporterExactlyOnce() throws Exception {
        StatsAggregateOperator operator = newStatsOperator(true, reportingSpec(true));
        Harness harness = new Harness(operator);
        try {
            harness.open();
        } finally {
            harness.close();
        }

        operator.close();

        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "testing:validate",
                        "testing:create",
                        "testing:open",
                        "testing:flush",
                        "testing:close");
    }

    @Test
    void optionalReporterFailuresDoNotChangeCleanupSummaryCounters() throws Exception {
        CleanupSummary baseline = aggregateScopeOnly(disabledReporterSpec());

        TestingAuditReporterFactory.reset();
        TestingAuditReporterFactory.fail("testing", "report", "injected-optional-report-failure");
        CleanupSummary withOptionalFailure = aggregateScopeOnly(reportingSpec(false));

        assertThat(withOptionalFailure).usingRecursiveComparison().isEqualTo(baseline);
        assertThat(TestingAuditReporterFactory.callCount("testing:report")).isEqualTo(3);
    }

    @Test
    void reporterEventsStayOutOfCleanupTaskStatsAndStreamRecords() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 16L);
        CleanupStats scan =
                CleanupStats.scanBuilder(scope)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(10L)
                                        .add(RuleDecisionCounters.candidate(10L)))
                        .build();
        try (Harness harness = new Harness(true, reportingSpec(true))) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(1L, 0L, Collections.emptyMap())));
            harness.processElement(new StreamRecord<>(scan));
            harness.endInput();

            assertThat(TestingAuditReporterFactory.events("testing"))
                    .hasSize(5)
                    .allSatisfy(
                            event ->
                                    assertThat((Object) event)
                                            .isNotInstanceOf(CleanTask.class)
                                            .isNotInstanceOf(CleanupStats.class)
                                            .isNotInstanceOf(StreamRecord.class));
            assertThat(harness.getOutput())
                    .singleElement()
                    .isInstanceOfSatisfying(
                            StreamRecord.class,
                            record ->
                                    assertThat(((StreamRecord<?>) record).getValue())
                                            .isInstanceOf(CleanupSummary.class)
                                            .isNotInstanceOf(AuditEvent.class));
        }
    }

    private static CleanupSummary aggregateScopeOnly(AuditReporterSpec reporterSpec)
            throws Exception {
        try (Harness harness = new Harness(true, reporterSpec)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));
            harness.endInput();
            return harness.summaries().get(0);
        }
    }

    private static void assertRuntimeIdentity(OpenContextSnapshot context, Harness harness) {
        assertThat(context.getRunId()).isEqualTo(RUN_ID);
        assertThat(context.isDryRun()).isTrue();
        assertThat(context.getStage()).isEqualTo(AuditStage.SUMMARY);
        assertThat(context.getOperatorName()).isEqualTo("StatsAggregate");
        assertThat(context.getSubtaskIndex()).isZero();
        assertThat(context.getAttemptNumber()).isZero();
        assertThat(context.getUserCodeClassLoader())
                .isSameAs(harness.getEnvironment().getUserCodeClassLoader().asClassLoader());
    }

    private static AuditReporterSpec disabledReporterSpec() {
        return new AuditReporterSpec(RUN_ID, Collections.<ReporterSpec>emptyList());
    }

    private static AuditReporterSpec reportingSpec(boolean required) {
        return new AuditReporterSpec(
                RUN_ID,
                Collections.singletonList(
                        new ReporterSpec(
                                "testing", required, Collections.<String, String>emptyMap())));
    }

    private static StatsAggregateOperator newStatsOperator(
            boolean dryRun, AuditReporterSpec reporterSpec) {
        try {
            Constructor<StatsAggregateOperator> constructor =
                    StatsAggregateOperator.class.getDeclaredConstructor(
                            boolean.class, AuditReporterSpec.class);
            return constructor.newInstance(dryRun, reporterSpec);
        } catch (NoSuchMethodException missingTaskFiveConstructor) {
            throw new AssertionError(
                    "Task 5 requires StatsAggregateOperator(boolean, AuditReporterSpec)",
                    missingTaskFiveConstructor);
        } catch (ReflectiveOperationException reflectionFailure) {
            throw new AssertionError(
                    "Unable to construct StatsAggregateOperator", reflectionFailure);
        }
    }

    private static final class Harness
            extends OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> {

        private Harness(boolean dryRun) throws Exception {
            this(dryRun, disabledReporterSpec());
        }

        private Harness(boolean dryRun, AuditReporterSpec reporterSpec) throws Exception {
            this(newStatsOperator(dryRun, reporterSpec));
        }

        private Harness(StatsAggregateOperator operator) throws Exception {
            super(operator, 1, 1, 0);
        }

        @SuppressWarnings("unchecked")
        private List<CleanupSummary> summaries() {
            return getRecordOutput().stream()
                    .map(record -> ((StreamRecord<CleanupSummary>) record).getValue())
                    .collect(Collectors.toList());
        }
    }
}
