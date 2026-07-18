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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory.OpenContextSnapshot;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class ScanAndCleanFunctionTest {

    private static final String RUN_ID = "00000000-0000-0000-0000-000000000005";

    @BeforeEach
    void resetReporterProbe() {
        TestingAuditReporterFactory.reset();
    }

    @Test
    void forwardsScopeSummaryWithoutOpeningFilesystemState() throws Exception {
        ScopePlanStats plan = new ScopePlanStats();
        ScopeSummaryTask marker = ScopeSummaryTask.from(plan);
        CleanupStats expected = marker.stats();
        ScanAndCleanFunction function = newScanFunction(disabledReporterSpec(), true);
        List<CleanupStats> output = new ArrayList<>();

        function.processElement(marker, null, new ListCollector(output));

        assertThat(output).containsExactly(expected);
    }

    @Test
    void scanOpenUsesActualRuntimeIdentity() throws Exception {
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        try (OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                scanHarness(function)) {
            harness.open();

            assertThat(TestingAuditReporterFactory.openContexts("testing"))
                    .singleElement()
                    .satisfies(context -> assertRuntimeIdentity(context, harness));
        }
    }

    @Test
    void requiredReporterOpenFailureFailsScanHarnessOpen() throws Exception {
        TestingAuditReporterFactory.fail("testing", "open", "injected-open-failure");
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        try (OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                scanHarness(function)) {
            assertThatThrownBy(harness::open)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("open")
                    .hasMessageNotContaining("injected-open-failure");
        }
    }

    @Test
    void noTaskAttemptFlushesThenClosesReporterExactlyOnce() throws Exception {
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);
        try {
            harness.open();
            assertThat(harness.getOutput()).isEmpty();
        } finally {
            harness.close();
        }

        function.close();

        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "testing:validate",
                        "testing:create",
                        "testing:open",
                        "testing:flush",
                        "testing:close");
        assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
        assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
    }

    @Test
    void scanCloseStillClosesAfterRequiredFlushFailure() throws Exception {
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);
        try {
            harness.open();
            TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
            TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");

            assertThatThrownBy(function::close)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("flush")
                    .hasMessageNotContaining("injected-flush-failure")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .singleElement()
                                            .satisfies(
                                                    suppressed ->
                                                            assertThat(suppressed)
                                                                    .hasMessageContaining("testing")
                                                                    .hasMessageContaining("close")
                                                                    .hasMessageNotContaining(
                                                                            "injected-close-failure")));
        } finally {
            harness.close();
        }

        assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
        assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
    }

    @Test
    void scanProcessingFailureRemainsPrimaryWhenReporterTeardownFails(@TempDir Path tempDir)
            throws Exception {
        Files.write(tempDir.resolve("unknown.bin"), new byte[] {1});
        TestingAuditReporterFactory.fail("testing", "report", "injected-report-failure");
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);

        try {
            harness.open();
            Throwable processingFailure =
                    catchThrowable(
                            () ->
                                    harness.processElement(
                                            new StreamRecord<CleanTask>(
                                                    new OrphanDirCleanTask(
                                                            ScopeIdentity.orphanTable(
                                                                    "db", "old_table-1", 1L),
                                                            tempDir.toUri().toString(),
                                                            System.currentTimeMillis() + 1L,
                                                            true,
                                                            false))));

            assertThat(processingFailure)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("report")
                    .hasMessageNotContaining("injected-report-failure");

            function.close();

            assertThat(processingFailure.getSuppressed())
                    .singleElement()
                    .satisfies(
                            suppressed -> {
                                assertThat(suppressed)
                                        .hasMessageContaining("testing")
                                        .hasMessageContaining("flush")
                                        .hasMessageNotContaining("injected-flush-failure");
                                assertThat(suppressed.getSuppressed())
                                        .singleElement()
                                        .satisfies(
                                                closeFailure ->
                                                        assertThat(closeFailure)
                                                                .hasMessageContaining("testing")
                                                                .hasMessageContaining("close")
                                                                .hasMessageNotContaining(
                                                                        "injected-close-failure"));
                            });
        } finally {
            TestingAuditReporterFactory.reset();
            try {
                function.close();
            } finally {
                harness.close();
            }
        }
    }

    @Test
    void optionalReporterOpenFailureDoesNotChangeForwardedCleanupStats() throws Exception {
        TestingAuditReporterFactory.fail("testing", "open", "injected-optional-open-failure");
        ScopeSummaryTask marker = ScopeSummaryTask.from(new ScopePlanStats());
        ScanAndCleanFunction function = newScanFunction(reportingSpec(false), true);
        try (OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                scanHarness(function)) {
            harness.open();
            harness.processElement(new StreamRecord<CleanTask>(marker));

            assertThat(harness.extractOutputValues())
                    .singleElement()
                    .usingRecursiveComparison()
                    .isEqualTo(marker.stats());
            assertThat(TestingAuditReporterFactory.events("testing")).isEmpty();
        }
    }

    private static OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> scanHarness(
            ScanAndCleanFunction function) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(function), 8, 3, 2);
    }

    private static void assertRuntimeIdentity(
            OpenContextSnapshot context,
            OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness) {
        assertThat(context.getRunId()).isEqualTo(RUN_ID);
        assertThat(context.isDryRun()).isTrue();
        assertThat(context.getStage()).isEqualTo(AuditStage.SCAN);
        assertThat(context.getOperatorName()).isEqualTo("ScanAndClean");
        assertThat(context.getSubtaskIndex()).isEqualTo(2);
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

    private static ScanAndCleanFunction newScanFunction(
            AuditReporterSpec reporterSpec, boolean dryRun) {
        try {
            Constructor<ScanAndCleanFunction> constructor =
                    ScanAndCleanFunction.class.getDeclaredConstructor(
                            long.class, Map.class, AuditReporterSpec.class, boolean.class);
            return constructor.newInstance(
                    100L, Collections.<String, String>emptyMap(), reporterSpec, dryRun);
        } catch (NoSuchMethodException missingTaskFiveConstructor) {
            throw new AssertionError(
                    "Task 5 requires ScanAndCleanFunction(long, Map, AuditReporterSpec, boolean)",
                    missingTaskFiveConstructor);
        } catch (ReflectiveOperationException reflectionFailure) {
            throw new AssertionError("Unable to construct ScanAndCleanFunction", reflectionFailure);
        }
    }

    private static final class ListCollector implements Collector<CleanupStats> {
        private final List<CleanupStats> output;

        private ListCollector(List<CleanupStats> output) {
            this.output = output;
        }

        @Override
        public void collect(CleanupStats record) {
            output.add(record);
        }

        @Override
        public void close() {}
    }
}
