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
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory.OpenContextSnapshot;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.FlussPaths;

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
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    void splitsRemoteFsRateEvenlyAcrossScanSubtasks() {
        assertThat(ScanAndCleanFunction.perSubtaskRate(100L, 16)).isEqualTo(6.25d);
    }

    @Test
    void keepsJobLimitWhenParallelismExceedsRate() {
        double perSubtask = ScanAndCleanFunction.perSubtaskRate(3L, 8);

        assertThat(perSubtask).isEqualTo(0.375d);
        assertThat(perSubtask * 8).isEqualTo(3.0d);
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
    void emitsOneBoundedLifecyclePairPerScanSubtaskAttempt() throws Exception {
        ScopeIdentity firstScope =
                ScopeIdentity.table("db", "first", 1L).withPartitionAndBucket(null, 0);
        ScopeIdentity secondScope =
                ScopeIdentity.table("db", "second", 2L).withPartitionAndBucket(null, 1);
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        try (OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                scanHarness(function)) {
            harness.open();
            harness.processElement(new StreamRecord<CleanTask>(emptyBucketTask(firstScope)));
            harness.processElement(
                    new StreamRecord<CleanTask>(ScopeSummaryTask.from(new ScopePlanStats())));
            harness.processElement(new StreamRecord<CleanTask>(emptyBucketTask(secondScope)));
        }

        assertThat(TestingAuditReporterFactory.events("testing"))
                .filteredOn(event -> event.getAction().equals("scan_start"))
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.getDimensions())
                                    .containsEntry("parallelism", "3")
                                    .containsEntry(
                                            "assigned_remote_fs_rate",
                                            Double.toString(100.0d / 3.0d))
                                    .containsEntry("scan_parallelism", "3")
                                    .containsEntry(
                                            "effective_remote_fs_rate_limit_per_second",
                                            Double.toString(100.0d / 3.0d));
                            assertThat(event.getMetrics())
                                    .containsEntry("remote_fs_rate_limit", 100L);
                        });
        assertThat(TestingAuditReporterFactory.events("testing"))
                .filteredOn(event -> event.getAction().equals("scan_subtask_summary"))
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.getMetrics())
                                    .containsEntry("tasks_completed", 2L)
                                    .containsKey("elapsed_ms")
                                    .containsEntry("scanned_files", 0L)
                                    .containsEntry("scanned_bytes", 0L)
                                    .containsEntry("files_per_second_millis", 0L)
                                    .containsEntry("planned_files", 0L)
                                    .containsEntry("planned_dirs", 0L)
                                    .containsEntry("planned_bytes", 0L)
                                    .containsEntry("deleted_files", 0L)
                                    .containsEntry("empty_dirs_removed", 0L)
                                    .containsEntry("delete_failures", 0L)
                                    .containsEntry("bytes_reclaimed", 0L);
                            assertThat(event.getDimensions())
                                    .containsEntry("files_per_second", "0.000");
                            assertThat(event.getPath()).isNull();
                        });
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
    void scanStartReportFailureOwnsRuntimeCleanup() throws Exception {
        TestingAuditReporterFactory.fail("testing", "report", "injected-report-failure");
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);

        try {
            Throwable openFailure = catchThrowable(harness::open);
            Throwable hardCloseFailure = catchThrowable(harness::close);

            assertThat(openFailure)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("report")
                    .hasMessageNotContaining("injected-report-failure")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .singleElement()
                                            .satisfies(
                                                    cleanupFailure -> {
                                                        assertThat(cleanupFailure)
                                                                .hasMessageContaining("testing")
                                                                .hasMessageContaining("flush")
                                                                .hasMessageNotContaining(
                                                                        "injected-flush-failure");
                                                        assertThat(cleanupFailure.getSuppressed())
                                                                .singleElement()
                                                                .satisfies(
                                                                        closeFailure ->
                                                                                assertThat(
                                                                                                closeFailure)
                                                                                        .hasMessageContaining(
                                                                                                "testing")
                                                                                        .hasMessageContaining(
                                                                                                "close")
                                                                                        .hasMessageNotContaining(
                                                                                                "injected-close-failure"));
                                                    }));
            assertThat(hardCloseFailure).isNull();
            assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .extracting(event -> event.getAction())
                    .containsExactly("scan_start");
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
                        "testing:report",
                        "testing:report",
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
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);

        try {
            harness.open();
            TestingAuditReporterFactory.fail("testing", "report", "injected-report-failure");
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
                                        .hasMessageContaining("report")
                                        .hasMessageNotContaining("injected-report-failure");
                                assertThat(suppressed.getSuppressed())
                                        .hasSize(2)
                                        .anySatisfy(
                                                flushFailure ->
                                                        assertThat(flushFailure)
                                                                .hasMessageContaining("testing")
                                                                .hasMessageContaining("flush")
                                                                .hasMessageNotContaining(
                                                                        "injected-flush-failure"))
                                        .anySatisfy(
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
    void terminalSummaryRetainsPartialProgressWhenDeleteAuditFails(@TempDir Path tempDir)
            throws Exception {
        long cutoff = System.currentTimeMillis() + 10_000L;
        Path deletedFile = createLogFile(tempDir, 1, ".log", cutoff - 1_000L);
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 42L).withPartitionAndBucket(7L, 3);
        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), false);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness = scanHarness(function);

        try {
            harness.open();
            TestingAuditReporterFactory.fail("testing", "report", "injected-report-failure");

            Throwable processingFailure =
                    catchThrowable(
                            () ->
                                    harness.processElement(
                                            new StreamRecord<CleanTask>(
                                                    new BucketCleanTask(
                                                            scope,
                                                            tempDir.toUri().toString(),
                                                            null,
                                                            Collections.<String>emptySet(),
                                                            Collections.<String>emptySet(),
                                                            Collections.<String>emptySet(),
                                                            cutoff,
                                                            false,
                                                            false))));

            assertThat(processingFailure)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("report");
            assertThat(deletedFile).doesNotExist();
            assertThat(harness.extractOutputValues()).isEmpty();

            function.close();

            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("scan_subtask_summary"))
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.getMetrics())
                                            .containsEntry("tasks_completed", 0L)
                                            .containsEntry("scanned_files", 1L)
                                            .containsEntry("scanned_bytes", 1L)
                                            .containsKey("files_per_second_millis")
                                            .containsEntry("planned_files", 1L)
                                            .containsEntry("planned_dirs", 0L)
                                            .containsEntry("planned_bytes", 1L)
                                            .containsEntry("deleted_files", 1L)
                                            .containsEntry("empty_dirs_removed", 0L)
                                            .containsEntry("delete_failures", 0L)
                                            .containsEntry("bytes_reclaimed", 1L));
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("scan_subtask_summary"))
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event.getDimensions().get("files_per_second"))
                                            .matches("[0-9]+\\.[0-9]{3}"));
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

    @Test
    void reportsBoundedDecisionEvidenceAndExhaustiveDryRunCandidate(@TempDir Path tempDir)
            throws Exception {
        long cutoff = System.currentTimeMillis() - 10_000L;
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 42L).withPartitionAndBucket(7L, 3);
        Set<String> activePaths = new HashSet<>();

        Path activeFile = createLogFile(tempDir, 1, ".log", cutoff - 1_000L);
        activePaths.add(relativeLogPath(activeFile));
        Path candidateFile = createLogFile(tempDir, 2, ".index", cutoff - 2_000L);
        Path unknownFile = Files.write(tempDir.resolve("unknown.bin"), new byte[] {1});
        Files.setLastModifiedTime(unknownFile, FileTime.fromMillis(cutoff - 3_000L));
        for (int i = 3; i < 7; i++) {
            createLogFile(tempDir, i, ".timeindex", cutoff + i);
        }

        ScanAndCleanFunction function = newScanFunction(reportingSpec(true), true);
        try (OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                scanHarness(function)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<CleanTask>(
                            new BucketCleanTask(
                                    scope,
                                    tempDir.toUri().toString(),
                                    null,
                                    activePaths,
                                    Collections.<String>emptySet(),
                                    Collections.<String>emptySet(),
                                    Collections.<String>emptySet(),
                                    true,
                                    cutoff,
                                    true,
                                    false)));
        }

        List<AuditEvent> events = TestingAuditReporterFactory.events("testing");
        assertThat(events)
                .filteredOn(event -> event.getAction().equals("decision_sample"))
                .anySatisfy(
                        event -> {
                            assertThat(event.getPath()).isEqualTo(auditPath(activeFile));
                            assertThat(event.getReasonCode()).isEqualTo("keep_active");
                            assertThat(event.getDimensions())
                                    .containsEntry("reference_type", "log_segment")
                                    .containsEntry("reference_match_kind", "relative_path")
                                    .containsEntry("reference_key", relativeLogPath(activeFile));
                        })
                .anySatisfy(
                        event -> {
                            assertThat(event.getPath()).isEqualTo(auditPath(unknownFile));
                            assertThat(event.getReasonCode()).isEqualTo("unknown_file_type");
                            assertThat(event.getSizeBytes()).isEqualTo(1L);
                        });
        assertThat(events)
                .filteredOn(
                        event ->
                                event.getAction().equals("decision_sample")
                                        && "newer_than_cutoff".equals(event.getReasonCode()))
                .hasSize(3);
        assertThat(events)
                .filteredOn(event -> event.getAction().equals("diagnostic_sampling_summary"))
                .singleElement()
                .satisfies(
                        event ->
                                assertThat(event.getMetrics())
                                        .containsEntry("total_count", 4L)
                                        .containsEntry("emitted_samples", 3L)
                                        .containsEntry("suppressed_samples", 1L));
        assertThat(events)
                .filteredOn(event -> event.getAction().equals("would_delete"))
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.getPath()).isEqualTo(auditPath(candidateFile));
                            assertThat(event.getMetrics())
                                    .containsEntry("cutoff_ms", cutoff)
                                    .containsEntry("mtime_minus_cutoff_ms", -2_000L);
                            assertThat(event.getFlags()).containsEntry("dry_run", true);
                        });
        assertThat(events)
                .filteredOn(event -> event.getAction().equals("scan_subtask_summary"))
                .singleElement()
                .satisfies(
                        event ->
                                assertThat(event.getMetrics())
                                        .containsEntry("tasks_completed", 1L)
                                        .containsEntry("scanned_files", 7L)
                                        .containsEntry("scanned_bytes", 7L)
                                        .containsKey("files_per_second_millis")
                                        .containsEntry("planned_files", 1L)
                                        .containsEntry("planned_bytes", 1L));
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

    private static BucketCleanTask emptyBucketTask(ScopeIdentity scope) {
        return new BucketCleanTask(
                scope,
                null,
                null,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                0L,
                true,
                false);
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

    private static Path createLogFile(
            Path root, int directoryNumber, String suffix, long modificationTime) throws Exception {
        String segment = String.format("00000000-0000-0000-0000-%012d", directoryNumber);
        Path dir = Files.createDirectories(root.resolve(segment));
        Path file =
                Files.write(
                        dir.resolve(FlussPaths.filenamePrefixFromOffset(0L) + suffix),
                        new byte[] {1});
        Files.setLastModifiedTime(file, FileTime.fromMillis(modificationTime));
        return file;
    }

    private static String relativeLogPath(Path file) {
        return file.getParent().getFileName() + "/" + file.getFileName();
    }

    private static String auditPath(Path file) {
        return new FsPath(file.toUri().toString()).toString();
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
