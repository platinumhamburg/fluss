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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.job.CleanupStats;
import org.apache.fluss.flink.action.orphan.job.CleanupSummary;
import org.apache.fluss.flink.action.orphan.job.RuleDecisionCounters;
import org.apache.fluss.flink.action.orphan.job.ScopePlanStats;
import org.apache.fluss.flink.action.orphan.job.StatsAggregateOperator;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.fs.FsPath;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.fail;

/** Tests the same-source legacy text and structured-event audit facade. */
public class AuditLoggerTest {

    private static final String RUN_ID = "3b5939f1-9837-49d8-8a02-945273a0d7e2";
    private static final long EVENT_TIME_MILLIS = 1_700_000_000_123L;
    private static final String EVENT_TIME = Instant.ofEpochMilli(EVENT_TIME_MILLIS).toString();
    private static final String OPERATOR = "ScanAndClean";
    private static final int SUBTASK = 3;
    private static final int ATTEMPT = 2;
    private static final FsPath PATH = new FsPath("file:///warehouse/db/orders/file.sst");
    private static final FsPath DIR = new FsPath("file:///warehouse/db/orders/orphan-dir");
    private static final ScopeIdentity SCOPE =
            ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 4);
    private static final String PROVIDER_RESOURCE =
            "META-INF/services/" + AuditReporterFactory.class.getName();

    @TempDir Path tempDir;

    private final List<URLClassLoader> providerLoaders = new ArrayList<>();
    private final AtomicInteger providerLoaderSequence = new AtomicInteger();

    @AfterEach
    void resetReporter() throws IOException {
        LocalAuditReporterFactory.reset();
        for (URLClassLoader loader : providerLoaders) {
            loader.close();
        }
        providerLoaders.clear();
    }

    @Test
    void everyPublicLogOverloadPreservesTextAndReportsOneSameSourceEvent() throws Exception {
        LocalAuditReporterFactory.reset();
        AuditReporterContext context = reporterContext();
        AuditReporterRuntime runtime = openTestingRuntime(context);
        CountingClock clock = new CountingClock(EVENT_TIME_MILLIS);
        CountingEventIds eventIds = new CountingEventIds();
        AuditLogger logger = structuredLogger(runtime, context, clock, eventIds);
        List<LogCase> cases = allPublicLogOverloads();
        List<String> logs = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(logs)) {
            for (int i = 0; i < cases.size(); i++) {
                LogCase testCase = cases.get(i);
                int logCount = logs.size();
                int eventCount = LocalAuditReporterFactory.events().size();

                testCase.invocation.run(logger);

                assertThat(logs).as(testCase.name + " text count").hasSize(logCount + 1);
                List<AuditEvent> reported = LocalAuditReporterFactory.events();
                assertThat(reported).as(testCase.name + " event count").hasSize(eventCount + 1);
                assertParity(testCase, logs.get(logCount), reported.get(eventCount), i + 1);
            }
        } finally {
            runtime.close();
        }

        assertThat(cases).hasSize(33);
        assertThat(clock.calls()).isEqualTo(cases.size());
        assertThat(eventIds.calls()).isEqualTo(cases.size());
        assertThat(logs)
                .noneMatch(
                        line ->
                                line.contains("action=keep_active")
                                        || line.contains("action=newer_than_cutoff"));
        assertThat(LocalAuditReporterFactory.events())
                .noneMatch(
                        event ->
                                event.getAction().equals("keep_active")
                                        || event.getAction().equals("newer_than_cutoff"));
    }

    @Test
    void statsAggregateOperatorEmitsBoundedAuditSummaries() throws Exception {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        CleanupStats stats =
                CleanupStats.scanBuilder(orders)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 2L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(10L)
                                        .add(RuleDecisionCounters.scanned(5L))
                                        .add(RuleDecisionCounters.candidate(10L))
                                        .add(RuleDecisionCounters.keepActive(5L)))
                        .skipped(SkipReasonCode.KEEP_ACTIVE, 1L)
                        .build();
        List<String> logs = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(logs);
                OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StatsAggregateOperator(true), 1, 1, 0)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(
                            CleanupStats.scope(
                                    1L,
                                    1L,
                                    Collections.singletonMap(SkipReasonCode.RPC_ERROR, 1L))));
            harness.processElement(new StreamRecord<>(stats));
            harness.endInput();
        }

        assertThat(logs)
                .anyMatch(
                        log ->
                                log.contains("action=table_rule_summary")
                                        && log.contains("database=db")
                                        && log.contains("table=orders")
                                        && log.contains("object_type=log_segment")
                                        && log.contains("keep_active_files=1")
                                        && log.contains("candidate_files=1"));
        assertThat(logs)
                .anyMatch(
                        log ->
                                log.contains("action=coverage_summary")
                                        && log.contains("metadata_read_failed_targets=1")
                                        && log.contains("rpc_failed_targets=1")
                                        && log.contains("complete=false"));
        assertThat(logs)
                .anyMatch(
                        log ->
                                log.contains("action=audit_integrity")
                                        && log.contains("rule_counters_consistent=true")
                                        && log.contains("coverage_complete=false")
                                        && log.contains("dry_run_counters_consistent=true")
                                        && log.contains("inconsistent_object_types=0")
                                        && log.contains("inconsistent_scopes=0"));
        assertThat(logs)
                .noneMatch(
                        log ->
                                log.contains("action=scan_heartbeat")
                                        || log.contains("action=scan_progress")
                                        || log.contains("action=keep_active")
                                        || log.contains("action=newer_than_cutoff"));
    }

    @Test
    void mapsSummaryCoverageAndIntegrityToApprovedMetricAndFlagNames() throws Exception {
        LocalAuditReporterFactory.reset();
        AuditReporterContext context = reporterContext();
        AuditReporterRuntime runtime = openTestingRuntime(context);
        AuditLogger logger =
                structuredLogger(
                        runtime,
                        context,
                        new CountingClock(EVENT_TIME_MILLIS),
                        new CountingEventIds());
        RuleDecisionCounters counters = ruleCounters();
        Map<SkipReasonCode, Long> skipped = skippedCounts();

        try (AuditCapture ignored = new AuditCapture(new CopyOnWriteArrayList<String>())) {
            logger.logTableRuleSummary(SCOPE, CleanupObjectType.LOG_SEGMENT, counters, true);
            logger.logGlobalRuleSummary(CleanupObjectType.LOG_SEGMENT, counters, true);
            logger.logCoverageSummary(skipped, 3L, 4L, 5L, 6L, false, true);
            logger.logAuditIntegrity(cleanupSummary());
            logger.logSummary(10L, 2L, 1L, 3L, 20L, true);
        } finally {
            runtime.close();
        }

        List<AuditEvent> events = LocalAuditReporterFactory.events();
        assertThat(events).hasSize(5);
        assertThat(events.get(0).getMetrics())
                .containsExactly(
                        entry("scanned_files", 6L),
                        entry("scanned_bytes", 21L),
                        entry("keep_active_files", 1L),
                        entry("keep_active_bytes", 2L),
                        entry("newer_than_cutoff_files", 1L),
                        entry("newer_than_cutoff_bytes", 3L),
                        entry("mtime_unavailable_files", 1L),
                        entry("mtime_unavailable_bytes", 4L),
                        entry("unknown_file_type_files", 1L),
                        entry("unknown_file_type_bytes", 5L),
                        entry("candidate_files", 2L),
                        entry("candidate_bytes", 7L));
        assertThat(events.get(0).getFlags()).containsExactly(entry("dry_run", true));
        assertThat(events.get(1).getMetrics()).isEqualTo(events.get(0).getMetrics());
        assertThat(events.get(1).getFlags()).isEqualTo(events.get(0).getFlags());
        assertThat(events.get(2).getMetrics())
                .containsExactly(
                        entry("no_remote_manifest_targets", 1L),
                        entry("empty_active_set_targets", 2L),
                        entry("metadata_read_failed_targets", 3L),
                        entry("directory_list_failed_targets", 7L),
                        entry("rpc_failed_targets", 8L),
                        entry("mtime_unavailable_files", 4L),
                        entry("mtime_unavailable_bytes", 5L),
                        entry("mtime_unavailable_dirs", 6L));
        assertThat(events.get(2).getFlags())
                .containsExactly(
                        entry("complete", false),
                        entry("action_required", true),
                        entry("dry_run", true));
        assertThat(events.get(3).getMetrics())
                .containsExactly(
                        entry("inconsistent_object_types", 0L), entry("inconsistent_scopes", 0L));
        assertThat(events.get(3).getFlags())
                .containsExactly(
                        entry("rule_counters_consistent", true),
                        entry("coverage_complete", true),
                        entry("dry_run_counters_consistent", true),
                        entry("dry_run", true));
        assertThat(events.get(4).getMetrics())
                .containsExactly(
                        entry("scanned", 10L),
                        entry("deleted_total", 3L),
                        entry("deleted_files", 2L),
                        entry("empty_dirs_removed", 1L),
                        entry("delete_failures", 3L),
                        entry("bytes_reclaimed", 20L));
        assertThat(events.get(4).getFlags()).containsExactly(entry("dry_run", true));
    }

    @Test
    void reporterFailurePropagatesOnlyAfterLegacyTextWasEmittedOnce() throws Exception {
        LocalAuditReporterFactory.reset();
        LocalAuditReporterFactory.failReport("provider-secret");
        AuditReporterContext context = reporterContext();
        AuditReporterRuntime runtime = openTestingRuntime(context);
        AuditLogger logger =
                structuredLogger(
                        runtime,
                        context,
                        new CountingClock(EVENT_TIME_MILLIS),
                        new CountingEventIds());
        List<String> logs = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(logs)) {
            assertThatThrownBy(() -> logger.logSkipDb("db", "rpc_error"))
                    .isInstanceOf(AuditReportingException.class)
                    .hasMessage("Audit reporter 'logger-testing' failed during report")
                    .hasMessageNotContaining("provider-secret");
        } finally {
            runtime.close();
        }

        assertThat(logs).hasSize(1);
        assertThat(logs.get(0))
                .isEqualTo(
                        "WARN action=skip_db reason=rpc_error db=db ts="
                                + EVENT_TIME
                                + identitySuffix(eventId(1)));
        assertThat(LocalAuditReporterFactory.reportCalls()).isEqualTo(1);
        assertThat(LocalAuditReporterFactory.events()).hasSize(1);
    }

    @Test
    void mtimeUnavailableRemainsOneSanitizedSamplePerLoggerAndCapsAt128Characters()
            throws Exception {
        LocalAuditReporterFactory.reset();
        AuditReporterContext context = reporterContext();
        AuditReporterRuntime runtime = openTestingRuntime(context);
        CountingClock clock = new CountingClock(EVENT_TIME_MILLIS);
        CountingEventIds eventIds = new CountingEventIds();
        AuditLogger logger = structuredLogger(runtime, context, clock, eventIds);
        String longSuffix = String.join("", Collections.nCopies(140, "a"));
        List<String> logs = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(logs)) {
            logger.logMtimeUnavailableOnce(
                    SCOPE, CleanupObjectType.LOG_SEGMENT, "file", "bad path/" + longSuffix);
            logger.logMtimeUnavailableOnce(
                    SCOPE, CleanupObjectType.DIRECTORY, "directory", "second-dir");
        } finally {
            runtime.close();
        }

        assertThat(logs).hasSize(1);
        assertThat(LocalAuditReporterFactory.events()).hasSize(1);
        assertThat(clock.calls()).isEqualTo(1);
        assertThat(eventIds.calls()).isEqualTo(1);
        String sampleName = fieldValue(logs.get(0), "sample_name");
        assertThat(sampleName).hasSize(128).matches("[A-Za-z0-9._-]+").startsWith("bad_path_");
        assertThat(LocalAuditReporterFactory.events().get(0).getDimensions())
                .containsEntry("sample_name", sampleName);
    }

    @Test
    void noArgConstructorRemainsLogOnly() {
        List<String> logs = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();

        try (AuditCapture ignored = new AuditCapture(logs)) {
            logger.logWouldDelete(PATH, RuleId.KV_SHARED_SST);
        }

        assertThat(logs).hasSize(1);
        assertThat(logs.get(0))
                .contains("action=would_delete rule=kv-shared-sst path=" + PATH)
                .contains(" run_id=")
                .contains(" event_id=")
                .endsWith(" operator=none subtask=none attempt=none");
        assertThat(LocalAuditReporterFactory.events()).isEmpty();
    }

    @Test
    void exposesProductionConstructorAndKeepsDeterministicInjectionPackagePrivate() {
        try {
            Constructor<AuditLogger> production =
                    AuditLogger.class.getDeclaredConstructor(
                            AuditReporterRuntime.class, AuditReporterContext.class);
            Constructor<AuditLogger> deterministic =
                    AuditLogger.class.getDeclaredConstructor(
                            AuditReporterRuntime.class,
                            AuditReporterContext.class,
                            LongSupplier.class,
                            Supplier.class);

            assertThat(Modifier.isPublic(production.getModifiers())).isTrue();
            assertThat(Modifier.isPublic(deterministic.getModifiers())).isFalse();
            assertThat(Modifier.isProtected(deterministic.getModifiers())).isFalse();
            assertThat(Modifier.isPrivate(deterministic.getModifiers())).isFalse();
        } catch (ReflectiveOperationException e) {
            fail("AuditLogger production or deterministic constructor is missing", e);
        }
    }

    private static void assertParity(
            LogCase testCase, String log, AuditEvent event, int eventIndex) {
        String expectedEventId = eventId(eventIndex);
        assertThat(log)
                .as(testCase.name)
                .isEqualTo(
                        testCase.severity
                                + " "
                                + testCase.legacyText
                                + identitySuffix(expectedEventId));
        assertThat(event.getSeverity()).isEqualTo(testCase.severity);
        assertThat(event.getStage()).isEqualTo(testCase.stage);
        assertThat(event.getAction()).isEqualTo(testCase.action);
        assertThat(event.getEventTimeMillis()).isEqualTo(EVENT_TIME_MILLIS);
        assertThat(Instant.parse(fieldValue(log, "ts")).toEpochMilli())
                .isEqualTo(event.getEventTimeMillis());
        assertThat(event.getRunId()).isEqualTo(RUN_ID).isEqualTo(fieldValue(log, "run_id"));
        assertThat(event.getEventId())
                .isEqualTo(expectedEventId)
                .isEqualTo(fieldValue(log, "event_id"));
        assertThat(event.getOperatorName()).isEqualTo(OPERATOR);
        assertThat(event.getSubtaskIndex()).isEqualTo(SUBTASK);
        assertThat(event.getAttemptNumber()).isEqualTo(ATTEMPT);
        assertThat(fieldValue(log, "operator")).isEqualTo(OPERATOR);
        assertThat(fieldValue(log, "subtask")).isEqualTo(Integer.toString(SUBTASK));
        assertThat(fieldValue(log, "attempt")).isEqualTo(Integer.toString(ATTEMPT));
        assertPayload(testCase, event);
        assertStableEnvelopeIsNotDuplicated(event);
    }

    private static void assertPayload(LogCase testCase, AuditEvent event) {
        ExpectedPayload expected = testCase.expectedPayload;
        assertThat(event.getSchemaVersion()).as(testCase.name).isEqualTo(AuditEvent.SCHEMA_VERSION);
        assertThat(
                        new Object[] {
                            event.getDatabase(),
                            event.getTable(),
                            event.getTableId(),
                            event.getPartitionId(),
                            event.getBucketId(),
                            event.getScopeKind(),
                            event.getObjectType(),
                            event.getPath(),
                            event.getSizeBytes(),
                            event.getMtimeMs(),
                            event.getRule(),
                            event.getReasonCode(),
                            event.getResult()
                        })
                .as(testCase.name + " stable envelope")
                .containsExactly(expected.envelope.toArray());
        assertThat(event.getDimensions())
                .as(testCase.name + " dimensions")
                .isEqualTo(expected.dimensions);
        assertThat(event.getMetrics()).as(testCase.name + " metrics").isEqualTo(expected.metrics);
        assertThat(event.getFlags()).as(testCase.name + " flags").isEqualTo(expected.flags);
    }

    private static void assertStableEnvelopeIsNotDuplicated(AuditEvent event) {
        List<String> envelopeKeys =
                Arrays.asList(
                        "schema_version",
                        "event_id",
                        "run_id",
                        "event_time",
                        "severity",
                        "stage",
                        "action",
                        "operator",
                        "subtask",
                        "attempt",
                        "database",
                        "table",
                        "table_id",
                        "partition_id",
                        "bucket_id",
                        "scope",
                        "object_type",
                        "path",
                        "size_bytes",
                        "mtime_ms",
                        "rule",
                        "reason_code",
                        "result");
        for (String key : envelopeKeys) {
            assertThat(event.getDimensions()).doesNotContainKey(key);
            assertThat(event.getMetrics()).doesNotContainKey(key);
            assertThat(event.getFlags()).doesNotContainKey(key);
        }
    }

    private static List<LogCase> allPublicLogOverloads() throws Exception {
        long cutoff = 1_600_000_000_000L;
        String cutoffIso =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.ofEpochMilli(cutoff));
        OrphanCleanConfig config = cleanupConfig();
        ScopePlanStats plan = scopePlan();
        FileMeta file = new FileMeta(PATH, 12L, 13L);
        RuleDecisionCounters counters = ruleCounters();
        Map<SkipReasonCode, Long> skipped = skippedCounts();
        CleanupSummary summary = cleanupSummary();
        List<LogCase> cases = new ArrayList<>();

        cases.add(
                logCase(
                        "logCutoff",
                        AuditSeverity.INFO,
                        AuditStage.RUN,
                        "cutoff",
                        "action=cutoff older_than_iso="
                                + cutoffIso
                                + " older_than_ms="
                                + cutoff
                                + " ts="
                                + EVENT_TIME,
                        payload()
                                .dimension("older_than_iso", cutoffIso)
                                .metric("older_than_ms", cutoff),
                        logger -> logger.logCutoff(cutoff)));
        cases.add(
                logCase(
                        "logRunStart",
                        AuditSeverity.INFO,
                        AuditStage.RUN,
                        "run_start",
                        "action=run_start scope=db.orders older_than_ms="
                                + config.olderThanMillis()
                                + " dry_run=true parallelism=3 remote_fs_rate_limit=17 allow_delete_manifest=true"
                                + " allow_clean_orphan_tables=true allow_clean_orphan_partitions=true ts="
                                + EVENT_TIME,
                        payload()
                                .scopeKind("db.orders")
                                .dimension("parallelism", "3")
                                .metric("older_than_ms", config.olderThanMillis())
                                .metric(
                                        "remote_fs_rate_limit",
                                        config.remoteFsOpRateLimitPerSecond())
                                .flag("dry_run", true)
                                .flag("allow_delete_manifest", true)
                                .flag("allow_clean_orphan_tables", true)
                                .flag("allow_clean_orphan_partitions", true),
                        logger -> logger.logRunStart(config)));
        cases.add(
                logCase(
                        "logScopePlan",
                        AuditSeverity.INFO,
                        AuditStage.SCOPE,
                        "scope_plan",
                        "action=scope_plan databases=1 tables=1 partitions=1 discovered_buckets=1 bucket_tasks=1"
                                + " orphan_dir_tasks=1 skipped_no_remote_manifest=1 skipped_empty_kv_active_set=1"
                                + " skipped_out_of_scope_root=1 metadata_failures=1 ts="
                                + EVENT_TIME,
                        payload()
                                .metric("databases", 1L)
                                .metric("tables", 1L)
                                .metric("partitions", 1L)
                                .metric("discovered_buckets", 1L)
                                .metric("bucket_tasks", 1L)
                                .metric("orphan_dir_tasks", 1L)
                                .metric("skipped_no_remote_manifest", 1L)
                                .metric("skipped_empty_kv_active_set", 1L)
                                .metric("skipped_out_of_scope_root", 1L)
                                .metric("metadata_failures", 1L),
                        logger -> logger.logScopePlan(plan)));
        cases.add(
                logCase(
                        "logDeleted(path)",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "deleted",
                        "action=deleted rule=kv-shared-sst path="
                                + PATH
                                + " ok=true ts="
                                + EVENT_TIME,
                        payload().path(PATH.toString()).rule("kv-shared-sst").flag("ok", true),
                        logger -> logger.logDeleted(PATH, RuleId.KV_SHARED_SST, true)));
        cases.add(
                logCase(
                        "logWouldDelete(path)",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "would_delete",
                        "action=would_delete rule=kv-shared-sst path=" + PATH + " ts=" + EVENT_TIME,
                        payload().path(PATH.toString()).rule("kv-shared-sst"),
                        logger -> logger.logWouldDelete(PATH, RuleId.KV_SHARED_SST)));
        cases.add(
                logCase(
                        "logWouldDelete(file)",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "would_delete",
                        objectText(
                                "would_delete", "older_than_cutoff", "planned", true, false, false),
                        objectPayload("older_than_cutoff", "planned", true, false, false),
                        logger -> logger.logWouldDelete(file, RuleId.KV_SHARED_SST, SCOPE)));
        cases.add(
                logCase(
                        "logDeleted(file)",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "deleted",
                        objectText("deleted", "older_than_cutoff", "success", false, false, false),
                        objectPayload("older_than_cutoff", "success", false, false, false),
                        logger -> logger.logDeleted(file, RuleId.KV_SHARED_SST, SCOPE)));
        cases.add(
                logCase(
                        "logDeleteFailed",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "delete_failed",
                        objectText("delete_failed", "io_error", "failed", false, true, true),
                        objectPayload("io_error", "failed", false, true, true),
                        logger ->
                                logger.logDeleteFailed(
                                        file, RuleId.KV_SHARED_SST, SCOPE, "io_error", true)));
        cases.add(
                logCase(
                        "logDirDeleted",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "dir_deleted",
                        "action=dir_deleted path=" + DIR + " ts=" + EVENT_TIME,
                        payload().objectType("directory").path(DIR.toString()),
                        logger -> logger.logDirDeleted(DIR)));
        cases.add(
                logCase(
                        "logWouldDeleteDir",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "would_delete_dir",
                        "action=would_delete_dir path=" + DIR + " ts=" + EVENT_TIME,
                        payload().objectType("directory").path(DIR.toString()),
                        logger -> logger.logWouldDeleteDir(DIR)));
        cases.add(
                logCase(
                        "logWouldDeleteDirectory",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "would_delete",
                        directoryText(
                                "would_delete",
                                "empty_and_older_than_cutoff",
                                "planned",
                                true,
                                false,
                                false),
                        directoryPayload(
                                "empty_and_older_than_cutoff", "planned", true, false, false),
                        logger -> logger.logWouldDeleteDirectory(DIR, 14L, SCOPE, true)));
        cases.add(
                logCase(
                        "logDeletedDirectory",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "deleted",
                        directoryText(
                                "deleted",
                                "empty_and_older_than_cutoff",
                                "deleted",
                                false,
                                false,
                                false),
                        directoryPayload(
                                "empty_and_older_than_cutoff", "deleted", false, false, false),
                        logger -> logger.logDeletedDirectory(DIR, 14L, SCOPE, false)));
        cases.add(
                logCase(
                        "logSkippedDirectory",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "skip_directory",
                        directoryText("skip_directory", "not_empty", "skipped", true, true, true),
                        directoryPayload("not_empty", "skipped", true, true, true),
                        logger ->
                                logger.logSkippedDirectory(
                                        DIR, 14L, SCOPE, "not_empty", true, true, true)));
        cases.add(
                logCase(
                        "logDirectoryDeleteFailed",
                        AuditSeverity.INFO,
                        AuditStage.SCAN,
                        "delete_failed",
                        directoryText("delete_failed", "io_error", "failed", false, true, true),
                        directoryPayload("io_error", "failed", false, true, true),
                        logger ->
                                logger.logDirectoryDeleteFailed(
                                        DIR, 14L, SCOPE, "io_error", false, true)));
        cases.add(
                logCase(
                        "logSkipUnknown",
                        AuditSeverity.WARN,
                        AuditStage.SCAN,
                        "skip_unknown",
                        "action=skip_unknown rule=unknown path=" + PATH + " ts=" + EVENT_TIME,
                        payload().path(PATH.toString()).rule("unknown"),
                        logger -> logger.logSkipUnknown(PATH, RuleId.UNKNOWN)));
        cases.add(
                logCase(
                        "logMtimeUnavailableOnce",
                        AuditSeverity.ERROR,
                        AuditStage.SCAN,
                        "mtime_unavailable",
                        "audit_version=1 stage=scan action=mtime_unavailable database=db table=orders table_id=7"
                                + " partition_id=11 bucket_id=4 object_type=log_segment entry_kind=file"
                                + " sample_name=first.log action_required=true ts="
                                + EVENT_TIME,
                        scopePayload()
                                .objectType("log_segment")
                                .dimension("entry_kind", "file")
                                .dimension("sample_name", "first.log")
                                .flag("action_required", true),
                        logger ->
                                logger.logMtimeUnavailableOnce(
                                        SCOPE,
                                        CleanupObjectType.LOG_SEGMENT,
                                        "file",
                                        "first.log")));
        cases.add(
                logCase(
                        "logBucketAborted",
                        AuditSeverity.ERROR,
                        AuditStage.SCOPE,
                        "bucket_aborted",
                        "action=bucket_aborted bucket=7/11/4 reason=rpc_error ts=" + EVENT_TIME,
                        payload().reasonCode("rpc_error").dimension("bucket", "7/11/4"),
                        logger -> logger.logBucketAborted("7/11/4", "rpc_error")));
        cases.add(
                logCase(
                        "logSkipDb",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_db",
                        "action=skip_db reason=rpc_error db=db ts=" + EVENT_TIME,
                        payload().database("db").reasonCode("rpc_error"),
                        logger -> logger.logSkipDb("db", "rpc_error")));
        cases.add(
                logCase(
                        "logSkipTable",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_table",
                        "action=skip_table reason=rpc_error db=db table=orders ts=" + EVENT_TIME,
                        payload().database("db").table("orders").reasonCode("rpc_error"),
                        logger -> logger.logSkipTable("db", "orders", "rpc_error")));
        cases.add(
                logCase(
                        "logSkipPartitionList",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_partition_list",
                        "action=skip_partition_list reason=rpc_error db=db table=orders ts="
                                + EVENT_TIME,
                        payload().database("db").table("orders").reasonCode("rpc_error"),
                        logger -> logger.logSkipPartitionList("db", "orders", "rpc_error")));
        cases.add(
                logCase(
                        "logSkipKvTarget",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_kv_target",
                        "action=skip_kv_target reason=rpc_error table_id=7 partition_id=11 ts="
                                + EVENT_TIME,
                        payload().tableId(7L).partitionId(11L).reasonCode("rpc_error"),
                        logger -> logger.logSkipKvTarget(7L, 11L, "rpc_error")));
        cases.add(
                logCase(
                        "logSkipKvBucket",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_kv_bucket",
                        "action=skip_kv_bucket reason=empty_active_set table_id=7 partition_id=11 bucket_id=4 ts="
                                + EVENT_TIME,
                        payload()
                                .tableId(7L)
                                .partitionId(11L)
                                .bucketId(4)
                                .reasonCode("empty_active_set"),
                        logger -> logger.logSkipKvBucket(7L, 11L, 4, "empty_active_set")));
        cases.add(
                logCase(
                        "logSkipLogTarget",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_log_target",
                        "action=skip_log_target reason=rpc_error table_id=7 partition_id=11 ts="
                                + EVENT_TIME,
                        payload().tableId(7L).partitionId(11L).reasonCode("rpc_error"),
                        logger -> logger.logSkipLogTarget(7L, 11L, "rpc_error")));
        cases.add(
                logCase(
                        "logSkipLogBucket",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_log_bucket",
                        "action=skip_log_bucket reason=no_remote_manifest table_id=7 partition_id=11 bucket_id=4 ts="
                                + EVENT_TIME,
                        payload()
                                .tableId(7L)
                                .partitionId(11L)
                                .bucketId(4)
                                .reasonCode("no_remote_manifest"),
                        logger -> logger.logSkipLogBucket(7L, 11L, 4, "no_remote_manifest")));
        cases.add(
                logCase(
                        "logSkipOrphanTable",
                        AuditSeverity.INFO,
                        AuditStage.SCOPE,
                        "skip_orphan_table",
                        "action=skip_orphan_table reason=disabled path="
                                + DIR
                                + " ts="
                                + EVENT_TIME,
                        payload()
                                .objectType("directory")
                                .path(DIR.toString())
                                .reasonCode("disabled"),
                        logger -> logger.logSkipOrphanTable(DIR, "disabled")));
        cases.add(
                logCase(
                        "logSkipOrphanTableScan",
                        AuditSeverity.WARN,
                        AuditStage.SCOPE,
                        "skip_orphan_table_scan",
                        "action=skip_orphan_table_scan reason=incomplete db=db ts=" + EVENT_TIME,
                        payload().database("db").reasonCode("incomplete"),
                        logger -> logger.logSkipOrphanTableScan("db", "incomplete")));
        cases.add(
                logCase(
                        "logSkipOrphanPartition",
                        AuditSeverity.INFO,
                        AuditStage.SCOPE,
                        "skip_orphan_partition",
                        "action=skip_orphan_partition reason=disabled path="
                                + DIR
                                + " ts="
                                + EVENT_TIME,
                        payload()
                                .objectType("directory")
                                .path(DIR.toString())
                                .reasonCode("disabled"),
                        logger -> logger.logSkipOrphanPartition(DIR, "disabled")));
        cases.add(
                logCase(
                        "logSkipBucketOutOfScope",
                        AuditSeverity.INFO,
                        AuditStage.SCOPE,
                        "skip_bucket_target",
                        "action=skip_bucket_target reason=out-of-scope-root table_id=7 partition_id=11"
                                + " resolved_root=file:///other ts="
                                + EVENT_TIME,
                        payload()
                                .tableId(7L)
                                .partitionId(11L)
                                .reasonCode("out-of-scope-root")
                                .dimension("resolved_root", "file:///other"),
                        logger -> logger.logSkipBucketOutOfScope(7L, 11L, "file:///other")));
        cases.add(
                logCase(
                        "logSummary",
                        AuditSeverity.INFO,
                        AuditStage.SUMMARY,
                        "summary",
                        "action=summary scanned=10 deleted_total=3 deleted_files=2 empty_dirs_removed=1"
                                + " delete_failures=3 bytes_reclaimed=20 dry_run=true ts="
                                + EVENT_TIME,
                        payload()
                                .metric("scanned", 10L)
                                .metric("deleted_total", 3L)
                                .metric("deleted_files", 2L)
                                .metric("empty_dirs_removed", 1L)
                                .metric("delete_failures", 3L)
                                .metric("bytes_reclaimed", 20L)
                                .flag("dry_run", true),
                        logger -> logger.logSummary(10L, 2L, 1L, 3L, 20L, true)));
        cases.add(
                logCase(
                        "logTableRuleSummary",
                        AuditSeverity.INFO,
                        AuditStage.SUMMARY,
                        "table_rule_summary",
                        ruleText(
                                "table_rule_summary",
                                "database=db table=orders table_id=7 object_type=log_segment",
                                counters),
                        rulePayload(false, counters, true),
                        logger ->
                                logger.logTableRuleSummary(
                                        SCOPE, CleanupObjectType.LOG_SEGMENT, counters, true)));
        cases.add(
                logCase(
                        "logGlobalRuleSummary",
                        AuditSeverity.INFO,
                        AuditStage.SUMMARY,
                        "summary_by_rule",
                        ruleText(
                                "summary_by_rule",
                                "scope=global object_type=log_segment",
                                counters),
                        rulePayload(true, counters, true),
                        logger ->
                                logger.logGlobalRuleSummary(
                                        CleanupObjectType.LOG_SEGMENT, counters, true)));
        cases.add(
                logCase(
                        "logCoverageSummary",
                        AuditSeverity.INFO,
                        AuditStage.SUMMARY,
                        "coverage_summary",
                        "action=coverage_summary no_remote_manifest_targets=1 empty_active_set_targets=2"
                                + " metadata_read_failed_targets=3 directory_list_failed_targets=7 rpc_failed_targets=8"
                                + " mtime_unavailable_files=4 mtime_unavailable_bytes=5 mtime_unavailable_dirs=6"
                                + " complete=false action_required=true dry_run=true ts="
                                + EVENT_TIME,
                        payload()
                                .metric("no_remote_manifest_targets", 1L)
                                .metric("empty_active_set_targets", 2L)
                                .metric("metadata_read_failed_targets", 3L)
                                .metric("directory_list_failed_targets", 7L)
                                .metric("rpc_failed_targets", 8L)
                                .metric("mtime_unavailable_files", 4L)
                                .metric("mtime_unavailable_bytes", 5L)
                                .metric("mtime_unavailable_dirs", 6L)
                                .flag("complete", false)
                                .flag("action_required", true)
                                .flag("dry_run", true),
                        logger -> logger.logCoverageSummary(skipped, 3L, 4L, 5L, 6L, false, true)));
        cases.add(
                logCase(
                        "logAuditIntegrity",
                        AuditSeverity.INFO,
                        AuditStage.SUMMARY,
                        "audit_integrity",
                        "action=audit_integrity rule_counters_consistent=true coverage_complete=true"
                                + " dry_run_counters_consistent=true inconsistent_object_types=0 inconsistent_scopes=0"
                                + " dry_run=true ts="
                                + EVENT_TIME,
                        payload()
                                .metric("inconsistent_object_types", 0L)
                                .metric("inconsistent_scopes", 0L)
                                .flag("rule_counters_consistent", true)
                                .flag("coverage_complete", true)
                                .flag("dry_run_counters_consistent", true)
                                .flag("dry_run", true),
                        logger -> logger.logAuditIntegrity(summary)));
        return cases;
    }

    private static String objectText(
            String action,
            String reason,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return "audit_version=1 stage=scan action="
                + action
                + " object_type=kv_shared_sst path="
                + PATH
                + " size_bytes=12 mtime_ms=13 rule=kv-shared-sst reason_code="
                + reason
                + " result="
                + result
                + " database=db table=orders table_id=7 partition_id=11 bucket_id=4"
                + " dry_run="
                + dryRun
                + " retryable="
                + retryable
                + " action_required="
                + actionRequired
                + " ts="
                + EVENT_TIME;
    }

    private static String directoryText(
            String action,
            String reason,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return "audit_version=1 stage=scan action="
                + action
                + " object_type=directory path="
                + DIR
                + " size_bytes=0 mtime_ms=14 rule=empty-directory reason_code="
                + reason
                + " result="
                + result
                + " database=db table=orders table_id=7 partition_id=11 bucket_id=4"
                + " dry_run="
                + dryRun
                + " retryable="
                + retryable
                + " action_required="
                + actionRequired
                + " ts="
                + EVENT_TIME;
    }

    private static String ruleText(
            String action, String dimensions, RuleDecisionCounters counters) {
        return "action="
                + action
                + " "
                + dimensions
                + " scanned_files="
                + counters.scannedFiles()
                + " scanned_bytes="
                + counters.scannedBytes()
                + " keep_active_files="
                + counters.keepActiveFiles()
                + " keep_active_bytes="
                + counters.keepActiveBytes()
                + " newer_than_cutoff_files="
                + counters.newerThanCutoffFiles()
                + " newer_than_cutoff_bytes="
                + counters.newerThanCutoffBytes()
                + " mtime_unavailable_files="
                + counters.mtimeUnavailableFiles()
                + " mtime_unavailable_bytes="
                + counters.mtimeUnavailableBytes()
                + " unknown_file_type_files="
                + counters.unknownFileTypeFiles()
                + " unknown_file_type_bytes="
                + counters.unknownFileTypeBytes()
                + " candidate_files="
                + counters.candidateFiles()
                + " candidate_bytes="
                + counters.candidateBytes()
                + " dry_run=true ts="
                + EVENT_TIME;
    }

    private static LogCase logCase(
            String name,
            AuditSeverity severity,
            AuditStage stage,
            String action,
            String legacyText,
            ExpectedPayload expectedPayload,
            LogInvocation invocation) {
        return new LogCase(name, severity, stage, action, legacyText, expectedPayload, invocation);
    }

    private static ExpectedPayload payload() {
        return new ExpectedPayload();
    }

    private static ExpectedPayload scopePayload() {
        return payload()
                .database("db")
                .table("orders")
                .tableId(7L)
                .partitionId(11L)
                .bucketId(4)
                .scopeKind("table");
    }

    private static ExpectedPayload objectPayload(
            String reason,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return scopePayload()
                .objectType("kv_shared_sst")
                .path(PATH.toString())
                .sizeBytes(12L)
                .mtimeMs(13L)
                .rule("kv-shared-sst")
                .reasonCode(reason)
                .result(result)
                .flag("dry_run", dryRun)
                .flag("retryable", retryable)
                .flag("action_required", actionRequired);
    }

    private static ExpectedPayload directoryPayload(
            String reason,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return scopePayload()
                .objectType("directory")
                .path(DIR.toString())
                .sizeBytes(0L)
                .mtimeMs(14L)
                .rule("empty-directory")
                .reasonCode(reason)
                .result(result)
                .flag("dry_run", dryRun)
                .flag("retryable", retryable)
                .flag("action_required", actionRequired);
    }

    private static ExpectedPayload rulePayload(
            boolean global, RuleDecisionCounters counters, boolean dryRun) {
        ExpectedPayload expected =
                payload()
                        .scopeKind(global ? "global" : "table")
                        .objectType("log_segment")
                        .metric("scanned_files", counters.scannedFiles())
                        .metric("scanned_bytes", counters.scannedBytes())
                        .metric("keep_active_files", counters.keepActiveFiles())
                        .metric("keep_active_bytes", counters.keepActiveBytes())
                        .metric("newer_than_cutoff_files", counters.newerThanCutoffFiles())
                        .metric("newer_than_cutoff_bytes", counters.newerThanCutoffBytes())
                        .metric("mtime_unavailable_files", counters.mtimeUnavailableFiles())
                        .metric("mtime_unavailable_bytes", counters.mtimeUnavailableBytes())
                        .metric("unknown_file_type_files", counters.unknownFileTypeFiles())
                        .metric("unknown_file_type_bytes", counters.unknownFileTypeBytes())
                        .metric("candidate_files", counters.candidateFiles())
                        .metric("candidate_bytes", counters.candidateBytes())
                        .flag("dry_run", dryRun);
        if (!global) {
            expected.database("db").table("orders").tableId(7L);
        }
        return expected;
    }

    private static OrphanCleanConfig cleanupConfig() {
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(
                        new String[] {
                            "--bootstrap-server",
                            "h:9123",
                            "--database",
                            "db",
                            "--table",
                            "orders",
                            "--older-than",
                            "2020-01-01T00:00:00Z",
                            "--dry-run",
                            "--parallelism",
                            "3",
                            "--remote-fs-op-rate-limit-per-second",
                            "17",
                            "--allow-delete-manifest",
                            "--allow-clean-orphan-tables",
                            "--allow-clean-orphan-partitions"
                        }));
    }

    private static ScopePlanStats scopePlan() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.database();
        stats.table();
        stats.partition();
        stats.discoveredBucket();
        stats.bucketTask();
        stats.orphanDirTask();
        stats.skippedNoRemoteManifest();
        stats.skippedEmptyKvActiveSet();
        stats.skippedOutOfScopeRoot();
        stats.metadataFailure();
        return stats;
    }

    private static RuleDecisionCounters ruleCounters() {
        return RuleDecisionCounters.scanned(2L)
                .add(RuleDecisionCounters.scanned(3L))
                .add(RuleDecisionCounters.scanned(4L))
                .add(RuleDecisionCounters.scanned(5L))
                .add(RuleDecisionCounters.scanned(6L))
                .add(RuleDecisionCounters.scanned(1L))
                .add(RuleDecisionCounters.keepActive(2L))
                .add(RuleDecisionCounters.newerThanCutoff(3L))
                .add(RuleDecisionCounters.mtimeUnavailable(4L))
                .add(RuleDecisionCounters.unknownFileType(5L))
                .add(RuleDecisionCounters.candidate(6L))
                .add(RuleDecisionCounters.candidate(1L));
    }

    private static Map<SkipReasonCode, Long> skippedCounts() {
        Map<SkipReasonCode, Long> skipped = new EnumMap<>(SkipReasonCode.class);
        skipped.put(SkipReasonCode.NO_REMOTE_MANIFEST, 1L);
        skipped.put(SkipReasonCode.EMPTY_KV_ACTIVE_SET, 2L);
        skipped.put(SkipReasonCode.DIRECTORY_LIST_FAILED, 7L);
        skipped.put(SkipReasonCode.RPC_ERROR, 8L);
        return skipped;
    }

    private static CleanupSummary cleanupSummary() throws Exception {
        try (OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new StatsAggregateOperator(true), 1, 1, 0)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(CleanupStats.scope(0L, 0L, Collections.emptyMap())));
            harness.endInput();
            for (Object output : harness.getOutput()) {
                if (output instanceof StreamRecord) {
                    Object value = ((StreamRecord<?>) output).getValue();
                    if (value instanceof CleanupSummary) {
                        return (CleanupSummary) value;
                    }
                }
            }
        }
        throw new AssertionError("missing CleanupSummary");
    }

    private AuditReporterContext reporterContext() throws IOException {
        Path providerRoot = tempDir.resolve("provider-" + providerLoaderSequence.incrementAndGet());
        Path serviceFile = providerRoot.resolve(PROVIDER_RESOURCE);
        Files.createDirectories(serviceFile.getParent());
        Files.write(
                serviceFile,
                Collections.singletonList(LocalAuditReporterFactory.class.getName()),
                StandardCharsets.UTF_8);
        URLClassLoader providerLoader =
                new ProviderClassLoader(
                        new URL[] {providerRoot.toUri().toURL()},
                        AuditLoggerTest.class.getClassLoader());
        providerLoaders.add(providerLoader);
        return new AuditReporterContext(
                RUN_ID, true, AuditStage.SCAN, OPERATOR, SUBTASK, ATTEMPT, providerLoader);
    }

    private static AuditReporterRuntime openTestingRuntime(AuditReporterContext context) {
        Map<String, String> options = new LinkedHashMap<>();
        AuditReporterSpec.ReporterSpec reporter =
                new AuditReporterSpec.ReporterSpec("logger-testing", true, options);
        return AuditReporterRuntime.open(
                new AuditReporterSpec(RUN_ID, Collections.singletonList(reporter)), context);
    }

    private static AuditLogger structuredLogger(
            AuditReporterRuntime runtime,
            AuditReporterContext context,
            LongSupplier clock,
            Supplier<String> eventIds) {
        try {
            Constructor<AuditLogger> constructor =
                    AuditLogger.class.getDeclaredConstructor(
                            AuditReporterRuntime.class,
                            AuditReporterContext.class,
                            LongSupplier.class,
                            Supplier.class);
            constructor.setAccessible(true);
            return constructor.newInstance(runtime, context, clock, eventIds);
        } catch (ReflectiveOperationException e) {
            return fail("AuditLogger structured injection constructor is missing", e);
        }
    }

    private static String fieldValue(String log, String field) {
        String marker = " " + field + "=";
        int start = log.indexOf(marker);
        assertThat(start).as("field %s in %s", field, log).isGreaterThanOrEqualTo(0);
        start += marker.length();
        int end = log.indexOf(' ', start);
        return end < 0 ? log.substring(start) : log.substring(start, end);
    }

    private static String identitySuffix(String eventId) {
        return " run_id="
                + RUN_ID
                + " event_id="
                + eventId
                + " operator="
                + OPERATOR
                + " subtask="
                + SUBTASK
                + " attempt="
                + ATTEMPT;
    }

    private static String eventId(int index) {
        return String.format("00000000-0000-0000-0000-%012d", index);
    }

    private interface LogInvocation {
        void run(AuditLogger logger);
    }

    private static final class LogCase {
        private final String name;
        private final AuditSeverity severity;
        private final AuditStage stage;
        private final String action;
        private final String legacyText;
        private final ExpectedPayload expectedPayload;
        private final LogInvocation invocation;

        private LogCase(
                String name,
                AuditSeverity severity,
                AuditStage stage,
                String action,
                String legacyText,
                ExpectedPayload expectedPayload,
                LogInvocation invocation) {
            this.name = name;
            this.severity = severity;
            this.stage = stage;
            this.action = action;
            this.legacyText = legacyText;
            this.expectedPayload = expectedPayload;
            this.invocation = invocation;
        }
    }

    private static final class ExpectedPayload {
        private final List<Object> envelope =
                new ArrayList<>(Collections.nCopies(13, (Object) null));
        private final Map<String, String> dimensions = new LinkedHashMap<>();
        private final Map<String, Long> metrics = new LinkedHashMap<>();
        private final Map<String, Boolean> flags = new LinkedHashMap<>();

        private ExpectedPayload database(String value) {
            envelope.set(0, value);
            return this;
        }

        private ExpectedPayload table(String value) {
            envelope.set(1, value);
            return this;
        }

        private ExpectedPayload tableId(long value) {
            envelope.set(2, value);
            return this;
        }

        private ExpectedPayload partitionId(long value) {
            envelope.set(3, value);
            return this;
        }

        private ExpectedPayload bucketId(int value) {
            envelope.set(4, value);
            return this;
        }

        private ExpectedPayload scopeKind(String value) {
            envelope.set(5, value);
            return this;
        }

        private ExpectedPayload objectType(String value) {
            envelope.set(6, value);
            return this;
        }

        private ExpectedPayload path(String value) {
            envelope.set(7, value);
            return this;
        }

        private ExpectedPayload sizeBytes(long value) {
            envelope.set(8, value);
            return this;
        }

        private ExpectedPayload mtimeMs(long value) {
            envelope.set(9, value);
            return this;
        }

        private ExpectedPayload rule(String value) {
            envelope.set(10, value);
            return this;
        }

        private ExpectedPayload reasonCode(String value) {
            envelope.set(11, value);
            return this;
        }

        private ExpectedPayload result(String value) {
            envelope.set(12, value);
            return this;
        }

        private ExpectedPayload dimension(String key, String value) {
            dimensions.put(key, value);
            return this;
        }

        private ExpectedPayload metric(String key, long value) {
            metrics.put(key, value);
            return this;
        }

        private ExpectedPayload flag(String key, boolean value) {
            flags.put(key, value);
            return this;
        }
    }

    private static final class CountingClock implements LongSupplier {
        private final long value;
        private final AtomicInteger calls = new AtomicInteger();

        private CountingClock(long value) {
            this.value = value;
        }

        @Override
        public long getAsLong() {
            calls.incrementAndGet();
            return value;
        }

        private int calls() {
            return calls.get();
        }
    }

    private static final class CountingEventIds implements Supplier<String> {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String get() {
            return eventId(calls.incrementAndGet());
        }

        private int calls() {
            return calls.get();
        }
    }

    /** Test-only provider with state isolated from {@link AuditReporterRuntimeTest}. */
    public static final class LocalAuditReporterFactory implements AuditReporterFactory {
        private static final Object LOCK = new Object();
        private static final List<AuditEvent> EVENTS = new ArrayList<>();
        private static int reportCalls;
        private static String reportFailure;

        public LocalAuditReporterFactory() {}

        @Override
        public String identifier() {
            return "logger-testing";
        }

        @Override
        public void validate(Map<String, String> options) {}

        @Override
        public AuditReporter create(Map<String, String> options) {
            return new AuditReporter() {
                @Override
                public void open(AuditReporterContext context) {}

                @Override
                public void report(AuditEvent event) throws Exception {
                    String failure;
                    synchronized (LOCK) {
                        reportCalls++;
                        EVENTS.add(event);
                        failure = reportFailure;
                    }
                    if (failure != null) {
                        throw new Exception(failure);
                    }
                }

                @Override
                public void flush() {}

                @Override
                public void close() {}
            };
        }

        private static void reset() {
            synchronized (LOCK) {
                EVENTS.clear();
                reportCalls = 0;
                reportFailure = null;
            }
        }

        private static void failReport(String message) {
            synchronized (LOCK) {
                reportFailure = message;
            }
        }

        private static List<AuditEvent> events() {
            synchronized (LOCK) {
                return new ArrayList<>(EVENTS);
            }
        }

        private static int reportCalls() {
            synchronized (LOCK) {
                return reportCalls;
            }
        }
    }

    private static final class ProviderClassLoader extends URLClassLoader {
        private ProviderClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (PROVIDER_RESOURCE.equals(name)) {
                return new Vector<>(Collections.list(findResources(name))).elements();
            }
            return super.getResources(name);
        }
    }

    private static final class AuditCapture implements AutoCloseable {
        private final LoggerContext context;
        private final LoggerConfig loggerConfig;
        private final Level previousLevel;
        private final CapturingAppender appender;

        private AuditCapture(List<String> events) {
            context = (LoggerContext) LogManager.getContext(false);
            loggerConfig = context.getConfiguration().getLoggerConfig("fluss.orphan.audit");
            previousLevel = loggerConfig.getLevel();
            appender = new CapturingAppender("audit-logger-test", events);
            appender.start();
            loggerConfig.setLevel(Level.INFO);
            loggerConfig.addAppender(appender, Level.INFO, null);
            context.updateLoggers();
        }

        @Override
        public void close() {
            loggerConfig.removeAppender(appender.getName());
            loggerConfig.setLevel(previousLevel);
            context.updateLoggers();
            appender.stop();
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> events;

        private CapturingAppender(String name, List<String> events) {
            super(name, null, null, false, null);
            this.events = events;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.getLevel().name() + " " + event.getMessage().getFormattedMessage());
        }
    }
}
