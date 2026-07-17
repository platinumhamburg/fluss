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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
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
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuditLoggerTest {

    private static final String RUN_ID = "123e4567-e89b-12d3-a456-426614174000";
    private static final long EVENT_TIME_MILLIS = 1_723_456_789_012L;
    private static final long CUTOFF_MILLIS = 1_704_067_200_000L;
    private static final String OPERATOR_NAME = "ScanAndClean";
    private static final int SUBTASK_INDEX = 3;
    private static final int ATTEMPT_NUMBER = 2;
    private static final AtomicLong APPENDER_IDS = new AtomicLong();
    private static final Object AUDIT_CAPTURE_LOCK = new Object();
    private static int activeAuditCaptures;
    private static Level auditCapturePreviousLevel;
    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @BeforeEach
    void resetTestingReporter() {
        TestingAuditReporterFactory.reset();
    }

    @Test
    void explicitLoggerEmitsCompleteSameSourceParityMatrix() throws Exception {
        OrphanCleanConfig config = completeConfig();
        ScopePlanStats plan = completeScopePlan();
        ScopeIdentity tableScope = ScopeIdentity.table("db", "orders", 7L);
        ScopeIdentity bucketScope = tableScope.withPartitionAndBucket(11L, 4);
        FsPath simpleFile = new FsPath("oss://audit-bucket/root/simple.log");
        FsPath structuredPath = new FsPath("oss://audit-bucket/root/segment-1.log");
        FileMeta structuredFile = new FileMeta(structuredPath, 4096L, 1_700_000_000_123L);
        FsPath directory = new FsPath("oss://audit-bucket/root/empty-dir");
        long directoryMtime = 1_700_000_000_456L;
        RuleDecisionCounters counters = completeRuleCounters();
        CleanupSummary integritySummary = createIntegritySummary();
        Map<SkipReasonCode, Long> skipped = new LinkedHashMap<>();
        skipped.put(SkipReasonCode.NO_REMOTE_MANIFEST, 2L);
        skipped.put(SkipReasonCode.EMPTY_KV_ACTIVE_SET, 3L);
        skipped.put(SkipReasonCode.DIRECTORY_LIST_FAILED, 5L);
        skipped.put(SkipReasonCode.RPC_ERROR, 7L);
        String longSuffix = String.join("", Collections.nCopies(140, "a"));
        String rawSampleName = "bad path/" + longSuffix;
        String sanitizedSampleName = ("bad_path_" + longSuffix).substring(0, 128);

        try (ParityHarness harness =
                new ParityHarness(AuditStage.RUN, OPERATOR_NAME, SUBTASK_INDEX, ATTEMPT_NUMBER)) {
            AuditLogger logger = harness.logger();
            String cutoffIso = CUTOFF_FORMATTER.format(Instant.ofEpochMilli(CUTOFF_MILLIS));

            harness.assertEmission(
                    expected(
                                    "action=cutoff older_than_iso="
                                            + cutoffIso
                                            + " older_than_ms="
                                            + CUTOFF_MILLIS,
                                    AuditSeverity.INFO,
                                    AuditStage.RUN,
                                    "cutoff")
                            .dimension("older_than_iso", cutoffIso)
                            .metric("older_than_ms", CUTOFF_MILLIS),
                    () -> logger.logCutoff(CUTOFF_MILLIS));

            harness.assertEmission(
                    expected(
                                    "action=run_start scope=db.orders older_than_ms="
                                            + CUTOFF_MILLIS
                                            + " dry_run=true parallelism=3"
                                            + " remote_fs_rate_limit=17 allow_delete_manifest=true"
                                            + " allow_clean_orphan_tables=true"
                                            + " allow_clean_orphan_partitions=true",
                                    AuditSeverity.INFO,
                                    AuditStage.RUN,
                                    "run_start")
                            .dimension("scope", "db.orders")
                            .dimension("parallelism", "3")
                            .metric("older_than_ms", CUTOFF_MILLIS)
                            .metric("remote_fs_rate_limit", 17L)
                            .flag("dry_run", true)
                            .flag("allow_delete_manifest", true)
                            .flag("allow_clean_orphan_tables", true)
                            .flag("allow_clean_orphan_partitions", true),
                    () -> logger.logRunStart(config));

            harness.assertEmission(
                    expected(
                                    "action=scope_plan databases=1 tables=2 partitions=3"
                                            + " discovered_buckets=4 bucket_tasks=5"
                                            + " orphan_dir_tasks=6 skipped_no_remote_manifest=7"
                                            + " skipped_empty_kv_active_set=8"
                                            + " skipped_out_of_scope_root=9 metadata_failures=10",
                                    AuditSeverity.INFO,
                                    AuditStage.SCOPE,
                                    "scope_plan")
                            .metric("databases", 1L)
                            .metric("tables", 2L)
                            .metric("partitions", 3L)
                            .metric("discovered_buckets", 4L)
                            .metric("bucket_tasks", 5L)
                            .metric("orphan_dir_tasks", 6L)
                            .metric("skipped_no_remote_manifest", 7L)
                            .metric("skipped_empty_kv_active_set", 8L)
                            .metric("skipped_out_of_scope_root", 9L)
                            .metric("metadata_failures", 10L),
                    () -> logger.logScopePlan(plan));

            harness.assertEmission(
                    expected(
                                    "action=deleted rule=log-segment path="
                                            + simpleFile
                                            + " ok=false",
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "deleted")
                            .stable("object_type", "log_segment")
                            .stable("path", simpleFile.toString())
                            .stable("rule", "log-segment")
                            .flag("ok", false),
                    () -> logger.logDeleted(simpleFile, RuleId.LOG_SEGMENT, false));

            harness.assertEmission(
                    expected(
                                    "action=would_delete rule=log-segment path=" + simpleFile,
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "would_delete")
                            .stable("object_type", "log_segment")
                            .stable("path", simpleFile.toString())
                            .stable("rule", "log-segment"),
                    () -> logger.logWouldDelete(simpleFile, RuleId.LOG_SEGMENT));

            harness.assertEmission(
                    expected(
                                    structuredFileLegacy(
                                            "would_delete",
                                            structuredFile,
                                            bucketScope,
                                            "older_than_cutoff",
                                            "planned",
                                            true,
                                            false,
                                            false),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "would_delete")
                            .scope(bucketScope)
                            .stable("object_type", "log_segment")
                            .stable("path", structuredPath.toString())
                            .stable("size_bytes", 4096L)
                            .stable("mtime_ms", 1_700_000_000_123L)
                            .stable("rule", "log-segment")
                            .stable("reason_code", "older_than_cutoff")
                            .stable("result", "planned")
                            .flag("dry_run", true)
                            .flag("retryable", false)
                            .flag("action_required", false),
                    () -> logger.logWouldDelete(structuredFile, RuleId.LOG_SEGMENT, bucketScope));

            harness.assertEmission(
                    expected(
                                    structuredFileLegacy(
                                            "deleted",
                                            structuredFile,
                                            bucketScope,
                                            "older_than_cutoff",
                                            "success",
                                            false,
                                            false,
                                            false),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "deleted")
                            .scope(bucketScope)
                            .stable("object_type", "log_segment")
                            .stable("path", structuredPath.toString())
                            .stable("size_bytes", 4096L)
                            .stable("mtime_ms", 1_700_000_000_123L)
                            .stable("rule", "log-segment")
                            .stable("reason_code", "older_than_cutoff")
                            .stable("result", "success")
                            .flag("dry_run", false)
                            .flag("retryable", false)
                            .flag("action_required", false),
                    () -> logger.logDeleted(structuredFile, RuleId.LOG_SEGMENT, bucketScope));

            harness.assertEmission(
                    expected(
                                    structuredFileLegacy(
                                            "delete_failed",
                                            structuredFile,
                                            bucketScope,
                                            "rpc_error",
                                            "failed",
                                            false,
                                            true,
                                            true),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "delete_failed")
                            .scope(bucketScope)
                            .stable("object_type", "log_segment")
                            .stable("path", structuredPath.toString())
                            .stable("size_bytes", 4096L)
                            .stable("mtime_ms", 1_700_000_000_123L)
                            .stable("rule", "log-segment")
                            .stable("reason_code", "rpc_error")
                            .stable("result", "failed")
                            .flag("dry_run", false)
                            .flag("retryable", true)
                            .flag("action_required", true),
                    () ->
                            logger.logDeleteFailed(
                                    structuredFile,
                                    RuleId.LOG_SEGMENT,
                                    bucketScope,
                                    "rpc_error",
                                    true));

            harness.assertEmission(
                    expected(
                                    "action=dir_deleted path=" + directory,
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "dir_deleted")
                            .stable("object_type", "directory")
                            .stable("path", directory.toString()),
                    () -> logger.logDirDeleted(directory));

            harness.assertEmission(
                    expected(
                                    "action=would_delete_dir path=" + directory,
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "would_delete_dir")
                            .stable("object_type", "directory")
                            .stable("path", directory.toString()),
                    () -> logger.logWouldDeleteDir(directory));

            harness.assertEmission(
                    expected(
                                    structuredDirectoryLegacy(
                                            "would_delete",
                                            directory,
                                            directoryMtime,
                                            bucketScope,
                                            "empty_and_older_than_cutoff",
                                            "planned",
                                            true,
                                            false,
                                            false),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "would_delete")
                            .scope(bucketScope)
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("size_bytes", 0L)
                            .stable("mtime_ms", directoryMtime)
                            .stable("rule", "empty-directory")
                            .stable("reason_code", "empty_and_older_than_cutoff")
                            .stable("result", "planned")
                            .flag("dry_run", true)
                            .flag("retryable", false)
                            .flag("action_required", false),
                    () ->
                            logger.logWouldDeleteDirectory(
                                    directory, directoryMtime, bucketScope, true));

            harness.assertEmission(
                    expected(
                                    structuredDirectoryLegacy(
                                            "deleted",
                                            directory,
                                            directoryMtime,
                                            bucketScope,
                                            "empty_and_older_than_cutoff",
                                            "deleted",
                                            false,
                                            false,
                                            false),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "deleted")
                            .scope(bucketScope)
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("size_bytes", 0L)
                            .stable("mtime_ms", directoryMtime)
                            .stable("rule", "empty-directory")
                            .stable("reason_code", "empty_and_older_than_cutoff")
                            .stable("result", "deleted")
                            .flag("dry_run", false)
                            .flag("retryable", false)
                            .flag("action_required", false),
                    () ->
                            logger.logDeletedDirectory(
                                    directory, directoryMtime, bucketScope, false));

            harness.assertEmission(
                    expected(
                                    structuredDirectoryLegacy(
                                            "skip_directory",
                                            directory,
                                            directoryMtime,
                                            bucketScope,
                                            "not_empty",
                                            "skipped",
                                            true,
                                            true,
                                            true),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "skip_directory")
                            .scope(bucketScope)
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("size_bytes", 0L)
                            .stable("mtime_ms", directoryMtime)
                            .stable("rule", "empty-directory")
                            .stable("reason_code", "not_empty")
                            .stable("result", "skipped")
                            .flag("dry_run", true)
                            .flag("retryable", true)
                            .flag("action_required", true),
                    () ->
                            logger.logSkippedDirectory(
                                    directory,
                                    directoryMtime,
                                    bucketScope,
                                    "not_empty",
                                    true,
                                    true,
                                    true));

            harness.assertEmission(
                    expected(
                                    structuredDirectoryLegacy(
                                            "delete_failed",
                                            directory,
                                            directoryMtime,
                                            bucketScope,
                                            "permission_denied",
                                            "failed",
                                            false,
                                            false,
                                            true),
                                    AuditSeverity.INFO,
                                    AuditStage.SCAN,
                                    "delete_failed")
                            .scope(bucketScope)
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("size_bytes", 0L)
                            .stable("mtime_ms", directoryMtime)
                            .stable("rule", "empty-directory")
                            .stable("reason_code", "permission_denied")
                            .stable("result", "failed")
                            .flag("dry_run", false)
                            .flag("retryable", false)
                            .flag("action_required", true),
                    () ->
                            logger.logDirectoryDeleteFailed(
                                    directory,
                                    directoryMtime,
                                    bucketScope,
                                    "permission_denied",
                                    false,
                                    false));

            harness.assertEmission(
                    expected(
                                    "action=skip_unknown rule=unknown path=" + simpleFile,
                                    AuditSeverity.WARN,
                                    AuditStage.SCAN,
                                    "skip_unknown")
                            .stable("object_type", "unknown")
                            .stable("path", simpleFile.toString())
                            .stable("rule", "unknown")
                            .stable("reason_code", "unknown_file_type"),
                    () -> logger.logSkipUnknown(simpleFile, RuleId.UNKNOWN));

            harness.assertEmission(
                    expected(
                                    "audit_version=1 stage=scan action=mtime_unavailable"
                                            + " database=db table=orders table_id=7"
                                            + " partition_id=11 bucket_id=4"
                                            + " object_type=log_segment entry_kind=file"
                                            + " sample_name="
                                            + sanitizedSampleName
                                            + " action_required=true",
                                    AuditSeverity.ERROR,
                                    AuditStage.SCAN,
                                    "mtime_unavailable")
                            .scope(bucketScope)
                            .stable("object_type", "log_segment")
                            .dimension("entry_kind", "file")
                            .dimension("sample_name", sanitizedSampleName)
                            .flag("action_required", true),
                    () ->
                            logger.logMtimeUnavailableOnce(
                                    bucketScope,
                                    CleanupObjectType.LOG_SEGMENT,
                                    "file",
                                    rawSampleName));

            harness.assertFullySuppressed(
                    () ->
                            logger.logMtimeUnavailableOnce(
                                    ScopeIdentity.global(),
                                    CleanupObjectType.DIRECTORY,
                                    "directory",
                                    "second-dir"));

            harness.assertEmission(
                    expected(
                                    "action=bucket_aborted bucket=7/11/4 reason=metadata_error",
                                    AuditSeverity.ERROR,
                                    AuditStage.SCOPE,
                                    "bucket_aborted")
                            .stable("reason_code", "metadata_error")
                            .dimension("bucket", "7/11/4"),
                    () -> logger.logBucketAborted("7/11/4", "metadata_error"));

            harness.assertEmission(
                    expected(
                                    "action=skip_db reason=list_failed db=db",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_db")
                            .stable("database", "db")
                            .stable("reason_code", "list_failed"),
                    () -> logger.logSkipDb("db", "list_failed"));

            harness.assertEmission(
                    expected(
                                    "action=skip_table reason=table_info_failed db=db table=orders",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_table")
                            .stable("database", "db")
                            .stable("table", "orders")
                            .stable("reason_code", "table_info_failed"),
                    () -> logger.logSkipTable("db", "orders", "table_info_failed"));

            harness.assertEmission(
                    expected(
                                    "action=skip_partition_list reason=partition_rpc_failed"
                                            + " db=db table=orders",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_partition_list")
                            .stable("database", "db")
                            .stable("table", "orders")
                            .stable("reason_code", "partition_rpc_failed"),
                    () -> logger.logSkipPartitionList("db", "orders", "partition_rpc_failed"));

            harness.assertEmission(
                    expected(
                                    "action=skip_kv_target reason=rpc_error"
                                            + " table_id=7 partition_id=11",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_kv_target")
                            .stable("table_id", 7L)
                            .stable("partition_id", 11L)
                            .stable("reason_code", "rpc_error"),
                    () -> logger.logSkipKvTarget(7L, 11L, "rpc_error"));

            harness.assertEmission(
                    expected(
                                    "action=skip_kv_bucket reason=empty_active_set"
                                            + " table_id=7 partition_id=11 bucket_id=4",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_kv_bucket")
                            .stable("table_id", 7L)
                            .stable("partition_id", 11L)
                            .stable("bucket_id", 4)
                            .stable("reason_code", "empty_active_set"),
                    () -> logger.logSkipKvBucket(7L, 11L, 4, "empty_active_set"));

            harness.assertEmission(
                    expected(
                                    "action=skip_log_target reason=rpc_error"
                                            + " table_id=7 partition_id=11",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_log_target")
                            .stable("table_id", 7L)
                            .stable("partition_id", 11L)
                            .stable("reason_code", "rpc_error"),
                    () -> logger.logSkipLogTarget(7L, 11L, "rpc_error"));

            harness.assertEmission(
                    expected(
                                    "action=skip_log_bucket reason=no_remote_manifest"
                                            + " table_id=7 partition_id=11 bucket_id=4",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_log_bucket")
                            .stable("table_id", 7L)
                            .stable("partition_id", 11L)
                            .stable("bucket_id", 4)
                            .stable("reason_code", "no_remote_manifest"),
                    () -> logger.logSkipLogBucket(7L, 11L, 4, "no_remote_manifest"));

            harness.assertEmission(
                    expected(
                                    "action=skip_orphan_table reason=opt_in_disabled path="
                                            + directory,
                                    AuditSeverity.INFO,
                                    AuditStage.SCOPE,
                                    "skip_orphan_table")
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("reason_code", "opt_in_disabled"),
                    () -> logger.logSkipOrphanTable(directory, "opt_in_disabled"));

            harness.assertEmission(
                    expected(
                                    "action=skip_orphan_table_scan reason=incomplete_table_set db=db",
                                    AuditSeverity.WARN,
                                    AuditStage.SCOPE,
                                    "skip_orphan_table_scan")
                            .stable("database", "db")
                            .stable("reason_code", "incomplete_table_set"),
                    () -> logger.logSkipOrphanTableScan("db", "incomplete_table_set"));

            harness.assertEmission(
                    expected(
                                    "action=skip_orphan_partition reason=opt_in_disabled path="
                                            + directory,
                                    AuditSeverity.INFO,
                                    AuditStage.SCOPE,
                                    "skip_orphan_partition")
                            .stable("object_type", "directory")
                            .stable("path", directory.toString())
                            .stable("reason_code", "opt_in_disabled"),
                    () -> logger.logSkipOrphanPartition(directory, "opt_in_disabled"));

            harness.assertEmission(
                    expected(
                                    "action=skip_bucket_target reason=out-of-scope-root"
                                            + " table_id=7 partition_id=11"
                                            + " resolved_root=oss://other/root",
                                    AuditSeverity.INFO,
                                    AuditStage.SCOPE,
                                    "skip_bucket_target")
                            .stable("table_id", 7L)
                            .stable("partition_id", 11L)
                            .stable("reason_code", "out-of-scope-root")
                            .dimension("resolved_root", "oss://other/root"),
                    () -> logger.logSkipBucketOutOfScope(7L, 11L, "oss://other/root"));

            harness.assertEmission(
                    expected(
                                    "action=summary scanned=20 deleted_total=5 deleted_files=3"
                                            + " empty_dirs_removed=2 delete_failures=1"
                                            + " bytes_reclaimed=99 dry_run=true",
                                    AuditSeverity.INFO,
                                    AuditStage.SUMMARY,
                                    "summary")
                            .metric("scanned", 20L)
                            .metric("deleted_total", 5L)
                            .metric("deleted_files", 3L)
                            .metric("empty_dirs_removed", 2L)
                            .metric("delete_failures", 1L)
                            .metric("bytes_reclaimed", 99L)
                            .flag("dry_run", true),
                    () -> logger.logSummary(20L, 3L, 2L, 1L, 99L, true));

            harness.assertEmission(
                    expected(
                                    ruleSummaryLegacy(
                                            "table_rule_summary",
                                            "database=db table=orders table_id=7"
                                                    + " object_type=log_segment",
                                            counters,
                                            true),
                                    AuditSeverity.INFO,
                                    AuditStage.SUMMARY,
                                    "table_rule_summary")
                            .scope(tableScope)
                            .stable("object_type", "log_segment")
                            .ruleMetrics(counters)
                            .flag("dry_run", true),
                    () ->
                            logger.logTableRuleSummary(
                                    tableScope, CleanupObjectType.LOG_SEGMENT, counters, true));

            harness.assertEmission(
                    expected(
                                    ruleSummaryLegacy(
                                            "summary_by_rule",
                                            "scope=global object_type=kv_shared_sst",
                                            counters,
                                            false),
                                    AuditSeverity.INFO,
                                    AuditStage.SUMMARY,
                                    "summary_by_rule")
                            .stable("scope_kind", "global")
                            .stable("object_type", "kv_shared_sst")
                            .ruleMetrics(counters)
                            .flag("dry_run", false),
                    () ->
                            logger.logGlobalRuleSummary(
                                    CleanupObjectType.KV_SHARED_SST, counters, false));

            harness.assertEmission(
                    expected(
                                    "action=coverage_summary no_remote_manifest_targets=2"
                                            + " empty_active_set_targets=3"
                                            + " metadata_read_failed_targets=4"
                                            + " directory_list_failed_targets=5"
                                            + " rpc_failed_targets=7 mtime_unavailable_files=6"
                                            + " mtime_unavailable_bytes=60"
                                            + " mtime_unavailable_dirs=8 complete=false"
                                            + " action_required=true dry_run=true",
                                    AuditSeverity.INFO,
                                    AuditStage.SUMMARY,
                                    "coverage_summary")
                            .metric("no_remote_manifest_targets", 2L)
                            .metric("empty_active_set_targets", 3L)
                            .metric("metadata_read_failed_targets", 4L)
                            .metric("directory_list_failed_targets", 5L)
                            .metric("rpc_failed_targets", 7L)
                            .metric("mtime_unavailable_files", 6L)
                            .metric("mtime_unavailable_bytes", 60L)
                            .metric("mtime_unavailable_dirs", 8L)
                            .flag("complete", false)
                            .flag("action_required", true)
                            .flag("dry_run", true),
                    () -> logger.logCoverageSummary(skipped, 4L, 6L, 60L, 8L, false, true));

            harness.assertEmission(
                    expected(
                                    "action=audit_integrity rule_counters_consistent="
                                            + integritySummary.ruleCountersConsistent()
                                            + " coverage_complete="
                                            + integritySummary.coverageComplete()
                                            + " dry_run_counters_consistent="
                                            + integritySummary.dryRunCountersConsistent()
                                            + " inconsistent_object_types="
                                            + integritySummary.inconsistentObjectTypes()
                                            + " inconsistent_scopes="
                                            + integritySummary.inconsistentScopes()
                                            + " dry_run="
                                            + integritySummary.dryRun(),
                                    AuditSeverity.INFO,
                                    AuditStage.SUMMARY,
                                    "audit_integrity")
                            .metric(
                                    "inconsistent_object_types",
                                    integritySummary.inconsistentObjectTypes())
                            .metric("inconsistent_scopes", integritySummary.inconsistentScopes())
                            .flag(
                                    "rule_counters_consistent",
                                    integritySummary.ruleCountersConsistent())
                            .flag("coverage_complete", integritySummary.coverageComplete())
                            .flag(
                                    "dry_run_counters_consistent",
                                    integritySummary.dryRunCountersConsistent())
                            .flag("dry_run", integritySummary.dryRun()),
                    () -> logger.logAuditIntegrity(integritySummary));

            assertThat(harness.logs()).hasSize(33);
            assertThat(TestingAuditReporterFactory.events("testing")).hasSize(33);
            assertThat(harness.clockCalls()).isEqualTo(33);
            assertThat(harness.eventIdCalls()).isEqualTo(33);
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .extracting(AuditEvent::getAction)
                    .doesNotContain(
                            "keep_active", "newer_than_cutoff", "scan_progress", "scan_heartbeat");
        }
    }

    @Test
    void emitsBoundedRuleAndCoverageSummaries() throws Exception {
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
        List<String> events = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(events);
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

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=table_rule_summary")
                                        && event.contains("database=db")
                                        && event.contains("table=orders")
                                        && event.contains("object_type=log_segment")
                                        && event.contains("keep_active_files=1")
                                        && event.contains("candidate_files=1"));
        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=coverage_summary")
                                        && event.contains("metadata_read_failed_targets=1")
                                        && event.contains("rpc_failed_targets=1")
                                        && event.contains("complete=false"));
        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=audit_integrity")
                                        && event.contains("rule_counters_consistent=true")
                                        && event.contains("coverage_complete=false")
                                        && event.contains("dry_run_counters_consistent=true")
                                        && event.contains("inconsistent_object_types=0")
                                        && event.contains("inconsistent_scopes=0"));
        assertThat(events)
                .noneMatch(
                        event ->
                                event.contains("action=scan_heartbeat")
                                        || event.contains("action=scan_progress")
                                        || event.contains("action=keep_active")
                                        || event.contains("action=newer_than_cutoff"));
    }

    @Test
    void mtimeUnavailableErrorIsBoundedPerLoggerInstance() {
        List<String> events = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();
        ScopeIdentity scope =
                ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 4);

        try (AuditCapture ignored = new AuditCapture(events)) {
            logger.logMtimeUnavailableOnce(
                    scope, CleanupObjectType.LOG_SEGMENT, "file", "first.log");
            logger.logMtimeUnavailableOnce(
                    scope, CleanupObjectType.DIRECTORY, "directory", "second-dir");
        }

        assertThat(events.stream().filter(e -> e.contains("action=mtime_unavailable")).count())
                .isEqualTo(1L);
        assertThat(events)
                .anyMatch(
                        event ->
                                event.startsWith("ERROR ")
                                        && event.contains("table_id=7")
                                        && event.contains("partition_id=11")
                                        && event.contains("bucket_id=4")
                                        && event.contains("entry_kind=file")
                                        && event.contains("sample_name=first.log")
                                        && event.contains("action_required=true"));
    }

    @Test
    void mtimeUnavailableSampleNameIsSanitizedAndCapped() {
        List<String> events = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();
        String longSuffix = String.join("", Collections.nCopies(140, "a"));

        try (AuditCapture ignored = new AuditCapture(events)) {
            logger.logMtimeUnavailableOnce(
                    ScopeIdentity.global(),
                    CleanupObjectType.DIRECTORY,
                    "directory",
                    "bad path/" + longSuffix);
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=mtime_unavailable")
                                        && event.contains("sample_name=bad_path_")
                                        && !event.contains(longSuffix));
    }

    @Test
    void requiredReporterFailureKeepsExactlyOneTextRecordBeforePropagation() {
        FsPath path = new FsPath("oss://audit-bucket/root/unknown.bin");

        try (ParityHarness harness =
                new ParityHarness(AuditStage.SCAN, OPERATOR_NAME, SUBTASK_INDEX, ATTEMPT_NUMBER)) {
            TestingAuditReporterFactory.fail("testing", "report", "injected-required-failure");

            assertThatThrownBy(() -> harness.logger().logSkipUnknown(path, RuleId.UNKNOWN))
                    .isInstanceOf(AuditReportingException.class);

            assertThat(harness.logs()).hasSize(1);
            assertThat(harness.logs().get(0).level).isEqualTo(Level.WARN);
            assertThat(harness.logs().get(0).message)
                    .isEqualTo(
                            "action=skip_unknown rule=unknown path="
                                    + path
                                    + " ts="
                                    + Instant.ofEpochMilli(EVENT_TIME_MILLIS)
                                    + identitySuffix(
                                            CountingEventIds.eventId(1),
                                            OPERATOR_NAME,
                                            SUBTASK_INDEX,
                                            ATTEMPT_NUMBER));
            assertThat(TestingAuditReporterFactory.events("testing")).hasSize(1);
            AuditEvent event = TestingAuditReporterFactory.events("testing").get(0);
            assertThat(event.getEventId()).isEqualTo(CountingEventIds.eventId(1));
            assertThat(event.getRunId()).isEqualTo(RUN_ID);
            assertThat(event.getEventTimeMillis()).isEqualTo(EVENT_TIME_MILLIS);
            assertThat(event.getSeverity()).isEqualTo(AuditSeverity.WARN);
            assertThat(event.getStage()).isEqualTo(AuditStage.SCAN);
            assertThat(event.getAction()).isEqualTo("skip_unknown");
            assertThat(TestingAuditReporterFactory.callCount("testing:report")).isEqualTo(1);
            assertThat(harness.clockCalls()).isEqualTo(1);
            assertThat(harness.eventIdCalls()).isEqualTo(1);
        }
    }

    @Test
    void explicitMtimeEventUsesEmptyFallbackAndNullProducerIdentity() {
        try (ParityHarness harness = new ParityHarness(AuditStage.SCAN, null, null, null)) {
            harness.assertEmission(
                    expected(
                                    "audit_version=1 stage=scan action=mtime_unavailable"
                                            + " database= table= table_id=none partition_id=none"
                                            + " bucket_id=none object_type=directory"
                                            + " entry_kind=directory sample_name=empty"
                                            + " action_required=true",
                                    AuditSeverity.ERROR,
                                    AuditStage.SCAN,
                                    "mtime_unavailable")
                            .stable("scope_kind", "global")
                            .stable("object_type", "directory")
                            .dimension("entry_kind", "directory")
                            .dimension("sample_name", "empty")
                            .flag("action_required", true),
                    () ->
                            harness.logger()
                                    .logMtimeUnavailableOnce(
                                            ScopeIdentity.global(),
                                            CleanupObjectType.DIRECTORY,
                                            "directory",
                                            ""));
        }
    }

    @Test
    void legacyConstructorKeepsTextOnlyShapeWithoutIdentitySuffix() {
        List<String> events = new CopyOnWriteArrayList<>();
        FsPath path = new FsPath("oss://audit-bucket/root/legacy.log");

        try (AuditCapture ignored = new AuditCapture(events)) {
            new AuditLogger().logWouldDelete(path, RuleId.LOG_SEGMENT);
        }

        assertThat(events).hasSize(1);
        assertThat(events.get(0))
                .startsWith("INFO action=would_delete rule=log-segment path=" + path + " ts=")
                .doesNotContain(" run_id=", " event_id=", " operator=", " subtask=", " attempt=");
        String timestamp = events.get(0).substring(events.get(0).indexOf(" ts=") + 4);
        assertThat(Instant.parse(timestamp).toEpochMilli()).isPositive();
        assertThat(TestingAuditReporterFactory.events("testing")).isEmpty();
        assertThat(TestingAuditReporterFactory.totalInstantiations()).isZero();
    }

    @Test
    void auditCaptureIgnoresRecordsFromUnrelatedLoggers() {
        List<String> events = new CopyOnWriteArrayList<>();
        FsPath path = new FsPath("oss://audit-bucket/root/isolated.log");

        try (AuditCapture ignored = new AuditCapture(events)) {
            LogManager.getLogger("org.apache.fluss.flink.test.unrelated")
                    .info("unrelated-test-record");
            new AuditLogger().logWouldDelete(path, RuleId.LOG_SEGMENT);
        }

        assertThat(events).hasSize(1);
        assertThat(events.get(0))
                .startsWith("INFO action=would_delete rule=log-segment path=" + path + " ts=");
    }

    @Test
    void structuredAuditCaptureIgnoresRecordsFromUnrelatedLoggers() {
        List<CapturedLog> events = new CopyOnWriteArrayList<>();
        FsPath path = new FsPath("oss://audit-bucket/root/structured-isolated.log");

        try (StructuredAuditCapture ignored = new StructuredAuditCapture(events)) {
            LogManager.getLogger("org.apache.fluss.flink.test.structured.unrelated")
                    .info("unrelated-structured-test-record");
            new AuditLogger().logWouldDelete(path, RuleId.LOG_SEGMENT);
        }

        assertThat(events).hasSize(1);
        assertThat(events.get(0).level).isEqualTo(Level.INFO);
        assertThat(events.get(0).message)
                .startsWith("action=would_delete rule=log-segment path=" + path + " ts=");
    }

    @Test
    void overlappingAuditCapturesCloseIndependently() {
        List<String> firstEvents = new CopyOnWriteArrayList<>();
        List<String> secondEvents = new CopyOnWriteArrayList<>();
        FsPath firstPath = new FsPath("oss://audit-bucket/root/overlap-first.log");
        FsPath secondPath = new FsPath("oss://audit-bucket/root/overlap-second.log");

        AuditCapture first = new AuditCapture(firstEvents);
        AuditCapture second = new AuditCapture(secondEvents);
        boolean firstClosed = false;
        try {
            new AuditLogger().logWouldDelete(firstPath, RuleId.LOG_SEGMENT);
            assertThat(firstEvents).hasSize(1);
            assertThat(secondEvents).hasSize(1);

            first.close();
            firstClosed = true;
            new AuditLogger().logWouldDelete(secondPath, RuleId.LOG_SEGMENT);
            assertThat(firstEvents).hasSize(1);
            assertThat(secondEvents).hasSize(2);
        } finally {
            if (!firstClosed) {
                first.close();
            }
            second.close();
        }
    }

    @Test
    void publicLogMethodSurfaceRemainsExactlyTheThirtyThreeMethodBaseline() {
        Set<String> actual =
                Arrays.stream(AuditLogger.class.getDeclaredMethods())
                        .filter(method -> Modifier.isPublic(method.getModifiers()))
                        .filter(method -> method.getName().startsWith("log"))
                        .map(AuditLoggerTest::signature)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        assertThat(actual)
                .containsExactlyInAnyOrder(
                        "logCutoff(long)",
                        "logRunStart(OrphanCleanConfig)",
                        "logScopePlan(ScopePlanStats)",
                        "logDeleted(FsPath,RuleId,boolean)",
                        "logWouldDelete(FsPath,RuleId)",
                        "logWouldDelete(FileMeta,RuleId,ScopeIdentity)",
                        "logDeleted(FileMeta,RuleId,ScopeIdentity)",
                        "logDeleteFailed(FileMeta,RuleId,ScopeIdentity,String,boolean)",
                        "logDirDeleted(FsPath)",
                        "logWouldDeleteDir(FsPath)",
                        "logWouldDeleteDirectory(FsPath,long,ScopeIdentity,boolean)",
                        "logDeletedDirectory(FsPath,long,ScopeIdentity,boolean)",
                        "logSkippedDirectory(FsPath,long,ScopeIdentity,String,boolean,boolean,boolean)",
                        "logDirectoryDeleteFailed(FsPath,long,ScopeIdentity,String,boolean,boolean)",
                        "logSkipUnknown(FsPath,RuleId)",
                        "logMtimeUnavailableOnce(ScopeIdentity,CleanupObjectType,String,String)",
                        "logBucketAborted(String,String)",
                        "logSkipDb(String,String)",
                        "logSkipTable(String,String,String)",
                        "logSkipPartitionList(String,String,String)",
                        "logSkipKvTarget(long,Long,String)",
                        "logSkipKvBucket(long,Long,int,String)",
                        "logSkipLogTarget(long,Long,String)",
                        "logSkipLogBucket(long,Long,int,String)",
                        "logSkipOrphanTable(FsPath,String)",
                        "logSkipOrphanTableScan(String,String)",
                        "logSkipOrphanPartition(FsPath,String)",
                        "logSkipBucketOutOfScope(long,Long,String)",
                        "logSummary(long,long,long,long,long,boolean)",
                        "logTableRuleSummary(ScopeIdentity,CleanupObjectType,RuleDecisionCounters,boolean)",
                        "logGlobalRuleSummary(CleanupObjectType,RuleDecisionCounters,boolean)",
                        "logCoverageSummary(Map,long,long,long,long,boolean,boolean)",
                        "logAuditIntegrity(CleanupSummary)");
        assertThat(actual).hasSize(33);
        assertThat(actual)
                .noneMatch(
                        method ->
                                method.contains("KeepActive")
                                        || method.contains("NewerThanCutoff")
                                        || method.contains("Progress")
                                        || method.contains("Heartbeat"));
    }

    private static OrphanCleanConfig completeConfig() {
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--database",
                            "db",
                            "--table",
                            "orders",
                            "--older-than",
                            "2024-01-01T00:00:00Z",
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

    private static ScopePlanStats completeScopePlan() {
        ScopePlanStats stats = new ScopePlanStats();
        repeat(1, stats::database);
        repeat(2, stats::table);
        repeat(3, stats::partition);
        repeat(4, stats::discoveredBucket);
        repeat(5, stats::bucketTask);
        repeat(6, stats::orphanDirTask);
        repeat(7, stats::skippedNoRemoteManifest);
        repeat(8, stats::skippedEmptyKvActiveSet);
        repeat(9, stats::skippedOutOfScopeRoot);
        repeat(10, stats::metadataFailure);
        return stats;
    }

    private static void repeat(int count, Runnable increment) {
        for (int i = 0; i < count; i++) {
            increment.run();
        }
    }

    private static RuleDecisionCounters completeRuleCounters() {
        return RuleDecisionCounters.scanned(10L)
                .add(RuleDecisionCounters.scanned(20L))
                .add(RuleDecisionCounters.scanned(30L))
                .add(RuleDecisionCounters.scanned(40L))
                .add(RuleDecisionCounters.scanned(110L))
                .add(RuleDecisionCounters.keepActive(10L))
                .add(RuleDecisionCounters.newerThanCutoff(20L))
                .add(RuleDecisionCounters.mtimeUnavailable(30L))
                .add(RuleDecisionCounters.unknownFileType(40L))
                .add(RuleDecisionCounters.candidate(110L));
    }

    private static CleanupSummary createIntegritySummary() throws Exception {
        ScopeIdentity scope = ScopeIdentity.table("db", "orders", 7L);
        CleanupStats stats =
                CleanupStats.scanBuilder(scope)
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

        try (SummaryHarness harness = new SummaryHarness()) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(
                            CleanupStats.scope(
                                    1L,
                                    1L,
                                    Collections.singletonMap(SkipReasonCode.RPC_ERROR, 1L))));
            harness.processElement(new StreamRecord<>(stats));
            harness.endInput();
            assertThat(harness.summaries()).hasSize(1);
            return harness.summaries().get(0);
        }
    }

    private static String structuredFileLegacy(
            String action,
            FileMeta file,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return "audit_version=1 stage=scan action="
                + action
                + " object_type=log_segment path="
                + file.path()
                + " size_bytes="
                + file.size()
                + " mtime_ms="
                + file.modificationTime()
                + " rule=log-segment reason_code="
                + reasonCode
                + " result="
                + result
                + " database="
                + scope.database()
                + " table="
                + scope.table()
                + " table_id="
                + scope.tableId()
                + " partition_id="
                + scope.partitionId()
                + " bucket_id="
                + scope.bucketId()
                + " dry_run="
                + dryRun
                + " retryable="
                + retryable
                + " action_required="
                + actionRequired;
    }

    private static String structuredDirectoryLegacy(
            String action,
            FsPath directory,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        return "audit_version=1 stage=scan action="
                + action
                + " object_type=directory path="
                + directory
                + " size_bytes=0 mtime_ms="
                + modificationTime
                + " rule=empty-directory reason_code="
                + reasonCode
                + " result="
                + result
                + " database="
                + scope.database()
                + " table="
                + scope.table()
                + " table_id="
                + textValue(scope.tableId())
                + " partition_id="
                + textValue(scope.partitionId())
                + " bucket_id="
                + textValue(scope.bucketId())
                + " dry_run="
                + dryRun
                + " retryable="
                + retryable
                + " action_required="
                + actionRequired;
    }

    private static String ruleSummaryLegacy(
            String action, String dimensions, RuleDecisionCounters counters, boolean dryRun) {
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
                + " dry_run="
                + dryRun;
    }

    private static ExpectedEmission expected(
            String legacyWithoutTimestamp,
            AuditSeverity severity,
            AuditStage stage,
            String action) {
        return new ExpectedEmission(legacyWithoutTimestamp, severity, stage, action);
    }

    private static String signature(Method method) {
        return method.getName()
                + "("
                + Arrays.stream(method.getParameterTypes())
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(","))
                + ")";
    }

    private static String identitySuffix(
            String eventId, String operatorName, Integer subtaskIndex, Integer attemptNumber) {
        return " run_id="
                + RUN_ID
                + " event_id="
                + eventId
                + " operator="
                + textValue(operatorName)
                + " subtask="
                + textValue(subtaskIndex)
                + " attempt="
                + textValue(attemptNumber);
    }

    private static String textValue(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static AuditReporterRuntime openTestingRuntime(AuditReporterContext context) {
        ReporterSpec reporter = new ReporterSpec("testing", true, Collections.emptyMap());
        return AuditReporterRuntime.open(
                new AuditReporterSpec(RUN_ID, Collections.singletonList(reporter)), context);
    }

    private static AuditLogger newDeterministicLogger(
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
        } catch (NoSuchMethodException e) {
            throw new AssertionError(
                    "Task 4 deterministic AuditLogger constructor is not implemented", e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AssertionError("Task 4 deterministic AuditLogger constructor is unusable", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError("Task 4 deterministic AuditLogger constructor failed", cause);
        }
    }

    private static final class CountingClock implements LongSupplier {
        private int calls;

        @Override
        public long getAsLong() {
            return EVENT_TIME_MILLIS + calls++;
        }

        private int calls() {
            return calls;
        }

        private long nextValue() {
            return EVENT_TIME_MILLIS + calls;
        }
    }

    private static final class CountingEventIds implements Supplier<String> {
        private int calls;

        @Override
        public String get() {
            calls++;
            return eventId(calls);
        }

        private int calls() {
            return calls;
        }

        private String nextValue() {
            return eventId(calls + 1);
        }

        private static String eventId(int sequence) {
            return String.format(java.util.Locale.ROOT, "123e4567-e89b-12d3-a456-%012d", sequence);
        }
    }

    private static final class ExpectedEmission {
        private final String legacyWithoutTimestamp;
        private final AuditSeverity severity;
        private final AuditStage stage;
        private final String action;
        private final LinkedHashMap<String, Object> stableFields = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> dimensions = new LinkedHashMap<>();
        private final LinkedHashMap<String, Long> metrics = new LinkedHashMap<>();
        private final LinkedHashMap<String, Boolean> flags = new LinkedHashMap<>();

        private ExpectedEmission(
                String legacyWithoutTimestamp,
                AuditSeverity severity,
                AuditStage stage,
                String action) {
            this.legacyWithoutTimestamp = legacyWithoutTimestamp;
            this.severity = severity;
            this.stage = stage;
            this.action = action;
        }

        private ExpectedEmission stable(String name, Object value) {
            if (value != null) {
                stableFields.put(name, value);
            }
            return this;
        }

        private ExpectedEmission scope(ScopeIdentity scope) {
            stable("scope_kind", scope.kind().name().toLowerCase(Locale.ROOT));
            if (scope.kind() != ScopeKind.GLOBAL) {
                stable("database", scope.database());
                stable("table", scope.table());
                stable("table_id", scope.tableId());
                stable("partition_id", scope.partitionId());
                stable("bucket_id", scope.bucketId());
            }
            return this;
        }

        private ExpectedEmission dimension(String name, String value) {
            dimensions.put(name, value);
            return this;
        }

        private ExpectedEmission metric(String name, long value) {
            metrics.put(name, value);
            return this;
        }

        private ExpectedEmission flag(String name, boolean value) {
            flags.put(name, value);
            return this;
        }

        private ExpectedEmission ruleMetrics(RuleDecisionCounters counters) {
            return metric("scanned_files", counters.scannedFiles())
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
                    .metric("candidate_bytes", counters.candidateBytes());
        }
    }

    private static final class ParityHarness implements AutoCloseable {
        private final AuditReporterRuntime runtime;
        private final AuditReporterContext context;
        private final CountingClock clock = new CountingClock();
        private final CountingEventIds eventIds = new CountingEventIds();
        private final List<CapturedLog> logs = new CopyOnWriteArrayList<>();
        private final AuditLogger logger;
        private final StructuredAuditCapture capture;

        private ParityHarness(
                AuditStage contextStage,
                String operatorName,
                Integer subtaskIndex,
                Integer attemptNumber) {
            context =
                    new AuditReporterContext(
                            RUN_ID,
                            false,
                            contextStage,
                            operatorName,
                            subtaskIndex,
                            attemptNumber,
                            AuditLoggerTest.class.getClassLoader());
            runtime = openTestingRuntime(context);
            try {
                logger = newDeterministicLogger(runtime, context, clock, eventIds);
            } catch (RuntimeException | Error failure) {
                try {
                    runtime.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
            capture = new StructuredAuditCapture(logs);
        }

        private AuditLogger logger() {
            return logger;
        }

        private List<CapturedLog> logs() {
            return logs;
        }

        private int clockCalls() {
            return clock.calls();
        }

        private int eventIdCalls() {
            return eventIds.calls();
        }

        private void assertEmission(ExpectedEmission expected, Runnable invocation) {
            int textCount = logs.size();
            List<AuditEvent> eventsBefore = TestingAuditReporterFactory.events("testing");
            int eventCount = eventsBefore.size();
            int clockCalls = clock.calls();
            int eventIdCalls = eventIds.calls();
            long expectedEventTime = clock.nextValue();
            String expectedEventId = eventIds.nextValue();

            invocation.run();

            assertThat(logs).hasSize(textCount + 1);
            List<AuditEvent> eventsAfter = TestingAuditReporterFactory.events("testing");
            assertThat(eventsAfter).hasSize(eventCount + 1);
            assertThat(clock.calls()).isEqualTo(clockCalls + 1);
            assertThat(eventIds.calls()).isEqualTo(eventIdCalls + 1);

            CapturedLog text = logs.get(textCount);
            AuditEvent event = eventsAfter.get(eventCount);
            String suffix =
                    identitySuffix(
                            expectedEventId,
                            context.getOperatorName(),
                            context.getSubtaskIndex(),
                            context.getAttemptNumber());
            String expectedMessage =
                    expected.legacyWithoutTimestamp
                            + " ts="
                            + Instant.ofEpochMilli(expectedEventTime)
                            + suffix;

            assertThat(text.level).isEqualTo(logLevel(expected.severity));
            assertThat(text.level.name()).isEqualTo(event.getSeverity().name());
            assertThat(text.message).isEqualTo(expectedMessage);
            int timestampStart = text.message.indexOf(" ts=") + 4;
            int timestampEnd = text.message.indexOf(" run_id=", timestampStart);
            assertThat(timestampStart).isGreaterThan(3);
            assertThat(timestampEnd).isGreaterThan(timestampStart);
            assertThat(
                            Instant.parse(text.message.substring(timestampStart, timestampEnd))
                                    .toEpochMilli())
                    .isEqualTo(event.getEventTimeMillis());

            assertThat(event.getRunId()).isEqualTo(RUN_ID);
            assertThat(event.getEventId()).isEqualTo(expectedEventId);
            assertThat(event.getEventTimeMillis()).isEqualTo(expectedEventTime);
            assertThat(event.getOperatorName()).isEqualTo(context.getOperatorName());
            assertThat(event.getSubtaskIndex()).isEqualTo(context.getSubtaskIndex());
            assertThat(event.getAttemptNumber()).isEqualTo(context.getAttemptNumber());
            assertThat(event.getSeverity()).isEqualTo(expected.severity);
            assertThat(event.getStage()).isEqualTo(expected.stage);
            assertThat(event.getAction()).isEqualTo(expected.action);
            assertThat(stableFields(event)).isEqualTo(expected.stableFields);
            assertThat(event.getDimensions()).containsExactlyEntriesOf(expected.dimensions);
            assertThat(event.getMetrics()).containsExactlyEntriesOf(expected.metrics);
            assertThat(event.getFlags()).containsExactlyEntriesOf(expected.flags);
            assertNoDuplicateScalarFields(event);
        }

        private void assertFullySuppressed(Runnable invocation) {
            int textCount = logs.size();
            int eventCount = TestingAuditReporterFactory.events("testing").size();
            int clockCalls = clock.calls();
            int eventIdCalls = eventIds.calls();

            invocation.run();

            assertThat(logs).hasSize(textCount);
            assertThat(TestingAuditReporterFactory.events("testing")).hasSize(eventCount);
            assertThat(clock.calls()).isEqualTo(clockCalls);
            assertThat(eventIds.calls()).isEqualTo(eventIdCalls);
        }

        @Override
        public void close() {
            capture.close();
            runtime.close();
        }
    }

    private static Map<String, Object> stableFields(AuditEvent event) {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        putIfPresent(fields, "database", event.getDatabase());
        putIfPresent(fields, "table", event.getTable());
        putIfPresent(fields, "table_id", event.getTableId());
        putIfPresent(fields, "partition_id", event.getPartitionId());
        putIfPresent(fields, "bucket_id", event.getBucketId());
        putIfPresent(fields, "scope_kind", event.getScopeKind());
        putIfPresent(fields, "object_type", event.getObjectType());
        putIfPresent(fields, "path", event.getPath());
        putIfPresent(fields, "size_bytes", event.getSizeBytes());
        putIfPresent(fields, "mtime_ms", event.getMtimeMs());
        putIfPresent(fields, "rule", event.getRule());
        putIfPresent(fields, "reason_code", event.getReasonCode());
        putIfPresent(fields, "result", event.getResult());
        return fields;
    }

    private static void putIfPresent(Map<String, Object> fields, String name, Object value) {
        if (value != null) {
            fields.put(name, value);
        }
    }

    private static void assertNoDuplicateScalarFields(AuditEvent event) {
        Set<String> allNames = new LinkedHashSet<>(stableFields(event).keySet());
        int expectedNameCount = allNames.size();
        expectedNameCount += event.getDimensions().size();
        expectedNameCount += event.getMetrics().size();
        expectedNameCount += event.getFlags().size();
        allNames.addAll(event.getDimensions().keySet());
        allNames.addAll(event.getMetrics().keySet());
        allNames.addAll(event.getFlags().keySet());

        assertThat(allNames).hasSize(expectedNameCount);
        assertThat(allNames)
                .doesNotContain(
                        "schema_version",
                        "event_id",
                        "run_id",
                        "event_time_millis",
                        "severity",
                        "stage",
                        "action",
                        "operator",
                        "subtask",
                        "attempt");
    }

    private static Level logLevel(AuditSeverity severity) {
        switch (severity) {
            case INFO:
                return Level.INFO;
            case WARN:
                return Level.WARN;
            case ERROR:
                return Level.ERROR;
            default:
                throw new AssertionError(severity);
        }
    }

    private static Logger attachAuditAppender(AbstractAppender appender) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Logger logger = context.getLogger("fluss.orphan.audit");
        synchronized (AUDIT_CAPTURE_LOCK) {
            if (activeAuditCaptures == 0) {
                auditCapturePreviousLevel = logger.getLevel();
            }
            activeAuditCaptures++;
            logger.addAppender(appender);
            logger.setLevel(Level.INFO);
        }
        return logger;
    }

    private static void detachAuditAppender(Logger logger, AbstractAppender appender) {
        synchronized (AUDIT_CAPTURE_LOCK) {
            logger.removeAppender(appender);
            activeAuditCaptures--;
            if (activeAuditCaptures == 0) {
                logger.setLevel(auditCapturePreviousLevel);
                auditCapturePreviousLevel = null;
            } else {
                logger.setLevel(Level.INFO);
            }
        }
        appender.stop();
    }

    private static final class SummaryHarness
            extends OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> {

        private SummaryHarness() throws Exception {
            super(new StatsAggregateOperator(true), 1, 1, 0);
        }

        @SuppressWarnings("unchecked")
        private List<CleanupSummary> summaries() {
            return getRecordOutput().stream()
                    .map(record -> ((StreamRecord<CleanupSummary>) record).getValue())
                    .collect(Collectors.toList());
        }
    }

    private static final class StructuredAuditCapture implements AutoCloseable {
        private final Logger logger;
        private final StructuredCapturingAppender appender;

        private StructuredAuditCapture(List<CapturedLog> logs) {
            appender =
                    new StructuredCapturingAppender(
                            "audit-logger-parity-test-" + APPENDER_IDS.incrementAndGet(), logs);
            appender.start();
            logger = attachAuditAppender(appender);
        }

        @Override
        public void close() {
            detachAuditAppender(logger, appender);
        }
    }

    private static final class StructuredCapturingAppender extends AbstractAppender {
        private final List<CapturedLog> logs;
        private final long ownerThreadId;

        private StructuredCapturingAppender(String name, List<CapturedLog> logs) {
            super(name, null, null, false, null);
            this.logs = logs;
            this.ownerThreadId = Thread.currentThread().getId();
        }

        @Override
        public void append(LogEvent event) {
            if (event.getThreadId() == ownerThreadId
                    && "fluss.orphan.audit".equals(event.getLoggerName())) {
                logs.add(
                        new CapturedLog(
                                event.getLevel(), event.getMessage().getFormattedMessage()));
            }
        }
    }

    private static final class CapturedLog {
        private final Level level;
        private final String message;

        private CapturedLog(Level level, String message) {
            this.level = level;
            this.message = message;
        }
    }

    private static final class AuditCapture implements AutoCloseable {
        private final Logger logger;
        private final CapturingAppender appender;

        private AuditCapture(List<String> events) {
            appender =
                    new CapturingAppender(
                            "audit-logger-test-" + APPENDER_IDS.incrementAndGet(), events);
            appender.start();
            logger = attachAuditAppender(appender);
        }

        @Override
        public void close() {
            detachAuditAppender(logger, appender);
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> events;
        private final long ownerThreadId;

        private CapturingAppender(String name, List<String> events) {
            super(name, null, null, false, null);
            this.events = events;
            this.ownerThreadId = Thread.currentThread().getId();
        }

        @Override
        public void append(LogEvent event) {
            if (event.getThreadId() == ownerThreadId
                    && "fluss.orphan.audit".equals(event.getLoggerName())) {
                events.add(
                        event.getLevel().name() + " " + event.getMessage().getFormattedMessage());
            }
        }
    }
}
