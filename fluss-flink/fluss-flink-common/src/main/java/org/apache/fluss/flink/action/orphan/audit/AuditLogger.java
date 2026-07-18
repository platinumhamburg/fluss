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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.job.CleanupSummary;
import org.apache.fluss.flink.action.orphan.job.RuleDecisionCounters;
import org.apache.fluss.flink.action.orphan.job.ScopePlanStats;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Structured audit writer for the orphan files cleanup action.
 *
 * <p>The dedicated logger name {@code fluss.orphan.audit} can be routed to a separate sink. Every
 * legacy text record is written before the same-source event is passed to configured reporters.
 */
@Internal
public final class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("fluss.orphan.audit");

    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final AuditReporterRuntime reporterRuntime;
    private final AuditReporterContext context;
    private final LongSupplier clock;
    private final Supplier<String> eventIdSupplier;

    private boolean mtimeUnavailableSampleLogged;

    /** Creates a compatibility logger that writes only the fixed text audit record. */
    public AuditLogger() {
        String runId = UUID.randomUUID().toString();
        this.reporterRuntime = null;
        this.context =
                new AuditReporterContext(
                        runId,
                        false,
                        AuditStage.RUN,
                        null,
                        null,
                        null,
                        AuditLogger.class.getClassLoader());
        this.clock = System::currentTimeMillis;
        this.eventIdSupplier = () -> UUID.randomUUID().toString();
    }

    /** Creates a logger backed by an explicitly opened reporter runtime. */
    public AuditLogger(AuditReporterRuntime reporterRuntime, AuditReporterContext context) {
        this(
                reporterRuntime,
                context,
                System::currentTimeMillis,
                () -> UUID.randomUUID().toString());
    }

    AuditLogger(
            AuditReporterRuntime reporterRuntime,
            AuditReporterContext context,
            LongSupplier clock,
            Supplier<String> eventIdSupplier) {
        this.reporterRuntime = Objects.requireNonNull(reporterRuntime, "reporterRuntime");
        this.context = Objects.requireNonNull(context, "context");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.eventIdSupplier = Objects.requireNonNull(eventIdSupplier, "eventIdSupplier");
    }

    /** One-shot startup event recording the frozen file cutoff that drives deletion decisions. */
    public void logCutoff(long olderThanMillis) {
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put(
                "older_than_iso", CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis)));
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("older_than_ms", olderThanMillis);
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.RUN, "cutoff")
                        .dimensions(dimensions)
                        .metrics(metrics)
                        .build();
        emit(
                event,
                "action=cutoff older_than_iso={} older_than_ms={}",
                CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis)),
                olderThanMillis);
    }

    /** One-shot, non-secret execution configuration at normal INFO level. */
    public void logRunStart(OrphanCleanConfig config) {
        String scope = config.allDatabases() ? "all-databases" : config.database().get();
        if (config.table().isPresent()) {
            scope = scope + "." + config.table().get();
        }
        String parallelism =
                config.parallelism().isPresent()
                        ? config.parallelism().get().toString()
                        : "default";
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("parallelism", parallelism);
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("older_than_ms", config.olderThanMillis());
        metrics.put("remote_fs_rate_limit", config.remoteFsOpRateLimitPerSecond());
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("dry_run", config.dryRun());
        flags.put("allow_delete_manifest", config.allowDeleteManifest());
        flags.put("allow_clean_orphan_tables", config.allowCleanOrphanTables());
        flags.put("allow_clean_orphan_partitions", config.allowCleanOrphanPartitions());
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.RUN, "run_start")
                        .scopeKind(scope)
                        .dimensions(dimensions)
                        .metrics(metrics)
                        .flags(flags)
                        .build();
        emit(
                event,
                "action=run_start scope={} older_than_ms={} dry_run={} parallelism={}"
                        + " remote_fs_rate_limit={} allow_delete_manifest={}"
                        + " allow_clean_orphan_tables={} allow_clean_orphan_partitions={}",
                scope,
                config.olderThanMillis(),
                config.dryRun(),
                config.parallelism().isPresent() ? config.parallelism().get() : "default",
                config.remoteFsOpRateLimitPerSecond(),
                config.allowDeleteManifest(),
                config.allowCleanOrphanTables(),
                config.allowCleanOrphanPartitions());
    }

    /** One-shot aggregate of discovered scope, expected skips, and emitted cleanup work. */
    public void logScopePlan(ScopePlanStats stats) {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("databases", stats.databases());
        metrics.put("tables", stats.tables());
        metrics.put("partitions", stats.partitions());
        metrics.put("discovered_buckets", stats.discoveredBuckets());
        metrics.put("bucket_tasks", stats.bucketTasks());
        metrics.put("orphan_dir_tasks", stats.orphanDirTasks());
        metrics.put("skipped_no_remote_manifest", stats.skippedNoRemoteManifestCount());
        metrics.put("skipped_empty_kv_active_set", stats.skippedEmptyKvActiveSetCount());
        metrics.put("skipped_out_of_scope_root", stats.skippedOutOfScopeRootCount());
        metrics.put("metadata_failures", stats.metadataFailures());
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scope_plan")
                        .metrics(metrics)
                        .build();
        emit(
                event,
                "action=scope_plan databases={} tables={} partitions={} discovered_buckets={}"
                        + " bucket_tasks={} orphan_dir_tasks={} skipped_no_remote_manifest={}"
                        + " skipped_empty_kv_active_set={} skipped_out_of_scope_root={}"
                        + " metadata_failures={}",
                stats.databases(),
                stats.tables(),
                stats.partitions(),
                stats.discoveredBuckets(),
                stats.bucketTasks(),
                stats.orphanDirTasks(),
                stats.skippedNoRemoteManifestCount(),
                stats.skippedEmptyKvActiveSetCount(),
                stats.skippedOutOfScopeRootCount(),
                stats.metadataFailures());
    }

    public void logDeleted(FsPath path, RuleId ruleId, boolean ok) {
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("ok", ok);
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "deleted")
                        .path(path.toString())
                        .rule(ruleId.toString())
                        .flags(flags)
                        .build();
        emit(event, "action=deleted rule={} path={} ok={}", ruleId, path, ok);
    }

    public void logWouldDelete(FsPath path, RuleId ruleId) {
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "would_delete")
                        .path(path.toString())
                        .rule(ruleId.toString())
                        .build();
        emit(event, "action=would_delete rule={} path={}", ruleId, path);
    }

    public void logWouldDelete(FileMeta file, RuleId ruleId, ScopeIdentity scope) {
        logObjectAction(
                "would_delete",
                file,
                ruleId,
                scope,
                "older_than_cutoff",
                "planned",
                true,
                false,
                false);
    }

    public void logDeleted(FileMeta file, RuleId ruleId, ScopeIdentity scope) {
        logObjectAction(
                "deleted",
                file,
                ruleId,
                scope,
                "older_than_cutoff",
                "success",
                false,
                false,
                false);
    }

    public void logDeleteFailed(
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            boolean retryable) {
        logObjectAction(
                "delete_failed", file, ruleId, scope, reasonCode, "failed", false, retryable, true);
    }

    private void logObjectAction(
            String action,
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        Map<String, Boolean> flags = actionFlags(dryRun, retryable, actionRequired);
        AuditEvent event =
                scopeEvent(AuditSeverity.INFO, AuditStage.SCAN, action, scope)
                        .objectType(lower(ruleId.objectType().name()))
                        .path(file.path().toString())
                        .sizeBytes(file.size())
                        .mtimeMs(file.modificationTime())
                        .rule(ruleId.toString())
                        .reasonCode(reasonCode)
                        .result(result)
                        .flags(flags)
                        .build();
        emit(
                event,
                "audit_version=1 stage=scan action={} object_type={} path={}"
                        + " size_bytes={} mtime_ms={} rule={} reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={}",
                action,
                lower(ruleId.objectType().name()),
                file.path(),
                file.size(),
                file.modificationTime(),
                ruleId,
                reasonCode,
                result,
                scope.database(),
                scope.table(),
                scope.tableId(),
                scope.partitionId(),
                scope.bucketId(),
                dryRun,
                retryable,
                actionRequired);
    }

    public void logDirDeleted(FsPath dir) {
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "dir_deleted")
                        .objectType("directory")
                        .path(dir.toString())
                        .build();
        emit(event, "action=dir_deleted path={}", dir);
    }

    public void logWouldDeleteDir(FsPath dir) {
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "would_delete_dir")
                        .objectType("directory")
                        .path(dir.toString())
                        .build();
        emit(event, "action=would_delete_dir path={}", dir);
    }

    public void logWouldDeleteDirectory(
            FsPath dir, long modificationTime, ScopeIdentity scope, boolean dryRun) {
        logDirectoryAction(
                "would_delete",
                dir,
                modificationTime,
                scope,
                "empty_and_older_than_cutoff",
                "planned",
                dryRun,
                false,
                false);
    }

    public void logDeletedDirectory(
            FsPath dir, long modificationTime, ScopeIdentity scope, boolean dryRun) {
        logDirectoryAction(
                "deleted",
                dir,
                modificationTime,
                scope,
                "empty_and_older_than_cutoff",
                "deleted",
                dryRun,
                false,
                false);
    }

    public void logSkippedDirectory(
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        logDirectoryAction(
                "skip_directory",
                dir,
                modificationTime,
                scope,
                reasonCode,
                "skipped",
                dryRun,
                retryable,
                actionRequired);
    }

    public void logDirectoryDeleteFailed(
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            boolean dryRun,
            boolean retryable) {
        logDirectoryAction(
                "delete_failed",
                dir,
                modificationTime,
                scope,
                reasonCode,
                "failed",
                dryRun,
                retryable,
                true);
    }

    private void logDirectoryAction(
            String action,
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        AuditEvent event =
                scopeEvent(AuditSeverity.INFO, AuditStage.SCAN, action, scope)
                        .objectType("directory")
                        .path(dir.toString())
                        .sizeBytes(0L)
                        .mtimeMs(modificationTime)
                        .rule("empty-directory")
                        .reasonCode(reasonCode)
                        .result(result)
                        .flags(actionFlags(dryRun, retryable, actionRequired))
                        .build();
        emit(
                event,
                "audit_version=1 stage=scan action={} object_type=directory path={}"
                        + " size_bytes=0 mtime_ms={} rule=empty-directory reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={}",
                action,
                dir,
                modificationTime,
                reasonCode,
                result,
                scope.database(),
                scope.table(),
                nullable(scope.tableId()),
                nullable(scope.partitionId()),
                nullable(scope.bucketId()),
                dryRun,
                retryable,
                actionRequired);
    }

    public void logSkipUnknown(FsPath path, RuleId ruleId) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCAN, "skip_unknown")
                        .path(path.toString())
                        .rule(ruleId.toString())
                        .build();
        emit(event, "action=skip_unknown rule={} path={}", ruleId, path);
    }

    /** Emits at most one actionable unavailable-mtime sample for this scan subtask. */
    public void logMtimeUnavailableOnce(
            ScopeIdentity scope,
            CleanupObjectType objectType,
            String entryKind,
            String sampleName) {
        if (mtimeUnavailableSampleLogged) {
            return;
        }
        mtimeUnavailableSampleLogged = true;
        String sanitizedSampleName = sanitizeSampleName(sampleName);
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("entry_kind", entryKind);
        dimensions.put("sample_name", sanitizedSampleName);
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("action_required", true);
        AuditEvent event =
                scopeEvent(AuditSeverity.ERROR, AuditStage.SCAN, "mtime_unavailable", scope)
                        .objectType(lower(objectType.name()))
                        .dimensions(dimensions)
                        .flags(flags)
                        .build();
        emit(
                event,
                "audit_version=1 stage=scan action=mtime_unavailable"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " object_type={} entry_kind={} sample_name={} action_required=true",
                scope.database(),
                scope.table(),
                nullable(scope.tableId()),
                nullable(scope.partitionId()),
                nullable(scope.bucketId()),
                lower(objectType.name()),
                entryKind,
                sanitizedSampleName);
    }

    private static String sanitizeSampleName(String value) {
        StringBuilder sanitized = new StringBuilder(Math.min(value.length(), 128));
        for (int i = 0; i < value.length() && sanitized.length() < 128; i++) {
            char c = value.charAt(i);
            boolean allowed =
                    (c >= 'a' && c <= 'z')
                            || (c >= 'A' && c <= 'Z')
                            || (c >= '0' && c <= '9')
                            || c == '.'
                            || c == '_'
                            || c == '-';
            sanitized.append(allowed ? c : '_');
        }
        return sanitized.length() == 0 ? "empty" : sanitized.toString();
    }

    public void logBucketAborted(String bucketStr, String reason) {
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("bucket", bucketStr);
        AuditEvent event =
                newEvent(AuditSeverity.ERROR, AuditStage.SCOPE, "bucket_aborted")
                        .reasonCode(reason)
                        .dimensions(dimensions)
                        .build();
        emit(event, "action=bucket_aborted bucket={} reason={}", bucketStr, reason);
    }

    /** Skip an entire database during scope enumeration due to listTables failure. */
    public void logSkipDb(String dbName, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_db")
                        .database(dbName)
                        .reasonCode(reason)
                        .build();
        emit(event, "action=skip_db reason={} db={}", reason, dbName);
    }

    /** Skip a single table during scope enumeration due to getTableInfo or RPC failure. */
    public void logSkipTable(String dbName, String tableName, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_table")
                        .database(dbName)
                        .table(tableName)
                        .reasonCode(reason)
                        .build();
        emit(event, "action=skip_table reason={} db={} table={}", reason, dbName, tableName);
    }

    public void logSkipPartitionList(String dbName, String tableName, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_partition_list")
                        .database(dbName)
                        .table(tableName)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_partition_list reason={} db={} table={}",
                reason,
                dbName,
                tableName);
    }

    public void logSkipKvTarget(long tableId, Long partitionId, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_kv_target reason={} table_id={} partition_id={}",
                reason,
                tableId,
                partitionId);
    }

    public void logSkipKvBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_bucket")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_kv_bucket reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    /**
     * Skip shared SST cleanup for a single bucket because the active set could not be determined
     * (metadata read failure). The bucket's snap-private and log cleanup proceed normally.
     */
    public void logSkipKvSharedSst(long tableId, Long partitionId, int bucketId, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_shared_sst")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_kv_shared_sst reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    public void logSkipLogTarget(long tableId, Long partitionId, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_log_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_log_target reason={} table_id={} partition_id={}",
                reason,
                tableId,
                partitionId);
    }

    public void logSkipLogBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_log_bucket")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode(reason)
                        .build();
        emit(
                event,
                "action=skip_log_bucket reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    public void logSkipOrphanTable(FsPath dir, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_orphan_table")
                        .objectType("directory")
                        .path(dir.toString())
                        .reasonCode(reason)
                        .build();
        emit(event, "action=skip_orphan_table reason={} path={}", reason, dir);
    }

    public void logSkipOrphanTableScan(String dbName, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_orphan_table_scan")
                        .database(dbName)
                        .reasonCode(reason)
                        .build();
        emit(event, "action=skip_orphan_table_scan reason={} db={}", reason, dbName);
    }

    public void logSkipOrphanPartition(FsPath dir, String reason) {
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_orphan_partition")
                        .objectType("directory")
                        .path(dir.toString())
                        .reasonCode(reason)
                        .build();
        emit(event, "action=skip_orphan_partition reason={} path={}", reason, dir);
    }

    public void logSkipBucketOutOfScope(long tableId, Long partitionId, String resolvedRoot) {
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("resolved_root", resolvedRoot);
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_bucket_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode("out-of-scope-root")
                        .dimensions(dimensions)
                        .build();
        emit(
                event,
                "action=skip_bucket_target reason=out-of-scope-root table_id={} partition_id={}"
                        + " resolved_root={}",
                tableId,
                partitionId,
                resolvedRoot);
    }

    public void logSummary(
            long scanned,
            long deletedFiles,
            long emptyDirsRemoved,
            long deleteFailures,
            long bytesReclaimed,
            boolean dryRun) {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("scanned", scanned);
        metrics.put("deleted_total", deletedFiles + emptyDirsRemoved);
        metrics.put("deleted_files", deletedFiles);
        metrics.put("empty_dirs_removed", emptyDirsRemoved);
        metrics.put("delete_failures", deleteFailures);
        metrics.put("bytes_reclaimed", bytesReclaimed);
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("dry_run", dryRun);
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "summary")
                        .metrics(metrics)
                        .flags(flags)
                        .build();
        emit(
                event,
                "action=summary scanned={} deleted_total={} deleted_files={} empty_dirs_removed={}"
                        + " delete_failures={} bytes_reclaimed={} dry_run={}",
                scanned,
                deletedFiles + emptyDirsRemoved,
                deletedFiles,
                emptyDirsRemoved,
                deleteFailures,
                bytesReclaimed,
                dryRun);
    }

    public void logTableRuleSummary(
            ScopeIdentity scope,
            CleanupObjectType objectType,
            RuleDecisionCounters counters,
            boolean dryRun) {
        logRuleDecisions(
                "table_rule_summary",
                "database="
                        + scope.database()
                        + " table="
                        + scope.table()
                        + " table_id="
                        + nullable(scope.tableId())
                        + " object_type="
                        + lower(objectType.name()),
                scope,
                objectType,
                counters,
                dryRun);
    }

    public void logGlobalRuleSummary(
            CleanupObjectType objectType, RuleDecisionCounters counters, boolean dryRun) {
        logRuleDecisions(
                "summary_by_rule",
                "scope=global object_type=" + lower(objectType.name()),
                ScopeIdentity.global(),
                objectType,
                counters,
                dryRun);
    }

    public void logCoverageSummary(
            Map<SkipReasonCode, Long> skipped,
            long metadataFailures,
            long mtimeUnavailableFiles,
            long mtimeUnavailableBytes,
            long mtimeUnavailableDirs,
            boolean coverageComplete,
            boolean dryRun) {
        long noRemoteManifest = skipped.getOrDefault(SkipReasonCode.NO_REMOTE_MANIFEST, 0L);
        long emptyActiveSet = skipped.getOrDefault(SkipReasonCode.EMPTY_KV_ACTIVE_SET, 0L);
        long directoryListFailed = skipped.getOrDefault(SkipReasonCode.DIRECTORY_LIST_FAILED, 0L);
        long rpcFailed = skipped.getOrDefault(SkipReasonCode.RPC_ERROR, 0L);
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("no_remote_manifest_targets", noRemoteManifest);
        metrics.put("empty_active_set_targets", emptyActiveSet);
        metrics.put("metadata_read_failed_targets", metadataFailures);
        metrics.put("directory_list_failed_targets", directoryListFailed);
        metrics.put("rpc_failed_targets", rpcFailed);
        metrics.put("mtime_unavailable_files", mtimeUnavailableFiles);
        metrics.put("mtime_unavailable_bytes", mtimeUnavailableBytes);
        metrics.put("mtime_unavailable_dirs", mtimeUnavailableDirs);
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("complete", coverageComplete);
        flags.put("action_required", !coverageComplete);
        flags.put("dry_run", dryRun);
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "coverage_summary")
                        .metrics(metrics)
                        .flags(flags)
                        .build();
        emit(
                event,
                "action=coverage_summary no_remote_manifest_targets={}"
                        + " empty_active_set_targets={} metadata_read_failed_targets={}"
                        + " directory_list_failed_targets={} rpc_failed_targets={}"
                        + " mtime_unavailable_files={} mtime_unavailable_bytes={}"
                        + " mtime_unavailable_dirs={} complete={} action_required={} dry_run={}",
                noRemoteManifest,
                emptyActiveSet,
                metadataFailures,
                directoryListFailed,
                rpcFailed,
                mtimeUnavailableFiles,
                mtimeUnavailableBytes,
                mtimeUnavailableDirs,
                coverageComplete,
                !coverageComplete,
                dryRun);
    }

    public void logAuditIntegrity(CleanupSummary summary) {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("inconsistent_object_types", summary.inconsistentObjectTypes());
        metrics.put("inconsistent_scopes", summary.inconsistentScopes());
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("rule_counters_consistent", summary.ruleCountersConsistent());
        flags.put("coverage_complete", summary.coverageComplete());
        flags.put("dry_run_counters_consistent", summary.dryRunCountersConsistent());
        flags.put("dry_run", summary.dryRun());
        AuditEvent event =
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "audit_integrity")
                        .metrics(metrics)
                        .flags(flags)
                        .build();
        emit(
                event,
                "action=audit_integrity rule_counters_consistent={} coverage_complete={}"
                        + " dry_run_counters_consistent={} inconsistent_object_types={}"
                        + " inconsistent_scopes={} dry_run={}",
                summary.ruleCountersConsistent(),
                summary.coverageComplete(),
                summary.dryRunCountersConsistent(),
                summary.inconsistentObjectTypes(),
                summary.inconsistentScopes(),
                summary.dryRun());
    }

    private void logRuleDecisions(
            String action,
            String legacyDimensions,
            ScopeIdentity scope,
            CleanupObjectType objectType,
            RuleDecisionCounters counters,
            boolean dryRun) {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("scanned_files", counters.scannedFiles());
        metrics.put("scanned_bytes", counters.scannedBytes());
        metrics.put("keep_active_files", counters.keepActiveFiles());
        metrics.put("keep_active_bytes", counters.keepActiveBytes());
        metrics.put("newer_than_cutoff_files", counters.newerThanCutoffFiles());
        metrics.put("newer_than_cutoff_bytes", counters.newerThanCutoffBytes());
        metrics.put("mtime_unavailable_files", counters.mtimeUnavailableFiles());
        metrics.put("mtime_unavailable_bytes", counters.mtimeUnavailableBytes());
        metrics.put("unknown_file_type_files", counters.unknownFileTypeFiles());
        metrics.put("unknown_file_type_bytes", counters.unknownFileTypeBytes());
        metrics.put("candidate_files", counters.candidateFiles());
        metrics.put("candidate_bytes", counters.candidateBytes());
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("dry_run", dryRun);
        AuditEvent.Builder builder =
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, action)
                        .scopeKind(lower(scope.kind().name()))
                        .objectType(lower(objectType.name()))
                        .metrics(metrics)
                        .flags(flags);
        if (scope.kind() != ScopeKind.GLOBAL) {
            builder.database(scope.database()).table(scope.table()).tableId(scope.tableId());
        }
        AuditEvent event = builder.build();
        emit(
                event,
                "action={} {} scanned_files={} scanned_bytes={} keep_active_files={}"
                        + " keep_active_bytes={} newer_than_cutoff_files={}"
                        + " newer_than_cutoff_bytes={} mtime_unavailable_files={}"
                        + " mtime_unavailable_bytes={} unknown_file_type_files={}"
                        + " unknown_file_type_bytes={} candidate_files={} candidate_bytes={}"
                        + " dry_run={}",
                action,
                legacyDimensions,
                counters.scannedFiles(),
                counters.scannedBytes(),
                counters.keepActiveFiles(),
                counters.keepActiveBytes(),
                counters.newerThanCutoffFiles(),
                counters.newerThanCutoffBytes(),
                counters.mtimeUnavailableFiles(),
                counters.mtimeUnavailableBytes(),
                counters.unknownFileTypeFiles(),
                counters.unknownFileTypeBytes(),
                counters.candidateFiles(),
                counters.candidateBytes(),
                dryRun);
    }

    private AuditEvent.Builder newEvent(AuditSeverity severity, AuditStage stage, String action) {
        long eventTimeMillis = clock.getAsLong();
        String eventId = eventIdSupplier.get();
        return AuditEvent.builder()
                .eventId(eventId)
                .runId(context.getRunId())
                .eventTimeMillis(eventTimeMillis)
                .severity(severity)
                .stage(stage)
                .action(action)
                .operatorName(context.getOperatorName())
                .subtaskIndex(context.getSubtaskIndex())
                .attemptNumber(context.getAttemptNumber());
    }

    private AuditEvent.Builder scopeEvent(
            AuditSeverity severity, AuditStage stage, String action, ScopeIdentity scope) {
        return newEvent(severity, stage, action)
                .database(scope.database())
                .table(scope.table())
                .tableId(scope.tableId())
                .partitionId(scope.partitionId())
                .bucketId(scope.bucketId())
                .scopeKind(lower(scope.kind().name()));
    }

    private void emit(
            AuditEvent event, String legacyTemplateWithoutTimestamp, Object... legacyArgs) {
        String template =
                legacyTemplateWithoutTimestamp
                        + " ts={} run_id={} event_id={} operator={} subtask={} attempt={}";
        Object[] args = Arrays.copyOf(legacyArgs, legacyArgs.length + 6);
        args[legacyArgs.length] = Instant.ofEpochMilli(event.getEventTimeMillis());
        args[legacyArgs.length + 1] = event.getRunId();
        args[legacyArgs.length + 2] = event.getEventId();
        args[legacyArgs.length + 3] = nullable(event.getOperatorName());
        args[legacyArgs.length + 4] = nullable(event.getSubtaskIndex());
        args[legacyArgs.length + 5] = nullable(event.getAttemptNumber());
        switch (event.getSeverity()) {
            case INFO:
                AUDIT.info(template, args);
                break;
            case WARN:
                AUDIT.warn(template, args);
                break;
            case ERROR:
                AUDIT.error(template, args);
                break;
            default:
                throw new IllegalArgumentException("severity");
        }
        if (reporterRuntime != null) {
            reporterRuntime.report(event);
        }
    }

    private static Map<String, Boolean> actionFlags(
            boolean dryRun, boolean retryable, boolean actionRequired) {
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("dry_run", dryRun);
        flags.put("retryable", retryable);
        flags.put("action_required", actionRequired);
        return flags;
    }

    private static String nullable(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static String lower(String value) {
        return value.toLowerCase(Locale.ROOT);
    }
}
