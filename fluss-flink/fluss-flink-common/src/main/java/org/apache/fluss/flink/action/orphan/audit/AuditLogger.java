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
import org.apache.fluss.flink.action.orphan.job.CleanStats;
import org.apache.fluss.flink.action.orphan.job.CleanupCounters;
import org.apache.fluss.flink.action.orphan.job.CleanupReport;
import org.apache.fluss.flink.action.orphan.job.ScopePlanStats;
import org.apache.fluss.flink.action.orphan.job.TableCleanupSummary;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Structured audit log writer for the orphan files cleanup action.
 *
 * <p>The dedicated logger name {@code fluss.orphan.audit} can be routed to a separate sink (e.g.
 * SLS) by deployment-specific log4j configuration.
 */
@Internal
public final class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("fluss.orphan.audit");

    /**
     * Formats cutoff epoch-ms back to the {@code yyyy-MM-dd HH:mm:ss} CLI grammar in the server's
     * local zone, so the audit line and the original {@code --older-than} value can be compared
     * verbatim.
     */
    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * One-shot startup event recording the frozen file cutoff that drives this run's deletion
     * decisions. Emitted before any other audit line so log readers can recover the exact threshold
     * without having to re-parse the original CLI arguments.
     */
    public void logCutoff(String runId, long olderThanMillis) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scope action=cutoff"
                        + " older_than_iso={} older_than_ms={} ts={}",
                runId,
                CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis)),
                olderThanMillis,
                Instant.now());
    }

    /** One-shot, non-secret execution configuration at normal INFO level. */
    public void logRunStart(String runId, OrphanCleanConfig config) {
        String scope = config.allDatabases() ? "all-databases" : config.database().get();
        if (config.table().isPresent()) {
            scope = scope + "." + config.table().get();
        }
        AUDIT.info(
                "audit_version=1 run_id={} stage=scope action=run_start"
                        + " scope={} older_than_ms={} dry_run={} parallelism={}"
                        + " remote_fs_rate_limit={} allow_delete_manifest={}"
                        + " allow_clean_orphan_tables={} allow_clean_orphan_partitions={}"
                        + " progress_log_interval_ms={} post_run_wait_ms={} ts={}",
                runId,
                scope,
                config.olderThanMillis(),
                config.dryRun(),
                config.parallelism().isPresent() ? config.parallelism().get() : "default",
                config.remoteFsOpRateLimitPerSecond(),
                config.allowDeleteManifest(),
                config.allowCleanOrphanTables(),
                config.allowCleanOrphanPartitions(),
                config.progressLogInterval().toMillis(),
                config.postRunWait().toMillis(),
                Instant.now());
    }

    /** One-shot aggregate of discovered scope, expected skips, and emitted cleanup work. */
    public void logScopePlan(String runId, ScopePlanStats stats) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scope action=scope_plan"
                        + " databases={} tables={} partitions={} discovered_buckets={}"
                        + " bucket_tasks={} orphan_dir_tasks={} skipped_no_remote_manifest={}"
                        + " skipped_empty_kv_active_set={} skipped_out_of_scope_root={}"
                        + " metadata_failures={} ts={}",
                runId,
                stats.databases(),
                stats.tables(),
                stats.partitions(),
                stats.discoveredBuckets(),
                stats.bucketTasks(),
                stats.orphanDirTasks(),
                stats.skippedNoRemoteManifestCount(),
                stats.skippedEmptyKvActiveSetCount(),
                stats.skippedOutOfScopeRootCount(),
                stats.metadataFailures(),
                Instant.now());
    }

    public void logScanStart(
            String runId,
            boolean dryRun,
            int subtask,
            int parallelism,
            int attempt,
            long progressIntervalMillis) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scan action=scan_start dry_run={}"
                        + " subtask={} parallelism={} attempt={} progress_interval_ms={} ts={}",
                runId,
                dryRun,
                subtask,
                parallelism,
                attempt,
                progressIntervalMillis,
                Instant.now());
    }

    public void logScanProgress(
            String runId,
            boolean dryRun,
            int subtask,
            int parallelism,
            int attempt,
            long tasksCompleted,
            CleanupCounters counters,
            long elapsedMillis) {
        logScanCounters(
                "scan_progress",
                runId,
                dryRun,
                subtask,
                parallelism,
                attempt,
                tasksCompleted,
                counters,
                elapsedMillis);
    }

    public void logScanSubtaskSummary(
            String runId,
            boolean dryRun,
            int subtask,
            int parallelism,
            int attempt,
            long tasksCompleted,
            CleanupCounters counters,
            long elapsedMillis) {
        logScanCounters(
                "scan_subtask_summary",
                runId,
                dryRun,
                subtask,
                parallelism,
                attempt,
                tasksCompleted,
                counters,
                elapsedMillis);
    }

    private void logScanCounters(
            String action,
            String runId,
            boolean dryRun,
            int subtask,
            int parallelism,
            int attempt,
            long tasksCompleted,
            CleanupCounters counters,
            long elapsedMillis) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scan action={} dry_run={} subtask={}"
                        + " parallelism={} attempt={} tasks_completed={} scanned_files={}"
                        + " planned_files={} planned_dirs={} planned_bytes={} deleted_files={}"
                        + " empty_dirs_removed={} delete_failures={} bytes_reclaimed={}"
                        + " elapsed_ms={} ts={}",
                runId,
                action,
                dryRun,
                subtask,
                parallelism,
                attempt,
                tasksCompleted,
                counters.scannedFiles(),
                counters.plannedFiles(),
                counters.plannedDirs(),
                counters.plannedBytes(),
                counters.deletedFiles(),
                counters.emptyDirsRemoved(),
                counters.deleteFailures(),
                counters.bytesReclaimed(),
                elapsedMillis,
                Instant.now());
    }

    public void logDeleted(FsPath path, RuleId ruleId, boolean ok) {
        AUDIT.info("action=deleted rule={} path={} ok={} ts={}", ruleId, path, ok, Instant.now());
    }

    public void logWouldDelete(FsPath path, RuleId ruleId) {
        AUDIT.info("action=would_delete rule={} path={} ts={}", ruleId, path, Instant.now());
    }

    public void logWouldDelete(String runId, FileMeta file, RuleId ruleId, ScopeIdentity scope) {
        logObjectAction(
                "would_delete",
                runId,
                file,
                ruleId,
                scope,
                "older_than_cutoff",
                "planned",
                true,
                false,
                false);
    }

    public void logDeleted(String runId, FileMeta file, RuleId ruleId, ScopeIdentity scope) {
        logObjectAction(
                "deleted",
                runId,
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
            String runId,
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            boolean retryable) {
        logObjectAction(
                "delete_failed",
                runId,
                file,
                ruleId,
                scope,
                reasonCode,
                "failed",
                false,
                retryable,
                true);
    }

    private void logObjectAction(
            String action,
            String runId,
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scan action={} object_type={} path={}"
                        + " size_bytes={} mtime_ms={} rule={} reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={} ts={}",
                runId,
                action,
                ruleId.objectType().name().toLowerCase(Locale.ROOT),
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
                actionRequired,
                Instant.now());
    }

    public void logDirDeleted(FsPath dir) {
        AUDIT.info("action=dir_deleted path={} ts={}", dir, Instant.now());
    }

    public void logWouldDeleteDir(FsPath dir) {
        AUDIT.info("action=would_delete_dir path={} ts={}", dir, Instant.now());
    }

    public void logWouldDeleteDirectory(
            String runId, FsPath dir, long modificationTime, ScopeIdentity scope, boolean dryRun) {
        logDirectoryAction(
                runId,
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
            String runId, FsPath dir, long modificationTime, ScopeIdentity scope, boolean dryRun) {
        logDirectoryAction(
                runId,
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
            String runId,
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        logDirectoryAction(
                runId,
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
            String runId,
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            boolean dryRun,
            boolean retryable) {
        logDirectoryAction(
                runId,
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

    private static void logDirectoryAction(
            String runId,
            String action,
            FsPath dir,
            long modificationTime,
            ScopeIdentity scope,
            String reasonCode,
            String result,
            boolean dryRun,
            boolean retryable,
            boolean actionRequired) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=scan action={} object_type=directory"
                        + " path={} size_bytes=0 mtime_ms={} rule=empty-directory reason_code={}"
                        + " result={} {} dry_run={} retryable={} action_required={} ts={}",
                runId,
                action,
                dir,
                modificationTime,
                reasonCode,
                result,
                objectScopeFields(scope),
                dryRun,
                retryable,
                actionRequired,
                Instant.now());
    }

    public void logSkipUnknown(FsPath path, RuleId ruleId) {
        AUDIT.warn("action=skip_unknown rule={} path={} ts={}", ruleId, path, Instant.now());
    }

    public void logBucketAborted(String bucketStr, String reason) {
        AUDIT.error(
                "action=bucket_aborted bucket={} reason={} ts={}",
                bucketStr,
                reason,
                Instant.now());
    }

    /** Skip an entire database during scope enumeration due to listTables failure. */
    public void logSkipDb(String dbName, String reason) {
        AUDIT.warn("action=skip_db reason={} db={} ts={}", reason, dbName, Instant.now());
    }

    /** Skip a single table during scope enumeration due to getTableInfo or RPC failure. */
    public void logSkipTable(String dbName, String tableName, String reason) {
        AUDIT.warn(
                "action=skip_table reason={} db={} table={} ts={}",
                reason,
                dbName,
                tableName,
                Instant.now());
    }

    /**
     * Skip listPartitionInfos for a table due to RPC failure (both active-partition cleanup and
     * orphan-partition scan are suppressed for this table).
     */
    public void logSkipPartitionList(String dbName, String tableName, String reason) {
        AUDIT.warn(
                "action=skip_partition_list reason={} db={} table={} ts={}",
                reason,
                dbName,
                tableName,
                Instant.now());
    }

    /**
     * Skip KV cleanup for one (tableId, partitionId) target — emitted when {@code ListKvSnapshots}
     * fails after retries. {@code partitionId} is null for non-partitioned tables.
     */
    public void logSkipKvTarget(long tableId, Long partitionId, String reason) {
        AUDIT.warn(
                "action=skip_kv_target reason={} table_id={} partition_id={} ts={}",
                reason,
                tableId,
                partitionId,
                Instant.now());
    }

    /**
     * Skip KV cleanup for a single bucket whose {@code ListKvSnapshots} response carried no
     * active-snapshot entries. Empty per-bucket active set is treated as "cannot prove what is
     * active" and the bucket is skipped to avoid mis-deletion.
     */
    public void logSkipKvBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AUDIT.warn(
                "action=skip_kv_bucket reason={} table_id={} partition_id={} bucket_id={} ts={}",
                reason,
                tableId,
                partitionId,
                bucketId,
                Instant.now());
    }

    /**
     * Skip log cleanup for one (tableId, partitionId) target — emitted when {@code
     * ListRemoteLogManifests} fails after retries. {@code partitionId} is null for non-partitioned
     * tables.
     */
    public void logSkipLogTarget(long tableId, Long partitionId, String reason) {
        AUDIT.warn(
                "action=skip_log_target reason={} table_id={} partition_id={} ts={}",
                reason,
                tableId,
                partitionId,
                Instant.now());
    }

    /**
     * Skip log cleanup for a single bucket whose remote manifest was not returned by the {@code
     * ListRemoteLogManifests} RPC (the bucket has not yet committed any remote manifest).
     */
    public void logSkipLogBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AUDIT.warn(
                "action=skip_log_bucket reason={} table_id={} partition_id={} bucket_id={} ts={}",
                reason,
                tableId,
                partitionId,
                bucketId,
                Instant.now());
    }

    /** Default-conservative skip of an orphan-table dir (opt-in flag not set). */
    public void logSkipOrphanTable(FsPath dir, String reason) {
        AUDIT.info("action=skip_orphan_table reason={} path={} ts={}", reason, dir, Instant.now());
    }

    /**
     * Skip the orphan-table scan for a database whose table-info set is incomplete (e.g. {@code
     * --table} single-table mode, or {@code listTables}/{@code getTableInfo} failures left holes in
     * the active table id set). Distinct from {@link #logSkipDb}, which means the whole database
     * scope is dropped.
     */
    public void logSkipOrphanTableScan(String dbName, String reason) {
        AUDIT.warn(
                "action=skip_orphan_table_scan reason={} db={} ts={}",
                reason,
                dbName,
                Instant.now());
    }

    /** Default-conservative skip of an orphan-partition dir (opt-in flag not set). */
    public void logSkipOrphanPartition(FsPath dir, String reason) {
        AUDIT.info(
                "action=skip_orphan_partition reason={} path={} ts={}", reason, dir, Instant.now());
    }

    /** Skip a bucket target because its metadata-resolved root is outside cluster config. */
    public void logSkipBucketOutOfScope(long tableId, Long partitionId, String resolvedRoot) {
        AUDIT.info(
                "action=skip_bucket_target reason=out-of-scope-root table_id={} partition_id={}"
                        + " resolved_root={} ts={}",
                tableId,
                partitionId,
                resolvedRoot,
                Instant.now());
    }

    /**
     * Final summary event emitted once at the end of a run, carrying the headline counters that
     * operators query most often ("how many files were removed and how much space was reclaimed").
     * Routed through the dedicated audit logger so the result is queryable from the same sink as
     * the per-file {@code action=deleted} / {@code action=skip_*} lines.
     */
    public void logSummary(CleanStats stats, boolean dryRun) {
        AUDIT.info(
                "action=summary scanned_files={} planned_files={} planned_dirs={} planned_bytes={}"
                        + " planned_size={} deleted_total={} deleted_files={} empty_dirs_removed={}"
                        + " delete_failures={} bytes_reclaimed={} reclaimed_size={} dry_run={} ts={}",
                stats.scannedFiles(),
                stats.plannedFiles(),
                stats.plannedDirs(),
                stats.plannedBytes(),
                formatBytes(stats.plannedBytes()),
                stats.deletedFiles() + stats.emptyDirsRemoved(),
                stats.deletedFiles(),
                stats.emptyDirsRemoved(),
                stats.deleteFailures(),
                stats.bytesReclaimed(),
                formatBytes(stats.bytesReclaimed()),
                dryRun,
                Instant.now());
    }

    /** Emits a complete pre-aggregated report that can be inspected without log-side reduction. */
    public void logReport(
            String runId, String stage, String finalAction, CleanupReport report, boolean dryRun) {
        List<TableCleanupSummary> tables = new ArrayList<>(report.tables().values());
        tables.sort(
                Comparator.comparing((TableCleanupSummary table) -> table.scope().database())
                        .thenComparing(table -> table.scope().table())
                        .thenComparing(table -> table.scope().kind().name())
                        .thenComparing(
                                table ->
                                        table.scope().tableId() == null
                                                ? Long.MIN_VALUE
                                                : table.scope().tableId()));
        CleanupCounters tableTotal = CleanupCounters.empty();
        long tableTasksPlanned = 0L;
        long tableMetadataFailures = 0L;
        for (TableCleanupSummary table : tables) {
            ScopeIdentity scope = table.scope();
            tableTotal = tableTotal.add(table.counters());
            tableTasksPlanned += table.tasksPlanned();
            tableMetadataFailures += table.metadataFailures();
            logCounters(
                    runId,
                    stage,
                    "table_summary",
                    scopeFields(scope)
                            + " tasks_planned="
                            + table.tasksPlanned()
                            + " metadata_failures="
                            + table.metadataFailures(),
                    table.counters(),
                    dryRun);
            for (Map.Entry<CleanupObjectType, CleanupCounters> entry :
                    table.byObjectType().entrySet()) {
                logCounters(
                        runId,
                        stage,
                        "table_object_summary",
                        scopeFields(scope) + " object_type=" + lower(entry.getKey().name()),
                        entry.getValue(),
                        dryRun);
            }
            for (Map.Entry<SkipReasonCode, Long> entry : table.bySkipReason().entrySet()) {
                logReason(
                        runId,
                        stage,
                        "table_skip_summary",
                        scopeFields(scope)
                                + " tasks_planned="
                                + table.tasksPlanned()
                                + " metadata_failures="
                                + table.metadataFailures(),
                        entry.getKey(),
                        entry.getValue(),
                        dryRun);
            }
        }
        for (Map.Entry<String, CleanupCounters> entry :
                new TreeMap<>(report.databases()).entrySet()) {
            logCounters(
                    runId,
                    stage,
                    "database_summary",
                    "database=" + entry.getKey(),
                    entry.getValue(),
                    dryRun);
        }
        for (Map.Entry<CleanupObjectType, CleanupCounters> entry :
                report.byObjectType().entrySet()) {
            logCounters(
                    runId,
                    stage,
                    "summary_by_type",
                    "object_type=" + lower(entry.getKey().name()),
                    entry.getValue(),
                    dryRun);
        }
        for (Map.Entry<SkipReasonCode, Long> entry : report.bySkipReason().entrySet()) {
            logReason(
                    runId,
                    stage,
                    "summary_by_reason",
                    "scope=global",
                    entry.getKey(),
                    entry.getValue(),
                    dryRun);
        }
        AUDIT.info(
                "audit_version=1 run_id={} stage={} action=audit_integrity"
                        + " global_equals_table_sum={} plan_equals_table_sum={} table_count={}"
                        + " dry_run={} ts={}",
                runId,
                stage,
                sameCounters(report.global(), tableTotal),
                report.tasksPlanned() == tableTasksPlanned
                        && report.metadataFailures() == tableMetadataFailures,
                tables.size(),
                dryRun,
                Instant.now());
        logCounters(
                runId,
                stage,
                finalAction,
                "scope=global tasks_planned="
                        + report.tasksPlanned()
                        + " metadata_failures="
                        + report.metadataFailures(),
                report.global(),
                dryRun);
    }

    public void logRetentionWaitStart(String runId, long waitMillis) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=aggregate action=retention_wait_start"
                        + " wait_ms={} ts={}",
                runId,
                waitMillis,
                Instant.now());
    }

    public void logRetentionWaitEnd(String runId, long waitMillis) {
        AUDIT.info(
                "audit_version=1 run_id={} stage=aggregate action=retention_wait_end"
                        + " wait_ms={} ts={}",
                runId,
                waitMillis,
                Instant.now());
    }

    public void logRetentionWaitStart(long waitMillis) {
        AUDIT.info("action=retention_wait_start wait_ms={} ts={}", waitMillis, Instant.now());
    }

    public void logRetentionWaitEnd(long waitMillis) {
        AUDIT.info("action=retention_wait_end wait_ms={} ts={}", waitMillis, Instant.now());
    }

    private static void logCounters(
            String runId,
            String stage,
            String action,
            String dimensions,
            CleanupCounters counters,
            boolean dryRun) {
        AUDIT.info(
                "audit_version=1 run_id={} stage={} action={} {}"
                        + " scanned_files={} planned_files={} planned_dirs={} planned_bytes={}"
                        + " planned_size={} deleted_total={} deleted_files={}"
                        + " empty_dirs_removed={} delete_failures={} bytes_reclaimed={}"
                        + " reclaimed_size={} dry_run={} ts={}",
                runId,
                stage,
                action,
                dimensions,
                counters.scannedFiles(),
                counters.plannedFiles(),
                counters.plannedDirs(),
                counters.plannedBytes(),
                formatBytes(counters.plannedBytes()),
                counters.deletedFiles() + counters.emptyDirsRemoved(),
                counters.deletedFiles(),
                counters.emptyDirsRemoved(),
                counters.deleteFailures(),
                counters.bytesReclaimed(),
                formatBytes(counters.bytesReclaimed()),
                dryRun,
                Instant.now());
    }

    private static void logReason(
            String runId,
            String stage,
            String action,
            String dimensions,
            SkipReasonCode reason,
            long count,
            boolean dryRun) {
        AUDIT.info(
                "audit_version=1 run_id={} stage={} action={} {} reason_code={}"
                        + " category={} count={} retryable={} action_required={} dry_run={} ts={}",
                runId,
                stage,
                action,
                dimensions,
                lower(reason.name()),
                lower(reason.category().name()),
                count,
                reason.retryable(),
                reason.actionRequired(),
                dryRun,
                Instant.now());
    }

    private static String scopeFields(ScopeIdentity scope) {
        return "scope_kind="
                + lower(scope.kind().name())
                + " database="
                + scope.database()
                + " table="
                + scope.table()
                + " table_id="
                + nullable(scope.tableId());
    }

    private static String objectScopeFields(ScopeIdentity scope) {
        return scopeFields(scope)
                + " partition_id="
                + nullable(scope.partitionId())
                + " bucket_id="
                + nullable(scope.bucketId());
    }

    private static String nullable(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static String lower(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private static boolean sameCounters(CleanupCounters left, CleanupCounters right) {
        return left.scannedFiles() == right.scannedFiles()
                && left.plannedFiles() == right.plannedFiles()
                && left.plannedDirs() == right.plannedDirs()
                && left.plannedBytes() == right.plannedBytes()
                && left.deletedFiles() == right.deletedFiles()
                && left.emptyDirsRemoved() == right.emptyDirsRemoved()
                && left.deleteFailures() == right.deleteFailures()
                && left.bytesReclaimed() == right.bytesReclaimed();
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024L) {
            return String.format(Locale.ROOT, "%.2f KiB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024L * 1024L) {
            return String.format(Locale.ROOT, "%.2f MiB", bytes / (1024.0 * 1024.0));
        }
        return String.format(Locale.ROOT, "%.2f GiB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
