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
import java.util.Locale;
import java.util.Map;

/**
 * Structured audit log writer for the orphan files cleanup action.
 *
 * <p>The dedicated logger name {@code fluss.orphan.audit} can be routed to a separate sink (e.g.
 * SLS) by deployment-specific log4j configuration.
 */
@Internal
public final class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("fluss.orphan.audit");

    private boolean mtimeUnavailableSampleLogged;

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
    public void logCutoff(long olderThanMillis) {
        AUDIT.info(
                "action=cutoff older_than_iso={} older_than_ms={} ts={}",
                CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis)),
                olderThanMillis,
                Instant.now());
    }

    /** One-shot, non-secret execution configuration at normal INFO level. */
    public void logRunStart(OrphanCleanConfig config) {
        String scope = config.allDatabases() ? "all-databases" : config.database().get();
        if (config.table().isPresent()) {
            scope = scope + "." + config.table().get();
        }
        AUDIT.info(
                "action=run_start scope={} older_than_ms={} dry_run={} parallelism={}"
                        + " remote_fs_rate_limit={} allow_delete_manifest={}"
                        + " allow_clean_orphan_tables={} allow_clean_orphan_partitions={} ts={}",
                scope,
                config.olderThanMillis(),
                config.dryRun(),
                config.parallelism().isPresent() ? config.parallelism().get() : "default",
                config.remoteFsOpRateLimitPerSecond(),
                config.allowDeleteManifest(),
                config.allowCleanOrphanTables(),
                config.allowCleanOrphanPartitions(),
                Instant.now());
    }

    /** One-shot aggregate of discovered scope, expected skips, and emitted cleanup work. */
    public void logScopePlan(ScopePlanStats stats) {
        AUDIT.info(
                "action=scope_plan databases={} tables={} partitions={} discovered_buckets={}"
                        + " bucket_tasks={} orphan_dir_tasks={} skipped_no_remote_manifest={}"
                        + " skipped_empty_kv_active_set={} skipped_out_of_scope_root={}"
                        + " metadata_failures={} ts={}",
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

    public void logDeleted(FsPath path, RuleId ruleId, boolean ok) {
        AUDIT.info("action=deleted rule={} path={} ok={} ts={}", ruleId, path, ok, Instant.now());
    }

    public void logWouldDelete(FsPath path, RuleId ruleId) {
        AUDIT.info("action=would_delete rule={} path={} ts={}", ruleId, path, Instant.now());
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
        AUDIT.info(
                "audit_version=1 stage=scan action={} object_type={} path={}"
                        + " size_bytes={} mtime_ms={} rule={} reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={} ts={}",
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

    private static void logDirectoryAction(
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
                "audit_version=1 stage=scan action={} object_type=directory path={}"
                        + " size_bytes=0 mtime_ms={} rule=empty-directory reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={} ts={}",
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
                actionRequired,
                Instant.now());
    }

    public void logSkipUnknown(FsPath path, RuleId ruleId) {
        AUDIT.warn("action=skip_unknown rule={} path={} ts={}", ruleId, path, Instant.now());
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
        AUDIT.error(
                "audit_version=1 stage=scan action=mtime_unavailable"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " object_type={} entry_kind={} sample_name={}"
                        + " action_required=true ts={}",
                scope.database(),
                scope.table(),
                nullable(scope.tableId()),
                nullable(scope.partitionId()),
                nullable(scope.bucketId()),
                lower(objectType.name()),
                entryKind,
                sanitizeSampleName(sampleName),
                Instant.now());
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
     * Skip shared SST cleanup for a single bucket because the active set could not be determined
     * (metadata read failure). The bucket's snap-private and log cleanup proceed normally.
     */
    public void logSkipKvSharedSst(long tableId, Long partitionId, int bucketId, String reason) {
        AUDIT.warn(
                "action=skip_kv_shared_sst reason={} table_id={} partition_id={}"
                        + " bucket_id={} ts={}",
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
    public void logSummary(
            long scanned,
            long deletedFiles,
            long emptyDirsRemoved,
            long deleteFailures,
            long bytesReclaimed,
            boolean dryRun) {
        AUDIT.info(
                "action=summary scanned={} deleted_total={} deleted_files={} empty_dirs_removed={}"
                        + " delete_failures={} bytes_reclaimed={} dry_run={} ts={}",
                scanned,
                deletedFiles + emptyDirsRemoved,
                deletedFiles,
                emptyDirsRemoved,
                deleteFailures,
                bytesReclaimed,
                dryRun,
                Instant.now());
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
                counters,
                dryRun);
    }

    public void logGlobalRuleSummary(
            CleanupObjectType objectType, RuleDecisionCounters counters, boolean dryRun) {
        logRuleDecisions(
                "summary_by_rule",
                "scope=global object_type=" + lower(objectType.name()),
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
        AUDIT.info(
                "action=coverage_summary no_remote_manifest_targets={}"
                        + " empty_active_set_targets={} metadata_read_failed_targets={}"
                        + " directory_list_failed_targets={} rpc_failed_targets={}"
                        + " mtime_unavailable_files={} mtime_unavailable_bytes={}"
                        + " mtime_unavailable_dirs={} complete={}"
                        + " action_required={} dry_run={} ts={}",
                skipped.getOrDefault(SkipReasonCode.NO_REMOTE_MANIFEST, 0L),
                skipped.getOrDefault(SkipReasonCode.EMPTY_KV_ACTIVE_SET, 0L),
                metadataFailures,
                skipped.getOrDefault(SkipReasonCode.DIRECTORY_LIST_FAILED, 0L),
                skipped.getOrDefault(SkipReasonCode.RPC_ERROR, 0L),
                mtimeUnavailableFiles,
                mtimeUnavailableBytes,
                mtimeUnavailableDirs,
                coverageComplete,
                !coverageComplete,
                dryRun,
                Instant.now());
    }

    public void logAuditIntegrity(CleanupSummary summary) {
        AUDIT.info(
                "action=audit_integrity rule_counters_consistent={} coverage_complete={}"
                        + " dry_run_counters_consistent={} inconsistent_object_types={}"
                        + " inconsistent_scopes={} dry_run={} ts={}",
                summary.ruleCountersConsistent(),
                summary.coverageComplete(),
                summary.dryRunCountersConsistent(),
                summary.inconsistentObjectTypes(),
                summary.inconsistentScopes(),
                summary.dryRun(),
                Instant.now());
    }

    private static void logRuleDecisions(
            String action, String dimensions, RuleDecisionCounters counters, boolean dryRun) {
        AUDIT.info(
                "action={} {} scanned_files={} scanned_bytes={} keep_active_files={}"
                        + " keep_active_bytes={} newer_than_cutoff_files={}"
                        + " newer_than_cutoff_bytes={} mtime_unavailable_files={}"
                        + " mtime_unavailable_bytes={} unknown_file_type_files={}"
                        + " unknown_file_type_bytes={} candidate_files={} candidate_bytes={}"
                        + " dry_run={} ts={}",
                action,
                dimensions,
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
                dryRun,
                Instant.now());
    }

    private static String nullable(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static String lower(String value) {
        return value.toLowerCase(Locale.ROOT);
    }
}
