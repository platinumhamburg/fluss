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
import org.apache.fluss.flink.action.orphan.job.CleanupCounters;
import org.apache.fluss.flink.action.orphan.job.CleanupSummary;
import org.apache.fluss.flink.action.orphan.job.RuleDecisionCounters;
import org.apache.fluss.flink.action.orphan.job.ScopePlanStats;
import org.apache.fluss.flink.action.orphan.job.ScopeTargetStats;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleEvaluation;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Structured audit log writer for the orphan files cleanup action.
 *
 * <p>The dedicated logger name {@code fluss.orphan.audit} can be routed to a separate sink (e.g.
 * SLS) by deployment-specific log4j configuration.
 */
@Internal
public final class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("fluss.orphan.audit");

    private final @Nullable AuditReporterRuntime runtime;
    private final @Nullable AuditReporterContext context;
    private final LongSupplier clock;
    private final Supplier<String> eventIds;

    private boolean mtimeUnavailableSampleLogged;
    private final Map<DiagnosticSampleKey, SampleCounter> diagnosticSamples = new LinkedHashMap<>();
    private boolean diagnosticSamplingSummariesFlushed;

    private static final int MAX_NORMAL_DECISION_SAMPLES = 3;

    /** Formats the frozen cutoff with an unambiguous UTC offset. */
    private static final DateTimeFormatter CUTOFF_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    /** Creates a legacy text-only logger without an external reporter or producer identity. */
    public AuditLogger() {
        this.runtime = null;
        this.context = null;
        this.clock = System::currentTimeMillis;
        this.eventIds = () -> UUID.randomUUID().toString();
    }

    /** Creates a same-source logger for an explicitly opened reporter runtime. */
    public AuditLogger(AuditReporterRuntime runtime, AuditReporterContext context) {
        this(runtime, context, System::currentTimeMillis, () -> UUID.randomUUID().toString());
    }

    AuditLogger(
            AuditReporterRuntime runtime,
            AuditReporterContext context,
            LongSupplier clock,
            Supplier<String> eventIds) {
        this.runtime = requireNonNull(runtime, "runtime");
        this.context = requireNonNull(context, "context");
        this.clock = requireNonNull(clock, "clock");
        this.eventIds = requireNonNull(eventIds, "eventIds");
    }

    /**
     * One-shot startup event recording the frozen file cutoff that drives this run's deletion
     * decisions. Emitted before any other audit line so log readers can recover the exact threshold
     * without having to re-parse the original CLI arguments.
     */
    public void logCutoff(long olderThanMillis) {
        String olderThanIso = CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis));
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.RUN, "cutoff")
                        .dimension("older_than_iso", olderThanIso)
                        .metric("older_than_ms", olderThanMillis),
                "action=cutoff older_than_iso={} older_than_ms={}",
                olderThanIso,
                olderThanMillis);
    }

    /** One-shot, non-secret execution configuration at normal INFO level. */
    public void logRunStart(OrphanCleanConfig config) {
        String scope = config.allDatabases() ? "all-databases" : config.database().get();
        if (config.table().isPresent()) {
            scope = scope + "." + config.table().get();
        }
        Object parallelism =
                config.parallelism().isPresent() ? config.parallelism().get() : "default";
        String cutoffSource = config.olderThanConfigured() ? "explicit" : "default";
        long eventTimeMillis = clock.getAsLong();
        long cutoffAgeMillis = eventTimeMillis - config.olderThanMillis();
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.RUN, "run_start", eventTimeMillis)
                        .dimension("scope", scope)
                        .dimension("parallelism", parallelism.toString())
                        .dimension(
                                "scope_enumeration_concurrency",
                                Integer.toString(config.scopeEnumerationConcurrency()))
                        .dimension("scan_parallelism", parallelism.toString())
                        .dimension("cutoff_source", cutoffSource)
                        .metric("older_than_ms", config.olderThanMillis())
                        .metric("cutoff_age_ms", cutoffAgeMillis)
                        .metric("remote_fs_rate_limit", config.remoteFsOpRateLimitPerSecond())
                        .flag("cutoff_in_future", cutoffAgeMillis < 0L)
                        .flag("dry_run", config.dryRun())
                        .flag("allow_delete_manifest", config.allowDeleteManifest())
                        .flag("allow_clean_orphan_tables", config.allowCleanOrphanTables())
                        .flag("allow_clean_orphan_partitions", config.allowCleanOrphanPartitions()),
                "action=run_start scope={} older_than_ms={} cutoff_source={} cutoff_age_ms={}"
                        + " cutoff_in_future={} dry_run={} parallelism={}"
                        + " scope_enumeration_concurrency={} scan_parallelism={}"
                        + " remote_fs_rate_limit={} allow_delete_manifest={}"
                        + " allow_clean_orphan_tables={} allow_clean_orphan_partitions={}",
                scope,
                config.olderThanMillis(),
                cutoffSource,
                cutoffAgeMillis,
                cutoffAgeMillis < 0L,
                config.dryRun(),
                parallelism,
                config.scopeEnumerationConcurrency(),
                parallelism,
                config.remoteFsOpRateLimitPerSecond(),
                config.allowDeleteManifest(),
                config.allowCleanOrphanTables(),
                config.allowCleanOrphanPartitions());
    }

    /** One-shot aggregate of discovered scope, expected skips, and emitted cleanup work. */
    public void logScopePlan(ScopePlanStats stats) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scope_plan")
                        .metric("databases", stats.databases())
                        .metric("tables", stats.tables())
                        .metric("partitions", stats.partitions())
                        .metric("discovered_buckets", stats.discoveredBuckets())
                        .metric("bucket_tasks", stats.bucketTasks())
                        .metric("orphan_dir_tasks", stats.orphanDirTasks())
                        .metric("skipped_no_remote_manifest", stats.skippedNoRemoteManifestCount())
                        .metric("skipped_empty_kv_active_set", stats.skippedEmptyKvActiveSetCount())
                        .metric("skipped_out_of_scope_root", stats.skippedOutOfScopeRootCount())
                        .metric("metadata_failures", stats.metadataFailures())
                        .metric("scope_targets", stats.scopeTargets())
                        .metric("target_buckets", stats.targetBuckets())
                        .metric("log_resolved_buckets", stats.logResolvedBuckets())
                        .metric("log_no_manifest_buckets", stats.logNoManifestBuckets())
                        .metric("log_read_failed_buckets", stats.logReadFailedBuckets())
                        .metric("log_unavailable_buckets", stats.logUnavailableBuckets())
                        .metric("out_of_scope_buckets", stats.outOfScopeBuckets())
                        .metric("kv_target_buckets", stats.kvTargetBuckets())
                        .metric("kv_active_buckets", stats.kvActiveBuckets())
                        .metric("kv_empty_buckets", stats.kvEmptyBuckets())
                        .metric("kv_unavailable_buckets", stats.kvUnavailableBuckets())
                        .metric("kv_out_of_scope_buckets", stats.kvOutOfScopeBuckets())
                        .metric("incomplete_targets", stats.incompleteTargets())
                        .flag("coverage_complete", stats.coverageComplete())
                        .flag("counters_consistent", stats.countersConsistent())
                        .flag("action_required", !stats.coverageComplete()),
                "action=scope_plan databases={} tables={} partitions={} discovered_buckets={}"
                        + " bucket_tasks={} orphan_dir_tasks={} skipped_no_remote_manifest={}"
                        + " skipped_empty_kv_active_set={} skipped_out_of_scope_root={}"
                        + " metadata_failures={} scope_targets={} target_buckets={}"
                        + " log_resolved_buckets={} log_no_manifest_buckets={}"
                        + " log_read_failed_buckets={} log_unavailable_buckets={}"
                        + " out_of_scope_buckets={} kv_target_buckets={} kv_active_buckets={}"
                        + " kv_empty_buckets={} kv_unavailable_buckets={} incomplete_targets={}"
                        + " kv_out_of_scope_buckets={}"
                        + " coverage_complete={} counters_consistent={} action_required={}",
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
                stats.scopeTargets(),
                stats.targetBuckets(),
                stats.logResolvedBuckets(),
                stats.logNoManifestBuckets(),
                stats.logReadFailedBuckets(),
                stats.logUnavailableBuckets(),
                stats.outOfScopeBuckets(),
                stats.kvTargetBuckets(),
                stats.kvActiveBuckets(),
                stats.kvEmptyBuckets(),
                stats.kvUnavailableBuckets(),
                stats.incompleteTargets(),
                stats.kvOutOfScopeBuckets(),
                stats.coverageComplete(),
                stats.countersConsistent(),
                !stats.coverageComplete());
    }

    public void logScopePhaseStart(String phase) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scope_phase_start")
                        .dimension("phase", phase),
                "audit_version=1 stage=scope action=scope_phase_start phase={}",
                phase);
    }

    public void logScopePhaseEnd(
            String phase,
            long durationMillis,
            long targetsCompleted,
            long targetsFailed,
            boolean complete) {
        logScopePhaseEnd(phase, durationMillis, targetsCompleted, targetsFailed, complete, null);
    }

    public void logScopePhaseEnd(
            String phase,
            long durationMillis,
            long targetsCompleted,
            long targetsFailed,
            boolean complete,
            @Nullable Integer scopeEnumerationConcurrency) {
        long targetsPerSecondMillis =
                durationMillis == 0L ? 0L : targetsCompleted * 1_000_000L / durationMillis;
        EventDraft event =
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scope_phase_end")
                        .dimension("phase", phase)
                        .metric("duration_ms", durationMillis)
                        .metric("targets_completed", targetsCompleted)
                        .metric("targets_failed", targetsFailed)
                        .metric("targets_per_second_millis", targetsPerSecondMillis)
                        .flag("complete", complete)
                        .flag("action_required", !complete);
        if (scopeEnumerationConcurrency == null) {
            emit(
                    event,
                    "audit_version=1 stage=scope action=scope_phase_end phase={} duration_ms={}"
                            + " targets_completed={} targets_failed={}"
                            + " targets_per_second_millis={} complete={} action_required={}",
                    phase,
                    durationMillis,
                    targetsCompleted,
                    targetsFailed,
                    targetsPerSecondMillis,
                    complete,
                    !complete);
            return;
        }
        event.dimension(
                "scope_enumeration_concurrency", Integer.toString(scopeEnumerationConcurrency));
        emit(
                event,
                "audit_version=1 stage=scope action=scope_phase_end phase={} duration_ms={}"
                        + " targets_completed={} targets_failed={}"
                        + " targets_per_second_millis={} scope_enumeration_concurrency={}"
                        + " complete={} action_required={}",
                phase,
                durationMillis,
                targetsCompleted,
                targetsFailed,
                targetsPerSecondMillis,
                scopeEnumerationConcurrency,
                complete,
                !complete);
    }

    /** One-shot effective Scan execution configuration for this subtask attempt. */
    public void logScanStart(
            long remoteFsRateLimit, int scanParallelism, double effectiveRatePerSecond) {
        String effectiveRate = Double.toString(effectiveRatePerSecond);
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "scan_start")
                        .dimension("parallelism", Integer.toString(scanParallelism))
                        .dimension("assigned_remote_fs_rate", effectiveRate)
                        .dimension("scan_parallelism", Integer.toString(scanParallelism))
                        .dimension("effective_remote_fs_rate_limit_per_second", effectiveRate)
                        .metric("remote_fs_rate_limit", remoteFsRateLimit),
                "audit_version=1 stage=scan action=scan_start parallelism={}"
                        + " assigned_remote_fs_rate={} scan_parallelism={}"
                        + " remote_fs_rate_limit={}"
                        + " effective_remote_fs_rate_limit_per_second={}",
                scanParallelism,
                effectiveRate,
                scanParallelism,
                remoteFsRateLimit,
                effectiveRate);
    }

    /** One-shot bounded scalar summary for this Scan subtask attempt. */
    public void logScanSubtaskSummary(
            long elapsedMillis, long tasksCompleted, CleanupCounters counters) {
        logScanSubtaskSummary(elapsedMillis, tasksCompleted, counters, 0L);
    }

    public void logScanSubtaskSummary(
            long elapsedMillis, long tasksCompleted, CleanupCounters counters, long scannedBytes) {
        long filesPerSecondMillis =
                fixedPointRate(counters.scannedFiles(), elapsedMillis, 1_000_000L);
        String filesPerSecond = fixedPointDecimal(filesPerSecondMillis);
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "scan_subtask_summary")
                        .dimension("files_per_second", filesPerSecond)
                        .metric("elapsed_ms", elapsedMillis)
                        .metric("tasks_completed", tasksCompleted)
                        .metric("scanned_files", counters.scannedFiles())
                        .metric("scanned_bytes", scannedBytes)
                        .metric("files_per_second_millis", filesPerSecondMillis)
                        .metric("planned_files", counters.plannedFiles())
                        .metric("planned_dirs", counters.plannedDirs())
                        .metric("planned_bytes", counters.plannedBytes())
                        .metric("deleted_files", counters.deletedFiles())
                        .metric("empty_dirs_removed", counters.emptyDirsRemoved())
                        .metric("delete_failures", counters.deleteFailures())
                        .metric("bytes_reclaimed", counters.bytesReclaimed()),
                "audit_version=1 stage=scan action=scan_subtask_summary elapsed_ms={}"
                        + " tasks_completed={} scanned_files={} scanned_bytes={}"
                        + " files_per_second_millis={} files_per_second={}"
                        + " planned_files={} planned_dirs={}"
                        + " planned_bytes={} deleted_files={} empty_dirs_removed={}"
                        + " delete_failures={} bytes_reclaimed={}",
                elapsedMillis,
                tasksCompleted,
                counters.scannedFiles(),
                scannedBytes,
                filesPerSecondMillis,
                filesPerSecond,
                counters.plannedFiles(),
                counters.plannedDirs(),
                counters.plannedBytes(),
                counters.deletedFiles(),
                counters.emptyDirsRemoved(),
                counters.deleteFailures(),
                counters.bytesReclaimed());
    }

    private static long fixedPointRate(long count, long durationMillis, long scale) {
        if (count <= 0L || durationMillis <= 0L) {
            return 0L;
        }
        BigInteger rate =
                BigInteger.valueOf(count)
                        .multiply(BigInteger.valueOf(scale))
                        .divide(BigInteger.valueOf(durationMillis));
        BigInteger maximum = BigInteger.valueOf(Long.MAX_VALUE);
        return rate.compareTo(maximum) > 0 ? Long.MAX_VALUE : rate.longValue();
    }

    private static String fixedPointDecimal(long filesPerSecondMillis) {
        long whole = filesPerSecondMillis / 1_000L;
        long fraction = filesPerSecondMillis % 1_000L;
        if (fraction < 10L) {
            return whole + ".00" + fraction;
        }
        if (fraction < 100L) {
            return whole + ".0" + fraction;
        }
        return whole + "." + fraction;
    }

    public void logScopeTargetSummary(ScopeTargetStats stats) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scope_target_summary")
                        .scope(stats.scope())
                        .metric("expected_buckets", stats.expectedBuckets())
                        .metric("log_resolved_buckets", stats.logResolvedBuckets())
                        .metric("log_no_manifest_buckets", stats.logNoManifestBuckets())
                        .metric("log_read_failed_buckets", stats.logReadFailedBuckets())
                        .metric("log_unavailable_buckets", stats.logUnavailableBuckets())
                        .metric("out_of_scope_buckets", stats.outOfScopeBuckets())
                        .metric("kv_active_buckets", stats.kvActiveBuckets())
                        .metric("kv_empty_buckets", stats.kvEmptyBuckets())
                        .metric("kv_unavailable_buckets", stats.kvUnavailableBuckets())
                        .metric("tasks_emitted", stats.tasksEmitted())
                        .metric("duration_ms", stats.durationMillis())
                        .flag("kv_applicable", stats.kvApplicable())
                        .flag("complete", stats.complete())
                        .flag("log_coverage_consistent", stats.logCoverageConsistent())
                        .flag("kv_coverage_consistent", stats.kvCoverageConsistent())
                        .flag(
                                "action_required",
                                !stats.complete()
                                        || !stats.logCoverageConsistent()
                                        || !stats.kvCoverageConsistent()),
                "audit_version=1 stage=scope action=scope_target_summary database={} table={}"
                        + " table_id={} partition_id={} expected_buckets={}"
                        + " log_resolved_buckets={} log_no_manifest_buckets={}"
                        + " log_read_failed_buckets={} log_unavailable_buckets={}"
                        + " out_of_scope_buckets={}"
                        + " kv_active_buckets={} kv_empty_buckets={} kv_unavailable_buckets={}"
                        + " tasks_emitted={} duration_ms={} kv_applicable={} complete={}"
                        + " log_coverage_consistent={} kv_coverage_consistent={}"
                        + " action_required={}",
                stats.scope().database(),
                stats.scope().table(),
                nullable(stats.scope().tableId()),
                nullable(stats.scope().partitionId()),
                stats.expectedBuckets(),
                stats.logResolvedBuckets(),
                stats.logNoManifestBuckets(),
                stats.logReadFailedBuckets(),
                stats.logUnavailableBuckets(),
                stats.outOfScopeBuckets(),
                stats.kvActiveBuckets(),
                stats.kvEmptyBuckets(),
                stats.kvUnavailableBuckets(),
                stats.tasksEmitted(),
                stats.durationMillis(),
                stats.kvApplicable(),
                stats.complete(),
                stats.logCoverageConsistent(),
                stats.kvCoverageConsistent(),
                !stats.complete()
                        || !stats.logCoverageConsistent()
                        || !stats.kvCoverageConsistent());
    }

    public void logDeleted(FsPath path, RuleId ruleId, boolean ok) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "deleted")
                        .object(ruleId)
                        .path(path.toString())
                        .flag("ok", ok),
                "action=deleted rule={} path={} ok={}",
                ruleId,
                path,
                ok);
    }

    public void logWouldDelete(FsPath path, RuleId ruleId) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "would_delete")
                        .object(ruleId)
                        .path(path.toString()),
                "action=would_delete rule={} path={}",
                ruleId,
                path);
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
                false,
                null);
    }

    public void logWouldDelete(
            FileMeta file, RuleId ruleId, ScopeIdentity scope, long cutoffMillis) {
        logObjectAction(
                "would_delete",
                file,
                ruleId,
                scope,
                "older_than_cutoff",
                "planned",
                true,
                false,
                false,
                cutoffMillis);
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
                false,
                null);
    }

    public void logDeleted(FileMeta file, RuleId ruleId, ScopeIdentity scope, long cutoffMillis) {
        logObjectAction(
                "deleted",
                file,
                ruleId,
                scope,
                "older_than_cutoff",
                "success",
                false,
                false,
                false,
                cutoffMillis);
    }

    public void logDeleteFailed(
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            boolean retryable) {
        logObjectAction(
                "delete_failed",
                file,
                ruleId,
                scope,
                reasonCode,
                "failed",
                false,
                retryable,
                true,
                null);
    }

    public void logDeleteFailed(
            FileMeta file,
            RuleId ruleId,
            ScopeIdentity scope,
            String reasonCode,
            boolean retryable,
            long cutoffMillis) {
        logObjectAction(
                "delete_failed",
                file,
                ruleId,
                scope,
                reasonCode,
                "failed",
                false,
                retryable,
                true,
                cutoffMillis);
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
            boolean actionRequired,
            @Nullable Long cutoffMillis) {
        EventDraft draft =
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, action)
                        .scope(scope)
                        .object(ruleId)
                        .path(file.path().toString())
                        .sizeBytes(file.size())
                        .mtimeMs(file.modificationTime())
                        .reasonCode(reasonCode)
                        .result(result)
                        .flag("dry_run", dryRun)
                        .flag("retryable", retryable)
                        .flag("action_required", actionRequired);
        if (cutoffMillis != null) {
            draft.metric("cutoff_ms", cutoffMillis)
                    .metric(
                            "mtime_minus_cutoff_ms",
                            subtractSaturated(file.modificationTime(), cutoffMillis));
            emit(
                    draft,
                    "audit_version=1 stage=scan action={} object_type={} path={}"
                            + " size_bytes={} mtime_ms={} cutoff_ms={}"
                            + " mtime_minus_cutoff_ms={} rule={} reason_code={} result={}"
                            + " database={} table={} table_id={} partition_id={} bucket_id={}"
                            + " dry_run={} retryable={} action_required={}",
                    action,
                    ruleId.objectType().name().toLowerCase(Locale.ROOT),
                    file.path(),
                    file.size(),
                    file.modificationTime(),
                    cutoffMillis,
                    subtractSaturated(file.modificationTime(), cutoffMillis),
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
            return;
        }
        emit(
                draft,
                "audit_version=1 stage=scan action={} object_type={} path={}"
                        + " size_bytes={} mtime_ms={} rule={} reason_code={} result={}"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " dry_run={} retryable={} action_required={}",
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
                actionRequired);
    }

    /** Emits a bounded normal-decision sample while counting every matching decision. */
    public void logDecisionSample(
            ScopeIdentity scope,
            FileMeta file,
            RuleId ruleId,
            RuleEvaluation evaluation,
            long cutoffMillis,
            boolean dryRun) {
        String objectType = lower(ruleId.objectType().name());
        DiagnosticSampleKey key =
                new DiagnosticSampleKey(
                        scope,
                        objectType,
                        evaluation.reasonCode(),
                        context == null ? null : context.getSubtaskIndex(),
                        context == null ? null : context.getAttemptNumber());
        SampleCounter counter = diagnosticSamples.get(key);
        if (counter == null) {
            counter = new SampleCounter(scope, objectType, evaluation);
            diagnosticSamples.put(key, counter);
        }
        counter.totalCount++;
        if (counter.emittedSamples >= MAX_NORMAL_DECISION_SAMPLES) {
            return;
        }
        counter.emittedSamples++;

        boolean actionRequired = evaluation.decision() == Decision.MTIME_UNAVAILABLE;
        AuditSeverity severity = actionRequired ? AuditSeverity.ERROR : AuditSeverity.INFO;
        EventDraft draft =
                newEvent(severity, AuditStage.SCAN, "decision_sample")
                        .scope(scope)
                        .object(ruleId)
                        .path(file.path().toString())
                        .sizeBytes(file.size())
                        .mtimeMs(file.modificationTime())
                        .reasonCode(evaluation.reasonCode())
                        .result("skipped")
                        .dimension("decision", lower(evaluation.decision().name()))
                        .metric("cutoff_ms", cutoffMillis)
                        .metric(
                                "mtime_minus_cutoff_ms",
                                subtractSaturated(file.modificationTime(), cutoffMillis))
                        .flag("dry_run", dryRun)
                        .flag("retryable", false)
                        .flag("action_required", actionRequired);
        evaluation.referenceType().ifPresent(value -> draft.dimension("reference_type", value));
        evaluation
                .referenceMatchKind()
                .ifPresent(value -> draft.dimension("reference_match_kind", value));
        evaluation.referenceKey().ifPresent(value -> draft.dimension("reference_key", value));
        emit(
                draft,
                "audit_version=1 stage=scan action=decision_sample object_type={} path={}"
                        + " size_bytes={} mtime_ms={} cutoff_ms={} mtime_minus_cutoff_ms={}"
                        + " rule={} decision={} reason_code={} database={} table={} table_id={}"
                        + " partition_id={} bucket_id={} reference_type={}"
                        + " reference_match_kind={} reference_key={} dry_run={} retryable=false"
                        + " action_required={}",
                objectType,
                file.path(),
                file.size(),
                file.modificationTime(),
                cutoffMillis,
                subtractSaturated(file.modificationTime(), cutoffMillis),
                ruleId,
                lower(evaluation.decision().name()),
                evaluation.reasonCode(),
                scope.database(),
                scope.table(),
                nullable(scope.tableId()),
                nullable(scope.partitionId()),
                nullable(scope.bucketId()),
                evaluation.referenceType().orElse("none"),
                evaluation.referenceMatchKind().orElse("none"),
                evaluation.referenceKey().orElse("none"),
                dryRun,
                actionRequired);
    }

    /** Emits one accounting event for each diagnostic group whose samples were suppressed. */
    public void flushDiagnosticSamplingSummaries() {
        if (diagnosticSamplingSummariesFlushed) {
            return;
        }
        diagnosticSamplingSummariesFlushed = true;
        for (SampleCounter counter : diagnosticSamples.values()) {
            long suppressed = counter.totalCount - counter.emittedSamples;
            if (suppressed == 0L) {
                continue;
            }
            emit(
                    newEvent(AuditSeverity.INFO, AuditStage.SCAN, "diagnostic_sampling_summary")
                            .scope(counter.scope)
                            .objectType(counter.objectType)
                            .reasonCode(counter.evaluation.reasonCode())
                            .dimension("decision", lower(counter.evaluation.decision().name()))
                            .metric("total_count", counter.totalCount)
                            .metric("emitted_samples", counter.emittedSamples)
                            .metric("suppressed_samples", suppressed),
                    "audit_version=1 stage=scan action=diagnostic_sampling_summary"
                            + " database={} table={} table_id={} partition_id={} object_type={}"
                            + " decision={} reason_code={} total_count={} emitted_samples={}"
                            + " suppressed_samples={}",
                    counter.scope.database(),
                    counter.scope.table(),
                    nullable(counter.scope.tableId()),
                    nullable(counter.scope.partitionId()),
                    counter.objectType,
                    lower(counter.evaluation.decision().name()),
                    counter.evaluation.reasonCode(),
                    counter.totalCount,
                    counter.emittedSamples,
                    suppressed);
        }
    }

    public void logDirDeleted(FsPath dir) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "dir_deleted")
                        .objectType("directory")
                        .path(dir.toString()),
                "action=dir_deleted path={}",
                dir);
    }

    public void logWouldDeleteDir(FsPath dir) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, "would_delete_dir")
                        .objectType("directory")
                        .path(dir.toString()),
                "action=would_delete_dir path={}",
                dir);
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
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCAN, action)
                        .scope(scope)
                        .objectType("directory")
                        .path(dir.toString())
                        .sizeBytes(0L)
                        .mtimeMs(modificationTime)
                        .rule("empty-directory")
                        .reasonCode(reasonCode)
                        .result(result)
                        .flag("dry_run", dryRun)
                        .flag("retryable", retryable)
                        .flag("action_required", actionRequired),
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
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCAN, "skip_unknown")
                        .object(ruleId)
                        .path(path.toString())
                        .reasonCode("unknown_file_type"),
                "action=skip_unknown rule={} path={}",
                ruleId,
                path);
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
        emit(
                newEvent(AuditSeverity.ERROR, AuditStage.SCAN, "mtime_unavailable")
                        .scope(scope)
                        .objectType(lower(objectType.name()))
                        .dimension("entry_kind", entryKind)
                        .dimension("sample_name", sanitizedSampleName)
                        .flag("action_required", true),
                "audit_version=1 stage=scan action=mtime_unavailable"
                        + " database={} table={} table_id={} partition_id={} bucket_id={}"
                        + " object_type={} entry_kind={} sample_name={}"
                        + " action_required=true",
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
        emit(
                newEvent(AuditSeverity.ERROR, AuditStage.SCOPE, "bucket_aborted")
                        .reasonCode(reason)
                        .dimension("bucket", bucketStr),
                "action=bucket_aborted bucket={} reason={}",
                bucketStr,
                reason);
    }

    /** Skip an entire database during scope enumeration due to listTables failure. */
    public void logSkipDb(String dbName, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_db")
                        .database(dbName)
                        .reasonCode(reason),
                "action=skip_db reason={} db={}",
                reason,
                dbName);
    }

    /** Skip a single table during scope enumeration due to getTableInfo or RPC failure. */
    public void logSkipTable(String dbName, String tableName, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_table")
                        .database(dbName)
                        .table(tableName)
                        .reasonCode(reason),
                "action=skip_table reason={} db={} table={}",
                reason,
                dbName,
                tableName);
    }

    /**
     * Skip listPartitionInfos for a table due to RPC failure (both active-partition cleanup and
     * orphan-partition scan are suppressed for this table).
     */
    public void logSkipPartitionList(String dbName, String tableName, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_partition_list")
                        .database(dbName)
                        .table(tableName)
                        .reasonCode(reason),
                "action=skip_partition_list reason={} db={} table={}",
                reason,
                dbName,
                tableName);
    }

    /**
     * Skip KV cleanup for one (tableId, partitionId) target — emitted when {@code ListKvSnapshots}
     * fails after retries. {@code partitionId} is null for non-partitioned tables.
     */
    public void logSkipKvTarget(long tableId, Long partitionId, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode(reason),
                "action=skip_kv_target reason={} table_id={} partition_id={}",
                reason,
                tableId,
                partitionId);
    }

    /**
     * Skip KV cleanup for a single bucket whose {@code ListKvSnapshots} response carried no
     * active-snapshot entries. Empty per-bucket active set is treated as "cannot prove what is
     * active" and the bucket is skipped to avoid mis-deletion.
     */
    public void logSkipKvBucket(long tableId, Long partitionId, int bucketId, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_bucket")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode(reason),
                "action=skip_kv_bucket reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    /** Skip shared-SST cleanup when active snapshot metadata cannot be resolved completely. */
    public void logSkipKvSharedSst(long tableId, Long partitionId, int bucketId, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_kv_shared_sst")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode("metadata_read_failed"),
                "action=skip_kv_shared_sst reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    /**
     * Skip log cleanup for one (tableId, partitionId) target — emitted when {@code
     * ListRemoteLogManifests} fails after retries. {@code partitionId} is null for non-partitioned
     * tables.
     */
    public void logSkipLogTarget(long tableId, Long partitionId, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_log_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode(reason),
                "action=skip_log_target reason={} table_id={} partition_id={}",
                reason,
                tableId,
                partitionId);
    }

    /**
     * Skip log cleanup for a single bucket whose remote manifest was not returned by the {@code
     * ListRemoteLogManifests} RPC (the bucket has not yet committed any remote manifest).
     */
    public void logSkipLogBucket(long tableId, Long partitionId, int bucketId, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_log_bucket")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode(reason),
                "action=skip_log_bucket reason={} table_id={} partition_id={} bucket_id={}",
                reason,
                tableId,
                partitionId,
                bucketId);
    }

    /** Scan a log bucket with no committed remote manifest and therefore no active references. */
    public void logScanLogBucketWithoutManifest(long tableId, Long partitionId, int bucketId) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "scan_log_bucket_without_manifest")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode("no_remote_manifest"),
                "action=scan_log_bucket_without_manifest reason=no_remote_manifest"
                        + " table_id={} partition_id={} bucket_id={}",
                tableId,
                partitionId,
                bucketId);
    }

    /** Scan a KV bucket after the RPC authoritatively reports no active snapshots. */
    public void logScanKvBucketWithoutActiveSnapshots(
            long tableId, Long partitionId, int bucketId) {
        emit(
                newEvent(
                                AuditSeverity.INFO,
                                AuditStage.SCOPE,
                                "scan_kv_bucket_without_active_snapshots")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .bucketId(bucketId)
                        .reasonCode("no_active_snapshots"),
                "action=scan_kv_bucket_without_active_snapshots reason=no_active_snapshots"
                        + " table_id={} partition_id={} bucket_id={}",
                tableId,
                partitionId,
                bucketId);
    }

    /** Default-conservative skip of an orphan-table dir (opt-in flag not set). */
    public void logSkipOrphanTable(FsPath dir, String reason) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_orphan_table")
                        .objectType("directory")
                        .path(dir.toString())
                        .reasonCode(reason),
                "action=skip_orphan_table reason={} path={}",
                reason,
                dir);
    }

    /**
     * Skip the orphan-table scan for a database whose table-info set is incomplete (e.g. {@code
     * --table} single-table mode, or {@code listTables}/{@code getTableInfo} failures left holes in
     * the active table id set). Distinct from {@link #logSkipDb}, which means the whole database
     * scope is dropped.
     */
    public void logSkipOrphanTableScan(String dbName, String reason) {
        emit(
                newEvent(AuditSeverity.WARN, AuditStage.SCOPE, "skip_orphan_table_scan")
                        .database(dbName)
                        .reasonCode(reason),
                "action=skip_orphan_table_scan reason={} db={}",
                reason,
                dbName);
    }

    /** Default-conservative skip of an orphan-partition dir (opt-in flag not set). */
    public void logSkipOrphanPartition(FsPath dir, String reason) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_orphan_partition")
                        .objectType("directory")
                        .path(dir.toString())
                        .reasonCode(reason),
                "action=skip_orphan_partition reason={} path={}",
                reason,
                dir);
    }

    /** Skip a bucket target because its metadata-resolved root is outside cluster config. */
    public void logSkipBucketOutOfScope(long tableId, Long partitionId, String resolvedRoot) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SCOPE, "skip_bucket_target")
                        .tableId(tableId)
                        .partitionId(partitionId)
                        .reasonCode("out-of-scope-root")
                        .dimension("resolved_root", resolvedRoot),
                "action=skip_bucket_target reason=out-of-scope-root table_id={} partition_id={}"
                        + " resolved_root={}",
                tableId,
                partitionId,
                resolvedRoot);
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
        long deletedTotal = deletedFiles + emptyDirsRemoved;
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "summary")
                        .metric("scanned", scanned)
                        .metric("deleted_total", deletedTotal)
                        .metric("deleted_files", deletedFiles)
                        .metric("empty_dirs_removed", emptyDirsRemoved)
                        .metric("delete_failures", deleteFailures)
                        .metric("bytes_reclaimed", bytesReclaimed)
                        .flag("dry_run", dryRun),
                "action=summary scanned={} deleted_total={} deleted_files={} empty_dirs_removed={}"
                        + " delete_failures={} bytes_reclaimed={} dry_run={}",
                scanned,
                deletedTotal,
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
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "table_rule_summary")
                        .scope(scope)
                        .objectType(lower(objectType.name())),
                "table_rule_summary",
                "database="
                        + scope.database()
                        + " table="
                        + scope.table()
                        + " table_id="
                        + nullable(scope.tableId())
                        + " object_type="
                        + lower(objectType.name()),
                objectType,
                counters,
                dryRun);
    }

    public void logGlobalRuleSummary(
            CleanupObjectType objectType, RuleDecisionCounters counters, boolean dryRun) {
        logRuleDecisions(
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "summary_by_rule")
                        .scopeKind("global")
                        .objectType(lower(objectType.name())),
                "summary_by_rule",
                "scope=global object_type=" + lower(objectType.name()),
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
        long noRemoteManifestTargets = skipped.getOrDefault(SkipReasonCode.NO_REMOTE_MANIFEST, 0L);
        long emptyActiveSetTargets = skipped.getOrDefault(SkipReasonCode.EMPTY_KV_ACTIVE_SET, 0L);
        long directoryListFailedTargets =
                skipped.getOrDefault(SkipReasonCode.DIRECTORY_LIST_FAILED, 0L);
        long rpcFailedTargets = skipped.getOrDefault(SkipReasonCode.RPC_ERROR, 0L);
        boolean physicalScanComplete =
                coverageComplete && noRemoteManifestTargets == 0L && emptyActiveSetTargets == 0L;
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "coverage_summary")
                        .metric("no_remote_manifest_targets", noRemoteManifestTargets)
                        .metric("empty_active_set_targets", emptyActiveSetTargets)
                        .metric("metadata_read_failed_targets", metadataFailures)
                        .metric("directory_list_failed_targets", directoryListFailedTargets)
                        .metric("rpc_failed_targets", rpcFailedTargets)
                        .metric("mtime_unavailable_files", mtimeUnavailableFiles)
                        .metric("mtime_unavailable_bytes", mtimeUnavailableBytes)
                        .metric("mtime_unavailable_dirs", mtimeUnavailableDirs)
                        .flag("complete", coverageComplete)
                        .flag("physical_scan_complete", physicalScanComplete)
                        .flag("action_required", !coverageComplete)
                        .flag("dry_run", dryRun),
                "action=coverage_summary no_remote_manifest_targets={}"
                        + " empty_active_set_targets={} metadata_read_failed_targets={}"
                        + " directory_list_failed_targets={} rpc_failed_targets={}"
                        + " mtime_unavailable_files={} mtime_unavailable_bytes={}"
                        + " mtime_unavailable_dirs={} complete={} physical_scan_complete={}"
                        + " action_required={} dry_run={}",
                noRemoteManifestTargets,
                emptyActiveSetTargets,
                metadataFailures,
                directoryListFailedTargets,
                rpcFailedTargets,
                mtimeUnavailableFiles,
                mtimeUnavailableBytes,
                mtimeUnavailableDirs,
                coverageComplete,
                physicalScanComplete,
                !coverageComplete,
                dryRun);
    }

    public void logAuditIntegrity(CleanupSummary summary) {
        emit(
                newEvent(AuditSeverity.INFO, AuditStage.SUMMARY, "audit_integrity")
                        .metric("inconsistent_object_types", summary.inconsistentObjectTypes())
                        .metric("inconsistent_scopes", summary.inconsistentScopes())
                        .metric("incomplete_scope_targets", summary.incompleteScopeTargets())
                        .flag("scope_counters_consistent", summary.scopeCountersConsistent())
                        .flag("rule_counters_consistent", summary.ruleCountersConsistent())
                        .flag("counters_consistent", summary.countersConsistent())
                        .flag("coverage_complete", summary.coverageComplete())
                        .flag("dry_run_counters_consistent", summary.dryRunCountersConsistent())
                        .flag("dry_run", summary.dryRun()),
                "action=audit_integrity scope_counters_consistent={}"
                        + " rule_counters_consistent={} counters_consistent={} coverage_complete={}"
                        + " dry_run_counters_consistent={} inconsistent_object_types={}"
                        + " inconsistent_scopes={} incomplete_scope_targets={} dry_run={}",
                summary.scopeCountersConsistent(),
                summary.ruleCountersConsistent(),
                summary.countersConsistent(),
                summary.coverageComplete(),
                summary.dryRunCountersConsistent(),
                summary.inconsistentObjectTypes(),
                summary.inconsistentScopes(),
                summary.incompleteScopeTargets(),
                summary.dryRun());
    }

    public void logMetadataFailure(
            AuditStage stage,
            ScopeIdentity scope,
            CleanupObjectType objectType,
            AuditFailureDetail detail) {
        logFailure("metadata_failure", stage, scope, objectType, detail);
    }

    public void logFilesystemFailure(
            AuditStage stage,
            ScopeIdentity scope,
            CleanupObjectType objectType,
            AuditFailureDetail detail) {
        logFailure("filesystem_failure", stage, scope, objectType, detail);
    }

    public void logRpcFailure(
            AuditStage stage,
            ScopeIdentity scope,
            CleanupObjectType objectType,
            AuditFailureDetail detail) {
        logFailure("rpc_failure", stage, scope, objectType, detail);
    }

    private void logFailure(
            String action,
            AuditStage stage,
            ScopeIdentity scope,
            CleanupObjectType objectType,
            AuditFailureDetail detail) {
        AuditSeverity severity = detail.actionRequired() ? AuditSeverity.ERROR : AuditSeverity.WARN;
        EventDraft draft =
                newEvent(severity, stage, action)
                        .scope(scope)
                        .objectType(lower(objectType.name()))
                        .reasonCode(detail.failureCategory())
                        .dimension("operation", detail.operation())
                        .dimension("failure_category", detail.failureCategory())
                        .dimension("exception_class", detail.exceptionClass())
                        .metric("attempts", detail.attempts())
                        .flag("retryable", detail.retryable())
                        .flag("action_required", detail.actionRequired())
                        .flag("consistency_race_possible", detail.consistencyRacePossible());
        if (detail.targetPath() != null) {
            draft.path(detail.targetPath()).dimension("target_path", detail.targetPath());
        }
        if (detail.metadataPath() != null) {
            draft.path(detail.metadataPath()).dimension("metadata_path", detail.metadataPath());
        }
        if (detail.errno() != null) {
            draft.dimension("errno", detail.errno());
        }
        emit(
                draft,
                "audit_version=1 stage={} action={} database={} table={} table_id={}"
                        + " partition_id={} bucket_id={} object_type={} operation={}"
                        + " failure_category={} target_path={} metadata_path={}"
                        + " exception_class={} errno={} attempts={} retryable={}"
                        + " action_required={} consistency_race_possible={}",
                lower(stage.name()),
                action,
                scope.database(),
                scope.table(),
                nullable(scope.tableId()),
                nullable(scope.partitionId()),
                nullable(scope.bucketId()),
                lower(objectType.name()),
                detail.operation(),
                detail.failureCategory(),
                nullable(detail.targetPath()),
                nullable(detail.metadataPath()),
                detail.exceptionClass(),
                nullable(detail.errno()),
                detail.attempts(),
                detail.retryable(),
                detail.actionRequired(),
                detail.consistencyRacePossible());
    }

    private void logRuleDecisions(
            EventDraft draft,
            String action,
            String dimensions,
            CleanupObjectType objectType,
            RuleDecisionCounters counters,
            boolean dryRun) {
        boolean referenceMatchAbsent =
                (objectType == CleanupObjectType.LOG_SEGMENT
                                || objectType == CleanupObjectType.KV_SHARED_SST)
                        && counters.scannedFiles() > 0L
                        && counters.keepActiveFiles() == 0L;
        boolean allScannedNewerThanCutoff =
                counters.scannedFiles() > 0L
                        && counters.newerThanCutoffFiles() == counters.scannedFiles();
        boolean allScannedKeepActive =
                counters.scannedFiles() > 0L
                        && counters.keepActiveFiles() == counters.scannedFiles();
        emit(
                draft.metric("scanned_files", counters.scannedFiles())
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
                        .flag("reference_match_absent", referenceMatchAbsent)
                        .flag("all_scanned_newer_than_cutoff", allScannedNewerThanCutoff)
                        .flag("all_scanned_keep_active", allScannedKeepActive)
                        .flag("dry_run", dryRun),
                "action={} {} scanned_files={} scanned_bytes={} keep_active_files={}"
                        + " keep_active_bytes={} newer_than_cutoff_files={}"
                        + " newer_than_cutoff_bytes={} mtime_unavailable_files={}"
                        + " mtime_unavailable_bytes={} unknown_file_type_files={}"
                        + " unknown_file_type_bytes={} candidate_files={} candidate_bytes={}"
                        + " reference_match_absent={} all_scanned_newer_than_cutoff={}"
                        + " all_scanned_keep_active={} dry_run={}",
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
                referenceMatchAbsent,
                allScannedNewerThanCutoff,
                allScannedKeepActive,
                dryRun);
    }

    private EventDraft newEvent(AuditSeverity severity, AuditStage stage, String action) {
        return newEvent(severity, stage, action, clock.getAsLong());
    }

    private EventDraft newEvent(
            AuditSeverity severity, AuditStage stage, String action, long eventTimeMillis) {
        if (runtime == null) {
            return new EventDraft(severity, eventTimeMillis, null);
        }

        AuditReporterContext reporterContext = context;
        AuditEvent.Builder builder =
                AuditEvent.builder()
                        .eventId(eventIds.get())
                        .runId(reporterContext.getRunId())
                        .eventTimeMillis(eventTimeMillis)
                        .severity(severity)
                        .stage(stage)
                        .action(action)
                        .operatorName(reporterContext.getOperatorName())
                        .subtaskIndex(reporterContext.getSubtaskIndex())
                        .attemptNumber(reporterContext.getAttemptNumber());
        return new EventDraft(severity, eventTimeMillis, builder);
    }

    private void emit(EventDraft draft, String legacyTemplate, Object... legacyArgs) {
        AuditEvent event = draft.build();
        Instant timestamp = Instant.ofEpochMilli(draft.eventTimeMillis);
        if (event == null) {
            write(draft.severity, legacyTemplate + " ts={}", append(legacyArgs, timestamp));
            return;
        }

        write(
                event.getSeverity(),
                legacyTemplate + " ts={} run_id={} event_id={} operator={} subtask={} attempt={}",
                append(
                        legacyArgs,
                        timestamp,
                        event.getRunId(),
                        event.getEventId(),
                        nullable(event.getOperatorName()),
                        nullable(event.getSubtaskIndex()),
                        nullable(event.getAttemptNumber())));
        runtime.report(event);
    }

    private static void write(AuditSeverity severity, String template, Object[] arguments) {
        switch (severity) {
            case INFO:
                AUDIT.info(template, arguments);
                break;
            case WARN:
                AUDIT.warn(template, arguments);
                break;
            case ERROR:
                AUDIT.error(template, arguments);
                break;
            default:
                throw new IllegalArgumentException("severity");
        }
    }

    private static Object[] append(Object[] prefix, Object... suffix) {
        Object[] values = new Object[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, values, 0, prefix.length);
        System.arraycopy(suffix, 0, values, prefix.length, suffix.length);
        return values;
    }

    private static <T> T requireNonNull(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field);
        }
        return value;
    }

    private static String nullable(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static String lower(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private static long subtractSaturated(long left, long right) {
        try {
            return Math.subtractExact(left, right);
        } catch (ArithmeticException overflow) {
            return left < right ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    private static final class SampleCounter {
        private final ScopeIdentity scope;
        private final String objectType;
        private final RuleEvaluation evaluation;
        private long totalCount;
        private long emittedSamples;

        private SampleCounter(ScopeIdentity scope, String objectType, RuleEvaluation evaluation) {
            this.scope = scope;
            this.objectType = objectType;
            this.evaluation = evaluation;
        }
    }

    private static final class EventDraft {
        private final AuditSeverity severity;
        private final long eventTimeMillis;
        private final @Nullable AuditEvent.Builder builder;
        private final Map<String, String> dimensions = new LinkedHashMap<>();
        private final Map<String, Long> metrics = new LinkedHashMap<>();
        private final Map<String, Boolean> flags = new LinkedHashMap<>();

        private EventDraft(
                AuditSeverity severity,
                long eventTimeMillis,
                @Nullable AuditEvent.Builder builder) {
            this.severity = severity;
            this.eventTimeMillis = eventTimeMillis;
            this.builder = builder;
        }

        private EventDraft scope(ScopeIdentity scope) {
            if (builder != null) {
                builder.scopeKind(lower(scope.kind().name()));
                if (scope.kind() != ScopeKind.GLOBAL) {
                    builder.database(scope.database())
                            .table(scope.table())
                            .tableId(scope.tableId())
                            .partitionId(scope.partitionId())
                            .bucketId(scope.bucketId());
                }
            }
            return this;
        }

        private EventDraft database(@Nullable String value) {
            if (builder != null) {
                builder.database(value);
            }
            return this;
        }

        private EventDraft table(@Nullable String value) {
            if (builder != null) {
                builder.table(value);
            }
            return this;
        }

        private EventDraft tableId(@Nullable Long value) {
            if (builder != null) {
                builder.tableId(value);
            }
            return this;
        }

        private EventDraft partitionId(@Nullable Long value) {
            if (builder != null) {
                builder.partitionId(value);
            }
            return this;
        }

        private EventDraft bucketId(@Nullable Integer value) {
            if (builder != null) {
                builder.bucketId(value);
            }
            return this;
        }

        private EventDraft scopeKind(@Nullable String value) {
            if (builder != null) {
                builder.scopeKind(value);
            }
            return this;
        }

        private EventDraft objectType(@Nullable String value) {
            if (builder != null) {
                builder.objectType(value);
            }
            return this;
        }

        private EventDraft path(@Nullable String value) {
            if (builder != null) {
                builder.path(value);
            }
            return this;
        }

        private EventDraft sizeBytes(@Nullable Long value) {
            if (builder != null) {
                builder.sizeBytes(value);
            }
            return this;
        }

        private EventDraft mtimeMs(@Nullable Long value) {
            if (builder != null) {
                builder.mtimeMs(value);
            }
            return this;
        }

        private EventDraft rule(@Nullable String value) {
            if (builder != null) {
                builder.rule(value);
            }
            return this;
        }

        private EventDraft reasonCode(@Nullable String value) {
            if (builder != null) {
                builder.reasonCode(value);
            }
            return this;
        }

        private EventDraft result(@Nullable String value) {
            if (builder != null) {
                builder.result(value);
            }
            return this;
        }

        private EventDraft object(RuleId ruleId) {
            return objectType(lower(ruleId.objectType().name())).rule(ruleId.toString());
        }

        private EventDraft dimension(String name, String value) {
            if (builder != null) {
                dimensions.put(name, value);
            }
            return this;
        }

        private EventDraft metric(String name, long value) {
            if (builder != null) {
                metrics.put(name, value);
            }
            return this;
        }

        private EventDraft flag(String name, boolean value) {
            if (builder != null) {
                flags.put(name, value);
            }
            return this;
        }

        @Nullable
        private AuditEvent build() {
            if (builder == null) {
                return null;
            }
            return builder.dimensions(dimensions).metrics(metrics).flags(flags).build();
        }
    }
}
