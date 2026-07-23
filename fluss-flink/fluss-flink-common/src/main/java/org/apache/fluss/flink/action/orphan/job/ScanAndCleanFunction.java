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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.orphan.audit.AuditFailureDetail;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.action.orphan.fs.FileSystemProbe;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.MtimePolicy;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.flink.action.orphan.rule.RuleEvaluation;
import org.apache.fluss.flink.adapter.ProcessFunctionAdapter;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;

/**
 * Stage 2 of the orphan files cleanup job. Runs at user-configured parallelism (N) and performs
 * pure FS operations — no coordinator RPC interaction.
 *
 * <p>Each subtask processes assigned {@link CleanTask} items serially:
 *
 * <ul>
 *   <li>{@link BucketCleanTask}: second-reads manifests from object storage to build the active
 *       reference set, then walks log/kv directories and deletes orphan files and old empty child
 *       directories.
 *   <li>{@link OrphanDirCleanTask}: recursively walks the orphan directory and deletes all files
 *       older than the cutoff, then removes old empty directories bottom-up.
 * </ul>
 *
 * <p>Each task emits a single {@link CleanupStats} containing scalar counters. Remote filesystem
 * operation rate is limited per-subtask: {@code configuredRate / runtimeParallelism}. The serial
 * processing within each subtask guarantees no concurrent throttler access.
 */
@Internal
public final class ScanAndCleanFunction extends ProcessFunctionAdapter<CleanTask, CleanupStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScanAndCleanFunction.class);

    private final long remoteFsOpRateLimitPerSecond;
    private final Map<String, String> extraConfigs;
    private final AuditReporterSpec auditReporterSpec;
    private final boolean dryRun;

    private transient AuditReporterRuntime auditRuntime;
    private transient AuditLogger audit;
    private transient RateLimiter remoteFsOpRateLimiter;
    private transient Throwable processingFailure;
    private transient long scanStartMillis;
    private transient long tasksCompleted;
    private transient CleanupCounters subtaskCounters;
    private transient long subtaskScannedBytes;

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond, Map<String, String> extraConfigs) {
        this(remoteFsOpRateLimitPerSecond, extraConfigs, null, false);
    }

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond,
            Map<String, String> extraConfigs,
            AuditReporterSpec auditReporterSpec,
            boolean dryRun) {
        this.remoteFsOpRateLimitPerSecond = remoteFsOpRateLimitPerSecond;
        this.extraConfigs = extraConfigs;
        this.auditReporterSpec = auditReporterSpec;
        this.dryRun = dryRun;
    }

    @Override
    protected void doOpen() throws Exception {
        processingFailure = null;
        scanStartMillis = System.currentTimeMillis();
        tasksCompleted = 0L;
        subtaskCounters = CleanupCounters.empty();
        subtaskScannedBytes = 0L;
        if (!extraConfigs.isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(extraConfigs), null);
        }
        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        int parallelism = RuntimeContextAdapter.getNumberOfParallelSubtasks(runtimeContext);
        int subtaskIndex = RuntimeContextAdapter.getIndexOfThisSubtask(runtimeContext);
        remoteFsOpRateLimiter =
                RateLimiter.create(perSubtaskRate(remoteFsOpRateLimitPerSecond, parallelism));
        if (auditReporterSpec == null) {
            audit = new AuditLogger();
        } else {
            AuditReporterContext reporterContext =
                    new AuditReporterContext(
                            auditReporterSpec.runId(),
                            auditReporterSpec.clusterId(),
                            dryRun,
                            AuditStage.SCAN,
                            "ScanAndClean",
                            subtaskIndex,
                            RuntimeContextAdapter.getAttemptNumber(runtimeContext),
                            getRuntimeContext().getUserCodeClassLoader());
            auditRuntime = AuditReporterRuntime.open(auditReporterSpec, reporterContext);
            audit = new AuditLogger(auditRuntime, reporterContext);
        }
        try {
            audit.logScanStart(
                    remoteFsOpRateLimitPerSecond,
                    parallelism,
                    perSubtaskRate(remoteFsOpRateLimitPerSecond, parallelism));
        } catch (Exception | Error openFailure) {
            try {
                closeAuditRuntime(false);
            } catch (RuntimeException | Error cleanupFailure) {
                openFailure.addSuppressed(cleanupFailure);
            }
            throw openFailure;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            closeAuditRuntime();
        } catch (RuntimeException | Error lifecycleFailure) {
            if (processingFailure == null) {
                throw lifecycleFailure;
            }
            processingFailure.addSuppressed(lifecycleFailure);
        } finally {
            super.close();
        }
    }

    @Override
    public void processElement(CleanTask task, Context ctx, Collector<CleanupStats> out)
            throws Exception {
        ScanTaskProgress taskProgress = null;
        boolean progressMerged = false;
        try {
            if (task instanceof ScopeSummaryTask) {
                out.collect(((ScopeSummaryTask) task).stats());
            } else if (task instanceof BucketCleanTask) {
                taskProgress = new ScanTaskProgress();
                CleanupStats stats = processBucketTask((BucketCleanTask) task, taskProgress);
                mergeTaskProgress(taskProgress);
                progressMerged = true;
                out.collect(stats);
                tasksCompleted++;
            } else if (task instanceof OrphanDirCleanTask) {
                taskProgress = new ScanTaskProgress();
                CleanupStats stats = processOrphanDirTask((OrphanDirCleanTask) task, taskProgress);
                mergeTaskProgress(taskProgress);
                progressMerged = true;
                out.collect(stats);
                tasksCompleted++;
            }
        } catch (Exception | Error failure) {
            if (taskProgress != null && !progressMerged) {
                mergeTaskProgress(taskProgress);
            }
            if (processingFailure == null) {
                processingFailure = failure;
            }
            throw failure;
        }
    }

    // -------------------------------------------------------------------------
    // BucketCleanTask processing
    // -------------------------------------------------------------------------

    private CleanupStats processBucketTask(BucketCleanTask task, ScanTaskProgress progress)
            throws IOException {
        FsPath logDir = task.logTabletDir() != null ? new FsPath(task.logTabletDir()) : null;
        FsPath kvDir = task.kvTabletDir() != null ? new FsPath(task.kvTabletDir()) : null;

        FsPath anyDir = logDir != null ? logDir : kvDir;
        if (anyDir == null) {
            return CleanupStats.emptyScan(task.scope());
        }

        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        task.logSegmentRelativePaths(),
                        task.kvActiveSnapDirs(),
                        task.logActiveManifestPaths(),
                        task.kvSharedSstFileNames(),
                        task.kvSharedSstRefsComplete());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());
        SafeDeleter safeDeleter =
                createSafeDeleter(anyDir.getFileSystem(), task.dryRun(), task.scope(), progress);
        BucketCleaner cleaner =
                new BucketCleaner(
                        dispatcher,
                        safeDeleter,
                        audit,
                        task.cutoffMillis(),
                        remoteFsOpRateLimiter,
                        task.dryRun(),
                        task.scope(),
                        progress);

        BucketCleaner.BucketCleanStats bucketStats = cleaner.clean(activeRefs, logDir, kvDir);

        return CleanupStats.scan(
                task.scope(),
                new CleanupCounters(
                        bucketStats.scannedFiles,
                        bucketStats.plannedFiles,
                        bucketStats.plannedDirs,
                        bucketStats.plannedBytes,
                        bucketStats.deletedFiles,
                        bucketStats.emptyDirsRemoved,
                        bucketStats.deleteFailures,
                        bucketStats.bytesReclaimed),
                bucketStats.byObjectType,
                bucketStats.bySkipReason,
                bucketStats.byRuleDecision);
    }

    // -------------------------------------------------------------------------
    // OrphanDirCleanTask processing
    // -------------------------------------------------------------------------

    private CleanupStats processOrphanDirTask(OrphanDirCleanTask task, ScanTaskProgress progress)
            throws IOException {
        FsPath dirPath = new FsPath(task.dirPath());
        FileSystem fs = dirPath.getFileSystem();
        SafeDeleter safeDeleter = createSafeDeleter(fs, task.dryRun(), task.scope(), progress);
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest(), true);

        CleanupStats.Builder stats = CleanupStats.scanBuilder(task.scope());

        Optional<FileStatus> rootStatusResult;
        try {
            rootStatusResult = FileSystemProbe.getFileStatus(fs, dirPath, remoteFsOpRateLimiter);
        } catch (IOException e) {
            audit.logFilesystemFailure(
                    AuditStage.SCAN,
                    task.scope(),
                    CleanupObjectType.DIRECTORY,
                    AuditFailureDetail.builder("get_file_status", "directory_list_failed")
                            .targetPath(dirPath)
                            .exceptionClass(e.getClass())
                            .retryable(true)
                            .actionRequired(true)
                            .build());
            stats.skipped(SkipReasonCode.DIRECTORY_LIST_FAILED, 1L);
            return stats.build();
        }
        if (!rootStatusResult.isPresent()) {
            audit.logSkippedDirectory(
                    dirPath,
                    Long.MAX_VALUE,
                    task.scope(),
                    "directory_not_found",
                    task.dryRun(),
                    false,
                    false);
            stats.skipped(SkipReasonCode.DIRECTORY_NOT_FOUND, 1L);
            return stats.build();
        }
        FileStatus rootStatus = rootStatusResult.get();
        long rootModificationTime = rootStatus.getModificationTime();
        if (MtimePolicy.isUnavailable(rootModificationTime)) {
            stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
            audit.logSkippedDirectory(
                    dirPath,
                    rootModificationTime,
                    task.scope(),
                    "mtime_unavailable",
                    task.dryRun(),
                    false,
                    true);
        }
        Deque<DirVisit> stack = new ArrayDeque<DirVisit>();
        stack.push(
                new DirVisit(
                        dirPath,
                        false,
                        rootStatus.isDir()
                                && MtimePolicy.isOlderThanCutoff(
                                        rootModificationTime, task.cutoffMillis()),
                        rootModificationTime,
                        null));
        while (!stack.isEmpty()) {
            DirVisit visit = stack.pop();
            if (visit.postOrder) {
                boolean plannedRemoval = visit.oldEnough && !visit.hasRemainingChild;
                if (plannedRemoval) {
                    if (task.dryRun()) {
                        stats.plannedDirectory(1L);
                        progress.recordPlannedDirectory();
                        audit.logWouldDeleteDirectory(
                                visit.dir, visit.modificationTime, task.scope(), true);
                        continue;
                    }
                    SafeDeleter.DirectoryDeleteResult result =
                            safeDeleter.deleteEmptyDirDetailed(visit.dir, visit.modificationTime);
                    switch (result) {
                        case SUCCESS:
                            stats.plannedDirectory(1L);
                            progress.recordPlannedDirectory();
                            stats.removedDirectory(1L);
                            break;
                        case NOT_FOUND:
                            stats.skipped(SkipReasonCode.DIRECTORY_NOT_FOUND, 1L);
                            break;
                        case NOT_EMPTY:
                            stats.skipped(SkipReasonCode.DIRECTORY_NOT_EMPTY, 1L);
                            visit.markParentRemaining();
                            break;
                        case LIST_FAILED:
                            stats.skipped(SkipReasonCode.DIRECTORY_LIST_FAILED, 1L);
                            visit.markParentRemaining();
                            break;
                        case DELETE_FAILED:
                            stats.deleteFailed(CleanupObjectType.DIRECTORY, 1L);
                            visit.markParentRemaining();
                            break;
                        default:
                            visit.markParentRemaining();
                            break;
                    }
                } else if (visit.parent != null) {
                    visit.parent.hasRemainingChild = true;
                }
                continue;
            }
            Optional<FileStatus[]> listing;
            try {
                listing = FileSystemProbe.listStatus(fs, visit.dir, remoteFsOpRateLimiter);
            } catch (IOException e) {
                LOG.warn("Failed to list directory: {}", visit.dir, e);
                audit.logFilesystemFailure(
                        AuditStage.SCAN,
                        task.scope(),
                        CleanupObjectType.DIRECTORY,
                        AuditFailureDetail.builder("list_directory", "directory_list_failed")
                                .targetPath(visit.dir)
                                .exceptionClass(e.getClass())
                                .retryable(true)
                                .actionRequired(true)
                                .build());
                stats.skipped(SkipReasonCode.DIRECTORY_LIST_FAILED, 1L);
                if (visit.parent != null) {
                    visit.parent.hasRemainingChild = true;
                }
                continue;
            }
            if (!listing.isPresent()) {
                audit.logSkippedDirectory(
                        visit.dir,
                        visit.modificationTime,
                        task.scope(),
                        "directory_not_found",
                        task.dryRun(),
                        false,
                        false);
                stats.skipped(SkipReasonCode.DIRECTORY_NOT_FOUND, 1L);
                continue;
            }
            FileStatus[] children = listing.get();
            visit.postOrder = true;
            stack.push(visit);
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    long modificationTime = child.getModificationTime();
                    if (MtimePolicy.isUnavailable(modificationTime)) {
                        stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
                        audit.logSkippedDirectory(
                                childPath,
                                modificationTime,
                                task.scope(),
                                "mtime_unavailable",
                                task.dryRun(),
                                false,
                                true);
                    }
                    stack.push(
                            new DirVisit(
                                    childPath,
                                    false,
                                    MtimePolicy.isOlderThanCutoff(
                                            modificationTime, task.cutoffMillis()),
                                    modificationTime,
                                    visit));
                    continue;
                }
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                CleanupObjectType objectType = rule.id().objectType();
                stats.scanned(objectType, 1L);
                progress.recordScannedFile(meta.size());
                stats.ruleDecision(objectType, RuleDecisionCounters.scanned(meta.size()));
                RuleEvaluation evaluation =
                        rule.evaluateDetailed(
                                meta, BucketActiveRefs.knownEmpty(), task.cutoffMillis());
                Decision decision =
                        MtimePolicy.failClosed(evaluation.decision(), meta.modificationTime());
                if (decision != evaluation.decision()) {
                    evaluation = RuleEvaluation.decision(decision, "mtime_unavailable");
                }
                switch (decision) {
                    case DELETE:
                        stats.ruleDecision(objectType, RuleDecisionCounters.candidate(meta.size()));
                        stats.planned(objectType, 1L, meta.size());
                        progress.recordPlannedFile(meta.size());
                        if (safeDeleter.deleteFile(
                                meta, decision, rule.id(), task.cutoffMillis())) {
                            if (!task.dryRun()) {
                                stats.deleted(objectType, 1L, meta.size());
                            }
                        } else {
                            stats.deleteFailed(objectType, 1L);
                            visit.hasRemainingChild = true;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.unknownFileType(meta.size()));
                        audit.logDecisionSample(
                                task.scope(),
                                meta,
                                rule.id(),
                                evaluation,
                                task.cutoffMillis(),
                                task.dryRun());
                        stats.skipped(SkipReasonCode.UNKNOWN_FILE_TYPE, 1L);
                        visit.hasRemainingChild = true;
                        break;
                    case KEEP_ACTIVE:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.keepActive(meta.size()));
                        audit.logDecisionSample(
                                task.scope(),
                                meta,
                                rule.id(),
                                evaluation,
                                task.cutoffMillis(),
                                task.dryRun());
                        stats.skipped(SkipReasonCode.KEEP_ACTIVE, 1L);
                        visit.hasRemainingChild = true;
                        break;
                    case MTIME_UNAVAILABLE:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.mtimeUnavailable(meta.size()));
                        stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
                        audit.logDecisionSample(
                                task.scope(),
                                meta,
                                rule.id(),
                                evaluation,
                                task.cutoffMillis(),
                                task.dryRun());
                        visit.hasRemainingChild = true;
                        break;
                    case DEFER:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.newerThanCutoff(meta.size()));
                        audit.logDecisionSample(
                                task.scope(),
                                meta,
                                rule.id(),
                                evaluation,
                                task.cutoffMillis(),
                                task.dryRun());
                        stats.skipped(SkipReasonCode.NEWER_THAN_CUTOFF, 1L);
                        visit.hasRemainingChild = true;
                        break;
                    default:
                        visit.hasRemainingChild = true;
                        break;
                }
            }
        }

        return stats.build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SafeDeleter createSafeDeleter(
            FileSystem fs, boolean dryRun, ScopeIdentity scope, ScanTaskProgress progress) {
        return new SafeDeleter(fs, dryRun, audit, remoteFsOpRateLimiter, scope, progress);
    }

    private void mergeTaskProgress(ScanTaskProgress progress) {
        subtaskCounters = subtaskCounters.add(progress.snapshot());
        subtaskScannedBytes += progress.scannedBytes();
    }

    static double perSubtaskRate(long totalRate, int parallelism) {
        return ((double) totalRate) / parallelism;
    }

    private void closeAuditRuntime() {
        closeAuditRuntime(true);
    }

    private void closeAuditRuntime(boolean emitSummary) {
        AuditReporterRuntime runtime = auditRuntime;
        AuditLogger logger = audit;
        auditRuntime = null;
        audit = null;

        RuntimeException failure = null;
        try {
            if (logger != null && emitSummary) {
                logger.logScanSubtaskSummary(
                        Math.max(0L, System.currentTimeMillis() - scanStartMillis),
                        tasksCompleted,
                        subtaskCounters,
                        subtaskScannedBytes);
            }
        } catch (RuntimeException summaryFailure) {
            failure = summaryFailure;
        }
        try {
            if (logger != null) {
                logger.flushDiagnosticSamplingSummaries();
            }
        } catch (RuntimeException samplingFailure) {
            if (failure == null) {
                failure = samplingFailure;
            } else {
                failure.addSuppressed(samplingFailure);
            }
        }
        if (runtime != null) {
            try {
                runtime.flush();
            } catch (RuntimeException flushFailure) {
                if (failure == null) {
                    failure = flushFailure;
                } else {
                    failure.addSuppressed(flushFailure);
                }
            }
        }
        if (runtime != null) {
            try {
                runtime.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static final class DirVisit {
        private final FsPath dir;
        private boolean postOrder;
        private final boolean oldEnough;
        private final long modificationTime;
        private final DirVisit parent;
        private boolean hasRemainingChild;

        private DirVisit(
                FsPath dir,
                boolean postOrder,
                boolean oldEnough,
                long modificationTime,
                DirVisit parent) {
            this.dir = dir;
            this.postOrder = postOrder;
            this.oldEnough = oldEnough;
            this.modificationTime = modificationTime;
            this.parent = parent;
        }

        private void markParentRemaining() {
            if (parent != null) {
                parent.hasRemainingChild = true;
            }
        }
    }
}
