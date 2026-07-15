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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.MtimePolicy;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

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
public final class ScanAndCleanFunction extends ProcessFunction<CleanTask, CleanupStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScanAndCleanFunction.class);

    private final long remoteFsOpRateLimitPerSecond;
    private final Map<String, String> extraConfigs;

    private transient AuditLogger audit;
    private transient RateLimiter remoteFsOpRateLimiter;

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond, Map<String, String> extraConfigs) {
        this.remoteFsOpRateLimitPerSecond = remoteFsOpRateLimitPerSecond;
        this.extraConfigs = extraConfigs;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext)
            throws Exception {
        super.open(openContext);
        if (!extraConfigs.isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(extraConfigs), null);
        }
        audit = new AuditLogger();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        // Distribute the configured rate as base + 1 extra for the first `remainder` subtasks.
        // Flink does not provide a cross-JVM limiter here, so this is a best-effort job-level
        // target. Each subtask gets at least 1/s; if parallelism exceeds the configured rate, the
        // effective aggregate can exceed the target by that floor.
        remoteFsOpRateLimiter =
                RateLimiter.create(
                        perSubtaskRate(remoteFsOpRateLimitPerSecond, parallelism, subtaskIndex));
    }

    @Override
    public void processElement(CleanTask task, Context ctx, Collector<CleanupStats> out)
            throws Exception {
        if (task instanceof ScopeSummaryTask) {
            out.collect(((ScopeSummaryTask) task).stats());
        } else if (task instanceof BucketCleanTask) {
            out.collect(processBucketTask((BucketCleanTask) task));
        } else if (task instanceof OrphanDirCleanTask) {
            out.collect(processOrphanDirTask((OrphanDirCleanTask) task));
        }
    }

    // -------------------------------------------------------------------------
    // BucketCleanTask processing
    // -------------------------------------------------------------------------

    private CleanupStats processBucketTask(BucketCleanTask task) throws IOException {
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
                        task.logActiveManifestPaths());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());
        SafeDeleter safeDeleter =
                createSafeDeleter(anyDir.getFileSystem(), task.dryRun(), task.scope());
        BucketCleaner cleaner =
                new BucketCleaner(
                        dispatcher,
                        safeDeleter,
                        audit,
                        task.cutoffMillis(),
                        remoteFsOpRateLimiter,
                        task.dryRun(),
                        task.scope());

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

    private CleanupStats processOrphanDirTask(OrphanDirCleanTask task) throws IOException {
        FsPath dirPath = new FsPath(task.dirPath());
        FileSystem fs = dirPath.getFileSystem();
        remoteFsOpRateLimiter.acquire();
        if (!fs.exists(dirPath)) {
            return CleanupStats.emptyScan(task.scope());
        }

        SafeDeleter safeDeleter = createSafeDeleter(fs, task.dryRun(), task.scope());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());

        CleanupStats.Builder stats = CleanupStats.scanBuilder(task.scope());

        remoteFsOpRateLimiter.acquire();
        FileStatus rootStatus = fs.getFileStatus(dirPath);
        long rootModificationTime = rootStatus.getModificationTime();
        if (MtimePolicy.isUnavailable(rootModificationTime)) {
            stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
            audit.logMtimeUnavailableOnce(
                    task.scope(), CleanupObjectType.DIRECTORY, "directory", dirPath.getName());
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
                    stats.plannedDirectory(1L);
                    SafeDeleter.DirectoryDeleteResult result =
                            safeDeleter.deleteEmptyDirDetailed(visit.dir, visit.modificationTime);
                    switch (result) {
                        case SUCCESS:
                            if (!task.dryRun()) {
                                stats.removedDirectory(1L);
                            }
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
            FileStatus[] children;
            try {
                remoteFsOpRateLimiter.acquire();
                children = fs.listStatus(visit.dir);
            } catch (IOException e) {
                LOG.warn("Failed to list directory: {}", visit.dir, e);
                stats.skipped(SkipReasonCode.DIRECTORY_LIST_FAILED, 1L);
                if (visit.parent != null) {
                    visit.parent.hasRemainingChild = true;
                }
                continue;
            }
            if (children == null) {
                if (visit.parent != null) {
                    visit.parent.hasRemainingChild = true;
                }
                continue;
            }
            visit.postOrder = true;
            stack.push(visit);
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    long modificationTime = child.getModificationTime();
                    if (MtimePolicy.isUnavailable(modificationTime)) {
                        stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
                        audit.logMtimeUnavailableOnce(
                                task.scope(),
                                CleanupObjectType.DIRECTORY,
                                "directory",
                                childPath.getName());
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
                stats.ruleDecision(objectType, RuleDecisionCounters.scanned(meta.size()));
                Decision decision;
                if (MtimePolicy.isUnavailable(meta.modificationTime())) {
                    decision =
                            MtimePolicy.failClosed(
                                    rule.evaluate(
                                            meta, BucketActiveRefs.empty(), task.cutoffMillis()),
                                    meta.modificationTime());
                } else if (meta.modificationTime() >= task.cutoffMillis()) {
                    decision = Decision.DEFER;
                } else {
                    decision = rule.evaluate(meta, BucketActiveRefs.empty(), task.cutoffMillis());
                }
                switch (decision) {
                    case DELETE:
                        stats.ruleDecision(objectType, RuleDecisionCounters.candidate(meta.size()));
                        stats.planned(objectType, 1L, meta.size());
                        if (safeDeleter.deleteFile(meta, decision, rule.id())) {
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
                        audit.logSkipUnknown(meta.path(), rule.id());
                        stats.skipped(SkipReasonCode.UNKNOWN_FILE_TYPE, 1L);
                        visit.hasRemainingChild = true;
                        break;
                    case KEEP_ACTIVE:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.keepActive(meta.size()));
                        stats.skipped(SkipReasonCode.KEEP_ACTIVE, 1L);
                        visit.hasRemainingChild = true;
                        break;
                    case MTIME_UNAVAILABLE:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.mtimeUnavailable(meta.size()));
                        stats.skipped(SkipReasonCode.MTIME_UNAVAILABLE, 1L);
                        audit.logMtimeUnavailableOnce(
                                task.scope(), objectType, "file", meta.path().getName());
                        visit.hasRemainingChild = true;
                        break;
                    case DEFER:
                        stats.ruleDecision(
                                objectType, RuleDecisionCounters.newerThanCutoff(meta.size()));
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

    private SafeDeleter createSafeDeleter(FileSystem fs, boolean dryRun, ScopeIdentity scope) {
        return new SafeDeleter(fs, dryRun, audit, remoteFsOpRateLimiter, scope);
    }

    private static double perSubtaskRate(long totalRate, int parallelism, int subtaskIndex) {
        long base = totalRate / parallelism;
        long remainder = totalRate % parallelism;
        long quota = base + (subtaskIndex < remainder ? 1L : 0L);
        return Math.max(1.0, (double) quota);
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
