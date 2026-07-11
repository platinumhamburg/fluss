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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;

/**
 * Per-bucket orphan cleanup for live buckets: walks the provided bucket directories and dispatches
 * each file to the appropriate {@link FileRule} using the caller-supplied active reference set.
 *
 * <p>All deletions go through {@link SafeDeleter} (no recursive deletes). Unknown file types are
 * skipped with an audit warning per the design's "unknown-types-not-deleted" principle.
 */
@Internal
public final class BucketCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(BucketCleaner.class);

    private final RuleDispatcher dispatcher;
    private final SafeDeleter safeDeleter;
    private final AuditLogger audit;
    private final long cutoffMillis;
    private final RateLimiter remoteFsOpRateLimiter;
    private final boolean dryRun;

    public BucketCleaner(
            RuleDispatcher dispatcher,
            SafeDeleter safeDeleter,
            AuditLogger audit,
            long cutoffMillis,
            RateLimiter remoteFsOpRateLimiter,
            boolean dryRun) {
        this.dispatcher = dispatcher;
        this.safeDeleter = safeDeleter;
        this.audit = audit;
        this.cutoffMillis = cutoffMillis;
        this.remoteFsOpRateLimiter = remoteFsOpRateLimiter;
        this.dryRun = dryRun;
    }

    /** Cleans one bucket's log/kv subtrees using the caller-supplied active reference set. */
    public BucketCleanStats clean(BucketActiveRefs activeRefs, FsPath... bucketDirs)
            throws IOException {
        BucketCleanStats stats = BucketCleanStats.empty();
        for (FsPath bucketDir : bucketDirs) {
            if (bucketDir != null) {
                walkAndCleanDir(bucketDir, activeRefs, stats);
            }
        }
        return stats;
    }

    private void walkAndCleanDir(FsPath root, BucketActiveRefs activeRefs, BucketCleanStats stats)
            throws IOException {
        FileSystem fs = root.getFileSystem();
        remoteFsOpRateLimiter.acquire();
        if (!fs.exists(root)) {
            return;
        }
        Deque<DirVisit> stack = new ArrayDeque<DirVisit>();
        stack.push(new DirVisit(root, false, false, -1L, null, true));
        while (!stack.isEmpty()) {
            DirVisit visit = stack.pop();
            if (visit.postOrder) {
                boolean plannedRemoval = visit.oldEnough && !visit.hasRemainingChild;
                if (plannedRemoval) {
                    stats.recordPlannedDirectory();
                    SafeDeleter.DirectoryDeleteResult result =
                            safeDeleter.deleteEmptyDirDetailed(visit.dir, visit.modificationTime);
                    switch (result) {
                        case SUCCESS:
                            if (!dryRun) {
                                stats.recordRemovedDirectory();
                            }
                            break;
                        case NOT_EMPTY:
                            stats.recordSkip(SkipReasonCode.DIRECTORY_NOT_EMPTY);
                            visit.markParentRemaining();
                            break;
                        case LIST_FAILED:
                            stats.recordSkip(SkipReasonCode.DIRECTORY_LIST_FAILED);
                            visit.markParentRemaining();
                            break;
                        case DELETE_FAILED:
                            stats.recordDeleteFailure(CleanupObjectType.DIRECTORY);
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
                stats.recordSkip(SkipReasonCode.DIRECTORY_LIST_FAILED);
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
            if (!visit.root) {
                visit.postOrder = true;
                stack.push(visit);
            }
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    stack.push(
                            new DirVisit(
                                    childPath,
                                    false,
                                    child.getModificationTime() < cutoffMillis,
                                    child.getModificationTime(),
                                    visit,
                                    false));
                    continue;
                }
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                Decision decision = rule.evaluate(meta, activeRefs, cutoffMillis);
                CleanupObjectType objectType = rule.id().objectType();
                stats.recordScanned(objectType);
                switch (decision) {
                    case DELETE:
                        stats.recordPlanned(objectType, meta.size());
                        if (safeDeleter.deleteFile(meta, decision, rule.id())) {
                            if (!dryRun) {
                                stats.recordDeleted(objectType, meta.size());
                            }
                        } else {
                            stats.recordDeleteFailure(objectType);
                            visit.hasRemainingChild = true;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        audit.logSkipUnknown(meta.path(), rule.id());
                        stats.recordSkip(SkipReasonCode.UNKNOWN_FILE_TYPE);
                        visit.hasRemainingChild = true;
                        break;
                    case KEEP_ACTIVE:
                        stats.recordSkip(SkipReasonCode.KEEP_ACTIVE);
                        visit.hasRemainingChild = true;
                        break;
                    case DEFER:
                        stats.recordSkip(SkipReasonCode.NEWER_THAN_CUTOFF);
                        visit.hasRemainingChild = true;
                        break;
                    default:
                        visit.hasRemainingChild = true;
                        break;
                }
            }
        }
    }

    /** Per-bucket cleanup statistics. */
    public static final class BucketCleanStats {
        public long scannedFiles;
        public long plannedFiles;
        public long plannedDirs;
        public long plannedBytes;
        public long deletedFiles;
        public long emptyDirsRemoved;
        public long deleteFailures;
        public long bytesReclaimed;
        public final Map<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        public final Map<SkipReasonCode, Long> bySkipReason = new EnumMap<>(SkipReasonCode.class);

        public static BucketCleanStats empty() {
            return new BucketCleanStats();
        }

        private void recordScanned(CleanupObjectType type) {
            scannedFiles++;
            addByObjectType(type, new CleanupCounters(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
        }

        private void recordPlanned(CleanupObjectType type, long bytes) {
            plannedFiles++;
            plannedBytes += bytes;
            addByObjectType(type, new CleanupCounters(0L, 1L, 0L, bytes, 0L, 0L, 0L, 0L));
        }

        private void recordDeleted(CleanupObjectType type, long bytes) {
            deletedFiles++;
            bytesReclaimed += bytes;
            addByObjectType(type, new CleanupCounters(0L, 0L, 0L, 0L, 1L, 0L, 0L, bytes));
        }

        private void recordDeleteFailure(CleanupObjectType type) {
            deleteFailures++;
            addByObjectType(type, new CleanupCounters(0L, 0L, 0L, 0L, 0L, 0L, 1L, 0L));
        }

        private void recordPlannedDirectory() {
            plannedDirs++;
            addByObjectType(
                    CleanupObjectType.DIRECTORY,
                    new CleanupCounters(0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L));
        }

        private void recordRemovedDirectory() {
            emptyDirsRemoved++;
            addByObjectType(
                    CleanupObjectType.DIRECTORY,
                    new CleanupCounters(0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L));
        }

        private void recordSkip(SkipReasonCode reason) {
            bySkipReason.put(reason, bySkipReason.getOrDefault(reason, 0L) + 1L);
        }

        private void addByObjectType(CleanupObjectType type, CleanupCounters delta) {
            byObjectType.put(
                    type, byObjectType.getOrDefault(type, CleanupCounters.empty()).add(delta));
        }
    }

    private static final class DirVisit {
        private final FsPath dir;
        private boolean postOrder;
        private final boolean oldEnough;
        private final long modificationTime;
        private final DirVisit parent;
        private final boolean root;
        private boolean hasRemainingChild;

        private DirVisit(
                FsPath dir,
                boolean postOrder,
                boolean oldEnough,
                long modificationTime,
                DirVisit parent,
                boolean root) {
            this.dir = dir;
            this.postOrder = postOrder;
            this.oldEnough = oldEnough;
            this.modificationTime = modificationTime;
            this.parent = parent;
            this.root = root;
        }

        private void markParentRemaining() {
            if (parent != null) {
                parent.hasRemainingChild = true;
            }
        }
    }
}
