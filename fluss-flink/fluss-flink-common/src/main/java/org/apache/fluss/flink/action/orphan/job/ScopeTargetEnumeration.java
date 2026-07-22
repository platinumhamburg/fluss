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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.flink.action.orphan.audit.AuditFailureDetail;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.build.ActiveRefsFetcher;
import org.apache.fluss.flink.action.orphan.build.KvActiveRefsFetchResult;
import org.apache.fluss.flink.action.orphan.build.KvSharedSstFetchResult;
import org.apache.fluss.flink.action.orphan.build.LogActiveRefsFetchResult;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.normalizeRoot;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.physicalPath;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.remoteSubDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveRemoteDataDir;

/** Serial enumeration of one live table or partition target without caller-owned side effects. */
final class ScopeTargetEnumeration {

    private ScopeTargetEnumeration() {}

    interface Worker {
        Result enumerate(Input input) throws Exception;
    }

    static final class EnumerationException extends Exception {
        private static final long serialVersionUID = 1L;

        private final Result partialResult;

        EnumerationException(Result partialResult, Throwable failure) {
            super(failure);
            this.partialResult = partialResult;
        }

        Result partialResult() {
            return partialResult;
        }

        Throwable originalFailure() {
            return getCause();
        }

        void rethrowOriginal() throws Exception {
            Throwable failure = originalFailure();
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    static Worker worker(Admin admin, RateLimiter remoteFsOpRateLimiter) {
        return new TargetWorker(admin, remoteFsOpRateLimiter);
    }

    static final class Input {
        private final String databaseName;
        private final String tableName;
        private final long tableId;
        private final TablePath tablePath;
        private final TableInfo tableInfo;
        @Nullable private final PartitionInfo partitionInfo;
        private final List<TableBucket> buckets;
        @Nullable private final String remoteDataDir;
        private final List<String> clusterRoots;
        private final long cutoffMillis;
        private final boolean dryRun;
        private final boolean allowDeleteManifest;

        Input(
                String databaseName,
                String tableName,
                long tableId,
                TablePath tablePath,
                TableInfo tableInfo,
                @Nullable PartitionInfo partitionInfo,
                List<TableBucket> buckets,
                @Nullable String remoteDataDir,
                List<String> clusterRoots,
                long cutoffMillis,
                boolean dryRun,
                boolean allowDeleteManifest) {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.tableInfo = tableInfo;
            this.partitionInfo = partitionInfo;
            this.buckets = Collections.unmodifiableList(new ArrayList<TableBucket>(buckets));
            this.remoteDataDir = remoteDataDir;
            this.clusterRoots = Collections.unmodifiableList(new ArrayList<String>(clusterRoots));
            this.cutoffMillis = cutoffMillis;
            this.dryRun = dryRun;
            this.allowDeleteManifest = allowDeleteManifest;
        }

        ScopeIdentity scope() {
            Long partitionId = partitionInfo == null ? null : partitionInfo.getPartitionId();
            return ScopeIdentity.table(databaseName, tableName, tableId)
                    .withPartitionAndBucket(partitionId, null);
        }
    }

    static final class Result {
        private final ScopeTargetStats targetStats;
        private final ScopePlanStats planDelta;
        private final List<CleanTask> tasks;
        private final List<Diagnostic> diagnostics;
        private final boolean finishTargetOnReplay;
        private final long targetStartMillis;
        private final boolean targetComplete;

        private Result(
                ScopeTargetStats targetStats,
                ScopePlanStats planDelta,
                List<CleanTask> tasks,
                List<Diagnostic> diagnostics,
                boolean finishTargetOnReplay,
                long targetStartMillis,
                boolean targetComplete) {
            this.targetStats = targetStats.snapshot();
            this.planDelta = new ScopePlanStats();
            this.planDelta.mergeFrom(planDelta);
            this.tasks = Collections.unmodifiableList(new ArrayList<CleanTask>(tasks));
            this.diagnostics = Collections.unmodifiableList(new ArrayList<Diagnostic>(diagnostics));
            this.finishTargetOnReplay = finishTargetOnReplay;
            this.targetStartMillis = targetStartMillis;
            this.targetComplete = targetComplete;
        }

        static Builder builder(ScopeTargetStats targetStats) {
            return new Builder(targetStats);
        }

        static Result empty(ScopeIdentity scope) {
            return builder(new ScopeTargetStats(scope, 0L, false)).build();
        }

        ScopeTargetStats targetStats() {
            return targetStats.snapshot();
        }

        void replay(AuditLogger audit, ScopePlanStats total, Consumer<CleanTask> collector) {
            replay(audit, total, collector, System::currentTimeMillis);
        }

        void replay(
                AuditLogger audit,
                ScopePlanStats total,
                Consumer<CleanTask> collector,
                LongSupplier clock) {
            total.mergeFrom(planDelta);
            ScopeTargetStats replayStats = targetStats.snapshot();
            try {
                for (Diagnostic diagnostic : diagnostics) {
                    diagnostic.emit(audit);
                }
                for (CleanTask task : tasks) {
                    collector.accept(task);
                }
            } finally {
                if (finishTargetOnReplay) {
                    long durationMillis = Math.max(0L, clock.getAsLong() - targetStartMillis);
                    if (targetComplete) {
                        replayStats.complete(durationMillis);
                    } else {
                        replayStats.incomplete(durationMillis);
                    }
                }
                total.target(replayStats);
                audit.logScopeTargetSummary(replayStats);
            }
        }

        static final class Builder {
            private final ScopeTargetStats targetStats;
            private final ScopePlanStats planDelta = new ScopePlanStats();
            private final List<CleanTask> tasks = new ArrayList<CleanTask>();
            private final List<Diagnostic> diagnostics = new ArrayList<Diagnostic>();
            private boolean finishTargetOnReplay;
            private long targetStartMillis;
            private boolean targetComplete;

            private Builder(ScopeTargetStats targetStats) {
                this.targetStats = targetStats;
            }

            Builder discoveredBuckets(long count) {
                planDelta.discoveredBuckets(count);
                return this;
            }

            Builder task(CleanTask task) {
                tasks.add(task);
                planDelta.bucketTask();
                targetStats.taskEmitted();
                return this;
            }

            Builder rpcFailure(
                    ScopeIdentity scope, CleanupObjectType objectType, AuditFailureDetail failure) {
                diagnostics.add(new RpcFailureDiagnostic(scope, objectType, failure));
                return this;
            }

            Builder metadataFailure() {
                planDelta.metadataFailure();
                return this;
            }

            Builder metadataFailure(
                    ScopeIdentity scope, CleanupObjectType objectType, AuditFailureDetail failure) {
                diagnostics.add(new MetadataFailureDiagnostic(scope, objectType, failure));
                return metadataFailure();
            }

            Builder outOfScopeRoot(long tableId, @Nullable Long partitionId, String root) {
                diagnostics.add(new OutOfScopeRootDiagnostic(tableId, partitionId, root));
                planDelta.skippedOutOfScopeRoot();
                return this;
            }

            Builder scanLogBucketWithoutManifest(
                    long tableId, @Nullable Long partitionId, int bucketId) {
                diagnostics.add(
                        new ScanLogBucketWithoutManifestDiagnostic(tableId, partitionId, bucketId));
                return this;
            }

            Builder scanKvBucketWithoutActiveSnapshots(
                    long tableId, @Nullable Long partitionId, int bucketId) {
                diagnostics.add(
                        new ScanKvBucketWithoutActiveSnapshotsDiagnostic(
                                tableId, partitionId, bucketId));
                return this;
            }

            Builder targetTiming(long startMillis, boolean complete) {
                finishTargetOnReplay = true;
                targetStartMillis = startMillis;
                targetComplete = complete;
                return this;
            }

            Result build() {
                return new Result(
                        targetStats,
                        planDelta,
                        tasks,
                        diagnostics,
                        finishTargetOnReplay,
                        targetStartMillis,
                        targetComplete);
            }
        }
    }

    interface Diagnostic {
        void emit(AuditLogger audit);
    }

    private static final class RpcFailureDiagnostic implements Diagnostic {
        private final ScopeIdentity scope;
        private final CleanupObjectType objectType;
        private final AuditFailureDetail failure;

        private RpcFailureDiagnostic(
                ScopeIdentity scope, CleanupObjectType objectType, AuditFailureDetail failure) {
            this.scope = scope;
            this.objectType = objectType;
            this.failure = failure;
        }

        @Override
        public void emit(AuditLogger audit) {
            audit.logRpcFailure(AuditStage.SCOPE, scope, objectType, failure);
        }
    }

    private static final class MetadataFailureDiagnostic implements Diagnostic {
        private final ScopeIdentity scope;
        private final CleanupObjectType objectType;
        private final AuditFailureDetail failure;

        private MetadataFailureDiagnostic(
                ScopeIdentity scope, CleanupObjectType objectType, AuditFailureDetail failure) {
            this.scope = scope;
            this.objectType = objectType;
            this.failure = failure;
        }

        @Override
        public void emit(AuditLogger audit) {
            audit.logMetadataFailure(AuditStage.SCOPE, scope, objectType, failure);
        }
    }

    private static final class OutOfScopeRootDiagnostic implements Diagnostic {
        private final long tableId;
        @Nullable private final Long partitionId;
        private final String root;

        private OutOfScopeRootDiagnostic(long tableId, @Nullable Long partitionId, String root) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.root = root;
        }

        @Override
        public void emit(AuditLogger audit) {
            audit.logSkipBucketOutOfScope(tableId, partitionId, root);
        }
    }

    private static final class ScanLogBucketWithoutManifestDiagnostic implements Diagnostic {
        private final long tableId;
        @Nullable private final Long partitionId;
        private final int bucketId;

        private ScanLogBucketWithoutManifestDiagnostic(
                long tableId, @Nullable Long partitionId, int bucketId) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.bucketId = bucketId;
        }

        @Override
        public void emit(AuditLogger audit) {
            audit.logScanLogBucketWithoutManifest(tableId, partitionId, bucketId);
        }
    }

    private static final class ScanKvBucketWithoutActiveSnapshotsDiagnostic implements Diagnostic {
        private final long tableId;
        @Nullable private final Long partitionId;
        private final int bucketId;

        private ScanKvBucketWithoutActiveSnapshotsDiagnostic(
                long tableId, @Nullable Long partitionId, int bucketId) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.bucketId = bucketId;
        }

        @Override
        public void emit(AuditLogger audit) {
            audit.logScanKvBucketWithoutActiveSnapshots(tableId, partitionId, bucketId);
        }
    }

    private static final class TargetWorker implements Worker {
        private final ActiveRefsFetcher fetcher;

        private TargetWorker(Admin admin, RateLimiter remoteFsOpRateLimiter) {
            this.fetcher = new ActiveRefsFetcher(admin, 3, remoteFsOpRateLimiter);
        }

        @Override
        public Result enumerate(Input input) throws Exception {
            ScopeTargetStats targetStats =
                    new ScopeTargetStats(
                            input.scope(), input.buckets.size(), input.tableInfo.hasPrimaryKey());
            Result.Builder result = Result.builder(targetStats);
            result.discoveredBuckets(input.buckets.size());
            long targetStartMillis = System.currentTimeMillis();
            boolean targetComplete = false;
            Throwable targetFailure = null;
            try {
                enumerate(input, targetStats, result);
                targetComplete =
                        targetStats.logCoverageConsistent()
                                && targetStats.kvCoverageConsistent()
                                && !targetStats.hasCoverageFailure();
            } catch (Throwable failure) {
                targetFailure = failure;
            }
            Result enumerationResult =
                    result.targetTiming(targetStartMillis, targetComplete).build();
            if (targetFailure != null) {
                throw new EnumerationException(enumerationResult, targetFailure);
            }
            return enumerationResult;
        }

        private void enumerate(Input input, ScopeTargetStats targetStats, Result.Builder result) {
            Long partitionId =
                    input.partitionInfo == null ? null : input.partitionInfo.getPartitionId();
            ScopeIdentity targetScope = targetStats.scope();
            String resolvedRemoteDataDir =
                    resolveRemoteDataDir(input.tableInfo, input.partitionInfo, input.remoteDataDir);

            if (!input.clusterRoots.contains(normalizeRoot(resolvedRemoteDataDir))) {
                targetStats.outOfScope();
                result.outOfScopeRoot(input.tableId, partitionId, resolvedRemoteDataDir);
                return;
            }

            LogActiveRefsFetchResult logResult =
                    fetcher.fetchLogActiveRefsByBucket(input.tableId, partitionId);
            if (!logResult.listOk()) {
                targetStats.logRpcFailed();
                result.rpcFailure(
                                targetScope,
                                CleanupObjectType.LOG_MANIFEST,
                                logResult.listFailureDetail())
                        .metadataFailure();
            }

            Map<Integer, Set<String>> kvActiveByBucket = Collections.emptyMap();
            boolean kvTargetOk = false;
            if (input.tableInfo.hasPrimaryKey()) {
                KvActiveRefsFetchResult kvResult =
                        fetcher.fetchKvActiveSnapDirs(input.tableId, partitionId);
                if (kvResult.listOk()) {
                    kvActiveByBucket = new HashMap<>(kvResult.activeSnapDirsByBucket());
                    kvTargetOk = true;
                } else {
                    targetStats.kvRpcFailed();
                    result.rpcFailure(
                                    targetScope,
                                    CleanupObjectType.KV_SNAPSHOT_FILE,
                                    kvResult.listFailureDetail())
                            .metadataFailure();
                }
            }

            FsPath remoteLogDir =
                    remoteSubDir(resolvedRemoteDataDir, FlussPaths.REMOTE_LOG_DIR_NAME);
            FsPath remoteKvDir = remoteSubDir(resolvedRemoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME);

            for (TableBucket tableBucket : input.buckets) {
                enumerateBucket(
                        input,
                        targetStats,
                        result,
                        partitionId,
                        targetScope,
                        logResult,
                        kvActiveByBucket,
                        kvTargetOk,
                        remoteLogDir,
                        remoteKvDir,
                        tableBucket);
            }
        }

        private void enumerateBucket(
                Input input,
                ScopeTargetStats targetStats,
                Result.Builder result,
                @Nullable Long partitionId,
                ScopeIdentity targetScope,
                LogActiveRefsFetchResult logResult,
                Map<Integer, Set<String>> kvActiveByBucket,
                boolean kvTargetOk,
                FsPath remoteLogDir,
                FsPath remoteKvDir,
                TableBucket tableBucket) {
            int bucketId = tableBucket.getBucket();
            String logTabletDir = null;
            Set<String> logSegmentRelativePaths = Collections.emptySet();
            Set<String> logActiveManifestPaths = Collections.emptySet();

            if (logResult.listOk()) {
                switch (logResult.statusFor(bucketId)) {
                    case RESOLVED:
                        targetStats.logResolvedBucket();
                        logTabletDir =
                                FlussPaths.remoteLogTabletDir(
                                                remoteLogDir,
                                                physicalPath(input.tablePath, input.partitionInfo),
                                                tableBucket)
                                        .toString();
                        logSegmentRelativePaths =
                                logResult.activeRefsOf(bucketId).logSegmentRelativePaths();
                        logActiveManifestPaths =
                                logResult.activeRefsOf(bucketId).logActiveManifestPaths();
                        break;
                    case READ_FAILED:
                        targetStats.logReadFailedBucket();
                        result.metadataFailure(
                                targetScope.withPartitionAndBucket(partitionId, bucketId),
                                CleanupObjectType.LOG_MANIFEST,
                                logResult.readFailureDetail(bucketId));
                        break;
                    case NOT_LISTED:
                        targetStats.logNoManifestBucket();
                        logTabletDir =
                                FlussPaths.remoteLogTabletDir(
                                                remoteLogDir,
                                                physicalPath(input.tablePath, input.partitionInfo),
                                                tableBucket)
                                        .toString();
                        result.scanLogBucketWithoutManifest(input.tableId, partitionId, bucketId);
                        break;
                    default:
                        break;
                }
            }

            String kvTabletDir = null;
            Set<String> kvActiveSnaps = Collections.emptySet();
            Set<String> kvSharedSstFileNames = Collections.emptySet();
            boolean kvSharedSstRefsComplete = false;
            if (kvTargetOk) {
                kvTabletDir =
                        FlussPaths.remoteKvTabletDir(
                                        remoteKvDir,
                                        physicalPath(input.tablePath, input.partitionInfo),
                                        tableBucket)
                                .toString();
                kvActiveSnaps = kvActiveByBucket.getOrDefault(bucketId, Collections.emptySet());
                KvSharedSstFetchResult sstResult =
                        fetcher.fetchKvSharedSstFileNamesWithRefresh(
                                input.tableId,
                                partitionId,
                                bucketId,
                                new FsPath(kvTabletDir),
                                kvActiveByBucket);
                kvActiveSnaps = kvActiveByBucket.getOrDefault(bucketId, Collections.emptySet());
                if (kvActiveSnaps.isEmpty()) {
                    targetStats.kvEmptyBucket();
                    result.scanKvBucketWithoutActiveSnapshots(input.tableId, partitionId, bucketId);
                } else {
                    targetStats.kvActiveBucket();
                }
                if (sstResult.allMetadataReadOk()) {
                    kvSharedSstFileNames = sstResult.sharedSstFileNames();
                    kvSharedSstRefsComplete = true;
                } else {
                    targetStats.metadataFailure();
                    result.metadataFailure(
                            targetScope.withPartitionAndBucket(partitionId, bucketId),
                            CleanupObjectType.KV_SHARED_SST,
                            sstResult.failureDetail());
                }
            }

            if (logTabletDir == null && kvTabletDir == null) {
                return;
            }

            result.task(
                    new BucketCleanTask(
                            targetScope.withPartitionAndBucket(partitionId, bucketId),
                            logTabletDir,
                            kvTabletDir,
                            logSegmentRelativePaths,
                            logActiveManifestPaths,
                            kvActiveSnaps,
                            kvSharedSstFileNames,
                            kvSharedSstRefsComplete,
                            input.cutoffMillis,
                            input.dryRun,
                            input.allowDeleteManifest));
        }
    }
}
