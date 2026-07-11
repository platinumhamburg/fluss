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
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.action.orphan.OrphanCleanUtils;
import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.action.orphan.build.ActiveRefsFetcher;
import org.apache.fluss.flink.action.orphan.build.KvActiveRefsFetchResult;
import org.apache.fluss.flink.action.orphan.build.KvSharedSstFetchResult;
import org.apache.fluss.flink.action.orphan.build.LogActiveRefsFetchResult;
import org.apache.fluss.flink.action.orphan.build.MaxKnownIdsTracker;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.rule.OrphanDirDetector;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.enumerateBuckets;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.fetchClusterConfigMap;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.normalizeRoot;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.physicalPath;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.remoteSubDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDirs;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveRemoteDataDir;

/**
 * Stage 1 of the orphan files cleanup job. Runs at parallelism=1 and concentrates all coordinator
 * RPC interaction in a single subtask.
 *
 * <p>For each live bucket, emits a {@link BucketCleanTask} containing the FS paths and manifest
 * locations needed for Stage 2 to execute cleanup without coordinator access. For each detected
 * orphan directory, emits an {@link OrphanDirCleanTask}.
 */
@Internal
public final class ScopeEnumeratorFunction extends ProcessFunction<Integer, CleanTask> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScopeEnumeratorFunction.class);
    private static final String[] TOP_LEVEL_DIRS = {
        FlussPaths.REMOTE_LOG_DIR_NAME, FlussPaths.REMOTE_KV_DIR_NAME
    };
    public static final OutputTag<TablePlanStats> TABLE_PLAN_STATS =
            new OutputTag<TablePlanStats>("table-plan-stats") {};

    private final OrphanCleanConfig config;
    private final String runId;
    private transient TablePlanTracker tablePlans;
    private transient ScopeProgressTracker scopeProgress;
    private transient ScopeHeartbeat scopeHeartbeat;

    public ScopeEnumeratorFunction(OrphanCleanConfig config, String runId) {
        this.config = config;
        this.runId = runId;
    }

    @Override
    public void processElement(Integer trigger, Context ctx, Collector<CleanTask> out)
            throws Exception {
        if (!config.extraConfigs().isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(config.extraConfigs()), null);
        }

        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.bootstrapServer());
        // Pass through client-related extra configs (e.g. security/auth).
        for (Map.Entry<String, String> entry : config.extraConfigs().entrySet()) {
            if (entry.getKey().startsWith("client.")) {
                flussConfig.setString(entry.getKey(), entry.getValue());
            }
        }

        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            // Fail fast on incompatible servers: the action jar may be deployed against an
            // older cluster that does not implement ListRemoteLogManifests / ListKvSnapshots.
            // Without this guard, every per-target fetch would degrade to skip_log_target /
            // skip_kv_target audit events and the job would exit "successfully" with
            // deleted=0, masking the incompatibility.
            verifyServerSupportsRequiredApis(admin);

            AuditLogger audit = new AuditLogger();
            ScopePlanStats planStats = new ScopePlanStats();
            tablePlans = new TablePlanTracker();
            audit.logRunStart(runId, config);
            audit.logCutoff(runId, config.olderThanMillis());
            scopeProgress =
                    new ScopeProgressTracker(
                            config.progressLogInterval(),
                            (phase, stats) -> audit.logScopeProgress(runId, phase, stats));
            scopeHeartbeat =
                    new ScopeHeartbeat(
                            config.progressLogInterval(),
                            planStats,
                            snapshot ->
                                    audit.logScopeHeartbeat(
                                            runId,
                                            snapshot.phase(),
                                            snapshot.completedTargets(),
                                            snapshot.totalTargets(),
                                            snapshot.database(),
                                            snapshot.table(),
                                            snapshot.tableId(),
                                            snapshot.partitionId(),
                                            snapshot.targetElapsedMillis(),
                                            snapshot.stats()));

            try {
                RateLimiter remoteFsOpRateLimiter =
                        RateLimiter.create((double) config.remoteFsOpRateLimitPerSecond());
                ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, 3, remoteFsOpRateLimiter);
                MaxKnownIdsTracker tracker = new MaxKnownIdsTracker();
                scopeHeartbeat.phase("cluster_metadata");
                audit.logScopePhase(runId, "cluster_metadata");
                Map<String, String> clusterConfigMap = fetchClusterConfigMap(admin);
                String clusterRemoteDataDir = resolveClusterRemoteDataDir(clusterConfigMap);
                List<String> clusterRoots =
                        normalizeRoots(resolveClusterRemoteDataDirs(clusterConfigMap));

                scopeHeartbeat.phase("active_metadata");
                audit.logScopePhase(runId, "active_metadata");
                Map<String, DbScanState> dbStates =
                        enumerateActiveScope(admin, audit, tracker, planStats);
                Set<Long> activeTableIds = collectActiveTableIds(dbStates);
                Set<Long> activePartitionIds = collectActivePartitionIds(dbStates);

                scopeHeartbeat.totalTargets(countBucketTargets(dbStates));
                scopeHeartbeat.phase("task_planning");
                audit.logScopePhase(runId, "task_planning");
                for (DbScanState dbState : dbStates.values()) {
                    for (LiveTableScope liveTable : dbState.liveTables) {
                        emitBucketTasks(
                                liveTable,
                                fetcher,
                                audit,
                                clusterRemoteDataDir,
                                clusterRoots,
                                planStats,
                                out);
                        emitOrphanPartitionDirTasks(
                                liveTable,
                                tracker,
                                clusterRoots,
                                audit,
                                remoteFsOpRateLimiter,
                                planStats,
                                out);
                    }
                    emitOrphanTableDirTasks(
                            dbState,
                            tracker,
                            clusterRoots,
                            audit,
                            remoteFsOpRateLimiter,
                            planStats,
                            out);
                }
                emitOrphanDirTasksUnderUnknownDatabases(
                        dbStates.keySet(),
                        activeTableIds,
                        activeTableIdsComplete(dbStates),
                        activePartitionIds,
                        activePartitionIdsComplete(dbStates),
                        tracker,
                        clusterRoots,
                        audit,
                        remoteFsOpRateLimiter,
                        planStats,
                        out);
                audit.logScopePlan(runId, planStats);
                for (TablePlanStats tablePlan : tablePlans.snapshots()) {
                    ctx.output(TABLE_PLAN_STATS, tablePlan);
                }
            } finally {
                scopeHeartbeat.close();
            }
        }
    }

    private static long countBucketTargets(Map<String, DbScanState> dbStates) {
        long targets = 0L;
        for (DbScanState dbState : dbStates.values()) {
            for (LiveTableScope liveTable : dbState.liveTables) {
                if (!liveTable.partitioned) {
                    targets++;
                } else if (liveTable.partitionInfosComplete) {
                    targets += liveTable.partitions.size();
                }
            }
        }
        return targets;
    }

    /** Normalizes each root in the list and returns a deduplicated ordered list. */
    private static List<String> normalizeRoots(List<String> roots) {
        LinkedHashSet<String> normalized = new LinkedHashSet<String>();
        for (String root : roots) {
            normalized.add(normalizeRoot(root));
        }
        return new ArrayList<String>(normalized);
    }

    /**
     * Probes the two RPCs this action depends on and throws if the connected server does not
     * implement them. A sentinel {@code tableId} of {@link Long#MAX_VALUE} is used so that on a
     * compatible server the call simply fails with a benign error (typically table-not-found),
     * whereas an incompatible server raises {@link UnsupportedVersionException} during ApiVersions
     * negotiation. Any non-{@code UnsupportedVersionException} outcome is treated as proof that the
     * RPC is recognized.
     */
    private static void verifyServerSupportsRequiredApis(Admin admin) {
        long sentinelTableId = Long.MAX_VALUE;
        probeApi(
                "ListRemoteLogManifests",
                () -> admin.listRemoteLogManifests(sentinelTableId, null).get());
        probeApi("ListKvSnapshots", () -> admin.listKvSnapshots(sentinelTableId, null).get());
    }

    private static void probeApi(String apiName, ThrowingProbe probe) {
        try {
            probe.run();
        } catch (Throwable t) {
            if (isUnsupportedVersion(t)) {
                throw new UnsupportedOperationException(
                        "Orphan files cleanup requires the Fluss server to support the "
                                + apiName
                                + " RPC, which the connected cluster does not. Upgrade the"
                                + " cluster to a version that exposes this RPC, or run an"
                                + " older orphan-files-cleanup action that targets this server.",
                        t);
            }
            if (isConnectionFailure(t)) {
                throw new IllegalStateException(
                        "Failed to connect to Fluss cluster while probing "
                                + apiName
                                + " RPC. The bootstrap server may be unreachable.",
                        t);
            }
            // Any other failure means the RPC is recognized; the call merely failed because of
            // the sentinel target id. Compatibility is satisfied.
        }
    }

    private static boolean isConnectionFailure(Throwable t) {
        Throwable cause = ExceptionUtils.stripExecutionException(t);
        while (cause != null) {
            if (cause instanceof NetworkException
                    || cause instanceof DisconnectException
                    || cause instanceof IOException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isUnsupportedVersion(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof UnsupportedVersionException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @FunctionalInterface
    private interface ThrowingProbe {
        void run() throws Exception;
    }

    // -------------------------------------------------------------------------
    // Scope enumeration (coordinator RPCs only)
    // -------------------------------------------------------------------------

    private Map<String, DbScanState> enumerateActiveScope(
            Admin admin, AuditLogger audit, MaxKnownIdsTracker tracker, ScopePlanStats planStats) {
        List<String> dbs = resolveDatabasesToScan(admin, audit, planStats);
        Map<String, DbScanState> result = new LinkedHashMap<String, DbScanState>();
        for (String dbName : dbs) {
            planStats.database();
            DbScanState dbState = new DbScanState(dbName);
            result.put(dbName, dbState);
            if (config.table().isPresent()) {
                dbState.tableInfosComplete = false;
                resolveTable(admin, audit, tracker, dbState, config.table().get(), true, planStats);
                scopeProgress.maybeLog("metadata", planStats);
                continue;
            }
            List<String> tableNames;
            try {
                tableNames = admin.listTables(dbName).get();
            } catch (Exception e) {
                audit.logSkipDb(dbName, classifyName(e));
                planStats.metadataFailure();
                dbState.tableInfosComplete = false;
                continue;
            }
            for (String tableName : tableNames) {
                resolveTable(admin, audit, tracker, dbState, tableName, false, planStats);
                scopeProgress.maybeLog("metadata", planStats);
            }
        }
        return result;
    }

    private List<String> resolveDatabasesToScan(
            Admin admin, AuditLogger audit, ScopePlanStats planStats) {
        if (config.allDatabases()) {
            try {
                return admin.listDatabases().get();
            } catch (Exception e) {
                audit.logSkipDb("*", classifyName(e));
                planStats.metadataFailure();
                throw new IllegalStateException(
                        "Failed to list databases from Fluss cluster. "
                                + "The coordinator server may be unreachable.",
                        e);
            }
        }
        String databaseName = config.database().get();
        try {
            if (admin.databaseExists(databaseName).get()) {
                return Collections.singletonList(databaseName);
            }
        } catch (Exception e) {
            audit.logSkipDb(databaseName, classifyName(e));
            planStats.metadataFailure();
            throw new IllegalStateException(
                    "Failed to check existence of database '"
                            + databaseName
                            + "'. "
                            + "The coordinator server may be unreachable.",
                    e);
        }
        audit.logSkipDb(databaseName, RpcErrorClassifier.Category.NOT_FOUND.name());
        return Collections.emptyList();
    }

    private void resolveTable(
            Admin admin,
            AuditLogger audit,
            MaxKnownIdsTracker tracker,
            DbScanState dbState,
            String tableName,
            boolean explicitTableTarget,
            ScopePlanStats planStats) {
        TablePath tablePath = TablePath.of(dbState.dbName, tableName);
        TableInfo tableInfo;
        try {
            tableInfo = admin.getTableInfo(tablePath).get();
        } catch (Exception e) {
            RpcErrorClassifier.Category category = RpcErrorClassifier.classify(e);
            ScopeIdentity unresolved = ScopeIdentity.unresolvedTable(dbState.dbName, tableName);
            tablePlans.ensure(unresolved);
            if (category == RpcErrorClassifier.Category.NOT_FOUND) {
                tablePlans.skip(unresolved, SkipReasonCode.TABLE_NOT_EXIST);
            } else {
                tablePlans.metadataFailure(unresolved);
            }
            if (category != RpcErrorClassifier.Category.NOT_FOUND || explicitTableTarget) {
                audit.logSkipTable(dbState.dbName, tableName, category.name());
                planStats.metadataFailure();
                dbState.tableInfosComplete = false;
            }
            return;
        }
        tracker.observeTableId(tableInfo.getTableId());
        dbState.activeTableIds.add(tableInfo.getTableId());

        LiveTableScope liveTable = new LiveTableScope(dbState.dbName, tableName, tableInfo);
        tablePlans.ensure(liveTable.scope());
        dbState.liveTables.add(liveTable);
        planStats.table();
        if (!tableInfo.isPartitioned()) {
            return;
        }
        try {
            List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
            TableInfo confirm = admin.getTableInfo(tablePath).get();
            if (confirm.getTableId() != tableInfo.getTableId()) {
                audit.logSkipTable(dbState.dbName, tableName, "table-recreated-during-enumeration");
                liveTable.partitionInfosComplete = false;
                tablePlans.metadataFailure(liveTable.scope());
                return;
            }
            for (PartitionInfo partition : partitions) {
                liveTable.partitions.add(partition);
                liveTable.activePartitionIds.add(partition.getPartitionId());
                tracker.observePartitionId(partition.getPartitionId());
                planStats.partition();
            }
        } catch (Exception e) {
            audit.logSkipPartitionList(dbState.dbName, tableName, classifyName(e));
            planStats.metadataFailure();
            liveTable.partitionInfosComplete = false;
            tablePlans.metadataFailure(liveTable.scope());
        }
    }

    // -------------------------------------------------------------------------
    // Emit BucketCleanTasks (per-target RPC + per-bucket task emission)
    // -------------------------------------------------------------------------

    private void emitBucketTasks(
            LiveTableScope liveTable,
            ActiveRefsFetcher fetcher,
            AuditLogger audit,
            @Nullable String clusterRemoteDataDir,
            List<String> clusterRoots,
            ScopePlanStats planStats,
            Collector<CleanTask> out) {
        if (liveTable.partitioned && !liveTable.partitionInfosComplete) {
            return;
        }
        List<PartitionInfo> partitionTargets =
                liveTable.partitioned
                        ? liveTable.partitions
                        : Collections.<PartitionInfo>singletonList(null);
        for (PartitionInfo partitionInfo : partitionTargets) {
            emitBucketTasksForTarget(
                    liveTable,
                    partitionInfo,
                    fetcher,
                    audit,
                    clusterRemoteDataDir,
                    clusterRoots,
                    planStats,
                    out);
        }
    }

    private void emitBucketTasksForTarget(
            LiveTableScope liveTable,
            @Nullable PartitionInfo partitionInfo,
            ActiveRefsFetcher fetcher,
            AuditLogger audit,
            @Nullable String clusterRemoteDataDir,
            List<String> clusterRoots,
            ScopePlanStats planStats,
            Collector<CleanTask> out) {
        Long partitionId = partitionInfo == null ? null : partitionInfo.getPartitionId();

        String remoteDataDir =
                resolveRemoteDataDir(liveTable.tableInfo, partitionInfo, clusterRemoteDataDir);

        // Scope guard: skip this target if its metadata-resolved root is not part of the
        // cluster's configured remote data directories.
        if (!clusterRoots.contains(normalizeRoot(remoteDataDir))) {
            audit.logSkipBucketOutOfScope(liveTable.tableId, partitionId, remoteDataDir);
            planStats.skippedOutOfScopeRoot();
            tablePlans.skip(liveTable.scope(), SkipReasonCode.OUT_OF_SCOPE_ROOT);
            scopeHeartbeat.targetComplete();
            return;
        }

        scopeHeartbeat.targetStart(
                liveTable.tablePath.getDatabaseName(),
                liveTable.tablePath.getTableName(),
                liveTable.tableId,
                partitionId);
        audit.logScopeTargetStart(
                runId,
                liveTable.tablePath.getDatabaseName(),
                liveTable.tablePath.getTableName(),
                liveTable.tableId,
                partitionId);
        long targetStartNanos = System.nanoTime();
        LogActiveRefsFetchResult logResult =
                fetcher.fetchLogActiveRefsByBucket(liveTable.tableId, partitionId);
        if (!logResult.listOk()) {
            audit.logSkipLogTarget(liveTable.tableId, partitionId, logResult.listFailureReason());
            planStats.metadataFailure();
            tablePlans.metadataFailure(liveTable.scope());
        }

        Map<Integer, Set<String>> kvActiveByBucket = Collections.emptyMap();
        boolean kvTargetOk = false;
        String kvStatus = "not_applicable";
        if (liveTable.tableInfo.hasPrimaryKey()) {
            KvActiveRefsFetchResult kvResult =
                    fetcher.fetchKvActiveSnapDirs(liveTable.tableId, partitionId);
            if (kvResult.listOk()) {
                kvActiveByBucket = kvResult.activeSnapDirsByBucket();
                kvTargetOk = true;
                kvStatus = "ok";
            } else {
                kvStatus = "failed";
                audit.logSkipKvTarget(liveTable.tableId, partitionId, kvResult.listFailureReason());
                planStats.metadataFailure();
                tablePlans.metadataFailure(liveTable.scope());
            }
        }
        audit.logScopeTargetComplete(
                runId,
                liveTable.tablePath.getDatabaseName(),
                liveTable.tablePath.getTableName(),
                liveTable.tableId,
                partitionId,
                logResult.listOk() ? "ok" : "failed",
                kvStatus,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - targetStartNanos));
        scopeHeartbeat.targetComplete();

        FsPath remoteLogDir = remoteSubDir(remoteDataDir, FlussPaths.REMOTE_LOG_DIR_NAME);
        FsPath remoteKvDir = remoteSubDir(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME);

        for (TableBucket tableBucket : enumerateBuckets(liveTable.tableInfo, partitionInfo)) {
            planStats.discoveredBucket();
            scopeProgress.maybeLog("bucket_tasks", planStats);
            int bucketId = tableBucket.getBucket();

            String logTabletDir = null;

            Set<String> logSegmentRelativePaths = Collections.emptySet();
            Set<String> logActiveManifestPaths = Collections.emptySet();

            if (logResult.listOk()) {
                switch (logResult.statusFor(bucketId)) {
                    case RESOLVED:
                        logTabletDir =
                                FlussPaths.remoteLogTabletDir(
                                                remoteLogDir,
                                                physicalPath(liveTable.tablePath, partitionInfo),
                                                tableBucket)
                                        .toString();
                        logSegmentRelativePaths =
                                logResult.activeRefsOf(bucketId).logSegmentRelativePaths();
                        logActiveManifestPaths =
                                logResult.activeRefsOf(bucketId).logActiveManifestPaths();
                        break;
                    case READ_FAILED:
                        audit.logBucketAborted(
                                OrphanCleanUtils.bucketScopeKey(
                                        liveTable.tableId, partitionId, bucketId),
                                logResult.readFailureReason(bucketId));
                        planStats.metadataFailure();
                        tablePlans.metadataFailure(liveTable.scope());
                        break;
                    case NOT_LISTED:
                        planStats.skippedNoRemoteManifest();
                        tablePlans.skip(liveTable.scope(), SkipReasonCode.NO_REMOTE_MANIFEST);
                        break;
                    default:
                        break;
                }
            }

            String kvTabletDir = null;
            Set<String> kvActiveSnaps = Collections.emptySet();
            Set<String> kvSharedSstFileNames = Collections.emptySet();
            if (kvTargetOk && kvActiveByBucket.containsKey(bucketId)) {
                kvTabletDir =
                        FlussPaths.remoteKvTabletDir(
                                        remoteKvDir,
                                        physicalPath(liveTable.tablePath, partitionInfo),
                                        tableBucket)
                                .toString();
                kvActiveSnaps = kvActiveByBucket.get(bucketId);
                KvSharedSstFetchResult sstResult =
                        fetcher.fetchKvSharedSstFileNames(new FsPath(kvTabletDir), kvActiveSnaps);
                if (sstResult.allMetadataReadOk()) {
                    kvSharedSstFileNames = sstResult.sharedSstFileNames();
                } else {
                    audit.logSkipKvSharedSst(
                            liveTable.tableId, partitionId, bucketId, sstResult.failureReason());
                }
            } else if (kvTargetOk) {
                planStats.skippedEmptyKvActiveSet();
                tablePlans.skip(liveTable.scope(), SkipReasonCode.EMPTY_KV_ACTIVE_SET);
            }

            if (logTabletDir == null && kvTabletDir == null) {
                continue;
            }

            out.collect(
                    new BucketCleanTask(
                            ScopeIdentity.table(
                                            liveTable.dbName,
                                            liveTable.tableName,
                                            liveTable.tableId)
                                    .withPartitionAndBucket(partitionId, bucketId),
                            logTabletDir,
                            kvTabletDir,
                            logSegmentRelativePaths,
                            logActiveManifestPaths,
                            kvActiveSnaps,
                            kvSharedSstFileNames,
                            config.olderThanMillis(),
                            config.dryRun(),
                            config.allowDeleteManifest()));
            planStats.bucketTask();
            tablePlans.task(liveTable.scope());
        }
    }

    // -------------------------------------------------------------------------
    // Emit OrphanDirCleanTasks
    // -------------------------------------------------------------------------

    private void emitOrphanTableDirTasks(
            DbScanState dbState,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        if (!dbState.tableInfosComplete) {
            audit.logSkipOrphanTableScan(dbState.dbName, "tableInfos-incomplete");
            return;
        }
        Set<Long> activeTableIds = dbState.activeTableIds;
        long maxKnownTableId = tracker.maxKnownTableId();
        boolean emit = config.allowCleanOrphanTables();
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath dbDir = remoteSubDir(root, topLevel + "/" + dbState.dbName);
                if (emit) {
                    forEachOrphanDirUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            remoteFsOpRateLimiter,
                            dir -> {
                                out.collect(
                                        orphanDirCleanTask(
                                                ScopeIdentity.orphanTable(
                                                        dbState.dbName, dir.getName(), null),
                                                dir));
                                planStats.orphanDirTask();
                                tablePlans.task(
                                        ScopeIdentity.orphanTable(
                                                dbState.dbName, dir.getName(), null));
                            },
                            planStats);
                } else {
                    forEachOrphanDirUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            remoteFsOpRateLimiter,
                            dir -> {
                                audit.logSkipOrphanTable(dir, "default-conservative");
                                tablePlans.skip(
                                        ScopeIdentity.orphanTable(
                                                dbState.dbName, dir.getName(), null),
                                        SkipReasonCode.CONSERVATIVE_MODE_DISABLED);
                            },
                            planStats);
                }
            }
        }
    }

    private void emitOrphanPartitionDirTasks(
            LiveTableScope liveTable,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        if (!liveTable.partitioned || !liveTable.partitionInfosComplete) {
            return;
        }
        Set<Long> activePartitionIds = liveTable.activePartitionIds;
        long maxKnownPartitionId = tracker.maxKnownPartitionId();
        boolean emit = config.allowCleanOrphanPartitions();
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath tableDir =
                        FlussPaths.remoteTableDir(
                                remoteSubDir(root, topLevel),
                                liveTable.tablePath,
                                liveTable.tableId);
                if (emit) {
                    forEachOrphanDirUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            remoteFsOpRateLimiter,
                            dir -> {
                                out.collect(
                                        orphanDirCleanTask(
                                                ScopeIdentity.table(
                                                        liveTable.dbName,
                                                        liveTable.tableName,
                                                        liveTable.tableId),
                                                dir));
                                planStats.orphanDirTask();
                                tablePlans.task(liveTable.scope());
                            },
                            planStats);
                } else {
                    forEachOrphanDirUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            remoteFsOpRateLimiter,
                            dir -> {
                                audit.logSkipOrphanPartition(dir, "default-conservative");
                                tablePlans.skip(
                                        liveTable.scope(),
                                        SkipReasonCode.CONSERVATIVE_MODE_DISABLED);
                            },
                            planStats);
                }
            }
        }
    }

    private void emitOrphanDirTasksUnderUnknownDatabases(
            Set<String> activeDbNames,
            Set<Long> activeTableIds,
            boolean activeTableIdsComplete,
            Set<Long> activePartitionIds,
            boolean activePartitionIdsComplete,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        if (config.table().isPresent()) {
            return;
        }
        if (!activeTableIdsComplete) {
            audit.logSkipOrphanTableScan("*", "tableInfos-incomplete");
            return;
        }
        if (config.allDatabases()) {
            emitOrphanDirTasksUnderAllUnknownDatabases(
                    activeDbNames,
                    activeTableIds,
                    activePartitionIds,
                    activePartitionIdsComplete,
                    tracker,
                    clusterRoots,
                    audit,
                    remoteFsOpRateLimiter,
                    planStats,
                    out);
            return;
        }

        String databaseName = config.database().get();
        if (activeDbNames.contains(databaseName)) {
            return;
        }
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath dbDir = remoteSubDir(root, topLevel + "/" + databaseName);
                emitOrphanDirTasksUnderUnknownDatabase(
                        dbDir,
                        activeTableIds,
                        activePartitionIds,
                        activePartitionIdsComplete,
                        tracker,
                        audit,
                        remoteFsOpRateLimiter,
                        planStats,
                        out);
            }
        }
    }

    private void emitOrphanDirTasksUnderAllUnknownDatabases(
            Set<String> activeDbNames,
            Set<Long> activeTableIds,
            Set<Long> activePartitionIds,
            boolean activePartitionIdsComplete,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath topLevelDir = remoteSubDir(root, topLevel);
                FileSystem fs = getFileSystemIfExists(topLevelDir, remoteFsOpRateLimiter);
                if (fs == null) {
                    continue;
                }
                FileStatus[] entries = listStatuses(fs, topLevelDir, remoteFsOpRateLimiter);
                if (entries == null) {
                    planStats.metadataFailure();
                    continue;
                }
                for (FileStatus entry : entries) {
                    if (!entry.isDir()) {
                        continue;
                    }
                    String dbName = entry.getPath().getName();
                    if (activeDbNames.contains(dbName)) {
                        continue;
                    }
                    emitOrphanDirTasksUnderUnknownDatabase(
                            entry.getPath(),
                            activeTableIds,
                            activePartitionIds,
                            activePartitionIdsComplete,
                            tracker,
                            audit,
                            remoteFsOpRateLimiter,
                            planStats,
                            out);
                }
            }
        }
    }

    private void emitOrphanDirTasksUnderUnknownDatabase(
            FsPath dbDir,
            Set<Long> activeTableIds,
            Set<Long> activePartitionIds,
            boolean activePartitionIdsComplete,
            MaxKnownIdsTracker tracker,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        FileSystem fs = getFileSystemIfExists(dbDir, remoteFsOpRateLimiter);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, dbDir, remoteFsOpRateLimiter);
        if (entries == null) {
            planStats.metadataFailure();
            return;
        }
        long maxKnownTableId = tracker.maxKnownTableId();
        for (FileStatus entry : entries) {
            if (!entry.isDir()) {
                continue;
            }
            FsPath tableDir = entry.getPath();
            if (!OrphanDirDetector.isOrphanTable(
                    tableDir.getName(), activeTableIds, maxKnownTableId)) {
                continue;
            }
            ScopeIdentity orphanScope =
                    ScopeIdentity.orphanTable(dbDir.getName(), tableDir.getName(), null);
            if (config.allowCleanOrphanTables()) {
                out.collect(orphanDirCleanTask(orphanScope, tableDir));
                planStats.orphanDirTask();
                tablePlans.task(orphanScope);
            } else {
                audit.logSkipOrphanTable(tableDir, "default-conservative");
                tablePlans.skip(orphanScope, SkipReasonCode.CONSERVATIVE_MODE_DISABLED);
                emitOrphanPartitionDirTasksUnderUnknownTable(
                        tableDir,
                        activePartitionIds,
                        activePartitionIdsComplete,
                        tracker,
                        remoteFsOpRateLimiter,
                        planStats,
                        out);
            }
        }
    }

    private void emitOrphanPartitionDirTasksUnderUnknownTable(
            FsPath tableDir,
            Set<Long> activePartitionIds,
            boolean activePartitionIdsComplete,
            MaxKnownIdsTracker tracker,
            RateLimiter remoteFsOpRateLimiter,
            ScopePlanStats planStats,
            Collector<CleanTask> out)
            throws IOException {
        if (!config.allowCleanOrphanPartitions() || !activePartitionIdsComplete) {
            return;
        }
        long maxKnownPartitionId = tracker.maxKnownPartitionId();
        forEachOrphanDirUnderParent(
                tableDir,
                dirName ->
                        OrphanDirDetector.isOrphanPartition(
                                dirName, activePartitionIds, maxKnownPartitionId),
                remoteFsOpRateLimiter,
                dir -> {
                    ScopeIdentity orphanScope =
                            ScopeIdentity.orphanTable(
                                    tableDir.getParent().getName(), tableDir.getName(), null);
                    out.collect(orphanDirCleanTask(orphanScope, dir));
                    planStats.orphanDirTask();
                    tablePlans.task(orphanScope);
                },
                planStats);
    }

    private OrphanDirCleanTask orphanDirCleanTask(ScopeIdentity scope, FsPath dir) {
        return new OrphanDirCleanTask(
                scope,
                dir.toString(),
                config.olderThanMillis(),
                config.dryRun(),
                config.allowDeleteManifest());
    }

    private void forEachOrphanDirUnderParent(
            FsPath parentDir,
            Predicate<String> isOrphan,
            RateLimiter remoteFsOpRateLimiter,
            Consumer<FsPath> action,
            ScopePlanStats planStats)
            throws IOException {
        FileSystem fs = getFileSystemIfExists(parentDir, remoteFsOpRateLimiter);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir, remoteFsOpRateLimiter);
        if (entries == null) {
            planStats.metadataFailure();
            return;
        }
        for (FileStatus entry : entries) {
            if (!entry.isDir()) {
                continue;
            }
            if (!isOrphan.test(entry.getPath().getName())) {
                continue;
            }
            action.accept(entry.getPath());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Set<Long> collectActiveTableIds(Map<String, DbScanState> dbStates) {
        Set<Long> ids = new LinkedHashSet<Long>();
        for (DbScanState dbState : dbStates.values()) {
            ids.addAll(dbState.activeTableIds);
        }
        return ids;
    }

    private static Set<Long> collectActivePartitionIds(Map<String, DbScanState> dbStates) {
        Set<Long> ids = new LinkedHashSet<Long>();
        for (DbScanState dbState : dbStates.values()) {
            for (LiveTableScope liveTable : dbState.liveTables) {
                ids.addAll(liveTable.activePartitionIds);
            }
        }
        return ids;
    }

    private static boolean activeTableIdsComplete(Map<String, DbScanState> dbStates) {
        for (DbScanState dbState : dbStates.values()) {
            if (!dbState.tableInfosComplete) {
                return false;
            }
        }
        return true;
    }

    private static boolean activePartitionIdsComplete(Map<String, DbScanState> dbStates) {
        for (DbScanState dbState : dbStates.values()) {
            for (LiveTableScope liveTable : dbState.liveTables) {
                if (liveTable.partitioned && !liveTable.partitionInfosComplete) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String classifyName(Throwable e) {
        return RpcErrorClassifier.classify(e).name();
    }

    @Nullable
    private static FileSystem getFileSystemIfExists(FsPath dir, RateLimiter remoteFsOpRateLimiter)
            throws IOException {
        FileSystem fs = dir.getFileSystem();
        remoteFsOpRateLimiter.acquire();
        return fs.exists(dir) ? fs : null;
    }

    @Nullable
    private static FileStatus[] listStatuses(
            FileSystem fs, FsPath dir, RateLimiter remoteFsOpRateLimiter) {
        try {
            remoteFsOpRateLimiter.acquire();
            return fs.listStatus(dir);
        } catch (IOException e) {
            LOG.warn("Failed to list directory: {}", dir, e);
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Internal state classes
    // -------------------------------------------------------------------------

    private static final class DbScanState {
        final String dbName;
        boolean tableInfosComplete = true;
        final Set<Long> activeTableIds = new LinkedHashSet<Long>();
        final List<LiveTableScope> liveTables = new ArrayList<LiveTableScope>();

        DbScanState(String dbName) {
            this.dbName = dbName;
        }
    }

    private static final class LiveTableScope {
        final String dbName;
        final String tableName;
        final TablePath tablePath;
        final long tableId;
        final TableInfo tableInfo;
        final boolean partitioned;
        boolean partitionInfosComplete = true;
        final List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        final Set<Long> activePartitionIds = new LinkedHashSet<Long>();

        LiveTableScope(String dbName, String tableName, TableInfo tableInfo) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.tablePath = tableInfo.getTablePath();
            this.tableId = tableInfo.getTableId();
            this.tableInfo = tableInfo;
            this.partitioned = tableInfo.isPartitioned();
        }

        ScopeIdentity scope() {
            return ScopeIdentity.table(dbName, tableName, tableId);
        }
    }
}
