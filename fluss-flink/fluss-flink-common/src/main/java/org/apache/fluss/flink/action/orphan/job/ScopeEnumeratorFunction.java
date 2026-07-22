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
import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.audit.AuditFailureDetail;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.build.MaxKnownIdsTracker;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.rule.OrphanDirDetector;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.enumerateBuckets;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.fetchClusterConfigMap;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.normalizeRoot;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.remoteSubDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDirs;

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

    private final OrphanCleanConfig config;

    private transient AuditReporterRuntime auditRuntime;
    private transient AuditLogger audit;

    public ScopeEnumeratorFunction(OrphanCleanConfig config) {
        this.config = config;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext)
            throws Exception {
        super.open(openContext);
        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        AuditReporterContext reporterContext =
                new AuditReporterContext(
                        config.auditReporterSpec().runId(),
                        config.dryRun(),
                        AuditStage.SCOPE,
                        "ScopeEnumerator",
                        RuntimeContextAdapter.getIndexOfThisSubtask(runtimeContext),
                        RuntimeContextAdapter.getAttemptNumber(runtimeContext),
                        getRuntimeContext().getUserCodeClassLoader());
        auditRuntime = AuditReporterRuntime.open(config.auditReporterSpec(), reporterContext);
        audit = new AuditLogger(auditRuntime, reporterContext);
    }

    @Override
    public void processElement(Integer trigger, Context ctx, Collector<CleanTask> out)
            throws Exception {
        Throwable processingFailure = null;
        try {
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

            Connection connection = ConnectionFactory.createConnection(flussConfig);
            boolean connectionTransferred = false;
            Throwable connectionOwnerFailure = null;
            try {
                ScopePlanStats planStats = new ScopePlanStats();
                audit.logRunStart(config);
                audit.logCutoff(config.olderThanMillis());

                RateLimiter remoteFsOpRateLimiter =
                        RateLimiter.create((double) config.remoteFsOpRateLimitPerSecond());
                MaxKnownIdsTracker tracker = new MaxKnownIdsTracker();
                Map<String, String> clusterConfigMap;
                String clusterRemoteDataDir;
                List<String> clusterRoots;
                Map<String, DbScanState> dbStates;
                try (Admin admin = connection.getAdmin()) {
                    // Fail fast on incompatible servers: the action jar may be deployed against an
                    // older cluster that does not implement ListRemoteLogManifests /
                    // ListKvSnapshots. Without this guard, every per-target fetch would degrade to
                    // skip_log_target / skip_kv_target audit events and the job would exit
                    // "successfully" with deleted=0, masking the incompatibility.
                    verifyServerSupportsRequiredApis(admin);

                    long phaseStartMillis =
                            startScopePhase(audit, "cluster_configuration_resolution");
                    boolean phaseComplete = false;
                    try {
                        clusterConfigMap = fetchClusterConfigMap(admin);
                        clusterRemoteDataDir = resolveClusterRemoteDataDir(clusterConfigMap);
                        clusterRoots =
                                normalizeRoots(resolveClusterRemoteDataDirs(clusterConfigMap));
                        phaseComplete = true;
                    } finally {
                        endScopePhase(
                                audit,
                                "cluster_configuration_resolution",
                                phaseStartMillis,
                                0L,
                                0L,
                                phaseComplete);
                    }

                    phaseStartMillis = startScopePhase(audit, "metadata_inventory");
                    phaseComplete = false;
                    try {
                        dbStates = enumerateActiveScope(admin, audit, tracker, planStats);
                        phaseComplete = true;
                    } finally {
                        endScopePhase(
                                audit,
                                "metadata_inventory",
                                phaseStartMillis,
                                0L,
                                0L,
                                phaseComplete);
                    }
                }

                long phaseStartMillis = startScopePhase(audit, "live_target_planning");
                long targetsCompletedBefore = planStats.scopeTargets();
                long targetsFailedBefore = planStats.incompleteTargets();
                boolean phaseComplete = false;
                try {
                    ScopeTargetExecutor executor =
                            ScopeTargetExecutor.create(
                                    connection,
                                    config.scopeEnumerationConcurrency(),
                                    remoteFsOpRateLimiter);
                    connectionTransferred = true;
                    try (ScopeTargetExecutor ownedExecutor = executor) {
                        planTargetsAndOrphans(
                                config.scopeEnumerationConcurrency(),
                                new ArrayList<DbScanState>(dbStates.values()),
                                dbState -> dbState.liveTables,
                                liveTables ->
                                        ownedExecutor.forEachCompleted(
                                                buildTargetInputs(
                                                        liveTables,
                                                        clusterRemoteDataDir,
                                                        clusterRoots),
                                                result ->
                                                        result.replay(
                                                                audit, planStats, out::collect)),
                                liveTable ->
                                        emitOrphanPartitionDirTasks(
                                                liveTable,
                                                tracker,
                                                clusterRoots,
                                                audit,
                                                remoteFsOpRateLimiter,
                                                planStats,
                                                out),
                                dbState ->
                                        emitOrphanTableDirTasks(
                                                dbState,
                                                tracker,
                                                clusterRoots,
                                                audit,
                                                remoteFsOpRateLimiter,
                                                planStats,
                                                out));
                    }
                    phaseComplete = true;
                } finally {
                    endScopePhase(
                            audit,
                            "live_target_planning",
                            phaseStartMillis,
                            planStats.scopeTargets() - targetsCompletedBefore,
                            planStats.incompleteTargets() - targetsFailedBefore,
                            phaseComplete,
                            config.scopeEnumerationConcurrency());
                }
                audit.logScopePlan(planStats);
                out.collect(ScopeSummaryTask.from(planStats));
            } catch (Exception | Error failure) {
                connectionOwnerFailure = failure;
                throw failure;
            } finally {
                if (!connectionTransferred) {
                    try {
                        connection.close();
                    } catch (Exception | Error closeFailure) {
                        if (connectionOwnerFailure == null) {
                            throw closeFailure;
                        }
                        connectionOwnerFailure.addSuppressed(closeFailure);
                    }
                }
            }
        } catch (Exception | Error failure) {
            processingFailure = failure;
            throw failure;
        } finally {
            try {
                closeAuditRuntime();
            } catch (RuntimeException | Error lifecycleFailure) {
                if (processingFailure == null) {
                    throw lifecycleFailure;
                }
                processingFailure.addSuppressed(lifecycleFailure);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            closeAuditRuntime();
        } finally {
            super.close();
        }
    }

    private void closeAuditRuntime() {
        AuditReporterRuntime runtime = auditRuntime;
        auditRuntime = null;
        audit = null;
        if (runtime == null) {
            return;
        }

        RuntimeException failure = null;
        try {
            runtime.flush();
        } catch (RuntimeException flushFailure) {
            failure = flushFailure;
        }
        try {
            runtime.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static long startScopePhase(AuditLogger audit, String phase) {
        audit.logScopePhaseStart(phase);
        return System.currentTimeMillis();
    }

    private static void endScopePhase(
            AuditLogger audit,
            String phase,
            long startMillis,
            long targetsCompleted,
            long targetsFailed,
            boolean complete) {
        audit.logScopePhaseEnd(
                phase,
                Math.max(0L, System.currentTimeMillis() - startMillis),
                targetsCompleted,
                targetsFailed,
                complete);
    }

    private static void endScopePhase(
            AuditLogger audit,
            String phase,
            long startMillis,
            long targetsCompleted,
            long targetsFailed,
            boolean complete,
            int scopeEnumerationConcurrency) {
        audit.logScopePhaseEnd(
                phase,
                Math.max(0L, System.currentTimeMillis() - startMillis),
                targetsCompleted,
                targetsFailed,
                complete,
                Integer.valueOf(scopeEnumerationConcurrency));
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
                continue;
            }
            List<String> tableNames;
            try {
                tableNames = admin.listTables(dbName).get();
            } catch (Exception e) {
                audit.logRpcFailure(
                        AuditStage.SCOPE,
                        ScopeIdentity.database(dbName),
                        CleanupObjectType.DIRECTORY,
                        rpcFailure("list_tables", e));
                planStats.metadataFailure();
                dbState.tableInfosComplete = false;
                continue;
            }
            for (String tableName : tableNames) {
                resolveTable(admin, audit, tracker, dbState, tableName, false, planStats);
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
                audit.logRpcFailure(
                        AuditStage.SCOPE,
                        ScopeIdentity.global(),
                        CleanupObjectType.DIRECTORY,
                        rpcFailure("list_databases", e));
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
            audit.logRpcFailure(
                    AuditStage.SCOPE,
                    ScopeIdentity.database(databaseName),
                    CleanupObjectType.DIRECTORY,
                    rpcFailure("database_exists", e));
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
            if (category != RpcErrorClassifier.Category.NOT_FOUND || explicitTableTarget) {
                audit.logRpcFailure(
                        AuditStage.SCOPE,
                        ScopeIdentity.unresolvedTable(dbState.dbName, tableName),
                        CleanupObjectType.DIRECTORY,
                        rpcFailure("get_table_info", e));
                planStats.metadataFailure();
                dbState.tableInfosComplete = false;
            }
            return;
        }
        tracker.observeTableId(tableInfo.getTableId());
        dbState.activeTableIds.add(tableInfo.getTableId());

        LiveTableScope liveTable = new LiveTableScope(dbState.dbName, tableName, tableInfo);
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
                return;
            }
            for (PartitionInfo partition : partitions) {
                liveTable.partitions.add(partition);
                liveTable.activePartitionIds.add(partition.getPartitionId());
                tracker.observePartitionId(partition.getPartitionId());
                planStats.partition();
            }
        } catch (Exception e) {
            audit.logRpcFailure(
                    AuditStage.SCOPE,
                    ScopeIdentity.table(dbState.dbName, tableName, liveTable.tableId),
                    CleanupObjectType.DIRECTORY,
                    rpcFailure("list_partition_infos", e));
            planStats.metadataFailure();
            liveTable.partitionInfosComplete = false;
        }
    }

    // -------------------------------------------------------------------------
    // Build live target inputs (worker execution is handled separately)
    // -------------------------------------------------------------------------

    private List<ScopeTargetEnumeration.Input> buildTargetInputs(
            List<LiveTableScope> liveTables,
            @Nullable String clusterRemoteDataDir,
            List<String> clusterRoots) {
        List<ScopeTargetEnumeration.Input> inputs = new ArrayList<ScopeTargetEnumeration.Input>();
        for (LiveTableScope liveTable : liveTables) {
            if (liveTable.partitioned && !liveTable.partitionInfosComplete) {
                continue;
            }
            List<PartitionInfo> partitionTargets =
                    liveTable.partitioned
                            ? liveTable.partitions
                            : Collections.<PartitionInfo>singletonList(null);
            for (PartitionInfo partitionInfo : partitionTargets) {
                inputs.add(
                        new ScopeTargetEnumeration.Input(
                                liveTable.dbName,
                                liveTable.tableName,
                                liveTable.tableId,
                                liveTable.tablePath,
                                liveTable.tableInfo,
                                partitionInfo,
                                enumerateBuckets(liveTable.tableInfo, partitionInfo),
                                clusterRemoteDataDir,
                                clusterRoots,
                                config.olderThanMillis(),
                                config.dryRun(),
                                config.allowDeleteManifest()));
            }
        }
        return inputs;
    }

    static <D, T> void planTargetsAndOrphans(
            int concurrency,
            List<D> databases,
            Function<D, List<T>> liveTables,
            ThrowingConsumer<List<T>> targetPlanner,
            ThrowingConsumer<T> orphanPartitionPlanner,
            ThrowingConsumer<D> orphanTablePlanner)
            throws Exception {
        if (concurrency == 1) {
            for (D database : databases) {
                for (T table : liveTables.apply(database)) {
                    targetPlanner.accept(Collections.singletonList(table));
                    orphanPartitionPlanner.accept(table);
                }
                orphanTablePlanner.accept(database);
            }
            return;
        }

        List<T> targets = new ArrayList<T>();
        for (D database : databases) {
            targets.addAll(liveTables.apply(database));
        }
        targetPlanner.accept(targets);
        for (D database : databases) {
            for (T table : liveTables.apply(database)) {
                orphanPartitionPlanner.accept(table);
            }
            orphanTablePlanner.accept(database);
        }
    }

    @FunctionalInterface
    interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

    static void enumerateTarget(
            ScopeTargetEnumeration.Worker worker,
            ScopeTargetEnumeration.Input input,
            AuditLogger audit,
            ScopePlanStats planStats,
            Consumer<CleanTask> collector)
            throws Exception {
        try {
            worker.enumerate(input).replay(audit, planStats, collector);
        } catch (ScopeTargetEnumeration.EnumerationException enumerationFailure) {
            try {
                enumerationFailure.partialResult().replay(audit, planStats, collector);
            } catch (Throwable replayFailure) {
                enumerationFailure.originalFailure().addSuppressed(replayFailure);
            }
            enumerationFailure.rethrowOriginal();
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
                                        new OrphanDirCleanTask(
                                                ScopeIdentity.orphanTable(
                                                        dbState.dbName, dir.getName(), null),
                                                dir.toString(),
                                                config.olderThanMillis(),
                                                config.dryRun(),
                                                config.allowDeleteManifest()));
                                planStats.orphanDirTask();
                            },
                            planStats,
                            audit,
                            ScopeIdentity.global());
                } else {
                    forEachOrphanDirUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            remoteFsOpRateLimiter,
                            dir -> audit.logSkipOrphanTable(dir, "default-conservative"),
                            planStats,
                            audit,
                            ScopeIdentity.global());
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
                                        new OrphanDirCleanTask(
                                                ScopeIdentity.table(
                                                        liveTable.dbName,
                                                        liveTable.tableName,
                                                        liveTable.tableId),
                                                dir.toString(),
                                                config.olderThanMillis(),
                                                config.dryRun(),
                                                config.allowDeleteManifest()));
                                planStats.orphanDirTask();
                            },
                            planStats,
                            audit,
                            ScopeIdentity.table(
                                    liveTable.dbName, liveTable.tableName, liveTable.tableId));
                } else {
                    forEachOrphanDirUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            remoteFsOpRateLimiter,
                            dir -> audit.logSkipOrphanPartition(dir, "default-conservative"),
                            planStats,
                            audit,
                            ScopeIdentity.table(
                                    liveTable.dbName, liveTable.tableName, liveTable.tableId));
                }
            }
        }
    }

    private void forEachOrphanDirUnderParent(
            FsPath parentDir,
            Predicate<String> isOrphan,
            RateLimiter remoteFsOpRateLimiter,
            Consumer<FsPath> action,
            ScopePlanStats planStats,
            AuditLogger audit,
            ScopeIdentity scope)
            throws IOException {
        FileSystem fs;
        try {
            fs = getFileSystemIfExists(parentDir, remoteFsOpRateLimiter);
        } catch (IOException failure) {
            audit.logFilesystemFailure(
                    AuditStage.SCOPE,
                    scope,
                    CleanupObjectType.DIRECTORY,
                    filesystemFailure("exists", "io_error", parentDir, failure));
            planStats.metadataFailure();
            throw failure;
        }
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir, remoteFsOpRateLimiter, audit, scope);
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

    private static AuditFailureDetail rpcFailure(String operation, Throwable failure) {
        Throwable cause = ExceptionUtils.stripExecutionException(failure);
        RpcErrorClassifier.Category category = RpcErrorClassifier.classify(cause);
        return AuditFailureDetail.builder(operation, category.name().toLowerCase())
                .exceptionClass(cause.getClass())
                .attempts(1)
                .retryable(category == RpcErrorClassifier.Category.TRANSIENT)
                .consistencyRacePossible(category == RpcErrorClassifier.Category.NOT_FOUND)
                .build();
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
            FileSystem fs,
            FsPath dir,
            RateLimiter remoteFsOpRateLimiter,
            AuditLogger audit,
            ScopeIdentity scope) {
        try {
            remoteFsOpRateLimiter.acquire();
            return fs.listStatus(dir);
        } catch (IOException e) {
            LOG.warn("Failed to list directory: {}", dir, e);
            audit.logFilesystemFailure(
                    AuditStage.SCOPE,
                    scope,
                    CleanupObjectType.DIRECTORY,
                    filesystemFailure("list_directory", "directory_list_failed", dir, e));
            return null;
        }
    }

    private static AuditFailureDetail filesystemFailure(
            String operation, String category, FsPath targetPath, IOException failure) {
        return AuditFailureDetail.builder(operation, category)
                .targetPath(targetPath)
                .exceptionClass(failure.getClass())
                .attempts(1)
                .retryable(true)
                .build();
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
    }
}
