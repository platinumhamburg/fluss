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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidCoordinatorException;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.SecurityDisabledException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.rpc.messages.ControlledShutdownRequest;
import com.alibaba.fluss.rpc.messages.ControlledShutdownResponse;
import com.alibaba.fluss.rpc.messages.CreateAclsRequest;
import com.alibaba.fluss.rpc.messages.CreateAclsResponse;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateDatabaseResponse;
import com.alibaba.fluss.rpc.messages.CreatePartitionRequest;
import com.alibaba.fluss.rpc.messages.CreatePartitionResponse;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.CreateTableResponse;
import com.alibaba.fluss.rpc.messages.DropAclsRequest;
import com.alibaba.fluss.rpc.messages.DropAclsResponse;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseResponse;
import com.alibaba.fluss.rpc.messages.DropPartitionRequest;
import com.alibaba.fluss.rpc.messages.DropPartitionResponse;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.DropTableResponse;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbHeartbeatReqForTable;
import com.alibaba.fluss.rpc.messages.PbHeartbeatRespForTable;
import com.alibaba.fluss.rpc.netty.server.Session;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.authorizer.AclCreateResult;
import com.alibaba.fluss.server.authorizer.AclDeleteResult;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.coordinator.event.AccessContextEvent;
import com.alibaba.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import com.alibaba.fluss.server.coordinator.event.ControlledShutdownEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.entity.CommitKvSnapshotData;
import com.alibaba.fluss.server.entity.LakeTieringTableInfo;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import com.alibaba.fluss.server.metadata.BucketMetadata;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.TableMetadata;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBindingFilters;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.fromTablePath;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getCommitLakeTableSnapshotData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getPartitionSpec;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeCreateAclsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeDropAclsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static com.alibaba.fluss.utils.PartitionUtils.validatePartitionSpec;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** An RPC Gateway service for coordinator server. */
public final class CoordinatorService extends RpcServiceBase implements CoordinatorGateway {

    private final int defaultBucketNumber;
    private final int defaultReplicationFactor;
    private final Supplier<EventManager> eventManagerSupplier;
    private final Supplier<Integer> coordinatorEpochSupplier;
    private final ServerMetadataCache metadataCache;

    // null if the cluster hasn't configured datalake format
    private final @Nullable DataLakeFormat dataLakeFormat;
    private final @Nullable LakeCatalog lakeCatalog;
    private final LakeTableTieringManager lakeTableTieringManager;

    public CoordinatorService(
            Configuration conf,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            Supplier<CoordinatorEventProcessor> coordinatorEventProcessorSupplier,
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            @Nullable LakeCatalog lakeCatalog,
            LakeTableTieringManager lakeTableTieringManager) {
        super(remoteFileSystem, ServerType.COORDINATOR, zkClient, metadataManager, authorizer);
        this.defaultBucketNumber = conf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER);
        this.defaultReplicationFactor = conf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR);
        this.eventManagerSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEventManager();
        this.coordinatorEpochSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEpoch();
        this.dataLakeFormat = conf.getOptional(ConfigOptions.DATALAKE_FORMAT).orElse(null);
        this.lakeCatalog = lakeCatalog;
        this.lakeTableTieringManager = lakeTableTieringManager;
        this.metadataCache = metadataCache;
        checkState(
                (dataLakeFormat == null) == (lakeCatalog == null),
                "dataLakeFormat and lakeCatalog must both be null or both non-null, but dataLakeFormat is %s, lakeCatalog is %s.",
                dataLakeFormat,
                lakeCatalog);
    }

    @Override
    public String name() {
        return "coordinator";
    }

    @Override
    public void shutdown() {
        IOUtils.closeQuietly(lakeCatalog, "lake catalog");
    }

    @Override
    public CompletableFuture<CreateDatabaseResponse> createDatabase(CreateDatabaseRequest request) {
        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.CREATE, Resource.cluster());
        }

        CreateDatabaseResponse response = new CreateDatabaseResponse();
        try {
            TablePath.validateDatabaseName(request.getDatabaseName());
        } catch (InvalidDatabaseException e) {
            return FutureUtils.completedExceptionally(e);
        }

        DatabaseDescriptor databaseDescriptor;
        if (request.getDatabaseJson() != null) {
            databaseDescriptor = DatabaseDescriptor.fromJsonBytes(request.getDatabaseJson());
        } else {
            databaseDescriptor = DatabaseDescriptor.builder().build();
        }
        metadataManager.createDatabase(
                request.getDatabaseName(), databaseDescriptor, request.isIgnoreIfExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DropDatabaseResponse> dropDatabase(DropDatabaseRequest request) {
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DROP,
                    Resource.database(request.getDatabaseName()));
        }

        DropDatabaseResponse response = new DropDatabaseResponse();
        metadataManager.dropDatabase(
                request.getDatabaseName(), request.isIgnoreIfNotExists(), request.isCascade());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        tablePath.validate();
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.CREATE,
                    Resource.database(tablePath.getDatabaseName()));
        }

        TableDescriptor tableDescriptor;
        try {
            tableDescriptor = TableDescriptor.fromJsonBytes(request.getTableJson());
        } catch (Exception e) {
            if (e instanceof UncheckedIOException) {
                throw new InvalidTableException(
                        "Failed to parse table descriptor: " + e.getMessage());
            } else {
                // wrap the validate message to InvalidTableException
                throw new InvalidTableException(e.getMessage());
            }
        }

        // apply system defaults if the config is not set
        tableDescriptor = applySystemDefaults(tableDescriptor);

        if (tableDescriptor.hasPrimaryKey()) {
            throw new InvalidTableException(
                    "Currently, Primary-key table is not support in this cluster. if you want "
                            + "use Primary-key table, please contact 温粥(263086) for help.");
        }

        // the distribution and bucket count must be set now
        //noinspection OptionalGetWithoutIsPresent
        int bucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();

        // first, generate the assignment
        TableAssignment tableAssignment = null;
        // only when it's no partitioned table do we generate the assignment for it
        if (!tableDescriptor.isPartitioned()) {
            // the replication factor must be set now
            int replicaFactor = tableDescriptor.getReplicationFactor();
            TabletServerInfo[] servers = metadataCache.getLiveServers();
            tableAssignment = generateAssignment(bucketCount, replicaFactor, servers);
        }

        // TODO: should tolerate if the lake exist but matches our schema. This ensures eventually
        //  consistent by idempotently creating the table multiple times. See #846
        // before create table in fluss, we may create in lake
        if (isDataLakeEnabled(tableDescriptor)) {
            try {
                checkNotNull(lakeCatalog).createTable(tablePath, tableDescriptor);
            } catch (TableAlreadyExistException e) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in %s catalog, please "
                                        + "first drop the table in %s catalog or use a new table name.",
                                tablePath, dataLakeFormat, dataLakeFormat));
            }
        }

        // then create table;
        metadataManager.createTable(
                tablePath, tableDescriptor, tableAssignment, request.isIgnoreIfExists());

        return CompletableFuture.completedFuture(new CreateTableResponse());
    }

    private TableDescriptor applySystemDefaults(TableDescriptor tableDescriptor) {
        TableDescriptor newDescriptor = tableDescriptor;

        // not set bucket num
        if (!newDescriptor.getTableDistribution().isPresent()
                || !newDescriptor.getTableDistribution().get().getBucketCount().isPresent()) {
            newDescriptor = newDescriptor.withBucketCount(defaultBucketNumber);
        }

        // not set replication factor
        Map<String, String> properties = newDescriptor.getProperties();
        if (!properties.containsKey(ConfigOptions.TABLE_REPLICATION_FACTOR.key())) {
            newDescriptor = newDescriptor.withReplicationFactor(defaultReplicationFactor);
        }

        // override set num-precreate for auto-partition table with multi-level partition keys
        if (newDescriptor.getPartitionKeys().size() > 1
                && "true".equals(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()))
                && !properties.containsKey(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key())) {
            Map<String, String> newProperties = new HashMap<>(newDescriptor.getProperties());
            // disable precreate partitions for multi-level partitions.
            newProperties.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0");
            newDescriptor = newDescriptor.withProperties(newProperties);
        }

        // override the datalake format if the table hasn't set it and the cluster configured
        if (dataLakeFormat != null
                && !properties.containsKey(ConfigOptions.TABLE_DATALAKE_FORMAT.key())) {
            newDescriptor = newDescriptor.withDataLakeFormat(dataLakeFormat);
        }

        // lake table can only be enabled when the cluster configures datalake format
        boolean dataLakeEnabled = isDataLakeEnabled(tableDescriptor);
        if (dataLakeEnabled && dataLakeFormat == null) {
            throw new InvalidTableException(
                    String.format(
                            "'%s' is enabled for the table, but the Fluss cluster doesn't enable datalake tables.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED.key()));
        }

        return newDescriptor;
    }

    private boolean isDataLakeEnabled(TableDescriptor tableDescriptor) {
        String dataLakeEnabledValue =
                tableDescriptor.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        return Boolean.parseBoolean(dataLakeEnabledValue);
    }

    @Override
    public CompletableFuture<DropTableResponse> dropTable(DropTableRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DROP,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        DropTableResponse response = new DropTableResponse();
        metadataManager.dropTable(tablePath, request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CreatePartitionResponse> createPartition(
            CreatePartitionRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.WRITE,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        CreatePartitionResponse response = new CreatePartitionResponse();
        TableRegistration table = metadataManager.getTableRegistration(tablePath);
        if (!table.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support create partition.");
        }

        // first, validate the partition spec, and get resolved partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec);
        ResolvedPartitionSpec partitionToCreate =
                ResolvedPartitionSpec.fromPartitionSpec(table.partitionKeys, partitionSpec);

        // second, generate the PartitionAssignment.
        int replicaFactor = table.getTableConfig().getReplicationFactor();
        TabletServerInfo[] servers = metadataCache.getLiveServers();
        Map<Integer, BucketAssignment> bucketAssignments =
                generateAssignment(table.bucketCount, replicaFactor, servers)
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(table.tableId, bucketAssignments);

        metadataManager.createPartition(
                tablePath,
                table.tableId,
                partitionAssignment,
                partitionToCreate,
                request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DropPartitionResponse> dropPartition(DropPartitionRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.WRITE,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        DropPartitionResponse response = new DropPartitionResponse();
        TableRegistration table = metadataManager.getTableRegistration(tablePath);
        if (!table.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support drop partition.");
        }

        // first, validate the partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec);
        ResolvedPartitionSpec partitionToDrop =
                ResolvedPartitionSpec.fromPartitionSpec(table.partitionKeys, partitionSpec);

        metadataManager.dropPartition(tablePath, partitionToDrop, request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        String listenerName = currentListenerName();
        Session session = currentSession();

        AccessContextEvent<MetadataResponse> metadataResponseAccessContextEvent =
                new AccessContextEvent<>(
                        ctx ->
                                makeMetadataResponse(
                                        request,
                                        listenerName,
                                        session,
                                        authorizer,
                                        metadataCache,
                                        (tablePath) -> getTableMetadata(ctx, tablePath),
                                        ctx::getPhysicalTablePath,
                                        (physicalTablePath) ->
                                                getPartitionMetadata(ctx, physicalTablePath)));
        eventManagerSupplier.get().put(metadataResponseAccessContextEvent);
        return metadataResponseAccessContextEvent.getResultFuture();
    }

    public CompletableFuture<AdjustIsrResponse> adjustIsr(AdjustIsrRequest request) {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(new AdjustIsrReceivedEvent(getAdjustIsrData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<CommitKvSnapshotResponse> commitKvSnapshot(
            CommitKvSnapshotRequest request) {
        CompletableFuture<CommitKvSnapshotResponse> response = new CompletableFuture<>();
        // parse completed snapshot from request
        byte[] completedSnapshotBytes = request.getCompletedSnapshot();
        CompletedSnapshot completedSnapshot =
                CompletedSnapshotJsonSerde.fromJson(completedSnapshotBytes);
        CommitKvSnapshotData commitKvSnapshotData =
                new CommitKvSnapshotData(
                        completedSnapshot,
                        request.getCoordinatorEpoch(),
                        request.getBucketLeaderEpoch());
        eventManagerSupplier.get().put(new CommitKvSnapshotEvent(commitKvSnapshotData, response));
        return response;
    }

    @Override
    public CompletableFuture<CommitRemoteLogManifestResponse> commitRemoteLogManifest(
            CommitRemoteLogManifestRequest request) {
        CompletableFuture<CommitRemoteLogManifestResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new CommitRemoteLogManifestEvent(
                                getCommitRemoteLogManifestData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<CreateAclsResponse> createAcls(CreateAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        List<AclBinding> aclBindings = toAclBindings(request.getAclsList());
        List<AclCreateResult> aclCreateResults = authorizer.addAcls(currentSession(), aclBindings);
        return CompletableFuture.completedFuture(makeCreateAclsResponse(aclCreateResults));
    }

    @Override
    public CompletableFuture<DropAclsResponse> dropAcls(DropAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        List<AclBindingFilter> filters = toAclBindingFilters(request.getAclFiltersList());
        List<AclDeleteResult> aclDeleteResults = authorizer.dropAcls(currentSession(), filters);
        return CompletableFuture.completedFuture(makeDropAclsResponse(aclDeleteResults));
    }

    @Override
    public CompletableFuture<CommitLakeTableSnapshotResponse> commitLakeTableSnapshot(
            CommitLakeTableSnapshotRequest request) {
        CompletableFuture<CommitLakeTableSnapshotResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new CommitLakeTableSnapshotEvent(
                                getCommitLakeTableSnapshotData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
            LakeTieringHeartbeatRequest request) {
        LakeTieringHeartbeatResponse heartbeatResponse = new LakeTieringHeartbeatResponse();
        int currentCoordinatorEpoch = coordinatorEpochSupplier.get();
        heartbeatResponse.setCoordinatorEpoch(currentCoordinatorEpoch);

        // process failed tables
        for (PbHeartbeatReqForTable failedTable : request.getFailedTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addFailedTableResp().setTableId(failedTable.getTableId());
            try {
                validateHeartbeatRequest(failedTable, currentCoordinatorEpoch);
                lakeTableTieringManager.reportTieringFail(
                        failedTable.getTableId(), failedTable.getTieringEpoch());
            } catch (Throwable e) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(e).toErrorResponse());
            }
        }

        // process tiering tables
        for (PbHeartbeatReqForTable tieringTable : request.getTieringTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addTieringTableResp().setTableId(tieringTable.getTableId());
            try {
                validateHeartbeatRequest(tieringTable, currentCoordinatorEpoch);
                lakeTableTieringManager.renewTieringHeartbeat(
                        tieringTable.getTableId(), tieringTable.getTieringEpoch());
            } catch (Throwable t) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(t).toErrorResponse());
            }
        }

        // process finished tables
        for (PbHeartbeatReqForTable finishTable : request.getFinishedTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addFinishedTableResp().setTableId(finishTable.getTableId());
            try {
                validateHeartbeatRequest(finishTable, currentCoordinatorEpoch);
                lakeTableTieringManager.finishTableTiering(
                        finishTable.getTableId(), finishTable.getTieringEpoch());
            } catch (Throwable e) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(e).toErrorResponse());
            }
        }

        if (request.hasRequestTable() && request.isRequestTable()) {
            LakeTieringTableInfo lakeTieringTableInfo = lakeTableTieringManager.requestTable();
            if (lakeTieringTableInfo != null) {
                heartbeatResponse
                        .setTieringTable()
                        .setTableId(lakeTieringTableInfo.tableId())
                        .setTablePath(fromTablePath(lakeTieringTableInfo.tablePath()))
                        .setTieringEpoch(lakeTieringTableInfo.tieringEpoch());
            }
        }
        return CompletableFuture.completedFuture(heartbeatResponse);
    }

    @Override
    public CompletableFuture<ControlledShutdownResponse> controlledShutdown(
            ControlledShutdownRequest request) {
        CompletableFuture<ControlledShutdownResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new ControlledShutdownEvent(
                                request.getTabletServerId(),
                                request.getTabletServerEpoch(),
                                response));
        return response;
    }

    private void validateHeartbeatRequest(
            PbHeartbeatReqForTable heartbeatReqForTable, int currentEpoch) {
        if (heartbeatReqForTable.getCoordinatorEpoch() != currentEpoch) {
            throw new InvalidCoordinatorException(
                    String.format(
                            "The coordinator epoch %s in request is not match current coordinator epoch %d for table %d.",
                            heartbeatReqForTable.getCoordinatorEpoch(),
                            currentEpoch,
                            heartbeatReqForTable.getTableId()));
        }
    }

    private TableMetadata getTableMetadata(CoordinatorContext ctx, TablePath tablePath) {
        // always get table info from zk.
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = ctx.getTableIdByPath(tablePath);
        List<BucketMetadata> bucketMetadataList;
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            // TODO no need to get assignment from zk if refactor client metadata cache. Trace by
            // https://github.com/alibaba/fluss/issues/483
            // get table assignment from zk.
            bucketMetadataList =
                    getTableMetadataFromZk(
                            zkClient, tablePath, tableInfo.getTableId(), tableInfo.isPartitioned());
        } else {
            // get table assignment from coordinatorContext.
            bucketMetadataList =
                    getBucketMetadataFromContext(
                            ctx, tableId, null, ctx.getTableAssignment(tableId));
        }
        return new TableMetadata(tableInfo, bucketMetadataList);
    }

    private PartitionMetadata getPartitionMetadata(
            CoordinatorContext ctx, PhysicalTablePath partitionPath) {
        TablePath tablePath =
                new TablePath(partitionPath.getDatabaseName(), partitionPath.getTableName());
        String partitionName = partitionPath.getPartitionName();
        long tableId = ctx.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            // TODO no need to get assignment from zk if refactor client metadata cache. Trace by
            // https://github.com/alibaba/fluss/issues/483
            return getPartitionMetadataFromZk(partitionPath, zkClient);
        } else {
            Optional<Long> partitionIdOpt = ctx.getPartitionId(partitionPath);
            if (partitionIdOpt.isPresent()) {
                long partitionId = partitionIdOpt.get();
                List<BucketMetadata> bucketMetadataList =
                        getBucketMetadataFromContext(
                                ctx,
                                tableId,
                                partitionId,
                                ctx.getPartitionAssignment(
                                        new TablePartition(tableId, partitionId)));
                return new PartitionMetadata(
                        tableId, partitionName, partitionId, bucketMetadataList);
            } else {
                return getPartitionMetadataFromZk(partitionPath, zkClient);
            }
        }
    }

    private static List<BucketMetadata> getBucketMetadataFromContext(
            CoordinatorContext ctx,
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, List<Integer>> tableAssigment) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        tableAssigment.forEach(
                (bucketId, serverIds) -> {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                    Optional<LeaderAndIsr> optLeaderAndIsr = ctx.getBucketLeaderAndIsr(tableBucket);
                    Integer leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    bucketId,
                                    leader,
                                    ctx.getBucketLeaderEpoch(tableBucket),
                                    serverIds);
                    bucketMetadataList.add(bucketMetadata);
                });
        return bucketMetadataList;
    }
}
