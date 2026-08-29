/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableDescriptor.TableDistribution;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.ServerApiVersionSupport;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorRequestBatch;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.event.AbortBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.BeginBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadAsyncResultEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent;
import org.apache.fluss.server.coordinator.event.CommitBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.CheckedMultiResult;
import org.apache.fluss.server.zk.ZooKeeperClient.CheckedOperation;
import org.apache.fluss.server.zk.ZooKeeperClient.TableRegistrationSnapshot;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable checked-multi tests for ordinary metadata adoption and two-step Abort. */
class BulkLoadMetadataStoreTest {

    private static final String ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String SNAPSHOT_SHA =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final AtomicLong NEXT_TABLE_ID = new AtomicLong(9000L);

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient zkClient;
    private BulkLoadMetadataStore store;

    @TempDir private Path tempDir;

    @BeforeEach
    void setUp() {
        zkClient = ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        store = new BulkLoadMetadataStore(zkClient, 64 * 1024);
    }

    @AfterEach
    void tearDown() {
        zkClient.close();
    }

    @Test
    void testAdoptsOrdinaryMetadataIdempotentlyAndRejectsConflict() throws Exception {
        Fixture fixture = fixture(BulkLoadState.COMMITTING, new long[] {7L});
        BucketSnapshot snapshot = new BucketSnapshot(7L, 17L, fixture.snapshotPath);

        store.adoptBucketMetadata(
                fixture.transaction(),
                fixture.registration(),
                0,
                snapshot,
                fixture.coordinatorEpochVersion);
        store.adoptBucketMetadata(
                fixture.transaction(),
                fixture.registration(),
                0,
                snapshot,
                fixture.coordinatorEpochVersion);

        assertThat(zkClient.getTableBucketSnapshot(fixture.tableBucket, 7L)).contains(snapshot);

        String snapshotPath = ZkData.BucketSnapshotIdZNode.path(fixture.tableBucket, 7L);
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        snapshotPath,
                        ZkData.BucketSnapshotIdZNode.encode(
                                new BucketSnapshot(7L, 18L, fixture.snapshotPath)));
        assertThatThrownBy(
                        () ->
                                store.adoptBucketMetadata(
                                        fixture.transaction(),
                                        fixture.registration(),
                                        0,
                                        snapshot,
                                        fixture.coordinatorEpochVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting ordinary Snapshot");
    }

    @Test
    void testStaleAssignmentRejectsFenceReadyCas() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, null);
        Versioned<TableAssignment> staleAssignment = fixture.assignment();
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        staleAssignment.getPath(),
                        ZkData.TableIdZNode.encode(staleAssignment.getValue()));
        assertThatThrownBy(
                        () ->
                                store.markFenceReady(
                                        fixture.transaction(),
                                        fixture.registration(),
                                        staleAssignment,
                                        Collections.emptyList(),
                                        new long[] {7L},
                                        201L,
                                        fixture.coordinatorEpochVersion))
                .isInstanceOf(
                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException
                                .BadVersionException.class);
        assertThat(fixture.transaction().getValue().isFenceReady()).isFalse();
    }

    @Test
    void testAbortActivationRetainsOwnershipUntilTerminalCas() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, new long[] {7L});
        Versioned<TableRegistration> loading = fixture.registration();

        store.beginAbort(
                fixture.transaction(),
                loading,
                fixture.assignment(),
                Collections.emptyList(),
                BulkLoadAbortReason.TARGET_NOT_EMPTY,
                "Target contains local data.",
                201L,
                fixture.coordinatorEpochVersion);

        Versioned<BulkLoadTransaction> activeTransaction = fixture.transaction();
        assertThat(activeTransaction.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
        assertThat(activeTransaction.getValue().getAbortReason())
                .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        assertThat(activeTransaction.getValue().getAbortMessage())
                .isEqualTo("Target contains local data.");
        assertThat(fixture.registration().getValue().dataState).isEqualTo(BulkLoadDataState.ACTIVE);
        assertThat(fixture.registration().getValue().bulkLoadId).isEqualTo(ID);

        Versioned<BulkLoadTransaction> terminal =
                store.finishAbort(
                        activeTransaction,
                        fixture.registration(),
                        fixture.assignment(),
                        Collections.emptyList(),
                        202L,
                        1000L,
                        fixture.coordinatorEpochVersion);

        assertThat(terminal.getValue().getState()).isEqualTo(BulkLoadState.ABORTED);
        assertThat(terminal.getValue().getAbortReason())
                .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        assertThat(terminal.getValue().getAbortMessage()).isEqualTo("Target contains local data.");
        assertThat(fixture.registration().getValue().bulkLoadId).isNull();
    }

    @Test
    void testTargetNotEmptyAbortIgnoresReRegisteredHolder() throws Exception {
        Fixture fixture = fixture(BulkLoadState.BEGUN, new long[] {7L});
        TabletServerRegistration holderRegistration =
                new TabletServerRegistration(
                        "rack1", Endpoint.fromListenersString("CLIENT://host1:1234"), 100L);
        ZooKeeperClient originalHolder =
                ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZooKeeperClient replacementHolder = null;
        try {
            assertThat(originalHolder.getCuratorClient().blockUntilConnected(30, TimeUnit.SECONDS))
                    .isTrue();
            originalHolder.registerTabletServer(1, holderRegistration);
            Versioned<TabletServerRegistration> observedHolder =
                    observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode);

            originalHolder.close();
            waitUntil(
                    () -> !zkClient.pathExists(observedHolder.getPath()),
                    Duration.ofSeconds(30),
                    "Original TabletServer registration was not removed.");
            replacementHolder =
                    ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
            assertThat(
                            replacementHolder
                                    .getCuratorClient()
                                    .blockUntilConnected(30, TimeUnit.SECONDS))
                    .isTrue();
            replacementHolder.registerTabletServer(1, holderRegistration);
            Versioned<TabletServerRegistration> replacementRegistration =
                    observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode);
            assertThat(replacementRegistration.getEphemeralOwner())
                    .isNotEqualTo(observedHolder.getEphemeralOwner());

            store.beginAbort(
                    fixture.transaction(),
                    fixture.registration(),
                    fixture.assignment(),
                    Collections.singletonList(
                            new BulkLoadMetadataStore.RegisteredServer(1, observedHolder)),
                    BulkLoadAbortReason.TARGET_NOT_EMPTY,
                    "Target contains local data.",
                    201L,
                    fixture.coordinatorEpochVersion);

            Versioned<BulkLoadTransaction> aborting = fixture.transaction();
            assertThat(aborting.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
            assertThat(aborting.getValue().getAbortReason())
                    .isEqualTo(BulkLoadAbortReason.TARGET_NOT_EMPTY);
        } finally {
            if (replacementHolder != null) {
                replacementHolder.close();
            }
            originalHolder.close();
        }
    }

    @Test
    void testAbortWinsWhileFrozenManifestValidationIsInFlight() throws Exception {
        Configuration configuration = new Configuration();
        String remoteDataDir = tempDir.toUri().toString();
        configuration.setString(
                org.apache.fluss.config.ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        LakeCatalogDynamicLoader catalogLoader =
                new LakeCatalogDynamicLoader(configuration, null, true);
        TestCoordinatorChannelManager channelManager = new TestCoordinatorChannelManager();
        try {
            MetadataManager metadataManager =
                    new MetadataManager(zkClient, configuration, catalogLoader);
            String database = "bulkload_manager_abort";
            metadataManager.createDatabase(database, DatabaseDescriptor.builder().build(), false);
            TablePath tablePath = TablePath.of(database, "target");
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .primaryKey("id")
                                            .build())
                            .distributedBy(1, "id")
                            .build()
                            .withReplicationFactor(1);
            TableAssignment assignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(1)).build();
            long tableId =
                    metadataManager.createTable(
                            tablePath, remoteDataDir, descriptor, assignment, false);
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            TableBucket tableBucket = new TableBucket(tableId, 0);
            ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("bulkload-manager-abort-test");
            zkClient.ensureBulkLoadMetadataPaths();
            ensureServerIdsPath();

            Versioned<TableRegistration> active =
                    observed(ZkData.TableZNode.path(tablePath), ZkData.TableZNode::decode);
            BulkLoadHandle handle =
                    new BulkLoadHandle(PhysicalTablePath.of(tablePath), tableId, null, ID);
            long now = System.currentTimeMillis();
            BulkLoadTransaction begun =
                    new BulkLoadTransaction(
                            handle,
                            BulkLoadState.BEGUN,
                            "alice",
                            "USER",
                            remoteDataDir,
                            tableInfo.getSchemaId(),
                            active.getPath(),
                            active.getVersion() + 1,
                            null,
                            now,
                            now,
                            now + Duration.ofMinutes(1).toMillis(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            Versioned<BulkLoadTransaction> transaction =
                    store.createTransactionAndFence(
                            active,
                            observed(
                                    ZkData.TableIdZNode.path(tableId), ZkData.TableIdZNode::decode),
                            begun,
                            epoch.getCoordinatorEpochZkVersion());
            transaction =
                    store.markFenceReady(
                            transaction,
                            observed(ZkData.TableZNode.path(tablePath), ZkData.TableZNode::decode),
                            observed(
                                    ZkData.TableIdZNode.path(tableId), ZkData.TableIdZNode::decode),
                            Collections.emptyList(),
                            new long[] {7L},
                            now + 1,
                            epoch.getCoordinatorEpochZkVersion());

            ManifestFixture manifest = writeManifest(remoteDataDir, handle, tableBucket, 7L);
            transaction =
                    store.freezeManifest(
                            transaction,
                            observed(ZkData.TableZNode.path(tablePath), ZkData.TableZNode::decode),
                            manifest.path.toString(),
                            manifest.bytes.length,
                            manifest.sha256,
                            now + 2,
                            Duration.ofMinutes(1).toMillis(),
                            epoch.getCoordinatorEpochZkVersion());

            CoordinatorContext context = new CoordinatorContext(epoch);
            context.putTableInfo(tableInfo);
            context.putTablePath(tableId, tablePath);
            context.putBucketLeaderAndIsr(
                    tableBucket, new LeaderAndIsr(1, context.getCoordinatorEpoch()));
            context.setCoordinatorServerInfo(
                    new ServerInfo(
                            0,
                            null,
                            Collections.singletonList(new Endpoint("localhost", 10000, "INTERNAL")),
                            ServerType.COORDINATOR));
            TestingEventManager eventManager = new TestingEventManager();
            ManuallyTriggeredScheduledExecutorService ioExecutor =
                    new ManuallyTriggeredScheduledExecutorService();
            BulkLoadManager manager =
                    new BulkLoadManager(
                            zkClient,
                            configuration,
                            context,
                            metadataManager,
                            new CoordinatorRequestBatch(channelManager, eventManager, context),
                            ioExecutor,
                            eventManager,
                            new CompletedSnapshotStoreManager(
                                    1,
                                    ioExecutor,
                                    zkClient,
                                    TestingMetricGroups.COORDINATOR_METRICS,
                                    bucket -> false));

            CompletableFuture<CommitBulkLoadResponse> commitFuture = new CompletableFuture<>();
            manager.process(
                    new CommitBulkLoadEvent(
                            handle,
                            manifest.path.toString(),
                            (long) manifest.bytes.length,
                            manifest.sha256,
                            commitFuture));
            manager.process((BulkLoadAsyncResultEvent) eventManager.getEvents().get(0));
            assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);

            CompletableFuture<AbortBulkLoadResponse> abortFuture = new CompletableFuture<>();
            manager.process(new AbortBulkLoadEvent(handle, abortFuture));
            ioExecutor.trigger();
            manager.process((BulkLoadAsyncResultEvent) eventManager.getEvents().get(1));

            Versioned<BulkLoadTransaction> aborting =
                    observed(
                            ZkData.BulkLoadTableTransactionZNode.path(tableId, ID),
                            ZkData.BulkLoadTableTransactionZNode::decode);
            assertThat(aborting.getValue().getState()).isEqualTo(BulkLoadState.BEGUN);
            assertThat(aborting.getValue().getAbortReason())
                    .isEqualTo(BulkLoadAbortReason.ABORTED_BY_CALLER);

            manager.process((BulkLoadAsyncResultEvent) eventManager.getEvents().get(2));
            manager.process((BulkLoadAsyncResultEvent) eventManager.getEvents().get(3));
            assertThat(abortFuture.get(10, TimeUnit.SECONDS).getStatus().getState())
                    .isEqualTo(BulkLoadState.ABORTED.getCode());
            assertThat(commitFuture).isCompletedExceptionally();
        } finally {
            channelManager.close();
            catalogLoader.close();
        }
    }

    @Test
    void testStartupRejectsTableSnapshotIndexedByDifferentPhysicalId() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                org.apache.fluss.config.ConfigOptions.REMOTE_DATA_DIR, tempDir.toUri().toString());
        LakeCatalogDynamicLoader catalogLoader =
                new LakeCatalogDynamicLoader(configuration, null, true);
        TestCoordinatorChannelManager channelManager = new TestCoordinatorChannelManager();
        try {
            long registrationTableId = NEXT_TABLE_ID.getAndIncrement();
            long metadataTableId = NEXT_TABLE_ID.getAndIncrement();
            TablePath tablePath =
                    TablePath.of("bulkload_startup_identity", "table_" + registrationTableId);
            TableRegistration registration =
                    new TableRegistration(
                            registrationTableId,
                            null,
                            Collections.emptyList(),
                            new TableDistribution(1, Collections.singletonList("id")),
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            "file:///tmp/remote",
                            1L,
                            1L);
            create(ZkData.TableZNode.path(tablePath), ZkData.TableZNode.encode(registration));
            TableRegistrationSnapshot snapshot =
                    zkClient.getTableRegistrationSnapshots(Collections.singletonList(tablePath))
                            .get(tablePath);
            ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("bulkload-identity-test");
            CoordinatorContext context = new CoordinatorContext(epoch);
            context.putTablePath(metadataTableId, tablePath);
            TestingEventManager eventManager = new TestingEventManager();
            ManuallyTriggeredScheduledExecutorService ioExecutor =
                    new ManuallyTriggeredScheduledExecutorService();
            BulkLoadManager manager =
                    new BulkLoadManager(
                            zkClient,
                            configuration,
                            context,
                            new MetadataManager(zkClient, configuration, catalogLoader),
                            new CoordinatorRequestBatch(channelManager, eventManager, context),
                            ioExecutor,
                            eventManager,
                            new CompletedSnapshotStoreManager(
                                    1,
                                    ioExecutor,
                                    zkClient,
                                    TestingMetricGroups.COORDINATOR_METRICS,
                                    bucket -> false));

            assertThatThrownBy(
                            () ->
                                    manager.startupRecovery()
                                            .discover(
                                                    Collections.singletonMap(
                                                            metadataTableId, snapshot),
                                                    Collections.emptyMap()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("table identity");
        } finally {
            channelManager.close();
            catalogLoader.close();
        }
    }

    @Test
    void testUncertainCheckedMultiOutcomesReconcileActiveInventory() throws Exception {
        Configuration configuration = new Configuration();
        String remoteDataDir = tempDir.toUri().toString();
        configuration.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir);
        configuration.set(ConfigOptions.BULKLOAD_MAX_ACTIVE_TRANSACTIONS, 1);
        LakeCatalogDynamicLoader catalogLoader =
                new LakeCatalogDynamicLoader(configuration, null, true);
        TestCoordinatorChannelManager channelManager = new TestCoordinatorChannelManager();
        try {
            int serverId = Math.toIntExact(NEXT_TABLE_ID.getAndIncrement());
            String coordinatorId = "bulkload-uncertain-" + serverId;
            zkClient.registerCoordinatorServer(
                    new CoordinatorAddress(
                            coordinatorId,
                            Collections.singletonList(
                                    new Endpoint("localhost", 10000 + serverId, "INTERNAL")),
                            ServerApiVersionSupport.apiVersions(ServerType.COORDINATOR)));
            zkClient.registerTabletServer(
                    serverId,
                    new TabletServerRegistration(
                            "rack",
                            Collections.singletonList(
                                    new Endpoint("localhost", 20000 + serverId, "INTERNAL")),
                            1L,
                            TabletServerResource.unknown(),
                            ServerApiVersionSupport.apiVersions(ServerType.TABLET_SERVER)));
            ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader(coordinatorId);
            zkClient.ensureBulkLoadMetadataPaths();

            MetadataManager metadataManager =
                    new MetadataManager(zkClient, configuration, catalogLoader);
            String database = "bulkload_uncertain_" + serverId;
            metadataManager.createDatabase(database, DatabaseDescriptor.builder().build(), false);
            TablePath firstPath = TablePath.of(database, "first");
            TablePath secondPath = TablePath.of(database, "second");
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .primaryKey("id")
                                            .build())
                            .distributedBy(1, "id")
                            .build()
                            .withReplicationFactor(1);
            TableAssignment assignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(serverId)).build();
            long firstTableId =
                    metadataManager.createTable(
                            firstPath, remoteDataDir, descriptor, assignment, false);
            metadataManager.createTable(secondPath, remoteDataDir, descriptor, assignment, false);

            UncertainMultiZooKeeperClient uncertainZkClient =
                    new UncertainMultiZooKeeperClient(zkClient, configuration);
            Map<Integer, TabletServerGateway> gateways = new HashMap<>();
            gateways.put(
                    serverId,
                    new TestTabletServerGateway(false, Collections.emptySet()) {
                        @Override
                        public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                                UpdateMetadataRequest request) {
                            return CompletableFuture.completedFuture(new UpdateMetadataResponse());
                        }
                    });
            channelManager.setGateways(gateways);
            CoordinatorContext context = new CoordinatorContext(epoch);
            ServerInfo liveServer =
                    new ServerInfo(
                            serverId,
                            "rack",
                            Collections.singletonList(
                                    new Endpoint("localhost", 20000 + serverId, "INTERNAL")),
                            ServerType.TABLET_SERVER);
            context.setLiveTabletServers(Collections.singletonList(liveServer));
            context.putTableInfo(metadataManager.getTable(firstPath));
            context.putTablePath(firstTableId, firstPath);
            context.putBucketLeaderAndIsr(
                    new TableBucket(firstTableId, 0),
                    new LeaderAndIsr(serverId, context.getCoordinatorEpoch()));
            TestingEventManager eventManager = new TestingEventManager();
            ManuallyTriggeredScheduledExecutorService ioExecutor =
                    new ManuallyTriggeredScheduledExecutorService();
            BulkLoadManager manager =
                    new BulkLoadManager(
                            uncertainZkClient,
                            configuration,
                            context,
                            new MetadataManager(uncertainZkClient, configuration, catalogLoader),
                            new CoordinatorRequestBatch(channelManager, eventManager, context),
                            ioExecutor,
                            eventManager,
                            new CompletedSnapshotStoreManager(
                                    1,
                                    ioExecutor,
                                    uncertainZkClient,
                                    TestingMetricGroups.COORDINATOR_METRICS,
                                    bucket -> false));
            manager.bindTabletServerGateway(liveServer, true);

            uncertainZkClient.failNextCheckedMultiAndReadAfterCommit();
            BeginBulkLoadEvent uncertainBegin = beginEvent(firstPath);
            manager.process(uncertainBegin);
            assertThat(uncertainBegin.getResultFuture()).isCompletedExceptionally();
            Versioned<TableRegistration> firstOwned =
                    observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode);
            assertThat(firstOwned.getValue().dataState).isEqualTo(BulkLoadDataState.LOADING);
            assertThat(firstOwned.getValue().bulkLoadId).isNotNull();

            manager.process(beginEvent(secondPath));
            Versioned<TableRegistration> blocked =
                    observed(ZkData.TableZNode.path(secondPath), ZkData.TableZNode::decode);
            assertThat(blocked.getValue().dataState).isEqualTo(BulkLoadDataState.ACTIVE);
            assertThat(blocked.getValue().bulkLoadId).isNull();

            BulkLoadHandle firstHandle =
                    new BulkLoadHandle(
                            PhysicalTablePath.of(firstPath),
                            firstTableId,
                            null,
                            firstOwned.getValue().bulkLoadId);
            BulkLoadMetadataStore uncertainStore =
                    new BulkLoadMetadataStore(uncertainZkClient, 64 * 1024);
            Versioned<TabletServerRegistration> serverRegistration =
                    observed(ZkData.ServerIdZNode.path(serverId), ZkData.ServerIdZNode::decode);
            List<BulkLoadMetadataStore.RegisteredServer> holders =
                    Collections.singletonList(
                            new BulkLoadMetadataStore.RegisteredServer(
                                    serverId, serverRegistration));
            Versioned<BulkLoadTransaction> ready =
                    uncertainStore.markFenceReady(
                            observed(
                                    BulkLoadMetadataStore.transactionPath(firstHandle),
                                    ZkData.BulkLoadTableTransactionZNode::decode),
                            observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode),
                            observed(
                                    ZkData.TableIdZNode.path(firstTableId),
                                    ZkData.TableIdZNode::decode),
                            holders,
                            new long[] {7L},
                            10L,
                            epoch.getCoordinatorEpochZkVersion());
            Versioned<BulkLoadTransaction> aborting =
                    uncertainStore.beginAbort(
                            ready,
                            observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode),
                            observed(
                                    ZkData.TableIdZNode.path(firstTableId),
                                    ZkData.TableIdZNode::decode),
                            holders,
                            BulkLoadAbortReason.ABORTED_BY_CALLER,
                            null,
                            11L,
                            epoch.getCoordinatorEpochZkVersion());
            uncertainZkClient.failNextCheckedMultiAfterCommit();
            assertThatThrownBy(
                            () ->
                                    uncertainStore.finishAbort(
                                            aborting,
                                            observed(
                                                    ZkData.TableZNode.path(firstPath),
                                                    ZkData.TableZNode::decode),
                                            observed(
                                                    ZkData.TableIdZNode.path(firstTableId),
                                                    ZkData.TableIdZNode::decode),
                                            holders,
                                            12L,
                                            1000L,
                                            epoch.getCoordinatorEpochZkVersion()))
                    .isInstanceOf(KeeperException.ConnectionLossException.class);
            assertThat(
                            observed(
                                            BulkLoadMetadataStore.transactionPath(firstHandle),
                                            ZkData.BulkLoadTableTransactionZNode::decode)
                                    .getValue()
                                    .getState())
                    .isEqualTo(BulkLoadState.ABORTED);
            assertThat(
                            observed(ZkData.TableZNode.path(firstPath), ZkData.TableZNode::decode)
                                    .getValue()
                                    .bulkLoadId)
                    .isNull();

            manager.process(new BulkLoadMaintenanceEvent());
            manager.process(
                    (BulkLoadAsyncResultEvent)
                            eventManager.getEvents().get(eventManager.getEvents().size() - 1));
            manager.process(beginEvent(secondPath));
            Versioned<TableRegistration> admitted =
                    observed(ZkData.TableZNode.path(secondPath), ZkData.TableZNode::decode);
            assertThat(admitted.getValue().dataState).isEqualTo(BulkLoadDataState.LOADING);
            assertThat(admitted.getValue().bulkLoadId).isNotNull();
        } finally {
            channelManager.close();
            catalogLoader.close();
        }
    }

    @Test
    void testMaintenanceContinuesPastInconsistentOwnedTransaction() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                org.apache.fluss.config.ConfigOptions.REMOTE_DATA_DIR, tempDir.toUri().toString());
        LakeCatalogDynamicLoader catalogLoader =
                new LakeCatalogDynamicLoader(configuration, null, true);
        TestCoordinatorChannelManager channelManager = new TestCoordinatorChannelManager();
        try {
            Fixture damaged = fixture(BulkLoadState.BEGUN, null, Long.MAX_VALUE);
            Fixture healthy = fixture(BulkLoadState.BEGUN, null, Long.MAX_VALUE);
            ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("bulkload-maintenance-test");
            CoordinatorContext context = new CoordinatorContext(epoch);
            context.putTablePath(
                    damaged.handle.getTableId(), damaged.handle.getTarget().getTablePath());
            context.putTablePath(
                    healthy.handle.getTableId(), healthy.handle.getTarget().getTablePath());
            TestingEventManager eventManager = new TestingEventManager();
            ManuallyTriggeredScheduledExecutorService ioExecutor =
                    new ManuallyTriggeredScheduledExecutorService();
            BulkLoadManager manager =
                    new BulkLoadManager(
                            zkClient,
                            configuration,
                            context,
                            new MetadataManager(zkClient, configuration, catalogLoader),
                            new CoordinatorRequestBatch(channelManager, eventManager, context),
                            ioExecutor,
                            eventManager,
                            new CompletedSnapshotStoreManager(
                                    1,
                                    ioExecutor,
                                    zkClient,
                                    TestingMetricGroups.COORDINATOR_METRICS,
                                    bucket -> false));
            Map<TablePath, TableRegistrationSnapshot> snapshots =
                    zkClient.getTableRegistrationSnapshots(
                            Arrays.asList(
                                    damaged.handle.getTarget().getTablePath(),
                                    healthy.handle.getTarget().getTablePath()));
            Map<Long, TableRegistrationSnapshot> snapshotsById = new HashMap<>();
            snapshotsById.put(
                    damaged.handle.getTableId(),
                    snapshots.get(damaged.handle.getTarget().getTablePath()));
            snapshotsById.put(
                    healthy.handle.getTableId(),
                    snapshots.get(healthy.handle.getTarget().getTablePath()));
            BulkLoadStartupRecovery.Plan plan =
                    manager.startupRecovery().discover(snapshotsById, Collections.emptyMap());
            manager.startupRecovery().prepare(plan);
            manager.startupRecovery().resume(plan);

            zkClient.getCuratorClient()
                    .setData()
                    .forPath(
                            damaged.registrationPath,
                            ZkData.TableZNode.encode(damaged.registration().getValue()));
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(
                            healthy.transactionPath,
                            ZkData.BulkLoadTableTransactionZNode.encode(
                                    transaction(
                                            healthy.handle,
                                            BulkLoadState.BEGUN,
                                            healthy.registrationPath,
                                            null,
                                            0L)));

            manager.process(new BulkLoadMaintenanceEvent());

            assertThat(healthy.transaction().getValue().getAbortReason())
                    .isEqualTo(BulkLoadAbortReason.BUILD_DEADLINE_EXCEEDED);
        } finally {
            channelManager.close();
            catalogLoader.close();
        }
    }

    private Fixture fixture(BulkLoadState state, long[] snapshotIds) throws Exception {
        return fixture(state, snapshotIds, 300L);
    }

    private Fixture fixture(BulkLoadState state, long[] snapshotIds, long buildDeadlineMs)
            throws Exception {
        long tableId = NEXT_TABLE_ID.getAndIncrement();
        ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("bulkload-store-test");
        TablePath tablePath = TablePath.of("bulkload_store", "table_" + tableId);
        BulkLoadHandle handle =
                new BulkLoadHandle(PhysicalTablePath.of(tablePath), tableId, null, ID);
        TableRegistration active =
                new TableRegistration(
                        tableId,
                        null,
                        Collections.emptyList(),
                        new TableDistribution(1, Collections.singletonList("id")),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "file:///tmp/remote",
                        1L,
                        1L);
        TableRegistration loading = active.withDataState(BulkLoadDataState.LOADING, ID);
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(1)).build();
        String registrationPath = ZkData.TableZNode.path(tablePath);
        String assignmentPath = ZkData.TableIdZNode.path(tableId);
        String transactionPath = ZkData.BulkLoadTableTransactionZNode.path(tableId, ID);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        String snapshotPath =
                new FsPath(
                                FlussPaths.remoteKvSnapshotDir(
                                        FlussPaths.remoteKvTabletDir(
                                                new FsPath(
                                                        active.remoteDataDir,
                                                        FlussPaths.REMOTE_KV_DIR_NAME),
                                                handle.getTarget(),
                                                tableBucket),
                                        7L),
                                "_METADATA")
                        .toString();
        create(registrationPath, ZkData.TableZNode.encode(loading));
        create(assignmentPath, ZkData.TableIdZNode.encode(assignment));
        create(
                transactionPath,
                ZkData.BulkLoadTableTransactionZNode.encode(
                        transaction(
                                handle, state, registrationPath, snapshotIds, buildDeadlineMs)));
        create(ZkData.BucketIdZNode.path(tableBucket), new byte[0]);
        ensureServerIdsPath();
        return new Fixture(
                epoch.getCoordinatorEpochZkVersion(),
                handle,
                tableBucket,
                registrationPath,
                assignmentPath,
                transactionPath,
                snapshotPath);
    }

    private void create(String path, byte[] data) throws Exception {
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    private void ensureServerIdsPath() throws Exception {
        try {
            create(ZkData.ServerIdsZNode.path(), new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // The extension is shared by concurrently executing test methods.
        }
    }

    private static BeginBulkLoadEvent beginEvent(TablePath tablePath) {
        return new BeginBulkLoadEvent(
                PhysicalTablePath.of(tablePath),
                null,
                new FlussPrincipal("alice", "USER"),
                new CompletableFuture<BeginBulkLoadResponse>());
    }

    private <T> Versioned<T> observed(String path, Function<byte[], T> decoder) throws Exception {
        ZooKeeperClient.DataWithStat data = zkClient.getDataWithStat(path);
        return new Versioned<>(
                decoder.apply(data.getData()),
                path,
                data.getStat().getVersion(),
                data.getStat().getEphemeralOwner());
    }

    private static ManifestFixture writeManifest(
            String remoteDataDir, BulkLoadHandle handle, TableBucket tableBucket, long snapshotId)
            throws Exception {
        FsPath snapshotDirectory =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                                handle.getTarget(),
                                tableBucket),
                        snapshotId);
        FsPath dataPath = new FsPath(snapshotDirectory, "file.sst");
        write(dataPath, new byte[] {1});
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        tableBucket,
                        snapshotId,
                        snapshotDirectory.toString(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new KvSnapshotFileMetadata.FileHandle(
                                        dataPath.toString(), 1L, "file.sst")),
                        1L,
                        0L,
                        0L,
                        null);
        FsPath metadataPath = new FsPath(snapshotDirectory, "_METADATA");
        byte[] metadataBytes = KvSnapshotFileMetadataJsonSerde.toJson(metadata);
        write(metadataPath, metadataBytes);
        String manifestJson =
                "{\"version\":1,\"bulk_load_id\":\""
                        + handle.getBulkLoadId()
                        + "\",\"buckets\":[{\"bucket_id\":0,\"snapshot_metadata\":{\"path\":\""
                        + metadataPath
                        + "\",\"length\":"
                        + metadataBytes.length
                        + ",\"sha256\":\""
                        + sha256(metadataBytes)
                        + "\"}}]}";
        byte[] manifestBytes = manifestJson.getBytes(StandardCharsets.UTF_8);
        FsPath manifestPath = FlussPaths.bulkLoadManifestPath(remoteDataDir, handle);
        write(manifestPath, manifestBytes);
        return new ManifestFixture(manifestPath, manifestBytes, sha256(manifestBytes));
    }

    private static void write(FsPath path, byte[] bytes) throws Exception {
        Path localPath = java.nio.file.Paths.get(path.toUri());
        Files.createDirectories(localPath.getParent());
        Files.write(localPath, bytes);
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder result = new StringBuilder(64);
        for (byte value : digest) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static BulkLoadTransaction transaction(
            BulkLoadHandle handle, BulkLoadState state, String metadataPath, long[] snapshotIds) {
        return transaction(handle, state, metadataPath, snapshotIds, 300L);
    }

    private static BulkLoadTransaction transaction(
            BulkLoadHandle handle,
            BulkLoadState state,
            String metadataPath,
            long[] snapshotIds,
            long buildDeadlineMs) {
        boolean manifest = state != BulkLoadState.BEGUN;
        return new BulkLoadTransaction(
                handle,
                state,
                "alice",
                "USER",
                "file:///tmp/remote",
                1,
                metadataPath,
                0,
                snapshotIds,
                100L,
                200L,
                buildDeadlineMs,
                manifest ? 400L : null,
                null,
                manifest ? "file:///tmp/manifest.json" : null,
                manifest ? 100L : null,
                manifest ? SNAPSHOT_SHA : null,
                null,
                null);
    }

    private final class Fixture {
        private final int coordinatorEpochVersion;
        private final BulkLoadHandle handle;
        private final TableBucket tableBucket;
        private final String registrationPath;
        private final String assignmentPath;
        private final String transactionPath;
        private final String snapshotPath;

        private Fixture(
                int coordinatorEpochVersion,
                BulkLoadHandle handle,
                TableBucket tableBucket,
                String registrationPath,
                String assignmentPath,
                String transactionPath,
                String snapshotPath) {
            this.coordinatorEpochVersion = coordinatorEpochVersion;
            this.handle = handle;
            this.tableBucket = tableBucket;
            this.registrationPath = registrationPath;
            this.assignmentPath = assignmentPath;
            this.transactionPath = transactionPath;
            this.snapshotPath = snapshotPath;
        }

        private Versioned<TableRegistration> registration() throws Exception {
            return observed(registrationPath, ZkData.TableZNode::decode);
        }

        private Versioned<TableAssignment> assignment() throws Exception {
            return observed(assignmentPath, ZkData.TableIdZNode::decode);
        }

        private Versioned<BulkLoadTransaction> transaction() throws Exception {
            return observed(transactionPath, ZkData.BulkLoadTableTransactionZNode::decode);
        }
    }

    private static final class ManifestFixture {
        private final FsPath path;
        private final byte[] bytes;
        private final String sha256;

        private ManifestFixture(FsPath path, byte[] bytes, String sha256) {
            this.path = path;
            this.bytes = bytes;
            this.sha256 = sha256;
        }
    }

    private static final class UncertainMultiZooKeeperClient extends ZooKeeperClient {
        private boolean failNextCheckedMulti;
        private boolean failNextRead;
        private boolean failReadAfterCheckedMulti;

        private UncertainMultiZooKeeperClient(
                ZooKeeperClient connectedClient, Configuration configuration) {
            super(
                    new CuratorFrameworkWithUnhandledErrorListener(
                            connectedClient.getCuratorClient(), (message, failure) -> {}),
                    configuration);
        }

        private void failNextCheckedMultiAfterCommit() {
            failNextCheckedMulti = true;
        }

        private void failNextCheckedMultiAndReadAfterCommit() {
            failNextCheckedMulti = true;
            failReadAfterCheckedMulti = true;
        }

        @Override
        public Optional<ZooKeeperClient.DataWithStat> getDataWithStatIfExists(String path)
                throws Exception {
            if (failNextRead) {
                failNextRead = false;
                throw new KeeperException.ConnectionLossException();
            }
            return super.getDataWithStatIfExists(path);
        }

        @Override
        public CheckedMultiResult submitCheckedMulti(
                List<CheckedOperation> operations, long maxSerializedBytes) throws Exception {
            CheckedMultiResult result = super.submitCheckedMulti(operations, maxSerializedBytes);
            if (failNextCheckedMulti) {
                failNextCheckedMulti = false;
                failNextRead = failReadAfterCheckedMulti;
                failReadAfterCheckedMulti = false;
                throw new KeeperException.ConnectionLossException();
            }
            return result;
        }
    }
}
