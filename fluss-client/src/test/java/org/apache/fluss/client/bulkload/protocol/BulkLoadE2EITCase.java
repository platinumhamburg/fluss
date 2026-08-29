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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.bulkload.BulkLoadBucketFiles;
import org.apache.fluss.client.bulkload.BulkLoadBucketWriter;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.bulkload.BulkLoadClient;
import org.apache.fluss.client.bulkload.BulkLoadTestDataBuilder;
import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public-client BulkLoad acceptance harness shared by the end-to-end cases. */
final class BulkLoadE2EITCase extends ClientToServerITCaseBase {

    private static final Duration E2E_TIMEOUT = Duration.ofMinutes(5);
    private static final int BUCKET_COUNT = 5;
    private static final int INDEXED_ROWS_PER_BUCKET = 2050;
    private static final int STRESS_ROWS_PER_BUCKET = 1101;
    private static final String WAL_TARGET_PARTITION = "target";
    private static final String WAL_LIVE_PARTITION = "live";

    private static final Schema FULL_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema PARTITIONED_WAL_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .column("pt", DataTypes.STRING())
                    .primaryKey("id", "pt")
                    .build();

    @Test
    void testCommitRecoversAfterCoordinatorAndOriginalLeaderLoss() throws Exception {
        withFeatureCluster(
                cluster -> {
                    Configuration featureClientConfiguration = cluster.getClientConfig();
                    try (Connection featureConnection =
                                    ConnectionFactory.createConnection(featureClientConfiguration);
                            FlussAdmin featureAdmin = (FlussAdmin) featureConnection.getAdmin()) {
                        TablePath tablePath =
                                TablePath.of("bulkload_e2e", "partitioned_wal_import");
                        featureAdmin
                                .createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        featureAdmin
                                .createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(PARTITIONED_WAL_SCHEMA)
                                                .distributedBy(BUCKET_COUNT, "id")
                                                .partitionedBy("pt")
                                                .property(
                                                        ConfigOptions.TABLE_CHANGELOG_IMAGE,
                                                        ChangelogImage.WAL)
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        featureAdmin
                                .createPartition(
                                        tablePath,
                                        newPartitionSpec("pt", WAL_TARGET_PARTITION),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        featureAdmin
                                .createPartition(
                                        tablePath,
                                        newPartitionSpec("pt", WAL_LIVE_PARTITION),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                featureAdmin
                                        .getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        Map<String, Long> partitionIds =
                                featureAdmin
                                        .listPartitionInfos(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                        .stream()
                                        .collect(
                                                java.util.stream.Collectors.toMap(
                                                        PartitionInfo::getPartitionName,
                                                        PartitionInfo::getPartitionId));
                        assertThat(partitionIds)
                                .containsKeys(WAL_TARGET_PARTITION, WAL_LIVE_PARTITION);

                        try (Table table = featureConnection.getTable(tablePath)) {
                            runPartitionedWalRecoveryScenario(
                                    cluster,
                                    featureAdmin,
                                    table,
                                    tablePath,
                                    tableInfo,
                                    partitionIds);
                        }
                    }
                });
    }

    @Test
    void testImportFinalStandardFilesAndContinueOnlineWrites() throws Exception {
        withFeatureCluster(
                cluster -> {
                    Configuration featureClientConfiguration = cluster.getClientConfig();
                    Configuration accessProbeConfiguration =
                            new Configuration(featureClientConfiguration);
                    accessProbeConfiguration.set(
                            ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofSeconds(1));
                    accessProbeConfiguration.setInt(ConfigOptions.CLIENT_LOOKUP_MAX_RETRIES, 0);
                    accessProbeConfiguration.setInt(ConfigOptions.CLIENT_WRITER_RETRIES, 0);
                    accessProbeConfiguration.setBoolean(
                            ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
                    try (Connection featureConnection =
                                    ConnectionFactory.createConnection(featureClientConfiguration);
                            Connection accessProbeConnection =
                                    ConnectionFactory.createConnection(accessProbeConfiguration);
                            FlussAdmin featureAdmin = (FlussAdmin) featureConnection.getAdmin()) {
                        TablePath tablePath = TablePath.of("bulkload_e2e", "full_import");
                        featureAdmin
                                .createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        featureAdmin
                                .createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(BUCKET_COUNT, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                featureAdmin
                                        .getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                        try (Table table = featureConnection.getTable(tablePath);
                                Table accessProbeTable =
                                        accessProbeConnection.getTable(tablePath)) {
                            runFullImportScenario(
                                    cluster,
                                    featureClientConfiguration,
                                    featureAdmin,
                                    featureConnection.getBulkLoadClient(),
                                    table,
                                    accessProbeTable,
                                    tablePath,
                                    tableInfo);
                        }
                    }
                });
    }

    @Test
    void testSnapshotAtProvidedLogEndOffsetsPersistsAcrossRecovery() throws Exception {
        withFeatureCluster(
                cluster -> {
                    Configuration clientConfiguration = cluster.getClientConfig();
                    TablePath tablePath = TablePath.of("bulkload_e2e", "snapshot_only_import");
                    PhysicalTablePath target = PhysicalTablePath.of(tablePath);
                    List<Object[]> expectedRows = new ArrayList<>();
                    List<Object[]> firstTailRows = new ArrayList<>();
                    Map<Integer, Long> expectedOffsets = new LinkedHashMap<>();
                    Set<Integer> usedIds = new HashSet<>();
                    TableInfo tableInfo;

                    try (Connection connection =
                                    ConnectionFactory.createConnection(clientConfiguration);
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(BUCKET_COUNT, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        Map<Integer, List<Object[]>> rowsByBucket = new LinkedHashMap<>();
                        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                            expectedOffsets.put(bucket, 10_000L + bucket * 1_000L + 17L);
                            List<Object[]> bucketRows = new ArrayList<>();
                            for (int index = 0; index < 3; index++) {
                                Object[] values =
                                        rowForBucket(
                                                tableInfo,
                                                bucket,
                                                700_000 + bucket * 10_000 + index,
                                                usedIds,
                                                "snapshot-only-" + bucket + '-' + index);
                                bucketRows.add(values);
                                expectedRows.add(values);
                            }
                            rowsByBucket.put(bucket, bucketRows);
                        }

                        BulkLoadClient bulkLoadClient = connection.getBulkLoadClient();
                        BulkLoadBuildContext context =
                                bulkLoadClient.begin(target, null, E2E_TIMEOUT);
                        List<BulkLoadBucketFiles> bucketFiles = new ArrayList<>();
                        java.nio.file.Path workDirectory =
                                Files.createTempDirectory("fluss-bulkload-snapshot-only-");
                        try {
                            for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                                try (BulkLoadBucketWriter writer =
                                        new BulkLoadBucketWriter(
                                                context, bucket, workDirectory.toFile())) {
                                    for (Object[] values : rowsByBucket.get(bucket)) {
                                        writer.add(row(tableInfo.getRowType(), values));
                                    }
                                    bucketFiles.add(
                                            writer.finishAtLogEndOffset(
                                                    expectedOffsets.get(bucket)));
                                }
                            }
                        } finally {
                            org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(
                                    workDirectory.toFile());
                        }

                        BulkLoadStatus committed =
                                bulkLoadClient.commit(context, bucketFiles, E2E_TIMEOUT);
                        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED);
                        assertThat(latestOffsets(admin, target, expectedOffsets.keySet()))
                                .containsExactlyInAnyOrderEntriesOf(expectedOffsets);
                        try (Table table = connection.getTable(tablePath)) {
                            verifyPublicRows(table, tableInfo.getRowType(), expectedRows);
                            UpsertWriter writer = table.newUpsert().createWriter();
                            for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                                Object[] tail =
                                        rowForBucket(
                                                tableInfo,
                                                bucket,
                                                800_000 + bucket * 10_000,
                                                usedIds,
                                                "first-tail-" + bucket);
                                firstTailRows.add(tail);
                                writer.upsert(row(tableInfo.getRowType(), tail))
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            }
                            writer.flush();
                            expectedRows.addAll(firstTailRows);
                            verifyPublicRows(table, tableInfo.getRowType(), expectedRows);
                            Map<Integer, Long> afterWriteOffsets = new LinkedHashMap<>();
                            for (Map.Entry<Integer, Long> entry : expectedOffsets.entrySet()) {
                                afterWriteOffsets.put(entry.getKey(), entry.getValue() + 1L);
                            }
                            assertThat(latestOffsets(admin, target, expectedOffsets.keySet()))
                                    .containsExactlyInAnyOrderEntriesOf(afterWriteOffsets);
                            try (LogScanner scanner = table.newScan().createLogScanner()) {
                                for (Map.Entry<Integer, Long> entry : expectedOffsets.entrySet()) {
                                    scanner.subscribe(entry.getKey(), entry.getValue());
                                }
                                Map<Integer, ScanRecord> tails = new LinkedHashMap<>();
                                long deadline = System.nanoTime() + E2E_TIMEOUT.toNanos();
                                while (tails.size() < BUCKET_COUNT
                                        && System.nanoTime() < deadline) {
                                    ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                                    for (TableBucket bucket : records.buckets()) {
                                        List<ScanRecord> recordsForBucket = records.records(bucket);
                                        if (!recordsForBucket.isEmpty()) {
                                            tails.put(bucket.getBucket(), recordsForBucket.get(0));
                                        }
                                    }
                                }
                                assertThat(tails).hasSize(BUCKET_COUNT);
                                for (Map.Entry<Integer, ScanRecord> entry : tails.entrySet()) {
                                    assertThat(entry.getValue().logOffset())
                                            .isEqualTo(expectedOffsets.get(entry.getKey()));
                                }
                            }
                        }

                        assertNoBulkLoadRemoteLogArtifacts(cluster, tableInfo, target);

                        int restartedTabletServer =
                                cluster.waitAndGetLeader(
                                        new TableBucket(tableInfo.getTableId(), 0));
                        cluster.stopCoordinatorServer();
                        cluster.startCoordinatorServer();
                        cluster.stopTabletServer(restartedTabletServer);
                        cluster.startTabletServer(restartedTabletServer);
                    }

                    try (Connection recoveredConnection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            Admin recoveredAdmin = recoveredConnection.getAdmin();
                            Table recoveredTable = recoveredConnection.getTable(tablePath)) {
                        Map<Integer, Long> recoveredOffsets = new LinkedHashMap<>();
                        for (Map.Entry<Integer, Long> entry : expectedOffsets.entrySet()) {
                            recoveredOffsets.put(entry.getKey(), entry.getValue() + 1L);
                        }
                        verifyPublicRows(recoveredTable, tableInfo.getRowType(), expectedRows);
                        assertThat(latestOffsets(recoveredAdmin, target, expectedOffsets.keySet()))
                                .containsExactlyInAnyOrderEntriesOf(recoveredOffsets);

                        UpsertWriter writer = recoveredTable.newUpsert().createWriter();
                        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                            Object[] tail =
                                    rowForBucket(
                                            tableInfo,
                                            bucket,
                                            900_000 + bucket * 10_000,
                                            usedIds,
                                            "recovered-tail-" + bucket);
                            expectedRows.add(tail);
                            writer.upsert(row(tableInfo.getRowType(), tail))
                                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        }
                        writer.flush();
                        verifyPublicRows(recoveredTable, tableInfo.getRowType(), expectedRows);
                        Map<Integer, Long> finalOffsets = new LinkedHashMap<>();
                        for (Map.Entry<Integer, Long> entry : expectedOffsets.entrySet()) {
                            finalOffsets.put(entry.getKey(), entry.getValue() + 2L);
                        }
                        assertThat(latestOffsets(recoveredAdmin, target, expectedOffsets.keySet()))
                                .containsExactlyInAnyOrderEntriesOf(finalOffsets);
                    }
                });
    }

    @Test
    void testOrdinarySnapshotCommitAfterActiveTableRegistrationChanges() throws Exception {
        withFeatureCluster(
                cluster -> {
                    TablePath tablePath = TablePath.of("bulkload_e2e", "active_identity_refresh");
                    PhysicalTablePath target = PhysicalTablePath.of(tablePath);
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(1, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        BulkLoadClient bulkLoadClient = connection.getBulkLoadClient();
                        BulkLoadBuildContext context =
                                bulkLoadClient.begin(target, null, E2E_TIMEOUT);
                        java.nio.file.Path workDirectory =
                                Files.createTempDirectory("fluss-bulkload-active-refresh-");
                        BulkLoadBucketFiles bucketFiles;
                        try (BulkLoadBucketWriter writer =
                                new BulkLoadBucketWriter(context, 0, workDirectory.toFile())) {
                            writer.add(row(tableInfo.getRowType(), new Object[] {1, "bulkload"}));
                            bucketFiles = writer.finish();
                        } finally {
                            org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(
                                    workDirectory.toFile());
                        }
                        BulkLoadStatus committed =
                                bulkLoadClient.commit(
                                        context,
                                        Collections.singletonList(bucketFiles),
                                        E2E_TIMEOUT);
                        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED);

                        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
                        Replica leader = cluster.waitAndGetLeaderReplica(tableBucket);
                        Integer leaderId = leader.getLeaderId();
                        admin.alterTable(
                                        tablePath,
                                        Collections.singletonList(
                                                TableChange.set(
                                                        ConfigOptions
                                                                .TABLE_TIERED_LOG_LOCAL_SEGMENTS
                                                                .key(),
                                                        "5")),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        waitValue(
                                () ->
                                        leader.getLogTablet().getTieredLogLocalSegments() == 5
                                                ? Optional.of(Boolean.TRUE)
                                                : Optional.empty(),
                                E2E_TIMEOUT,
                                "table config metadata dispatch reaches the active leader");
                        assertThat(cluster.waitAndGetLeaderReplica(tableBucket).getLeaderId())
                                .isEqualTo(leaderId);

                        try (Table table = connection.getTable(tablePath)) {
                            writeAndAwaitOrdinaryRow(
                                    table,
                                    tableInfo,
                                    new Object[] {2, "ordinary"},
                                    "ordinary row before snapshot");
                        }
                        assertThat(cluster.triggerAndWaitSnapshot(tableBucket)).isNotNull();
                    }
                });
    }

    @Test
    void testAbortAfterBuildRestoresAccessAndLeavesStandardFilesForOrdinaryOrphanCleanup()
            throws Exception {
        withFeatureCluster(
                cluster -> {
                    Configuration featureClientConfiguration = cluster.getClientConfig();
                    try (Connection featureConnection =
                                    ConnectionFactory.createConnection(featureClientConfiguration);
                            FlussAdmin featureAdmin = (FlussAdmin) featureConnection.getAdmin()) {
                        TablePath tablePath = TablePath.of("bulkload_e2e", "built_abort");
                        featureAdmin
                                .createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        featureAdmin
                                .createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(BUCKET_COUNT, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                featureAdmin
                                        .getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                        try (Table table = featureConnection.getTable(tablePath)) {
                            runAbortAfterBuildScenario(featureAdmin, table, tablePath, tableInfo);
                        }
                    }
                });
    }

    @Test
    void testAbortConvergesWhenAssignedHolderIsAbsentBeforeFence() throws Exception {
        withFeatureCluster(
                cluster -> {
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        TablePath tablePath = TablePath.of("bulkload_e2e", "pre_fence_abort");
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(1, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
                        int leader = cluster.waitAndGetLeader(bucket);
                        TableAssignment assignment =
                                cluster.getZooKeeperClient()
                                        .getTableAssignment(tableInfo.getTableId())
                                        .orElseThrow(
                                                () ->
                                                        new AssertionError(
                                                                "Missing table assignment."));
                        List<Integer> replicas = assignment.getBucketAssignment(0).getReplicas();
                        assertThat(replicas).hasSize(3);
                        int absentHolder =
                                replicas.stream()
                                        .filter(holder -> holder != leader)
                                        .findFirst()
                                        .orElseThrow(
                                                () ->
                                                        new AssertionError(
                                                                "Missing follower holder."));
                        cluster.stopTabletServer(absentHolder);

                        ZooKeeperClient zkClient = cluster.getZooKeeperClient();
                        String holderPath = ZkData.ServerIdZNode.path(absentHolder);
                        waitValue(
                                () ->
                                        zkClient.pathExists(holderPath)
                                                ? Optional.empty()
                                                : Optional.of(Boolean.TRUE),
                                Duration.ofSeconds(30),
                                "stopped holder registration disappears before Begin");

                        bulkLoadRpc(admin).beginBulkLoad(PhysicalTablePath.of(tablePath));
                        TableRegistration registration =
                                waitValue(
                                        () -> {
                                            try {
                                                TableRegistration value =
                                                        ZkData.TableZNode.decode(
                                                                zkClient.getDataWithStat(
                                                                                ZkData.TableZNode
                                                                                        .path(
                                                                                                tablePath))
                                                                        .getData());
                                                return value.dataState == BulkLoadDataState.LOADING
                                                                && value.bulkLoadId != null
                                                        ? Optional.of(value)
                                                        : Optional.empty();
                                            } catch (Exception notLoadingYet) {
                                                return Optional.empty();
                                            }
                                        },
                                        Duration.ofSeconds(30),
                                        "pre-fence registration owns the BulkLoad transaction");
                        String transactionPath =
                                ZkData.BulkLoadTableTransactionZNode.path(
                                        tableInfo.getTableId(), registration.bulkLoadId);
                        BulkLoadTransaction transaction =
                                ZkData.BulkLoadTableTransactionZNode.decode(
                                        zkClient.getDataWithStat(transactionPath).getData());
                        assertThat(transaction.getState()).isEqualTo(BulkLoadState.BEGUN);
                        assertThat(transaction.isFenceReady()).isFalse();
                        assertThat(transaction.getBulkLoadId()).isEqualTo(registration.bulkLoadId);
                        assertThat(registration.dataState).isEqualTo(BulkLoadDataState.LOADING);
                        assertThat(registration.bulkLoadId).isEqualTo(transaction.getBulkLoadId());

                        BulkLoadStatus aborted =
                                bulkLoadRpc(admin)
                                        .abortBulkLoad(transaction.getHandle())
                                        .get(30, TimeUnit.SECONDS);
                        assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED);
                        BulkLoadTransaction terminal =
                                ZkData.BulkLoadTableTransactionZNode.decode(
                                        zkClient.getDataWithStat(transactionPath).getData());
                        TableRegistration active =
                                ZkData.TableZNode.decode(
                                        zkClient.getDataWithStat(ZkData.TableZNode.path(tablePath))
                                                .getData());
                        assertThat(terminal.getState()).isEqualTo(BulkLoadState.ABORTED);
                        assertThat(active.dataState).isEqualTo(BulkLoadDataState.ACTIVE);
                        assertThat(active.bulkLoadId).isNull();

                        try (Table table = connection.getTable(tablePath)) {
                            writeAndAwaitOrdinaryRow(
                                    table,
                                    tableInfo,
                                    new Object[] {1, "ordinary-after-pre-fence-abort"},
                                    "ordinary lookup succeeds after pre-fence Abort");
                        }
                    }
                });
    }

    @Test
    void testIdempotencyAndNonEmptyTargetFence() throws Exception {
        withFeatureCluster(
                cluster -> {
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        TablePath idempotent = TablePath.of("bulkload_e2e", "idempotent");
                        TablePath localNonEmpty = TablePath.of("bulkload_e2e", "local_non_empty");
                        TablePath remoteNonEmpty = TablePath.of("bulkload_e2e", "remote_non_empty");
                        admin.createDatabase(
                                        idempotent.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        Map<TablePath, TableInfo> tables = new LinkedHashMap<>();
                        for (TablePath tablePath :
                                Arrays.asList(idempotent, localNonEmpty, remoteNonEmpty)) {
                            admin.createTable(
                                            tablePath,
                                            TableDescriptor.builder()
                                                    .schema(FULL_SCHEMA)
                                                    .distributedBy(1, "id")
                                                    .build(),
                                            false)
                                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            TableInfo tableInfo =
                                    admin.getTableInfo(tablePath)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            cluster.waitUntilTableReady(tableInfo.getTableId());
                            tables.put(tablePath, tableInfo);
                        }

                        PhysicalTablePath idempotentTarget = PhysicalTablePath.of(idempotent);
                        BulkLoadTargetInfo firstBegin =
                                bulkLoadRpc(admin)
                                        .beginBulkLoad(idempotentTarget)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        assertThat(
                                        connection
                                                .getBulkLoadClient()
                                                .getInProgressBulkLoad(idempotentTarget))
                                .hasValueSatisfying(
                                        status -> {
                                            assertThat(status.getHandle())
                                                    .isEqualTo(firstBegin.getHandle());
                                            assertThat(status.getState())
                                                    .isEqualTo(BulkLoadState.BEGUN);
                                        });
                        assertThatThrownBy(
                                        () ->
                                                bulkLoadRpc(admin)
                                                        .beginBulkLoad(idempotentTarget)
                                                        .get(
                                                                E2E_TIMEOUT.toMillis(),
                                                                TimeUnit.MILLISECONDS))
                                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                                .hasMessageNotContaining(firstBegin.getHandle().getBulkLoadId());
                        try (BuiltBulkLoadInput input =
                                buildInput(
                                        firstBegin,
                                        java.util.Collections.<Object[]>singletonList(
                                                new Object[] {1, "idempotent"}),
                                        ChangelogImage.FULL)) {
                            BulkLoadStatus firstCommit =
                                    commit(admin, input)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            BulkLoadStatus repeatedCommit =
                                    commit(admin, input)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(repeatedCommit).isEqualTo(firstCommit);
                            assertThat(firstCommit.getState()).isEqualTo(BulkLoadState.COMMITTED);
                        }
                        assertThat(
                                        connection
                                                .getBulkLoadClient()
                                                .getInProgressBulkLoad(idempotentTarget))
                                .isEmpty();

                        TableInfo localTableInfo = tables.get(localNonEmpty);
                        try (Table localTable = connection.getTable(localNonEmpty)) {
                            writeAndAwaitOrdinaryRow(
                                    localTable,
                                    localTableInfo,
                                    new Object[] {2, "ordinary-local"},
                                    "ordinary local content before rejected Begin");
                        }
                        assertTargetNotEmpty(
                                admin, PhysicalTablePath.of(localNonEmpty), "contains local data");

                        TableInfo remoteTableInfo = tables.get(remoteNonEmpty);
                        TableBucket remoteBucket = new TableBucket(remoteTableInfo.getTableId(), 0);
                        FsPath remoteManifestPath =
                                new FsPath(
                                        cluster.getRemoteDataDir(),
                                        "registered-remote-manifest.json");
                        Files.createDirectories(Paths.get(remoteManifestPath.toUri()).getParent());
                        Files.write(
                                Paths.get(remoteManifestPath.toUri()),
                                new RemoteLogManifest(
                                                PhysicalTablePath.of(remoteNonEmpty),
                                                remoteBucket,
                                                Collections.emptyList())
                                        .toJsonBytes());
                        cluster.getZooKeeperClient()
                                .upsertRemoteLogManifestHandle(
                                        remoteBucket,
                                        new RemoteLogManifestHandle(remoteManifestPath, -1L));
                        assertTargetNotEmpty(
                                admin,
                                PhysicalTablePath.of(remoteNonEmpty),
                                "ordinary remote data");
                    }
                });
    }

    @Test
    void testRetainedTerminalHandleIsIdempotentAcrossConnections() throws Exception {
        withFeatureCluster(
                cluster -> {
                    Configuration clientConfiguration = cluster.getClientConfig();
                    TablePath committedTable =
                            TablePath.of("bulkload_e2e", "retained_committed_handle");
                    TablePath abortedTable =
                            TablePath.of("bulkload_e2e", "retained_aborted_handle");
                    PhysicalTablePath committedTarget = PhysicalTablePath.of(committedTable);
                    PhysicalTablePath abortedTarget = PhysicalTablePath.of(abortedTable);
                    TableInfo committedTableInfo;
                    TableInfo abortedTableInfo;
                    BulkLoadTargetInfo committedTargetInfo;
                    Object[] committedOrdinaryRow =
                            new Object[] {Integer.MAX_VALUE - 1, "ordinary-after-commit"};
                    Object[] abortedOrdinaryRow =
                            new Object[] {Integer.MAX_VALUE, "ordinary-after-abort"};

                    try (Connection connection =
                                    ConnectionFactory.createConnection(clientConfiguration);
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        admin.createDatabase(
                                        committedTable.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        for (TablePath tablePath : Arrays.asList(committedTable, abortedTable)) {
                            admin.createTable(
                                            tablePath,
                                            TableDescriptor.builder()
                                                    .schema(FULL_SCHEMA)
                                                    .distributedBy(BUCKET_COUNT, "id")
                                                    .build(),
                                            false)
                                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        }
                        committedTableInfo =
                                admin.getTableInfo(committedTable)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        abortedTableInfo =
                                admin.getTableInfo(abortedTable)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(committedTableInfo.getTableId());
                        cluster.waitUntilTableReady(abortedTableInfo.getTableId());
                        committedTargetInfo =
                                bulkLoadRpc(admin)
                                        .beginBulkLoad(committedTarget)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }

                    Set<Integer> usedIds = new HashSet<>();
                    List<Object[]> committedRows = new ArrayList<>();
                    for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                        committedRows.add(
                                rowForBucket(
                                        committedTableInfo,
                                        bucket,
                                        bucket * 100_000,
                                        usedIds,
                                        "retained-committed"));
                    }
                    try (BuiltBulkLoadInput input =
                            buildInput(committedTargetInfo, committedRows, ChangelogImage.FULL)) {
                        BulkLoadStatus committedStatus;
                        BulkLoadStatus abortedStatus;
                        BulkLoadHandle abortedHandle;
                        try (Connection connection =
                                        ConnectionFactory.createConnection(clientConfiguration);
                                FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                            committedStatus =
                                    commit(admin, input)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(committedStatus.getState())
                                    .isEqualTo(BulkLoadState.COMMITTED);

                            BulkLoadTargetInfo abortedTargetInfo =
                                    bulkLoadRpc(admin)
                                            .beginBulkLoad(abortedTarget)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            abortedHandle = abortedTargetInfo.getHandle();
                            abortedStatus =
                                    bulkLoadRpc(admin)
                                            .abortBulkLoad(abortedHandle)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(abortedStatus.getState()).isEqualTo(BulkLoadState.ABORTED);

                            BulkLoadHandle freshAbortedHandle =
                                    bulkLoadRpc(admin)
                                            .beginBulkLoad(abortedTarget)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                            .getHandle();
                            assertThat(freshAbortedHandle).isNotEqualTo(abortedHandle);
                            assertThat(
                                            bulkLoadRpc(admin)
                                                    .abortBulkLoad(freshAbortedHandle)
                                                    .get(
                                                            E2E_TIMEOUT.toMillis(),
                                                            TimeUnit.MILLISECONDS)
                                                    .getState())
                                    .isEqualTo(BulkLoadState.ABORTED);

                            try (Table table = connection.getTable(committedTable)) {
                                writeAndAwaitOrdinaryRow(
                                        table,
                                        committedTableInfo,
                                        committedOrdinaryRow,
                                        "ordinary lookup succeeds after Commit");
                            }
                            try (Table table = connection.getTable(abortedTable)) {
                                writeAndAwaitOrdinaryRow(
                                        table,
                                        abortedTableInfo,
                                        abortedOrdinaryRow,
                                        "ordinary lookup succeeds after Abort");
                            }
                        }

                        try (Connection recoveredConnection =
                                        ConnectionFactory.createConnection(clientConfiguration);
                                FlussAdmin recoveredAdmin =
                                        (FlussAdmin) recoveredConnection.getAdmin();
                                Table committedAccess =
                                        recoveredConnection.getTable(committedTable);
                                Table abortedAccess = recoveredConnection.getTable(abortedTable)) {
                            BulkLoadStatus repeatedCommit =
                                    commit(recoveredAdmin, input)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(repeatedCommit).isEqualTo(committedStatus);
                            BulkLoadStatus repeatedAbort =
                                    bulkLoadRpc(recoveredAdmin)
                                            .abortBulkLoad(abortedHandle)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(repeatedAbort).isEqualTo(abortedStatus);
                            assertThat(
                                            recoveredConnection
                                                    .getBulkLoadClient()
                                                    .getInProgressBulkLoad(committedTarget))
                                    .isEmpty();
                            assertThat(
                                            recoveredConnection
                                                    .getBulkLoadClient()
                                                    .getInProgressBulkLoad(abortedTarget))
                                    .isEmpty();
                            awaitPublicRow(
                                    committedAccess,
                                    committedTableInfo.getRowType(),
                                    committedOrdinaryRow,
                                    "ordinary lookup succeeds after repeated Commit");
                            awaitPublicRow(
                                    abortedAccess,
                                    abortedTableInfo.getRowType(),
                                    abortedOrdinaryRow,
                                    "ordinary lookup succeeds after repeated Abort");
                        }

                        try (Connection droppedTargetConnection =
                                        ConnectionFactory.createConnection(clientConfiguration);
                                FlussAdmin droppedTargetAdmin =
                                        (FlussAdmin) droppedTargetConnection.getAdmin()) {
                            droppedTargetAdmin
                                    .dropTable(abortedTable, false)
                                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            BulkLoadStatus repeatedAbortAfterDrop =
                                    bulkLoadRpc(droppedTargetAdmin)
                                            .abortBulkLoad(abortedHandle)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                            assertThat(repeatedAbortAfterDrop).isEqualTo(abortedStatus);
                        }
                    }
                });
    }

    @Test
    void testReRegisteredAssignedHolderCannotEscapeLoadingFence() throws Exception {
        withFeatureCluster(
                cluster -> {
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        TablePath tablePath = TablePath.of("bulkload_e2e", "re_registered_holder");
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(1, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        TableBucket bucket = new TableBucket(tableInfo.getTableId(), 0);
                        int leader = cluster.waitAndGetLeader(bucket);
                        TableAssignment assignment =
                                cluster.getZooKeeperClient()
                                        .getTableAssignment(tableInfo.getTableId())
                                        .orElseThrow(
                                                () ->
                                                        new AssertionError(
                                                                "Missing table assignment."));
                        List<Integer> followers =
                                assignment.getBucketAssignment(0).getReplicas().stream()
                                        .filter(holder -> holder != leader)
                                        .collect(java.util.stream.Collectors.toList());
                        assertThat(followers).hasSize(2);
                        int staleHolder = followers.get(0);
                        int unavailableHolder = followers.get(1);

                        Replica staleReplica =
                                cluster.getTabletServerById(staleHolder)
                                        .getReplicaManager()
                                        .getReplicaOrException(bucket);
                        staleReplica.appendRecordsToFollower(genMemoryLogRecordsByObject(DATA1));
                        assertThat(staleReplica.getLocalLogEndOffset()).isEqualTo(DATA1.size());
                        cluster.stopTabletServer(staleHolder);
                        cluster.stopTabletServer(unavailableHolder);
                        cluster.assertHasTabletServerNumber(1);

                        CompletableFuture<BulkLoadTargetInfo> beginFuture =
                                bulkLoadRpc(admin).beginBulkLoad(PhysicalTablePath.of(tablePath));
                        assertThatThrownBy(() -> beginFuture.get(5, TimeUnit.SECONDS))
                                .isInstanceOf(TimeoutException.class);

                        cluster.startTabletServer(staleHolder);
                        Replica reRegisteredReplica =
                                waitValue(
                                        () -> {
                                            try {
                                                Replica replica =
                                                        cluster.getTabletServerById(staleHolder)
                                                                .getReplicaManager()
                                                                .getReplicaOrException(bucket);
                                                BulkLoadTargetMetadata target =
                                                        replica.getBulkLoadTargetMetadata();
                                                return target != null
                                                                && target.isLoading()
                                                                && replica.getLocalLogEndOffset()
                                                                        > 0
                                                        ? Optional.of(replica)
                                                        : Optional.empty();
                                            } catch (RuntimeException notReady) {
                                                return Optional.empty();
                                            }
                                        },
                                        E2E_TIMEOUT,
                                        "re-registered holder applies LOADING before access");
                        assertThat(reRegisteredReplica.getLocalLogEndOffset())
                                .isEqualTo(DATA1.size());

                        TabletServerGateway staleGateway =
                                cluster.newTabletServerClientForNode(staleHolder);
                        FetchLogResponse fetchResponse =
                                staleGateway
                                        .fetchLog(
                                                newFetchLogRequest(
                                                        -1, tableInfo.getTableId(), 0, 0L))
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        assertThat(fetchResponse.getTablesRespsCount()).isEqualTo(1);
                        assertThat(fetchResponse.getTablesRespAt(0).getBucketsRespsCount())
                                .isEqualTo(1);
                        assertThat(
                                        fetchResponse
                                                .getTablesRespAt(0)
                                                .getBucketsRespAt(0)
                                                .getErrorCode())
                                .isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
                        assertThat(
                                        fetchResponse
                                                .getTablesRespAt(0)
                                                .getBucketsRespAt(0)
                                                .getErrorMessage())
                                .contains("BulkLoad target is LOADING and rejects external access")
                                .contains(bucket.toString());

                        cluster.startTabletServer(unavailableHolder);
                        assertThatThrownBy(
                                        () ->
                                                beginFuture.get(
                                                        E2E_TIMEOUT.toMillis(),
                                                        TimeUnit.MILLISECONDS))
                                .isInstanceOf(ExecutionException.class)
                                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                                .hasMessageContaining(BulkLoadAbortReason.TARGET_NOT_EMPTY.name());
                        assertThat(
                                        connection
                                                .getBulkLoadClient()
                                                .getInProgressBulkLoad(
                                                        PhysicalTablePath.of(tablePath)))
                                .isEmpty();
                    }
                });
    }

    @Test
    void testPublicBucketWriterRejectsRowForAnotherBucket() throws Exception {
        withFeatureCluster(
                cluster -> {
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        TablePath tablePath =
                                TablePath.of("bulkload_e2e", "public_writer_bucket_validation");
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(BUCKET_COUNT, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        BulkLoadClient bulkLoadClient = connection.getBulkLoadClient();
                        BulkLoadBuildContext context =
                                bulkLoadClient.begin(
                                        PhysicalTablePath.of(tablePath), null, E2E_TIMEOUT);
                        Object[] bucketOneValues =
                                rowForBucket(tableInfo, 1, 0, new HashSet<>(), "wrong-bucket");
                        java.nio.file.Path workDirectory =
                                Files.createTempDirectory("fluss-bulkload-wrong-bucket-");
                        try {
                            try (BulkLoadBucketWriter writer =
                                    new BulkLoadBucketWriter(context, 0, workDirectory.toFile())) {
                                assertThatThrownBy(
                                                () ->
                                                        writer.add(
                                                                row(
                                                                        tableInfo.getRowType(),
                                                                        bucketOneValues)))
                                        .isInstanceOf(IllegalArgumentException.class)
                                        .hasMessageContaining("belongs to bucket 1")
                                        .hasMessageContaining("writer owns bucket 0");
                            }
                        } finally {
                            org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(
                                    workDirectory.toFile());
                        }
                        assertThat(bulkLoadClient.abort(context.getHandle()).getState())
                                .isEqualTo(BulkLoadState.ABORTED);
                    }
                });
    }

    @Test
    void testBucketWriterOwnsOnlyUniqueAttemptChild() throws Exception {
        withFeatureCluster(
                cluster -> {
                    try (Connection connection =
                                    ConnectionFactory.createConnection(cluster.getClientConfig());
                            FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                        TablePath tablePath =
                                TablePath.of("bulkload_e2e", "writer_local_attempt_ownership");
                        admin.createDatabase(
                                        tablePath.getDatabaseName(),
                                        DatabaseDescriptor.EMPTY,
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        admin.createTable(
                                        tablePath,
                                        TableDescriptor.builder()
                                                .schema(FULL_SCHEMA)
                                                .distributedBy(1, "id")
                                                .build(),
                                        false)
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        TableInfo tableInfo =
                                admin.getTableInfo(tablePath)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        cluster.waitUntilTableReady(tableInfo.getTableId());

                        BulkLoadClient bulkLoadClient = connection.getBulkLoadClient();
                        BulkLoadBuildContext context =
                                bulkLoadClient.begin(
                                        PhysicalTablePath.of(tablePath), null, E2E_TIMEOUT);
                        java.nio.file.Path callerParent =
                                Files.createTempDirectory("fluss-bulkload-caller-parent-");
                        java.nio.file.Path sentinel = callerParent.resolve("caller-sentinel");
                        byte[] sentinelBytes = new byte[] {3, 1, 4, 1, 5};
                        Object[] staleRow = new Object[] {1, "stale-from-old-attempt"};
                        Object[] intendedRow = new Object[] {2, "current-complete-input"};
                        try {
                            Files.write(sentinel, sentinelBytes);
                            seedLegacyBucketDb(callerParent, tableInfo, staleRow);

                            BulkLoadBucketFiles bucketFiles;
                            try (BulkLoadBucketWriter writer =
                                    new BulkLoadBucketWriter(context, 0, callerParent.toFile())) {
                                writer.add(row(tableInfo.getRowType(), intendedRow));
                                bucketFiles = writer.finish();
                            }
                            assertThat(
                                            bulkLoadClient
                                                    .commit(
                                                            context,
                                                            Collections.singletonList(bucketFiles),
                                                            E2E_TIMEOUT)
                                                    .getState())
                                    .isEqualTo(BulkLoadState.COMMITTED);

                            try (Table table = connection.getTable(tablePath)) {
                                assertThat(
                                                table.newLookup()
                                                        .createLookuper()
                                                        .lookup(row((Integer) staleRow[0]))
                                                        .get(
                                                                E2E_TIMEOUT.toMillis(),
                                                                TimeUnit.MILLISECONDS)
                                                        .getSingletonRow())
                                        .as("a fresh attempt must not reuse legacy parent/db rows")
                                        .isNull();
                                verifyPublicRows(
                                        table,
                                        tableInfo.getRowType(),
                                        Collections.singletonList(intendedRow));
                            }
                            assertThat(Files.readAllBytes(sentinel))
                                    .as("closing a writer must preserve its borrowed caller parent")
                                    .containsExactly(sentinelBytes);
                        } finally {
                            org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(
                                    callerParent.toFile());
                        }
                    }
                });
    }

    private static void assertTargetNotEmpty(
            FlussAdmin admin, PhysicalTablePath target, String expectedEvidence) throws Exception {
        assertThatThrownBy(
                        () ->
                                bulkLoadRpc(admin)
                                        .beginBulkLoad(target)
                                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining(BulkLoadAbortReason.TARGET_NOT_EMPTY.name())
                .hasMessageContaining(expectedEvidence);
        assertThat(bulkLoadRpc(admin).getInProgressBulkLoad(target).get()).isEmpty();
    }

    private static void writeAndAwaitOrdinaryRow(
            Table table, TableInfo tableInfo, Object[] values, String description)
            throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(tableInfo.getRowType(), values))
                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        writer.flush();
        awaitPublicRow(table, tableInfo.getRowType(), values, description);
    }

    private static void runFullImportScenario(
            FlussClusterExtension cluster,
            Configuration featureClientConfiguration,
            FlussAdmin featureAdmin,
            BulkLoadClient bulkLoadClient,
            Table table,
            Table accessProbeTable,
            TablePath tablePath,
            TableInfo tableInfo)
            throws Exception {
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-e2e-default").hashCode()
                                & Integer.MAX_VALUE)
                        % 100_000;
        Set<Integer> usedIds = new HashSet<>();
        List<Object[]> inputRows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT - 1; bucket++) {
            int records = bucket == 0 ? INDEXED_ROWS_PER_BUCKET : 6;
            for (int record = 0; record < records; record++) {
                inputRows.add(
                        rowForBucket(
                                tableInfo,
                                bucket,
                                seedBase + bucket * 10_000 + record,
                                usedIds,
                                "bucket-" + bucket + "-record-" + record));
            }
        }
        Object[] first = inputRows.get(0);
        Object[] overwrittenFirst = Arrays.copyOf(first, first.length);
        overwrittenFirst[1] = "overwritten-" + first[0];
        List<Object[]> readinessProbes = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            readinessProbes.add(
                    rowForBucket(
                            tableInfo,
                            bucket,
                            seedBase + 100_000 + bucket,
                            usedIds,
                            "readiness-probe-" + bucket));
        }
        awaitPublicBucketReadiness(table, readinessProbes);

        PhysicalTablePath target = PhysicalTablePath.of(tablePath);
        BulkLoadBuildContext context = bulkLoadClient.begin(target, null, E2E_TIMEOUT);
        assertThat(bulkLoadClient.getInProgressBulkLoad(target))
                .hasValueSatisfying(
                        status -> {
                            assertThat(status.getHandle()).isEqualTo(context.getHandle());
                            assertThat(status.getState()).isEqualTo(BulkLoadState.BEGUN);
                        });
        assertLoadingAccessRejected(accessProbeTable, tableInfo, first);

        List<BulkLoadBucketFiles> bucketFiles = new ArrayList<>();
        Map<Integer, Long> expectedInitialOffsets = new LinkedHashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            java.nio.file.Path workDirectory =
                    Files.createTempDirectory("fluss-bulkload-default-snapshot-");
            try (BulkLoadBucketWriter writer =
                    new BulkLoadBucketWriter(context, bucket, workDirectory.toFile())) {
                Set<Integer> finalKeys = new HashSet<>();
                for (Object[] values : inputRows) {
                    if (bucketContains(tableInfo, bucket, values)) {
                        if (values[0].equals(first[0])) {
                            writer.add(row(tableInfo.getRowType(), overwrittenFirst));
                        }
                        writer.add(row(tableInfo.getRowType(), values));
                        finalKeys.add((Integer) values[0]);
                    }
                }
                expectedInitialOffsets.put(bucket, (long) finalKeys.size());
                bucketFiles.add(writer.finish());
            } finally {
                org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(workDirectory.toFile());
            }
        }

        int controlledBucket = 0;
        TableBucket controlledTableBucket =
                new TableBucket(tableInfo.getTableId(), controlledBucket);
        int stoppedServer = cluster.waitAndGetLeader(controlledTableBucket);
        BulkLoadStatus committed = bulkLoadClient.commit(context, bucketFiles, E2E_TIMEOUT);
        assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED);
        assertThat(latestOffsets(featureAdmin, target, expectedInitialOffsets.keySet()))
                .containsExactlyInAnyOrderEntriesOf(expectedInitialOffsets);
        verifyPublicRows(table, tableInfo.getRowType(), inputRows);
        assertNoBulkLoadRemoteLogArtifacts(cluster, tableInfo, target);

        cluster.stopTabletServer(stoppedServer);
        try (Connection recoveredConnection =
                        ConnectionFactory.createConnection(featureClientConfiguration);
                Admin recoveredAdmin = recoveredConnection.getAdmin();
                Table recoveredTable = recoveredConnection.getTable(tablePath)) {
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), inputRows);

            Object[] ordinaryRow =
                    rowForBucket(
                            tableInfo,
                            controlledBucket,
                            seedBase + 10_000,
                            usedIds,
                            "ordinary-tail");
            UpsertWriter writer = recoveredTable.newUpsert().createWriter();
            writer.upsert(row(tableInfo.getRowType(), ordinaryRow))
                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer.flush();

            List<Object[]> rowsAfterWrite = new ArrayList<>(inputRows);
            rowsAfterWrite.add(Arrays.copyOf(ordinaryRow, ordinaryRow.length));
            verifyPublicRows(recoveredTable, tableInfo.getRowType(), rowsAfterWrite);
            Map<Integer, Long> expectedOffsetsAfterWrite =
                    new LinkedHashMap<>(expectedInitialOffsets);
            expectedOffsetsAfterWrite.put(
                    controlledBucket, expectedInitialOffsets.get(controlledBucket) + 1L);
            assertThat(latestOffsets(recoveredAdmin, target, expectedInitialOffsets.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(expectedOffsetsAfterWrite);
        }
    }

    private static void runAbortAfterBuildScenario(
            FlussAdmin featureAdmin, Table table, TablePath tablePath, TableInfo tableInfo)
            throws Exception {
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-abort-e2e-default")
                                                .hashCode()
                                        & Integer.MAX_VALUE)
                                % 100_000
                        + 600_000;
        Set<Integer> usedIds = new HashSet<>();
        List<Object[]> inputRows = new ArrayList<>();
        List<Object[]> readinessProbes = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            inputRows.add(
                    rowForBucket(
                            tableInfo,
                            bucket,
                            seedBase + bucket * 10_000,
                            usedIds,
                            "aborted-input-bucket-" + bucket));
            readinessProbes.add(
                    rowForBucket(
                            tableInfo,
                            bucket,
                            seedBase + 100_000 + bucket,
                            usedIds,
                            "abort-readiness-bucket-" + bucket));
        }
        awaitPublicBucketReadiness(table, readinessProbes);

        BulkLoadTargetInfo targetInfo =
                bulkLoadRpc(featureAdmin)
                        .beginBulkLoad(PhysicalTablePath.of(tablePath))
                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        try (BuiltBulkLoadInput input = buildInput(targetInfo, inputRows, ChangelogImage.FULL)) {
            assertThatThrownBy(
                            () ->
                                    featureAdmin
                                            .dropTable(tablePath, false)
                                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(InvalidAlterTableException.class);

            BulkLoadStatus aborted =
                    bulkLoadRpc(featureAdmin)
                            .abortBulkLoad(input.getHandle())
                            .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED);

            verifyEmptyPublicTarget(featureAdmin, table, tablePath);

            Object[] ordinaryRow =
                    rowForBucket(tableInfo, 0, seedBase + 200_000, usedIds, "ordinary-after-abort");
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(tableInfo.getRowType(), ordinaryRow))
                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            writer.flush();

            awaitPublicRow(
                    table,
                    tableInfo.getRowType(),
                    ordinaryRow,
                    "ordinary lookup succeeds after aborting built output");
            assertThat(
                            table.newLookup()
                                    .createLookuper()
                                    .lookup(row(Integer.MAX_VALUE))
                                    .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                    .getSingletonRow())
                    .isNull();
            assertThat(toLogicalRows(awaitPublicBatchScan(table)))
                    .containsExactlyElementsOf(
                            java.util.Collections.<Object[]>singletonList(ordinaryRow));
            verifySingleOrdinaryLogAfterAbort(featureAdmin, table, tablePath, ordinaryRow);
        }
    }

    private static void runPartitionedWalRecoveryScenario(
            FlussClusterExtension cluster,
            FlussAdmin featureAdmin,
            Table table,
            TablePath tablePath,
            TableInfo tableInfo,
            Map<String, Long> partitionIds)
            throws Exception {
        long targetPartitionId = partitionIds.get(WAL_TARGET_PARTITION);
        long livePartitionId = partitionIds.get(WAL_LIVE_PARTITION);
        int seedBase =
                (System.getProperty("test.randomization.seed", "bulkload-wal-e2e-default")
                                                .hashCode()
                                        & Integer.MAX_VALUE)
                                % 100_000
                        + 300_000;
        Set<Integer> usedIds = new HashSet<>();
        List<Object[]> targetRows = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            targetRows.addAll(
                    partitionRowsForBucket(
                            tableInfo,
                            bucket,
                            seedBase + bucket * 100_000,
                            usedIds,
                            STRESS_ROWS_PER_BUCKET,
                            WAL_TARGET_PARTITION));
        }
        List<Object[]> liveRows = new ArrayList<>();
        writeAndAwaitPartitionRow(
                table,
                tableInfo.getRowType(),
                liveRows,
                partitionRowForBucket(
                        tableInfo,
                        0,
                        seedBase + 100_000,
                        usedIds,
                        "live-before-begin",
                        WAL_LIVE_PARTITION),
                "non-target partition is live before Begin");

        BulkLoadTargetInfo targetInfo =
                bulkLoadRpc(featureAdmin)
                        .beginBulkLoad(PhysicalTablePath.of(tablePath, WAL_TARGET_PARTITION))
                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(targetInfo.getHandle().getPartitionId()).isEqualTo(targetPartitionId);
        writeAndAwaitPartitionRow(
                table,
                tableInfo.getRowType(),
                liveRows,
                partitionRowForBucket(
                        tableInfo,
                        1,
                        seedBase + 110_000,
                        usedIds,
                        "live-after-begin",
                        WAL_LIVE_PARTITION),
                "non-target partition is live while target is LOADING");

        try (BuiltBulkLoadInput input = buildInput(targetInfo, targetRows, ChangelogImage.WAL)) {
            BulkLoadTestDataBuilder.BuildResult expected = input.getBuildResult();
            assertThat(expected.getBuckets())
                    .hasSize(BUCKET_COUNT)
                    .allSatisfy(
                            bucket -> {
                                assertThat(bucket.getRowCount()).isNull();
                                assertThat(bucket.getLogEndOffset())
                                        .isEqualTo(STRESS_ROWS_PER_BUCKET);
                                assertThat(bucket.getExpectedLogicalRows())
                                        .hasSize(STRESS_ROWS_PER_BUCKET);
                            });

            writeAndAwaitPartitionRow(
                    table,
                    tableInfo.getRowType(),
                    liveRows,
                    partitionRowForBucket(
                            tableInfo,
                            2,
                            seedBase + 120_000,
                            usedIds,
                            "live-before-commit",
                            WAL_LIVE_PARTITION),
                    "non-target partition is live before target Commit");

            int controlledBucket = 0;
            TableBucket controlledTableBucket =
                    new TableBucket(tableInfo.getTableId(), targetPartitionId, controlledBucket);
            int originalLeader = cluster.waitAndGetLeader(controlledTableBucket);
            cluster.stopTabletServer(originalLeader);
            int replacementLeader =
                    awaitReplacementLeader(cluster, controlledTableBucket, originalLeader);
            assertThat(replacementLeader).isNotEqualTo(originalLeader);

            cluster.stopCoordinatorServer();
            cluster.startCoordinatorServer();
            try (Connection recoveredConnection =
                            ConnectionFactory.createConnection(cluster.getClientConfig());
                    FlussAdmin recoveredAdmin = (FlussAdmin) recoveredConnection.getAdmin();
                    Table recoveredTable = recoveredConnection.getTable(tablePath)) {
                BulkLoadTransactionDriver transactionDriver =
                        new BulkLoadTransactionDriver(bulkLoadRpc(recoveredAdmin));
                CompletableFuture<Void> recoveredCommit =
                        publicOperation(
                                () ->
                                        transactionDriver.commitUntilReady(
                                                input.getHandle(),
                                                manifestHandle(input),
                                                E2E_TIMEOUT));
                recoveredCommit.get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                verifyPartitionedPublicState(
                        recoveredAdmin,
                        recoveredTable,
                        tablePath,
                        tableInfo,
                        WAL_TARGET_PARTITION,
                        expected,
                        null,
                        WAL_LIVE_PARTITION,
                        liveRows);

                Object[] ordinaryTail =
                        partitionRowForBucket(
                                tableInfo,
                                controlledBucket,
                                seedBase + 140_000,
                                usedIds,
                                "ordinary-target-tail",
                                WAL_TARGET_PARTITION);
                try {
                    writeAndAwaitPartitionRow(
                            recoveredTable,
                            tableInfo.getRowType(),
                            null,
                            ordinaryTail,
                            "ordinary target write continues from E",
                            Duration.ofSeconds(15));
                } catch (TimeoutException timeout) {
                    throw new AssertionError(
                            "Ordinary target write at E timed out; public operation outcome: "
                                    + timeout
                                    + "; replica facts: "
                                    + cluster.describeReplicaStateForTimeout(controlledTableBucket),
                            timeout);
                }
                assertThat(cluster.triggerAndWaitSnapshot(controlledTableBucket)).isNotNull();
                writeAndAwaitPartitionRow(
                        recoveredTable,
                        tableInfo.getRowType(),
                        liveRows,
                        partitionRowForBucket(
                                tableInfo,
                                1,
                                seedBase + 150_000,
                                usedIds,
                                "live-after-recovery",
                                WAL_LIVE_PARTITION),
                        "non-target partition remains live after both process restarts");
                verifyPartitionedPublicState(
                        recoveredAdmin,
                        recoveredTable,
                        tablePath,
                        tableInfo,
                        WAL_TARGET_PARTITION,
                        expected,
                        ordinaryTail,
                        WAL_LIVE_PARTITION,
                        liveRows);
            }
        }
    }

    private static void assertLoadingAccessRejected(
            Table table, TableInfo tableInfo, Object[] existingRow) {
        awaitLoadingAccessRejection(
                () -> table.newLookup().createLookuper().lookup(row((Integer) existingRow[0])),
                E2E_TIMEOUT,
                "point lookup");
        awaitLoadingAccessRejection(
                () ->
                        publicOperation(
                                () -> {
                                    try (BatchScanner scanner =
                                            table.newScan()
                                                    .createBatchScanner(
                                                            new TableBucket(
                                                                    tableInfo.getTableId(), 0))) {
                                        scanner.pollBatch(Duration.ofSeconds(1));
                                    }
                                }),
                E2E_TIMEOUT,
                "batch scan");
        awaitLoadingAccessRejection(
                () ->
                        publicOperation(
                                () -> {
                                    try (LogScanner scanner = table.newScan().createLogScanner()) {
                                        scanner.subscribeFromBeginning(0);
                                        scanner.poll(Duration.ofSeconds(1));
                                    }
                                }),
                E2E_TIMEOUT,
                "log scan");
        awaitLoadingAccessRejection(
                () -> {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    return writer.upsert(
                            row(
                                    tableInfo.getRowType(),
                                    new Object[] {
                                        (Integer) existingRow[0] + 1_000_000, "fenced-write"
                                    }));
                },
                E2E_TIMEOUT,
                "ordinary write");
    }

    private static void writeAndAwaitPartitionRow(
            Table table,
            RowType rowType,
            List<Object[]> retainedRows,
            Object[] values,
            String description)
            throws Exception {
        writeAndAwaitPartitionRow(table, rowType, retainedRows, values, description, E2E_TIMEOUT);
    }

    private static void writeAndAwaitPartitionRow(
            Table table,
            RowType rowType,
            List<Object[]> retainedRows,
            Object[] values,
            String description,
            Duration writeTimeout)
            throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.upsert(row(rowType, values)).get(writeTimeout.toMillis(), TimeUnit.MILLISECONDS);
        writer.flush();
        awaitPublicPartitionRow(table, rowType, values, description);
        if (retainedRows != null) {
            retainedRows.add(Arrays.copyOf(values, values.length));
        }
    }

    private static void awaitPublicPartitionRow(
            Table table, RowType rowType, Object[] expected, String description) {
        AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
        InternalRow actual;
        try {
            actual =
                    waitValue(
                            () -> {
                                try {
                                    InternalRow observed =
                                            table.newLookup()
                                                    .createLookuper()
                                                    .lookup(row((Integer) expected[0], expected[2]))
                                                    .get(5, TimeUnit.SECONDS)
                                                    .getSingletonRow();
                                    lastOutcome.set(
                                            observed == null ? "missing row" : "row visible");
                                    return Optional.ofNullable(observed);
                                } catch (Exception failure) {
                                    Throwable cause = rootCause(failure);
                                    lastOutcome.set(describe(cause));
                                    rethrowUnlessTransient(cause);
                                    return Optional.empty();
                                }
                            },
                            E2E_TIMEOUT,
                            description);
        } catch (AssertionError timeout) {
            throw new AssertionError(
                    "Timed out waiting for public partition row: "
                            + description
                            + "; last outcome: "
                            + lastOutcome.get(),
                    timeout);
        }
        assertThatRow(actual)
                .withSchema(rowType)
                .isEqualTo(row(rowType, Arrays.copyOf(expected, expected.length)));
    }

    private static void verifyPartitionedPublicState(
            Admin featureAdmin,
            Table table,
            TablePath tablePath,
            TableInfo tableInfo,
            String targetPartition,
            BulkLoadTestDataBuilder.BuildResult targetExpected,
            Object[] targetTail,
            String livePartition,
            List<Object[]> liveRows)
            throws Exception {
        List<Object[]> expectedRows = new ArrayList<>(targetExpected.getExpectedKvRows());
        if (targetTail != null) {
            expectedRows.add(Arrays.copyOf(targetTail, targetTail.length));
        }
        expectedRows.addAll(copyLogicalRows(liveRows));
        List<Object[]> lookupRows = new ArrayList<>();
        for (BulkLoadTestDataBuilder.BucketData bucket : targetExpected.getBuckets()) {
            List<Object[]> bucketRows = bucket.getExpectedLogicalRows();
            lookupRows.add(bucketRows.get(0));
            lookupRows.add(bucketRows.get(bucketRows.size() / 2));
            lookupRows.add(bucketRows.get(bucketRows.size() - 1));
        }
        if (targetTail != null) {
            lookupRows.add(targetTail);
        }
        lookupRows.addAll(liveRows);
        for (Object[] expected : lookupRows) {
            awaitPublicPartitionRow(
                    table, tableInfo.getRowType(), expected, "exact partitioned KV");
        }
        assertThat(
                        table.newLookup()
                                .createLookuper()
                                .lookup(row(Integer.MAX_VALUE, targetPartition))
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .getSingletonRow())
                .isNull();
        awaitExactPartitionedBatchScan(table, expectedRows);

        Map<Integer, List<Object[]>> targetLogs = new LinkedHashMap<>();
        Map<Integer, Integer> targetInsertPrefixes = new LinkedHashMap<>();
        Map<Integer, Long> targetEndOffsets = new LinkedHashMap<>();
        for (BulkLoadTestDataBuilder.BucketData bucket : targetExpected.getBuckets()) {
            targetLogs.put(bucket.getBucketId(), new ArrayList<>());
            targetInsertPrefixes.put(bucket.getBucketId(), 0);
            targetEndOffsets.put(bucket.getBucketId(), bucket.getLogEndOffset());
        }
        if (targetTail != null) {
            int bucket = bucketFor(tableInfo, targetTail);
            targetLogs.get(bucket).add(Arrays.copyOf(targetTail, targetTail.length));
            targetEndOffsets.put(bucket, targetEndOffsets.get(bucket) + 1L);
        }
        assertExactPublicLogs(
                featureAdmin,
                table,
                PhysicalTablePath.of(tablePath, targetPartition),
                targetLogs,
                targetInsertPrefixes,
                targetEndOffsets);

        Map<Integer, List<Object[]>> liveLogs = new LinkedHashMap<>();
        Map<Integer, Integer> liveInsertPrefixes = new LinkedHashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            liveLogs.put(bucket, new ArrayList<>());
            liveInsertPrefixes.put(bucket, 0);
        }
        for (Object[] liveRow : liveRows) {
            liveLogs.get(bucketFor(tableInfo, liveRow)).add(Arrays.copyOf(liveRow, liveRow.length));
        }
        Map<Integer, Long> liveEndOffsets = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<Object[]>> entry : liveLogs.entrySet()) {
            liveEndOffsets.put(entry.getKey(), (long) entry.getValue().size());
        }
        assertExactPublicLogs(
                featureAdmin,
                table,
                PhysicalTablePath.of(tablePath, livePartition),
                liveLogs,
                liveInsertPrefixes,
                liveEndOffsets);
    }

    private static void awaitExactPartitionedBatchScan(Table table, List<Object[]> expectedRows) {
        AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
        List<Object[]> actual;
        try {
            actual =
                    waitValue(
                            () -> {
                                try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                                    List<Object[]> observed =
                                            toLogicalRows(BatchScanUtils.collectRows(scanner));
                                    lastOutcome.set("observed " + observed.size() + " rows");
                                    return observed.size() == expectedRows.size()
                                            ? Optional.of(observed)
                                            : Optional.empty();
                                } catch (Exception failure) {
                                    Throwable cause = rootCause(failure);
                                    lastOutcome.set(describe(cause));
                                    rethrowUnlessTransient(cause);
                                    return Optional.empty();
                                }
                            },
                            E2E_TIMEOUT,
                            "exact partitioned public batch scan");
        } catch (AssertionError timeout) {
            throw new AssertionError(
                    "Partitioned public batch scan did not converge; last outcome: "
                            + lastOutcome.get(),
                    timeout);
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private static void assertExactPublicLogs(
            Admin admin,
            Table table,
            PhysicalTablePath target,
            Map<Integer, List<Object[]>> expectedByBucket,
            Map<Integer, Integer> insertPrefixesByBucket,
            Map<Integer, Long> expectedEndOffsets)
            throws Exception {
        int expectedRecordCount = 0;
        for (Map.Entry<Integer, List<Object[]>> entry : expectedByBucket.entrySet()) {
            expectedRecordCount += entry.getValue().size();
        }
        AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
        try {
            waitValue(
                    () -> {
                        try {
                            Map<Integer, Long> latest =
                                    latestOffsets(admin, target, expectedByBucket.keySet());
                            lastOutcome.set(latest.toString());
                            return latest.equals(expectedEndOffsets)
                                    ? Optional.of(Boolean.TRUE)
                                    : Optional.empty();
                        } catch (Exception failure) {
                            Throwable cause = rootCause(failure);
                            lastOutcome.set(describe(cause));
                            rethrowUnlessTransient(cause);
                            return Optional.empty();
                        }
                    },
                    E2E_TIMEOUT,
                    "BulkLoad offsets become visible for " + target);
        } catch (AssertionError timeout) {
            throw new AssertionError(
                    "BulkLoad offsets did not converge for "
                            + target
                            + "; last outcome: "
                            + lastOutcome.get(),
                    timeout);
        }

        Map<Integer, List<ScanRecord>> actualByBucket = new LinkedHashMap<>();
        for (Integer bucket : expectedByBucket.keySet()) {
            actualByBucket.put(bucket, new ArrayList<>());
        }
        Long partitionId = partitionId(admin, target);
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (Integer bucket : expectedByBucket.keySet()) {
                if (partitionId == null) {
                    scanner.subscribeFromBeginning(bucket);
                } else {
                    scanner.subscribeFromBeginning(partitionId, bucket);
                }
            }
            long deadlineNanos = System.nanoTime() + E2E_TIMEOUT.toNanos();
            int actualCount = 0;
            while (actualCount < expectedRecordCount && System.nanoTime() < deadlineNanos) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets()) {
                    assertThat(bucket.getPartitionId()).isEqualTo(partitionId);
                    List<ScanRecord> bucketRecords = records.records(bucket);
                    actualByBucket.get(bucket.getBucket()).addAll(bucketRecords);
                    actualCount += bucketRecords.size();
                }
            }
            assertThat(actualCount).isEqualTo(expectedRecordCount);
        }
        for (Map.Entry<Integer, List<Object[]>> entry : expectedByBucket.entrySet()) {
            List<ScanRecord> actual = actualByBucket.get(entry.getKey());
            assertThat(actual).hasSameSizeAs(entry.getValue());
            int insertPrefix = insertPrefixesByBucket.get(entry.getKey());
            List<Object[]> actualInsertRows = new ArrayList<>(insertPrefix);
            long firstExpectedOffset =
                    expectedEndOffsets.get(entry.getKey()) - entry.getValue().size();
            for (int offset = 0; offset < entry.getValue().size(); offset++) {
                ScanRecord record = actual.get(offset);
                assertThat(record.logOffset()).isEqualTo(firstExpectedOffset + offset);
                assertThat(record.getChangeType())
                        .isEqualTo(
                                offset < insertPrefix
                                        ? ChangeType.INSERT
                                        : ChangeType.UPDATE_AFTER);
                if (offset < insertPrefix) {
                    actualInsertRows.add(toLogicalRow(record.getRow()));
                } else {
                    assertThatRow(record.getRow())
                            .withSchema(table.getTableInfo().getRowType())
                            .isEqualTo(
                                    row(
                                            table.getTableInfo().getRowType(),
                                            entry.getValue().get(offset)));
                }
            }
            assertThat(actualInsertRows)
                    .usingRecursiveComparison()
                    .ignoringCollectionOrder()
                    .isEqualTo(entry.getValue().subList(0, insertPrefix));
        }
    }

    private static Map<Integer, Long> latestOffsets(
            Admin admin, PhysicalTablePath target, Set<Integer> buckets) throws Exception {
        List<Integer> bucketList = new ArrayList<>(buckets);
        return target.getPartitionName() == null
                ? admin.listOffsets(target.getTablePath(), bucketList, new OffsetSpec.LatestSpec())
                        .all()
                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                : admin.listOffsets(
                                target.getTablePath(),
                                target.getPartitionName(),
                                bucketList,
                                new OffsetSpec.LatestSpec())
                        .all()
                        .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void assertNoBulkLoadRemoteLogArtifacts(
            FlussClusterExtension cluster, TableInfo tableInfo, PhysicalTablePath target)
            throws Exception {
        ZooKeeperClient zkClient = cluster.getZooKeeperClient();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucket);
            assertThat(zkClient.pathExists(ZkData.BucketRemoteLogsZNode.path(tableBucket)))
                    .isFalse();
            FsPath remoteTabletDirectory =
                    FlussPaths.remoteLogTabletDir(
                            new FsPath(
                                    tableInfo.getRemoteDataDir(), FlussPaths.REMOTE_LOG_DIR_NAME),
                            target,
                            tableBucket);
            java.nio.file.Path localRemoteTabletDirectory =
                    Paths.get(remoteTabletDirectory.toUri());
            if (Files.exists(localRemoteTabletDirectory)) {
                try (Stream<java.nio.file.Path> paths = Files.walk(localRemoteTabletDirectory)) {
                    assertThat(paths.filter(Files::isRegularFile)).isEmpty();
                }
            }
        }
    }

    private static Long partitionId(Admin admin, PhysicalTablePath target) throws Exception {
        if (target.getPartitionName() == null) {
            return null;
        }
        return admin
                .listPartitionInfos(target.getTablePath())
                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .stream()
                .filter(info -> info.getPartitionName().equals(target.getPartitionName()))
                .map(PartitionInfo::getPartitionId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing partition metadata for " + target));
    }

    private static int awaitReplacementLeader(
            FlussClusterExtension cluster, TableBucket tableBucket, int originalLeader) {
        return waitValue(
                () -> {
                    Optional<LeaderAndIsr> role =
                            cluster.getZooKeeperClient().getLeaderAndIsr(tableBucket);
                    if (role.isPresent() && role.get().leader() != originalLeader) {
                        return Optional.of(role.get().leader());
                    }
                    return Optional.empty();
                },
                E2E_TIMEOUT,
                "replacement leader after the original leader is permanently stopped");
    }

    private static void awaitPublicRow(
            Table table, RowType rowType, Object[] expected, String description) {
        AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
        InternalRow actual;
        try {
            actual =
                    waitValue(
                            () -> {
                                try {
                                    InternalRow observed =
                                            table.newLookup()
                                                    .createLookuper()
                                                    .lookup(row((Integer) expected[0]))
                                                    .get(5, TimeUnit.SECONDS)
                                                    .getSingletonRow();
                                    lastOutcome.set(
                                            observed == null ? "missing row" : "row visible");
                                    return Optional.ofNullable(observed);
                                } catch (Exception failure) {
                                    Throwable cause = rootCause(failure);
                                    lastOutcome.set(describe(cause));
                                    rethrowUnlessTransient(cause);
                                    return Optional.empty();
                                }
                            },
                            E2E_TIMEOUT,
                            description);
        } catch (AssertionError timeout) {
            throw new AssertionError(
                    "Timed out waiting for public recovery: "
                            + description
                            + "; last public outcome: "
                            + lastOutcome.get(),
                    timeout);
        }
        assertThatRow(actual)
                .withSchema(rowType)
                .isEqualTo(row(rowType, Arrays.copyOf(expected, expected.length)));
    }

    private static void awaitPublicBucketReadiness(Table table, List<Object[]> probes) {
        Lookuper lookuper = table.newLookup().createLookuper();
        for (Object[] probe : probes) {
            AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
            try {
                waitValue(
                        () -> {
                            try {
                                InternalRow actual =
                                        lookuper.lookup(row((Integer) probe[0]))
                                                .get(5, TimeUnit.SECONDS)
                                                .getSingletonRow();
                                lastOutcome.set(actual == null ? "missing row" : "unexpected row");
                                return actual == null
                                        ? Optional.of(Boolean.TRUE)
                                        : Optional.empty();
                            } catch (Exception failure) {
                                Throwable cause = rootCause(failure);
                                lastOutcome.set(describe(cause));
                                rethrowUnlessTransient(cause);
                                return Optional.empty();
                            }
                        },
                        E2E_TIMEOUT,
                        "public lookup routing for bucket readiness");
            } catch (AssertionError timeout) {
                throw new AssertionError(
                        "Timed out waiting for public bucket readiness; last public outcome: "
                                + lastOutcome.get(),
                        timeout);
            }
        }
    }

    private static void verifyPublicRows(Table table, RowType rowType, List<Object[]> expectedRows)
            throws Exception {
        Lookuper lookuper = table.newLookup().createLookuper();
        long lookupDeadlineNanos = System.nanoTime() + E2E_TIMEOUT.toNanos();
        List<CompletableFuture<LookupResult>> lookupFutures = new ArrayList<>(expectedRows.size());
        for (Object[] expected : expectedRows) {
            lookupFutures.add(lookuper.lookup(row((Integer) expected[0])));
        }
        CompletableFuture<LookupResult> absentLookup = lookuper.lookup(row(Integer.MAX_VALUE));

        for (int rowIndex = 0; rowIndex < expectedRows.size(); rowIndex++) {
            Object[] expected = expectedRows.get(rowIndex);
            int key = (Integer) expected[0];
            InternalRow actual =
                    awaitLookup(
                                    lookupFutures.get(rowIndex),
                                    lookupDeadlineNanos,
                                    "public row for key " + key)
                            .getSingletonRow();
            assertThatRow(actual)
                    .as("public lookup result for key %s", key)
                    .withSchema(rowType)
                    .isEqualTo(row(rowType, Arrays.copyOf(expected, expected.length)));
        }

        assertThat(
                        awaitLookup(
                                        absentLookup,
                                        lookupDeadlineNanos,
                                        "absent public key " + Integer.MAX_VALUE)
                                .getSingletonRow())
                .as("public lookup result for absent key %s", Integer.MAX_VALUE)
                .isNull();

        List<InternalRow> scanned = awaitPublicBatchScan(table);
        assertThat(toLogicalRows(scanned)).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private static LookupResult awaitLookup(
            CompletableFuture<LookupResult> lookupFuture, long deadlineNanos, String description) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new AssertionError("Timed out verifying " + description + ".");
        }
        try {
            return lookupFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (ExecutionException failure) {
            throw new AssertionError("Lookup failed for " + description + ".", failure.getCause());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(
                    "Interrupted while verifying " + description + ".", interrupted);
        } catch (TimeoutException timeout) {
            throw new AssertionError("Timed out verifying " + description + ".", timeout);
        }
    }

    private static List<InternalRow> awaitPublicBatchScan(Table table) {
        AtomicReference<String> lastFailure = new AtomicReference<>("not attempted");
        try {
            return waitValue(
                    () -> {
                        try (BatchScanner scanner = table.newScan().createBatchScanner()) {
                            return Optional.of(BatchScanUtils.collectRows(scanner));
                        } catch (RuntimeException failure) {
                            Throwable cause = rootCause(failure);
                            lastFailure.set(describe(cause));
                            rethrowUnlessTransient(cause);
                            return Optional.empty();
                        }
                    },
                    E2E_TIMEOUT,
                    "public batch scan after metadata convergence");
        } catch (AssertionError timeout) {
            throw new AssertionError(
                    "Public batch scan did not converge; last failure: " + lastFailure.get(),
                    timeout);
        }
    }

    private static void verifyEmptyPublicTarget(
            Admin featureAdmin, Table table, TablePath tablePath) throws Exception {
        assertThat(awaitPublicBatchScan(table)).isEmpty();
        Map<Integer, Long> expectedEndOffsets = new LinkedHashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            expectedEndOffsets.put(bucket, 0L);
        }
        assertThat(
                        featureAdmin
                                .listOffsets(
                                        tablePath,
                                        new ArrayList<>(expectedEndOffsets.keySet()),
                                        new OffsetSpec.LatestSpec())
                                .all()
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .containsExactlyInAnyOrderEntriesOf(expectedEndOffsets);
    }

    private static void verifySingleOrdinaryLogAfterAbort(
            Admin featureAdmin, Table table, TablePath tablePath, Object[] ordinaryRow)
            throws Exception {
        int bucket = bucketFor(table.getTableInfo(), ordinaryRow);
        assertThat(
                        featureAdmin
                                .listOffsets(
                                        tablePath,
                                        Arrays.asList(0, 1, 2),
                                        new OffsetSpec.LatestSpec())
                                .all()
                                .get(E2E_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .containsEntry(bucket, 1L)
                .allSatisfy(
                        (actualBucket, offset) ->
                                assertThat(offset).isEqualTo(actualBucket == bucket ? 1L : 0L));
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(bucket);
            ScanRecord record =
                    waitValue(
                            () -> {
                                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                                List<ScanRecord> bucketRecords =
                                        records.records(
                                                new TableBucket(
                                                        table.getTableInfo().getTableId(), bucket));
                                return bucketRecords.isEmpty()
                                        ? Optional.empty()
                                        : Optional.of(bucketRecords.get(0));
                            },
                            E2E_TIMEOUT,
                            "ordinary log record after pre-commit Abort");
            assertThat(record.logOffset()).isZero();
            assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
            assertThatRow(record.getRow())
                    .withSchema(table.getTableInfo().getRowType())
                    .isEqualTo(row(table.getTableInfo().getRowType(), ordinaryRow));
        }
    }

    private static List<Object[]> toLogicalRows(List<InternalRow> rows) {
        List<Object[]> logicalRows = new ArrayList<>(rows.size());
        for (InternalRow row : rows) {
            logicalRows.add(toLogicalRow(row));
        }
        return logicalRows;
    }

    private static Object[] toLogicalRow(InternalRow row) {
        if (row.getFieldCount() == 2) {
            return new Object[] {
                row.getInt(0), row.isNullAt(1) ? null : row.getString(1).toString()
            };
        }
        return new Object[] {
            row.getInt(0),
            row.isNullAt(1) ? null : row.getString(1).toString(),
            row.isNullAt(2) ? null : row.getString(2).toString()
        };
    }

    private static List<Object[]> copyLogicalRows(List<Object[]> rows) {
        List<Object[]> copies = new ArrayList<>(rows.size());
        for (Object[] values : rows) {
            copies.add(Arrays.copyOf(values, values.length));
        }
        return copies;
    }

    private static int firstNonEmptyBucket(BulkLoadTestDataBuilder.BuildResult expected) {
        for (BulkLoadTestDataBuilder.BucketData bucket : expected.getBuckets()) {
            if (bucket.getLogEndOffset() > 0L) {
                return bucket.getBucketId();
            }
        }
        throw new AssertionError("FULL acceptance input must contain a non-empty bucket.");
    }

    private static Object[] rowForBucket(
            TableInfo tableInfo,
            int targetBucket,
            int startId,
            Set<Integer> usedIds,
            String payloadPrefix) {
        for (int candidate = startId; candidate < startId + 100_000; candidate++) {
            Object[] values = new Object[] {candidate, payloadPrefix + '-' + candidate};
            if (!usedIds.contains(candidate) && bucketContains(tableInfo, targetBucket, values)) {
                usedIds.add(candidate);
                return values;
            }
        }
        throw new AssertionError("Unable to find deterministic row for bucket " + targetBucket);
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        int commonLength = Math.min(left.length, right.length);
        for (int index = 0; index < commonLength; index++) {
            int comparison = (left[index] & 0xff) - (right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return left.length - right.length;
    }

    private static void seedLegacyBucketDb(
            java.nio.file.Path callerParent, TableInfo tableInfo, Object[] values)
            throws Exception {
        java.nio.file.Path dbDirectory = callerParent.resolve("db");
        Files.createDirectories(dbDirectory);
        InternalRow logicalRow = row(tableInfo.getRowType(), values);
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        InternalRow.FieldGetter[] fieldGetters =
                new InternalRow.FieldGetter[tableInfo.getRowType().getFieldCount()];
        for (int field = 0; field < fieldGetters.length; field++) {
            fieldGetters[field] =
                    InternalRow.createFieldGetter(tableInfo.getRowType().getTypeAt(field), field);
        }
        RocksDB.loadLibrary();
        try (RowEncoder rowEncoder =
                        RowEncoder.create(
                                tableInfo.getTableConfig().getKvFormat(), tableInfo.getRowType());
                Options options =
                        new Options()
                                .setCreateIfMissing(true)
                                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                                .setTableFormatConfig(new BlockBasedTableConfig());
                RocksDB db = RocksDB.open(options, dbDirectory.toString())) {
            rowEncoder.startNewRow();
            for (int field = 0; field < fieldGetters.length; field++) {
                rowEncoder.encodeField(field, fieldGetters[field].getFieldOrNull(logicalRow));
            }
            db.put(
                    keyEncoder.encodeKey(logicalRow),
                    ValueEncoder.encodeValue(
                            (short) tableInfo.getSchemaId(), rowEncoder.finishRow().copy()));
        }
    }

    private static Object[] partitionRowForBucket(
            TableInfo tableInfo,
            int targetBucket,
            int startId,
            Set<Integer> usedIds,
            String payloadPrefix,
            String partition) {
        for (int candidate = startId; candidate < startId + 100_000; candidate++) {
            Object[] values = new Object[] {candidate, payloadPrefix + '-' + candidate, partition};
            if (!usedIds.contains(candidate) && bucketContains(tableInfo, targetBucket, values)) {
                usedIds.add(candidate);
                return values;
            }
        }
        throw new AssertionError(
                "Unable to find deterministic partition row for bucket " + targetBucket);
    }

    private static List<Object[]> partitionRowsForBucket(
            TableInfo tableInfo,
            int targetBucket,
            int startId,
            Set<Integer> usedIds,
            int count,
            String partition) {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        List<Object[]> rows = new ArrayList<>(count);
        for (int candidate = startId; rows.size() < count; candidate++) {
            String payload;
            if (candidate % 101 == 0) {
                payload = null;
            } else {
                StringBuilder value = new StringBuilder(candidate % 97 == 0 ? "生产数据-" : "target-");
                int suffixLength = candidate % 257;
                for (int index = 0; index < suffixLength; index++) {
                    value.append((char) ('a' + index % 26));
                }
                payload = value.append('-').append(candidate).toString();
            }
            Object[] values = new Object[] {candidate, payload, partition};
            int bucket =
                    bucketingFunction.bucketing(
                            bucketKeyEncoder.encodeKey(row(tableInfo.getRowType(), values)),
                            tableInfo.getNumBuckets());
            if (bucket == targetBucket && usedIds.add(candidate)) {
                rows.add(values);
            }
        }
        return rows;
    }

    private static boolean bucketContains(TableInfo tableInfo, int targetBucket, Object[] values) {
        return bucketFor(tableInfo, values) == targetBucket;
    }

    private static int bucketFor(TableInfo tableInfo, Object[] values) {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        return bucketingFunction.bucketing(
                bucketKeyEncoder.encodeKey(row(tableInfo.getRowType(), values)),
                tableInfo.getNumBuckets());
    }

    private static CompletableFuture<Void> publicOperation(CheckedRunnable operation) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        operation.run();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private static Configuration featureClusterConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        configuration.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofHours(1));
        configuration.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        configuration.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        configuration.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return configuration;
    }

    private static void withFeatureCluster(CheckedClusterScenario scenario) throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setClusterConf(featureClusterConfiguration())
                        .build();
        Throwable scenarioFailure = null;
        try {
            cluster.start();
            scenario.run(cluster);
        } catch (Exception | AssertionError failure) {
            scenarioFailure = failure;
            throw failure;
        } finally {
            try {
                cluster.close();
            } catch (Exception closeFailure) {
                if (scenarioFailure != null) {
                    scenarioFailure.addSuppressed(closeFailure);
                } else {
                    throw new AssertionError("BulkLoad test cluster close failed.", closeFailure);
                }
            }
        }
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private interface CheckedClusterScenario {
        void run(FlussClusterExtension cluster) throws Exception;
    }

    private static CompletableFuture<BulkLoadStatus> commit(
            FlussAdmin admin, BuiltBulkLoadInput input) {
        return bulkLoadRpc(admin).commitBulkLoad(input.getHandle(), manifestHandle(input));
    }

    private static BulkLoadRpcClient bulkLoadRpc(FlussAdmin admin) {
        return new BulkLoadRpcClient(admin.getAdminGateway());
    }

    private static BulkLoadFileHandle manifestHandle(BuiltBulkLoadInput input) {
        return new BulkLoadFileHandle(
                input.getBuildResult().getManifestPath().toString(),
                input.getManifestLength(),
                input.getManifestSha256());
    }

    private static BuiltBulkLoadInput buildInput(
            BulkLoadTargetInfo targetInfo,
            List<Object[]> logicalRows,
            ChangelogImage changelogImage) {
        checkNotNull(targetInfo, "BulkLoad target info must not be null.");
        try {
            return new BuiltBulkLoadInput(
                    targetInfo,
                    new BulkLoadTestDataBuilder().build(targetInfo, logicalRows, changelogImage));
        } catch (Exception e) {
            throw new CompletionException("Failed to build public BulkLoad input.", e);
        }
    }

    private static void awaitLoadingAccessRejection(
            Supplier<? extends CompletableFuture<?>> publicAccess,
            Duration timeout,
            String description) {
        AtomicReference<String> lastOutcome = new AtomicReference<>("not attempted");
        try {
            waitValue(
                    () -> {
                        try {
                            publicAccess.get().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                            lastOutcome.set("succeeded");
                        } catch (ExecutionException | CompletionException failure) {
                            Throwable cause = rootCause(failure);
                            lastOutcome.set(describe(cause));
                            if (isLoadingAccessRejection(cause)) {
                                return Optional.of(Boolean.TRUE);
                            }
                        }
                        return Optional.empty();
                    },
                    timeout,
                    "Timed out waiting for public LOADING access rejection: " + description);
        } catch (AssertionError timeoutFailure) {
            throw new AssertionError(
                    "Timed out waiting for public LOADING access rejection: "
                            + description
                            + "; last public access outcome: "
                            + lastOutcome.get(),
                    timeoutFailure);
        }
    }

    private static boolean isLoadingAccessRejection(Throwable failure) {
        return failure instanceof ApiException
                && failure.getMessage() != null
                && failure.getMessage()
                        .contains("BulkLoad target is LOADING and rejects external access");
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static void rethrowUnlessTransient(Throwable failure) {
        if (!(failure instanceof TimeoutException) && !(failure instanceof RetriableException)) {
            ExceptionUtils.rethrow(failure);
        }
    }

    private static String describe(Throwable outcome) {
        String message = outcome.getMessage();
        return outcome.getClass().getSimpleName()
                + (message == null || message.isEmpty() ? "" : ": " + message);
    }

    /** Frozen target information and independently retained deterministic input. */
    public static final class BuiltBulkLoadInput implements AutoCloseable {
        private final BulkLoadTargetInfo targetInfo;
        private final BulkLoadTestDataBuilder.BuildResult buildResult;

        private BuiltBulkLoadInput(
                BulkLoadTargetInfo targetInfo, BulkLoadTestDataBuilder.BuildResult buildResult) {
            this.targetInfo = targetInfo;
            this.buildResult = buildResult;
        }

        /** Returns the frozen target information that authorized input creation. */
        public BulkLoadTargetInfo getTargetInfo() {
            return targetInfo;
        }

        /** Returns the handle used by subsequent lifecycle calls. */
        public BulkLoadHandle getHandle() {
            return targetInfo.getHandle();
        }

        /** Returns the retained manifest length. */
        public long getManifestLength() {
            return buildResult.getManifestLength();
        }

        /** Returns the retained manifest digest. */
        public String getManifestSha256() {
            return buildResult.getManifestSha256();
        }

        /** Returns independently retained rows, per-bucket offsets, files, and digests. */
        public BulkLoadTestDataBuilder.BuildResult getBuildResult() {
            return buildResult;
        }

        @Override
        public void close() {
            buildResult.close();
        }
    }
}
