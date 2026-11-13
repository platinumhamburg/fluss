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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBExtension;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Initial import, transaction-independent recovery, replication and later snapshots. */
class BulkLoadITCase {
    @RegisterExtension public final RocksDBExtension rocksDB = new RocksDBExtension();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommittedInitialImportRestoresAcrossTakeover(boolean remoteLogEnabled)
            throws Exception {
        Configuration conf =
                new Configuration()
                        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1))
                        .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1)
                        .set(
                                ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION,
                                remoteLogEnabled ? Duration.ofHours(1) : Duration.ZERO);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(2)
                        .setClusterConf(conf)
                        .build();
        try {
            cluster.start();
            TablePath path = TablePath.of("bulkload_db", "orders");
            long tableId =
                    createTable(
                            cluster,
                            path,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA_PK)
                                    .distributedBy(1, "a")
                                    .build()
                                    .withReplicationFactor(2));
            TableBucket bucket = new TableBucket(tableId, 0);
            cluster.waitUntilAllReplicaReady(bucket);
            CoordinatorGateway coordinator = cluster.newCoordinatorClient();
            BeginBulkLoadRequest begin = begin(path, "load-a");
            PbBulkLoadHandle handle =
                    coordinator.beginBulkLoad(begin).get().getStatus().getHandle();
            new ZkSequenceIDCounter(
                            cluster.getZooKeeperClient().getCuratorClient(),
                            ZkData.BucketSnapshotSequenceIdZNode.path(bucket))
                    .getAndAdd(1L << 40);
            AllocateBulkLoadBucketSnapshotResponse allocation =
                    coordinator
                            .allocateBulkLoadBucketSnapshot(
                                    new AllocateBulkLoadBucketSnapshotRequest()
                                            .setHandle(handle)
                                            .setBucketId(0))
                            .get();
            assertThat(allocation.getLogOffset()).isZero();
            assertThat(allocation.getSnapshotId()).isGreaterThan(Integer.MAX_VALUE);
            List<KvRecord> imported = new ArrayList<>();
            imported.addAll(genKvRecords(new Object[] {1, "import-one"}));
            imported.addAll(genKvRecords(new Object[] {3, "import-three"}));
            CompletedSnapshotHandle input =
                    produceSnapshot(
                            cluster,
                            path,
                            bucket,
                            imported,
                            allocation.getLogOffset(),
                            allocation.getSnapshotId());
            cluster.stopTabletServer(0);
            cluster.stopTabletServer(1);
            CommitBulkLoadBucketSnapshotRequest commit =
                    new CommitBulkLoadBucketSnapshotRequest().setHandle(handle).setBucketId(0);
            coordinator.commitBulkLoadBucketSnapshot(commit).get();
            assertThat(
                            coordinator
                                    .commitBulkLoad(
                                            new org.apache.fluss.rpc.messages
                                                            .CommitBulkLoadRequest()
                                                    .setHandle(handle))
                                    .get()
                                    .getStatus()
                                    .getState())
                    .isEqualTo(1);
            assertThat(cluster.getZooKeeperClient().getRecoverySnapshot(bucket).get().zkVersion())
                    .isZero();
            PbBulkLoadHandle next =
                    coordinator.beginBulkLoad(begin(path, "load-b")).get().getStatus().getHandle();
            coordinator.abortBulkLoad(new AbortBulkLoadRequest().setHandle(next)).get();
            cluster.stopCoordinatorServer();
            cluster.startCoordinatorServer();
            cluster.startTabletServer(0);
            cluster.startTabletServer(1);
            List<KvRecord> tail = genKvRecords(new Object[] {4, "after-import"});
            retry(Duration.ofMinutes(1), () -> putRecords(cluster, bucket, tail));
            imported.addAll(tail);
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        assertRows(cluster, bucket, imported);
                    });
            assertThat(
                            cluster.getTabletServerById(cluster.waitAndGetLeader(bucket))
                                    .getReplicaManager()
                                    .getReplicaOrException(bucket)
                                    .getLogTablet()
                                    .localLogStartOffset())
                    .isZero();
            assertThat(
                            cluster.getZooKeeperClient()
                                    .getTableBucketLatestSnapshot(bucket)
                                    .get()
                                    .getSnapshotId())
                    .isEqualTo(allocation.getSnapshotId());
            assertThat(
                            input.getMetadataFilePath()
                                    .getFileSystem()
                                    .exists(input.getMetadataFilePath()))
                    .isTrue();

            cluster.newCoordinatorClient().commitBulkLoadBucketSnapshot(commit).get();
            assertRows(cluster, bucket, imported);
            waitForFullIsr(cluster, bucket);
            int oldLeader = cluster.waitAndGetLeader(bucket);
            cluster.stopTabletServer(oldLeader);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(cluster.waitAndGetLeader(bucket)).isNotEqualTo(oldLeader));
            assertRows(cluster, bucket, imported);
            cluster.startTabletServer(oldLeader);
            cluster.waitUntilAllReplicaReady(bucket);
            Replica servingReplica =
                    cluster.getTabletServerById(cluster.waitAndGetLeader(bucket))
                            .getReplicaManager()
                            .getReplicaOrException(bucket);
            KvTablet servingKv = servingReplica.getKvTablet();
            CompletedSnapshot ordinary = cluster.triggerAndWaitSnapshot(bucket);
            assertThat(ordinary.getLogOffset()).isEqualTo(1);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            input.getMetadataFilePath()
                                                    .getFileSystem()
                                                    .exists(input.getMetadataFilePath()))
                                    .isFalse());
            assertThat(
                            cluster.getZooKeeperClient()
                                    .getRecoverySnapshot(bucket)
                                    .get()
                                    .data()
                                    .snapshotId)
                    .isEqualTo(ordinary.getSnapshotID());
            assertThat(
                            queryRecovery(cluster, bucket, allocation.getSnapshotId())
                                    .hasRecoveredSnapshotId())
                    .isFalse();
            long selectedSnapshotId =
                    cluster.getZooKeeperClient()
                            .getRecoverySnapshot(bucket)
                            .get()
                            .data()
                            .snapshotId;
            retry(
                    Duration.ofSeconds(10),
                    () -> {
                        PbNotifyLeaderAndIsrRespForBucket response =
                                queryRecovery(cluster, bucket, selectedSnapshotId);
                        assertThat(response.hasRecoveredSnapshotId()).isTrue();
                        assertThat(response.getRecoveredSnapshotId()).isEqualTo(selectedSnapshotId);
                    });
            assertThat(servingReplica.getKvTablet()).isSameAs(servingKv);
            waitForFullIsr(cluster, bucket);
            int leader = cluster.waitAndGetLeader(bucket);
            cluster.stopTabletServer(leader);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(cluster.waitAndGetLeader(bucket)).isNotEqualTo(leader));
            assertRows(cluster, bucket, imported);
        } finally {
            cluster.close();
        }
    }

    private static void waitForFullIsr(FlussClusterExtension cluster, TableBucket bucket)
            throws Exception {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(cluster.getZooKeeperClient().getLeaderAndIsr(bucket).get().isr())
                                .hasSize(2));
    }

    private static PbNotifyLeaderAndIsrRespForBucket queryRecovery(
            FlussClusterExtension cluster, TableBucket bucket, long selectedSnapshotId)
            throws Exception {
        int leader = cluster.waitAndGetLeader(bucket);
        Replica replica =
                cluster.getTabletServerById(leader)
                        .getReplicaManager()
                        .getReplicaOrException(bucket);
        NotifyLeaderAndIsrData data =
                new NotifyLeaderAndIsrData(
                        replica.getPhysicalTablePath(),
                        bucket,
                        Arrays.asList(0, 1),
                        cluster.getZooKeeperClient().getLeaderAndIsr(bucket).get(),
                        replica.getRoutingBucketCount(),
                        replica.getBucketCountEpoch(),
                        selectedSnapshotId);
        return cluster.newTabletServerClientForNode(leader)
                .notifyLeaderAndIsr(
                        ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest(
                                replica.getCoordinatorEpoch(),
                                Collections.singletonList(
                                        ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr(data))))
                .get()
                .getNotifyBucketsLeaderRespAt(0);
    }

    private static BeginBulkLoadRequest begin(TablePath path, String id) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(id);
        request.setTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        return request;
    }

    private CompletedSnapshotHandle produceSnapshot(
            FlussClusterExtension cluster,
            TablePath tablePath,
            TableBucket bucket,
            List<KvRecord> records,
            long offset,
            long snapshotId)
            throws Exception {
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(cluster.getRemoteDataDir(), "kv"),
                        PhysicalTablePath.of(tablePath),
                        bucket);
        FsPath location = FlussPaths.remoteKvSnapshotDir(tabletDir, snapshotId);
        SnapshotLocation snapshotLocation =
                new SnapshotLocation(
                        location.getFileSystem(),
                        location,
                        FlussPaths.remoteKvSharedDir(tabletDir),
                        1024);
        for (Tuple2<byte[], byte[]> entry : getKeyValuePairs(records)) {
            rocksDB.getRocksDb().put(entry.f0, entry.f1);
        }
        ExecutorService uploader = Executors.newSingleThreadExecutor();
        KvSnapshotHandle files;
        try (ResourceGuard guard = new ResourceGuard();
                CloseableRegistry registry = new CloseableRegistry();
                RocksIncrementalSnapshot snapshot =
                        new RocksIncrementalSnapshot(
                                new HashMap<>(),
                                rocksDB.getRocksDb(),
                                guard,
                                new KvSnapshotDataUploader(uploader),
                                rocksDB.getRockDbDir(),
                                -1L,
                                new ThreadSafeSimpleCounter())) {
            files =
                    snapshot.asyncSnapshot(
                                    snapshot.syncPrepareResources(snapshotId),
                                    snapshotId,
                                    new TabletState(offset, (long) records.size(), null),
                                    snapshotLocation)
                            .get(registry)
                            .getKvSnapshotHandle();
        } finally {
            uploader.shutdownNow();
        }
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        bucket,
                        snapshotId,
                        location.toString(),
                        fileMetadata(files.getSharedKvFileHandles()),
                        fileMetadata(files.getPrivateFileHandles()),
                        files.getIncrementalSize(),
                        offset,
                        (long) records.size(),
                        Collections.emptyList());
        FsPath metadataPath = CompletedSnapshot.getMetadataFilePath(location);
        try (FSDataOutputStream output =
                metadataPath
                        .getFileSystem()
                        .create(metadataPath, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(KvSnapshotFileMetadataJsonSerde.toJson(metadata));
        }
        return new CompletedSnapshotHandle(snapshotId, metadataPath, offset);
    }

    private static List<KvSnapshotFileMetadata.FileHandle> fileMetadata(
            List<KvFileHandleAndLocalPath> files) {
        List<KvSnapshotFileMetadata.FileHandle> result = new ArrayList<>();
        for (KvFileHandleAndLocalPath file : files) {
            result.add(
                    new KvSnapshotFileMetadata.FileHandle(
                            file.getKvFileHandle().getFilePath(),
                            file.getKvFileHandle().getSize(),
                            file.getLocalPath()));
        }
        return result;
    }

    private static void putRecords(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        PutKvResponse response =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket))
                        .putKv(
                                newPutKvRequest(
                                        bucket.getTableId(),
                                        bucket.getBucket(),
                                        -1,
                                        toKvRecordBatch(records)))
                        .get();
        assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();
    }

    private static void assertRows(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        TabletServerGateway gateway =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket));
        for (Tuple2<byte[], byte[]> keyValue : getKeyValuePairs(records)) {
            assertLookupResponse(
                    gateway.lookup(
                                    newLookupRequest(
                                            bucket.getTableId(), bucket.getBucket(), keyValue.f0))
                            .get(),
                    keyValue.f1);
        }
    }
}
