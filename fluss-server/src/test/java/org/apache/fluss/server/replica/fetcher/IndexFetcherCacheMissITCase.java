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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.server.testutils.KvTestUtils.putKvToPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that reproduces the root cause of the index fetch dead loop when DataBucket
 * partition metadata is missing from a TabletServer's metadata cache.
 *
 * <p>Root cause: When a Coordinator's UpdateMetadata RPC fails for a TabletServer (e.g., during
 * rolling upgrade, transient network issue), that server's metadataCache never receives the
 * DataBucket partition metadata. The IndexFetcherManager's {@code handleRunning()} then gets {@code
 * Optional.empty()} from {@code metadataCache.getBucketLeaderId(dataBucket)} and marks the target
 * FAILED. Meanwhile {@code queryLeader()} succeeds via ZK fallback, creating an infinite loop:
 *
 * <ol>
 *   <li>{@code queryLeader()} ZK fallback → RUNNING
 *   <li>{@code handleRunning()} cache miss → FAILED
 *   <li>5s retry → back to step 1
 * </ol>
 *
 * <p>This test simulates the cache miss by manually removing partition metadata from a server's
 * cache after initial replication succeeds, then verifying that subsequent index replication
 * stalls.
 */
public class IndexFetcherCacheMissITCase {
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int BUCKET_COUNT = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        conf.set(ConfigOptions.INDEX_REPLICA_FETCH_WAIT_MAX_TIME, Duration.ofMillis(200));
        conf.set(ConfigOptions.LOG_REPLICA_WRITE_OPERATION_PURGE_NUMBER, 100);
        conf.set(ConfigOptions.LOG_REPLICA_FETCH_OPERATION_PURGE_NUMBER, 100);
        return conf;
    }

    /**
     * Reproduces the dead loop caused by missing DataBucket partition metadata in cache.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Create partitioned table with secondary index, create partition, write data
     *   <li>Wait for initial index replication to complete (proves normal path works)
     *   <li>Remove DataBucket partition metadata from one server's cache (simulates RPC failure)
     *   <li>Write more data to the partition
     *   <li>Verify index replication recovers and processes the new data
     * </ol>
     *
     * <p>Without the fix (ZK fallback in handleRunning), this test will timeout because the
     * IndexFetcherManager enters the RUNNING→FAILED dead loop after cache removal.
     */
    @Test
    void testIndexFetchRecoveryAfterCacheMiss() throws Exception {
        // Step 1: Create a partitioned table with secondary index
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .index("idx_name", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(BUCKET_COUNT, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        TablePath tablePath = TablePath.of("test_db", "cache_miss_test");
        long dataTableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TablePath indexTablePath = TablePath.forIndexTable(tablePath, "idx_name");
        TableInfo indexTableInfo = metadataManager.getTable(indexTablePath);
        assertThat(indexTableInfo).isNotNull();
        long indexTableId = indexTableInfo.getTableId();

        // Wait for index table buckets to be ready
        for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
            TableBucket indexBucket = new TableBucket(indexTableId, bucketId);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexBucket);
        }

        // Step 2: Create a partition and write initial data
        String partitionValue =
                java.time.LocalDate.now()
                        .plusDays(100)
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        PartitionSpec partitionSpec =
                new PartitionSpec(Collections.singletonMap("dt", partitionValue));
        long partitionId =
                createPartition(FLUSS_CLUSTER_EXTENSION, tablePath, partitionSpec, false);

        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(dataTableId, partitionId);

        RowType keyType =
                DataTypes.ROW(
                        new org.apache.fluss.types.DataField("id", DataTypes.INT()),
                        new org.apache.fluss.types.DataField("dt", DataTypes.STRING()));
        RowType rowType = schema.getRowType();

        TableBucket dataBucket0 = new TableBucket(dataTableId, partitionId, 0);
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket0);

        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);

        // Write initial records
        int initialRecordCount = 20;
        writeRecords(
                gateway,
                dataTableId,
                partitionId,
                0,
                keyType,
                rowType,
                partitionValue,
                0,
                initialRecordCount);

        // Wait for HW to advance
        Replica dataReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataBucket0);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(dataReplica.getLogHighWatermark())
                                .isGreaterThanOrEqualTo(initialRecordCount));

        // Wait for initial index replication to complete
        retry(
                Duration.ofMinutes(2),
                () -> {
                    long totalIndexRecords = countTotalIndexRecords(indexTableId);
                    assertThat(totalIndexRecords)
                            .as("Initial index replication should complete")
                            .isGreaterThanOrEqualTo(initialRecordCount);
                });
        long initialIndexCount = countTotalIndexRecords(indexTableId);

        // Step 3: Remove DataBucket partition metadata from ALL servers' caches.
        for (int serverId = 0; serverId < NUM_TABLET_SERVERS; serverId++) {
            TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            TabletServerMetadataCache cache = tabletServer.getMetadataCache();

            // Verify metadata exists before removal
            Optional<Integer> leaderBefore = cache.getBucketLeaderId(dataBucket0);
            assertThat(leaderBefore).as("Metadata should exist before removal").isPresent();

            // Remove the partition metadata
            cache.removePartitionBucketMetadata(partitionId);

            // Verify metadata is gone
            Optional<Integer> leaderAfter = cache.getBucketLeaderId(dataBucket0);
            assertThat(leaderAfter).as("Metadata should be gone after removal").isEmpty();
        }

        // Step 4: Write more data
        int additionalRecordCount = 30;
        writeRecords(
                gateway,
                dataTableId,
                partitionId,
                0,
                keyType,
                rowType,
                partitionValue,
                initialRecordCount,
                additionalRecordCount);

        // Wait for HW to advance for additional data
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(dataReplica.getLogHighWatermark())
                                .isGreaterThanOrEqualTo(
                                        initialRecordCount + additionalRecordCount));

        // Step 5: Verify index replication recovers and processes ALL data.
        // Without the fix, this will timeout because handleRunning() marks FAILED on cache miss
        // and the dead loop prevents index replication from making progress.
        int totalExpectedRecords = initialRecordCount + additionalRecordCount;

        retry(
                Duration.ofMinutes(2),
                () -> {
                    long totalIndexRecords = countTotalIndexRecords(indexTableId);
                    assertThat(totalIndexRecords)
                            .as(
                                    "Index replication should recover and process all %d records",
                                    totalExpectedRecords)
                            .isGreaterThanOrEqualTo(totalExpectedRecords);
                });
    }

    private void writeRecords(
            TabletServerGateway gateway,
            long tableId,
            long partitionId,
            int bucketId,
            RowType keyType,
            RowType rowType,
            String partitionValue,
            int startId,
            int count)
            throws Exception {
        List<Tuple2<Object[], Object[]>> records = new ArrayList<>();
        for (int i = startId; i < startId + count; i++) {
            Object[] key = new Object[] {i, partitionValue};
            Object[] value = new Object[] {i, "name_" + i, partitionValue};
            records.add(Tuple2.of(key, value));
        }
        KvRecordBatch kvRecords = genKvRecordBatch(keyType, rowType, records);
        PutKvResponse response =
                putKvToPartition(gateway, tableId, partitionId, bucketId, kvRecords);
        assertThat(response.getBucketsRespAt(0).hasErrorCode())
                .as("Write should succeed")
                .isFalse();
    }

    private long countTotalIndexRecords(long indexTableId) throws Exception {
        long total = 0;
        for (int bucketId = 0; bucketId < BUCKET_COUNT; bucketId++) {
            TableBucket indexBucket = new TableBucket(indexTableId, bucketId);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader).getReplicaManager();
            Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
            List<byte[]> kvData = indexReplica.getKvTablet().limitScan(Integer.MAX_VALUE);
            total += kvData.size();
        }
        return total;
    }
}
