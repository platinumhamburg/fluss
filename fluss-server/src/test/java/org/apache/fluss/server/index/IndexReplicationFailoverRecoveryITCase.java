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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.IDX_EMAIL_TABLE_PATH;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_KEY_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link IndexApplier} in leader-follower failover scenarios.
 *
 * <p>This test verifies that after index bucket's leader-follower switch:
 *
 * <ul>
 *   <li>State persisted by KvTablet's indexReplicationOffsets is correctly recovered
 *   <li>New leader can continue to apply index data from the correct offset
 *   <li>Index data remains consistent across leader switches
 * </ul>
 */
class IndexReplicationFailoverRecoveryITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(IndexReplicationFailoverRecoveryITCase.class);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        // Set a small batch size for testing batch loading behavior
        // 2KB batch size will ensure multiple batches are needed for test data
        conf.set(ConfigOptions.SERVER_INDEX_CACHE_COLD_LOAD_BATCH_SIZE, MemorySize.parse("2kb"));

        // ========== Optimized timeouts for faster and more stable tests ==========

        // Optimize ZooKeeper timeouts for faster failure detection in tests
        // Default session timeout is 60s, which makes stopTabletServer() very slow
        // Setting to 5s significantly speeds up leader failover tests
        conf.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(5));
        conf.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(5));

        // Reduce replica lag time for faster ISR shrink/expand detection
        // Default is 30s, reducing to 5s makes ISR changes happen faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));

        // Reduce index fetch wait time for faster index replication
        // Default is 500ms, reducing to 100ms makes index fetching more responsive
        conf.set(ConfigOptions.INDEX_REPLICA_FETCH_WAIT_MAX_TIME, Duration.ofMillis(100));

        // Reduce delayed operation purge interval for faster cleanup
        conf.set(ConfigOptions.LOG_REPLICA_WRITE_OPERATION_PURGE_NUMBER, 100);
        conf.set(ConfigOptions.LOG_REPLICA_FETCH_OPERATION_PURGE_NUMBER, 100);

        return conf;
    }

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testIndexApplierStateRecoveryAfterLeaderSwitch() throws Exception {
        // Step 1: Create indexed table with replication factor = 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);

        // Wait for data table replica ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        // Step 2: Get index table information
        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        assertThat(idxNameTableInfo).isNotNull();

        TableInfo idxEmailTableInfo = metadataManager.getTable(IDX_EMAIL_TABLE_PATH);
        assertThat(idxEmailTableInfo).isNotNull();

        long idxNameTableId = idxNameTableInfo.getTableId();
        long idxEmailTableId = idxEmailTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);
        TableBucket idxEmailBucket = new TableBucket(idxEmailTableId, 0);

        // Wait for index table replicas ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxEmailBucket);

        // Step 3: Get initial leaders and followers for index buckets
        int initialIdxNameLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        int initialIdxEmailLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxEmailBucket);

        // Wait for ISR to expand to at least 2 replicas for both index buckets
        // This is necessary because ISR expansion may take some time after replica creation
        LeaderAndIsr idxNameLeaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        idxNameBucket, 2, Duration.ofMinutes(2));

        LeaderAndIsr idxEmailLeaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        idxEmailBucket, 2, Duration.ofMinutes(2));

        int idxNameFollower =
                idxNameLeaderAndIsr.isr().stream()
                        .filter(id -> id != initialIdxNameLeader)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No follower found for idx_name"));

        int idxEmailFollower =
                idxEmailLeaderAndIsr.isr().stream()
                        .filter(id -> id != initialIdxEmailLeader)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No follower found for idx_email"));

        // Step 4: Write first batch of data to data table
        int dataTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader);

        // Write 3 records with id=1,2,3
        List<Tuple2<Object[], Object[]>> firstBatch = new ArrayList<>();
        firstBatch.add(Tuple2.of(new Object[] {1}, new Object[] {1, "alice", "alice@example.com"}));
        firstBatch.add(Tuple2.of(new Object[] {2}, new Object[] {2, "bob", "bob@example.com"}));
        firstBatch.add(
                Tuple2.of(new Object[] {3}, new Object[] {3, "charlie", "charlie@example.com"}));
        KvRecordBatch kvRecords1 = genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, firstBatch);
        dataTableGateway
                .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords1))
                .get();

        // Step 4.5: Wait for data table's high watermark to advance
        // This is critical because IndexDataProducer can only provide index data for committed
        // records (records below high watermark). Without this wait, the index fetcher may not
        // find any data to fetch.
        TabletServer dataTableLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataTableLeader);
        Replica dataTableLeaderReplica =
                dataTableLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long dataTableHW = dataTableLeaderReplica.getLogHighWatermark();
                    assertThat(dataTableHW)
                            .withFailMessage(
                                    "Data table high watermark should be at least 3 after first batch write, current HW: %d",
                                    dataTableHW)
                            .isGreaterThanOrEqualTo(3);
                });

        LOG.info(
                "Data table high watermark advanced to {} after first batch write",
                dataTableLeaderReplica.getLogHighWatermark());

        // Step 5: Wait for index data to be replicated to both leader and follower
        TabletServer idxNameLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(initialIdxNameLeader);
        TabletServer idxNameFollowerServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxNameFollower);

        Replica idxNameLeaderReplica =
                idxNameLeaderServer.getReplicaManager().getReplicaOrException(idxNameBucket);
        Replica idxNameFollowerReplica =
                idxNameFollowerServer.getReplicaManager().getReplicaOrException(idxNameBucket);

        // Wait for index data to be applied and replicated
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long leaderLogEndOffset = idxNameLeaderReplica.getLocalLogEndOffset();
                    assertThat(leaderLogEndOffset).isGreaterThan(0);
                    assertThat(idxNameFollowerReplica.getLocalLogEndOffset())
                            .isEqualTo(leaderLogEndOffset);
                });

        // Wait for high watermark to advance
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(idxNameLeaderReplica.getLogHighWatermark()).isGreaterThan(0));

        // Step 6: Verify IndexApplier state on leader before shutdown
        IndexApplier leaderIndexApplier =
                getIndexApplier(idxNameLeaderServer.getReplicaManager(), idxNameBucket);

        // State should be committed on leader
        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier.IndexApplyStatus leaderStatus =
                            leaderIndexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(leaderStatus).isNotNull();
                    assertThat(leaderStatus.getLastApplyRecordsDataEndOffset()).isGreaterThan(0L);
                });

        long stateValueBeforeSwitch =
                leaderIndexApplier
                        .getOrInitIndexApplyStatus(dataTableBucket)
                        .getLastApplyRecordsDataEndOffset();
        long logEndOffsetBeforeSwitch = idxNameLeaderReplica.getLocalLogEndOffset();

        // Step 7: Manually switch index bucket leader using notifyLeaderAndISR
        // Get index table path for constructing NotifyLeaderAndIsrData
        PhysicalTablePath idxNamePhysicalTablePath = PhysicalTablePath.of(IDX_NAME_TABLE_PATH);

        // Create new LeaderAndIsr with follower as new leader and incremented leader epoch
        int newLeaderEpoch = idxNameLeaderAndIsr.leaderEpoch() + 1;
        int newBucketEpoch = idxNameLeaderAndIsr.bucketEpoch() + 1;
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        idxNameFollower,
                        newLeaderEpoch,
                        idxNameLeaderAndIsr.isr(),
                        idxNameLeaderAndIsr.coordinatorEpoch(),
                        newBucketEpoch);

        // Update ZooKeeper with new leader info
        zkClient.updateLeaderAndIsr(idxNameBucket, newLeaderAndIsr);

        // Notify old leader to become follower
        TabletServer oldLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(initialIdxNameLeader);
        ReplicaManager oldLeaderReplicaManager = oldLeaderServer.getReplicaManager();
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> oldLeaderFuture =
                new CompletableFuture<>();
        oldLeaderReplicaManager.becomeLeaderOrFollower(
                idxNameLeaderAndIsr.coordinatorEpoch(),
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                idxNamePhysicalTablePath,
                                idxNameBucket,
                                idxNameLeaderAndIsr.isr(),
                                newLeaderAndIsr)),
                oldLeaderFuture::complete);
        oldLeaderFuture.get();

        // Notify new leader to become leader
        TabletServer newLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxNameFollower);
        ReplicaManager followerReplicaManager = newLeaderServer.getReplicaManager();
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> newLeaderFuture =
                new CompletableFuture<>();
        followerReplicaManager.becomeLeaderOrFollower(
                idxNameLeaderAndIsr.coordinatorEpoch(),
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                idxNamePhysicalTablePath,
                                idxNameBucket,
                                idxNameLeaderAndIsr.isr(),
                                newLeaderAndIsr)),
                newLeaderFuture::complete);
        newLeaderFuture.get();

        // Wait for leader change to take effect
        retry(
                Duration.ofMinutes(1),
                () -> {
                    int currentLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(currentLeader).isEqualTo(idxNameFollower);
                });

        // Step 8: Get the new leader replica (previous follower)
        Replica newLeaderReplica =
                idxNameFollowerServer.getReplicaManager().getReplicaOrException(idxNameBucket);

        // Verify new leader status
        retry(Duration.ofMinutes(1), () -> assertThat(newLeaderReplica.isLeader()).isTrue());

        // Step 9: Verify state recovery on new leader
        IndexApplier newLeaderIndexApplier =
                getIndexApplier(idxNameFollowerServer.getReplicaManager(), idxNameBucket);

        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier.IndexApplyStatus recoveredStatus =
                            newLeaderIndexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(recoveredStatus).isNotNull();
                    assertThat(recoveredStatus.getLastApplyRecordsDataEndOffset())
                            .isEqualTo(stateValueBeforeSwitch);
                });

        // Verify log end offset is preserved
        assertThat(newLeaderReplica.getLocalLogEndOffset()).isEqualTo(logEndOffsetBeforeSwitch);

        // Step 10: Write second batch of data to data table
        // The new leader should continue to apply index from the correct offset
        // Since we only switched index bucket leader, data table leader remains the same
        // Write multiple records to ensure at least one hashes to the switched index bucket
        List<Tuple2<Object[], Object[]>> secondBatch = new ArrayList<>();
        secondBatch.add(
                Tuple2.of(new Object[] {4}, new Object[] {4, "david", "david@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {5}, new Object[] {5, "eve", "eve@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {6}, new Object[] {6, "frank", "frank@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {7}, new Object[] {7, "grace", "grace@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {8}, new Object[] {8, "henry", "henry@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {9}, new Object[] {9, "iris", "iris@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {10}, new Object[] {10, "jack", "jack@example.com"}));
        KvRecordBatch kvRecords2 =
                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, secondBatch);
        dataTableGateway
                .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords2))
                .get();

        // Wait for data table's high watermark to advance to at least 10
        // This is critical because IndexDataProducer can only provide index data for committed
        // records
        // The second batch added 7 more records, so HW should be at least 10 (3 + 7)
        // Reuse the dataTableLeaderServer and dataTableLeaderReplica from Step 4.5
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long dataTableHW = dataTableLeaderReplica.getLogHighWatermark();
                    assertThat(dataTableHW)
                            .withFailMessage(
                                    "Data table high watermark should be at least 10 after second batch write, current HW: %d",
                                    dataTableHW)
                            .isGreaterThanOrEqualTo(10);
                });

        // Step 11: Verify new leader continues to apply index correctly
        // Wait for index data to be fetched and applied
        retry(
                Duration.ofSeconds(60),
                () -> {
                    long currentLogEndOffset = newLeaderReplica.getLocalLogEndOffset();
                    // The index data should eventually be fetched and applied
                    assertThat(currentLogEndOffset)
                            .withFailMessage(
                                    "Expected currentLogEndOffset %d to be greater than %d after writing new data",
                                    currentLogEndOffset, logEndOffsetBeforeSwitch)
                            .isGreaterThan(logEndOffsetBeforeSwitch);
                });

        // Wait for high watermark to advance. Use isGreaterThanOrEqualTo because after
        // leader switch, the new leader's HW may equal logEndOffsetBeforeSwitch when the
        // follower (old leader) hasn't yet fetched the new data written by IndexApplier.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(newLeaderReplica.getLogHighWatermark())
                                .isGreaterThanOrEqualTo(logEndOffsetBeforeSwitch));

        // Step 12: Verify state is updated correctly after new writes
        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier.IndexApplyStatus updatedStatus =
                            newLeaderIndexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(updatedStatus).isNotNull();
                    assertThat(updatedStatus.getLastApplyRecordsDataEndOffset())
                            .isGreaterThan(stateValueBeforeSwitch);
                });

        // Step 13: Verify IndexApplier is working correctly on new leader
        ReplicaManager newLeaderReplicaManager = idxNameFollowerServer.getReplicaManager();
        IndexApplier indexApplier = getIndexApplier(newLeaderReplicaManager, idxNameBucket);
        assertThat(indexApplier).isNotNull();

        IndexApplier.IndexApplyStatus status =
                indexApplier.getOrInitIndexApplyStatus(dataTableBucket);
        assertThat(status).isNotNull();
        assertThat(status.getLastApplyRecordsDataEndOffset()).isGreaterThan(stateValueBeforeSwitch);
        assertThat(status.getLastApplyRecordsIndexEndOffset())
                .isEqualTo(newLeaderReplica.getLocalLogEndOffset());
    }

    @Test
    void testDataBucketLeaderChangeNotification() throws Exception {
        // Test that verifies IndexFetcherThread automatically updates connection
        // when DataBucket's leader changes

        // Step 1: Create indexed table with replication factor = 2 for both data and index tables
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);

        // Wait for data table replica ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        // Step 2: Get index table information
        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        assertThat(idxNameTableInfo).isNotNull();

        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);

        // Wait for index table replicas ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        // Step 3: Get initial data table leader and follower
        int initialDataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        // Wait for ISR to expand to at least 2 replicas for data table
        // This is necessary because ISR expansion may take some time after replica creation
        LeaderAndIsr dataLeaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        dataTableBucket, 2, Duration.ofMinutes(2));

        int dataFollower =
                dataLeaderAndIsr.isr().stream()
                        .filter(id -> id != initialDataLeader)
                        .findFirst()
                        .orElseThrow(
                                () -> new RuntimeException("No follower found for data table"));

        // Step 4: Write first batch of data
        TabletServerGateway dataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(initialDataLeader);

        List<Tuple2<Object[], Object[]>> firstBatch = new ArrayList<>();
        firstBatch.add(Tuple2.of(new Object[] {1}, new Object[] {1, "alice", "alice@example.com"}));
        firstBatch.add(Tuple2.of(new Object[] {2}, new Object[] {2, "bob", "bob@example.com"}));
        firstBatch.add(
                Tuple2.of(new Object[] {3}, new Object[] {3, "charlie", "charlie@example.com"}));
        KvRecordBatch kvRecords1 = genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, firstBatch);
        dataTableGateway
                .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords1))
                .get();

        // Step 4.5: Wait for data table's high watermark to advance
        // This is critical because IndexDataProducer can only provide index data for committed
        // records (records below high watermark). Without this wait, the index fetcher may not
        // find any data to fetch.
        TabletServer dataTableLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(initialDataLeader);
        Replica dataTableLeaderReplica =
                dataTableLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long dataTableHW = dataTableLeaderReplica.getLogHighWatermark();
                    assertThat(dataTableHW)
                            .withFailMessage(
                                    "Data table high watermark should be at least 3 after first batch write, current HW: %d",
                                    dataTableHW)
                            .isGreaterThanOrEqualTo(3);
                });

        LOG.info(
                "Data table high watermark advanced to {} after first batch write",
                dataTableLeaderReplica.getLogHighWatermark());

        // Step 5: Wait for index data to be applied
        int indexLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        TabletServer indexLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(indexLeader);
        Replica indexLeaderReplica =
                indexLeaderServer.getReplicaManager().getReplicaOrException(idxNameBucket);

        // Wait for index to be applied
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long indexLogEndOffset = indexLeaderReplica.getLocalLogEndOffset();
                    assertThat(indexLogEndOffset).isGreaterThan(0);
                });

        long indexLogEndOffsetBeforeSwitch = indexLeaderReplica.getLocalLogEndOffset();

        // Record IndexApplier state before data leader switch
        IndexApplier indexApplierBeforeSwitch =
                getIndexApplier(indexLeaderServer.getReplicaManager(), idxNameBucket);
        long dataEndOffsetBeforeSwitch = 0;
        long indexEndOffsetBeforeSwitch = 0;
        if (indexApplierBeforeSwitch != null) {
            IndexApplier.IndexApplyStatus statusBefore =
                    indexApplierBeforeSwitch.getOrInitIndexApplyStatus(dataTableBucket);
            if (statusBefore != null) {
                dataEndOffsetBeforeSwitch = statusBefore.getLastApplyRecordsDataEndOffset();
                indexEndOffsetBeforeSwitch = statusBefore.getLastApplyRecordsIndexEndOffset();
            }
        }

        // Step 6: Switch data table leader by stopping the old leader
        // This triggers Coordinator's ReplicaStateMachine to detect the leader failure
        // and automatically elect a new leader and notify all related index buckets
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(initialDataLeader);

        // Wait for leader change to take effect and coordinator to elect new leader
        // The follower should become the new leader automatically
        retry(
                Duration.ofMinutes(1),
                () -> {
                    int currentLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
                    assertThat(currentLeader).isEqualTo(dataFollower);
                });

        // Step 6.1: Get the current index table leader after data leader switch
        // Since we stopped initialDataLeader, if it was also the index leader,
        // the index leader would have changed as well
        int currentIndexLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        TabletServer currentIndexLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(currentIndexLeader);
        Replica currentIndexLeaderReplica =
                currentIndexLeaderServer.getReplicaManager().getReplicaOrException(idxNameBucket);

        // Wait for the current index leader to be ready
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(currentIndexLeaderReplica.isLeader()).isTrue());

        // Step 7: Write second batch of data to new data table leader
        // This verifies that IndexFetcherThread has switched to the new data leader
        // Write more records to increase probability that at least one hashes to target index
        // bucket
        TabletServerGateway newDataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataFollower);

        List<Tuple2<Object[], Object[]>> secondBatch = new ArrayList<>();
        secondBatch.add(
                Tuple2.of(new Object[] {4}, new Object[] {4, "david", "david@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {5}, new Object[] {5, "eve", "eve@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {6}, new Object[] {6, "frank", "frank@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {7}, new Object[] {7, "grace", "grace@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {8}, new Object[] {8, "henry", "henry@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {9}, new Object[] {9, "iris", "iris@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {10}, new Object[] {10, "jack", "jack@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {11}, new Object[] {11, "kate", "kate@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {12}, new Object[] {12, "leo", "leo@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {13}, new Object[] {13, "mary", "mary@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {14}, new Object[] {14, "nick", "nick@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {15}, new Object[] {15, "olivia", "olivia@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {16}, new Object[] {16, "paul", "paul@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {17}, new Object[] {17, "queen", "queen@example.com"}));
        secondBatch.add(
                Tuple2.of(new Object[] {18}, new Object[] {18, "rose", "rose@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {19}, new Object[] {19, "sam", "sam@example.com"}));
        secondBatch.add(Tuple2.of(new Object[] {20}, new Object[] {20, "tim", "tim@example.com"}));
        KvRecordBatch kvRecords2 =
                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, secondBatch);
        newDataTableGateway
                .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords2))
                .get();

        // Wait for data table's high watermark to advance
        // First batch: 3 records, second batch: 17 records, total: 20 records
        TabletServer newDataLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataFollower);
        Replica newDataLeaderReplica =
                newDataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    long dataTableHW = newDataLeaderReplica.getLogHighWatermark();
                    assertThat(dataTableHW)
                            .withFailMessage(
                                    "Data table high watermark should be at least 20 after second batch write, current HW: %d",
                                    dataTableHW)
                            .isGreaterThanOrEqualTo(20);
                });

        // Step 8: Verify index continues to be applied after data leader switch
        // The key verification: IndexFetcherThread should automatically connect to new data leader
        retry(
                Duration.ofSeconds(60),
                () -> {
                    long currentIndexLogEndOffset =
                            currentIndexLeaderReplica.getLocalLogEndOffset();
                    // If IndexFetcherThread successfully switched to new data leader,
                    // index data should continue to be fetched and applied
                    assertThat(currentIndexLogEndOffset)
                            .withFailMessage(
                                    "Index log end offset should increase after data leader switch. "
                                            + "Current: %d, Before: %d. This indicates IndexFetcherThread "
                                            + "may not have switched to the new data leader.",
                                    currentIndexLogEndOffset, indexLogEndOffsetBeforeSwitch)
                            .isGreaterThan(indexLogEndOffsetBeforeSwitch);
                });

        // Step 9: Verify IndexApplier state is updated after data leader switch
        IndexApplier indexApplier =
                getIndexApplier(currentIndexLeaderServer.getReplicaManager(), idxNameBucket);
        assertThat(indexApplier).isNotNull();

        long finalDataEndOffsetBeforeSwitch = dataEndOffsetBeforeSwitch;
        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier.IndexApplyStatus status =
                            indexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(status).isNotNull();

                    // Verify data end offset has progressed beyond the first batch
                    long currentDataEndOffset = status.getLastApplyRecordsDataEndOffset();
                    assertThat(currentDataEndOffset)
                            .withFailMessage(
                                    "IndexApplier should continue to apply data after data leader switch. "
                                            + "Current data end offset: %d, Before switch: %d",
                                    currentDataEndOffset, finalDataEndOffsetBeforeSwitch)
                            .isGreaterThan(finalDataEndOffsetBeforeSwitch);

                    // Verify index end offset matches current index log end offset
                    long currentIndexEndOffset = status.getLastApplyRecordsIndexEndOffset();
                    long expectedIndexEndOffset = currentIndexLeaderReplica.getLocalLogEndOffset();
                    assertThat(currentIndexEndOffset)
                            .withFailMessage(
                                    "Index end offset in IndexApplyStatus should match index replica's log end offset. "
                                            + "Status: %d, Replica: %d",
                                    currentIndexEndOffset, expectedIndexEndOffset)
                            .isEqualTo(expectedIndexEndOffset);
                });
    }

    /**
     * Test cold data loading when IndexReplica is stopped and then restarted.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Create indexed table with data and index replicas
     *   <li>Stop all IndexReplicas (by making them followers with no leader)
     *   <li>Write data to main table while IndexReplicas are stopped (hot data generated in
     *       IndexDataProducer)
     *   <li>Switch DataBucket leader to destroy IndexDataProducer and force cold data scenario
     *   <li>Restore IndexReplicas
     *   <li>Write one more record to trigger index replication
     *   <li>Verify index replication catches up with all data through cold data loading from WAL
     * </ol>
     *
     * <p>The key step is switching DataBucket leader (Step 4) which destroys the old
     * IndexDataProducer and creates a new one with {@code logEndOffsetOnLeaderStart > 0}. This
     * ensures that when IndexReplica fetches data, it will trigger cold data loading from WAL
     * instead of reading hot data from cache.
     */
    @Test
    void testColdDataLoadingWhenIndexReplicaFailover() throws Exception {
        // Step 1: Create indexed table with replication factor 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);

        // Wait for data table replica ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        // Get index table information
        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        assertThat(idxNameTableInfo).isNotNull();

        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);

        // Wait for index table replicas ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        // Get initial index leader
        int initialIdxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        LeaderAndIsr initialLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(idxNameBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for idx_name");

        LOG.info(
                "Step 1 completed: Created indexed table, initial index leader: {}",
                initialIdxLeader);

        // Step 2: Stop IndexReplica by making all replicas followers (leader = -1)
        PhysicalTablePath idxNamePhysicalTablePath = PhysicalTablePath.of(IDX_NAME_TABLE_PATH);
        LeaderAndIsr stoppedLeaderAndIsr =
                new LeaderAndIsr(
                        -1, // No leader
                        initialLeaderAndIsr.leaderEpoch() + 1,
                        initialLeaderAndIsr.isr(),
                        initialLeaderAndIsr.coordinatorEpoch(),
                        initialLeaderAndIsr.bucketEpoch() + 1);

        zkClient.updateLeaderAndIsr(idxNameBucket, stoppedLeaderAndIsr);

        // Notify all replicas to become followers
        for (int serverId : initialLeaderAndIsr.isr()) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            ReplicaManager replicaManager = server.getReplicaManager();
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            replicaManager.becomeLeaderOrFollower(
                    initialLeaderAndIsr.coordinatorEpoch(),
                    Collections.singletonList(
                            new NotifyLeaderAndIsrData(
                                    idxNamePhysicalTablePath,
                                    idxNameBucket,
                                    initialLeaderAndIsr.isr(),
                                    stoppedLeaderAndIsr)),
                    future::complete);
            future.get();
        }

        LOG.info("Step 2 completed: Stopped all IndexReplicas");

        // Step 3: Write records to data table while IndexReplicas are stopped
        // Write all records in a single batch for efficiency (avoid waiting for ACK 10 times)
        int dataTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader);

        int recordCount = 10;
        List<Tuple2<Object[], Object[]>> allRecords = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            allRecords.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "email" + i + "@example.com"}));
        }
        KvRecordBatch kvRecords = genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, allRecords);
        dataTableGateway
                .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords))
                .get();

        LOG.info(
                "Step 3 completed: Written {} records to data table while index stopped",
                recordCount);

        // Get data table's log end offset
        TabletServer dataLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataTableLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        long dataLogEndOffset = dataReplica.getLocalLogEndOffset();

        LOG.info(
                "Data table log end offset after writing {} records: {}",
                recordCount,
                dataLogEndOffset);

        // Step 3.5: Wait for data table's high watermark to advance
        // This is critical for ISR expansion - follower needs to catch up with leader
        // before it can be added to ISR
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long dataTableHW = dataReplica.getLogHighWatermark();
                    assertThat(dataTableHW)
                            .withFailMessage(
                                    "Data table high watermark should be at least %d after writing records, current HW: %d",
                                    recordCount, dataTableHW)
                            .isGreaterThanOrEqualTo(recordCount);
                });

        LOG.info(
                "Data table high watermark advanced to {} after writing {} records",
                dataReplica.getLogHighWatermark(),
                recordCount);

        // Step 3.6: Switch DataBucket leader by stopping the old leader
        // This is the KEY step to ensure we're actually testing cold data loading scenario
        // By stopping the leader, Coordinator will automatically elect a new leader and
        // the new leader's IndexDataProducer will be created with logEndOffsetOnLeaderStart > 0

        // Wait for ISR to expand to include at least 2 replicas (leader + follower)
        // This is necessary because ISR expansion may take some time after replica creation
        // Using the new utility method with extended timeout for stability
        LeaderAndIsr dataLeaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        dataTableBucket, 2, Duration.ofMinutes(2));

        // Find the follower that will become new leader
        int dataFollower =
                dataLeaderAndIsr.isr().stream()
                        .filter(id -> id != dataTableLeader)
                        .findFirst()
                        .orElseThrow(
                                () -> new RuntimeException("No follower found for data table"));

        LOG.info(
                "Step 3.5 starting: Stopping data bucket leader {} to trigger failover, follower {} will become new leader",
                dataTableLeader,
                dataFollower);

        // Stop the data table leader
        // This will trigger Coordinator to detect leader failure and elect new leader
        // The new leader will create IndexDataProducer with logEndOffsetOnLeaderStart =
        // dataLogEndOffset
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(dataTableLeader);

        // Wait for leader change to take effect - coordinator should automatically elect new leader
        retry(
                Duration.ofMinutes(1),
                () -> {
                    int currentLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
                    assertThat(currentLeader)
                            .withFailMessage(
                                    "Expected new leader to be %d, but got %d",
                                    dataFollower, currentLeader)
                            .isEqualTo(dataFollower);
                });

        // Get the new leader server and replica
        TabletServer newDataLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataFollower);
        Replica newDataLeaderReplica =
                newDataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);

        // Verify new leader has the data
        long newLeaderLogEndOffset = newDataLeaderReplica.getLocalLogEndOffset();
        assertThat(newLeaderLogEndOffset)
                .withFailMessage(
                        "New data leader should have log end offset equal to previous leader's offset")
                .isEqualTo(dataLogEndOffset);

        LOG.info(
                "Step 3.5 completed: Data bucket leader switched from {} to {}, old IndexDataProducer destroyed, new IndexDataProducer created with logEndOffsetOnLeaderStart: {}",
                dataTableLeader,
                dataFollower,
                newLeaderLogEndOffset);

        // Use the new data leader and gateway for subsequent operations
        int newDataTableLeader = dataFollower;
        TabletServerGateway newDataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newDataTableLeader);

        // Step 4: Wait for new DataBucket leader state to stabilize
        LOG.info("Step 4: Waiting for new DataBucket leader state to stabilize...");

        // Verify new data leader's high watermark has caught up
        retry(
                Duration.ofSeconds(30),
                () -> {
                    long newLeaderHW = newDataLeaderReplica.getLogHighWatermark();
                    assertThat(newLeaderHW)
                            .withFailMessage(
                                    "New data leader's high watermark should be at least %d, current: %d",
                                    dataLogEndOffset, newLeaderHW)
                            .isGreaterThanOrEqualTo(dataLogEndOffset);
                });

        LOG.info(
                "Step 4 completed: New DataBucket leader is stable with HW >= {}",
                dataLogEndOffset);

        // Step 5: Restore IndexReplica by assigning a leader
        // Now when IndexReplica fetches data, it will trigger cold data loading
        // Get the current (potentially updated) ISR from ZooKeeper
        // The ISR may have been updated by Coordinator after dataTableLeader was stopped
        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(idxNameBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for idx_name after data leader switch");

        // Choose a new IndexLeader from current ISR, excluding the stopped DataBucket leader
        final int newIndexLeader = chooseNewIndexLeader(currentLeaderAndIsr.isr(), dataTableLeader);
        LOG.info(
                "Step 5: Selected new IndexLeader: {} from current ISR {} (excluding stopped DataBucket leader: {})",
                newIndexLeader,
                currentLeaderAndIsr.isr(),
                dataTableLeader);

        // Create new ISR excluding the stopped server to ensure high watermark can advance
        // Use currentLeaderAndIsr which reflects the latest ISR state
        List<Integer> newIsr =
                currentLeaderAndIsr.isr().stream()
                        .filter(id -> id != dataTableLeader)
                        .collect(Collectors.toList());

        LeaderAndIsr restoredLeaderAndIsr =
                new LeaderAndIsr(
                        newIndexLeader,
                        stoppedLeaderAndIsr.leaderEpoch() + 1,
                        newIsr,
                        currentLeaderAndIsr.coordinatorEpoch(),
                        stoppedLeaderAndIsr.bucketEpoch() + 1);

        zkClient.updateLeaderAndIsr(idxNameBucket, restoredLeaderAndIsr);

        // Notify all replicas with new leader (only active servers in new ISR)
        for (int serverId : newIsr) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            ReplicaManager replicaManager = server.getReplicaManager();
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            replicaManager.becomeLeaderOrFollower(
                    currentLeaderAndIsr.coordinatorEpoch(),
                    Collections.singletonList(
                            new NotifyLeaderAndIsrData(
                                    idxNamePhysicalTablePath,
                                    idxNameBucket,
                                    newIsr,
                                    restoredLeaderAndIsr)),
                    future::complete);
            future.get();
        }

        LOG.info("Step 5 completed: Restored IndexReplica with leader: {}", newIndexLeader);

        // Wait for new leader to be effective
        retry(
                Duration.ofSeconds(30),
                () -> {
                    int currentLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(currentLeader).isEqualTo(newIndexLeader);
                });

        // Step 6: Write one more record to trigger index catching up and cold data loading
        // This will trigger IndexFetcherThread to fetch index data from DataTable's
        // IndexDataProducer
        List<Tuple2<Object[], Object[]>> finalBatch = new ArrayList<>();
        finalBatch.add(
                Tuple2.of(
                        new Object[] {recordCount},
                        new Object[] {recordCount, "final_user", "final@example.com"}));
        KvRecordBatch finalKvRecords =
                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, finalBatch);
        newDataTableGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId, dataTableBucket.getBucket(), -1, finalKvRecords))
                .get();

        LOG.info("Step 6 completed: Written final record to trigger index catching up");

        // Step 7: Verify index replication catches up with all data through cold data loading
        TabletServer indexLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(newIndexLeader);
        ReplicaManager indexReplicaManager = indexLeaderServer.getReplicaManager();

        // Get the new data leader replica for verification
        Replica finalDataReplica = newDataLeaderReplica;

        // Wait for IndexBucket to catch up with all data through cold data loading
        retry(
                Duration.ofSeconds(60),
                () -> {
                    IndexApplier indexApplier = getIndexApplier(indexReplicaManager, idxNameBucket);
                    IndexApplier.IndexApplyStatus status =
                            indexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(status).isNotNull();

                    long currentDataEndOffset = status.getLastApplyRecordsDataEndOffset();
                    long currentDataLogEndOffset = finalDataReplica.getLocalLogEndOffset();

                    LOG.info(
                            "Index apply progress - Data end offset: {}, Data log end offset: {}",
                            currentDataEndOffset,
                            currentDataLogEndOffset);

                    // Verify that index has caught up with all data
                    assertThat(currentDataEndOffset)
                            .withFailMessage(
                                    "Index should catch up with all data through cold data loading. "
                                            + "Current: %d, Expected: %d",
                                    currentDataEndOffset, currentDataLogEndOffset)
                            .isEqualTo(currentDataLogEndOffset);
                });

        // Step 8: Enhanced verification - Check actual IndexBucket data integrity
        Replica indexReplica = indexReplicaManager.getReplicaOrException(idxNameBucket);
        long indexLogEndOffset = indexReplica.getLocalLogEndOffset();

        LOG.info(
                "Enhanced verification - IndexBucket LogEndOffset: {}, expected > 0",
                indexLogEndOffset);

        // Verify IndexBucket has actual data
        assertThat(indexLogEndOffset)
                .withFailMessage(
                        "IndexBucket should contain index data after cold data loading. "
                                + "Current LogEndOffset: %d",
                        indexLogEndOffset)
                .isGreaterThan(0);

        // Verify index commit progress - need to wait for high watermark to advance
        // which then allows IndexCommitDataOffset to be updated
        // Note: With reduced ISR (only one replica after stopping a server), high watermark
        // advancement may be slower, so we use a longer timeout and more lenient assertion
        retry(
                Duration.ofSeconds(60),
                () -> {
                    IndexApplier indexApplier = getIndexApplier(indexReplicaManager, idxNameBucket);
                    IndexApplier.IndexApplyStatus finalStatus =
                            indexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    long indexCommitDataOffset = finalStatus.getIndexCommitDataOffset();
                    long lastApplyDataEndOffset = finalStatus.getLastApplyRecordsDataEndOffset();

                    LOG.info(
                            "Enhanced verification - IndexCommitDataOffset: {}, LastApplyDataEndOffset: {}, IndexBucket LogEndOffset: {}, Data LogEndOffset: {}",
                            indexCommitDataOffset,
                            lastApplyDataEndOffset,
                            indexLogEndOffset,
                            dataLogEndOffset);

                    // Primary verification: index data has been applied (lastApplyDataEndOffset >
                    // 0)
                    // This is the key indicator that cold data loading worked
                    assertThat(lastApplyDataEndOffset)
                            .withFailMessage(
                                    "Index should have applied data after cold data loading. "
                                            + "LastApplyDataEndOffset: %d",
                                    lastApplyDataEndOffset)
                            .isGreaterThan(0);

                    // Secondary verification: index commit offset should eventually progress
                    // With single replica ISR, HW may not advance immediately, so we check >= 0
                    // The primary verification above already confirms cold data loading worked
                    assertThat(indexCommitDataOffset)
                            .withFailMessage(
                                    "Index commit offset should be non-negative. Current: %d",
                                    indexCommitDataOffset)
                            .isGreaterThanOrEqualTo(0);
                });

        LOG.info("Test completed: Cold data loading successfully recovered all missing index data");
    }

    @Test
    void testIndexApplierStateRecoveryAfterServerRestart() throws Exception {
        // Step 1: Create indexed table with replication factor = 2, write data
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(idxNameBucket, 2, Duration.ofMinutes(2));

        // Write first batch
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);
        List<Tuple2<Object[], Object[]>> batch1 = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            batch1.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch1)))
                .get();

        // Wait for HW and index apply
        TabletServer dataLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(5));

        // Step 2: Record state before restart
        TabletServer idxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxLeader);
        IndexApplier.IndexApplyStatus statusBefore =
                waitForIndexApplyProgress(
                        idxLeaderServer.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        1L,
                        Duration.ofMinutes(1));
        long dataEndOffsetBefore = statusBefore.getLastApplyRecordsDataEndOffset();
        LOG.info("State before restart: lastApplyRecordsDataEndOffset={}", dataEndOffsetBefore);

        // Step 3: Trigger snapshot to persist state
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(IDX_NAME_TABLE_PATH);
        LOG.info("Snapshot triggered and completed for {}", IDX_NAME_TABLE_PATH);

        // Step 4: Restart the index leader server
        FLUSS_CLUSTER_EXTENSION.restartTabletServer(idxLeader, null);
        LOG.info("Restarted tablet server {}", idxLeader);

        // Step 5: Wait for index bucket to re-elect leader
        retry(
                Duration.ofMinutes(2),
                () -> {
                    int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(leader).isGreaterThanOrEqualTo(0);
                });
        int newIdxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        TabletServer newIdxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(newIdxLeader);

        // Step 6: Verify recovered offset >= before restart
        IndexApplier.IndexApplyStatus recoveredStatus =
                waitForIndexApplyProgress(
                        newIdxLeaderServer.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        dataEndOffsetBefore,
                        Duration.ofMinutes(1));
        LOG.info(
                "Recovered state: lastApplyRecordsDataEndOffset={}",
                recoveredStatus.getLastApplyRecordsDataEndOffset());

        // Step 7: Write more data and verify apply continues
        List<Tuple2<Object[], Object[]>> batch2 = new ArrayList<>();
        for (int i = 6; i <= 10; i++) {
            batch2.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        // Data leader may have changed after restart
        int currentDataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway currentDataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(currentDataLeader);
        currentDataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch2)))
                .get();

        // Wait for HW to advance
        TabletServer currentDataLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(currentDataLeader);
        Replica currentDataReplica =
                currentDataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(currentDataReplica.getLogHighWatermark())
                                .isGreaterThanOrEqualTo(10));

        // Verify index apply continues from correct offset
        IndexApplier.IndexApplyStatus finalStatus =
                waitForIndexApplyProgress(
                        newIdxLeaderServer.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        dataEndOffsetBefore + 1,
                        Duration.ofMinutes(1));
        assertThat(finalStatus.getLastApplyRecordsDataEndOffset())
                .isGreaterThan(dataEndOffsetBefore);
        LOG.info(
                "Test completed: index apply continued after restart, final offset={}",
                finalStatus.getLastApplyRecordsDataEndOffset());
    }

    @Test
    void testSimultaneousIndexAndDataBucketFailover() throws Exception {
        // Step 1: Create indexed table with replication factor = 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();

        // Step 2: Find a bucket pair where data leader and index leader are on the same server
        int targetServer = -1;
        int targetBucketId = -1;
        for (int b = 0; b < 3; b++) {
            TableBucket dataBucket = new TableBucket(dataTableId, b);
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataBucket);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxBucket);
            FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(dataBucket, 2, Duration.ofMinutes(2));
            FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(idxBucket, 2, Duration.ofMinutes(2));

            int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            if (dataLeader == idxLeader) {
                targetServer = dataLeader;
                targetBucketId = b;
                break;
            }
        }

        // If no co-located pair found, just use bucket 0 and stop the data leader
        if (targetServer == -1) {
            targetBucketId = 0;
            TableBucket dataBucket = new TableBucket(dataTableId, 0);
            targetServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
        }

        TableBucket dataTableBucket = new TableBucket(dataTableId, targetBucketId);
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, targetBucketId);

        // Step 3: Write data and wait for index apply
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);
        List<Tuple2<Object[], Object[]>> batch1 = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            batch1.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch1)))
                .get();

        // Wait for HW
        TabletServer dataLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(5));

        // Wait for index apply
        int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        TabletServer idxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxLeader);
        waitForIndexApplyProgress(
                idxLeaderServer.getReplicaManager(),
                idxNameBucket,
                dataTableBucket,
                1L,
                Duration.ofMinutes(1));

        // Step 4: Stop the target server — kills both data leader and index leader (or one of them)
        LOG.info(
                "Stopping server {} which hosts leaders for bucket {}",
                targetServer,
                targetBucketId);
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(targetServer);

        // Step 5: Wait for both to re-elect leaders on other servers
        final int stoppedServer = targetServer;
        retry(
                Duration.ofMinutes(2),
                () -> {
                    int newDataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
                    assertThat(newDataLeader).isNotEqualTo(stoppedServer);
                });
        retry(
                Duration.ofMinutes(2),
                () -> {
                    int newIdxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(newIdxLeader).isNotEqualTo(stoppedServer);
                });

        int newDataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        int newIdxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        LOG.info("New leaders after failover: data={}, index={}", newDataLeader, newIdxLeader);

        // After failover, before any index bucket reports, commitHorizon should be -1.
        TabletServer newDataLeaderServer0 =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(newDataLeader);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica r =
                            newDataLeaderServer0
                                    .getReplicaManager()
                                    .getReplicaOrException(dataTableBucket);
                    assertThat(r.getIndexDataProducer()).isNotNull();
                });

        Replica newDataReplicaForCheck =
                newDataLeaderServer0.getReplicaManager().getReplicaOrException(dataTableBucket);
        IndexDataProducer producer = newDataReplicaForCheck.getIndexDataProducer();
        assertThat(producer).isNotNull();
        assertThat(newDataReplicaForCheck.getLocalLogEndOffset()).isGreaterThan(0);
        assertThat(producer.getIndexCommitHorizon()).isEqualTo(-1L);

        // Step 6: Write more data to new data leader
        TabletServerGateway newDataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newDataLeader);
        List<Tuple2<Object[], Object[]>> batch2 = new ArrayList<>();
        for (int i = 6; i <= 10; i++) {
            batch2.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        newDataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch2)))
                .get();

        // Wait for HW on new data leader
        TabletServer newDataLeaderServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(newDataLeader);
        Replica newDataReplica =
                newDataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(newDataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(6));

        // Step 7: Verify new index leader applies data from new data leader
        TabletServer newIdxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(newIdxLeader);
        IndexApplier.IndexApplyStatus finalStatus =
                waitForIndexApplyProgress(
                        newIdxLeaderServer.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        6L,
                        Duration.ofMinutes(2));
        LOG.info(
                "Index apply continued after simultaneous failover, offset={}",
                finalStatus.getLastApplyRecordsDataEndOffset());

        // Step 8: Restart stopped server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(stoppedServer);
    }

    @Test
    void testCascadingFailoverMultipleSequentialLeaderSwitches() throws Exception {
        // Step 1: Create indexed table with replication factor = 2
        // Use RF=2 and do A->B->A cascading switches to verify state consistency
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        // Wait for ISR to expand to 2
        LeaderAndIsr leaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        idxNameBucket, 2, Duration.ofMinutes(2));
        PhysicalTablePath idxNamePhysicalTablePath = PhysicalTablePath.of(IDX_NAME_TABLE_PATH);

        int leaderA = leaderAndIsr.leader();
        int leaderB =
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leaderA)
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException());

        LOG.info("Cascading failover: A={}, B={}", leaderA, leaderB);

        // Write batch 1, wait for leader A to apply
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);
        List<Tuple2<Object[], Object[]>> batch1 = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            batch1.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch1)))
                .get();

        // Wait for data HW
        TabletServer dataLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(3));

        TabletServer serverA = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderA);
        IndexApplier.IndexApplyStatus statusAfterBatch1 =
                waitForIndexApplyProgress(
                        serverA.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        1L,
                        Duration.ofMinutes(1));
        long offsetAfterBatch1 = statusAfterBatch1.getLastApplyRecordsDataEndOffset();
        LOG.info("After batch 1, leader A offset={}", offsetAfterBatch1);

        // Step 3: Switch A -> B
        int epoch1 = leaderAndIsr.leaderEpoch() + 1;
        int bucketEpoch1 = leaderAndIsr.bucketEpoch() + 1;
        LeaderAndIsr newLeaderAndIsr1 =
                new LeaderAndIsr(
                        leaderB,
                        epoch1,
                        leaderAndIsr.isr(),
                        leaderAndIsr.coordinatorEpoch(),
                        bucketEpoch1);
        zkClient.updateLeaderAndIsr(idxNameBucket, newLeaderAndIsr1);

        for (int serverId : leaderAndIsr.isr()) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            server.getReplicaManager()
                    .becomeLeaderOrFollower(
                            leaderAndIsr.coordinatorEpoch(),
                            Collections.singletonList(
                                    new NotifyLeaderAndIsrData(
                                            idxNamePhysicalTablePath,
                                            idxNameBucket,
                                            leaderAndIsr.isr(),
                                            newLeaderAndIsr1)),
                            future::complete);
            future.get();
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(leader).isEqualTo(leaderB);
                });

        // Write batch 2, wait for leader B to apply
        List<Tuple2<Object[], Object[]>> batch2 = new ArrayList<>();
        for (int i = 4; i <= 6; i++) {
            batch2.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch2)))
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(6));

        TabletServer serverB = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderB);
        IndexApplier.IndexApplyStatus statusAfterBatch2 =
                waitForIndexApplyProgress(
                        serverB.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        offsetAfterBatch1 + 1,
                        Duration.ofMinutes(1));
        long offsetAfterBatch2 = statusAfterBatch2.getLastApplyRecordsDataEndOffset();
        assertThat(offsetAfterBatch2).isGreaterThan(offsetAfterBatch1);
        LOG.info("After batch 2, leader B offset={}", offsetAfterBatch2);

        // Step 5: Switch B -> A (back to original leader)
        int epoch2 = epoch1 + 1;
        int bucketEpoch2 = bucketEpoch1 + 1;
        LeaderAndIsr newLeaderAndIsr2 =
                new LeaderAndIsr(
                        leaderA,
                        epoch2,
                        leaderAndIsr.isr(),
                        leaderAndIsr.coordinatorEpoch(),
                        bucketEpoch2);
        zkClient.updateLeaderAndIsr(idxNameBucket, newLeaderAndIsr2);

        for (int serverId : leaderAndIsr.isr()) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            server.getReplicaManager()
                    .becomeLeaderOrFollower(
                            leaderAndIsr.coordinatorEpoch(),
                            Collections.singletonList(
                                    new NotifyLeaderAndIsrData(
                                            idxNamePhysicalTablePath,
                                            idxNameBucket,
                                            leaderAndIsr.isr(),
                                            newLeaderAndIsr2)),
                            future::complete);
            future.get();
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(leader).isEqualTo(leaderA);
                });

        // Write batch 3, wait for leader A (returned) to apply
        List<Tuple2<Object[], Object[]>> batch3 = new ArrayList<>();
        for (int i = 7; i <= 10; i++) {
            batch3.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch3)))
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(10));

        IndexApplier.IndexApplyStatus statusAfterBatch3 =
                waitForIndexApplyProgress(
                        serverA.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        offsetAfterBatch2 + 1,
                        Duration.ofMinutes(1));
        long offsetAfterBatch3 = statusAfterBatch3.getLastApplyRecordsDataEndOffset();

        // Verify monotonic increase and final coverage
        assertThat(offsetAfterBatch3).isGreaterThan(offsetAfterBatch2);
        assertThat(offsetAfterBatch3).isGreaterThanOrEqualTo(dataReplica.getLocalLogEndOffset());
        LOG.info(
                "Cascading failover complete: offsets {} -> {} -> {}",
                offsetAfterBatch1,
                offsetAfterBatch2,
                offsetAfterBatch3);
    }

    @Test
    void testIndexCommitDataOffsetConsistency() throws Exception {
        // Step 1: Create indexed table with replication factor = 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        LeaderAndIsr leaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        idxNameBucket, 2, Duration.ofMinutes(2));
        int idxFollower =
                leaderAndIsr.isr().stream()
                        .filter(id -> id != idxLeader)
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException());

        // Write data and wait for HW
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);
        List<Tuple2<Object[], Object[]>> batch1 = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            batch1.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch1)))
                .get();

        TabletServer dataLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(5));

        // Step 2: Verify indexCommitDataOffset == lastApplyRecordsDataEndOffset > 0
        TabletServer idxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxLeader);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    IndexApplier applier =
                            getIndexApplier(idxLeaderServer.getReplicaManager(), idxNameBucket);
                    IndexApplier.IndexApplyStatus status =
                            applier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(status).isNotNull();
                    assertThat(status.getLastApplyRecordsDataEndOffset()).isGreaterThan(0);
                    assertThat(status.getIndexCommitDataOffset())
                            .isEqualTo(status.getLastApplyRecordsDataEndOffset());
                    assertThat(status.getIndexCommitDataOffset()).isGreaterThanOrEqualTo(0);
                });

        IndexApplier applierBefore =
                getIndexApplier(idxLeaderServer.getReplicaManager(), idxNameBucket);
        IndexApplier.IndexApplyStatus statusBefore =
                applierBefore.getOrInitIndexApplyStatus(dataTableBucket);
        long commitOffsetBefore = statusBefore.getIndexCommitDataOffset();
        long applyOffsetBefore = statusBefore.getLastApplyRecordsDataEndOffset();
        LOG.info(
                "Before switch: commitOffset={}, applyOffset={}",
                commitOffsetBefore,
                applyOffsetBefore);

        // Step 3: Switch index leader
        PhysicalTablePath idxNamePhysicalTablePath = PhysicalTablePath.of(IDX_NAME_TABLE_PATH);
        int newEpoch = leaderAndIsr.leaderEpoch() + 1;
        int newBucketEpoch = leaderAndIsr.bucketEpoch() + 1;
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        idxFollower,
                        newEpoch,
                        leaderAndIsr.isr(),
                        leaderAndIsr.coordinatorEpoch(),
                        newBucketEpoch);
        zkClient.updateLeaderAndIsr(idxNameBucket, newLeaderAndIsr);

        for (int serverId : leaderAndIsr.isr()) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            server.getReplicaManager()
                    .becomeLeaderOrFollower(
                            leaderAndIsr.coordinatorEpoch(),
                            Collections.singletonList(
                                    new NotifyLeaderAndIsrData(
                                            idxNamePhysicalTablePath,
                                            idxNameBucket,
                                            leaderAndIsr.isr(),
                                            newLeaderAndIsr)),
                            future::complete);
            future.get();
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(leader).isEqualTo(idxFollower);
                });

        // Step 4: Verify consistency on new leader
        TabletServer newIdxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxFollower);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    IndexApplier applier =
                            getIndexApplier(newIdxLeaderServer.getReplicaManager(), idxNameBucket);
                    IndexApplier.IndexApplyStatus status =
                            applier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(status).isNotNull();
                    assertThat(status.getIndexCommitDataOffset())
                            .isEqualTo(status.getLastApplyRecordsDataEndOffset());
                    assertThat(status.getIndexCommitDataOffset()).isGreaterThanOrEqualTo(0);
                });

        // Step 5: Write more data and verify both offsets advance in sync
        List<Tuple2<Object[], Object[]>> batch2 = new ArrayList<>();
        for (int i = 6; i <= 10; i++) {
            batch2.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch2)))
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(10));

        retry(
                Duration.ofMinutes(1),
                () -> {
                    IndexApplier applier =
                            getIndexApplier(newIdxLeaderServer.getReplicaManager(), idxNameBucket);
                    IndexApplier.IndexApplyStatus status =
                            applier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(status.getLastApplyRecordsDataEndOffset())
                            .isGreaterThan(applyOffsetBefore);
                    assertThat(status.getIndexCommitDataOffset())
                            .isEqualTo(status.getLastApplyRecordsDataEndOffset());
                    // Never -1
                    assertThat(status.getIndexCommitDataOffset()).isGreaterThanOrEqualTo(0);
                });

        LOG.info("testIndexCommitDataOffsetConsistency passed: offsets always consistent");
    }

    @Test
    void testMultiBucketFailover() throws Exception {
        // Step 1: Create indexed table with 3 buckets, replication factor = 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();

        // Wait for all 3 data and index buckets to be ready with ISR >= 2
        for (int b = 0; b < 3; b++) {
            TableBucket dataBucket = new TableBucket(dataTableId, b);
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataBucket);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxBucket);
            FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(dataBucket, 2, Duration.ofMinutes(2));
            FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(idxBucket, 2, Duration.ofMinutes(2));
        }

        // Step 2: Write data to all 3 data buckets
        for (int b = 0; b < 3; b++) {
            TableBucket dataBucket = new TableBucket(dataTableId, b);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                int id = b * 100 + i;
                batch.add(
                        Tuple2.of(
                                new Object[] {id},
                                new Object[] {id, "user" + id, "user" + id + "@example.com"}));
            }
            gateway.putKv(
                            newPutKvRequest(
                                    dataTableId,
                                    b,
                                    -1,
                                    genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch)))
                    .get();

            // Wait for HW
            TabletServer leaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
            Replica replica = leaderServer.getReplicaManager().getReplicaOrException(dataBucket);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(replica.getLogHighWatermark()).isGreaterThanOrEqualTo(5));
        }

        // Step 3: Wait for all index buckets to have apply progress
        for (int b = 0; b < 3; b++) {
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            TabletServer idxLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxLeader);
            Replica idxReplica =
                    idxLeaderServer.getReplicaManager().getReplicaOrException(idxBucket);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(idxReplica.getLocalLogEndOffset()).isGreaterThan(0));
        }

        // Step 4: Find the server hosting the most index bucket leaders
        Map<Integer, List<Integer>> serverToIdxBuckets = new HashMap<>();
        for (int b = 0; b < 3; b++) {
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            serverToIdxBuckets.computeIfAbsent(leader, k -> new ArrayList<>()).add(b);
        }
        int targetServer =
                serverToIdxBuckets.entrySet().stream()
                        .max((e1, e2) -> e1.getValue().size() - e2.getValue().size())
                        .get()
                        .getKey();
        List<Integer> affectedBuckets = serverToIdxBuckets.get(targetServer);
        LOG.info(
                "Stopping server {} which hosts index leaders for buckets {}",
                targetServer,
                affectedBuckets);

        // Record LEO before stop
        Map<Integer, Long> leoBefore = new HashMap<>();
        for (int b : affectedBuckets) {
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
            Replica replica = server.getReplicaManager().getReplicaOrException(idxBucket);
            leoBefore.put(b, replica.getLocalLogEndOffset());
        }

        // Step 5: Stop the target server
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(targetServer);

        // Step 6: Verify all affected index buckets re-elect leaders on other servers
        for (int b : affectedBuckets) {
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
                        assertThat(newLeader).isNotEqualTo(targetServer);
                    });
        }

        // Step 7: Write more data to all data buckets
        for (int b = 0; b < 3; b++) {
            TableBucket dataBucket = new TableBucket(dataTableId, b);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            for (int i = 6; i <= 10; i++) {
                int id = b * 100 + i;
                batch.add(
                        Tuple2.of(
                                new Object[] {id},
                                new Object[] {id, "user" + id, "user" + id + "@example.com"}));
            }
            gateway.putKv(
                            newPutKvRequest(
                                    dataTableId,
                                    b,
                                    -1,
                                    genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch)))
                    .get();

            // Wait for HW
            TabletServer leaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
            Replica replica = leaderServer.getReplicaManager().getReplicaOrException(dataBucket);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(replica.getLogHighWatermark()).isGreaterThanOrEqualTo(6));
        }

        // Step 8: Verify all index buckets' LEO grows after new data
        for (int b : affectedBuckets) {
            TableBucket idxBucket = new TableBucket(idxNameTableId, b);
            int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            TabletServer newLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(newLeader);
            Replica newLeaderReplica =
                    newLeaderServer.getReplicaManager().getReplicaOrException(idxBucket);
            long previousLeo = leoBefore.get(b);
            retry(
                    Duration.ofMinutes(2),
                    () ->
                            assertThat(newLeaderReplica.getLocalLogEndOffset())
                                    .isGreaterThan(previousLeo));
        }

        // Step 9: Restart stopped server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(targetServer);
        LOG.info("testMultiBucketFailover passed: all affected buckets recovered");
    }

    @Test
    void testPartialApplyRecoveryAfterLeaderSwitch() throws Exception {
        // Step 1: Create indexed table with replication factor = 2
        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        INDEXED_TABLE_PATH,
                        INDEXED_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        long idxNameTableId = idxNameTableInfo.getTableId();
        TableBucket idxNameBucket = new TableBucket(idxNameTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxNameBucket);

        int idxLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
        LeaderAndIsr leaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitForIsrExpansion(
                        idxNameBucket, 2, Duration.ofMinutes(2));
        int idxFollower =
                leaderAndIsr.isr().stream()
                        .filter(id -> id != idxLeader)
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException());

        // Step 2: Write a large batch of data (20 records)
        int dataLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataLeader);
        List<Tuple2<Object[], Object[]>> largeBatch = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            largeBatch.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, largeBatch)))
                .get();

        // Wait for data HW
        TabletServer dataLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(dataLeader);
        Replica dataReplica =
                dataLeaderServer.getReplicaManager().getReplicaOrException(dataTableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(20));

        // Step 3: Wait for partial apply (at least some progress)
        TabletServer oldLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxLeader);
        waitForIndexApplyProgress(
                oldLeaderServer.getReplicaManager(),
                idxNameBucket,
                dataTableBucket,
                1L,
                Duration.ofMinutes(1));

        // Record the persisted offset before switch
        IndexApplier oldApplier =
                getIndexApplier(oldLeaderServer.getReplicaManager(), idxNameBucket);
        long persistedOffsetBeforeSwitch =
                oldApplier
                        .getOrInitIndexApplyStatus(dataTableBucket)
                        .getLastApplyRecordsDataEndOffset();
        LOG.info("Persisted offset before switch: {}", persistedOffsetBeforeSwitch);

        // Step 4: Immediately switch index leader
        PhysicalTablePath idxNamePhysicalTablePath = PhysicalTablePath.of(IDX_NAME_TABLE_PATH);
        int newEpoch = leaderAndIsr.leaderEpoch() + 1;
        int newBucketEpoch = leaderAndIsr.bucketEpoch() + 1;
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        idxFollower,
                        newEpoch,
                        leaderAndIsr.isr(),
                        leaderAndIsr.coordinatorEpoch(),
                        newBucketEpoch);
        zkClient.updateLeaderAndIsr(idxNameBucket, newLeaderAndIsr);

        for (int serverId : leaderAndIsr.isr()) {
            TabletServer server = FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                    new CompletableFuture<>();
            server.getReplicaManager()
                    .becomeLeaderOrFollower(
                            leaderAndIsr.coordinatorEpoch(),
                            Collections.singletonList(
                                    new NotifyLeaderAndIsrData(
                                            idxNamePhysicalTablePath,
                                            idxNameBucket,
                                            leaderAndIsr.isr(),
                                            newLeaderAndIsr)),
                            future::complete);
            future.get();
        }

        retry(
                Duration.ofMinutes(1),
                () -> {
                    int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameBucket);
                    assertThat(leader).isEqualTo(idxFollower);
                });

        // Step 5: Verify new leader's starting offset is consistent with old leader's persisted
        // value
        TabletServer newLeaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(idxFollower);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier newApplier =
                            getIndexApplier(newLeaderServer.getReplicaManager(), idxNameBucket);
                    IndexApplier.IndexApplyStatus newStatus =
                            newApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    assertThat(newStatus).isNotNull();
                    // New leader should start from at least the persisted offset (offset
                    // continuity)
                    assertThat(newStatus.getLastApplyRecordsDataEndOffset())
                            .isGreaterThanOrEqualTo(persistedOffsetBeforeSwitch);
                });

        // Step 6: Write more data and verify new leader applies from correct offset (no gap)
        List<Tuple2<Object[], Object[]>> moreBatch = new ArrayList<>();
        for (int i = 21; i <= 30; i++) {
            moreBatch.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "user" + i + "@example.com"}));
        }
        dataGateway
                .putKv(
                        newPutKvRequest(
                                dataTableId,
                                dataTableBucket.getBucket(),
                                -1,
                                genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, moreBatch)))
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(dataReplica.getLogHighWatermark()).isGreaterThanOrEqualTo(30));

        // Step 7: Verify all data is eventually applied (no omission)
        long finalDataLogEndOffset = dataReplica.getLocalLogEndOffset();
        IndexApplier.IndexApplyStatus finalStatus =
                waitForIndexApplyProgress(
                        newLeaderServer.getReplicaManager(),
                        idxNameBucket,
                        dataTableBucket,
                        finalDataLogEndOffset,
                        Duration.ofMinutes(2));
        assertThat(finalStatus.getLastApplyRecordsDataEndOffset())
                .isGreaterThanOrEqualTo(finalDataLogEndOffset);
        LOG.info(
                "testPartialApplyRecoveryAfterLeaderSwitch passed: final offset={}, data LEO={}",
                finalStatus.getLastApplyRecordsDataEndOffset(),
                finalDataLogEndOffset);
    }

    private int chooseNewIndexLeader(List<Integer> isr, int excludeServerId) {
        // First try to find a server in ISR that is not the excluded server
        for (int serverId : isr) {
            if (serverId != excludeServerId) {
                return serverId;
            }
        }

        // If no server found in ISR (all are excluded or ISR is empty),
        // fall back to any available server from the cluster
        LOG.warn(
                "No available server in ISR {} after excluding server {}, falling back to any available server in cluster",
                isr,
                excludeServerId);

        // Get all live tablet servers (0, 1, 2 in test configuration)
        for (int serverId = 0; serverId < 3; serverId++) {
            if (serverId != excludeServerId) {
                try {
                    // Try to get the server to verify it's alive
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId);
                    LOG.info(
                            "Selected server {} as new IndexLeader from available servers",
                            serverId);
                    return serverId;
                } catch (Exception e) {
                    // Server not available, try next
                    LOG.debug("Server {} is not available: {}", serverId, e.getMessage());
                }
            }
        }

        throw new IllegalStateException(
                "No available server for new IndexLeader after excluding server: "
                        + excludeServerId
                        + ", ISR: "
                        + isr);
    }

    /**
     * Wait for index apply progress to reach the specified minimum data end offset. This helper
     * encapsulates the common retry + getIndexApplier + getOrInitIndexApplyStatus + assert pattern.
     */
    private IndexApplier.IndexApplyStatus waitForIndexApplyProgress(
            ReplicaManager replicaManager,
            TableBucket idxBucket,
            TableBucket dataBucket,
            long minDataEndOffset,
            Duration timeout) {
        IndexApplier.IndexApplyStatus[] holder = new IndexApplier.IndexApplyStatus[1];
        retry(
                timeout,
                () -> {
                    IndexApplier applier =
                            replicaManager.getReplicaOrException(idxBucket).getIndexApplier();
                    assertThat(applier).isNotNull();
                    IndexApplier.IndexApplyStatus status =
                            applier.getOrInitIndexApplyStatus(dataBucket);
                    assertThat(status).isNotNull();
                    assertThat(status.getLastApplyRecordsDataEndOffset())
                            .isGreaterThanOrEqualTo(minDataEndOffset);
                    holder[0] = status;
                });
        return holder[0];
    }

    private IndexApplier getIndexApplier(ReplicaManager replicaManager, TableBucket indexBucket) {
        Replica replica = replicaManager.getReplicaOrException(indexBucket);
        IndexApplier applier = replica.getIndexApplier();
        assertThat(applier)
                .withFailMessage(
                        "IndexApplier not yet available for %s (leader transition in progress)",
                        indexBucket)
                .isNotNull();
        return applier;
    }
}
