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
import org.apache.fluss.record.StateDefs;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.log.state.BucketStateManager;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 *   <li>State persisted by {@link org.apache.fluss.server.log.state.BucketStateManager} is
 *       correctly recovered
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

        LeaderAndIsr idxNameLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(idxNameBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for idx_name");

        LeaderAndIsr idxEmailLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(idxEmailBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for idx_email");

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

        // Step 6: Verify state on both leader and follower before shutdown
        BucketStateManager leaderStateManager =
                idxNameLeaderReplica.getLogTablet().bucketStateManager();
        BucketStateManager followerStateManager =
                idxNameFollowerReplica.getLogTablet().bucketStateManager();

        // State should be committed on both leader and follower
        retry(
                Duration.ofSeconds(30),
                () -> {
                    BucketStateManager.StateValueWithOffset leaderState =
                            leaderStateManager.getState(
                                    StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataTableBucket, true);
                    assertThat(leaderState).isNotNull();
                    assertThat(leaderState.getValue()).isInstanceOf(Long.class);
                    assertThat((Long) leaderState.getValue()).isGreaterThan(0L);

                    BucketStateManager.StateValueWithOffset followerState =
                            followerStateManager.getState(
                                    StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataTableBucket, true);
                    assertThat(followerState).isNotNull();
                    assertThat(followerState.getValue()).isEqualTo(leaderState.getValue());
                });

        long stateValueBeforeSwitch =
                (Long)
                        leaderStateManager
                                .getState(
                                        StateDefs.DATA_BUCKET_OFFSET_OF_INDEX,
                                        dataTableBucket,
                                        true)
                                .getValue();
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
        BucketStateManager newLeaderStateManager =
                newLeaderReplica.getLogTablet().bucketStateManager();

        retry(
                Duration.ofSeconds(30),
                () -> {
                    BucketStateManager.StateValueWithOffset recoveredState =
                            newLeaderStateManager.getState(
                                    StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataTableBucket, true);
                    assertThat(recoveredState).isNotNull();
                    assertThat(recoveredState.getValue()).isEqualTo(stateValueBeforeSwitch);
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
        // This is critical because IndexCache can only provide index data for committed records
        // The second batch added 7 more records, so HW should be at least 10 (3 + 7)
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
                                    "Data table high watermark should be at least 10 after second batch write, current HW: %d",
                                    dataTableHW)
                            .isGreaterThanOrEqualTo(10);
                });

        // Step 11: Verify new leader continues to apply index correctly
        // Wait for index data to be fetched and applied
        retry(
                Duration.ofMinutes(2),
                () -> {
                    long currentLogEndOffset = newLeaderReplica.getLocalLogEndOffset();
                    // The index data should eventually be fetched and applied
                    assertThat(currentLogEndOffset)
                            .withFailMessage(
                                    "Expected currentLogEndOffset %d to be greater than %d after writing new data",
                                    currentLogEndOffset, logEndOffsetBeforeSwitch)
                            .isGreaterThan(logEndOffsetBeforeSwitch);
                });

        // Wait for high watermark to advance
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(newLeaderReplica.getLogHighWatermark())
                                .isGreaterThan(logEndOffsetBeforeSwitch));

        // Step 12: Verify state is updated correctly after new writes
        retry(
                Duration.ofSeconds(30),
                () -> {
                    BucketStateManager.StateValueWithOffset updatedState =
                            newLeaderStateManager.getState(
                                    StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataTableBucket, true);
                    assertThat(updatedState).isNotNull();
                    assertThat((Long) updatedState.getValue())
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
        LeaderAndIsr dataLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(dataTableBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for data table");

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
        // Note: IndexFetcherManager uses exponential backoff for retries, so we need sufficient
        // timeout
        retry(
                Duration.ofMinutes(2),
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
     *       IndexCache)
     *   <li>Switch DataBucket leader to destroy IndexCache and force cold data scenario
     *   <li>Restore IndexReplicas
     *   <li>Write one more record to trigger index replication
     *   <li>Verify index replication catches up with all data through cold data loading from WAL
     * </ol>
     *
     * <p>The key step is switching DataBucket leader (Step 4) which destroys the old IndexCache and
     * creates a new one with {@code logEndOffsetOnLeaderStart > 0}. This ensures that when
     * IndexReplica fetches data, it will trigger cold data loading from WAL instead of reading hot
     * data from cache.
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

        // Step 3: Write 100 records to data table while IndexReplicas are stopped
        int dataTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader);

        int recordCount = 10;
        for (int i = 0; i < recordCount; i++) {
            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            batch.add(
                    Tuple2.of(
                            new Object[] {i},
                            new Object[] {i, "user" + i, "email" + i + "@example.com"}));
            KvRecordBatch kvRecords = genKvRecordBatch(INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, batch);
            dataTableGateway
                    .putKv(newPutKvRequest(dataTableId, dataTableBucket.getBucket(), -1, kvRecords))
                    .get();
        }

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

        // Step 3.5: Switch DataBucket leader by stopping the old leader
        // This is the KEY step to ensure we're actually testing cold data loading scenario
        // By stopping the leader, Coordinator will automatically elect a new leader and
        // the new leader's IndexCache will be created with logEndOffsetOnLeaderStart > 0
        LeaderAndIsr dataLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(dataTableBucket),
                        Duration.ofMinutes(1),
                        "Leader and isr not found for data table");

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
        // The new leader will create IndexCache with logEndOffsetOnLeaderStart = dataLogEndOffset
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
                "Step 3.5 completed: Data bucket leader switched from {} to {}, old IndexCache destroyed, new IndexCache created with logEndOffsetOnLeaderStart: {}",
                dataTableLeader,
                dataFollower,
                newLeaderLogEndOffset);

        // Use the new data leader and gateway for subsequent operations
        int newDataTableLeader = dataFollower;
        TabletServerGateway newDataTableGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newDataTableLeader);

        // Step 4: Wait for new DataBucket leader state to stabilize and IndexFetcherManager
        // to detect leader change
        // IndexFetcherManager's GoalReconciliationThread checks leader changes every 5 seconds,
        // so we need to wait at least 10 seconds to ensure it has time to:
        // 1. Detect the DataBucket leader change from ZooKeeper
        // 2. Mark old goals as failed
        // 3. Create new fetcher threads pointing to the new leader
        LOG.info(
                "Step 4: Waiting for new DataBucket leader state to stabilize and IndexFetcherManager to detect leader change...");
        Thread.sleep(12000);

        LOG.info(
                "Step 4 completed: New DataBucket leader is stable and IndexFetcherManager should have detected leader change");

        // Step 5: Restore IndexReplica by assigning a leader
        // Now when IndexReplica fetches data, it will trigger cold data loading
        // Choose a new IndexLeader from ISR, but exclude the stopped DataBucket leader
        final int newIndexLeader = chooseNewIndexLeader(initialLeaderAndIsr.isr(), dataTableLeader);
        LOG.info(
                "Step 5: Selected new IndexLeader: {} (excluding stopped DataBucket leader: {})",
                newIndexLeader,
                dataTableLeader);

        // Create new ISR excluding the stopped server to ensure high watermark can advance
        List<Integer> newIsr =
                initialLeaderAndIsr.isr().stream()
                        .filter(id -> id != dataTableLeader)
                        .collect(java.util.stream.Collectors.toList());

        LeaderAndIsr restoredLeaderAndIsr =
                new LeaderAndIsr(
                        newIndexLeader,
                        stoppedLeaderAndIsr.leaderEpoch() + 1,
                        newIsr,
                        initialLeaderAndIsr.coordinatorEpoch(),
                        stoppedLeaderAndIsr.bucketEpoch() + 1);

        zkClient.updateLeaderAndIsr(idxNameBucket, restoredLeaderAndIsr);

        // Notify all replicas with new leader (only active servers in new ISR)
        for (int serverId : newIsr) {
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
        // This will trigger IndexFetcherThread to fetch index data from DataTable's IndexCache
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
                Duration.ofMinutes(2),
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
        retry(
                Duration.ofSeconds(30),
                () -> {
                    IndexApplier indexApplier = getIndexApplier(indexReplicaManager, idxNameBucket);
                    IndexApplier.IndexApplyStatus finalStatus =
                            indexApplier.getOrInitIndexApplyStatus(dataTableBucket);
                    long indexCommitDataOffset = finalStatus.getIndexCommitDataOffset();

                    LOG.info(
                            "Enhanced verification - IndexCommitDataOffset: {}, IndexBucket LogEndOffset: {}, Data LogEndOffset: {}",
                            indexCommitDataOffset,
                            indexLogEndOffset,
                            dataLogEndOffset);

                    // Verify index commit offset is progressing properly
                    assertThat(indexCommitDataOffset)
                            .withFailMessage(
                                    "Index commit offset should be greater than 0 after index replication. "
                                            + "Current: %d",
                                    indexCommitDataOffset)
                            .isGreaterThan(0);
                });

        LOG.info("Test completed: Cold data loading successfully recovered all missing index data");
    }

    private int chooseNewIndexLeader(List<Integer> isr, int excludeServerId) {
        for (int serverId : isr) {
            if (serverId != excludeServerId) {
                return serverId;
            }
        }
        throw new IllegalStateException(
                "No available server for new IndexLeader after excluding server: "
                        + excludeServerId);
    }

    private IndexApplier getIndexApplier(ReplicaManager replicaManager, TableBucket indexBucket) {
        Replica replica = replicaManager.getReplicaOrException(indexBucket);
        return replica.getIndexApplier();
    }
}
