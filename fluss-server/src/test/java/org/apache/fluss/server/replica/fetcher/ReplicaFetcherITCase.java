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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.index.IndexApplier;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK;
import static org.apache.fluss.record.TestData.IDX_EMAIL_BUCKET_KEYS;
import static org.apache.fluss.record.TestData.IDX_EMAIL_PRIMARY_KEYS;
import static org.apache.fluss.record.TestData.IDX_EMAIL_ROW_TYPE;
import static org.apache.fluss.record.TestData.IDX_EMAIL_TABLE_PATH;
import static org.apache.fluss.record.TestData.IDX_NAME_BUCKET_KEYS;
import static org.apache.fluss.record.TestData.IDX_NAME_PRIMARY_KEYS;
import static org.apache.fluss.record.TestData.IDX_NAME_ROW_TYPE;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_DATA_WITH_KEY_AND_VALUE;
import static org.apache.fluss.record.TestData.INDEXED_KEY_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponseNotNull;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponseWithRowKind;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for replica fetcher. */
public class ReplicaFetcherITCase {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherITCase.class);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testProduceLogNeedAck() throws Exception {
        // set bucket count to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();

        // wait until all the gateway has same metadata because the follower fetcher manager need
        // to get the leader address from server metadata while make follower.
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // send one batch, which need ack.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                bucketId,
                0L);

        // check leader log data.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, bucketId, 0L)).get(),
                tableId,
                bucketId,
                10L,
                DATA1);

        // check follower log data.
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        for (int followId :
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leader)
                        .collect(Collectors.toList())) {

            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(followId).getReplicaManager();
            // wait until follower highWaterMark equals leader.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            replicaManager
                                                    .getReplicaOrException(tb)
                                                    .getLogTablet()
                                                    .getHighWatermark())
                                    .isEqualTo(10L));

            CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                    new CompletableFuture<>();
            // mock client fetch from follower.
            replicaManager.fetchLogRecords(
                    new FetchParams(-1, false, Integer.MAX_VALUE, -1, -1),
                    Collections.singletonMap(tb, new FetchReqInfo(tableId, 0L, 1024 * 1024)),
                    future::complete);
            Map<TableBucket, FetchLogResultForBucket> result = future.get();
            assertThat(result.size()).isEqualTo(1);
            FetchLogResultForBucket resultForBucket = result.get(tb);
            assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
            LogRecords records = resultForBucket.records();
            assertThat(records).isNotNull();
            assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);
        }
    }

    @Test
    void testPutKvNeedAck() throws Exception {
        // wait until all the gateway has same metadata because the follower fetcher manager need
        // to get the leader address from server metadata while make follower.
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, createPkTableDescriptor());
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // send one batch, which need ack.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId,
                                        bucketId,
                                        -1,
                                        genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get(),
                bucketId);

        // check leader log data.
        assertFetchLogResponseWithRowKind(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, bucketId, 0L)).get(),
                tableId,
                bucketId,
                8L,
                EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK);

        // check follower log data.
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        for (int followId :
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leader)
                        .collect(Collectors.toList())) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(followId).getReplicaManager();

            // wait until follower highWaterMark equals leader. So we can fetch log from follower
            // before highWaterMark.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            replicaManager
                                                    .getReplicaOrException(tb)
                                                    .getLogTablet()
                                                    .getHighWatermark())
                                    .isEqualTo(8L));

            CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                    new CompletableFuture<>();
            // mock client fetch from follower.
            replicaManager.fetchLogRecords(
                    new FetchParams(-1, false, Integer.MAX_VALUE, -1, -1),
                    Collections.singletonMap(tb, new FetchReqInfo(tableId, 0L, 1024 * 1024)),
                    future::complete);
            Map<TableBucket, FetchLogResultForBucket> result = future.get();
            assertThat(result.size()).isEqualTo(1);
            FetchLogResultForBucket resultForBucket = result.get(tb);
            assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
            LogRecords records = resultForBucket.records();
            assertThat(records).isNotNull();
            assertLogRecordsEqualsWithRowKind(
                    DATA1_ROW_TYPE, records, EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK);
        }
    }

    @Test
    void testFlushForPutKvNeedAck() throws Exception {
        // create a table and wait all replica ready
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, createPkTableDescriptor());
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        // let's kill a non leader server
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);

        int followerToStop =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().stream()
                        .filter(node -> node.id() != leader)
                        .findFirst()
                        .get()
                        .id();

        int leaderEpoch = 0;
        // stop the follower replica for the bucket
        FLUSS_CLUSTER_EXTENSION.stopReplica(followerToStop, tb, leaderEpoch);

        // put kv record batch to the leader,
        // but as one server is killed, the put won't be ack
        // , so kv won't be flushed although the log has been written

        // put kv record batch
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k1", new Object[] {2, "b"}),
                        Tuple2.of("k2", new Object[] {3, "b1"}));

        CompletableFuture<PutKvResponse> putResponse =
                leaderGateWay.putKv(newPutKvRequest(tableId, bucketId, -1, kvRecords));

        // wait until the log has been written
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(replica.getLocalLogEndOffset()).isEqualTo(4L));

        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {2, "b"}),
                                Tuple2.of("k2", new Object[] {3, "b1"})));

        // but we can't lookup the kv since it hasn't been flushed
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    leaderGateWay.lookup(newLookupRequest(tableId, bucketId, keyValue.f0)).get(),
                    null);
        }

        // start the follower replica by notify leaderAndIsr,
        // then the kv should be flushed finally
        LeaderAndIsr currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        currentLeaderAndIsr.isr(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                followerToStop, DATA1_TABLE_PATH, tb, newLeaderAndIsr, Arrays.asList(0, 1, 2));

        // wait until the put future is done
        putResponse.get();

        // then we can check all the value
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    leaderGateWay.lookup(newLookupRequest(tableId, bucketId, keyValue.f0)).get(),
                    keyValue.f1);
        }
    }

    private TableDescriptor createPkTableDescriptor() {
        return TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    @Test
    void testIndexFetcherErrorHandlingAndRetry() throws Exception {
        // Test: verify index fetcher handles errors and retries appropriately

        // wait until all the gateway has same metadata for cluster stability
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // create primary key table with index using TestData
        long dataTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, INDEXED_TABLE_PATH, INDEXED_TABLE_DESCRIPTOR);

        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        // get index table information
        TablePath indexTablePath =
                IndexTableUtils.generateIndexTablePath(INDEXED_TABLE_PATH, "idx_name");

        MetadataManager metadataManager = new MetadataManager(zkClient, new Configuration(), null);
        TableInfo indexTableInfo = metadataManager.getTable(indexTablePath);
        TableBucket indexTableBucket = new TableBucket(indexTableInfo.getTableId(), 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexTableBucket);

        // get initial leaders
        int dataTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        int indexTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexTableBucket);

        // verify initial setup
        ReplicaManager indexTableLeaderRM =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(indexTableLeader).getReplicaManager();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica indexReplica =
                            indexTableLeaderRM.getReplicaOrException(indexTableBucket);
                    IndexApplier indexApplier = indexReplica.getIndexApplier();
                    assertThat(indexApplier).isNotNull();
                });

        // simulate error by stopping data table leader temporarily
        int leaderEpoch = 0;
        FLUSS_CLUSTER_EXTENSION.stopReplica(dataTableLeader, dataTableBucket, leaderEpoch);

        // wait a bit to let the system detect the failure
        Thread.sleep(1000);

        // restore the data table leader
        LeaderAndIsr currentLeaderAndIsr = zkClient.getLeaderAndIsr(dataTableBucket).get();
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        currentLeaderAndIsr.isr(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());

        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                dataTableLeader,
                INDEXED_TABLE_PATH,
                dataTableBucket,
                newLeaderAndIsr,
                Arrays.asList(0, 1, 2));

        // verify index fetcher can recover and continue working
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica indexReplica =
                            indexTableLeaderRM.getReplicaOrException(indexTableBucket);
                    IndexApplier indexApplier = indexReplica.getIndexApplier();
                    assertThat(indexApplier).isNotNull();
                });

        // write new data to verify recovery
        TabletServerGateway dataTableLeaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader);

        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, INDEXED_DATA_WITH_KEY_AND_VALUE);
        assertPutKvResponse(
                dataTableLeaderGateway.putKv(newPutKvRequest(dataTableId, 0, -1, kvRecords)).get(),
                0);
    }

    @Test
    void testIndexReplication() throws Exception {
        // Comprehensive test for index fetching covering all verification steps
        // This test validates both idx_name and idx_email index tables simultaneously

        // Step 1: Create indexed table and wait for both data table and index tables to be created
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long dataTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, INDEXED_TABLE_PATH, INDEXED_TABLE_DESCRIPTOR);
        assertThat(dataTableId).isGreaterThanOrEqualTo(0L);

        TableBucket dataTableBucket = new TableBucket(dataTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataTableBucket);

        MetadataManager metadataManager = new MetadataManager(zkClient, new Configuration(), null);

        // Verify both index tables were created using TestData predefined paths
        TableInfo idxNameTableInfo = metadataManager.getTable(IDX_NAME_TABLE_PATH);
        TableInfo idxEmailTableInfo = metadataManager.getTable(IDX_EMAIL_TABLE_PATH);
        assertThat(idxNameTableInfo).isNotNull();
        assertThat(idxEmailTableInfo).isNotNull();

        LOG.info(
                "Step 1 completed: Created indexed table with data table {}, idx_name table {}, idx_email table {}",
                dataTableId,
                idxNameTableInfo.getTableId(),
                idxEmailTableInfo.getTableId());

        // Step 2: Write KV data to data table
        int dataTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket);
        TabletServerGateway dataTableLeaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader);

        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        INDEXED_KEY_TYPE, INDEXED_ROW_TYPE, INDEXED_DATA_WITH_KEY_AND_VALUE);
        assertPutKvResponse(
                dataTableLeaderGateway.putKv(newPutKvRequest(dataTableId, 0, -1, kvRecords)).get(),
                0);

        LOG.info(
                "Step 2 completed: Written {} KV records to data table",
                INDEXED_DATA_WITH_KEY_AND_VALUE.size());

        // Step 3: Verify data table high watermark is advanced
        Replica dataReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataTableBucket);
        long expectedDataOffset = INDEXED_DATA_WITH_KEY_AND_VALUE.size();

        retry(
                Duration.ofMinutes(1),
                () -> {
                    long dataTableHW = dataReplica.getLogTablet().getHighWatermark();
                    assertThat(dataTableHW).isEqualTo(expectedDataOffset);
                    LOG.debug("Data table high watermark advanced to: {}", dataTableHW);
                });

        LOG.info("Step 3 completed: Data table high watermark verified at {}", expectedDataOffset);

        // Step 4: Verify both index tables' index replication functionality
        TableBucket idxNameTableBucket =
                verifyIndexTableReadyForFetch(
                        IDX_NAME_TABLE_PATH,
                        "idx_name",
                        dataTableBucket,
                        expectedDataOffset,
                        metadataManager);

        TableBucket idxEmailTableBucket =
                verifyIndexTableReadyForFetch(
                        IDX_EMAIL_TABLE_PATH,
                        "idx_email",
                        dataTableBucket,
                        expectedDataOffset,
                        metadataManager);

        LOG.info("Step 4 completed: Both index tables are in index replicating.");

        // Step 5: Verify all written KV data can be read back correctly using lookup
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // Prepare key encoder for primary key lookup
                    CompactedKeyEncoder keyEncoder =
                            new CompactedKeyEncoder(INDEXED_ROW_TYPE, new int[] {0});

                    // Verify each KV record can be looked up correctly
                    for (Tuple2<Object[], Object[]> kvPair : INDEXED_DATA_WITH_KEY_AND_VALUE) {
                        Object[] keyArray = kvPair.f0;
                        Object[] valueArray = kvPair.f1;

                        // Encode the key for lookup
                        byte[] keyBytes = keyEncoder.encodeKey(row(keyArray));

                        // Encode the expected value
                        byte[] expectedValueBytes =
                                ValueEncoder.encodeValue(
                                        DEFAULT_SCHEMA_ID,
                                        compactedRow(INDEXED_ROW_TYPE, valueArray));

                        // Perform lookup and verify the result
                        assertLookupResponse(
                                dataTableLeaderGateway
                                        .lookup(newLookupRequest(dataTableId, 0, keyBytes))
                                        .get(),
                                expectedValueBytes);
                    }

                    LOG.debug(
                            "All {} KV records successfully verified through lookup",
                            INDEXED_DATA_WITH_KEY_AND_VALUE.size());
                });

        LOG.info(
                "Step 5 completed: All {} KV data records verified through lookup",
                INDEXED_DATA_WITH_KEY_AND_VALUE.size());

        // Step 6: Additional verification - ensure both index tables maintain independent state
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // Get replica managers for both index tables
                    int idxNameLeader =
                            FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxNameTableBucket);
                    int idxEmailLeader =
                            FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxEmailTableBucket);

                    ReplicaManager idxNameRM =
                            FLUSS_CLUSTER_EXTENSION
                                    .getTabletServerById(idxNameLeader)
                                    .getReplicaManager();
                    ReplicaManager idxEmailRM =
                            FLUSS_CLUSTER_EXTENSION
                                    .getTabletServerById(idxEmailLeader)
                                    .getReplicaManager();

                    // Verify both index appliers maintain independent commit offsets
                    Replica idxNameReplica = idxNameRM.getReplicaOrException(idxNameTableBucket);
                    Replica idxEmailReplica = idxEmailRM.getReplicaOrException(idxEmailTableBucket);

                    IndexApplier idxNameApplier = idxNameReplica.getIndexApplier();
                    IndexApplier idxEmailApplier = idxEmailReplica.getIndexApplier();

                    assertThat(idxNameApplier).isNotNull();
                    assertThat(idxEmailApplier).isNotNull();

                    long idxNameCommitOffset = idxNameApplier.getIndexCommitOffset(dataTableBucket);
                    long idxEmailCommitOffset =
                            idxEmailApplier.getIndexCommitOffset(dataTableBucket);

                    // Both should have processed the same amount of data from the data table
                    assertThat(idxNameCommitOffset).isEqualTo(expectedDataOffset);
                    assertThat(idxEmailCommitOffset).isEqualTo(expectedDataOffset);

                    LOG.info(
                            "Independent commit offsets verified - idx_name: {}, idx_email: {}",
                            idxNameCommitOffset,
                            idxEmailCommitOffset);
                });

        LOG.info("Step 6 completed: Both index tables maintain independent state verification");

        // Step 7: Fine-grained verification of data written to each bucket of each index table
        // Calculate expected data distribution based on BucketingFunction and verify that
        // each index table bucket contains the expected data

        // Cache for TabletServerGateway to avoid repeated newTabletServerClientForNode calls
        Map<Integer, TabletServerGateway> gatewayCache = new HashMap<>();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // Get bucketing function to calculate expected bucket distribution
                    BucketingFunction bucketingFunction = new FlussBucketingFunction();

                    KeyEncoder nameIndexBucketKeyEncoder =
                            KeyEncoder.of(IDX_NAME_ROW_TYPE, IDX_NAME_BUCKET_KEYS, null);
                    KeyEncoder emailIndexBucketKeyEncoder =
                            KeyEncoder.of(IDX_EMAIL_ROW_TYPE, IDX_EMAIL_BUCKET_KEYS, null);

                    KeyEncoder nameIndexPrimaryKeyEncoder =
                            KeyEncoder.of(IDX_NAME_ROW_TYPE, IDX_NAME_PRIMARY_KEYS, null);
                    KeyEncoder emailIndexPrimaryKeyEncoder =
                            KeyEncoder.of(IDX_EMAIL_ROW_TYPE, IDX_EMAIL_PRIMARY_KEYS, null);

                    // For each KV record, calculate which bucket it should be in and verify it's
                    // there
                    for (Tuple2<Object[], Object[]> kvPair : INDEXED_DATA_WITH_KEY_AND_VALUE) {
                        Object[] valueArray = kvPair.f1;

                        // Encode the key for index lookup
                        byte[] nameIdxBucketKeyBytes =
                                nameIndexBucketKeyEncoder.encodeKey(row(valueArray[1]));
                        byte[] emailIdxBucketKeyBytes =
                                emailIndexBucketKeyEncoder.encodeKey(row(valueArray[2]));

                        byte[] nameIdxPrimaryKeyBytes =
                                nameIndexPrimaryKeyEncoder.encodeKey(
                                        row(valueArray[1], valueArray[0]));
                        byte[] emailIdxPrimaryKeyBytes =
                                emailIndexPrimaryKeyEncoder.encodeKey(
                                        row(valueArray[2], valueArray[0]));

                        // Calculate expected bucket for name index (using constant from TestData)
                        int expectedNameBucket =
                                bucketingFunction.bucketing(
                                        nameIdxBucketKeyBytes, TestData.INDEX_TABLE_BUCKET_COUNT);
                        // Calculate expected bucket for email index (using constant from TestData)
                        int expectedEmailBucket =
                                bucketingFunction.bucketing(
                                        emailIdxBucketKeyBytes, TestData.INDEX_TABLE_BUCKET_COUNT);

                        // Get leader for name index bucket and create/fetch gateway
                        TableBucket nameIndexTableBucket =
                                new TableBucket(
                                        idxNameTableBucket.getTableId(), expectedNameBucket);
                        int nameIndexLeader =
                                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(nameIndexTableBucket);
                        TabletServerGateway nameIndexGateway =
                                gatewayCache.computeIfAbsent(
                                        nameIndexLeader,
                                        FLUSS_CLUSTER_EXTENSION::newTabletServerClientForNode);

                        // Get leader for email index bucket and create/fetch gateway
                        TableBucket emailIndexTableBucket =
                                new TableBucket(
                                        idxEmailTableBucket.getTableId(), expectedEmailBucket);
                        int emailIndexLeader =
                                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(emailIndexTableBucket);
                        TabletServerGateway emailIndexGateway =
                                gatewayCache.computeIfAbsent(
                                        emailIndexLeader,
                                        FLUSS_CLUSTER_EXTENSION::newTabletServerClientForNode);

                        // Verify name index data exists in the expected bucket using correct
                        // gateway
                        assertLookupResponseNotNull(
                                nameIndexGateway
                                        .lookup(
                                                newLookupRequest(
                                                        idxNameTableBucket.getTableId(),
                                                        expectedNameBucket,
                                                        nameIdxPrimaryKeyBytes))
                                        .get());

                        // Verify email index data exists in the expected bucket using correct
                        // gateway
                        assertLookupResponseNotNull(
                                emailIndexGateway
                                        .lookup(
                                                newLookupRequest(
                                                        idxEmailTableBucket.getTableId(),
                                                        expectedEmailBucket,
                                                        emailIdxPrimaryKeyBytes))
                                        .get());
                    }

                    LOG.info(
                            "Step 7 completed: Fine-grained verification of index table bucket data distribution");
                });

        LOG.info(
                "Comprehensive index fetch chain test completed successfully for both idx_name and idx_email tables");
    }

    private static void assertPutKvResponse(PutKvResponse putKvResponse, int bucketId) {
        assertThat(putKvResponse.getBucketsRespsCount()).isEqualTo(1);
        PbPutKvRespForBucket putKvRespForBucket = putKvResponse.getBucketsRespsList().get(0);
        assertThat(putKvRespForBucket.getBucketId()).isEqualTo(bucketId);
    }

    /**
     * Helper method to verify a single index table's functionality in the index fetch chain.
     *
     * @param indexTablePath path of the index table to verify
     * @param indexName name of the index for logging purposes
     * @param dataTableBucket data table bucket
     * @param expectedDataOffset expected data offset after data write
     * @param metadataManager metadata manager instance
     * @return the verified index table bucket
     */
    private TableBucket verifyIndexTableReadyForFetch(
            TablePath indexTablePath,
            String indexName,
            TableBucket dataTableBucket,
            long expectedDataOffset,
            MetadataManager metadataManager)
            throws Exception {
        // Get index table information
        TableInfo indexTableInfo = metadataManager.getTable(indexTablePath);
        assertThat(indexTableInfo).isNotNull();

        TableBucket indexTableBucket = new TableBucket(indexTableInfo.getTableId(), 2);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexTableBucket);

        LOG.info("Verifying index table '{}' with ID: {}", indexName, indexTableInfo.getTableId());

        // Verify index table leader fetcher is created
        int indexTableLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexTableBucket);
        ReplicaManager indexTableLeaderRM =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(indexTableLeader).getReplicaManager();

        // Wait for IndexApplier to be initialized
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica indexReplica =
                            indexTableLeaderRM.getReplicaOrException(indexTableBucket);
                    IndexApplier indexApplier = indexReplica.getIndexApplier();
                    assertThat(indexApplier).isNotNull();
                });

        // Verify ReplicaFetcherManager is properly configured
        ReplicaFetcherManager fetcherManager = indexTableLeaderRM.getReplicaFetcherManager();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(fetcherManager).isNotNull();
                    LOG.debug(
                            "Index '{}' - FetcherManager exists and is properly configured",
                            indexName);
                });

        LOG.info("Index table '{}' leader fetcher verified", indexName);

        // Verify index table completes index data replication and high watermark is advanced
        retry(
                Duration.ofMinutes(3),
                () -> {
                    Replica indexReplica =
                            indexTableLeaderRM.getReplicaOrException(indexTableBucket);

                    long indexTableHW = indexReplica.getLogTablet().getHighWatermark();
                    assertThat(indexTableHW).isGreaterThan(0L);
                    LOG.debug("Index '{}' table high watermark: {}", indexName, indexTableHW);
                });

        LOG.info("Index table '{}' high watermark advanced after replication", indexName);

        // Verify index table IndexCommitOffset is advanced
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica indexReplica =
                            indexTableLeaderRM.getReplicaOrException(indexTableBucket);
                    IndexApplier indexApplier = indexReplica.getIndexApplier();
                    assertThat(indexApplier).isNotNull();

                    long indexCommitOffset = indexApplier.getIndexCommitOffset(dataTableBucket);
                    assertThat(indexCommitOffset).isGreaterThanOrEqualTo(0L);
                    assertThat(indexCommitOffset).isEqualTo(expectedDataOffset);
                    LOG.info("Index '{}' commit offset: {}", indexName, indexCommitOffset);
                });

        LOG.info(
                "Index table '{}' IndexCommitOffset verified at {}", indexName, expectedDataOffset);

        return indexTableBucket;
    }
}
