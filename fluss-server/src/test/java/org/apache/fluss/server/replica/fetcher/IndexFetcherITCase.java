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
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.TsValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PbValue;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.index.IndexApplier;
import org.apache.fluss.server.index.IndexRowCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
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

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
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
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for replica fetcher. */
public class IndexFetcherITCase {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFetcherITCase.class);

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

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
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

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

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
                    // there with correct decoded values
                    for (int i = 0; i < INDEXED_DATA_WITH_KEY_AND_VALUE.size(); i++) {
                        Tuple2<Object[], Object[]> kvPair = INDEXED_DATA_WITH_KEY_AND_VALUE.get(i);
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

                        // Prepare expected values for name index table
                        // Index table row format: (indexColumn, primaryKey, __offset)
                        // For name index: (name, id, __offset)
                        Object[] expectedNameIndexRow =
                                new Object[] {
                                    valueArray[1], // name
                                    valueArray[0], // id (primary key)
                                    (long) i // __offset
                                };

                        // Prepare expected values for email index table
                        // For email index: (email, id, __offset)
                        Object[] expectedEmailIndexRow =
                                new Object[] {
                                    valueArray[2], // email
                                    valueArray[0], // id (primary key)
                                    (long) i // __offset
                                };

                        // Verify name index data with decoded value comparison
                        assertIndexLookupResponse(
                                nameIndexGateway
                                        .lookup(
                                                newLookupRequest(
                                                        idxNameTableBucket.getTableId(),
                                                        expectedNameBucket,
                                                        nameIdxPrimaryKeyBytes))
                                        .get(),
                                IDX_NAME_ROW_TYPE,
                                expectedNameIndexRow);

                        // Verify email index data with decoded value comparison
                        assertIndexLookupResponse(
                                emailIndexGateway
                                        .lookup(
                                                newLookupRequest(
                                                        idxEmailTableBucket.getTableId(),
                                                        expectedEmailBucket,
                                                        emailIdxPrimaryKeyBytes))
                                        .get(),
                                IDX_EMAIL_ROW_TYPE,
                                expectedEmailIndexRow);
                    }

                    LOG.info(
                            "Step 7 completed: Fine-grained verification of index table bucket data distribution");
                });

        LOG.info(
                "Comprehensive index fetch chain test completed successfully for both idx_name and idx_email tables");

        // Step 8: Verify IndexCache memory is properly garbage collected after replication
        // completion
        LOG.info("Step 8: Verifying IndexCache memory garbage collection");
        verifyIndexCacheMemoryReclaimed();

        LOG.info("Step 8 completed: IndexCache memory garbage collection verified");
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
            MetadataManager metadataManager) {
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

    /**
     * Helper method to decode and verify index table lookup response.
     *
     * <p>Index tables use TsValueEncoder, so we need TsValueDecoder to properly decode the value
     * and compare it with expected data.
     *
     * @param lookupResponse the lookup response to verify
     * @param indexRowType the row type of the index table
     * @param expectedValues the expected row values
     */
    private void assertIndexLookupResponse(
            LookupResponse lookupResponse, RowType indexRowType, Object[] expectedValues) {
        // First verify the response structure
        assertThat(lookupResponse.getBucketsRespsCount()).isEqualTo(1);
        PbLookupRespForBucket pbLookupRespForBucket = lookupResponse.getBucketsRespAt(0);
        assertThat(pbLookupRespForBucket.getValuesCount()).isEqualTo(1);
        PbValue pbValue = pbLookupRespForBucket.getValueAt(0);
        assertThat(pbValue.hasValues()).isTrue();

        // Decode the value using TsValueDecoder (index tables use TsValueEncoder)
        byte[] valueBytes = pbValue.getValues();
        DataType[] fieldTypes = indexRowType.getChildren().toArray(new DataType[0]);
        // Index tables use KvFormat.INDEXED, not COMPACTED
        RowDecoder rowDecoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        TsValueDecoder tsValueDecoder = new TsValueDecoder(rowDecoder);
        TsValueDecoder.TsValue decodedValue = tsValueDecoder.decodeValue(valueBytes);

        // Verify decoded row matches expected values using existing DataTestUtils method
        BinaryRow decodedRow = decodedValue.row;
        assertThat(decodedRow.getFieldCount()).isEqualTo(expectedValues.length);
        DataTestUtils.assertRowValueEquals(indexRowType, decodedRow, expectedValues);

        LOG.debug(
                "Index lookup response verified: timestamp={}, schemaId={}, row={}",
                decodedValue.ts,
                decodedValue.schemaId,
                java.util.Arrays.toString(expectedValues));
    }

    /**
     * Verifies that IndexCache memory is properly reclaimed after index replication completion.
     * This method checks that: 1. MemoryPool available memory has returned to near initial values
     * 2. IndexCache holds minimal or no memory 3. All memory segments have been properly garbage
     * collected
     */
    private void verifyIndexCacheMemoryReclaimed() {
        retry(
                Duration.ofMinutes(2),
                () -> {
                    // Trigger garbage collection to ensure proper memory cleanup
                    System.gc();
                    Thread.sleep(500); // Allow GC to complete

                    // Check memory usage across all tablet servers
                    for (int serverId = 0;
                            serverId < FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().size();
                            serverId++) {
                        ReplicaManager replicaManager =
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(serverId)
                                        .getReplicaManager();

                        try {
                            MemorySegmentPool indexCacheMemoryPool =
                                    replicaManager.getIndexCacheMemoryPool();

                            // Get memory pool statistics
                            long totalMemoryBytes = indexCacheMemoryPool.totalSize();
                            long availableMemoryBytes = indexCacheMemoryPool.availableMemory();
                            long usedMemoryBytes = totalMemoryBytes - availableMemoryBytes;
                            int freePages = indexCacheMemoryPool.freePages();

                            LOG.info(
                                    "TabletServer {} IndexCache Memory Status: "
                                            + "Total: {} bytes, Available: {} bytes, Used: {} bytes, FreePages: {}",
                                    serverId,
                                    totalMemoryBytes,
                                    availableMemoryBytes,
                                    usedMemoryBytes,
                                    freePages);

                            // Verify memory pool has returned to near-full availability
                            // Allow for small amount of retained memory (less than 5% of total)
                            double memoryUsageRatio = (double) usedMemoryBytes / totalMemoryBytes;
                            assertThat(memoryUsageRatio)
                                    .as(
                                            "Memory usage ratio should be minimal after GC for server %d",
                                            serverId)
                                    .isLessThan(0.05);

                            // Verify available memory is close to total memory
                            assertThat(availableMemoryBytes)
                                    .as(
                                            "Available memory should be close to total memory for server %d",
                                            serverId)
                                    .isGreaterThan((long) (totalMemoryBytes * 0.95));

                            // Use reflection to access private getOnlineReplicaList method
                            java.lang.reflect.Method getOnlineReplicaListMethod =
                                    ReplicaManager.class.getDeclaredMethod("getOnlineReplicaList");
                            getOnlineReplicaListMethod.setAccessible(true);
                            List<Replica> onlineReplicas =
                                    (List<Replica>)
                                            getOnlineReplicaListMethod.invoke(replicaManager);

                            // Check IndexCache statistics from all replicas
                            int totalCacheEntries = 0;
                            int totalMemorySegments = 0;

                            for (Replica replica : onlineReplicas) {
                                if (replica.getIndexCache() != null) {
                                    IndexRowCache indexRowCache =
                                            replica.getIndexCache().getIndexRowCache();
                                    if (indexRowCache != null) {
                                        totalCacheEntries += indexRowCache.getTotalEntries();
                                        totalMemorySegments +=
                                                indexRowCache.getTotalMemorySegmentCount();
                                    }
                                }
                            }

                            LOG.info(
                                    "TabletServer {} IndexCache Statistics: "
                                            + "Total entries: {}, Total memory segments: {}",
                                    serverId,
                                    totalCacheEntries,
                                    totalMemorySegments);

                            // Verify IndexCache holds minimal memory
                            // After replication completion, cache should be mostly empty
                            assertThat(totalMemorySegments)
                                    .as(
                                            "IndexCache should hold minimal memory segments after GC for server %d",
                                            serverId)
                                    .isLessThanOrEqualTo(10); // Allow for very small residual cache

                            // If there are entries, they should be minimal
                            if (totalCacheEntries > 0) {
                                assertThat(totalCacheEntries)
                                        .as(
                                                "IndexCache entries should be minimal after GC for server %d",
                                                serverId)
                                        .isLessThanOrEqualTo(
                                                100); // Allow for small residual entries
                            }

                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to access IndexCache memory information", e);
                        }
                    }

                    LOG.info("IndexCache memory reclamation verification completed successfully");
                });
    }

    /**
     * Test that index table correctly assigns timestamps from partition column for partitioned
     * tables. This is essential for TTL mechanism to work properly.
     *
     * <p>For partitioned tables, the timestamp should be extracted from the partition column value
     * and propagated to the index table buckets through Index Replication mechanism.
     */
    @Test
    void testPartitionedTableIndexTimestampAssignment() throws Exception {
        // Step 1: Create partitioned table with indexes
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long dataTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TestData.PARTITIONED_INDEXED_TABLE_PATH,
                        TestData.PARTITIONED_INDEXED_TABLE_DESCRIPTOR);
        assertThat(dataTableId).isGreaterThanOrEqualTo(0L);

        LOG.info("Step 1 completed: Created partitioned indexed table with ID {}", dataTableId);

        // Step 2: Get index table information
        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TableInfo idxNameTableInfo =
                metadataManager.getTable(TestData.PARTITIONED_IDX_NAME_TABLE_PATH);
        assertThat(idxNameTableInfo).isNotNull();

        // Get the number of buckets for the index table
        int indexBucketCount =
                idxNameTableInfo
                        .toTableDescriptor()
                        .getTableDistribution()
                        .map(dist -> dist.getBucketCount().orElse(1))
                        .orElse(1);

        // Wait for all index table buckets to be ready and IndexApplier to be initialized
        for (int bucketId = 0; bucketId < indexBucketCount; bucketId++) {
            final int finalBucketId = bucketId;
            TableBucket idxBucket = new TableBucket(idxNameTableInfo.getTableId(), finalBucketId);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxBucket);

            // Wait for IndexApplier to be initialized for this bucket
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader).getReplicaManager();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        Replica indexReplica = replicaManager.getReplicaOrException(idxBucket);
                        IndexApplier indexApplier = indexReplica.getIndexApplier();
                        assertThat(indexApplier).isNotNull();
                        LOG.debug("IndexApplier initialized for bucket {}", finalBucketId);
                    });
        }

        LOG.info(
                "Step 2 completed: Index table '{}' ready with ID {} and {} buckets, IndexAppliers initialized",
                TestData.PARTITIONED_IDX_NAME_TABLE_PATH,
                idxNameTableInfo.getTableId(),
                indexBucketCount);

        // Step 3: Create partitions with specific year values
        // Using format "yyyyMMdd" for proper timestamp extraction
        String partition2023 = "20230101"; // January 1, 2023
        String partition2024 = "20240101"; // January 1, 2024

        PartitionSpec partitionSpec2023 =
                new PartitionSpec(java.util.Collections.singletonMap("year", partition2023));
        PartitionSpec partitionSpec2024 =
                new PartitionSpec(java.util.Collections.singletonMap("year", partition2024));

        long partition2023Id =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        TestData.PARTITIONED_INDEXED_TABLE_PATH,
                        partitionSpec2023,
                        false);
        long partition2024Id =
                createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        TestData.PARTITIONED_INDEXED_TABLE_PATH,
                        partitionSpec2024,
                        false);

        // Wait for partitions to be ready (all buckets and replicas)
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(dataTableId, partition2023Id);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(dataTableId, partition2024Id);

        TableBucket dataTableBucket2023 = new TableBucket(dataTableId, partition2023Id, 0);
        TableBucket dataTableBucket2024 = new TableBucket(dataTableId, partition2024Id, 0);

        LOG.info(
                "Step 3 completed: Created partitions {} (ID: {}) and {} (ID: {})",
                partition2023,
                partition2023Id,
                partition2024,
                partition2024Id);

        // Step 4: Write data to partitions in batches
        int dataTableLeader2023 = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket2023);
        int dataTableLeader2024 = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataTableBucket2024);

        TabletServerGateway gateway2023 =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader2023);
        TabletServerGateway gateway2024 =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(dataTableLeader2024);

        Replica dataReplica2023 =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataTableBucket2023);
        Replica dataReplica2024 =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataTableBucket2024);

        // Batch 1: Write to 2023 partition
        List<Tuple2<Object[], Object[]>> batch1Data =
                Arrays.asList(
                        Tuple2.of(
                                new Object[] {1, partition2023},
                                new Object[] {1, "Alice", "alice@example.com", partition2023}),
                        Tuple2.of(
                                new Object[] {2, partition2023},
                                new Object[] {2, "Bob", "bob@example.com", partition2023}));

        KvRecordBatch batch1Records =
                genKvRecordBatch(
                        TestData.PARTITIONED_INDEXED_KEY_TYPE,
                        TestData.PARTITIONED_INDEXED_ROW_TYPE,
                        batch1Data);

        // Create PutKvRequest with partition ID for 2023 partition
        PutKvRequest putKvRequest2023 = new PutKvRequest();
        putKvRequest2023.setTableId(dataTableId).setAcks(-1).setTimeoutMs(10000);
        PbPutKvReqForBucket pbPutKvReqForBucket2023 = new PbPutKvReqForBucket();
        pbPutKvReqForBucket2023.setPartitionId(partition2023Id).setBucketId(0);
        DefaultKvRecordBatch batch1 = (DefaultKvRecordBatch) batch1Records;
        pbPutKvReqForBucket2023.setRecords(
                batch1.getMemorySegment(), batch1.getPosition(), batch1.sizeInBytes());
        putKvRequest2023.addAllBucketsReqs(Collections.singletonList(pbPutKvReqForBucket2023));
        assertPutKvResponse(gateway2023.putKv(putKvRequest2023).get(), 0);

        // Batch 2: Write to 2024 partition
        List<Tuple2<Object[], Object[]>> batch2Data =
                Arrays.asList(
                        Tuple2.of(
                                new Object[] {3, partition2024},
                                new Object[] {3, "Charlie", "charlie@example.com", partition2024}),
                        Tuple2.of(
                                new Object[] {4, partition2024},
                                new Object[] {4, "Diana", "diana@example.com", partition2024}));

        KvRecordBatch batch2Records =
                genKvRecordBatch(
                        TestData.PARTITIONED_INDEXED_KEY_TYPE,
                        TestData.PARTITIONED_INDEXED_ROW_TYPE,
                        batch2Data);

        // Create PutKvRequest with partition ID for 2024 partition
        PutKvRequest putKvRequest2024 = new PutKvRequest();
        putKvRequest2024.setTableId(dataTableId).setAcks(-1).setTimeoutMs(10000);
        PbPutKvReqForBucket pbPutKvReqForBucket2024 = new PbPutKvReqForBucket();
        pbPutKvReqForBucket2024.setPartitionId(partition2024Id).setBucketId(0);
        DefaultKvRecordBatch batch2 = (DefaultKvRecordBatch) batch2Records;
        pbPutKvReqForBucket2024.setRecords(
                batch2.getMemorySegment(), batch2.getPosition(), batch2.sizeInBytes());
        putKvRequest2024.addAllBucketsReqs(Collections.singletonList(pbPutKvReqForBucket2024));
        assertPutKvResponse(gateway2024.putKv(putKvRequest2024).get(), 0);

        LOG.info("Step 4 completed: Written {} records to 2023 and 2024 partitions", 4);

        // Verify data was written to main table
        List<byte[]> mainTableData2023 = dataReplica2023.getKvTablet().limitScan(Integer.MAX_VALUE);
        List<byte[]> mainTableData2024 = dataReplica2024.getKvTablet().limitScan(Integer.MAX_VALUE);
        LOG.info(
                "Main table data: 2023 partition has {} records, 2024 partition has {} records",
                mainTableData2023.size(),
                mainTableData2024.size());

        // Step 5: Read KV data from all index buckets and verify timestamps
        // After putKv().get() returns, index replication is already completed
        // Calculate expected timestamps from partition values
        // partition2023 = "20230101" -> January 1, 2023 00:00:00 UTC
        long expectedTimestamp2023 = parsePartitionStringToMillis("20230101");
        // partition2024 = "20240101" -> January 1, 2024 00:00:00 UTC
        long expectedTimestamp2024 = parsePartitionStringToMillis("20240101");

        LOG.info(
                "Expected timestamps - 2023: {}, 2024: {}",
                expectedTimestamp2023,
                expectedTimestamp2024);

        // Collect all index data from all buckets (data is hash distributed)
        Thread.sleep(5000); // Give some time for index replication to complete

        int totalRecordsWithValidTimestamp = 0;
        DataType[] fieldTypes = IDX_NAME_ROW_TYPE.getChildren().toArray(new DataType[0]);
        RowDecoder rowDecoder = RowDecoder.create(KvFormat.INDEXED, fieldTypes);
        TsValueDecoder tsValueDecoder = new TsValueDecoder(rowDecoder);

        for (int bucketId = 0; bucketId < indexBucketCount; bucketId++) {
            TableBucket idxBucket = new TableBucket(idxNameTableInfo.getTableId(), bucketId);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(idxBucket);

            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxBucket);
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader).getReplicaManager();
            Replica replica = replicaManager.getReplicaOrException(idxBucket);

            // Use limitScan to read all KV data from this bucket
            List<byte[]> kvValues = replica.getKvTablet().limitScan(Integer.MAX_VALUE);

            LOG.info("Bucket {}: Found {} records", bucketId, kvValues.size());

            for (byte[] valueBytes : kvValues) {
                // Decode value using TsValueDecoder (index tables use TsValueEncoder)
                TsValueDecoder.TsValue tsValue = tsValueDecoder.decodeValue(valueBytes);
                long timestamp = tsValue.ts;

                // Verify timestamp is valid and matches one of the expected partition timestamps
                assertThat(timestamp)
                        .as(
                                "Record in bucket %d should have valid timestamp (expected %d or %d)",
                                bucketId, expectedTimestamp2023, expectedTimestamp2024)
                        .isIn(expectedTimestamp2023, expectedTimestamp2024);

                totalRecordsWithValidTimestamp++;

                LOG.info(
                        "Bucket {}: Found record with timestamp {} (row: {})",
                        bucketId,
                        timestamp,
                        tsValue.row);
            }
        }

        LOG.info(
                "Step 5 completed: Verified {} records across {} buckets with correct timestamps",
                totalRecordsWithValidTimestamp,
                indexBucketCount);

        // Verify that we found all 4 records (2 from 2023 partition, 2 from 2024 partition)
        assertThat(totalRecordsWithValidTimestamp)
                .as("Should find all 4 written records")
                .isEqualTo(4);

        LOG.info(
                "Test completed: Partitioned table index timestamp assignment verified successfully");
    }

    /**
     * Helper method to parse partition string to milliseconds timestamp.
     *
     * @param partitionString partition string in format "yyyyMMdd"
     * @return timestamp in milliseconds
     */
    private long parsePartitionStringToMillis(String partitionString) {
        try {
            // Parse "yyyyMMdd" format to epoch milliseconds
            java.time.format.DateTimeFormatter formatter =
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
            java.time.LocalDate localDate = java.time.LocalDate.parse(partitionString, formatter);
            return localDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse partition string: " + partitionString, e);
        }
    }
}
