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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CreatePartitionRequest;
import org.apache.fluss.rpc.messages.PbKeyValue;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.index.IndexApplier;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for partitioned tables with global secondary indexes.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Partition creation and deletion with concurrent data writes
 *   <li>TTL timestamp propagation for single and multi-column partitioned tables
 * </ul>
 */
public class PartitionedIndexTableITCase {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedIndexTableITCase.class);

    private static final int INDEX_TABLE_BUCKET_COUNT = 3;

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
        conf.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        conf.set(ConfigOptions.INDEX_REPLICA_FETCH_WAIT_MAX_TIME, Duration.ofMillis(200));
        conf.set(ConfigOptions.LOG_REPLICA_WRITE_OPERATION_PURGE_NUMBER, 100);
        conf.set(ConfigOptions.LOG_REPLICA_FETCH_OPERATION_PURGE_NUMBER, 100);
        return conf;
    }

    /** Test partition creation and deletion with data writes. */
    @Test
    void testPartitionOperationsWithDataWrites() throws Exception {
        LOG.info("=== Starting partition operations test ===");

        final int totalPartitions = 10;
        final int recordsPerPartition = 200;
        // Note: Manual partition deletion is not allowed for tables with indexes.
        // Tables with indexes should only have partitions managed by auto-partition mechanism.

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .index("idx_name", "name")
                        .index("idx_email", "email")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        TablePath tablePath = TablePath.of("test_db", "partition_ops_test");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        LOG.info("Created table {} with ID {}", tablePath, tableId);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TablePath nameIndexPath = TablePath.of("test_db", "__partition_ops_test__index__idx_name");
        TableInfo nameIndexInfo = metadataManager.getTable(nameIndexPath);
        assertThat(nameIndexInfo).isNotNull();

        RowType keyType =
                DataTypes.ROW(
                        new org.apache.fluss.types.DataField("id", DataTypes.INT()),
                        new org.apache.fluss.types.DataField("dt", DataTypes.STRING()));
        RowType rowType = schema.getRowType();

        Map<String, Long> partitionIdByName = new HashMap<>();
        AtomicLong totalRecordsWritten = new AtomicLong(0);
        AtomicInteger failedWrites = new AtomicInteger(0);

        // Step 1: Create partitions and write data
        LOG.info(
                "Creating {} partitions and writing {} records each",
                totalPartitions,
                recordsPerPartition);

        for (int p = 0; p < totalPartitions; p++) {
            String partitionValue = String.format("2024%02d%02d", (p / 28) + 1, (p % 28) + 1);
            PartitionSpec partitionSpec = newPartitionSpec("dt", partitionValue);

            long partitionId =
                    createPartitionAndGetId(
                            tablePath, partitionSpec, Collections.singletonList("dt"));
            partitionIdByName.put(partitionValue, partitionId);

            TableBucket dataBucket = new TableBucket(tableId, partitionId, 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataBucket);

            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

            // Write data in batches using utility method
            int batchSize = 100;
            for (int i = 0; i < recordsPerPartition; i += batchSize) {
                List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
                int endIdx = Math.min(i + batchSize, recordsPerPartition);

                for (int j = i; j < endIdx; j++) {
                    int id = p * recordsPerPartition + j;
                    Object[] key = new Object[] {id, partitionValue};
                    Object[] value =
                            new Object[] {
                                id, "name_" + id, "email_" + id + "@test.com", partitionValue
                            };
                    batch.add(Tuple2.of(key, value));
                }

                KvRecordBatch kvRecords = genKvRecordBatch(keyType, rowType, batch);
                try {
                    PutKvResponse response =
                            putKvToPartition(gateway, tableId, partitionId, 0, kvRecords);
                    assertThat(response.getBucketsRespAt(0).hasErrorCode())
                            .as("Write to partition %s should succeed", partitionValue)
                            .isFalse();
                } catch (Exception e) {
                    LOG.error("Failed to write to partition {}", partitionValue, e);
                    failedWrites.incrementAndGet();
                    throw e;
                }
            }

            totalRecordsWritten.addAndGet(recordsPerPartition);

            // Wait for high watermark to advance
            Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataBucket);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(replica.getLogHighWatermark())
                                    .isGreaterThanOrEqualTo(recordsPerPartition));

            if ((p + 1) % 5 == 0) {
                LOG.info(
                        "Progress: Created {} partitions, written {} records",
                        p + 1,
                        totalRecordsWritten.get());
            }
        }

        assertThat(failedWrites.get()).as("All writes should succeed").isEqualTo(0);
        LOG.info(
                "Step 1 completed: {} partitions created, {} records written",
                totalPartitions,
                totalRecordsWritten.get());

        // Step 2: Verify index replication
        LOG.info("Verifying index replication");
        for (int bucketId = 0; bucketId < INDEX_TABLE_BUCKET_COUNT; bucketId++) {
            TableBucket indexBucket = new TableBucket(nameIndexInfo.getTableId(), bucketId);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexBucket);

            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader).getReplicaManager();

            final int finalBucketId = bucketId;
            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
                        IndexApplier indexApplier = indexReplica.getIndexApplier();
                        assertThat(indexApplier)
                                .as(
                                        "IndexApplier should be initialized for bucket %d",
                                        finalBucketId)
                                .isNotNull();
                        assertThat(indexReplica.getLogHighWatermark())
                                .as("Index bucket %d should have data", finalBucketId)
                                .isGreaterThan(0);
                    });
        }
        LOG.info("Step 2 completed: Index replication verified");

        // Step 3: Write additional data to all partitions
        LOG.info("Writing additional data to all {} partitions", totalPartitions);
        int additionalRecordsPerPartition = 100;
        AtomicLong additionalRecordsWritten = new AtomicLong(0);

        for (int p = 0; p < totalPartitions; p++) {
            String partitionValue = String.format("2024%02d%02d", (p / 28) + 1, (p % 28) + 1);
            Long partitionId = partitionIdByName.get(partitionValue);
            assertThat(partitionId)
                    .as("Partition ID should exist for %s", partitionValue)
                    .isNotNull();

            TableBucket dataBucket = new TableBucket(tableId, partitionId, 0);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            assertThat(leader)
                    .as("Leader should be found for partition %s", partitionValue)
                    .isGreaterThanOrEqualTo(0);

            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            for (int j = 0; j < additionalRecordsPerPartition; j++) {
                int id =
                        totalPartitions * recordsPerPartition
                                + p * additionalRecordsPerPartition
                                + j;
                Object[] key = new Object[] {id, partitionValue};
                Object[] value =
                        new Object[] {
                            id, "extra_" + id, "extra_" + id + "@test.com", partitionValue
                        };
                batch.add(Tuple2.of(key, value));
            }

            KvRecordBatch kvRecords = genKvRecordBatch(keyType, rowType, batch);
            PutKvResponse response = putKvToPartition(gateway, tableId, partitionId, 0, kvRecords);
            assertThat(response.getBucketsRespAt(0).hasErrorCode())
                    .as("Additional write to partition %s should succeed", partitionValue)
                    .isFalse();
            additionalRecordsWritten.addAndGet(additionalRecordsPerPartition);
        }
        LOG.info("Step 3 completed: {} additional records written", additionalRecordsWritten.get());

        // Step 4: Final verification
        LOG.info("Final verification of index data integrity");
        long totalIndexRecords = 0;
        for (int bucketId = 0; bucketId < INDEX_TABLE_BUCKET_COUNT; bucketId++) {
            TableBucket indexBucket = new TableBucket(nameIndexInfo.getTableId(), bucketId);
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader).getReplicaManager();
            Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
            List<byte[]> kvData = indexReplica.getKvTablet().limitScan(Integer.MAX_VALUE);
            totalIndexRecords += kvData.size();
            LOG.info("Index bucket {}: {} records", bucketId, kvData.size());
        }

        long expectedMinRecords = (long) totalPartitions * recordsPerPartition;
        assertThat(totalIndexRecords)
                .as("Total index records should be at least %d", expectedMinRecords)
                .isGreaterThanOrEqualTo(expectedMinRecords);

        LOG.info(
                "=== Test completed: {} partitions created, {} index records ===",
                totalPartitions,
                totalIndexRecords);
    }

    /** Test TTL timestamp propagation for partitioned tables. */
    @Test
    void testPartitionedTableTTLTimestampPropagation() throws Exception {
        LOG.info("=== Starting TTL timestamp propagation test ===");

        // Part 1: Single-column partition (DAY)
        LOG.info("Part 1: Testing single-column partition with DAY granularity");

        Schema singlePartitionSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .index("idx_name", "name")
                        .build();

        TableDescriptor singlePartitionDescriptor =
                TableDescriptor.builder()
                        .schema(singlePartitionSchema)
                        .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        TablePath singlePartitionPath = TablePath.of("test_db", "single_partition_ttl_test");
        long singleTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, singlePartitionPath, singlePartitionDescriptor);

        MetadataManager metadataManager =
                new MetadataManager(
                        zkClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TablePath singleIndexPath =
                TablePath.of("test_db", "__single_partition_ttl_test__index__idx_name");
        TableInfo singleIndexInfo = metadataManager.getTable(singleIndexPath);
        assertThat(singleIndexInfo).isNotNull();

        String[] singlePartitionDates = {"20240115", "20240215", "20240315"};
        int recordsPerPartition = 100;
        Map<String, Long> expectedTimestamps = new HashMap<>();

        RowType singleKeyType =
                DataTypes.ROW(
                        new org.apache.fluss.types.DataField("id", DataTypes.INT()),
                        new org.apache.fluss.types.DataField("dt", DataTypes.STRING()));
        RowType singleRowType = singlePartitionSchema.getRowType();

        for (String dateStr : singlePartitionDates) {
            long expectedTs = parsePartitionStringToMillis(dateStr, "yyyyMMdd");
            expectedTimestamps.put(dateStr, expectedTs);

            PartitionSpec partitionSpec = newPartitionSpec("dt", dateStr);
            long partitionId =
                    createPartitionAndGetId(
                            singlePartitionPath, partitionSpec, Collections.singletonList("dt"));

            TableBucket dataBucket = new TableBucket(singleTableId, partitionId, 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataBucket);

            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            for (int j = 0; j < recordsPerPartition; j++) {
                int idx = java.util.Arrays.asList(singlePartitionDates).indexOf(dateStr);
                int id = idx * recordsPerPartition + j;
                Object[] key = new Object[] {id, dateStr};
                Object[] value =
                        new Object[] {id, "user_" + id, "user_" + id + "@test.com", dateStr};
                batch.add(Tuple2.of(key, value));
            }

            KvRecordBatch kvRecords = genKvRecordBatch(singleKeyType, singleRowType, batch);
            PutKvResponse response =
                    putKvToPartition(gateway, singleTableId, partitionId, 0, kvRecords);
            assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();

            Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataBucket);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(replica.getLogHighWatermark())
                                    .isGreaterThanOrEqualTo(recordsPerPartition));
            LOG.info("Written {} records to partition {}", recordsPerPartition, dateStr);
        }

        // Verify timestamps in index table
        LOG.info("Verifying timestamps in single-partition index table");
        retry(
                Duration.ofMinutes(3),
                () -> {
                    int totalRecordsVerified = 0;
                    Map<Long, Integer> timestampCounts = new HashMap<>();

                    for (int bucketId = 0; bucketId < INDEX_TABLE_BUCKET_COUNT; bucketId++) {
                        TableBucket indexBucket =
                                new TableBucket(singleIndexInfo.getTableId(), bucketId);
                        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
                        ReplicaManager replicaManager =
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(leader)
                                        .getReplicaManager();
                        Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
                        List<byte[]> kvData =
                                indexReplica.getKvTablet().limitScan(Integer.MAX_VALUE);

                        for (byte[] valueBytes : kvData) {
                            MemorySegment segment = MemorySegment.wrap(valueBytes);
                            long timestamp = segment.getLongBigEndian(0);
                            assertThat(expectedTimestamps.values())
                                    .as("Timestamp %d should match partition timestamps", timestamp)
                                    .contains(timestamp);
                            timestampCounts.merge(timestamp, 1, Integer::sum);
                            totalRecordsVerified++;
                        }
                    }

                    assertThat(timestampCounts.size())
                            .as(
                                    "Should have records from all %d partitions",
                                    singlePartitionDates.length)
                            .isEqualTo(singlePartitionDates.length);
                    assertThat(totalRecordsVerified)
                            .as(
                                    "Total index records should be %d",
                                    singlePartitionDates.length * recordsPerPartition)
                            .isEqualTo(singlePartitionDates.length * recordsPerPartition);
                    LOG.info(
                            "Verified {} records with {} unique timestamps",
                            totalRecordsVerified,
                            timestampCounts.size());
                });
        LOG.info("Part 1 completed: Single-column partition TTL verification passed");

        // Part 2: Multi-column partition with combined timestamp column
        // For multi-column partitions, the auto-partition key must contain the full timestamp
        // information in the expected format (e.g., "yyyyMM" for MONTH time unit)
        LOG.info("Part 2: Testing multi-column partition with combined timestamp column");

        Schema multiPartitionSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("data", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("ym", DataTypes.STRING()) // Combined year-month column: "yyyyMM"
                        .primaryKey("id", "region", "ym")
                        .index("idx_name", "name")
                        .build();

        TableDescriptor multiPartitionDescriptor =
                TableDescriptor.builder()
                        .schema(multiPartitionSchema)
                        .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                        .partitionedBy("region", "ym")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "ym")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.MONTH)
                        .build();

        TablePath multiPartitionPath = TablePath.of("test_db", "multi_partition_ttl_test");
        long multiTableId =
                createTable(FLUSS_CLUSTER_EXTENSION, multiPartitionPath, multiPartitionDescriptor);

        TablePath multiIndexPath =
                TablePath.of("test_db", "__multi_partition_ttl_test__index__idx_name");
        TableInfo multiIndexInfo = metadataManager.getTable(multiIndexPath);
        assertThat(multiIndexInfo).isNotNull();

        // Partitions: region + ym (year-month in yyyyMM format)
        String[][] multiPartitions = {
            {"us-east", "202301"}, {"us-west", "202401"}, {"eu-west", "202501"}
        };
        Map<String, Long> multiExpectedTimestamps = new HashMap<>();

        RowType multiKeyType =
                DataTypes.ROW(
                        new org.apache.fluss.types.DataField("id", DataTypes.INT()),
                        new org.apache.fluss.types.DataField("region", DataTypes.STRING()),
                        new org.apache.fluss.types.DataField("ym", DataTypes.STRING()));
        RowType multiRowType = multiPartitionSchema.getRowType();

        for (String[] partition : multiPartitions) {
            String region = partition[0];
            String ym = partition[1];
            String partitionKey = region + "-" + ym;
            // For MONTH time unit, the expected timestamp is the first day of the month
            long expectedTs = parsePartitionStringToMillis(ym + "01", "yyyyMMdd");
            multiExpectedTimestamps.put(partitionKey, expectedTs);

            Map<String, String> partitionValues = new HashMap<>();
            partitionValues.put("region", region);
            partitionValues.put("ym", ym);
            PartitionSpec partitionSpec = new PartitionSpec(partitionValues);

            long partitionId =
                    createPartitionAndGetId(
                            multiPartitionPath, partitionSpec, Arrays.asList("region", "ym"));
            TableBucket dataBucket = new TableBucket(multiTableId, partitionId, 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(dataBucket);

            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(dataBucket);
            TabletServerGateway gateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

            List<Tuple2<Object[], Object[]>> batch = new ArrayList<>();
            for (int j = 0; j < recordsPerPartition; j++) {
                int idx = java.util.Arrays.asList(multiPartitions).indexOf(partition);
                int id = idx * recordsPerPartition + j;
                Object[] key = new Object[] {id, region, ym};
                Object[] value = new Object[] {id, "multi_user_" + id, "data_" + id, region, ym};
                batch.add(Tuple2.of(key, value));
            }

            KvRecordBatch kvRecords = genKvRecordBatch(multiKeyType, multiRowType, batch);
            PutKvResponse response =
                    putKvToPartition(gateway, multiTableId, partitionId, 0, kvRecords);
            assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();

            Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(dataBucket);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(replica.getLogHighWatermark())
                                    .isGreaterThanOrEqualTo(recordsPerPartition));
            LOG.info("Written {} records to partition {}-{}", recordsPerPartition, region, ym);
        }

        // Verify timestamps in multi-partition index table
        LOG.info("Verifying timestamps in multi-partition index table");
        retry(
                Duration.ofMinutes(3),
                () -> {
                    int totalRecordsVerified = 0;
                    Map<Long, Integer> timestampCounts = new HashMap<>();

                    for (int bucketId = 0; bucketId < INDEX_TABLE_BUCKET_COUNT; bucketId++) {
                        TableBucket indexBucket =
                                new TableBucket(multiIndexInfo.getTableId(), bucketId);
                        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
                        ReplicaManager replicaManager =
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(leader)
                                        .getReplicaManager();
                        Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
                        List<byte[]> kvData =
                                indexReplica.getKvTablet().limitScan(Integer.MAX_VALUE);

                        for (byte[] valueBytes : kvData) {
                            MemorySegment segment = MemorySegment.wrap(valueBytes);
                            long timestamp = segment.getLongBigEndian(0);
                            assertThat(multiExpectedTimestamps.values())
                                    .as(
                                            "Timestamp %d should match multi-partition timestamps",
                                            timestamp)
                                    .contains(timestamp);
                            timestampCounts.merge(timestamp, 1, Integer::sum);
                            totalRecordsVerified++;
                        }
                    }

                    assertThat(timestampCounts.size())
                            .as(
                                    "Should have records from all %d multi-partitions",
                                    multiPartitions.length)
                            .isEqualTo(multiPartitions.length);
                    assertThat(totalRecordsVerified)
                            .as(
                                    "Total multi-partition index records should be %d",
                                    multiPartitions.length * recordsPerPartition)
                            .isEqualTo(multiPartitions.length * recordsPerPartition);
                    LOG.info(
                            "Verified {} records with {} unique timestamps",
                            totalRecordsVerified,
                            timestampCounts.size());
                });

        LOG.info("=== TTL timestamp propagation test completed successfully ===");
    }

    private static PartitionSpec newPartitionSpec(String key, String value) {
        return new PartitionSpec(Collections.singletonMap(key, value));
    }

    /**
     * Parse partition string to milliseconds timestamp. Uses system default time zone to match the
     * actual behavior of auto-partition timestamp calculation.
     */
    private long parsePartitionStringToMillis(String partitionString, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDate localDate = LocalDate.parse(partitionString, formatter);
        return localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /** Create a PutKvRequest for partitioned table. */
    private static PutKvResponse putKvToPartition(
            TabletServerGateway gateway,
            long tableId,
            long partitionId,
            int bucketId,
            KvRecordBatch kvRecordBatch)
            throws Exception {
        org.apache.fluss.rpc.messages.PutKvRequest putKvRequest =
                new org.apache.fluss.rpc.messages.PutKvRequest();
        putKvRequest.setTableId(tableId).setAcks(-1).setTimeoutMs(30000);
        org.apache.fluss.rpc.messages.PbPutKvReqForBucket pbPutKvReqForBucket =
                new org.apache.fluss.rpc.messages.PbPutKvReqForBucket();
        pbPutKvReqForBucket.setPartitionId(partitionId).setBucketId(bucketId);
        org.apache.fluss.record.DefaultKvRecordBatch batch =
                (org.apache.fluss.record.DefaultKvRecordBatch) kvRecordBatch;
        pbPutKvReqForBucket.setRecords(
                batch.getMemorySegment(), batch.getPosition(), batch.sizeInBytes());
        putKvRequest.addAllBucketsReqs(Collections.singletonList(pbPutKvReqForBucket));
        return gateway.putKv(putKvRequest).get();
    }

    /**
     * Create a partition and return its partition ID by looking up the partition name in ZooKeeper.
     *
     * @param tablePath the table path
     * @param partitionSpec the partition spec
     * @param partitionKeys the partition keys in order (as defined in the table descriptor)
     */
    private long createPartitionAndGetId(
            TablePath tablePath, PartitionSpec partitionSpec, List<String> partitionKeys)
            throws Exception {
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();

        // Create the partition request
        CreatePartitionRequest createPartitionRequest =
                new CreatePartitionRequest().setIgnoreIfNotExists(false);
        createPartitionRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        List<PbKeyValue> pbPartitionKeyAndValues = new ArrayList<>();
        partitionSpec
                .getSpecMap()
                .forEach(
                        (partitionKey, value) ->
                                pbPartitionKeyAndValues.add(
                                        new PbKeyValue().setKey(partitionKey).setValue(value)));
        createPartitionRequest.setPartitionSpec().addAllPartitionKeyValues(pbPartitionKeyAndValues);

        // Create the partition
        coordinatorGateway.createPartition(createPartitionRequest).get();

        // Get the partition name from the spec (values joined by $ in partition keys order)
        List<String> orderedValues = new ArrayList<>();
        for (String key : partitionKeys) {
            orderedValues.add(partitionSpec.getSpecMap().get(key));
        }
        String partitionName = String.join("$", orderedValues);

        // Look up the partition ID from ZooKeeper
        TablePartition tablePartition = zkClient.getPartition(tablePath, partitionName).get();
        return tablePartition.getPartitionId();
    }
}
