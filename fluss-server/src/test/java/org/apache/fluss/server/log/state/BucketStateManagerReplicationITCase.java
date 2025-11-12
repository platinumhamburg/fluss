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

package org.apache.fluss.server.log.state;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultStateChangeLogs;
import org.apache.fluss.record.DefaultStateChangeLogsBuilder;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.StateChangeLogs;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.StateDefs.DATA_BUCKET_OFFSET_OF_INDEX;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link BucketStateManager} in log replication and leader-follower failover
 * scenarios.
 */
class BucketStateManagerReplicationITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        return conf;
    }

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testBucketStateManagerReplication() throws Exception {
        // Step 1: Create table with replication factor = 2
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_DESCRIPTOR.withReplicationFactor(2));
        TableBucket tb = new TableBucket(tableId, 0);

        // Step 2: Wait until all replicas are ready
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        // Step 3: Get leader and follower information from ZK
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofMinutes(1),
                        "Leader and isr not found");
        int leaderServerId = leaderAndIsr.leader();
        int followerServerId =
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leaderServerId)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No follower found"));

        // Step 4: Get leader and follower replicas using retry
        TabletServer leaderTabletServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderServerId);
        TabletServer followerTabletServer =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(followerServerId);
        ReplicaManager leaderRM = leaderTabletServer.getReplicaManager();
        ReplicaManager followerRM = followerTabletServer.getReplicaManager();

        Replica leaderReplica = leaderRM.getReplicaOrException(tb);
        Replica followerReplica = followerRM.getReplicaOrException(tb);

        // Verify leader and follower
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(leaderReplica.isLeader()).isTrue();
                    assertThat(followerReplica.isLeader()).isFalse();
                });

        // Step 5: Write data with state change logs to leader replica
        DefaultStateChangeLogsBuilder stateBuilder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(tableId, 0);
        TableBucket key2 = new TableBucket(tableId, 1);
        TableBucket key3 = new TableBucket(tableId, 2);
        TableBucket key4 = new TableBucket(tableId, 3);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key3, 300L);
        DefaultStateChangeLogs stateLogs1 = stateBuilder1.build();

        MemoryLogRecords records1 = createMemoryLogRecordsWithStateChangeLogs(0L, stateLogs1);

        // Append to leader using ProduceLogRequest
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServerId);
        leaderGateway.produceLog(newProduceLogRequest(tableId, tb.getBucket(), -1, records1)).get();

        // Step 6: Wait for follower to replicate data
        long expectedEndOffset = leaderReplica.getLocalLogEndOffset();
        retry(
                Duration.ofSeconds(30),
                () ->
                        assertThat(followerReplica.getLocalLogEndOffset())
                                .isEqualTo(expectedEndOffset));

        // Step 7: Wait for high watermark to advance
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(leaderReplica.getLogHighWatermark()).isEqualTo(expectedEndOffset));

        // Step 8: Verify leader BucketStateManager state
        BucketStateManager leaderStateManager = leaderReplica.getLogTablet().bucketStateManager();
        // State should be committed after high watermark advanced
        retry(
                Duration.ofSeconds(10),
                () -> {
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                            .getValue())
                            .isEqualTo(100L);
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)
                                            .getValue())
                            .isEqualTo(200L);
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                            .getValue())
                            .isEqualTo(300L);
                });

        // Step 9: Verify follower BucketStateManager state
        BucketStateManager followerStateManager =
                followerReplica.getLogTablet().bucketStateManager();
        // Follower should also have committed state after replication
        retry(
                Duration.ofSeconds(10),
                () -> {
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                            .getValue())
                            .isEqualTo(100L);
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)
                                            .getValue())
                            .isEqualTo(200L);
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                            .getValue())
                            .isEqualTo(300L);
                });

        // Step 10: Write more data with state changes
        DefaultStateChangeLogsBuilder stateBuilder2 = new DefaultStateChangeLogsBuilder();
        stateBuilder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 1001L);
        stateBuilder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key4, 400L);
        stateBuilder2.addLog(ChangeType.DELETE, DATA_BUCKET_OFFSET_OF_INDEX, key2, null);
        DefaultStateChangeLogs stateLogs2 = stateBuilder2.build();

        long baseOffset2 = leaderReplica.getLocalLogEndOffset();
        MemoryLogRecords records2 =
                createMemoryLogRecordsWithStateChangeLogs(baseOffset2, stateLogs2);

        leaderGateway.produceLog(newProduceLogRequest(tableId, tb.getBucket(), -1, records2)).get();

        // Step 11: Wait for follower to replicate the second batch
        long expectedEndOffset2 = leaderReplica.getLocalLogEndOffset();
        retry(
                Duration.ofSeconds(30),
                () ->
                        assertThat(followerReplica.getLocalLogEndOffset())
                                .isEqualTo(expectedEndOffset2));

        // Step 12: Wait for high watermark to advance again
        retry(
                Duration.ofSeconds(30),
                () ->
                        assertThat(leaderReplica.getLogHighWatermark())
                                .isEqualTo(expectedEndOffset2));

        // Step 13: Verify updated state on leader
        retry(
                Duration.ofSeconds(10),
                () -> {
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                            .getValue())
                            .isEqualTo(1001L);
                    assertThat(leaderStateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true))
                            .isNull();
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                            .getValue())
                            .isEqualTo(300L);
                    assertThat(
                                    leaderStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key4, true)
                                            .getValue())
                            .isEqualTo(400L);
                });

        // Step 14: Verify updated state on follower
        retry(
                Duration.ofSeconds(10),
                () -> {
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                            .getValue())
                            .isEqualTo(1001L);
                    assertThat(
                                    followerStateManager.getState(
                                            DATA_BUCKET_OFFSET_OF_INDEX, key2, true))
                            .isNull();
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                            .getValue())
                            .isEqualTo(300L);
                    assertThat(
                                    followerStateManager
                                            .getState(DATA_BUCKET_OFFSET_OF_INDEX, key4, true)
                                            .getValue())
                            .isEqualTo(400L);
                });

        // Step 15: Verify committed offsets are the same
        retry(
                Duration.ofSeconds(10),
                () ->
                        assertThat(leaderStateManager.lastCommittedOffset())
                                .isEqualTo(followerStateManager.lastCommittedOffset()));
    }

    private MemoryLogRecords createMemoryLogRecordsWithStateChangeLogs(
            long baseOffset, StateChangeLogs stateChangeLogs) throws Exception {
        // Create a simple log record with state change logs attached
        List<ChangeType> changeTypes = new ArrayList<>();
        changeTypes.add(ChangeType.APPEND_ONLY);

        List<InternalRow> rows = new ArrayList<>();
        rows.add(row(1, "test-data"));

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            1L,
                            DEFAULT_SCHEMA_ID,
                            Integer.MAX_VALUE,
                            DATA1_ROW_TYPE,
                            DEFAULT_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            LOG_MAGIC_VALUE_V3,
                            DEFAULT_SCHEMA_ID,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            stateChangeLogs);

            for (int i = 0; i < changeTypes.size(); i++) {
                builder.append(changeTypes.get(i), rows.get(i));
            }
            builder.setWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(LOG_MAGIC_VALUE_V3);
            return memoryLogRecords;
        }
    }
}
