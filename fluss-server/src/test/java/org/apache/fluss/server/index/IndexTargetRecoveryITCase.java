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
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Joint KV, WriterState, local WAL, and remote WAL recovery coverage for Index Tables. */
class IndexTargetRecoveryITCase {

    private static final String DB = "test_db";
    private static final String MAIN_TABLE = "index_target_recovery";
    private static final String INDEX_NAME = "idx_b";
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testSparseSequenceSurvivesJointRemoteRecoveryAndRejectsDelayedWrite()
            throws Exception {
        TablePath mainPath = TablePath.of(DB, MAIN_TABLE);
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName(MAIN_TABLE, INDEX_NAME));
        Schema mainSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "a").build();
        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, mainDescriptor);
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(AssertionError::new)
                        .tableId;
        TableBucket indexBucket = new TableBucket(indexTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(indexBucket);

        int originalLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Optional<TableAssignment> assignment = zkClient.getTableAssignment(indexTableId);
        assertThat(assignment).isPresent();
        List<Integer> replicas = assignment.get().getBucketAssignment(0).getReplicas();
        int followerToPromote =
                replicas.stream().filter(id -> id != originalLeader).findFirst().orElseThrow();
        int otherFollower =
                replicas.stream()
                        .filter(id -> id != originalLeader && id != followerToPromote)
                        .findFirst()
                        .orElseThrow();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(followerToPromote);
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(otherFollower);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(indexBucket, followerToPromote);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(indexBucket, otherFollower);

        TabletServerGateway originalGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(originalLeader);
        WriterKey writerKey = IndexWriterKey.encode(new TableBucket(mainTableId, 0));
        byte[] key = encodeIndexKey("latest", 1);
        putIndexMutation(originalGateway, indexTableId, writerKey, 100L, key, true);
        Replica originalReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(indexBucket);
        originalReplica.getLogTablet().roll(Optional.empty());
        putIndexMutation(originalGateway, indexTableId, writerKey, 500L, key, true);
        originalReplica.getLogTablet().roll(Optional.empty());

        CompletedSnapshot committedKvSnapshot =
                FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(indexBucket);
        long kvSnapshotOffset = committedKvSnapshot.getLogOffset();

        for (long sequence : Arrays.asList(900L, 1_300L, 1_700L)) {
            putIndexMutation(originalGateway, indexTableId, writerKey, sequence, key, true);
            originalReplica.getLogTablet().roll(Optional.empty());
        }

        LogTablet originalLog = originalReplica.getLogTablet();
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(indexBucket);
        waitUntil(
                () ->
                        originalLog.canFetchFromRemoteLog(kvSnapshotOffset)
                                && originalLog.localLogStartOffset() >= kvSnapshotOffset,
                Duration.ofMinutes(2),
                "wait for target WAL after the committed KV snapshot to be tiered and deleted");

        FLUSS_CLUSTER_EXTENSION.startTabletServer(followerToPromote);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(indexBucket, followerToPromote);
        Replica recoveredReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(
                        indexBucket, followerToPromote);
        assertThat(recoveredReplica.getLogTablet().localLogStartOffset())
                .isGreaterThan(kvSnapshotOffset);

        LeaderAndIsr current = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(indexBucket);
        LeaderAndIsr promoted =
                new LeaderAndIsr(
                        followerToPromote,
                        current.leaderEpoch() + 1,
                        current.isr(),
                        Collections.emptyList(),
                        current.coordinatorEpoch(),
                        current.bucketEpoch() + 1);
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                followerToPromote, indexPath, indexBucket, promoted, replicas);

        waitUntil(
                () -> {
                    try {
                        return recoveredReplica.lookups(Collections.singletonList(key)).get(0)
                                != null;
                    } catch (Exception ignored) {
                        return false;
                    }
                },
                Duration.ofMinutes(3),
                "wait for recovered Index Table KV state");

        byte[] valueBefore =
                recoveredReplica.lookups(Collections.singletonList(key)).get(0);
        long walEndBefore = recoveredReplica.getLogTablet().localLogEndOffset();
        FencedWriterStateEntry writerStateBefore =
                recoveredReplica
                        .getLogTablet()
                        .writerStateManager()
                        .lastFencedEntry(writerKey)
                        .orElseThrow(AssertionError::new);
        assertThat(writerStateBefore.lastSequence()).isEqualTo(1_700L);

        TabletServerGateway promotedGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(followerToPromote);
        putIndexMutation(promotedGateway, indexTableId, writerKey, 100L, key, false);

        assertThat(recoveredReplica.getLogTablet().localLogEndOffset()).isEqualTo(walEndBefore);
        assertThat(
                        recoveredReplica
                                .getLogTablet()
                                .writerStateManager()
                                .lastFencedEntry(writerKey))
                .contains(writerStateBefore);
        assertThat(recoveredReplica.lookups(Collections.singletonList(key)).get(0))
                .isEqualTo(valueBefore);

        FLUSS_CLUSTER_EXTENSION.startTabletServer(otherFollower);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }

    private static byte[] encodeIndexKey(String b, int a) {
        GenericRow row = new GenericRow(2);
        row.setField(0, fromString(b));
        row.setField(1, a);
        return new CompactedKeyEncoder(INDEX_ROW_TYPE).encodeKey(row);
    }

    private static void putIndexMutation(
            TabletServerGateway gateway,
            long indexTableId,
            WriterKey writerKey,
            long sequence,
            byte[] key,
            boolean upsert)
            throws Exception {
        BinaryRow row = compactedRow(INDEX_ROW_TYPE, new Object[] {"latest", 1});
        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.COMPACTED)) {
            builder.append(key, upsert ? row : null);
            builder.setWriterState(writerKey, sequence);
            BytesView batch = builder.build();
            PutKvRequest request =
                    new PutKvRequest()
                            .setTableId(indexTableId)
                            .setAcks(-1)
                            .setTimeoutMs(30_000);
            request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
            PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(0);
            bucketRequest.setRecordsBytesView(batch);
            gateway.putKv(request).get();
        }
    }
}
