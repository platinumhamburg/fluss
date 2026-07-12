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

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.decode.CompactedKeyDecoder;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.UnsafeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden-path end-to-end test for the partition data TTL lifecycle:
 *
 * <pre>
 * DROP PARTITION (Admin API)
 *   → Coordinator persists PartitionTombstone in ZK
 *   → UpdateMetadataRequest propagates tombstone to TabletServers
 *   → Write-path value filter rejects new entries for tombstoned partition
 *   → RocksDB compaction (FloorSetCompactionFilter) physically removes old entries
 * </pre>
 *
 * <p>This test exercises the REAL propagation path end-to-end, from Admin API through to physical
 * deletion. It does NOT inject tombstones directly into the cache (unlike {@link
 * IndexPushReplicationITCase#testDroppedPartitionEntriesAreFilteredFromIndex()} which simulates
 * propagation).
 *
 * <p><b>Requires the custom RocksDB fork JAR</b> (containing {@code FloorSetCompactionFilter}) to
 * be installed in the local Maven cache. Without it, the compaction filter is not available and the
 * physical deletion assertion will fail.
 */
class PartitionTTLGoldenPathITCase {

    private static final String DB = "test_db";
    private static final String INDEX_NAME = "idx_b";
    private static final int INDEX_BUCKET_COUNT = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }

    /**
     * Full lifecycle: CREATE → WRITE → DROP PARTITION → tombstone propagates → write-path filter →
     * compaction physically removes → other partitions survive.
     */
    @Test
    void testDropPartitionPhysicallyRemovesIndexEntries() throws Exception {
        // ---- Phase 1: Create partitioned main table + secondary index ----
        String mainName = "main_ttl_golden";
        TablePath mainPath = TablePath.of(DB, mainName);
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName(mainName, INDEX_NAME));

        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("a", "p")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .distributedBy(3, "a")
                        .partitionedBy("p")
                        .build();

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, descriptor);

        // Create partitions p0, p1, p2 via the public helper (handles RPC internally)
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                mainPath,
                new PartitionSpec(Collections.singletonMap("p", "p0")),
                false);
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                mainPath,
                new PartitionSpec(Collections.singletonMap("p", "p1")),
                false);
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                mainPath,
                new PartitionSpec(Collections.singletonMap("p", "p2")),
                false);

        // Wait for partitions and get their IDs
        Map<String, Long> partitions =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(mainPath, 3);
        long partitionId0 = partitions.get("p0");
        long partitionId1 = partitions.get("p1");
        long partitionId2 = partitions.get("p2");

        // Get the auto-derived Index Table
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Auto-derived Index Table "
                                                        + indexPath
                                                        + " not in ZK."))
                        .tableId;
        // Wait for all index buckets to be ready
        for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        // ---- Phase 2: Write synthetic entries to the index table with different partition IDs
        // ----
        // These simulate what IndexReplicator would push. The Index Table's KvTablet encodes them
        // in v3 format with the __partition_id as the tag via its installed tagExtractor.
        AlignedRow rowP0 = encodeIndexRow("alpha", 1, "p0", partitionId0);
        AlignedRow rowP1 = encodeIndexRow("beta", 2, "p1", partitionId1);
        AlignedRow rowP2 = encodeIndexRow("gamma", 3, "p2", partitionId2);
        byte[] keyP0 = encodeIndexKey(rowP0);
        byte[] keyP1 = encodeIndexKey(rowP1);
        byte[] keyP2 = encodeIndexKey(rowP2);

        int bucketP0 = computeIndexBucket("alpha");
        int bucketP1 = computeIndexBucket("beta");
        int bucketP2 = computeIndexBucket("gamma");

        writeIndexEntry(mainTableId, indexTableId, bucketP0, keyP0, rowP0, partitionId0, 1L);
        writeIndexEntry(mainTableId, indexTableId, bucketP1, keyP1, rowP1, partitionId1, 1L);
        writeIndexEntry(mainTableId, indexTableId, bucketP2, keyP2, rowP2, partitionId2, 1L);

        InternalRow decodedP1 =
                new CompactedKeyDecoder(INDEX_VALUE_ROW_TYPE, new int[] {0, 1, 2, 3})
                        .decodeKey(keyP1);
        assertThat(decodedP1.getLong(3))
                .as("physical index key must carry the old partition incarnation")
                .isEqualTo(partitionId1);
        Replica p1IndexReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(indexTableId, bucketP1));
        KvTablet p1KvTablet = p1IndexReplica.getKvTablet();
        KvPreWriteBuffer.Value bufferedP1Value =
                p1KvTablet.getKvPreWriteBuffer().get(Key.of(keyP1));
        byte[] encodedP1Value =
                bufferedP1Value == null
                        ? p1KvTablet.getRocksDBKv().get(keyP1)
                        : bufferedP1Value.get();
        assertThat(encodedP1Value).isNotNull();
        assertThat(UnsafeUtils.getLong(encodedP1Value, ValueEncoder.TAG_OFFSET))
                .as("v3 index value tag must carry the old partition incarnation")
                .isEqualTo(partitionId1);

        // Verify all entries are visible
        assertIndexEntryPresent(indexTableId, bucketP0, keyP0, true, "p0 entry must be visible");
        assertIndexEntryPresent(indexTableId, bucketP1, keyP1, true, "p1 entry must be visible");
        assertIndexEntryPresent(indexTableId, bucketP2, keyP2, true, "p2 entry must be visible");

        // Flush pre-write buffer to RocksDB so data is durable before the drop
        p1KvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Also flush other buckets that hold entries
        flushIndexBucket(indexTableId, bucketP0);
        flushIndexBucket(indexTableId, bucketP2);

        // ---- Phase 3: DROP PARTITION p1 via Admin API (the REAL propagation path) ----
        CoordinatorGateway coordinator = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinator
                .dropPartition(
                        newDropPartitionRequest(
                                mainPath,
                                new PartitionSpec(Collections.singletonMap("p", "p1")),
                                false))
                .get();

        // ---- Phase 4: Wait for tombstone to propagate to the TabletServer metadata cache ----
        int indexLeaderServerId =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, 0));
        TabletServerMetadataCache cache =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(indexLeaderServerId).getMetadataCache();

        waitUntil(
                () -> cache.getPartitionTombstone(mainTableId).isTombstoned(partitionId1),
                Duration.ofSeconds(30),
                "wait for partition tombstone to propagate from Coordinator to TabletServer");

        // Intermediate assertion: tombstone version must be > 0
        assertThat(cache.getPartitionTombstone(mainTableId).getVersion())
                .as("tombstone version must be > 0 after DROP PARTITION propagation")
                .isGreaterThan(0L);

        WriterKey oldWriter = IndexWriterKey.encode(new TableBucket(mainTableId, partitionId1, 0));
        waitUntil(
                () ->
                        !p1IndexReplica
                                .getLogTablet()
                                .writerStateManager()
                                .lastFencedEntry(oldWriter)
                                .isPresent(),
                Duration.ofSeconds(30),
                "wait for tombstone publication to retire the old partition writer");
        assertThat(p1IndexReplica.getLogTablet().writerStateManager().lastFencedEntry(oldWriter))
                .as("drop must retire the old partition writer")
                .isEmpty();

        assertIndexEntryPresent(
                indexTableId,
                bucketP1,
                keyP1,
                false,
                "old partition row must be filtered immediately after tombstone publication");
        assertThat(p1KvTablet.getRocksDBKv().get(keyP1))
                .as("old row must still be physically present before compaction")
                .isNotNull();

        // Recreate the same logical partition. Its immutable partition ID is a new incarnation.
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                mainPath,
                new PartitionSpec(Collections.singletonMap("p", "p1")),
                false);
        long recreatedPartitionId = waitForRecreatedPartition(mainPath, "p1", partitionId1);
        assertThat(recreatedPartitionId).isNotEqualTo(partitionId1);
        AlignedRow recreatedRow = encodeIndexRow("epsilon", 5, "p1", recreatedPartitionId);
        byte[] recreatedKey = encodeIndexKey(recreatedRow);
        int recreatedBucket = computeIndexBucket("epsilon");
        writeIndexEntry(
                mainTableId,
                indexTableId,
                recreatedBucket,
                recreatedKey,
                recreatedRow,
                recreatedPartitionId,
                1L);
        assertIndexEntryPresent(
                indexTableId,
                recreatedBucket,
                recreatedKey,
                true,
                "recreated partition row must be visible");

        // ---- Phase 5: Write-path filter — new write for p1 must be silently dropped ----
        AlignedRow delayedOldRow = encodeIndexRow("delta", 4, "p1", partitionId1);
        byte[] newKeyP1 = encodeIndexKey(delayedOldRow);
        int newBucketP1 = computeIndexBucket("delta");
        long oldWalEndBeforeDelayed =
                FLUSS_CLUSTER_EXTENSION
                        .waitAndGetLeaderReplica(new TableBucket(indexTableId, newBucketP1))
                        .getLogTablet()
                        .localLogEndOffset();
        long oldDeleteWalEndBeforeDelayed = p1IndexReplica.getLogTablet().localLogEndOffset();
        writeIndexEntry(
                mainTableId, indexTableId, newBucketP1, newKeyP1, delayedOldRow, partitionId1, 2L);
        writeIndexEntry(mainTableId, indexTableId, bucketP1, keyP1, null, partitionId1, 3L);

        // The write was silently dropped — the entry must be absent
        assertIndexEntryPresent(
                indexTableId,
                newBucketP1,
                newKeyP1,
                false,
                "new write for tombstoned partition must be filtered at write-path");
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .waitAndGetLeaderReplica(new TableBucket(indexTableId, newBucketP1))
                                .getLogTablet()
                                .localLogEndOffset())
                .as("delayed old-incarnation requests must not append target WAL")
                .isEqualTo(oldWalEndBeforeDelayed);
        assertThat(p1IndexReplica.getLogTablet().localLogEndOffset())
                .as("delayed old-incarnation DELETE must not append target WAL")
                .isEqualTo(oldDeleteWalEndBeforeDelayed);
        assertIndexEntryPresent(
                indexTableId,
                recreatedBucket,
                recreatedKey,
                true,
                "delayed old UPSERT and DELETE must not affect the recreated incarnation");

        // ---- Phase 6: Trigger compaction on p1's bucket → physical deletion ----
        RocksDBKv rocksDBKv = p1KvTablet.getRocksDBKv();
        rocksDBKv.getDb().compactRange();

        // ---- Phase 7: Verify physical deletion ----
        assertIndexEntryPresent(
                indexTableId,
                bucketP1,
                keyP1,
                false,
                "p1's old entry must be physically removed after compaction");
        assertThat(rocksDBKv.get(keyP1)).isNull();

        // ---- Phase 8: Other partitions must survive compaction ----
        assertIndexEntryPresent(
                indexTableId,
                bucketP0,
                keyP0,
                true,
                "p0 entry must survive compaction (not tombstoned)");
        assertIndexEntryPresent(
                indexTableId,
                bucketP2,
                keyP2,
                true,
                "p2 entry must survive compaction (not tombstoned)");
    }

    // ----- Helpers -----

    private static final RowType INDEX_VALUE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)),
                    new DataField("p", DataTypes.STRING().copy(false)),
                    new DataField(
                            IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                            DataTypes.BIGINT().copy(false)));

    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));

    private static AlignedRow encodeIndexRow(String b, int a, String p, long partitionId) {
        AlignedRow row = new AlignedRow(INDEX_VALUE_ROW_TYPE.getFieldCount());
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.reset();
        writer.writeString(0, fromString(b));
        writer.writeInt(1, a);
        writer.writeString(2, fromString(p));
        writer.writeLong(3, partitionId);
        writer.complete();
        return row;
    }

    private static byte[] encodeIndexKey(AlignedRow row) {
        return new CompactedKeyEncoder(INDEX_VALUE_ROW_TYPE).encodeKey(row);
    }

    private static int computeIndexBucket(String bValue) {
        CompactedKeyEncoder bucketKeyEncoder = new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE);
        GenericRow row = new GenericRow(1);
        row.setField(0, fromString(bValue));
        byte[] bucketKey = bucketKeyEncoder.encodeKey(row);
        return new FlussBucketingFunction().bucketing(bucketKey, INDEX_BUCKET_COUNT);
    }

    private void writeIndexEntry(
            long mainTableId,
            long indexTableId,
            int bucket,
            byte[] key,
            @Nullable AlignedRow row,
            long partitionId,
            long sequence)
            throws Exception {
        int leaderId =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, bucket));
        WriterKey writerKey = IndexWriterKey.encode(new TableBucket(mainTableId, partitionId, 0));
        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.ALIGNED)) {
            builder.append(key, row);
            builder.setWriterState(writerKey, sequence);
            BytesView batch = builder.build();
            PutKvRequest request =
                    new PutKvRequest().setTableId(indexTableId).setAcks(-1).setTimeoutMs(10_000);
            request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
            PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(bucket);
            bucketRequest.setRecordsBytesView(batch);
            PutKvResponse response =
                    FLUSS_CLUSTER_EXTENSION
                            .newTabletServerClientForNode(leaderId)
                            .putKv(request)
                            .get();
            assertThat(response.getBucketsRespsList())
                    .singleElement()
                    .satisfies(result -> assertThat(result.hasErrorCode()).isFalse());
        }
    }

    private void flushIndexBucket(long indexTableId, int bucket) throws Exception {
        Replica replica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(indexTableId, bucket));
        KvTablet kvTablet = replica.getKvTablet();
        if (kvTablet != null) {
            kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }
    }

    private void assertIndexEntryPresent(
            long indexTableId, int bucket, byte[] key, boolean expectPresent, String description) {
        TableBucket tb = new TableBucket(indexTableId, bucket);
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        waitUntil(
                () -> {
                    try {
                        boolean present = !replica.prefixLookup(key).isEmpty();
                        return present == expectPresent;
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(15),
                description);
    }

    private static long waitForRecreatedPartition(
            TablePath mainPath, String partitionName, long oldPartitionId) {
        Map<String, Long> partitions =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(mainPath, 3);
        Long recreated = partitions.get(partitionName);
        assertThat(recreated).isNotNull().isNotEqualTo(oldPartitionId);
        return recreated;
    }
}
