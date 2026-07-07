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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
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
                        .index(INDEX_NAME, "b")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .distributedBy(3, "a")
                        .partitionedBy("p")
                        .property(ConfigOptions.secondaryIndexBucketNumKey(INDEX_NAME), "3")
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

        // ---- Phase 2: Write synthetic entries to the index table with different partition IDs ----
        // These simulate what IndexReplicator would push. The Index Table's KvTablet encodes them
        // in v3 format with the __partition_id as the tag via its installed tagExtractor.
        byte[] keyP0 = encodeIndexKey("alpha", 1);
        byte[] keyP1 = encodeIndexKey("beta", 2);
        byte[] keyP2 = encodeIndexKey("gamma", 3);

        int bucketP0 = computeIndexBucket("alpha");
        int bucketP1 = computeIndexBucket("beta");
        int bucketP2 = computeIndexBucket("gamma");

        writeIndexEntry(indexTableId, bucketP0, keyP0, partitionId0);
        writeIndexEntry(indexTableId, bucketP1, keyP1, partitionId1);
        writeIndexEntry(indexTableId, bucketP2, keyP2, partitionId2);

        // Verify all entries are visible
        assertIndexEntryPresent(indexTableId, bucketP0, keyP0, true, "p0 entry must be visible");
        assertIndexEntryPresent(indexTableId, bucketP1, keyP1, true, "p1 entry must be visible");
        assertIndexEntryPresent(indexTableId, bucketP2, keyP2, true, "p2 entry must be visible");

        // Flush pre-write buffer to RocksDB so data is durable before the drop
        Replica p1IndexReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(indexTableId, bucketP1));
        KvTablet p1KvTablet = p1IndexReplica.getKvTablet();
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

        // ---- Phase 5: Write-path filter — new write for p1 must be silently dropped ----
        byte[] newKeyP1 = encodeIndexKey("delta", 4);
        int newBucketP1 = computeIndexBucket("delta");
        writeIndexEntry(indexTableId, newBucketP1, newKeyP1, partitionId1);

        // The write was silently dropped — the entry must be absent
        assertIndexEntryPresent(
                indexTableId,
                newBucketP1,
                newKeyP1,
                false,
                "new write for tombstoned partition must be filtered at write-path");

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
                    new DataField("a", DataTypes.INT().copy(false)));

    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));

    private static byte[] encodeIndexKey(String b, int a) {
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(INDEX_VALUE_ROW_TYPE);
        GenericRow row = new GenericRow(2);
        row.setField(0, fromString(b));
        row.setField(1, a);
        return encoder.encodeKey(row);
    }

    private static int computeIndexBucket(String bValue) {
        CompactedKeyEncoder bucketKeyEncoder = new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE);
        GenericRow row = new GenericRow(1);
        row.setField(0, fromString(bValue));
        byte[] bucketKey = bucketKeyEncoder.encodeKey(row);
        return new FlussBucketingFunction().bucketing(bucketKey, INDEX_BUCKET_COUNT);
    }

    /**
     * Writes a synthetic entry to the Index Table whose value carries the given partitionId in the
     * __partition_id column. The Index Table's KvTablet encodes this as v3 format with tag =
     * partitionId via its installed tagExtractor.
     */
    private void writeIndexEntry(long indexTableId, int bucket, byte[] key, long partitionId)
            throws Exception {
        // Partitioned Index Table schema: [b STRING, a INT, p STRING, __partition_id BIGINT]
        byte[] valueBytes = encodeAlignedValueWithPartitionId(partitionId);
        KvRecordBatch batch = synthesizeIndexBatch(key, valueBytes);
        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, bucket));
        FLUSS_CLUSTER_EXTENSION
                .newTabletServerClientForNode(leaderId)
                .putKv(newPutKvRequest(indexTableId, bucket, 1, batch))
                .get();
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
                        java.util.List<byte[]> result =
                                replica.lookups(Collections.singletonList(key));
                        boolean present = result.get(0) != null;
                        return present == expectPresent;
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(15),
                description);
    }

    private static byte[] encodeAlignedValueWithPartitionId(long partitionId) {
        // Partitioned Index Table schema: [b STRING, a INT, p STRING, __partition_id BIGINT]
        int arity = 4;
        AlignedRow row = new AlignedRow(arity);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.reset();
        writer.writeString(0, fromString("dummy"));
        writer.writeInt(1, 0);
        writer.writeString(2, fromString("px"));
        writer.writeLong(3, partitionId);
        writer.complete();
        byte[] body = new byte[row.getSizeInBytes()];
        row.copyTo(body, 0);
        return body;
    }

    private static KvRecordBatch synthesizeIndexBatch(byte[] key, byte[] valueBytes)
            throws IOException {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        /* schemaId */ 1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.ALIGNED);
        try {
            AlignedRow row = new AlignedRow(0);
            row.pointTo(MemorySegment.wrap(valueBytes), 0, valueBytes.length);
            builder.append(key, row);
            KvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
            batch.ensureValid();
            return batch;
        } finally {
            builder.close();
        }
    }
}
