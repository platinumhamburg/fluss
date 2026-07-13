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
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.ByteBufBytesView;
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
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.UnsafeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end immutable partition-incarnation lifecycle for secondary-index rows. */
class PartitionTTLGoldenPathITCase {

    private static final String DB = "test_db";
    private static final String INDEX_NAME = "idx_b";
    private static final String PARTITION_NAME = "p1";
    private static final int INDEX_BUCKET_COUNT = 3;
    private static final int MAIN_BUCKET_COUNT = 3;
    private static final int PRIMARY_KEY = 7;
    private static final String INDEXED_VALUE = "collision";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final RowType MAIN_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("p", DataTypes.STRING()));
    private static final RowType MAIN_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("a", DataTypes.INT().copy(false)));
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

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(configuration())
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }

    @Test
    void testDropRecreateCollisionRetiresWriterAndCompactsOnlyOldIncarnation()
            throws Exception {
        String mainName = "main_ttl_" + System.nanoTime();
        TablePath mainPath = TablePath.of(DB, mainName);
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName(mainName, INDEX_NAME));
        Schema schema =
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
                                INDEX_BUCKET_COUNT)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(MAIN_BUCKET_COUNT, "a")
                                .partitionedBy("p")
                                .build());
        createPartition(CLUSTER, mainPath, partitionSpec(), false);
        long oldPartitionId = CLUSTER.waitUntilPartitionsCreated(mainPath, 1).get(PARTITION_NAME);
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            CLUSTER.waitAndGetLeaderReplica(new TableBucket(indexTableId, bucket));
        }

        int mainBucket = mainBucket(PRIMARY_KEY);
        TableBucket oldSourceBucket = new TableBucket(mainTableId, oldPartitionId, mainBucket);
        CLUSTER.waitAndGetLeaderReplica(oldSourceBucket);
        AlignedRow oldPhysicalRow = physicalRow(oldPartitionId);
        byte[] oldPhysicalKey = physicalKey(oldPhysicalRow);
        int indexBucket = indexBucket(INDEXED_VALUE);
        TableBucket targetBucket = new TableBucket(indexTableId, indexBucket);
        Replica targetReplica = CLUSTER.waitAndGetLeaderReplica(targetBucket);
        WriterKey oldWriter = IndexWriterKey.encode(oldSourceBucket);

        // Encode both delayed requests while the old incarnation is live. They are released only
        // after the same logical partition and logical row have been recreated.
        BytesView delayedOldUpsert = indexMutation(oldWriter, 2L, oldPhysicalKey, oldPhysicalRow);
        BytesView delayedOldDelete = indexMutation(oldWriter, 3L, oldPhysicalKey, null);

        putMainRow(mainTableId, oldPartitionId, mainBucket);
        assertVisible(targetReplica, oldPhysicalKey, true, "old incarnation must be indexed");
        assertExactWriterState(targetReplica, oldWriter, 1L);
        KvTablet targetKv = targetReplica.getKvTablet();
        targetKv.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        byte[] oldValue = targetKv.getRocksDBKv().get(oldPhysicalKey);
        assertV3Tag(oldValue, oldPartitionId, "old v3 value tag");
        assertPhysicalKey(oldPhysicalKey, oldPartitionId);

        CoordinatorGateway coordinator = CLUSTER.newCoordinatorClient();
        coordinator.dropPartition(newDropPartitionRequest(mainPath, partitionSpec(), false)).get();
        TabletServerMetadataCache metadataCache =
                CLUSTER.getTabletServerById(CLUSTER.waitAndGetLeader(targetBucket))
                        .getMetadataCache();
        waitUntil(
                () ->
                        metadataCache
                                .getPartitionTombstone(mainTableId)
                                .isTombstoned(oldPartitionId),
                TIMEOUT,
                "old partition tombstone publication");
        waitUntil(
                () ->
                        !targetReplica
                                .getLogTablet()
                                .writerStateManager()
                                .lastFencedEntry(oldWriter)
                                .isPresent(),
                TIMEOUT,
                "old WriterState retirement");
        assertVisible(
                targetReplica,
                oldPhysicalKey,
                false,
                "old incarnation must be filtered immediately");
        assertThat(targetKv.getRocksDBKv().get(oldPhysicalKey))
                .as("old physical row remains until compaction")
                .isNotNull();

        createPartition(CLUSTER, mainPath, partitionSpec(), false);
        long newPartitionId = waitForRecreatedPartition(mainPath, oldPartitionId);
        assertThat(newPartitionId).isNotEqualTo(oldPartitionId);
        TableBucket newSourceBucket = new TableBucket(mainTableId, newPartitionId, mainBucket);
        CLUSTER.waitAndGetLeaderReplica(newSourceBucket);
        AlignedRow newPhysicalRow = physicalRow(newPartitionId);
        byte[] newPhysicalKey = physicalKey(newPhysicalRow);
        assertSameLogicalRowDifferentIncarnation(
                oldPhysicalKey, oldPartitionId, newPhysicalKey, newPartitionId);

        putMainRow(mainTableId, newPartitionId, mainBucket);
        assertVisible(targetReplica, newPhysicalKey, true, "new incarnation must be indexed");
        WriterKey newWriter = IndexWriterKey.encode(newSourceBucket);
        assertExactWriterState(targetReplica, newWriter, 1L);
        targetKv.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        assertV3Tag(
                targetKv.getRocksDBKv().get(newPhysicalKey),
                newPartitionId,
                "new v3 value tag");

        long walBeforeDelayed = targetReplica.getLogTablet().localLogEndOffset();
        putIndexMutation(indexTableId, indexBucket, delayedOldUpsert);
        putIndexMutation(indexTableId, indexBucket, delayedOldDelete);
        assertThat(targetReplica.getLogTablet().localLogEndOffset())
                .as("delayed old UPSERT and DELETE must append no target WAL")
                .isEqualTo(walBeforeDelayed);
        assertThat(targetReplica.getLogTablet().writerStateManager().lastFencedEntry(oldWriter))
                .as("delayed requests must not resurrect retired old WriterState")
                .isEmpty();
        assertExactWriterState(targetReplica, newWriter, 1L);
        assertVisible(
                targetReplica,
                newPhysicalKey,
                true,
                "delayed old requests must not affect the new collision row");
        assertVisible(targetReplica, oldPhysicalKey, false, "old row remains invisible");

        RocksDBKv rocks = targetKv.getRocksDBKv();
        rocks.getDb().compactRange();
        assertThat(rocks.get(oldPhysicalKey))
                .as("compaction physically removes only the old incarnation")
                .isNull();
        byte[] compactedNewValue = rocks.get(newPhysicalKey);
        assertV3Tag(compactedNewValue, newPartitionId, "new row survives compaction");
        assertVisible(targetReplica, newPhysicalKey, true, "new incarnation survives compaction");
    }

    private static PartitionSpec partitionSpec() {
        return new PartitionSpec(Collections.singletonMap("p", PARTITION_NAME));
    }

    private static void putMainRow(long tableId, long partitionId, int bucket) throws Exception {
        KvRecordBatch records =
                genKvRecordBatch(
                        DataTypes.ROW(new DataField("a", DataTypes.INT().copy(false))),
                        MAIN_ROW_TYPE,
                        Collections.singletonList(
                                Tuple2.of(
                                        new Object[] {PRIMARY_KEY},
                                        new Object[] {
                                            PRIMARY_KEY, INDEXED_VALUE, PARTITION_NAME
                                        })));
        PutKvRequest request = newPutKvRequest(tableId, bucket, 1, records);
        request.getBucketsReqsList().get(0).setPartitionId(partitionId);
        int leader = CLUSTER.waitAndGetLeader(new TableBucket(tableId, partitionId, bucket));
        PutKvResponse response = CLUSTER.newTabletServerClientForNode(leader).putKv(request).get();
        assertSuccess(response);
    }

    private static void putIndexMutation(long tableId, int bucket, BytesView records)
            throws Exception {
        PutKvRequest request =
                new PutKvRequest().setTableId(tableId).setAcks(-1).setTimeoutMs(10_000);
        request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
        PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(bucket);
        bucketRequest.setRecordsBytesView(records);
        int leader = CLUSTER.waitAndGetLeader(new TableBucket(tableId, bucket));
        assertSuccess(CLUSTER.newTabletServerClientForNode(leader).putKv(request).get());
    }

    private static BytesView indexMutation(
            WriterKey writer, long sequence, byte[] key, @Nullable AlignedRow row)
            throws Exception {
        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.ALIGNED)) {
            builder.append(key, row);
            builder.setWriterState(writer, sequence);
            ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
            byte[] copy = new byte[buffer.remaining()];
            buffer.get(copy);
            return new ByteBufBytesView(copy);
        }
    }

    private static AlignedRow physicalRow(long partitionId) {
        AlignedRow row = new AlignedRow(INDEX_VALUE_ROW_TYPE.getFieldCount());
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.reset();
        writer.writeString(0, fromString(INDEXED_VALUE));
        writer.writeInt(1, PRIMARY_KEY);
        writer.writeString(2, fromString(PARTITION_NAME));
        writer.writeLong(3, partitionId);
        writer.complete();
        return row;
    }

    private static byte[] physicalKey(AlignedRow row) {
        return new CompactedKeyEncoder(INDEX_VALUE_ROW_TYPE).encodeKey(row);
    }

    private static int mainBucket(int primaryKey) {
        GenericRow key = new GenericRow(1);
        key.setField(0, primaryKey);
        byte[] encoded = new CompactedKeyEncoder(MAIN_BUCKET_KEY_TYPE).encodeKey(key);
        return new FlussBucketingFunction().bucketing(encoded, MAIN_BUCKET_COUNT);
    }

    private static int indexBucket(String indexedValue) {
        GenericRow key = new GenericRow(1);
        key.setField(0, fromString(indexedValue));
        byte[] encoded = new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE).encodeKey(key);
        return new FlussBucketingFunction().bucketing(encoded, INDEX_BUCKET_COUNT);
    }

    private static void assertVisible(
            Replica replica, byte[] key, boolean expected, String description) {
        waitUntil(
                () -> {
                    try {
                        return !replica.prefixLookup(key).isEmpty() == expected;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                description);
    }

    private static void assertExactWriterState(
            Replica replica, WriterKey writer, long expectedSequence) {
        waitUntil(
                () ->
                        replica.getLogTablet()
                                .writerStateManager()
                                .lastFencedEntry(writer)
                                .map(FencedWriterStateEntry::lastSequence)
                                .orElse(-1L)
                                == expectedSequence,
                TIMEOUT,
                "exact WriterState sequence " + expectedSequence);
        assertThat(
                        replica.getLogTablet()
                                .writerStateManager()
                                .lastFencedEntry(writer)
                                .orElseThrow(AssertionError::new)
                                .lastSequence())
                .isEqualTo(expectedSequence);
    }

    private static void assertPhysicalKey(byte[] key, long partitionId) {
        InternalRow decoded =
                new CompactedKeyDecoder(INDEX_VALUE_ROW_TYPE, new int[] {0, 1, 2, 3})
                        .decodeKey(key);
        assertThat(decoded.getString(0).toString()).isEqualTo(INDEXED_VALUE);
        assertThat(decoded.getInt(1)).isEqualTo(PRIMARY_KEY);
        assertThat(decoded.getString(2).toString()).isEqualTo(PARTITION_NAME);
        assertThat(decoded.getLong(3)).isEqualTo(partitionId);
    }

    private static void assertSameLogicalRowDifferentIncarnation(
            byte[] oldKey, long oldId, byte[] newKey, long newId) {
        assertThat(newKey).isNotEqualTo(oldKey);
        assertPhysicalKey(oldKey, oldId);
        assertPhysicalKey(newKey, newId);
    }

    private static void assertV3Tag(byte[] value, long partitionId, String description) {
        assertThat(value).as(description).isNotNull();
        assertThat(UnsafeUtils.getLong(value, ValueEncoder.TAG_OFFSET))
                .as(description)
                .isEqualTo(partitionId);
    }

    private static long waitForRecreatedPartition(TablePath mainPath, long oldPartitionId) {
        waitUntil(
                () -> {
                    try {
                        return CLUSTER.getZooKeeperClient()
                                        .getPartition(mainPath, PARTITION_NAME)
                                        .map(partition -> partition.getPartitionId())
                                        .orElse(oldPartitionId)
                                != oldPartitionId;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                "new immutable partition incarnation");
        return CLUSTER.waitUntilPartitionsCreated(mainPath, 1).get(PARTITION_NAME);
    }

    private static void assertSuccess(PutKvResponse response) {
        assertThat(response.getBucketsRespsList())
                .singleElement()
                .satisfies(result -> assertThat(result.hasErrorCode()).isFalse());
    }
}
