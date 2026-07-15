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
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ITCase for the WAL-driven index replication pipeline (WAL → {@link IndexReplicator} →
 * {@code IndexAccumulator} → {@code IndexSender} → PutKv to the Index Table leader).
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@link #testInsertOnMainTablePushesEntryToIndexTable()} — INSERT pushes a single UPSERT to
 *       the Index Table.
 *   <li>{@link #testUpdateRewritesIndexEntry()} — UPDATE on the indexed column produces a DELETE on
 *       the old composite key and an UPSERT on the new composite key.
 *   <li>{@link #testDeleteRemovesIndexEntry()} — DELETE on the main table removes the index entry.
 *   <li>{@link #testEmptyWalBatchAdvancesSyncIndexOffset()} — empty WAL batches still advance index
 *       progress.
 *   <li>{@link #testAsyncVisibilityEventuallyVisible()} — with async {@code Schema.Index}
 *       visibility the PutKv ack does not wait for the push, but the index entry is eventually
 *       visible.
 *   <li>{@link #testDroppedPartitionEntriesAreFilteredFromIndex()} — with a partition tombstone
 *       injected into the TabletServer's metadata cache, an Index Table PutKv whose value carries
 *       the tombstoned partitionId is silently dropped by the apply-path filter.
 * </ul>
 *
 * <p>Leader-failover scenarios (main-table and index-table leader switchover) are covered by {@link
 * IndexPushFailoverITCase}.
 *
 * <p>Each test uses a distinct main-table name so that the shared {@link FlussClusterExtension}
 * cluster does not see colliding paths across scenarios.
 */
class IndexPushReplicationITCase {

    private static final String DB = "test_db";
    private static final String INDEX_NAME = "idx_b";

    /**
     * Single tablet server keeps replication trivial — both the main table's data leader and the
     * index table's leader are this one process so the index push is effectively a self-RPC. The
     * test still exercises the full {@code TabletServerGateway} → {@code KvManager.putAsLeader}
     * round-trip; we just don't need to coordinate ISR shrink/grow.
     */
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
     * Index value row type {@code (b STRING NOT NULL, a INT NOT NULL)}: composite of idx cols (b)
     * followed by the base PK (a), with {@code NOT NULL} forced because both make up the Index
     * Table's PK.
     */
    private static final RowType INDEX_VALUE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));

    /** Physical partitioned Index Table row: index columns, deduplicated base PK, partition ID. */
    private static final RowType PARTITIONED_INDEX_VALUE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)),
                    new DataField("p", DataTypes.STRING().copy(false)),
                    new DataField(
                            IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                            DataTypes.BIGINT().copy(false)));

    /** Bounded poll deadline for index visibility checks — well above the longest observed push. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Happy-path scenario #1 (FIP V2 §3, sync visibility): write 1 row to the indexed main table,
     * verify the corresponding entry shows up in the Index Table.
     */
    @Test
    void testInsertOnMainTablePushesEntryToIndexTable() throws Exception {
        String mainName = "main_t_insert";
        Fixture f = setupTables(mainName, /* visibility */ null);

        KvRecordBatch batch = genKvRecordBatch(new Object[] {1, "hello"});
        PutKvRequest putKvRequest =
                newPutKvRequest(f.mainTableId, /* bucketId */ 0, /* acks */ 1, batch);
        f.mainGateway.putKv(putKvRequest).get();

        byte[] indexKey = encodeIndexKey("hello", 1);

        // SYNC visibility (the default) means the PutKv ack only returns AFTER the index push has
        // been applied, so the entry must be visible IMMEDIATELY — without any polling. A
        // regression
        // that turned SYNC into async would leave this synchronous lookup empty.
        int indexBucket = computeIndexBucket("hello");
        Replica indexLeader =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(f.indexTableId, indexBucket));
        assertThat(indexLeader.lookups(Collections.singletonList(indexKey)).get(0))
                .as("under SYNC visibility the index entry must be visible right after the ack")
                .isNotNull();

        // The sync push completed before the ack, so the sync pushed offset must have advanced to
        // the write log end offset. A single row at log offset 0 advances it to 1.
        assertThat(f.mainLeaderReplica.getSyncIndexPushedOffset())
                .as(
                        "sync index pushed offset must equal the write log end offset after a sync write")
                .isEqualTo(1L);
    }

    /**
     * Scenario #2 (FIP V2 §2.5 update rule): an UPDATE that changes the indexed column emits a
     * DELETE for the old key plus an UPSERT for the new key. After a brief poll the old key must be
     * gone and the new key must be present.
     */
    @Test
    void testUpdateRewritesIndexEntry() throws Exception {
        String mainName = "main_t_update";
        Fixture f = setupTables(mainName, /* visibility */ null);

        // (1) Insert (a=1, b="hello"); wait for the index entry to land.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();
        byte[] oldIndexKey = encodeIndexKey("hello", 1);
        waitForIndexEntry(f.indexGateway, f.indexTableId, oldIndexKey, "hello", true);

        // (2) Update the same PK row with a different idx column value.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "world"})))
                .get();

        // (3) The old composite key disappears, the new one appears.
        byte[] newIndexKey = encodeIndexKey("world", 1);
        waitForIndexEntry(f.indexGateway, f.indexTableId, newIndexKey, "world", true);
        waitForIndexEntry(f.indexGateway, f.indexTableId, oldIndexKey, "hello", false);
    }

    /**
     * Scenario #3 (FIP V2 §2.5 delete rule): a DELETE on the main table emits a DELETE on the index
     * entry. After polling, the index lookup for the previously-inserted key must come back empty.
     */
    @Test
    void testDeleteRemovesIndexEntry() throws Exception {
        String mainName = "main_t_delete";
        Fixture f = setupTables(mainName, /* visibility */ null);

        // (1) Insert a row, wait for the index entry to land.
        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();
        byte[] indexKey = encodeIndexKey("hello", 1);
        waitForIndexEntry(f.indexGateway, f.indexTableId, indexKey, "hello", true);

        // (2) Delete the row by sending a KvRecord with a null value (tombstone) for the same PK.
        KvRecordBatch deleteBatch =
                genKvRecordBatch(
                        Collections.singletonList(Tuple2.of(new Object[] {1}, /* value */ null)));
        f.mainGateway.putKv(newPutKvRequest(f.mainTableId, 0, 1, deleteBatch)).get();

        // (3) The index entry for ("hello", 1) goes away.
        waitForIndexEntry(f.indexGateway, f.indexTableId, indexKey, "hello", false);
    }

    @Test
    void testEmptyWalBatchAdvancesSyncIndexOffset() throws Exception {
        String mainName = "main_t_empty_wal_batch";
        Fixture f = setupTables(mainName, /* visibility */ null);

        KvRecordBatch deleteMissingKey =
                genKvRecordBatch(
                        Collections.singletonList(Tuple2.of(new Object[] {404}, /* value */ null)));

        f.mainGateway
                .putKv(newPutKvRequest(f.mainTableId, 0, 1, deleteMissingKey))
                .get(30, TimeUnit.SECONDS);

        assertThat(f.mainLeaderReplica.getSyncIndexPushedOffset())
                .as("empty WAL batches must advance index progress to the batch next offset")
                .isEqualTo(1L);
    }

    /**
     * Scenario #4 (FIP V2 §2.6, async visibility): with async {@code Schema.Index} visibility the
     * PutKv ack returns BEFORE the index push completes; the entry is only eventually visible. We
     * don't assert immediate invisibility (would be flaky on fast machines / single-process
     * self-RPC) — the contract under test is "eventual visibility under ASYNC".
     */
    @Test
    void testAsyncVisibilityEventuallyVisible() throws Exception {
        String mainName = "main_t_async";
        Fixture f = setupTables(mainName, IndexVisibility.ASYNC);

        f.mainGateway
                .putKv(
                        newPutKvRequest(
                                f.mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();

        byte[] indexKey = encodeIndexKey("hello", 1);
        waitForIndexEntry(f.indexGateway, f.indexTableId, indexKey, "hello", true);
    }

    /**
     * Scenario #5 (Plan 3 §2.9.8, partition tombstone): once the TabletServer's metadata cache
     * holds a tombstone for a given {@code (mainTableId, partitionId)} pair, any subsequent Index
     * Table PutKv whose physical value carries that partitionId must be silently dropped by the
     * apply-path filter — no KV state change, no error, the surrounding batch is still acked
     * normally.
     *
     * <p><b>Pragmatic scoping (per P3T13 spec):</b> driving the full multi-partition + drop +
     * propagation chain through {@link FlussClusterExtension} is fixture-heavy and largely covered
     * by per-component tests (advancer, JSON serde, ZK persistence, propagation, filter). This
     * ITCase therefore exercises the end-to-end apply-path drop in-process by:
     *
     * <ol>
     *   <li>asserting a live partition entry lands in the Index Table (apply path is healthy);
     *   <li>injecting a tombstone for the {@code mainTableId} back-link the Index Table replica was
     *       constructed with directly into the TabletServer's metadata cache (simulating the
     *       Coordinator → TabletServer propagation that P3T6/P3T7 already cover);
     *   <li>sending a synthetic PutKv straight to the Index Table whose value bytes carry the
     *       tombstoned partitionId — bypassing the main table to control the wire bytes exactly the
     *       way a partitioned production push would shape them;
     *   <li>asserting the synthetic key is invisible (filter dropped), a second live partition is
     *       visible, and the original live entry remains visible.
     * </ol>
     */
    @Test
    void testDroppedPartitionEntriesAreFilteredFromIndex() throws Exception {
        String mainName = "main_t_tombstone";
        Fixture f = setupPartitionedTables(mainName);

        // (1) Prove a live partition entry reaches the Index Table before installing a tombstone.
        byte[] controlIndexKey =
                putPartitionedIndexEntry(f, "hello", 1, "p-live-before", 1001L, 1L);
        waitForIndexEntry(f.indexGateway, f.indexTableId, controlIndexKey, "hello", true);

        // (2) Inject a tombstone into the TabletServer's metadata cache.
        int indexLeaderServerId =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(f.indexTableId, 0));
        TabletServerMetadataCache cache =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(indexLeaderServerId).getMetadataCache();
        final long mainTableIdBackLink = f.mainTableId;
        final long droppedPartitionId = 4242L;
        try {
            cache.updatePartitionTombstone(
                    mainTableIdBackLink,
                    new PartitionTombstone(
                            /* floor */ -1L,
                            Collections.singleton(droppedPartitionId),
                            /* version */ 1L));
            assertThat(
                            cache.getPartitionTombstone(mainTableIdBackLink)
                                    .isTombstoned(droppedPartitionId))
                    .as("tombstone is observable on the Index Table leader's metadata cache")
                    .isTrue();

            // (3) Send a fenced PutKv directly to the Index Table with a physical row carrying the
            // tombstoned partition ID, matching the production partitioned index-push shape.
            byte[] droppedIndexKey =
                    putPartitionedIndexEntry(f, "dropped", 99, "p-dropped", droppedPartitionId, 1L);

            // (4) The filter dropped the record silently (no WAL append, no KV state change), so
            // the lookup for the synthetic key must return empty.
            waitForIndexEntry(f.indexGateway, f.indexTableId, droppedIndexKey, "dropped", false);

            // (5) Send a non-tombstoned entry and verify it survives the filter.
            byte[] liveKey = putPartitionedIndexEntry(f, "alive", 77, "p-live-after", 9999L, 1L);
            waitForIndexEntry(f.indexGateway, f.indexTableId, liveKey, "alive", true);
            waitForIndexEntry(f.indexGateway, f.indexTableId, controlIndexKey, "hello", true);
        } finally {
            // Reset the tombstone for this test's main-table id so any later interaction with
            // the same cache instance sees a clean state.
            cache.updatePartitionTombstone(mainTableIdBackLink, PartitionTombstone.EMPTY);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    /** Per-scenario state: ids, paths, gateways, and the main-table leader replica handle. */
    private static final class Fixture {
        final long mainTableId;
        final long indexTableId;
        final Replica mainLeaderReplica;
        final TabletServerGateway mainGateway;
        final TabletServerGateway indexGateway;

        Fixture(
                long mainTableId,
                long indexTableId,
                Replica mainLeaderReplica,
                TabletServerGateway mainGateway,
                TabletServerGateway indexGateway) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.mainLeaderReplica = mainLeaderReplica;
            this.mainGateway = mainGateway;
            this.indexGateway = indexGateway;
        }
    }

    /**
     * Creates the main table only — the Coordinator auto-derives the matching Index Table as part
     * of {@code processCreateTable} (P3T8). The {@code IndexReplicator} init on the main-table
     * leader uses the defer-and-retry path (P3T14): if {@code NotifyLeaderAndIsr} for the main
     * table reaches the TabletServer before the auto-derived Index Table's metadata broadcast, the
     * scheduler init is deferred and retried once the cache catches up. This helper polls until the
     * scheduler is wired so the rest of the test can proceed deterministically.
     */
    private static Fixture setupTables(String mainName, @Nullable IndexVisibility visibility)
            throws Exception {
        TablePath mainPath = TablePath.of(DB, mainName);
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName(mainName, INDEX_NAME));

        TableDescriptor mainDescriptor = buildMainTableDescriptor(visibility);

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, mainDescriptor);
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        Replica mainLeaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int mainLeaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);

        // Look up the auto-derived Index Table id directly from ZK — the Coordinator persisted
        // it during {@code processCreateTable} (P3T8) before returning.
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Auto-derived Index Table "
                                                        + indexPath
                                                        + " was not registered in ZK."))
                        .tableId;
        // Wait for ALL index bucket leaders to be ready (the push may target any bucket).
        for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        // The IndexReplicator may be deferred at the moment NotifyLeaderAndIsr arrives if the
        // index-table broadcast lost the race; the cache update fires the retry hook so the
        // scheduler is eventually wired. Poll until that completes.
        waitUntil(
                () -> mainLeaderReplica.getIndexManager().getIndexReplicator() != null,
                Duration.ofSeconds(30),
                "wait for IndexReplicator to be wired on the main-table leader after"
                        + " auto-derived Index Table metadata propagates");

        // Ensure the metadata cache on the main-table leader server has all index bucket leaders
        // resolved — the IndexReplicator needs getBucketLeader to return non-empty for the target
        // bucket, otherwise the push silently fails.
        final long idxTableId = indexTableId;
        TabletServerMetadataCache mainServerCache =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(mainLeaderServer).getMetadataCache();
        waitUntil(
                () -> {
                    for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
                        if (!mainServerCache.getBucketLeader(idxTableId, i).isPresent()) {
                            return false;
                        }
                    }
                    return true;
                },
                Duration.ofSeconds(30),
                "wait for all index bucket leaders to be resolved in metadata cache");

        TabletServerGateway mainGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(mainLeaderServer);
        TabletServerGateway indexGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, 0)));
        return new Fixture(mainTableId, indexTableId, mainLeaderReplica, mainGateway, indexGateway);
    }

    /**
     * Sets up a partitioned main table so its auto-derived Index Table includes the {@code
     * __partition_id} system column and uses {@code KvFormat.ALIGNED}. Required for testing the
     * partition tombstone value filter which only installs on partitioned Index Tables.
     */
    private static Fixture setupPartitionedTables(String mainName) throws Exception {
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

        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Auto-derived Index Table "
                                                        + indexPath
                                                        + " was not registered in ZK."))
                        .tableId;
        for (int i = 0; i < INDEX_BUCKET_COUNT; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        TabletServerGateway indexGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, 0)));

        return new Fixture(mainTableId, indexTableId, null, null, indexGateway);
    }

    /**
     * Polls the Index Table until the lookup for {@code key} matches the expected presence.
     * "Present" means the bucket response contains a value with non-null payload; "absent" means
     * the bucket returns no value or returns a tombstone.
     */
    private static final int INDEX_BUCKET_COUNT = 3;

    /** Bucket key type for the Index Table — only the indexed column(s), matching production. */
    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));

    private static void waitForIndexEntry(
            TabletServerGateway indexLeaderGateway,
            long indexTableId,
            byte[] key,
            String bValue,
            boolean expectPresent) {
        int bucketId = computeIndexBucket(bValue);
        TableBucket tb = new TableBucket(indexTableId, bucketId);
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        String desc =
                expectPresent
                        ? "wait for index entry to be visible on the Index Table"
                        : "wait for index entry to disappear from the Index Table";
        waitUntil(
                () -> {
                    try {
                        List<byte[]> result = replica.lookups(Collections.singletonList(key));
                        boolean present = result.get(0) != null;
                        return present == expectPresent;
                    } catch (Exception e) {
                        return false;
                    }
                },
                INDEX_VISIBILITY_TIMEOUT,
                desc);
    }

    private static Schema buildMainSchema(@Nullable IndexVisibility visibility) {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(
                        INDEX_NAME,
                        IndexType.SECONDARY,
                        Collections.singletonList("b"),
                        visibility == null ? IndexVisibility.SYNC : visibility,
                        3)
                .build();
    }

    private static TableDescriptor buildMainTableDescriptor(@Nullable IndexVisibility visibility) {
        return TableDescriptor.builder()
                .schema(buildMainSchema(visibility))
                .distributedBy(3, "a")
                .build();
    }

    /**
     * Encode the composite Index Table PK {@code (b, a)} the same way the production {@code
     * IndexMutations.KeyEncoder} does — a {@link CompactedKeyEncoder} over the derived index value
     * row type.
     */
    private static byte[] encodeIndexKey(String b, int a) {
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(INDEX_VALUE_ROW_TYPE);
        GenericRow row = new GenericRow(2);
        row.setField(0, fromString(b));
        row.setField(1, a);
        return encoder.encodeKey(row);
    }

    /**
     * Compute the target index bucket for a given index column value. The index table's bucket key
     * is the index columns only (just "b"), matching production's {@code
     * FlussBucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), indexBucketCount)}.
     */
    private static int computeIndexBucket(String bValue) {
        CompactedKeyEncoder bucketKeyEncoder = new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE);
        GenericRow row = new GenericRow(1);
        row.setField(0, fromString(bValue));
        byte[] bucketKey = bucketKeyEncoder.encodeKey(row);
        return new FlussBucketingFunction().bucketing(bucketKey, INDEX_BUCKET_COUNT);
    }

    private static AlignedRow encodePartitionedIndexValue(
            String b, int a, String p, long partitionId) {
        AlignedRow row = new AlignedRow(PARTITIONED_INDEX_VALUE_ROW_TYPE.getFieldCount());
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.reset();
        writer.writeString(0, fromString(b));
        writer.writeInt(1, a);
        writer.writeString(2, fromString(p));
        writer.writeLong(3, partitionId);
        writer.complete();
        return row;
    }

    /**
     * Writes one physical partitioned Index Table row through the protocol-V1 tablet path and
     * returns its complete physical key.
     */
    private static byte[] putPartitionedIndexEntry(
            Fixture fixture, String b, int a, String p, long partitionId, long sequence)
            throws IOException {
        AlignedRow row = encodePartitionedIndexValue(b, a, p, partitionId);
        byte[] key = new CompactedKeyEncoder(PARTITIONED_INDEX_VALUE_ROW_TYPE).encodeKey(row);
        int targetBucket = computeIndexBucket(b);
        TableBucket sourceBucket = new TableBucket(fixture.mainTableId, partitionId, 0);

        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(4096),
                        KvFormat.ALIGNED)) {
            builder.append(key, row);
            builder.setWriterState(IndexWriterKey.encode(sourceBucket), sequence);
            BytesView batch = builder.build();

            PutKvRequest request =
                    new PutKvRequest()
                            .setTableId(fixture.indexTableId)
                            .setAcks(-1)
                            .setTimeoutMs(10_000);
            request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
            PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(targetBucket);
            bucketRequest.setRecordsBytesView(batch);
            fixture.indexGateway.putKv(request).join();
        }
        return key;
    }
}
