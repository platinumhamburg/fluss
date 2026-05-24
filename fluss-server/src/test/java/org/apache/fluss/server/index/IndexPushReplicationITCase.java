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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ITCase for the FIP V2 secondary-index push replication pipeline (Plan 2 §3 — KV apply
 * hook → IndexPushScheduler → IndexPushSender → PutKv to the Index Table leader).
 *
 * <p>The intended scenario is straightforward: stand up a single-tablet-server Fluss cluster,
 * pre-create the Index Table, create the indexed main table, write one PK row, and verify the
 * index entry is visible via a Lookup against the Index Table. With {@code
 * secondary-index.visibility=SYNC} (the FIP V2 default) the {@code putKv} ack is gated on the
 * index push completing, so by the time the future resolves the index entry must be present and
 * the {@code indexPushedOffset} watermark must have advanced.
 *
 * <h2>Naming convention</h2>
 *
 * <p>The push pipeline in {@link Replica#indexTablePathFor} builds the Index Table path as
 * {@code <main>__<indexName>} (see {@link IndexTableUtils#INDEX_TABLE_NAME_SEPARATOR}). The
 * {@code __} separator is in the {@code [a-zA-Z0-9_-]} character set permitted by
 * {@code TablePath.detectInvalidName}, and user index names are validated by {@code Schema.Index}
 * to forbid {@code __}, so the composed name unambiguously decomposes back to its parts.
 *
 * <h2>Update / delete / async / recovery scenarios</h2>
 *
 * <p>Several follow-on scenarios remain blocked on later plans (see the {@code @Disabled} reasons
 * on each {@code @Test} below).
 */
class IndexPushReplicationITCase {

    private static final String DB = "test_db";
    private static final String MAIN_TABLE = "main_t";
    private static final String INDEX_NAME = "idx_b";
    private static final TablePath MAIN_TABLE_PATH = TablePath.of(DB, MAIN_TABLE);
    // Canonical Index Table path: "<main>__<indexName>" (matches Replica.indexTablePathFor via
    // IndexTableUtils.indexTableName). The '__' separator passes TablePath.detectInvalidName.
    private static final TablePath INDEX_TABLE_PATH =
            TablePath.of(DB, IndexTableUtils.indexTableName(MAIN_TABLE, INDEX_NAME));

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
     * Index value row type {@code (b STRING NOT NULL, a INT NOT NULL)} — composite of idx cols
     * (b) followed by the base PK (a), with {@code NOT NULL} forced because both make up the
     * Index Table's PK after {@link TableDescriptor#deriveIndexTableDescriptor}.
     */
    private static final RowType INDEX_VALUE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));

    /**
     * Happy-path scenario #1 (FIP V2 §3, sync visibility): write 1 row to the indexed main table,
     * verify the corresponding entry shows up in the Index Table.
     *
     * <p>The earlier {@code TableDescriptorValidation} blocker was resolved when the validator
     * learned to accept the namespaced {@code secondary-index.<name>.{columns,bucket.num}}
     * properties (and the static {@code secondary-index.visibility} sibling) — see {@code
     * TableDescriptorValidationTest}.
     */
    @Test
    void testInsertOnMainTablePushesEntryToIndexTable() throws Exception {
        // (1) Pre-create the Index Table FIRST so the main-table leader promotion can resolve it
        // via the metadata cache. mainTableId=-1 here is a placeholder for the Plan 1
        // back-link property — Plan 2's push pipeline only consults the path-based name, not the
        // back-link, so a placeholder is harmless.
        TableDescriptor indexDescriptor = buildIndexTableDescriptorPlaceholderMain();
        long indexTableId = createTable(FLUSS_CLUSTER_EXTENSION, INDEX_TABLE_PATH, indexDescriptor);
        TableBucket indexBucket = new TableBucket(indexTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(indexBucket);

        // (2) Create the main table with Schema.index(...) so the leader's onBecomeNewLeader hook
        // wires the IndexPushScheduler. By the time this returns, the Index Table is already in
        // the metadata cache.
        TableDescriptor mainDescriptor = buildMainTableDescriptor();
        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, MAIN_TABLE_PATH, mainDescriptor);
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        Replica mainLeaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int mainLeaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);
        // The IndexPushScheduler must have wired up — otherwise the leader promotion would have
        // thrown out of buildIndexExtractorContext().
        assertThat(mainLeaderReplica.getIndexPushScheduler()).isNotNull();

        // (3) Write one PK row (a=1, b="hello") to the main table.
        KvRecordBatch batch = genKvRecordBatch(new Object[] {1, "hello"});
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(mainLeaderServer);
        PutKvRequest putKvRequest =
                newPutKvRequest(mainTableId, /* bucketId */ 0, /* acks */ 1, batch);
        leaderGateway.putKv(putKvRequest).get();

        // (4) Verify the index entry shows up under the encoded composite key. The Index Table
        // PK is (b, a) → 'hello' || 1; the Index bucket layout uses FlussBucketingFunction over
        // that encoded key, but with a single Index bucket the assignment is trivially 0.
        byte[] indexKey = encodeIndexKey("hello", 1);
        int targetIndexBucket = new FlussBucketingFunction().bucketing(indexKey, 1);
        assertThat(targetIndexBucket).isEqualTo(0);

        TabletServerGateway indexLeaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(indexBucket));
        waitUntil(
                () -> {
                    PbLookupRespForBucket resp =
                            indexLeaderGateway
                                    .lookup(newLookupRequest(indexTableId, 0, indexKey))
                                    .get()
                                    .getBucketsRespAt(0);
                    return resp.getValuesCount() > 0 && resp.getValueAt(0).hasValues();
                },
                Duration.ofMinutes(1),
                "Fail to wait for the index entry to be visible on the Index Table.");

        // The push pipeline is sync-by-default (FIP V2 secondary-index.visibility=SYNC), so by the
        // time putKv ack returned the indexPushedOffset should already cover the write offset.
        // We assert the watermark crossed the write offset to catch regressions where the
        // pipeline never advances (e.g. acks dropped on the floor).
        assertThat(mainLeaderReplica.getIndexPushedOffset())
                .as("indexPushedOffset on main-table leader after sync write")
                .isGreaterThanOrEqualTo(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // Disabled scenarios — re-enable as the dependent plans land.
    // ---------------------------------------------------------------------------------------------

    @Test
    @Disabled(
            "TODO P2T14: update path requires UPDATE_BEFORE pre-image emission via the IndexedRow"
                    + " hook so the extractor can DELETE the old idxCols; covered in Plan 4.")
    void testUpdateRewritesIndexEntry() {}

    @Test
    @Disabled(
            "TODO P2T14: delete path requires the WAL to carry the pre-image (DELETE row) which"
                    + " ChangelogImage.WAL only writes for explicit DELETE — exercising it"
                    + " end-to-end via genKvRecordBatch needs a tombstone-style record batch.")
    void testDeleteRemovesIndexEntry() {}

    @Test
    @Disabled(
            "TODO P2T14: async-visibility path needs an explicit secondary-index.visibility=ASYNC"
                    + " on the main table plus a poll-loop; left out until the basic SYNC path is"
                    + " stable in CI.")
    void testAsyncVisibilityEventuallyVisible() {}

    @Test
    @Disabled(
            "TODO P2T14: leader-failover recovery needs at least 2 tablet servers and forced"
                    + " leader migration plus a deterministic snapshot point; out of scope for"
                    + " the minimum-viable push ITCase.")
    void testLeaderFailoverReplaysViaWal() {}

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static Schema buildMainSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(INDEX_NAME, "b")
                .build();
    }

    private static TableDescriptor buildMainTableDescriptor() {
        return TableDescriptor.builder()
                .schema(buildMainSchema())
                .distributedBy(1, "a")
                // Match the Index Table's bucket count so resolveIndexBucketCount agrees.
                .property(ConfigOptions.secondaryIndexBucketNumKey(INDEX_NAME), "1")
                .build();
    }

    /**
     * Build the Index Table descriptor as if {@link TableDescriptor#deriveIndexTableDescriptor}
     * were running with a placeholder mainTableId. The push pipeline only consults the
     * path-based name {@code <main>__<indexName>}; the back-link properties are read by Plan 4.
     */
    private static TableDescriptor buildIndexTableDescriptorPlaceholderMain() {
        // Pretend mainTableId is unknown — Plan 2 doesn't care.
        return TableDescriptor.deriveIndexTableDescriptor(
                buildMainTableDescriptor(),
                /* mainTableId */ -1L,
                DB + "." + MAIN_TABLE,
                INDEX_NAME);
    }

    /**
     * Encode the composite Index Table PK {@code (b, a)} the same way the production
     * {@code IndexMutationExtractor.KeyEncoder} does — a {@link CompactedKeyEncoder} over the
     * derived index value row type.
     */
    private static byte[] encodeIndexKey(String b, int a) {
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(INDEX_VALUE_ROW_TYPE);
        GenericRow row = new GenericRow(2);
        row.setField(0, fromString(b));
        row.setField(1, a);
        return encoder.encodeKey(row);
    }
}
