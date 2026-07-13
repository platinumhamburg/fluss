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
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.compacted.CompactedKeyWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.KvEntry;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Value;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.UnsafeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;

/** Independent mutation-window model checked against the production V1 target apply path. */
class IndexPushModelTest {

    private static final int SEEDS = 200;
    private static final int SOURCE_OPERATIONS = 200;
    private static final int DROP_OFFSET = 100;
    private static final int KEYS_PER_INCARNATION = 4;
    private static final int[] ALL_TARGET_COLUMNS = {0, 1, 2, 3};
    private static final short SCHEMA_ID = 1;
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                    DataTypes.FIELD("order_id", DataTypes.BIGINT()),
                    DataTypes.FIELD("dt", DataTypes.STRING()),
                    DataTypes.FIELD(
                            IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN, DataTypes.BIGINT()));

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
    void testMutationWindowsRecoveryAndIncarnations() throws Exception {
        Fixture fixture = createFixture();
        Map<WriterKey, WriterStateView> committedWriterStates = new HashMap<>();
        Map<String, String> globalReferenceRows = new HashMap<>();
        List<byte[]> allPhysicalKeys = new ArrayList<>();
        Set<Long> tombstonedPartitions = new HashSet<>();

        fixture.publishTombstone(PartitionTombstone.EMPTY);
        for (int seed = 0; seed < SEEDS; seed++) {
            runSeed(
                    fixture,
                    seed,
                    committedWriterStates,
                    globalReferenceRows,
                    allPhysicalKeys,
                    tombstonedPartitions);
        }
    }

    private static void runSeed(
            Fixture fixture,
            int seed,
            Map<WriterKey, WriterStateView> committedWriterStates,
            Map<String, String> globalReferenceRows,
            List<byte[]> allPhysicalKeys,
            Set<Long> tombstonedPartitions)
            throws Exception {
        SourceHistory history = SourceHistory.generate(fixture.mainTableId, seed);
        assertThat(history.mutations)
                .as("seed=%s source operations", seed)
                .hasSize(SOURCE_OPERATIONS);
        allPhysicalKeys.addAll(history.physicalKeys());
        EnumSet<EventFamily> coverage = EnumSet.noneOf(EventFamily.class);

        Delivery first = history.window("first fresh", 0, 1);
        fixture.applyFresh(first, committedWriterStates, true, allPhysicalKeys, seed);
        coverage.add(EventFamily.FRESH_DELIVERY);

        Delivery sameValue = history.window("same-value advance", 1, 2);
        fixture.applyFresh(sameValue, committedWriterStates, true, allPhysicalKeys, seed);
        fixture.applyNoOp(
                first, ExpectedOutcome.STALE, committedWriterStates, allPhysicalKeys, seed);
        coverage.add(EventFamily.SAME_VALUE_STALE_UPSERT);

        Random random = new Random(seed);
        int lostEnd = 8 + random.nextInt(12);
        Delivery lostResponse = history.window("lost response", 2, lostEnd);
        fixture.applyFresh(lostResponse, committedWriterStates, true, allPhysicalKeys, seed);
        // The successful result is deliberately not used as source progress. The identical delta
        // is retried as it would be after a lost response.
        fixture.applyNoOp(
                lostResponse, ExpectedOutcome.STALE, committedWriterStates, allPhysicalKeys, seed);
        coverage.add(EventFamily.EXACT_DUPLICATE_AFTER_LOST_RESPONSE);

        int laterEnd = 45 + random.nextInt(20);
        int earlierEnd = lostEnd + 1 + random.nextInt(laterEnd - lostEnd - 1);
        Delivery later = history.window("later window first", earlierEnd, laterEnd);
        fixture.applyFresh(later, committedWriterStates, true, allPhysicalKeys, seed);
        Delivery reordered = history.window("reordered earlier window", lostEnd, earlierEnd);
        fixture.applyNoOp(
                reordered, ExpectedOutcome.STALE, committedWriterStates, allPhysicalKeys, seed);
        coverage.add(EventFamily.OUT_OF_ORDER_DELIVERY);

        Delivery oldReplay = history.window("changed-boundary old replay", 0, DROP_OFFSET);
        assertThat(oldReplay.mutations)
                .as("seed=%s old replay is a mutation delta", seed)
                .containsExactlyElementsOf(history.mutations.subList(0, DROP_OFFSET));
        fixture.applyFresh(oldReplay, committedWriterStates, true, allPhysicalKeys, seed);
        coverage.add(EventFamily.CHANGED_WINDOW_BOUNDARIES);

        fixture.recoverWriterState(committedWriterStates, seed);
        coverage.add(EventFamily.TARGET_RESTART_RECOVERY);

        tombstonedPartitions.add(history.oldIncarnation.partitionId);
        fixture.publishTombstone(
                new PartitionTombstone(-1L, new HashSet<>(tombstonedPartitions), seed + 1L));
        committedWriterStates.remove(history.writerKey(history.oldIncarnation));
        fixture.assertWriterStates(committedWriterStates, seed);
        assertThat(history.newIncarnation.partitionId)
                .as("seed=%s recreated partition incarnation", seed)
                .isNotEqualTo(history.oldIncarnation.partitionId);
        assertThat(history.newIncarnation.generation)
                .as("seed=%s recreated partition generation", seed)
                .isGreaterThan(history.oldIncarnation.generation);
        assertThat(OracleProjection.project(history.logicalRow(history.oldIncarnation, 0)).key)
                .as("seed=%s physical keys include incarnation", seed)
                .isNotEqualTo(
                        OracleProjection.project(history.logicalRow(history.newIncarnation, 0))
                                .key);
        coverage.add(EventFamily.PARTITION_DROP_RECREATE);

        fixture.applyNoOp(
                history.singleMutation("delayed old UPSERT", 2),
                ExpectedOutcome.TOMBSTONED,
                committedWriterStates,
                allPhysicalKeys,
                seed);
        coverage.add(EventFamily.DELAYED_OLD_UPSERT);
        fixture.applyNoOp(
                history.singleMutation("delayed old DELETE", 3),
                ExpectedOutcome.TOMBSTONED,
                committedWriterStates,
                allPhysicalKeys,
                seed);
        coverage.add(EventFamily.DELAYED_OLD_DELETE);

        Delivery newCommitted = history.window("new incarnation baseline", DROP_OFFSET, 120);
        fixture.applyFresh(newCommitted, committedWriterStates, true, allPhysicalKeys, seed);

        Delivery truncated = history.window("uncommitted target WAL", 120, 140);
        PhysicalState beforeUncommitted =
                fixture.physicalState(truncated.writerKey, allPhysicalKeys);
        long committedHighWatermark = fixture.log.getHighWatermark();
        assertThat(committedHighWatermark)
                .as("seed=%s committed high watermark before truncation", seed)
                .isEqualTo(beforeUncommitted.walEndOffset);
        fixture.applyFresh(truncated, committedWriterStates, false, allPhysicalKeys, seed);
        assertThat(fixture.log.getHighWatermark())
                .as("seed=%s uncommitted batch remains above high watermark", seed)
                .isEqualTo(committedHighWatermark);

        fixture.replica.truncateTo(beforeUncommitted.walEndOffset);
        fixture.kv
                .getKvPreWriteBuffer()
                .truncateTo(beforeUncommitted.walEndOffset, TruncateReason.ERROR);
        fixture.recoverWriterState(committedWriterStates, seed);
        PhysicalState afterTruncation = fixture.physicalState(truncated.writerKey, allPhysicalKeys);
        assertThat(afterTruncation)
                .as("seed=%s truncation removes uncommitted KV and WriterState", seed)
                .usingRecursiveComparison()
                .ignoringFields("prewriteMaxLsn")
                .isEqualTo(beforeUncommitted);
        assertThat(afterTruncation.prewriteMaxLsn)
                .as("seed=%s empty prewrite buffer resets its maximum LSN", seed)
                .isEqualTo(-1L);
        fixture.applyFresh(truncated, committedWriterStates, true, allPhysicalKeys, seed);
        coverage.add(EventFamily.UNCOMMITTED_WAL_TRUNCATION);

        fixture.applyNoOp(
                truncated, ExpectedOutcome.STALE, committedWriterStates, allPhysicalKeys, seed);
        coverage.add(EventFamily.STALE_REDELIVERY);

        Delivery settle =
                history.window("final source-delta replay", DROP_OFFSET, SOURCE_OPERATIONS);
        assertThat(settle.mutations)
                .as("seed=%s settle replay contains source deltas only", seed)
                .containsExactlyElementsOf(
                        history.mutations.subList(DROP_OFFSET, SOURCE_OPERATIONS));
        fixture.applyFresh(settle, committedWriterStates, true, allPhysicalKeys, seed);
        fixture.assertWriterStates(committedWriterStates, seed);

        fixture.kv.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        fixture.kv.getRocksDBKv().getDb().compactRange();
        globalReferenceRows.putAll(history.terminalRows);
        assertThat(fixture.actualRows(allPhysicalKeys))
                .as("seed=%s final physical index map", seed)
                .isEqualTo(globalReferenceRows);
        coverage.add(EventFamily.FINAL_EXACT_EQUALITY);

        assertThat(coverage)
                .as("seed=%s mandatory event coverage", seed)
                .containsExactlyInAnyOrderElementsOf(EnumSet.allOf(EventFamily.class));
    }

    private static Fixture createFixture() throws Exception {
        String tableName = "mutation_model_" + System.nanoTime();
        TablePath mainPath = TablePath.of("task12", tableName);
        TablePath indexPath =
                TablePath.of("task12", IndexTableUtils.indexTableName(tableName, "idx_user"));
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Collections.singletonList("user_id"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .partitionedBy("dt")
                                .distributedBy(1, "order_id")
                                .build());
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;
        TableBucket indexBucket = new TableBucket(indexTableId, 0);
        Replica replica = CLUSTER.waitAndGetLeaderReplica(indexBucket);
        int tabletServerId = CLUSTER.waitAndGetLeader(indexBucket);
        ReplicaManager replicaManager =
                CLUSTER.getTabletServerById(tabletServerId).getReplicaManager();
        return new Fixture(
                mainTableId,
                replica,
                replica.getKvTablet(),
                replica.getLogTablet(),
                replicaManager,
                replicaManager.getServerMetricGroup());
    }

    private enum MutationKind {
        UPSERT,
        DELETE
    }

    private enum ExpectedOutcome {
        STALE,
        TOMBSTONED
    }

    private enum EventFamily {
        FRESH_DELIVERY,
        EXACT_DUPLICATE_AFTER_LOST_RESPONSE,
        OUT_OF_ORDER_DELIVERY,
        CHANGED_WINDOW_BOUNDARIES,
        SAME_VALUE_STALE_UPSERT,
        TARGET_RESTART_RECOVERY,
        UNCOMMITTED_WAL_TRUNCATION,
        PARTITION_DROP_RECREATE,
        DELAYED_OLD_UPSERT,
        DELAYED_OLD_DELETE,
        STALE_REDELIVERY,
        FINAL_EXACT_EQUALITY
    }

    private static final class SourceHistory {
        private final long mainTableId;
        private final int seed;
        private final List<SourceMutation> mutations;
        private final SourceIncarnation oldIncarnation;
        private final SourceIncarnation newIncarnation;
        private final Map<String, String> terminalRows;

        private SourceHistory(
                long mainTableId,
                int seed,
                List<SourceMutation> mutations,
                SourceIncarnation oldIncarnation,
                SourceIncarnation newIncarnation,
                Map<String, String> terminalRows) {
            this.mainTableId = mainTableId;
            this.seed = seed;
            this.mutations = mutations;
            this.oldIncarnation = oldIncarnation;
            this.newIncarnation = newIncarnation;
            this.terminalRows = terminalRows;
        }

        private static SourceHistory generate(long mainTableId, int seed) {
            long oldPartitionId = 10_000L + seed * 2L;
            long newPartitionId = oldPartitionId + 1L;
            SourceIncarnation oldIncarnation = new SourceIncarnation(oldPartitionId, 0);
            SourceIncarnation newIncarnation = new SourceIncarnation(newPartitionId, 1);
            Random random = new Random(seed * 31L + 17L);
            List<SourceMutation> mutations = new ArrayList<>(SOURCE_OPERATIONS);
            for (int operation = 0; operation < SOURCE_OPERATIONS; operation++) {
                SourceIncarnation incarnation =
                        operation < DROP_OFFSET ? oldIncarnation : newIncarnation;
                int key = random.nextInt(KEYS_PER_INCARNATION);
                MutationKind kind =
                        random.nextBoolean() ? MutationKind.UPSERT : MutationKind.DELETE;
                if (operation == 0 || operation == 1) {
                    key = 0;
                    kind = MutationKind.UPSERT;
                } else if (operation == 2) {
                    key = 1;
                    kind = MutationKind.UPSERT;
                } else if (operation == 3) {
                    key = 1;
                    kind = MutationKind.DELETE;
                } else if (operation == DROP_OFFSET || operation == SOURCE_OPERATIONS - 1) {
                    key = operation == DROP_OFFSET ? 0 : 3;
                    kind = MutationKind.UPSERT;
                }
                mutations.add(
                        new SourceMutation(
                                operation + 1L,
                                seed,
                                key,
                                "logical-partition-" + seed,
                                incarnation,
                                kind));
            }

            Map<String, String> terminalRows = new HashMap<>();
            applyReference(mutations.subList(0, DROP_OFFSET), terminalRows);
            for (int key = 0; key < KEYS_PER_INCARNATION; key++) {
                SourceMutation logicalRow =
                        new SourceMutation(
                                0L,
                                seed,
                                key,
                                "logical-partition-" + seed,
                                oldIncarnation,
                                MutationKind.UPSERT);
                terminalRows.remove(OracleProjection.project(logicalRow).encodedKey);
            }
            applyReference(mutations.subList(DROP_OFFSET, SOURCE_OPERATIONS), terminalRows);
            return new SourceHistory(
                    mainTableId, seed, mutations, oldIncarnation, newIncarnation, terminalRows);
        }

        private static void applyReference(
                List<SourceMutation> mutations, Map<String, String> referenceRows) {
            for (SourceMutation mutation : mutations) {
                OraclePhysicalRow row = OracleProjection.project(mutation);
                if (mutation.kind == MutationKind.UPSERT) {
                    referenceRows.put(row.encodedKey, row.encodedValue);
                } else {
                    referenceRows.remove(row.encodedKey);
                }
            }
        }

        private Delivery window(String label, int startInclusive, int endExclusive) {
            assertThat(startInclusive).isLessThan(endExclusive);
            List<SourceMutation> delta =
                    new ArrayList<>(mutations.subList(startInclusive, endExclusive));
            SourceIncarnation incarnation = delta.get(0).incarnation;
            assertThat(delta)
                    .as("%s must not cross partition incarnations", label)
                    .allSatisfy(mutation -> assertThat(mutation.incarnation).isSameAs(incarnation));
            return new Delivery(label, endExclusive, writerKey(incarnation), delta);
        }

        private Delivery singleMutation(String label, int operationIndex) {
            SourceMutation mutation = mutations.get(operationIndex);
            return new Delivery(
                    label,
                    mutation.exclusiveOffset,
                    writerKey(mutation.incarnation),
                    Collections.singletonList(mutation));
        }

        private List<byte[]> physicalKeys() {
            Map<String, byte[]> keys = new LinkedHashMap<>();
            for (SourceIncarnation incarnation :
                    new SourceIncarnation[] {oldIncarnation, newIncarnation}) {
                for (int key = 0; key < KEYS_PER_INCARNATION; key++) {
                    SourceMutation logicalRow = logicalRow(incarnation, key);
                    byte[] productionKey = ProductionProjection.project(logicalRow).key;
                    byte[] oracleKey = OracleProjection.project(logicalRow).key;
                    keys.put(encode(productionKey), productionKey);
                    keys.put(encode(oracleKey), oracleKey);
                }
            }
            return new ArrayList<>(keys.values());
        }

        private SourceMutation logicalRow(SourceIncarnation incarnation, int key) {
            return new SourceMutation(
                    0L, seed, key, "logical-partition-" + seed, incarnation, MutationKind.UPSERT);
        }

        private WriterKey writerKey(SourceIncarnation incarnation) {
            return IndexWriterKey.encode(new TableBucket(mainTableId, incarnation.partitionId, 0));
        }
    }

    private static final class SourceIncarnation {
        private final long partitionId;
        private final int generation;

        private SourceIncarnation(long partitionId, int generation) {
            this.partitionId = partitionId;
            this.generation = generation;
        }
    }

    private static final class ProductionMutation {
        private final byte[] key;
        private final AlignedRow value;

        private ProductionMutation(byte[] key, AlignedRow value) {
            this.key = key;
            this.value = value;
        }
    }

    private static final class ProductionProjection {
        private static final CompactedKeyEncoder KEY_ENCODER =
                new CompactedKeyEncoder(INDEX_ROW_TYPE);

        private static ProductionMutation project(SourceMutation mutation) {
            AlignedRow row = new AlignedRow(4);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.reset();
            writer.writeLong(0, mutation.seed * 100L + mutation.key);
            writer.writeLong(1, mutation.seed * 100L + mutation.key);
            writer.writeString(2, fromString(mutation.partition));
            writer.writeLong(3, mutation.incarnation.partitionId);
            writer.complete();
            return new ProductionMutation(KEY_ENCODER.encodeKey(row), row);
        }
    }

    private static final class OraclePhysicalRow {
        private final byte[] key;
        private final byte[] value;
        private final String encodedKey;
        private final String encodedValue;

        private OraclePhysicalRow(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
            this.encodedKey = encode(key);
            this.encodedValue = encode(value);
        }
    }

    private static final class OracleProjection {
        private static OraclePhysicalRow project(SourceMutation mutation) {
            CompactedKeyWriter keyWriter = new CompactedKeyWriter();
            keyWriter.writeLong(mutation.seed * 100L + mutation.key);
            keyWriter.writeLong(mutation.seed * 100L + mutation.key);
            keyWriter.writeString(mutation.partition);
            keyWriter.writeLong(mutation.incarnation.partitionId);

            AlignedRow oracleRow = new AlignedRow(4);
            AlignedRowWriter rowWriter = new AlignedRowWriter(oracleRow);
            rowWriter.reset();
            rowWriter.writeLong(0, mutation.seed * 100L + mutation.key);
            rowWriter.writeLong(1, mutation.seed * 100L + mutation.key);
            rowWriter.writeString(2, fromString(mutation.partition));
            rowWriter.writeLong(3, mutation.incarnation.partitionId);
            rowWriter.complete();

            byte[] value = new byte[Short.BYTES + Long.BYTES + oracleRow.getSizeInBytes()];
            UnsafeUtils.putShort(value, 0, SCHEMA_ID);
            UnsafeUtils.putLong(value, Short.BYTES, mutation.incarnation.partitionId);
            oracleRow.copyTo(value, Short.BYTES + Long.BYTES);
            return new OraclePhysicalRow(keyWriter.toBytes(), value);
        }
    }

    private static final class SourceMutation {
        private final long exclusiveOffset;
        private final int seed;
        private final int key;
        private final String partition;
        private final SourceIncarnation incarnation;
        private final MutationKind kind;

        private SourceMutation(
                long exclusiveOffset,
                int seed,
                int key,
                String partition,
                SourceIncarnation incarnation,
                MutationKind kind) {
            this.exclusiveOffset = exclusiveOffset;
            this.seed = seed;
            this.key = key;
            this.partition = partition;
            this.incarnation = incarnation;
            this.kind = kind;
        }
    }

    private static final class Delivery {
        private final String label;
        private final long sequence;
        private final WriterKey writerKey;
        private final List<SourceMutation> mutations;

        private Delivery(
                String label, long sequence, WriterKey writerKey, List<SourceMutation> mutations) {
            this.label = label;
            this.sequence = sequence;
            this.writerKey = writerKey;
            this.mutations = mutations;
        }
    }

    private static final class WriterStateView {
        private final long sequence;
        private final long targetWalOffset;

        private WriterStateView(long sequence, long targetWalOffset) {
            this.sequence = sequence;
            this.targetWalOffset = targetWalOffset;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof WriterStateView)) {
                return false;
            }
            WriterStateView that = (WriterStateView) obj;
            return sequence == that.sequence && targetWalOffset == that.targetWalOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequence, targetWalOffset);
        }

        @Override
        public String toString() {
            return "WriterStateView{" + sequence + ", wal=" + targetWalOffset + '}';
        }
    }

    private static final class PhysicalState {
        private final long walEndOffset;
        private final Optional<WriterStateView> writerState;
        private final long prewriteMaxLsn;
        private final int prewriteEntryCount;
        private final Map<String, String> prewriteValues;
        private final Map<String, String> rows;

        private PhysicalState(
                long walEndOffset,
                Optional<WriterStateView> writerState,
                long prewriteMaxLsn,
                int prewriteEntryCount,
                Map<String, String> prewriteValues,
                Map<String, String> rows) {
            this.walEndOffset = walEndOffset;
            this.writerState = writerState;
            this.prewriteMaxLsn = prewriteMaxLsn;
            this.prewriteEntryCount = prewriteEntryCount;
            this.prewriteValues = prewriteValues;
            this.rows = rows;
        }
    }

    private static final class FreshExpectation {
        private final int effectiveMutationCount;
        private final long walEndOffset;
        private final long lastWalOffset;
        private final long prewriteMaxLsn;
        private final List<KvEntry> uncommittedEntries;
        private final Map<Key, KvEntry> uncommittedEntryMap;
        private final Map<String, String> prewriteValues;
        private final Map<String, String> rows;

        private FreshExpectation(
                int effectiveMutationCount,
                long walEndOffset,
                long lastWalOffset,
                long prewriteMaxLsn,
                List<KvEntry> uncommittedEntries,
                Map<Key, KvEntry> uncommittedEntryMap,
                Map<String, String> prewriteValues,
                Map<String, String> rows) {
            this.effectiveMutationCount = effectiveMutationCount;
            this.walEndOffset = walEndOffset;
            this.lastWalOffset = lastWalOffset;
            this.prewriteMaxLsn = prewriteMaxLsn;
            this.uncommittedEntries = uncommittedEntries;
            this.uncommittedEntryMap = uncommittedEntryMap;
            this.prewriteValues = prewriteValues;
            this.rows = rows;
        }

        private static FreshExpectation from(
                PhysicalState before, Delivery delivery, boolean presenceSensitiveMerge) {
            Map<String, String> rows = new HashMap<>(before.rows);
            List<KvEntry> entries = new ArrayList<>();
            Map<Key, KvEntry> entryMap = new HashMap<>();
            int effectiveMutations = 0;

            for (SourceMutation mutation : delivery.mutations) {
                OraclePhysicalRow projected = OracleProjection.project(mutation);
                boolean wasPresent = rows.containsKey(projected.encodedKey);
                boolean effective;
                if (mutation.kind == MutationKind.UPSERT) {
                    effective =
                            !presenceSensitiveMerge
                                    || !Objects.equals(
                                            rows.get(projected.encodedKey), projected.encodedValue);
                    rows.put(projected.encodedKey, projected.encodedValue);
                } else {
                    effective = rows.remove(projected.encodedKey) != null;
                }
                if (!effective) {
                    continue;
                }

                Key key = Key.of(projected.key);
                Value value =
                        Value.of(mutation.kind == MutationKind.UPSERT ? projected.value : null);
                ChangeType changeType =
                        mutation.kind == MutationKind.UPSERT
                                ? (wasPresent ? ChangeType.UPDATE_AFTER : ChangeType.INSERT)
                                : ChangeType.DELETE;
                long lsn = before.walEndOffset + effectiveMutations;
                KvEntry previous = entryMap.get(key);
                KvEntry entry =
                        previous == null
                                ? KvEntry.of(changeType, key, value, lsn)
                                : KvEntry.of(changeType, key, value, lsn, previous);
                entries.add(entry);
                entryMap.put(key, entry);
                effectiveMutations++;
            }

            int walAdvance = Math.max(1, effectiveMutations);
            long lastWalOffset = before.walEndOffset + walAdvance - 1L;
            long prewriteMaxLsn =
                    effectiveMutations == 0
                            ? before.prewriteMaxLsn
                            : before.walEndOffset + effectiveMutations - 1L;
            Map<String, String> prewriteValues = new LinkedHashMap<>();
            entryMap.forEach(
                    (key, entry) ->
                            prewriteValues.put(
                                    encode(key.get()),
                                    entry.getValue().get() == null
                                            ? "<DELETE>"
                                            : encode(entry.getValue().get())));
            return new FreshExpectation(
                    effectiveMutations,
                    lastWalOffset + 1L,
                    lastWalOffset,
                    prewriteMaxLsn,
                    entries,
                    entryMap,
                    prewriteValues,
                    rows);
        }
    }

    private static final class Fixture {
        private final long mainTableId;
        private final Replica replica;
        private final KvTablet kv;
        private final LogTablet log;
        private final ReplicaManager replicaManager;
        private final TabletServerMetricGroup metrics;

        private Fixture(
                long mainTableId,
                Replica replica,
                KvTablet kv,
                LogTablet log,
                ReplicaManager replicaManager,
                TabletServerMetricGroup metrics) {
            this.mainTableId = mainTableId;
            this.replica = replica;
            this.kv = kv;
            this.log = log;
            this.replicaManager = replicaManager;
            this.metrics = metrics;
        }

        private void applyFresh(
                Delivery delivery,
                Map<WriterKey, WriterStateView> committedWriterStates,
                boolean throughReplica,
                List<byte[]> allPhysicalKeys,
                int seed)
                throws Exception {
            PhysicalState before = physicalState(delivery.writerKey, allPhysicalKeys);
            assertThat(before.prewriteEntryCount)
                    .as("%s starts with a flushed prewrite list", context(seed, delivery))
                    .isZero();
            assertThat(before.prewriteValues)
                    .as("%s starts with no pending prewrite values", context(seed, delivery))
                    .isEmpty();
            assertThat(kv.getKvPreWriteBuffer().getKvEntryMap())
                    .as("%s starts with a flushed prewrite map", context(seed, delivery))
                    .isEmpty();
            FreshExpectation expected = FreshExpectation.from(before, delivery, !throughReplica);
            long staleBefore = metrics.indexPushStaleV1Batches().getCount();
            long tombstoneBefore = metrics.indexPushTombstoneNoOpBatches().getCount();
            LogAppendInfo result = apply(delivery, throughReplica);
            PhysicalState after = physicalState(delivery.writerKey, allPhysicalKeys);

            assertThat(result.hasNoAppend()).as(context(seed, delivery)).isFalse();
            assertThat(result.duplicated()).as(context(seed, delivery)).isFalse();
            assertThat(result.firstOffset())
                    .as(context(seed, delivery))
                    .isEqualTo(before.walEndOffset);
            assertThat(result.lastOffset())
                    .as(context(seed, delivery))
                    .isEqualTo(expected.lastWalOffset);
            assertThat(result.numMessages())
                    .as("%s exact WAL advancement", context(seed, delivery))
                    .isEqualTo(Math.max(1, expected.effectiveMutationCount));
            assertThat(after.walEndOffset)
                    .as(context(seed, delivery))
                    .isEqualTo(expected.walEndOffset);
            assertThat(after.writerState)
                    .as(context(seed, delivery))
                    .contains(new WriterStateView(delivery.sequence, expected.lastWalOffset));
            assertThat(after.prewriteMaxLsn)
                    .as(context(seed, delivery))
                    .isEqualTo(expected.prewriteMaxLsn);
            assertThat(after.rows)
                    .as("%s exact physical rows", context(seed, delivery))
                    .isEqualTo(throughReplica ? expected.rows : before.rows);
            assertThat(metrics.indexPushStaleV1Batches().getCount())
                    .as(context(seed, delivery))
                    .isEqualTo(staleBefore);
            assertThat(metrics.indexPushTombstoneNoOpBatches().getCount())
                    .as(context(seed, delivery))
                    .isEqualTo(tombstoneBefore);

            if (throughReplica) {
                assertThat(log.getHighWatermark())
                        .as("%s committed high watermark", context(seed, delivery))
                        .isEqualTo(expected.walEndOffset);
                assertThat(kv.getKvPreWriteBuffer().getAllKvEntries())
                        .as("%s committed prewrite entries are flushed", context(seed, delivery))
                        .isEmpty();
                assertThat(kv.getKvPreWriteBuffer().getKvEntryMap())
                        .as("%s committed prewrite map is flushed", context(seed, delivery))
                        .isEmpty();
                assertThat(after.prewriteEntryCount).as(context(seed, delivery)).isZero();
                assertThat(after.prewriteValues).as(context(seed, delivery)).isEmpty();
                committedWriterStates.put(
                        delivery.writerKey,
                        new WriterStateView(delivery.sequence, expected.lastWalOffset));
            } else {
                assertThat(kv.getKvPreWriteBuffer().getAllKvEntries())
                        .as("%s exact uncommitted prewrite entries", context(seed, delivery))
                        .containsExactlyElementsOf(expected.uncommittedEntries);
                assertThat(kv.getKvPreWriteBuffer().getKvEntryMap())
                        .as("%s exact uncommitted prewrite map", context(seed, delivery))
                        .isEqualTo(expected.uncommittedEntryMap);
                assertThat(after.prewriteEntryCount)
                        .as(context(seed, delivery))
                        .isEqualTo(expected.effectiveMutationCount);
                assertThat(after.prewriteValues)
                        .as("%s exact uncommitted prewrite values", context(seed, delivery))
                        .isEqualTo(expected.prewriteValues);
            }
        }

        private void applyNoOp(
                Delivery delivery,
                ExpectedOutcome expectedOutcome,
                Map<WriterKey, WriterStateView> committedWriterStates,
                List<byte[]> allPhysicalKeys,
                int seed)
                throws Exception {
            PhysicalState before = physicalState(delivery.writerKey, allPhysicalKeys);
            long staleBefore = metrics.indexPushStaleV1Batches().getCount();
            long tombstoneBefore = metrics.indexPushTombstoneNoOpBatches().getCount();

            LogAppendInfo result = apply(delivery, true);

            if (expectedOutcome == ExpectedOutcome.STALE) {
                assertThat(result.duplicated()).as(context(seed, delivery)).isTrue();
                assertThat(result.hasNoAppend()).as(context(seed, delivery)).isFalse();
                assertThat(metrics.indexPushStaleV1Batches().getCount())
                        .as(context(seed, delivery))
                        .isEqualTo(staleBefore + 1L);
                assertThat(metrics.indexPushTombstoneNoOpBatches().getCount())
                        .as(context(seed, delivery))
                        .isEqualTo(tombstoneBefore);
            } else {
                assertThat(result.hasNoAppend()).as(context(seed, delivery)).isTrue();
                assertThat(metrics.indexPushTombstoneNoOpBatches().getCount())
                        .as(context(seed, delivery))
                        .isEqualTo(tombstoneBefore + 1L);
                assertThat(metrics.indexPushStaleV1Batches().getCount())
                        .as(context(seed, delivery))
                        .isEqualTo(staleBefore);
            }
            assertThat(physicalState(delivery.writerKey, allPhysicalKeys))
                    .as(
                            "%s has zero WAL, WriterState, and KV side effects",
                            context(seed, delivery))
                    .usingRecursiveComparison()
                    .isEqualTo(before);
            assertWriterStates(committedWriterStates, seed);
        }

        private LogAppendInfo apply(Delivery delivery, boolean throughReplica) throws Exception {
            assertThat(delivery.mutations).as(delivery.label).isNotEmpty();
            FencedKvRecordBatchBuilder builder =
                    FencedKvRecordBatchBuilder.builder(
                            SCHEMA_ID,
                            Integer.MAX_VALUE,
                            new UnmanagedPagedOutputView(4096),
                            KvFormat.ALIGNED);
            try {
                int appended = 0;
                for (SourceMutation mutation : delivery.mutations) {
                    ProductionMutation projected = ProductionProjection.project(mutation);
                    builder.append(
                            projected.key,
                            mutation.kind == MutationKind.UPSERT ? projected.value : null);
                    appended++;
                }
                assertThat(appended)
                        .as("%s carries only its mutation delta", delivery.label)
                        .isEqualTo(delivery.mutations.size());
                builder.setWriterState(delivery.writerKey, delivery.sequence);
                ByteBuffer bytes = builder.build().getByteBuf().nioBuffer();
                KvRecordBatch records = KvRecordBatchReader.pointToByteBuffer(bytes);
                return throughReplica
                        ? replica.putRecordsToLeader(records, null, MergeMode.OVERWRITE, -1)
                        : kv.putAsLeader(records, ALL_TARGET_COLUMNS, MergeMode.OVERWRITE);
            } finally {
                builder.close();
            }
        }

        private void recoverWriterState(
                Map<WriterKey, WriterStateView> committedWriterStates, int seed) throws Exception {
            long recoveryEnd = log.localLogEndOffset();
            assertThat(log.getHighWatermark())
                    .as("seed=%s recovery high watermark", seed)
                    .isEqualTo(recoveryEnd);
            log.writerStateManager().truncateFullyAndStartAt(0L);
            assertThat(log.writerStateManager().writerIdCount())
                    .as("seed=%s cleared state", seed)
                    .isZero();
            replica.loadWriterSnapshot(recoveryEnd);
            assertThat(log.writerStateManager().mapEndOffset())
                    .as("seed=%s recovered WriterState coverage end", seed)
                    .isEqualTo(recoveryEnd);
            assertWriterStates(committedWriterStates, seed);
        }

        private void assertWriterStates(
                Map<WriterKey, WriterStateView> committedWriterStates, int seed) {
            assertThat(log.writerStateManager().writerIdCount())
                    .as("seed=%s active WriterState count", seed)
                    .isEqualTo(committedWriterStates.size());
            committedWriterStates.forEach(
                    (writerKey, expected) ->
                            assertThat(writerState(writerKey))
                                    .as("seed=%s writer=%s", seed, writerKey)
                                    .contains(expected));
        }

        private Optional<WriterStateView> writerState(WriterKey writerKey) {
            return log.writerStateManager()
                    .lastFencedEntry(writerKey)
                    .map(
                            state ->
                                    new WriterStateView(
                                            state.lastSequence(),
                                            state.dominatingTargetWalOffset()));
        }

        private PhysicalState physicalState(WriterKey writerKey, List<byte[]> allPhysicalKeys) {
            KvPreWriteBuffer buffer = kv.getKvPreWriteBuffer();
            Map<String, String> prewriteValues = new LinkedHashMap<>();
            for (byte[] key : allPhysicalKeys) {
                Value value = buffer.get(Key.of(key));
                if (value != null) {
                    prewriteValues.put(
                            encode(key), value.get() == null ? "<DELETE>" : encode(value.get()));
                }
            }
            return new PhysicalState(
                    log.localLogEndOffset(),
                    writerState(writerKey),
                    buffer.getMaxLSN(),
                    buffer.getAllKvEntries().size(),
                    prewriteValues,
                    actualRows(allPhysicalKeys));
        }

        private Map<String, String> actualRows(List<byte[]> keys) {
            List<byte[]> values = replica.lookups(keys);
            Map<String, String> actual = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                if (values.get(i) != null) {
                    actual.put(encode(keys.get(i)), encode(values.get(i)));
                }
            }
            return actual;
        }

        private void publishTombstone(PartitionTombstone tombstone) {
            replicaManager.maybeUpdateMetadataCache(
                    0,
                    new ClusterMetadata(
                            null,
                            Collections.emptySet(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.singletonMap(mainTableId, tombstone)));
        }

        private static String context(int seed, Delivery delivery) {
            return "seed="
                    + seed
                    + ", delivery="
                    + delivery.label
                    + ", sequence="
                    + delivery.sequence;
        }
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
