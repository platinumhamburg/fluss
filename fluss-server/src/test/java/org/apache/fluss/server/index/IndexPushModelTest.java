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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Deterministic source-history model checked against the production V1 target apply path. */
class IndexPushModelTest {

    private static final int SEEDS = 200;
    private static final int SOURCE_OPERATIONS = 200;
    private static final int KEYS_PER_SEED = 4;
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));

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
    void testGeneratedDeliveriesConvergeWithoutStaleSideEffects() throws Exception {
        Fixture fixture = createFixture();
        for (int seed = 0; seed < SEEDS; seed++) {
            runSeed(fixture, seed);
        }
    }

    private static void runSeed(Fixture fixture, int seed) throws Exception {
        Random random = new Random(seed);
        ModelRow[] rows = new ModelRow[KEYS_PER_SEED];
        for (int key = 0; key < rows.length; key++) {
            rows[key] = row(seed, key);
        }

        boolean[] sourceProjection = new boolean[KEYS_PER_SEED];
        List<boolean[]> prefixProjection = new ArrayList<>(SOURCE_OPERATIONS + 1);
        prefixProjection.add(sourceProjection.clone());
        for (int operation = 0; operation < SOURCE_OPERATIONS; operation++) {
            int key = random.nextInt(KEYS_PER_SEED);
            sourceProjection[key] = random.nextBoolean();
            prefixProjection.add(sourceProjection.clone());
        }

        List<Delivery> deliveries = new ArrayList<>();
        int endOffset = 0;
        while (endOffset < SOURCE_OPERATIONS) {
            endOffset = Math.min(SOURCE_OPERATIONS, endOffset + 5 + random.nextInt(21));
            deliveries.add(new Delivery(endOffset, prefixProjection.get(endOffset)));
        }
        Collections.shuffle(deliveries, random);

        long staleKvMutationCount = 0L;
        long staleWalAppendCount = 0L;
        for (Delivery delivery : deliveries) {
            SideEffectCount counts = fixture.deliver(seed, rows, delivery);
            staleKvMutationCount += counts.kvMutations;
            staleWalAppendCount += counts.walAppends;

            if (random.nextBoolean()) {
                // Duplicate delivery models a lost response: target completion is intentionally
                // ignored, then the identical request is redelivered.
                CompletableFuture<PutKvResponse> ignored = fixture.send(seed, rows, delivery);
                waitUntil(
                        () -> fixture.lastSequence(seed).orElse(-1L) >= delivery.sequence,
                        java.time.Duration.ofSeconds(10),
                        "wait for ignored response delivery to reach target state");
                SideEffectCount retry = fixture.deliver(seed, rows, delivery);
                staleKvMutationCount += retry.kvMutations;
                staleWalAppendCount += retry.walAppends;
                ignored.get(10, TimeUnit.SECONDS);
            }
        }

        Delivery settle = new Delivery(SOURCE_OPERATIONS, prefixProjection.get(SOURCE_OPERATIONS));
        SideEffectCount settleCounts = fixture.deliver(seed, rows, settle);
        staleKvMutationCount += settleCounts.kvMutations;
        staleWalAppendCount += settleCounts.walAppends;

        Map<String, String> referenceRows = referenceRows(rows, sourceProjection);
        Map<String, String> actualIndexRows = fixture.actualRows(rows);
        assertThat(actualIndexRows).as("seed=%s", seed).isEqualTo(referenceRows);
        assertThat(fixture.lastSequence(seed)).as("seed=%s", seed).contains(200L);
        assertThat(staleKvMutationCount).as("seed=%s", seed).isZero();
        assertThat(staleWalAppendCount).as("seed=%s", seed).isZero();
    }

    private static Map<String, String> referenceRows(ModelRow[] rows, boolean[] projection) {
        Map<String, String> reference = new HashMap<>();
        for (int key = 0; key < rows.length; key++) {
            if (projection[key]) {
                reference.put(
                        Base64.getEncoder().encodeToString(rows[key].key),
                        Base64.getEncoder().encodeToString(rows[key].body));
            }
        }
        return reference;
    }

    private static ModelRow row(int seed, int keyIndex) {
        AlignedRow value = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(value);
        writer.reset();
        writer.writeString(0, fromString("seed-" + seed + "-key-" + keyIndex));
        writer.writeInt(1, seed * KEYS_PER_SEED + keyIndex);
        writer.complete();
        byte[] key = new CompactedKeyEncoder(INDEX_ROW_TYPE).encodeKey(value);
        byte[] body = ValueEncoder.encodeValue((short) 1, value);
        return new ModelRow(key, value, body);
    }

    private static Fixture createFixture() throws Exception {
        String tableName = "model_" + System.nanoTime();
        TablePath mainPath = TablePath.of("task12", tableName);
        TablePath indexPath =
                TablePath.of("task12", IndexTableUtils.indexTableName(tableName, "idx_b"));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                "idx_b",
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder().schema(schema).distributedBy(1, "a").build());
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;
        Replica replica = CLUSTER.waitAndGetLeaderReplica(new TableBucket(indexTableId, 0));
        TabletServerGateway gateway = CLUSTER.newTabletServerClientForNode(0);
        return new Fixture(mainTableId, indexTableId, replica, gateway);
    }

    private static final class ModelRow {
        private final byte[] key;
        private final AlignedRow value;
        private final byte[] body;

        private ModelRow(byte[] key, AlignedRow value, byte[] body) {
            this.key = key;
            this.value = value;
            this.body = body;
        }
    }

    private static final class Delivery {
        private final long sequence;
        private final boolean[] projection;

        private Delivery(long sequence, boolean[] projection) {
            this.sequence = sequence;
            this.projection = projection.clone();
        }
    }

    private static final class SideEffectCount {
        private final long kvMutations;
        private final long walAppends;

        private SideEffectCount(long kvMutations, long walAppends) {
            this.kvMutations = kvMutations;
            this.walAppends = walAppends;
        }
    }

    private static final class Fixture {
        private final long mainTableId;
        private final long indexTableId;
        private final Replica replica;
        private final TabletServerGateway gateway;

        private Fixture(
                long mainTableId, long indexTableId, Replica replica, TabletServerGateway gateway) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.replica = replica;
            this.gateway = gateway;
        }

        private SideEffectCount deliver(int seed, ModelRow[] rows, Delivery delivery)
                throws Exception {
            boolean stale =
                    lastSequence(seed).map(sequence -> delivery.sequence <= sequence).orElse(false);
            Map<String, String> beforeRows = stale ? actualRows(rows) : Collections.emptyMap();
            long walBefore = replica.getLogTablet().localLogEndOffset();
            assertSuccess(send(seed, rows, delivery).get(10, TimeUnit.SECONDS));
            if (!stale) {
                return new SideEffectCount(0L, 0L);
            }
            long kvMutations = actualRows(rows).equals(beforeRows) ? 0L : 1L;
            long walAppends = replica.getLogTablet().localLogEndOffset() == walBefore ? 0L : 1L;
            return new SideEffectCount(kvMutations, walAppends);
        }

        private CompletableFuture<PutKvResponse> send(int seed, ModelRow[] rows, Delivery delivery)
                throws IOException {
            FencedKvRecordBatchBuilder builder =
                    FencedKvRecordBatchBuilder.builder(
                            1,
                            Integer.MAX_VALUE,
                            new UnmanagedPagedOutputView(4096),
                            KvFormat.ALIGNED);
            for (int key = 0; key < rows.length; key++) {
                builder.append(rows[key].key, delivery.projection[key] ? rows[key].value : null);
            }
            builder.setWriterState(writerKey(seed), delivery.sequence);
            BytesView batch = builder.build();
            PutKvRequest request =
                    new PutKvRequest().setTableId(indexTableId).setAcks(-1).setTimeoutMs(10_000);
            request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
            PbPutKvReqForBucket bucket = request.addBucketsReq().setBucketId(0);
            bucket.setRecordsBytesView(batch);
            CompletableFuture<PutKvResponse> response = gateway.putKv(request);
            response.whenComplete((ignored, failure) -> builder.close());
            return response;
        }

        private WriterKey writerKey(int seed) {
            return IndexWriterKey.encode(new TableBucket(mainTableId, seed));
        }

        private Optional<Long> lastSequence(int seed) {
            return replica.getLogTablet()
                    .writerStateManager()
                    .lastFencedEntry(writerKey(seed))
                    .map(FencedWriterStateEntry::lastSequence);
        }

        private Map<String, String> actualRows(ModelRow[] rows) {
            List<byte[]> keys = new ArrayList<>(rows.length);
            for (ModelRow row : rows) {
                keys.add(row.key);
            }
            List<byte[]> values = replica.lookups(keys);
            Map<String, String> actual = new HashMap<>();
            for (int key = 0; key < rows.length; key++) {
                if (values.get(key) != null) {
                    actual.put(
                            Base64.getEncoder().encodeToString(rows[key].key),
                            Base64.getEncoder().encodeToString(values.get(key)));
                }
            }
            return actual;
        }

        private static void assertSuccess(PutKvResponse response) {
            assertThat(response.getBucketsRespsList())
                    .singleElement()
                    .satisfies(bucket -> assertThat(bucket.hasErrorCode()).isFalse());
        }
    }
}
