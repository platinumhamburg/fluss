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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Adversarial old-request ordering through the production PutKv RPC and replica apply path. */
class IndexPushOrderingITCase {

    private static final int INDEX_BUCKET_COUNT = 2;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));
    private static final RowType BUCKET_KEY_TYPE =
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
    void testReverseOldRequestsCannotOutrunDominatingWindows() throws Exception {
        Fixture fixture = createFixture();
        List<PhysicalRow> allRows = new ArrayList<>();

        // UPSERT -> DELETE: the newer delete establishes the fence before the delayed upsert.
        PhysicalRow upsertDelete = row("upsert-delete", 1);
        allRows.add(upsertDelete);
        HeldRequest oldUpsert = fixture.hold(0, 100L, upsertDelete, false);
        fixture.sendAndAssert(0, 200L, upsertDelete, true);
        oldUpsert.releaseStale();
        fixture.assertAbsent(upsertDelete);

        // DELETE -> UPSERT: start live, then let the newer upsert dominate the delayed delete.
        PhysicalRow deleteUpsert = row("delete-upsert", 2);
        allRows.add(deleteUpsert);
        fixture.sendAndAssert(1, 50L, deleteUpsert, false);
        HeldRequest oldDelete = fixture.hold(1, 100L, deleteUpsert, true);
        fixture.sendAndAssert(1, 200L, deleteUpsert, false);
        oldDelete.releaseStale();
        fixture.assertPresent(deleteUpsert);

        // Same-key UPSERT -> DELETE -> UPSERT, with both old requests released in reverse order.
        PhysicalRow threeStep = row("three-step", 3);
        allRows.add(threeStep);
        HeldRequest first = fixture.hold(2, 100L, threeStep, false);
        HeldRequest second = fixture.hold(2, 200L, threeStep, true);
        fixture.sendAndAssert(2, 300L, threeStep, false);
        second.releaseStale();
        first.releaseStale();
        fixture.assertPresent(threeStep);

        // Index-key movement spans two target buckets. The old-key delete and new-key upsert use
        // the same dominating source end offset, while a delayed old-key upsert is held back.
        PhysicalRow[] movement = rowsInDifferentBuckets(4);
        PhysicalRow oldKey = movement[0];
        PhysicalRow newKey = movement[1];
        allRows.add(oldKey);
        allRows.add(newKey);
        HeldRequest movingOldUpsert = fixture.hold(3, 100L, oldKey, false);
        fixture.sendAndAssert(3, 200L, oldKey, true);
        fixture.sendAndAssert(3, 200L, newKey, false);
        movingOldUpsert.releaseStale();
        fixture.assertAbsent(oldKey);
        fixture.assertPresent(newKey);

        // Lost response and retry: ignore the first response, redeliver the identical sequence,
        // and prove the stale retry appends no WAL.
        PhysicalRow lostResponse = row("lost-response", 5);
        allRows.add(lostResponse);
        fixture.sendAndAssert(4, 100L, lostResponse, false);
        long walBeforeRetry = fixture.walEnd(lostResponse.bucket);
        fixture.sendAndAssert(4, 100L, lostResponse, false);
        assertThat(fixture.walEnd(lostResponse.bucket)).isEqualTo(walBeforeRetry);

        // The exact final projection is the committed source-WAL reference for the schedules.
        assertThat(fixture.presence(allRows)).containsExactly(false, true, true, false, true, true);
    }

    private static Fixture createFixture() throws Exception {
        String tableName = "ordering_" + System.nanoTime();
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
                                INDEX_BUCKET_COUNT)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder().schema(schema).distributedBy(5, "a").build());
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;
        Replica[] replicas = new Replica[INDEX_BUCKET_COUNT];
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            replicas[bucket] =
                    CLUSTER.waitAndGetLeaderReplica(new TableBucket(indexTableId, bucket));
        }
        TabletServerGateway[] gateways = new TabletServerGateway[3];
        for (int connection = 0; connection < gateways.length; connection++) {
            gateways[connection] = CLUSTER.newTabletServerClientForNode(0);
        }
        return new Fixture(mainTableId, indexTableId, replicas, gateways);
    }

    private static PhysicalRow row(String indexedValue, int primaryKey) {
        AlignedRow value = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(value);
        writer.reset();
        writer.writeString(0, fromString(indexedValue));
        writer.writeInt(1, primaryKey);
        writer.complete();
        byte[] key = new CompactedKeyEncoder(INDEX_ROW_TYPE).encodeKey(value);
        return new PhysicalRow(indexedValue, value, key, bucket(indexedValue));
    }

    private static PhysicalRow[] rowsInDifferentBuckets(int primaryKey) {
        PhysicalRow first = row("move-0", primaryKey);
        for (int suffix = 1; suffix < 100; suffix++) {
            PhysicalRow candidate = row("move-" + suffix, primaryKey);
            if (candidate.bucket != first.bucket) {
                return new PhysicalRow[] {first, candidate};
            }
        }
        throw new AssertionError("unable to find keys in different target buckets");
    }

    private static int bucket(String indexedValue) {
        GenericRow bucketKey = new GenericRow(1);
        bucketKey.setField(0, fromString(indexedValue));
        return new FlussBucketingFunction()
                .bucketing(new CompactedKeyEncoder(BUCKET_KEY_TYPE).encodeKey(bucketKey), 2);
    }

    private static void setAdmissionHook(Replica replica, @Nullable Runnable hook)
            throws Exception {
        Method method = Replica.class.getDeclaredMethod("setAfterPutAdmission", Runnable.class);
        method.setAccessible(true);
        method.invoke(replica, hook);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class PhysicalRow {
        private final String indexedValue;
        private final AlignedRow value;
        private final byte[] key;
        private final int bucket;

        private PhysicalRow(String indexedValue, AlignedRow value, byte[] key, int bucket) {
            this.indexedValue = indexedValue;
            this.value = value;
            this.key = key;
            this.bucket = bucket;
        }
    }

    private static final class Fixture {
        private final long mainTableId;
        private final long indexTableId;
        private final Replica[] replicas;
        private final TabletServerGateway[] gateways;
        private final ExecutorService heldRequestExecutor = Executors.newFixedThreadPool(2);
        private int nextGateway;

        private Fixture(
                long mainTableId,
                long indexTableId,
                Replica[] replicas,
                TabletServerGateway[] gateways) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.replicas = replicas;
            this.gateways = gateways;
        }

        private HeldRequest hold(int sourceBucket, long sequence, PhysicalRow row, boolean delete)
                throws Exception {
            CountDownLatch admitted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Replica target = replicas[row.bucket];
            setAdmissionHook(
                    target,
                    () -> {
                        admitted.countDown();
                        await(release);
                    });
            KvRecordBatch records = records(sourceBucket, sequence, row, delete);
            CompletableFuture<Void> response =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    target.putRecordsToLeader(
                                            records, null, MergeMode.OVERWRITE, -1);
                                } catch (Exception e) {
                                    throw new AssertionError(e);
                                }
                            },
                            heldRequestExecutor);
            await(admitted);
            setAdmissionHook(target, null);
            return new HeldRequest(this, row.bucket, release, response);
        }

        private void sendAndAssert(int sourceBucket, long sequence, PhysicalRow row, boolean delete)
                throws Exception {
            assertSuccess(send(sourceBucket, sequence, row, delete).get(10, TimeUnit.SECONDS));
        }

        private CompletableFuture<PutKvResponse> send(
                int sourceBucket, long sequence, PhysicalRow row, boolean delete)
                throws IOException {
            BytesView batch = encoded(sourceBucket, sequence, row, delete);
            PutKvRequest request =
                    new PutKvRequest().setTableId(indexTableId).setAcks(-1).setTimeoutMs(10_000);
            request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
            PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(row.bucket);
            bucketRequest.setRecordsBytesView(batch);
            TabletServerGateway gateway = gateways[nextGateway++ % gateways.length];
            return gateway.putKv(request);
        }

        private KvRecordBatch records(
                int sourceBucket, long sequence, PhysicalRow row, boolean delete)
                throws IOException {
            BytesView batch = encoded(sourceBucket, sequence, row, delete);
            return KvRecordBatchReader.pointToByteBuffer(batch.getByteBuf().nioBuffer());
        }

        private BytesView encoded(int sourceBucket, long sequence, PhysicalRow row, boolean delete)
                throws IOException {
            FencedKvRecordBatchBuilder builder =
                    FencedKvRecordBatchBuilder.builder(
                            1,
                            Integer.MAX_VALUE,
                            new UnmanagedPagedOutputView(1024),
                            KvFormat.ALIGNED);
            builder.append(row.key, delete ? null : row.value);
            WriterKey writerKey = IndexWriterKey.encode(new TableBucket(mainTableId, sourceBucket));
            builder.setWriterState(writerKey, sequence);
            return builder.build();
        }

        private long walEnd(int bucket) {
            return replicas[bucket].getLogTablet().localLogEndOffset();
        }

        private void assertPresent(PhysicalRow row) {
            waitUntil(
                    () -> value(row) != null,
                    TIMEOUT,
                    "wait for exact index row " + row.indexedValue);
        }

        private void assertAbsent(PhysicalRow row) {
            waitUntil(
                    () -> value(row) == null,
                    TIMEOUT,
                    "wait for exact index deletion " + row.indexedValue);
        }

        @Nullable
        private byte[] value(PhysicalRow row) {
            return replicas[row.bucket].lookups(Collections.singletonList(row.key)).get(0);
        }

        private List<Boolean> presence(List<PhysicalRow> rows) {
            List<Boolean> presence = new ArrayList<>(rows.size());
            for (PhysicalRow row : rows) {
                presence.add(value(row) != null);
            }
            return presence;
        }

        private static void assertSuccess(PutKvResponse response) {
            assertThat(response.getBucketsRespsList())
                    .singleElement()
                    .satisfies(bucket -> assertThat(bucket.hasErrorCode()).isFalse());
        }
    }

    private static final class HeldRequest {
        private final Fixture fixture;
        private final int bucket;
        private final CountDownLatch release;
        private final CompletableFuture<Void> response;

        private HeldRequest(
                Fixture fixture,
                int bucket,
                CountDownLatch release,
                CompletableFuture<Void> response) {
            this.fixture = fixture;
            this.bucket = bucket;
            this.release = release;
            this.response = response;
        }

        private void releaseStale() throws Exception {
            long walBeforeRelease = fixture.walEnd(bucket);
            release.countDown();
            response.get(10, TimeUnit.SECONDS);
            assertThat(fixture.walEnd(bucket))
                    .as("released stale request must not append target WAL")
                    .isEqualTo(walBeforeRelease);
        }
    }
}
