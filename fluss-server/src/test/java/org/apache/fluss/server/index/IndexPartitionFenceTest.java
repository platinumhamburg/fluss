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

import org.apache.fluss.exception.StaleMetadataException;
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
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.UnsafeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests partition fencing at the Index Table V1 apply boundary. */
class IndexPartitionFenceTest extends ReplicaTestBase {

    private static final long MAIN_TABLE_ID = 9100L;
    private static final long INDEX_TABLE_ID = 9200L;
    private static final long PARTITION_ID = 10L;
    private static final short SCHEMA_ID = 1;

    @Test
    void testUninitializedPartitionedIndexRejectsBeforePrewrite() throws Exception {
        Fixture fixture = createFixture("uninitialized");
        WriterKey writerKey = writerKey(PARTITION_ID);

        assertThatThrownBy(
                        () ->
                                fixture.put(
                                        mutation(
                                                writerKey,
                                                100L,
                                                physicalRow(PARTITION_ID),
                                                PARTITION_ID)))
                .isInstanceOf(StaleMetadataException.class)
                .hasMessageContaining("not initialized");

        assertNoMutation(fixture, writerKey, 0L);
    }

    @Test
    void testInitializedEmptyAllowsWriteAndUsesValidatedPartitionAsV3Tag() throws Exception {
        Fixture fixture = createInitializedFixture("empty_baseline");
        WriterKey writerKey = writerKey(PARTITION_ID);
        BinaryRow row = physicalRow(PARTITION_ID);
        byte[] key = physicalKey(row);

        fixture.put(mutation(writerKey, 100L, row, key));

        byte[] encodedValue = fixture.kv.getKvPreWriteBuffer().get(Key.of(key)).get();
        assertThat(UnsafeUtils.getLong(encodedValue, ValueEncoder.TAG_OFFSET))
                .isEqualTo(PARTITION_ID);
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
        assertThat(fixture.log.writerStateManager().lastFencedEntry(writerKey)).isPresent();
    }

    @ParameterizedTest(name = "writerPid={0}, keyPid={1}, valuePid={2}")
    @MethodSource("partitionMismatches")
    void testRejectsWriterKeyPhysicalKeyAndValuePartitionMismatch(
            long writerPid, long keyPid, long valuePid) throws Exception {
        Fixture fixture = createInitializedFixture("mismatch_" + writerPid + keyPid + valuePid);
        WriterKey writerKey = writerKey(writerPid);
        BinaryRow value = physicalRow(valuePid);

        assertThatThrownBy(
                        () ->
                                fixture.put(
                                        mutation(
                                                writerKey,
                                                100L,
                                                value,
                                                physicalKey(physicalRow(keyPid)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition");

        assertNoMutation(fixture, writerKey, 0L);
    }

    private static Stream<Arguments> partitionMismatches() {
        return Stream.of(
                Arguments.of(PARTITION_ID, PARTITION_ID, PARTITION_ID + 1),
                Arguments.of(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID),
                Arguments.of(PARTITION_ID + 1, PARTITION_ID, PARTITION_ID));
    }

    @Test
    void testDeleteDecodesAndValidatesPhysicalKeyPartition() throws Exception {
        Fixture fixture = createInitializedFixture("delete_mismatch");
        WriterKey writerKey = writerKey(PARTITION_ID);
        BinaryRow liveRow = physicalRow(PARTITION_ID);
        byte[] liveKey = physicalKey(liveRow);
        fixture.put(mutation(writerKey, 100L, liveRow, liveKey));

        assertThatThrownBy(
                        () ->
                                fixture.put(
                                        mutation(
                                                writerKey,
                                                101L,
                                                null,
                                                physicalKey(physicalRow(PARTITION_ID + 1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition");

        assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
        assertThat(fixture.kv.getKvPreWriteBuffer().getAllKvEntries()).hasSize(1);
        assertThat(
                        fixture.log
                                .writerStateManager()
                                .lastFencedEntry(writerKey)
                                .map(FencedWriterStateEntry::lastSequence))
                .contains(100L);
    }

    @Test
    void testTombstonedRequestIsImmediateNoAppendWithoutState() throws Exception {
        Fixture fixture = createFixture("tombstoned_no_append");
        publishDirect(tombstone(PARTITION_ID));
        WriterKey writerKey = writerKey(PARTITION_ID);

        LogAppendInfo result =
                fixture.put(mutation(writerKey, 100L, physicalRow(PARTITION_ID), PARTITION_ID));

        assertThat(result.lastOffset()).isEqualTo(-1L);
        assertNoMutation(fixture, writerKey, 0L);
    }

    @Test
    void testTombstonedRequestCompletesDelayedWriteWithoutHighWatermarkWait() throws Exception {
        Fixture fixture = createFixture("immediate_response");
        publishDirect(tombstone(PARTITION_ID));
        WriterKey writerKey = writerKey(PARTITION_ID);
        CompletableFuture<List<PutKvResultForBucket>> response = new CompletableFuture<>();

        replicaManager.putRecordsToKv(
                10_000,
                -1,
                Collections.singletonMap(
                        fixture.log.getTableBucket(),
                        mutation(writerKey, 100L, physicalRow(PARTITION_ID), PARTITION_ID)),
                null,
                MergeMode.OVERWRITE,
                ApiKeys.PUT_KV.highestSupportedVersion,
                response::complete);

        assertThat(response.get(10, TimeUnit.SECONDS))
                .singleElement()
                .satisfies(
                        result -> {
                            assertThat(result.succeeded()).isTrue();
                            assertThat(result.getWriteLogEndOffset()).isEqualTo(-1L);
                        });
        assertNoMutation(fixture, writerKey, 0L);
    }

    @Test
    void testMetadataUpdateRetiresFencedWriter() throws Exception {
        Fixture fixture = createInitializedFixture("retirement");
        WriterKey writerKey = writerKey(PARTITION_ID);
        fixture.put(mutation(writerKey, 100L, physicalRow(PARTITION_ID), PARTITION_ID));
        assertThat(fixture.log.writerStateManager().lastFencedEntry(writerKey)).isPresent();

        publishThroughReplicaManager(tombstone(PARTITION_ID));

        assertThat(fixture.log.writerStateManager().lastFencedEntry(writerKey)).isEmpty();
    }

    @Test
    void testApplyHoldingKvLockFinishesBeforePublicationRetiresWriter() throws Exception {
        Fixture fixture = createInitializedFixture("apply_first");
        WriterKey writerKey = writerKey(PARTITION_ID);
        byte[] key = physicalKey(physicalRow(PARTITION_ID));
        CountDownLatch applyHasLock = new CountDownLatch(1);
        CountDownLatch releaseApply = new CountDownLatch(1);
        setKvHook(
                fixture.kv,
                "setAfterFencedPrecheck",
                () -> {
                    applyHasLock.countDown();
                    awaitUnchecked(releaseApply);
                });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<LogAppendInfo> apply =
                    executor.submit(
                            () ->
                                    fixture.put(
                                            mutation(
                                                    writerKey,
                                                    100L,
                                                    physicalRow(PARTITION_ID),
                                                    key)));
            await(applyHasLock);
            PartitionTombstone tombstone = tombstone(PARTITION_ID);
            publishDirect(tombstone);
            Future<?> retirement = executor.submit(() -> publishThroughReplicaManager(tombstone));

            releaseApply.countDown();
            assertThat(apply.get(10, TimeUnit.SECONDS).lastOffset()).isZero();
            retirement.get(10, TimeUnit.SECONDS);
        } finally {
            releaseApply.countDown();
            setKvHook(fixture.kv, "setAfterFencedPrecheck", null);
            executor.shutdownNow();
        }

        fixture.kv.flush(Long.MAX_VALUE, ignored -> {});
        assertThat(fixture.replica.prefixLookup(key)).isEmpty();
        assertThat(fixture.log.writerStateManager().lastFencedEntry(writerKey)).isEmpty();
    }

    @Test
    void testPublicationHoldingKvLockMakesLaterApplyNoOp() throws Exception {
        Fixture fixture = createInitializedFixture("publication_first");
        WriterKey writerKey = writerKey(PARTITION_ID);
        CountDownLatch publicationHasLock = new CountDownLatch(1);
        CountDownLatch releasePublication = new CountDownLatch(1);
        CountDownLatch applyContended = new CountDownLatch(1);
        setKvHook(fixture.kv, "setPutLockContentionHook", applyContended::countDown);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> publication =
                    executor.submit(
                            () ->
                                    fixture.kv
                                            .getGuardedExecutor()
                                            .execute(
                                                    () -> {
                                                        publishThroughReplicaManager(
                                                                tombstone(PARTITION_ID));
                                                        publicationHasLock.countDown();
                                                        awaitUnchecked(releasePublication);
                                                    }));
            await(publicationHasLock);
            Future<LogAppendInfo> apply =
                    executor.submit(
                            () ->
                                    fixture.put(
                                            mutation(
                                                    writerKey,
                                                    100L,
                                                    physicalRow(PARTITION_ID),
                                                    PARTITION_ID)));
            await(applyContended);

            releasePublication.countDown();
            publication.get(10, TimeUnit.SECONDS);
            assertThat(apply.get(10, TimeUnit.SECONDS).lastOffset()).isEqualTo(-1L);
        } finally {
            releasePublication.countDown();
            setKvHook(fixture.kv, "setPutLockContentionHook", null);
            executor.shutdownNow();
        }

        assertNoMutation(fixture, writerKey, 0L);
    }

    private Fixture createInitializedFixture(String name) throws Exception {
        Fixture fixture = createFixture(name);
        publishDirect(PartitionTombstone.EMPTY);
        return fixture;
    }

    private Fixture createFixture(String name) throws Exception {
        TableDescriptor mainDescriptor = partitionedMainTableDescriptor();
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(
                        mainDescriptor, MAIN_TABLE_ID, "test_db.orders", "idx_user");
        TablePath indexPath =
                TablePath.of(
                        "test_db", IndexTableUtils.indexTableName("orders_" + name, "idx_user"));
        zkClient.registerTable(
                indexPath,
                TableRegistration.newTable(
                        INDEX_TABLE_ID, DEFAULT_REMOTE_DATA_DIR, indexDescriptor));
        zkClient.registerFirstSchema(indexPath, indexDescriptor.getSchema());
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 0);
        makeKvTableAsLeader(indexBucket, indexPath, INITIAL_LEADER_EPOCH, false);
        Replica replica = replicaManager.getReplicaOrException(indexBucket);
        return new Fixture(replica, replica.getKvTablet(), replica.getLogTablet());
    }

    private static TableDescriptor partitionedMainTableDescriptor() {
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
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy("dt")
                .distributedBy(1, "order_id")
                .build();
    }

    private static WriterKey writerKey(long partitionId) {
        return IndexWriterKey.encode(new TableBucket(MAIN_TABLE_ID, partitionId, 0));
    }

    private static AlignedRow physicalRow(long partitionId) {
        AlignedRow row = new AlignedRow(4);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.reset();
        writer.writeLong(0, 7L);
        writer.writeLong(1, 42L);
        writer.writeString(2, fromString("2026-07-12"));
        writer.writeLong(3, partitionId);
        writer.complete();
        return row;
    }

    private static byte[] physicalKey(BinaryRow row) {
        return new CompactedKeyEncoder(indexRowType()).encodeKey(row);
    }

    private static RowType indexRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                DataTypes.FIELD("order_id", DataTypes.BIGINT()),
                DataTypes.FIELD("dt", DataTypes.STRING()),
                DataTypes.FIELD(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN, DataTypes.BIGINT()));
    }

    private static KvRecordBatch mutation(
            WriterKey writerKey, long sequence, BinaryRow row, long keyPartitionId)
            throws Exception {
        return mutation(writerKey, sequence, row, physicalKey(physicalRow(keyPartitionId)));
    }

    private static KvRecordBatch mutation(
            WriterKey writerKey, long sequence, BinaryRow row, byte[] key) throws Exception {
        FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        SCHEMA_ID, 1024, new UnmanagedPagedOutputView(256), KvFormat.ALIGNED);
        builder.append(key, row);
        builder.setWriterState(writerKey, sequence);
        return KvRecordBatchReader.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
    }

    private void publishDirect(PartitionTombstone tombstone) {
        serverMetadataCache.updatePartitionTombstone(MAIN_TABLE_ID, tombstone);
    }

    private void publishThroughReplicaManager(PartitionTombstone tombstone) {
        replicaManager.maybeUpdateMetadataCache(
                0,
                new ClusterMetadata(
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(MAIN_TABLE_ID, tombstone)));
    }

    private static PartitionTombstone tombstone(long partitionId) {
        return new PartitionTombstone(-1L, Collections.singleton(partitionId), 1L);
    }

    private static void assertNoMutation(Fixture fixture, WriterKey writerKey, long logEndOffset) {
        assertThat(fixture.kv.getKvPreWriteBuffer().getAllKvEntries()).isEmpty();
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(logEndOffset);
        assertThat(fixture.log.writerStateManager().lastFencedEntry(writerKey)).isEmpty();
    }

    private static void setKvHook(KvTablet kvTablet, String methodName, Runnable hook)
            throws Exception {
        Method method = KvTablet.class.getDeclaredMethod(methodName, Runnable.class);
        method.setAccessible(true);
        method.invoke(kvTablet, hook);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            await(latch);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class Fixture {
        private final Replica replica;
        private final KvTablet kv;
        private final LogTablet log;

        private Fixture(Replica replica, KvTablet kv, LogTablet log) {
            this.replica = replica;
            this.kv = kv;
            this.log = log;
        }

        private LogAppendInfo put(KvRecordBatch records) throws Exception {
            return kv.putAsLeader(records, null, MergeMode.OVERWRITE);
        }
    }
}
