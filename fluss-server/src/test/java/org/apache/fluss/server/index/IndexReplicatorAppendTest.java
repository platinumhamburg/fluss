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

import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IndexReplicator#appendOneSpec} — the per-index mutation derivation. The
 * focus is the write-amplification guard: an UPDATE that leaves the index value columns (index
 * columns plus the main-table primary key) unchanged must not emit a redundant upsert, while
 * inserts and index-key changes still produce the expected physical mutations.
 */
public class IndexReplicatorAppendTest {

    // Main row layout: [pk BIGINT, idx BIGINT, nonIndexed BIGINT].
    private static final RowType MAIN_ROW_TYPE =
            RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT());

    private static final long INDEX_TABLE_ID = 77L;
    private static final TableBucket SOURCE_BUCKET = new TableBucket(55L, 3);
    private static final short INDEX_SCHEMA_ID = 1;
    private static final Schema INDEX_SCHEMA =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("idx", DataTypes.BIGINT()),
                                    new Schema.Column("pk", DataTypes.BIGINT())))
                    .primaryKey("idx", "pk")
                    .build();

    private static IndexReplicator newReplicator() {
        return new IndexReplicator(
                null,
                Collections.emptyList(),
                new IndexAccumulator(),
                null,
                0L,
                1024,
                (sync, all) -> {});
    }

    /**
     * Builds a spec whose index value columns are {idx, pk}. Both the key and the stored value are
     * encoded from exactly those columns, mirroring production's {@code IndexSpecFactory}, so an
     * unchanged index key means an identical stored value.
     */
    private static IndexSpec spec() {
        int[] indexValueColumnIndices = new int[] {1, 0}; // idx column, then primary key
        RowType physicalRowType = RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT());
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(physicalRowType, new int[] {0, 1});
        RowEncoder valueRowEncoder =
                RowEncoder.create(
                        KvFormat.COMPACTED,
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.BIGINT(), DataTypes.BIGINT()
                        });
        IndexSpec.EntryEncoder entryEncoder =
                row -> {
                    valueRowEncoder.startNewRow();
                    valueRowEncoder.encodeField(0, row.getLong(1)); // idx
                    valueRowEncoder.encodeField(1, row.getLong(0)); // pk
                    BinaryRow value = valueRowEncoder.finishRow();
                    return new IndexSpec.IndexEntry(
                            keyEncoder.encodeKey(value), value, (int) (row.getLong(1) % 2));
                };

        return new IndexSpec(
                "idx",
                IndexVisibility.SYNC,
                INDEX_TABLE_ID,
                INDEX_SCHEMA_ID,
                KvFormat.COMPACTED,
                new int[] {1}, // idxColumnIndices
                entryEncoder);
    }

    private static GenericRow row(long pk, long idx, long nonIndexed) {
        GenericRow r = new GenericRow(3);
        r.setField(0, pk);
        r.setField(1, idx);
        r.setField(2, nonIndexed);
        return r;
    }

    @Test
    void insertEmitsUpsert() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations = replicator.appendOneSpec(spec(), null, row(1L, 10L, 100L), builders);

        assertThat(mutations).isEqualTo(1);
        assertThat(builders).hasSize(1);
        assertThat(onlyRecords(builders.values().iterator().next()).get(0).getRow()).isNotNull();
    }

    @Test
    void nonIndexColumnUpdateSkipsRedundantUpsert() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        // Same pk and same index value, only the non-indexed column changes: the index KV is
        // byte-for-byte identical, so no mutation must be produced. Without the guard this emits a
        // redundant upsert (mutations == 1), so this assertion fails on the un-fixed code.
        int mutations =
                replicator.appendOneSpec(spec(), row(1L, 10L, 100L), row(1L, 10L, 200L), builders);

        assertThat(mutations).isZero();
        assertThat(builders).isEmpty();
    }

    @Test
    void indexKeyChangeEmitsDeleteAndUpsert() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        // The index column changes from 10 to 20: the old key must be deleted and the new key
        // upserted (two mutations).
        int mutations =
                replicator.appendOneSpec(spec(), row(1L, 10L, 100L), row(1L, 20L, 100L), builders);

        assertThat(mutations).isEqualTo(2);
        assertThat(builders).hasSize(1);
        java.util.List<KvRecord> records = onlyRecords(builders.values().iterator().next());
        assertThat(records).hasSize(2);
        assertThat(records.get(0).getRow())
                .as("same-bucket old-key DELETE must precede the new-key UPSERT")
                .isNull();
        assertThat(records.get(1).getRow()).isNotNull();
        assertThat(records.get(0).getKey()).isNotEqualTo(records.get(1).getKey());
    }

    @Test
    void reusableEncoderKeepsOldDeleteRoutingAndWritesExactNewValue() {
        IndexReplicator replicator = newReplicator();
        IndexSpec spec = spec();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations =
                replicator.appendOneSpec(spec, row(1L, 10L, 100L), row(2L, 11L, 200L), builders);

        assertThat(mutations).isEqualTo(2);
        assertThat(builders)
                .containsOnlyKeys(
                        new TableBucket(INDEX_TABLE_ID, 0), new TableBucket(INDEX_TABLE_ID, 1));

        KvRecord delete = onlyRecords(builders.get(new TableBucket(INDEX_TABLE_ID, 0))).get(0);
        byte[] expectedOldKey =
                new CompactedKeyEncoder(
                                RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                new int[] {0, 1})
                        .encodeKey(GenericRow.of(10L, 1L));
        assertThat(delete.getKey()).isEqualTo(ByteBuffer.wrap(expectedOldKey));
        assertThat(delete.getRow()).isNull();

        KvRecord upsert = onlyRecords(builders.get(new TableBucket(INDEX_TABLE_ID, 1))).get(0);
        assertThat(upsert.getRow()).isNotNull();
        assertThat(upsert.getRow().getLong(0)).isEqualTo(11L);
        assertThat(upsert.getRow().getLong(1)).isEqualTo(2L);
    }

    @Test
    void validIndexToNullEmitsPhysicalDelete() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations =
                replicator.appendOneSpec(spec(), row(1L, 10L, 100L), row(1L, null, 100L), builders);

        assertThat(mutations).isEqualTo(1);
        assertThat(onlyRecords(builders.values().iterator().next()).get(0).getRow()).isNull();
    }

    @Test
    void nullIndexToValidEmitsUpsert() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations =
                replicator.appendOneSpec(spec(), row(1L, null, 100L), row(1L, 10L, 100L), builders);

        assertThat(mutations).isEqualTo(1);
        assertThat(onlyRecords(builders.values().iterator().next()).get(0).getRow()).isNotNull();
    }

    @Test
    void nullIndexToNullEmitsNothing() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations =
                replicator.appendOneSpec(
                        spec(), row(1L, null, 100L), row(1L, null, 200L), builders);

        assertThat(mutations).isZero();
        assertThat(builders).isEmpty();
    }

    @Test
    void adjacentUpdatePairInOneSourceBatchProducesOneFencedTargetBatch() throws Exception {
        PollFixture fixture =
                pollFixture(
                        5L,
                        1024,
                        Collections.singletonList(
                                Arrays.asList(
                                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)),
                                        record(6L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)))));

        assertThat(fixture.replicator.poll()).isTrue();

        IndexBatch targetBatch = fixture.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0));
        assertThat(targetBatch).isNotNull();
        assertThat(targetBatch.window().windowEndOffset()).isEqualTo(7L);
        KvRecordBatch decoded = decode(targetBatch);
        assertThat(decoded.fencedWriterKey()).isEqualTo(IndexWriterKey.encode(SOURCE_BUCKET));
        assertThat(decoded.fencedSequence()).isEqualTo(7L);
        assertThat(decoded.getRecordCount()).isEqualTo(2);
    }

    @Test
    void missingUpdateAfterFailsClosedWithoutRetainedRowState() throws Exception {
        assertCorrupt(
                5L,
                Collections.singletonList(
                        Collections.singletonList(
                                record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)))));
    }

    @Test
    void nonAdjacentUpdatePairFailsClosed() throws Exception {
        assertCorrupt(
                5L,
                Collections.singletonList(
                        Arrays.asList(
                                record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)),
                                record(6L, ChangeType.INSERT, row(2L, 30L, 100L)),
                                record(7L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)))));
    }

    @Test
    void updatePairSplitAcrossSourceBatchesFailsClosed() throws Exception {
        assertCorrupt(
                5L,
                Arrays.asList(
                        Collections.singletonList(
                                record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L))),
                        Collections.singletonList(
                                record(6L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)))));
    }

    @Test
    void resumeAtUnmatchedUpdateAfterFailsClosed() throws Exception {
        assertCorrupt(
                6L,
                Collections.singletonList(
                        Arrays.asList(
                                record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)),
                                record(6L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)))));
    }

    @Test
    void appendOnlyFailsClosedWithoutAdvancingOrStaging() throws Exception {
        assertCorrupt(
                5L,
                Collections.singletonList(
                        Collections.singletonList(
                                record(5L, ChangeType.APPEND_ONLY, row(1L, 10L, 100L)))));
    }

    @Test
    void resumeInsideSourceBatchSkipsRecordsBelowRequestedBoundary() throws Exception {
        PollFixture fixture =
                pollFixture(
                        6L,
                        1024,
                        Collections.singletonList(
                                Arrays.asList(
                                        record(5L, ChangeType.INSERT, row(1L, 10L, 100L)),
                                        record(6L, ChangeType.INSERT, row(2L, 20L, 100L)))));

        assertThat(fixture.replicator.poll()).isTrue();

        IndexBatch targetBatch = fixture.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0));
        KvRecordBatch decoded = decode(targetBatch);
        assertThat(decoded.getRecordCount()).isEqualTo(1);
        assertThat(decoded.fencedSequence()).isEqualTo(7L);
        assertThat(fixture.sourceWal.readOffsets).containsExactly(6L);
    }

    @Test
    void emptySourceBatchAdvancesToItsExclusiveNextOffset() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(1L);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored ->
                                CloseableIterator.wrap(
                                        Collections.<LogRecord>emptyList().iterator()));
        LogRecords records = mock(LogRecords.class);
        when(records.batches()).thenReturn(Collections.singletonList(batch));
        TestingSourceWal sourceWal = new TestingSourceWal(1L, records);
        IndexReplicator replicator =
                IndexReplicator.forTesting(
                        sourceWal,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        readContext,
                        -1L,
                        1024,
                        1024,
                        (sync, all) -> {});

        assertThat(replicator.poll()).isTrue();
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(1L);
        assertThat(sourceWal.readOffsets).containsExactly(0L);
        verify(batch).ensureValid();
    }

    @Test
    void corruptEmptySourceBatchFailsBeforeIterationOrAdvance() throws Exception {
        assertIntegrityFailureFailsClosed(Collections.emptyList());
    }

    @Test
    void corruptNonEmptySourceBatchFailsBeforeIterationOrAdvance() throws Exception {
        assertIntegrityFailureFailsClosed(
                Collections.singletonList(record(0L, ChangeType.INSERT, row(1L, 10L, 100L))));
    }

    @Test
    void eachSourceRowIsEncodedOnceWithoutTemporaryBucketBuilders() throws Exception {
        AtomicInteger encodes = new AtomicInteger();
        IndexSpec delegate = spec();
        IndexSpec countingSpec =
                new IndexSpec(
                        delegate.getIndexName(),
                        delegate.getVisibility(),
                        delegate.getIndexTableId(),
                        delegate.getIndexSchemaId(),
                        delegate.getIndexKvFormat(),
                        delegate.getIdxColumnIndices(),
                        row -> {
                            encodes.incrementAndGet();
                            return delegate.encodeEntry(row);
                        });
        PollFixture fixture =
                pollFixture(
                        0L,
                        1024,
                        countingSpec,
                        Collections.singletonList(
                                Arrays.asList(
                                        record(0L, ChangeType.INSERT, row(1L, 10L, 100L)),
                                        record(1L, ChangeType.INSERT, row(2L, 20L, 100L)))));

        assertThat(fixture.replicator.poll()).isTrue();

        assertThat(encodes).hasValue(2);
        assertThat(fixture.accumulator.buckets()).hasSize(1);
    }

    @Test
    void capturesOldUpdateMetadataBeforeAdvancingReusableSourceIterator() throws Exception {
        GenericRow reusedOldRow = row(1L, 10L, 100L);
        LogRecord before = record(0L, ChangeType.UPDATE_BEFORE, reusedOldRow);
        LogRecord after = record(1L, ChangeType.UPDATE_AFTER, row(1L, 11L, 100L));
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(2L);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored ->
                                CloseableIterator.wrap(
                                        new Iterator<LogRecord>() {
                                            private int next;

                                            @Override
                                            public boolean hasNext() {
                                                return next < 2;
                                            }

                                            @Override
                                            public LogRecord next() {
                                                if (next++ == 0) {
                                                    return before;
                                                }
                                                reusedOldRow.setField(0, 999L);
                                                reusedOldRow.setField(1, 99L);
                                                return after;
                                            }
                                        }));
        LogRecords records = mock(LogRecords.class);
        when(records.batches()).thenReturn(Collections.singletonList(batch));
        TestingSourceWal sourceWal = new TestingSourceWal(2L, records);
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator replicator =
                IndexReplicator.forTesting(
                        sourceWal,
                        Collections.singletonList(spec()),
                        accumulator,
                        readContext,
                        0L,
                        1024,
                        1024,
                        (sync, all) -> {});

        assertThat(replicator.poll()).isTrue();

        KvRecord delete =
                onlyRecords(accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0)).encoded())
                        .get(0);
        byte[] expectedOldKey =
                new CompactedKeyEncoder(
                                RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                new int[] {0, 1})
                        .encodeKey(GenericRow.of(10L, 1L));
        assertThat(delete.getKey()).isEqualTo(ByteBuffer.wrap(expectedOldKey));
    }

    @Test
    void preferredBoundCutsBeforeGroupAndNeverSplitsMultiBucketUpdate() throws Exception {
        PollFixture fixture =
                pollFixture(
                        5L,
                        1,
                        Collections.singletonList(
                                Arrays.asList(
                                        record(5L, ChangeType.INSERT, row(1L, 10L, 100L)),
                                        record(6L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)),
                                        record(7L, ChangeType.UPDATE_AFTER, row(1L, 11L, 100L)))));

        assertThat(fixture.replicator.poll()).isTrue();
        IndexBatch first = fixture.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0));
        assertThat(first.window().windowEndOffset()).isEqualTo(6L);
        assertThat(decode(first).getRecordCount()).isEqualTo(1);
        acknowledge(fixture.accumulator, first);

        assertThat(fixture.replicator.poll()).isTrue();
        IndexBatch delete = fixture.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0));
        IndexBatch upsert = fixture.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 1));
        assertThat(delete.window()).isSameAs(upsert.window());
        assertThat(delete.window().windowEndOffset()).isEqualTo(8L);
        assertThat(decode(delete).fencedSequence()).isEqualTo(8L);
        assertThat(decode(upsert).fencedSequence()).isEqualTo(8L);
        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(6L);
        acknowledge(fixture.accumulator, delete);
        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(6L);
        acknowledge(fixture.accumulator, upsert);
        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(8L);
    }

    @Test
    void failoverReplicatorsMayChooseDifferentValidWindowEnds() throws Exception {
        List<List<LogRecord>> source =
                Collections.singletonList(
                        Arrays.asList(
                                record(5L, ChangeType.INSERT, row(1L, 10L, 100L)),
                                record(6L, ChangeType.INSERT, row(2L, 20L, 100L))));
        PollFixture smaller = pollFixture(5L, 1, source);
        PollFixture larger = pollFixture(5L, 1024, source);

        assertThat(smaller.replicator.poll()).isTrue();
        assertThat(larger.replicator.poll()).isTrue();

        KvRecordBatch smallerBatch =
                decode(smaller.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0)));
        KvRecordBatch largerBatch =
                decode(larger.accumulator.pollFirst(new TableBucket(INDEX_TABLE_ID, 0)));
        assertThat(smallerBatch.fencedWriterKey()).isEqualTo(largerBatch.fencedWriterKey());
        assertThat(smallerBatch.fencedSequence()).isEqualTo(6L);
        assertThat(largerBatch.fencedSequence()).isEqualTo(7L);
        assertThat(largerBatch.getRecordCount()).isEqualTo(2);
    }

    @Test
    void publishesWindowStateBeforeSynchronousAccumulatorCallbacks() throws Exception {
        PollFixture fixture =
                pollFixture(
                        5L,
                        1024,
                        Collections.singletonList(
                                Arrays.asList(
                                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)),
                                        record(6L, ChangeType.UPDATE_AFTER, row(1L, 11L, 100L)))));
        fixture.accumulator.setAppendListener(
                bucket -> {
                    IndexBatch batch = fixture.accumulator.pollFirst(bucket);
                    acknowledge(fixture.accumulator, batch);
                });

        assertThat(fixture.replicator.poll()).isTrue();

        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(7L);
        assertThat(fixture.accumulator.pendingBytes()).isZero();
        assertThat(fixture.accumulator.hasUnsent()).isFalse();
    }

    @Test
    void terminalWindowFailureClearsMatchingInFlightWithoutAdvancing() throws Exception {
        PollFixture fixture =
                pollFixture(
                        5L,
                        1024,
                        Collections.singletonList(
                                Collections.singletonList(
                                        record(5L, ChangeType.INSERT, row(1L, 10L, 100L)))));

        assertThat(fixture.replicator.poll()).isTrue();
        IndexWindow window = fixture.replicator.inFlightWindow("idx");
        RuntimeException failure = new RuntimeException("terminal failure");

        assertThat(window).isNotNull();
        List<IndexBatch> drained = window.tryFailAndDrain(failure);

        assertThat(drained).hasSize(1);
        for (IndexBatch batch : drained) {
            fixture.accumulator.remove(batch);
            fixture.accumulator.release(batch);
        }

        assertThat(fixture.replicator.inFlightWindow("idx")).isNull();
        assertThat(fixture.replicator.terminalFailure()).isSameAs(failure);
        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(5L);
        assertThat(fixture.replicator.getAllIndexPushedOffset()).isEqualTo(5L);
        assertThat(window.registeredBatchCount()).isZero();
        assertThat(fixture.accumulator.pendingBytes()).isZero();
        assertThat(fixture.accumulator.hasUnsent()).isFalse();
    }

    @Test
    void deleteEmitsPhysicalDeleteOnly() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations = replicator.appendOneSpec(spec(), row(1L, 10L, 100L), null, builders);

        assertThat(mutations).isEqualTo(1);
        assertThat(builders).hasSize(1);
        KvRecord delete = onlyRecords(builders.values().iterator().next()).get(0);
        assertThat(delete.getRow()).isNull();
        assertThat(delete.getKey())
                .isEqualTo(ByteBuffer.wrap(spec().encodeEntry(row(1L, 10L, 100L)).key()));
    }

    private static List<KvRecord> onlyRecords(IndexReplicator.BucketBatchBuilder builder) {
        try {
            builder.builder.setWriterState(new WriterKey(0L, 0L), 0L);
            BytesView bytes = builder.builder.build();
            KvRecordBatch kvRecords =
                    KvRecordBatchReader.pointToByteBuffer(bytes.getByteBuf().nioBuffer());
            assertThat(kvRecords.idempotenceProtocolVersion()).isEqualTo(1);
            Iterator<KvRecord> iter =
                    kvRecords
                            .records(
                                    KvRecordReadContext.createReadContext(
                                            KvFormat.COMPACTED,
                                            new TestingSchemaGetter(INDEX_SCHEMA_ID, INDEX_SCHEMA)))
                            .iterator();
            List<KvRecord> records = new ArrayList<>();
            iter.forEachRemaining(records::add);
            return records;
        } catch (Exception e) {
            throw new AssertionError("Failed to decode index batch", e);
        }
    }

    private static void assertCorrupt(long initialOffset, List<List<LogRecord>> sourceBatches)
            throws Exception {
        PollFixture fixture = pollFixture(initialOffset, 1024, sourceBatches);

        assertThatThrownBy(fixture.replicator::poll)
                .isInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(fixture.replicator.getSyncIndexPushedOffset()).isEqualTo(initialOffset);
        assertThat(fixture.replicator.terminalFailure())
                .isInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(fixture.accumulator.hasUnsent()).isFalse();
        assertThat(fixture.replicator.poll()).isFalse();
        assertThat(fixture.sourceWal.readOffsets).containsExactly(initialOffset);
    }

    private static void assertIntegrityFailureFailsClosed(List<LogRecord> sourceBatch)
            throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(1L);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored -> CloseableIterator.wrap(new ArrayList<>(sourceBatch).iterator()));
        doThrow(new CorruptRecordException("injected crc failure")).when(batch).ensureValid();
        LogRecords records = mock(LogRecords.class);
        when(records.batches()).thenReturn(Collections.singletonList(batch));
        TestingSourceWal sourceWal = new TestingSourceWal(1L, records);
        IndexReplicator replicator =
                IndexReplicator.forTesting(
                        sourceWal,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        readContext,
                        0L,
                        1024,
                        1024,
                        (sync, all) -> {});

        assertThatThrownBy(replicator::poll)
                .isInstanceOf(IndexSourceWalCorruptionException.class)
                .hasCauseInstanceOf(CorruptRecordException.class);
        assertThat(replicator.getSyncIndexPushedOffset()).isZero();
        assertThat(replicator.terminalFailure())
                .isInstanceOf(IndexSourceWalCorruptionException.class);
        verify(batch, never()).records(readContext);
        verify(batch, never()).nextLogOffset();
        assertThat(replicator.poll()).isFalse();
    }

    private static PollFixture pollFixture(
            long initialOffset, int preferredMaxRequestBytes, List<List<LogRecord>> sourceBatches)
            throws Exception {
        return pollFixture(initialOffset, preferredMaxRequestBytes, spec(), sourceBatches);
    }

    private static PollFixture pollFixture(
            long initialOffset,
            int preferredMaxRequestBytes,
            IndexSpec spec,
            List<List<LogRecord>> sourceBatches)
            throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecords logRecords = mock(LogRecords.class);
        List<LogRecordBatch> batches = new ArrayList<>();
        long highWatermark = initialOffset;
        for (List<LogRecord> sourceBatch : sourceBatches) {
            LogRecordBatch batch = mock(LogRecordBatch.class);
            long baseOffset = sourceBatch.get(0).logOffset();
            long nextOffset = sourceBatch.get(sourceBatch.size() - 1).logOffset() + 1;
            when(batch.baseLogOffset()).thenReturn(baseOffset);
            when(batch.nextLogOffset()).thenReturn(nextOffset);
            when(batch.records(readContext))
                    .thenAnswer(
                            ignored ->
                                    CloseableIterator.wrap(
                                            new ArrayList<>(sourceBatch).iterator()));
            batches.add(batch);
            highWatermark = Math.max(highWatermark, nextOffset);
        }
        when(logRecords.batches()).thenReturn(batches);
        TestingSourceWal sourceWal = new TestingSourceWal(highWatermark, logRecords);
        IndexAccumulator accumulator = new IndexAccumulator();
        IndexReplicator replicator =
                IndexReplicator.forTesting(
                        sourceWal,
                        Collections.singletonList(spec),
                        accumulator,
                        readContext,
                        initialOffset,
                        1024,
                        preferredMaxRequestBytes,
                        (sync, all) -> {});
        return new PollFixture(sourceWal, accumulator, replicator);
    }

    private static KvRecordBatch decode(IndexBatch batch) {
        return KvRecordBatchReader.pointToByteBuffer(batch.encoded().getByteBuf().nioBuffer());
    }

    private static List<KvRecord> onlyRecords(BytesView encoded) {
        KvRecordBatch batch =
                KvRecordBatchReader.pointToByteBuffer(encoded.getByteBuf().nioBuffer());
        List<KvRecord> records = new ArrayList<>();
        batch.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED,
                                new TestingSchemaGetter(INDEX_SCHEMA_ID, INDEX_SCHEMA)))
                .forEach(records::add);
        return records;
    }

    private static void acknowledge(IndexAccumulator accumulator, IndexBatch batch) {
        accumulator.release(batch);
        batch.window().onBatchAcked(batch);
    }

    private static final class PollFixture {
        private final TestingSourceWal sourceWal;
        private final IndexAccumulator accumulator;
        private final IndexReplicator replicator;

        private PollFixture(
                TestingSourceWal sourceWal,
                IndexAccumulator accumulator,
                IndexReplicator replicator) {
            this.sourceWal = sourceWal;
            this.accumulator = accumulator;
            this.replicator = replicator;
        }
    }

    private static final class TestingSourceWal implements IndexReplicator.SourceWal {
        private final long highWatermark;
        private final LogRecords records;
        private final List<Long> readOffsets = new ArrayList<>();

        private TestingSourceWal(long highWatermark, LogRecords records) {
            this.highWatermark = highWatermark;
            this.records = records;
        }

        @Override
        public TableBucket tableBucket() {
            return SOURCE_BUCKET;
        }

        @Override
        public long highWatermark() {
            return highWatermark;
        }

        @Override
        public long logStartOffset() {
            return 0L;
        }

        @Override
        public FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
            readOffsets.add(offset);
            return new FetchDataInfo(records);
        }
    }

    private static GenericRow row(long pk, Long idx, long nonIndexed) {
        GenericRow r = new GenericRow(3);
        r.setField(0, pk);
        r.setField(1, idx);
        r.setField(2, nonIndexed);
        return r;
    }

    private static LogRecord record(long offset, ChangeType changeType, InternalRow row) {
        return new LogRecord() {
            @Override
            public long logOffset() {
                return offset;
            }

            @Override
            public long timestamp() {
                return 0L;
            }

            @Override
            public ChangeType getChangeType() {
                return changeType;
            }

            @Override
            public InternalRow getRow() {
                return row;
            }
        };
    }
}
