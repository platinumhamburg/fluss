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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IndexReplicator#appendOneSpec} — the per-index mutation derivation. The
 * focus is the write-amplification guard: an UPDATE that leaves the index value columns (index
 * columns plus the main-table primary key) unchanged must not emit a redundant upsert, while
 * inserts and index-key changes still produce the expected mutations.
 */
public class IndexReplicatorAppendTest {

    // Main row layout: [pk BIGINT, idx BIGINT, nonIndexed BIGINT].
    private static final RowType MAIN_ROW_TYPE =
            RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT());

    private static final long INDEX_TABLE_ID = 77L;

    private static IndexReplicator newReplicator() {
        return new IndexReplicator(
                null, Collections.emptyList(), new IndexAccumulator(), null, 0L, 1024, off -> {});
    }

    /**
     * Builds a spec whose index value columns are {idx, pk}. Both the key and the stored value are
     * encoded from exactly those columns, mirroring production's {@code IndexSpecFactory}, so an
     * unchanged index key means an identical stored value.
     */
    private static IndexSpec spec() {
        int[] indexValueColumnIndices = new int[] {1, 0}; // idx column, then primary key
        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(MAIN_ROW_TYPE, indexValueColumnIndices);

        DataType[] valueTypes = new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()};
        RowEncoder valueRowEncoder = RowEncoder.create(KvFormat.COMPACTED, valueTypes);
        IndexSpec.ValueEncoder valueEncoder =
                row -> {
                    valueRowEncoder.startNewRow();
                    valueRowEncoder.encodeField(0, row.getLong(1)); // idx
                    valueRowEncoder.encodeField(1, row.getLong(0)); // pk
                    return valueRowEncoder.finishRow();
                };

        ToIntFunction<InternalRow> bucketAssigner = r -> 0;

        return new IndexSpec(
                INDEX_TABLE_ID,
                1,
                KvFormat.COMPACTED,
                new int[] {1}, // idxColumnIndices
                keyEncoder,
                valueEncoder,
                bucketAssigner);
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
    }

    @Test
    void updatePairCanSpanRecordBatchBoundary() {
        IndexReplicator replicator =
                new IndexReplicator(
                        null,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        null,
                        0L,
                        1024,
                        off -> {});
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        long beforeResult =
                replicator.deriveAndAppend(
                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)), builders);
        long afterResult =
                replicator.deriveAndAppend(
                        record(6L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)), builders);

        assertThat(beforeResult)
                .as("UPDATE_BEFORE is not a complete index mutation and must not advance")
                .isEqualTo(5L);
        assertThat(afterResult).isEqualTo(7L);
        assertThat(builders).hasSize(1);
        assertThat(builders.values().iterator().next().count).isEqualTo(2);
    }

    @Test
    void pendingUpdateBeforeReadsFromNextOffsetWithoutAdvancingPushedOffset() {
        IndexReplicator replicator =
                new IndexReplicator(
                        null,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        null,
                        5L,
                        1024,
                        off -> {});
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        long beforeResult =
                replicator.deriveAndAppend(
                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)), builders);

        assertThat(beforeResult).isEqualTo(5L);
        assertThat(replicator.getIndexPushedOffset()).isEqualTo(5L);
        assertThat(replicator.nextReadOffset())
                .as("the next poll must continue after the pending UPDATE_BEFORE")
                .isEqualTo(6L);
    }

    @Test
    void unmatchedUpdateBeforeStopsBeforeAdvancingPastIt() {
        IndexReplicator replicator =
                new IndexReplicator(
                        null,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        null,
                        0L,
                        1024,
                        off -> {});
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        long beforeResult =
                replicator.deriveAndAppend(
                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)), builders);
        long insertResult =
                replicator.deriveAndAppend(
                        record(6L, ChangeType.INSERT, row(2L, 30L, 100L)), builders);

        assertThat(beforeResult).isEqualTo(5L);
        assertThat(insertResult)
                .as("the next window must re-read the incomplete UPDATE_BEFORE")
                .isEqualTo(-1L);
        assertThat(builders).isEmpty();
    }

    @Test
    void nonAdjacentUpdateAfterStopsBeforePairingWithUpdateBefore() {
        IndexReplicator replicator =
                new IndexReplicator(
                        null,
                        Collections.singletonList(spec()),
                        new IndexAccumulator(),
                        null,
                        0L,
                        1024,
                        off -> {});
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        long beforeResult =
                replicator.deriveAndAppend(
                        record(5L, ChangeType.UPDATE_BEFORE, row(1L, 10L, 100L)), builders);
        long afterResult =
                replicator.deriveAndAppend(
                        record(7L, ChangeType.UPDATE_AFTER, row(1L, 20L, 100L)), builders);

        assertThat(beforeResult).isEqualTo(5L);
        assertThat(afterResult)
                .as("UPDATE_BEFORE and UPDATE_AFTER must be adjacent in the WAL")
                .isEqualTo(-1L);
        assertThat(builders).isEmpty();
    }

    @Test
    void deleteEmitsTombstoneOnly() {
        IndexReplicator replicator = newReplicator();
        Map<TableBucket, IndexReplicator.BucketBatchBuilder> builders = new HashMap<>();

        int mutations = replicator.appendOneSpec(spec(), row(1L, 10L, 100L), null, builders);

        assertThat(mutations).isEqualTo(1);
        assertThat(builders).hasSize(1);
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
