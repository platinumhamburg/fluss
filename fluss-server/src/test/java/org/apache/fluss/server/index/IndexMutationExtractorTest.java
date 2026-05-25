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

import org.apache.fluss.metadata.IndexMutation;
import org.apache.fluss.metadata.IndexMutationOp;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link IndexMutationExtractor}. */
class IndexMutationExtractorTest {

    /** Stub row whose only meaningful method is {@link InternalRow#isNullAt(int)}. */
    private static InternalRow row(final Set<Integer> nullPositions) {
        return new InternalRow() {
            @Override
            public int getFieldCount() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean isNullAt(int pos) {
                return nullPositions.contains(pos);
            }

            @Override
            public boolean getBoolean(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte getByte(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public short getShort(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public float getFloat(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getDouble(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BinaryString getChar(int pos, int length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public BinaryString getString(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Decimal getDecimal(int pos, int precision, int scale) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TimestampNtz getTimestampNtz(int pos, int precision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TimestampLtz getTimestampLtz(int pos, int precision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getBinary(int pos, int length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getBytes(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalArray getArray(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalMap getMap(int pos) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InternalRow getRow(int pos, int numFields) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static IndexMutationExtractor.KeyEncoder fixedKeyEncoder(final byte... bytes) {
        return (r, idx, pk) -> bytes;
    }

    private static IndexMutationExtractor.ValueEncoder fixedValueEncoder(final byte... bytes) {
        return (r, idx, pk, pid) -> bytes;
    }

    private static IndexMutationExtractor.BucketAssigner fixedBucket(final int b) {
        return (k, r) -> b;
    }

    @Test
    void testInsertWithNonNullIdxEmitsSingleUpsert() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        /* indexTableId= */ 1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(ctx, null, row(Collections.emptySet()), 42L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.UPSERT);
        assertThat(result.get(0).getKey()).containsExactly(1);
        assertThat(result.get(0).getValue()).containsExactly(2);
        assertThat(result.get(0).getIndexBucket()).isEqualTo(3);
        assertThat(result.get(0).getSourceOffset()).isEqualTo(42L);
    }

    @Test
    void testDeleteEmitsSingleDelete() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(ctx, row(Collections.emptySet()), null, 42L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.DELETE);
        assertThat(result.get(0).getValue()).isNull();
    }

    @Test
    void testUpdateIdxChangedEmitsDeleteThenUpsert() {
        final int[] callIdx = {0};
        final byte[][] keys = {new byte[] {(byte) 0xA}, new byte[] {(byte) 0xB}};
        IndexMutationExtractor.KeyEncoder seqKey = (r, idx, pk) -> keys[callIdx[0]++];

        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        seqKey,
                        fixedValueEncoder((byte) 9),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        InternalRow oldR = row(Collections.emptySet());
        InternalRow newR = row(Collections.emptySet());

        List<IndexMutation> result = IndexMutationExtractor.extract(ctx, oldR, newR, 5L);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.DELETE);
        assertThat(result.get(0).getKey()).containsExactly(0xA);
        assertThat(result.get(1).getOp()).isEqualTo(IndexMutationOp.UPSERT);
        assertThat(result.get(1).getKey()).containsExactly(0xB);
        assertThat(result.get(0).getSourceOffset()).isEqualTo(5L);
        assertThat(result.get(1).getSourceOffset()).isEqualTo(5L);
    }

    @Test
    void testUpdateIdxSameEmitsSingleUpsert() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 7),
                        fixedValueEncoder((byte) 9),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(
                        ctx, row(Collections.emptySet()), row(Collections.emptySet()), 5L);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.UPSERT);
    }

    @Test
    void testInsertSkipsWhenAllIdxColumnsAreNull() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        Set<Integer> allNull = new HashSet<>();
        allNull.add(0);
        List<IndexMutation> result = IndexMutationExtractor.extract(ctx, null, row(allNull), 5L);
        assertThat(result).isEmpty();
    }

    @Test
    void testDeleteSkipsWhenOldIdxColumnsAreNull() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        Set<Integer> allNull = new HashSet<>();
        allNull.add(0);
        List<IndexMutation> result = IndexMutationExtractor.extract(ctx, row(allNull), null, 5L);
        assertThat(result).isEmpty();
    }

    @Test
    void testMultipleIndexPlansEachEmitIndependently() {
        IndexMutationExtractor.IndexPlan p1 =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 1),
                        fixedBucket(0));
        IndexMutationExtractor.IndexPlan p2 =
                new IndexMutationExtractor.IndexPlan(
                        2L,
                        new int[] {1},
                        fixedKeyEncoder((byte) 2),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(1));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Arrays.asList(p1, p2), new int[] {0, 1}, false, null);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(ctx, null, row(Collections.emptySet()), 5L);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getIndexTableId()).isEqualTo(1L);
        assertThat(result.get(1).getIndexTableId()).isEqualTo(2L);
    }

    @Test
    void testPartitionedTableResolvesPartitionIdFromNewRowIfPresent() {
        final int[] resolverCalls = {0};
        ToLongFunction<InternalRow> resolver =
                r -> {
                    resolverCalls[0]++;
                    return 100L;
                };

        IndexMutationExtractor.ValueEncoder pidCapturing =
                (r, idx, pk, pid) -> new byte[] {(byte) pid};
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        pidCapturing,
                        fixedBucket(0));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, true, resolver);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(ctx, null, row(Collections.emptySet()), 5L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getValue()).containsExactly(100);
        assertThat(resolverCalls[0]).isEqualTo(1);
    }

    @Test
    void testPartitionedTableResolvesPartitionIdFromOldRowOnDelete() {
        ToLongFunction<InternalRow> resolver = r -> 200L;
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 9),
                        fixedBucket(0));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, true, resolver);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(ctx, row(Collections.emptySet()), null, 5L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.DELETE);
    }

    @Test
    void testContextWithNoIndexesShortCircuitsToEmptyList() {
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.emptyList(), new int[] {0}, false, null);

        List<IndexMutation> result =
                IndexMutationExtractor.extract(
                        ctx, row(Collections.emptySet()), row(Collections.emptySet()), 5L);
        assertThat(result).isEmpty();
    }

    @Test
    void testUpdateWithNewIdxColsAllNullEmitsDeleteOnly() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 7),
                        fixedValueEncoder((byte) 9),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        InternalRow oldR = row(Collections.emptySet());
        Set<Integer> newAllNull = new HashSet<>();
        newAllNull.add(0);
        InternalRow newR = row(newAllNull);

        List<IndexMutation> result = IndexMutationExtractor.extract(ctx, oldR, newR, 100L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOp()).isEqualTo(IndexMutationOp.DELETE);
        assertThat(result.get(0).getSourceOffset()).isEqualTo(100L);
    }

    @Test
    void testContextRejectsPartitionedTrueWithNullResolver() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        assertThatThrownBy(
                        () ->
                                new IndexMutationExtractor.Context(
                                        Collections.singletonList(plan),
                                        new int[] {0},
                                        /* partitioned= */ true,
                                        /* partitionIdResolver= */ null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionIdResolver");
    }

    @Test
    void testBothRowsNullReturnsEmptyList() {
        IndexMutationExtractor.IndexPlan plan =
                new IndexMutationExtractor.IndexPlan(
                        1L,
                        new int[] {0},
                        fixedKeyEncoder((byte) 1),
                        fixedValueEncoder((byte) 2),
                        fixedBucket(3));
        IndexMutationExtractor.Context ctx =
                new IndexMutationExtractor.Context(
                        Collections.singletonList(plan), new int[] {0}, false, null);

        List<IndexMutation> result = IndexMutationExtractor.extract(ctx, null, null, 5L);
        assertThat(result).isEmpty();
    }
}
