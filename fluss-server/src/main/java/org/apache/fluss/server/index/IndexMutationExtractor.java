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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.IndexMutation;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Pure helper that derives {@link IndexMutation}s from a {@code (oldRow, newRow)} pair according to
 * the FIP V2 §2.5 extraction rules.
 *
 * <p>For each configured {@link IndexPlan}, the extractor emits zero, one, or two mutations:
 *
 * <ul>
 *   <li>INSERT (oldRow null, newRow non-null with all idx cols non-null) — single UPSERT
 *   <li>DELETE (oldRow non-null with all idx cols non-null, newRow null) — single DELETE
 *   <li>UPDATE with idx columns changed — DELETE old key + UPSERT new key
 *   <li>UPDATE with idx columns unchanged — single UPSERT (idempotent re-emit)
 *   <li>Sparse old/new — no DELETE / no UPSERT respectively
 * </ul>
 *
 * <p>This class is a pure function — no I/O, no logging, no state.
 */
@Internal
public final class IndexMutationExtractor {

    /** Encodes the index key from a base row. */
    @FunctionalInterface
    public interface KeyEncoder {
        byte[] encode(InternalRow row, int[] idxColumnIndices, int[] basePkColumnIndices);
    }

    /** Encodes the index value from a base row, with an optional partition id prefix. */
    @FunctionalInterface
    public interface ValueEncoder {
        byte[] encode(
                InternalRow row,
                int[] idxColumnIndices,
                int[] basePkColumnIndices,
                long partitionId);
    }

    /**
     * Maps a base-row mutation to its target Index Table bucket.
     *
     * <p>The encoded full-PK {@code key} (idxCols ++ basePK in the Index Table's PK column
     * order) is passed for completeness, but implementations bucket on the {@code row}'s
     * idxCols-only bytes — never on {@code key} — so the push-side bucketing aligns with
     * {@link org.apache.fluss.client.lookup.PrefixKeyLookuper}, which routes its lookup
     * probe by hashing only the idxCols.
     */
    @FunctionalInterface
    public interface BucketAssigner {
        int bucketOf(byte[] key, InternalRow row);
    }

    /** Per-table plan for one secondary index. */
    public static final class IndexPlan {

        private final long indexTableId;
        private final int[] idxColumnIndices;
        private final KeyEncoder keyEncoder;
        private final ValueEncoder valueEncoder;
        private final BucketAssigner bucketAssigner;

        public IndexPlan(
                long indexTableId,
                int[] idxColumnIndices,
                KeyEncoder keyEncoder,
                ValueEncoder valueEncoder,
                BucketAssigner bucketAssigner) {
            this.indexTableId = indexTableId;
            this.idxColumnIndices = checkNotNull(idxColumnIndices, "idxColumnIndices");
            this.keyEncoder = checkNotNull(keyEncoder, "keyEncoder");
            this.valueEncoder = checkNotNull(valueEncoder, "valueEncoder");
            this.bucketAssigner = checkNotNull(bucketAssigner, "bucketAssigner");
        }

        public long getIndexTableId() {
            return indexTableId;
        }

        public int[] getIdxColumnIndices() {
            return idxColumnIndices;
        }

        public KeyEncoder getKeyEncoder() {
            return keyEncoder;
        }

        public ValueEncoder getValueEncoder() {
            return valueEncoder;
        }

        public BucketAssigner getBucketAssigner() {
            return bucketAssigner;
        }
    }

    /** Per-main-table immutable context shared across all extract invocations. */
    public static final class Context {

        private final List<IndexPlan> plans;
        private final int[] basePkColumnIndices;
        private final boolean partitioned;
        @Nullable private final ToLongFunction<InternalRow> partitionIdResolver;

        public Context(
                List<IndexPlan> plans,
                int[] basePkColumnIndices,
                boolean partitioned,
                @Nullable ToLongFunction<InternalRow> partitionIdResolver) {
            this.plans =
                    Collections.unmodifiableList(new ArrayList<>(checkNotNull(plans, "plans")));
            this.basePkColumnIndices =
                    checkNotNull(basePkColumnIndices, "basePkColumnIndices").clone();
            this.partitioned = partitioned;
            this.partitionIdResolver = partitionIdResolver;
            if (partitioned) {
                checkArgument(
                        partitionIdResolver != null,
                        "partitionIdResolver must be provided when partitioned=true.");
            }
        }

        public boolean hasIndexes() {
            return !plans.isEmpty();
        }
    }

    private IndexMutationExtractor() {}

    /**
     * Derive the list of {@link IndexMutation}s for a single base-row mutation.
     *
     * @param ctx per-main-table context with index plans and partition resolver
     * @param oldRow pre-image, {@code null} for INSERT
     * @param newRow post-image, {@code null} for DELETE
     * @param sourceOffset WAL offset attached to every emitted mutation
     * @return ordered list of mutations (DELETE precedes UPSERT for idx-changed updates)
     */
    public static List<IndexMutation> extract(
            Context ctx,
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            long sourceOffset) {
        if (!ctx.hasIndexes()) {
            return Collections.emptyList();
        }
        if (oldRow == null && newRow == null) {
            return Collections.emptyList();
        }
        long partitionId = -1L;
        if (ctx.partitioned) {
            InternalRow rowForPartition = newRow != null ? newRow : oldRow;
            partitionId = ctx.partitionIdResolver.applyAsLong(rowForPartition);
        }
        List<IndexMutation> out = new ArrayList<>();
        for (IndexPlan plan : ctx.plans) {
            extractOne(ctx, plan, oldRow, newRow, partitionId, sourceOffset, out);
        }
        return out;
    }

    private static void extractOne(
            Context ctx,
            IndexPlan plan,
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            long partitionId,
            long sourceOffset,
            List<IndexMutation> out) {
        boolean oldHasIdx = oldRow != null && allIdxColumnsNonNull(oldRow, plan.idxColumnIndices);
        boolean newHasIdx = newRow != null && allIdxColumnsNonNull(newRow, plan.idxColumnIndices);

        byte[] oldKey = null;
        byte[] newKey = null;
        if (oldHasIdx) {
            oldKey =
                    plan.keyEncoder.encode(
                            oldRow, plan.idxColumnIndices, ctx.basePkColumnIndices);
        }
        if (newHasIdx) {
            newKey =
                    plan.keyEncoder.encode(
                            newRow, plan.idxColumnIndices, ctx.basePkColumnIndices);
        }
        boolean keysDiffer = oldKey != null && (newKey == null || !Arrays.equals(oldKey, newKey));

        if (oldHasIdx && keysDiffer) {
            out.add(
                    IndexMutation.delete(
                            plan.indexTableId,
                            plan.bucketAssigner.bucketOf(oldKey, oldRow),
                            oldKey,
                            sourceOffset));
        }
        if (newHasIdx) {
            byte[] newValue =
                    plan.valueEncoder.encode(
                            newRow, plan.idxColumnIndices, ctx.basePkColumnIndices, partitionId);
            out.add(
                    IndexMutation.upsert(
                            plan.indexTableId,
                            plan.bucketAssigner.bucketOf(newKey, newRow),
                            newKey,
                            newValue,
                            sourceOffset));
        }
    }

    private static boolean allIdxColumnsNonNull(InternalRow row, int[] idxColumnIndices) {
        for (int i : idxColumnIndices) {
            if (row.isNullAt(i)) {
                return false;
            }
        }
        return true;
    }
}
