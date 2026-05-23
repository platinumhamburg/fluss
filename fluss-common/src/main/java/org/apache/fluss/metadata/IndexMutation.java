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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A single mutation a data bucket leader will push to an Index Bucket leader.
 *
 * <p>For UPSERT, {@link #getValue()} is the encoded Index Table value (partitioned tables already
 * have the 8-byte partitionId prefix per {@code IndexTableUtils}). For DELETE, {@link #getValue()}
 * is {@code null}.
 */
@Internal
public final class IndexMutation {

    private final IndexMutationOp op;
    private final long indexTableId;
    private final int indexBucket;
    private final byte[] key;
    @Nullable private final byte[] value;
    private final long sourceOffset;

    private IndexMutation(
            IndexMutationOp op,
            long indexTableId,
            int indexBucket,
            byte[] key,
            @Nullable byte[] value,
            long sourceOffset) {
        this.op = checkNotNull(op, "op");
        this.indexTableId = indexTableId;
        this.indexBucket = indexBucket;
        this.key = checkNotNull(key, "key");
        this.value = value;
        this.sourceOffset = sourceOffset;
    }

    public static IndexMutation upsert(
            long indexTableId, int indexBucket, byte[] key, byte[] value, long sourceOffset) {
        checkArgument(value != null, "UPSERT mutation requires a non-null value.");
        return new IndexMutation(
                IndexMutationOp.UPSERT, indexTableId, indexBucket, key, value, sourceOffset);
    }

    public static IndexMutation delete(
            long indexTableId, int indexBucket, byte[] key, long sourceOffset) {
        return new IndexMutation(
                IndexMutationOp.DELETE, indexTableId, indexBucket, key, null, sourceOffset);
    }

    public IndexMutationOp getOp() {
        return op;
    }

    public long getIndexTableId() {
        return indexTableId;
    }

    public int getIndexBucket() {
        return indexBucket;
    }

    public byte[] getKey() {
        return key;
    }

    @Nullable
    public byte[] getValue() {
        return value;
    }

    public long getSourceOffset() {
        return sourceOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexMutation)) {
            return false;
        }
        IndexMutation that = (IndexMutation) o;
        return indexTableId == that.indexTableId
                && indexBucket == that.indexBucket
                && sourceOffset == that.sourceOffset
                && op == that.op
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(op, indexTableId, indexBucket, sourceOffset);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "IndexMutation{op="
                + op
                + ", indexTableId="
                + indexTableId
                + ", indexBucket="
                + indexBucket
                + ", sourceOffset="
                + sourceOffset
                + ", keyLen="
                + key.length
                + ", valueLen="
                + (value == null ? "null" : String.valueOf(value.length))
                + "}";
    }
}
