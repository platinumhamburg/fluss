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
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;

import java.util.function.ToIntFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Write-side specification for one secondary index: column mapping, encoders, bucketing. */
@Internal
public final class IndexSpec {

    /** Encodes one complete physical Index Table entry from a base row. */
    @FunctionalInterface
    public interface EntryEncoder {
        IndexEntry encode(InternalRow row);
    }

    @FunctionalInterface
    interface ValueEncoder {
        BinaryRow encode(InternalRow row, long sourceOffset, boolean deleted);
    }

    static final class IndexEntry {
        private final byte[] key;
        private final BinaryRow value;
        private final int targetBucket;

        IndexEntry(byte[] key, BinaryRow value, int targetBucket) {
            this.key = checkNotNull(key, "key");
            this.value = checkNotNull(value, "value");
            this.targetBucket = targetBucket;
        }

        byte[] key() {
            return key;
        }

        BinaryRow value() {
            return value;
        }

        int targetBucket() {
            return targetBucket;
        }
    }

    private final long indexTableId;
    private final String indexName;
    private final IndexVisibility visibility;
    private final int indexSchemaId;
    private final KvFormat indexKvFormat;
    private final int[] idxColumnIndices;
    private final EntryEncoder entryEncoder;

    public IndexSpec(
            String indexName,
            IndexVisibility visibility,
            long indexTableId,
            int indexSchemaId,
            KvFormat indexKvFormat,
            int[] idxColumnIndices,
            EntryEncoder entryEncoder) {
        this.indexName = checkNotNull(indexName, "indexName");
        this.visibility = checkNotNull(visibility, "visibility");
        this.indexTableId = indexTableId;
        this.indexSchemaId = indexSchemaId;
        this.indexKvFormat = indexKvFormat;
        this.idxColumnIndices = checkNotNull(idxColumnIndices, "idxColumnIndices").clone();
        checkArgument(idxColumnIndices.length > 0, "idxColumnIndices must not be empty.");
        this.entryEncoder = checkNotNull(entryEncoder, "entryEncoder");
    }

    IndexSpec(
            String indexName,
            IndexVisibility visibility,
            long indexTableId,
            int indexSchemaId,
            KvFormat indexKvFormat,
            int[] idxColumnIndices,
            KeyEncoder keyEncoder,
            ValueEncoder valueEncoder,
            ToIntFunction<InternalRow> bucketAssigner) {
        this(
                indexName,
                visibility,
                indexTableId,
                indexSchemaId,
                indexKvFormat,
                idxColumnIndices,
                row ->
                        new IndexEntry(
                                keyEncoder.encodeKey(row),
                                valueEncoder.encode(row, 0L, false),
                                bucketAssigner.applyAsInt(row)));
    }

    public long getIndexTableId() {
        return indexTableId;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexVisibility getVisibility() {
        return visibility;
    }

    public boolean isSync() {
        return visibility == IndexVisibility.SYNC;
    }

    public int getIndexSchemaId() {
        return indexSchemaId;
    }

    public KvFormat getIndexKvFormat() {
        return indexKvFormat;
    }

    public int[] getIdxColumnIndices() {
        return idxColumnIndices.clone();
    }

    IndexEntry encodeEntry(InternalRow row) {
        return entryEncoder.encode(row);
    }

    boolean hasIndexColumns(InternalRow row) {
        for (int idxColumnIndex : idxColumnIndices) {
            if (row.isNullAt(idxColumnIndex)) {
                return false;
            }
        }
        return true;
    }
}
