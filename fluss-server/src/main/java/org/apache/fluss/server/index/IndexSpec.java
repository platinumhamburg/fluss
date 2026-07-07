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

    /** Encodes the index value from a base row. Partition handling is baked into the closure. */
    @FunctionalInterface
    public interface ValueEncoder {
        BinaryRow encode(InternalRow row);
    }

    private final long indexTableId;
    private final int indexSchemaId;
    private final KvFormat indexKvFormat;
    private final int[] idxColumnIndices;
    private final KeyEncoder keyEncoder;
    private final ValueEncoder valueEncoder;
    private final ToIntFunction<InternalRow> bucketAssigner;

    public IndexSpec(
            long indexTableId,
            int indexSchemaId,
            KvFormat indexKvFormat,
            int[] idxColumnIndices,
            KeyEncoder keyEncoder,
            ValueEncoder valueEncoder,
            ToIntFunction<InternalRow> bucketAssigner) {
        this.indexTableId = indexTableId;
        this.indexSchemaId = indexSchemaId;
        this.indexKvFormat = indexKvFormat;
        this.idxColumnIndices = checkNotNull(idxColumnIndices, "idxColumnIndices").clone();
        checkArgument(idxColumnIndices.length > 0, "idxColumnIndices must not be empty.");
        this.keyEncoder = checkNotNull(keyEncoder, "keyEncoder");
        this.valueEncoder = checkNotNull(valueEncoder, "valueEncoder");
        this.bucketAssigner = checkNotNull(bucketAssigner, "bucketAssigner");
    }

    public long getIndexTableId() {
        return indexTableId;
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

    public KeyEncoder getKeyEncoder() {
        return keyEncoder;
    }

    public ValueEncoder getValueEncoder() {
        return valueEncoder;
    }

    public ToIntFunction<InternalRow> getBucketAssigner() {
        return bucketAssigner;
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
