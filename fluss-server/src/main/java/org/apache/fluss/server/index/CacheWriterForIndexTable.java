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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * Builder class that writes IndexedRow data directly to IndexBucketRowCache instead of constructing
 * intermediate IndexSegments. This provides the core functionality for the unified hot/cold data
 * processing pipeline.
 */
public final class CacheWriterForIndexTable implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheWriterForIndexTable.class);

    private TablePartitionId indexTablePartitionId;
    private Schema indexSchema;
    private int indexBucketCount;
    private final BucketingFunction bucketingFunction;
    private final IndexRowCache indexRowCache;

    // For building IndexedRow
    private IndexedRowEncoder indexedRowEncoder;
    private InternalRow.FieldGetter[] fieldGetters;
    private KeyEncoder bucketKeyEncoder;

    private int[] indexFieldIndicesOfDataRow;

    public CacheWriterForIndexTable(
            TablePartitionId indexTablePartitionId,
            Schema tableSchema,
            Schema indexSchema,
            int indexBucketCount,
            List<String> bucketKeys,
            BucketingFunction bucketingFunction,
            IndexRowCache indexRowCache) {
        this.indexTablePartitionId = indexTablePartitionId;
        this.indexSchema = indexSchema;
        this.indexBucketCount = indexBucketCount;
        this.bucketingFunction = bucketingFunction;
        this.indexRowCache = indexRowCache;

        // Create IndexedRowEncoder for row conversion
        this.indexedRowEncoder = new IndexedRowEncoder(indexSchema.getRowType());

        // Create field getters for data row conversion
        this.fieldGetters = new InternalRow.FieldGetter[tableSchema.getRowType().getFieldCount()];
        for (int i = 0; i < tableSchema.getRowType().getFieldCount(); i++) {
            fieldGetters[i] =
                    InternalRow.createFieldGetter(tableSchema.getRowType().getTypeAt(i), i);
        }
        this.bucketKeyEncoder = KeyEncoder.of(indexSchema.getRowType(), bucketKeys, null);

        this.indexFieldIndicesOfDataRow =
                tableSchema.getColumnIndexes(indexSchema.getPrimaryKeyColumnNames());
    }

    public void writeRecord(LogRecord walRecord, long offset) throws Exception {
        if (null != walRecord.getRow()) {
            IndexedRow indexedRow = createIndexedRow(walRecord, offset);
            byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(indexedRow);
            int targetBucketId = bucketingFunction.bucketing(bucketKeyBytes, indexBucketCount);
            indexRowCache.writeIndexedRow(
                    indexTablePartitionId,
                    targetBucketId,
                    offset,
                    walRecord.getChangeType(),
                    indexedRow);
        } else {
            indexRowCache.writeEmptyRows(indexTablePartitionId, offset);
        }
    }

    private IndexedRow createIndexedRow(LogRecord walRecord, long offset) {
        // Build IndexedRow directly from data row
        indexedRowEncoder.startNewRow();

        int fieldIndex = 0;

        // Add index column values - directly from dataRow to IndexedRowEncoder
        for (int dataColumnIndex : indexFieldIndicesOfDataRow) {
            if (dataColumnIndex >= 0) {
                Object value = fieldGetters[dataColumnIndex].getFieldOrNull(walRecord.getRow());
                indexedRowEncoder.encodeField(fieldIndex++, value);
            }
        }

        // Add __offset system column (last column)
        int offsetColumnIndex = indexSchema.getRowType().getFieldCount() - 1;
        indexedRowEncoder.encodeField(offsetColumnIndex, offset);

        return indexedRowEncoder.finishRow();
    }

    @Override
    public void close() {
        if (indexedRowEncoder != null) {
            try {
                indexedRowEncoder.close();
            } catch (Exception e) {
                LOG.warn("Failed to close IndexedRowEncoder", e);
            }
        }
    }
}
