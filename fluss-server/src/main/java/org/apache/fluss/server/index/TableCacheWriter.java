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
public final class TableCacheWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TableCacheWriter.class);

    private final long indexTableId;
    private final Schema indexSchema;
    private final int indexBucketCount;
    private final BucketingFunction bucketingFunction;
    private final IndexRowCache indexRowCache;

    // For building IndexedRow
    private final IndexedRowEncoder indexedRowEncoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final KeyEncoder bucketKeyEncoder;

    private final int[] indexFieldIndicesOfDataRow;

    public TableCacheWriter(
            long indexTableId,
            Schema tableSchema,
            Schema indexSchema,
            int indexBucketCount,
            List<String> bucketKeys,
            BucketingFunction bucketingFunction,
            IndexRowCache indexRowCache) {
        this.indexTableId = indexTableId;
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

    /**
     * Writes a record in batch mode, only targeting the specific bucket without writing empty rows
     * to other buckets. This method is used during batch processing to reduce empty row writes.
     *
     * @param walRecord the WAL record to process
     * @param offset the log offset
     * @param batchStartOffset the minimum offset for gap filling
     * @throws Exception if an error occurs during writing
     */
    public void writeRecordInBatch(LogRecord walRecord, long offset, long batchStartOffset)
            throws Exception {
        if (null != walRecord.getRow()) {
            IndexedRow indexedRow = createIndexedRow(walRecord, offset);
            byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(indexedRow);
            int targetBucketId = bucketingFunction.bucketing(bucketKeyBytes, indexBucketCount);

            // Only write to target bucket, do not write empty rows to other buckets
            indexRowCache.writeIndexedRowToTargetBucket(
                    indexTableId,
                    targetBucketId,
                    offset,
                    walRecord.getChangeType(),
                    indexedRow,
                    batchStartOffset);
        }
    }

    /**
     * Finalizes batch processing by synchronizing all buckets to the same offset level. This method
     * ensures that all buckets have consistent offset progression while minimizing the number of
     * empty row writes.
     *
     * @param lastOffset the last offset in the batch
     * @param batchStartOffset the start offset of the batch
     * @throws Exception if an error occurs during finalization
     */
    public void finalizeBatch(long lastOffset, long batchStartOffset) throws Exception {
        // Write empty rows to all buckets to ensure they are synchronized to the same offset level
        // This single write per batch replaces multiple empty row writes during batch processing
        indexRowCache.synchronizeAllBucketsToOffset(indexTableId, lastOffset, batchStartOffset);
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
