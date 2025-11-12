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
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

    // Auto-partition strategy for TTL, can be null if auto-partition is not enabled
    private final AutoPartitionStrategy autoPartitionStrategy;
    private final InternalRow.FieldGetter timePartitionFieldGetter;
    private final DataType timePartitionColumnType;

    public TableCacheWriter(
            long indexTableId,
            Schema tableSchema,
            Schema indexSchema,
            int indexBucketCount,
            List<String> bucketKeys,
            BucketingFunction bucketingFunction,
            IndexRowCache indexRowCache,
            @Nullable AutoPartitionStrategy autoPartitionStrategy) {
        this.indexTableId = indexTableId;
        this.indexSchema = indexSchema;
        this.indexBucketCount = indexBucketCount;
        this.bucketingFunction = bucketingFunction;
        this.indexRowCache = indexRowCache;
        this.autoPartitionStrategy = autoPartitionStrategy;

        // Get pre-computed time partition fields from auto-partition strategy
        if (autoPartitionStrategy != null) {
            this.timePartitionFieldGetter = autoPartitionStrategy.getTimePartitionFieldGetter();
            this.timePartitionColumnType = autoPartitionStrategy.getTimePartitionColumnType();
        } else {
            this.timePartitionFieldGetter = null;
            this.timePartitionColumnType = null;
        }

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

            // Extract partition column timestamp for TTL
            Long timestamp = extractPartitionTimestamp(walRecord.getRow());

            // Only write to target bucket, do not write empty rows to other buckets
            indexRowCache.writeIndexedRowToTargetBucket(
                    indexTableId,
                    targetBucketId,
                    offset,
                    walRecord.getChangeType(),
                    indexedRow,
                    batchStartOffset,
                    timestamp);
        }
    }

    /**
     * Extract partition column timestamp from main table record for TTL. This method only extracts
     * timestamps when the main table is a partitioned table with auto-partition enabled.
     *
     * @param row the main table row
     * @return the timestamp in milliseconds, or null if main table is non-partitioned or no time
     *     partitioning
     */
    @Nullable
    private Long extractPartitionTimestamp(InternalRow row) {
        if (timePartitionFieldGetter == null || timePartitionColumnType == null) {
            return null; // No auto-partition enabled or column not found
        }

        Object partitionValue = timePartitionFieldGetter.getFieldOrNull(row);
        if (partitionValue == null) {
            return null;
        }

        long timestamp = convertPartitionValueToTimestamp(partitionValue, timePartitionColumnType);
        return timestamp > 0 ? timestamp : null;
    }

    /**
     * Convert partition column value to milliseconds timestamp.
     *
     * @param value the partition column value
     * @param dataType the partition column data type
     * @return the timestamp in milliseconds, or -1 if conversion fails
     */
    private long convertPartitionValueToTimestamp(Object value, DataType dataType) {
        try {
            switch (dataType.getTypeRoot()) {
                case STRING:
                case CHAR:
                    // For STRING/CHAR partition columns, the value is a formatted time string
                    // Format depends on AutoPartitionTimeUnit: "yyyyMMddHH", "yyyyMMdd", etc.
                    return parsePartitionStringToTimestamp(
                            ((BinaryString) value).toString(), autoPartitionStrategy.timeUnit());

                case DATE:
                    // DATE: days since epoch -> milliseconds
                    Integer days = (Integer) value;
                    return days * 86400_000L;

                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampNtz timestampNtz = (TimestampNtz) value;
                    return timestampNtz.getMillisecond();

                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    TimestampLtz timestampLtz = (TimestampLtz) value;
                    return timestampLtz.getEpochMillisecond();

                case BIGINT:
                    // Assume BIGINT is milliseconds timestamp
                    return (Long) value;

                default:
                    LOG.warn(
                            "Unsupported partition column type for timestamp extraction: {}",
                            dataType);
                    return -1;
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract timestamp from partition value: {}", e.getMessage());
            return -1;
        }
    }

    /**
     * Parse partition string value to milliseconds timestamp based on AutoPartitionTimeUnit. This
     * method delegates to AutoPartitionTimeUnit which maintains its own cache.
     *
     * @param partitionString the partition string value
     * @param timeUnit the auto partition time unit
     * @return the timestamp in milliseconds, or -1 if parsing fails
     */
    private long parsePartitionStringToTimestamp(
            String partitionString, AutoPartitionTimeUnit timeUnit) {
        if (timeUnit == null) {
            return -1;
        }
        // Delegate to AutoPartitionTimeUnit which maintains its own cache
        return timeUnit.parsePartitionStringToTimestamp(partitionString);
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
