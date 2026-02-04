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
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Extracts index data for a single index table from data table WAL records and writes to
 * IndexBucketCacheManager. This provides the core functionality for the unified hot/cold data
 * processing pipeline.
 *
 * <p>Each SingleIndexExtractor handles one index definition and is responsible for:
 *
 * <ul>
 *   <li>Extracting index columns from data table rows
 *   <li>Building IndexedRow with proper encoding
 *   <li>Computing target bucket based on bucket keys
 *   <li>Writing to the appropriate bucket in IndexBucketCacheManager
 * </ul>
 */
public final class SingleIndexExtractor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SingleIndexExtractor.class);

    private final long indexTableId;
    private final int indexBucketCount;
    private final BucketingFunction bucketingFunction;
    private final IndexBucketCacheManager cacheManager;

    // For building IndexedRow
    private final IndexedRowEncoder indexedRowEncoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final KeyEncoder bucketKeyEncoder;

    private final int[] indexFieldIndicesOfDataRow;

    private final boolean kvTtlEnabled;

    // TTL extraction fields - only initialized when auto-partition is enabled
    private final @Nullable AutoPartitionTimeUnit timeUnit;
    private final @Nullable ZoneId timeZone;
    private final @Nullable InternalRow.FieldGetter timePartitionFieldGetter;
    private final @Nullable DataType timePartitionColumnType;

    public SingleIndexExtractor(
            long indexTableId,
            Schema tableSchema,
            Schema indexSchema,
            int indexBucketCount,
            List<String> bucketKeys,
            BucketingFunction bucketingFunction,
            IndexBucketCacheManager cacheManager,
            @Nullable AutoPartitionStrategy autoPartitionStrategy) {
        this.indexTableId = indexTableId;
        this.indexBucketCount = indexBucketCount;
        this.bucketingFunction = bucketingFunction;
        this.cacheManager = cacheManager;

        // Initialize TTL extraction fields from auto-partition strategy
        if (autoPartitionStrategy != null && autoPartitionStrategy.isAutoPartitionEnabled()) {
            this.timeUnit = autoPartitionStrategy.timeUnit();
            this.timeZone = autoPartitionStrategy.timeZone().toZoneId();
            String partitionKey = autoPartitionStrategy.key();
            RowType rowType = tableSchema.getRowType();
            int columnIndex = tableSchema.getColumnNames().indexOf(partitionKey);
            if (columnIndex >= 0) {
                this.timePartitionColumnType = rowType.getTypeAt(columnIndex);
                this.timePartitionFieldGetter =
                        InternalRow.createFieldGetter(timePartitionColumnType, columnIndex);
            } else {
                this.timePartitionColumnType = null;
                this.timePartitionFieldGetter = null;
            }
        } else {
            this.timeUnit = null;
            this.timeZone = null;
            this.timePartitionFieldGetter = null;
            this.timePartitionColumnType = null;
        }

        // Create IndexedRowEncoder for row conversion
        this.indexedRowEncoder = new IndexedRowEncoder(indexSchema.getRowType());

        int ttlFieldIndex =
                indexSchema.getRowType().getFieldIndex(TableDescriptor.INDEX_TTL_COLUMN_NAME);
        this.kvTtlEnabled = ttlFieldIndex >= 0;
        if (kvTtlEnabled
                && indexSchema.getRowType().getTypeAt(ttlFieldIndex).getTypeRoot()
                        != DataTypeRoot.BIGINT) {
            throw new IllegalStateException(
                    "TTL column '"
                            + TableDescriptor.INDEX_TTL_COLUMN_NAME
                            + "' must be BIGINT (epoch millis).");
        }

        this.indexFieldIndicesOfDataRow =
                tableSchema.getColumnIndexes(indexSchema.getPrimaryKeyColumnNames());

        // Create field getters only for index columns (not all table columns)
        // This avoids creating field getters for unsupported complex types (MAP, ARRAY, ROW, etc.)
        // that may exist in the table schema but are not part of the index
        this.fieldGetters = new InternalRow.FieldGetter[tableSchema.getRowType().getFieldCount()];
        for (int dataColumnIndex : indexFieldIndicesOfDataRow) {
            if (dataColumnIndex >= 0) {
                fieldGetters[dataColumnIndex] =
                        InternalRow.createFieldGetter(
                                tableSchema.getRowType().getTypeAt(dataColumnIndex),
                                dataColumnIndex);
            }
        }
        this.bucketKeyEncoder = KeyEncoder.of(indexSchema.getRowType(), bucketKeys, null);
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
            // Check if any index column has NULL value
            if (hasNullIndexColumn(walRecord.getRow())) {
                LOG.warn(
                        "Skipping record at offset {} for index table {} due to null index column value. "
                                + "This should not happen if schema validation is working correctly.",
                        offset,
                        indexTableId);
                return; // Skip this record
            }

            long ttlTimestampMillis =
                    kvTtlEnabled ? extractPartitionTimestamp(walRecord.getRow()) : Long.MAX_VALUE;
            IndexedRow indexedRow = createIndexedRow(walRecord, ttlTimestampMillis);
            byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(indexedRow);
            int targetBucketId = bucketingFunction.bucketing(bucketKeyBytes, indexBucketCount);

            // Only write to target bucket, do not write empty rows to other buckets
            cacheManager.writeIndexedRowToTargetBucket(
                    indexTableId,
                    targetBucketId,
                    offset,
                    walRecord.getChangeType(),
                    indexedRow,
                    batchStartOffset,
                    ttlTimestampMillis);
        }
    }

    /**
     * Extract partition column timestamp from main table record for TTL. This method only extracts
     * timestamps when the main table is a partitioned table with auto-partition enabled.
     *
     * @param row the main table row
     * @return the timestamp in milliseconds. If partition time cannot be derived, returns {@link
     *     Long#MAX_VALUE} to indicate "never expire" semantics.
     */
    private long extractPartitionTimestamp(InternalRow row) {
        InternalRow.FieldGetter fieldGetter = timePartitionFieldGetter;
        DataType partitionType = timePartitionColumnType;
        if (fieldGetter == null || partitionType == null) {
            // No auto-partition enabled or column not found
            return Long.MAX_VALUE;
        }

        Object partitionValue = fieldGetter.getFieldOrNull(row);
        if (partitionValue == null) {
            return Long.MAX_VALUE;
        }

        long timestamp = convertPartitionValueToTimestamp(partitionValue, partitionType);
        return timestamp > 0 ? timestamp : Long.MAX_VALUE;
    }

    /**
     * Convert partition column value to milliseconds timestamp.
     *
     * @param value the partition column value
     * @return the timestamp in milliseconds, or -1 if conversion fails
     */
    private long convertPartitionValueToTimestamp(Object value, DataType partitionType) {
        ZoneId zoneId = timeZone != null ? timeZone : ZoneOffset.UTC;
        try {
            switch (partitionType.getTypeRoot()) {
                case STRING:
                case CHAR:
                    // For STRING/CHAR partition columns, the value is a formatted time string
                    return parsePartitionStringToTimestamp(((BinaryString) value).toString());

                case DATE:
                    // DATE: days since epoch -> milliseconds (using configured time zone)
                    return LocalDate.ofEpochDay((Integer) value)
                            .atStartOfDay(zoneId)
                            .toInstant()
                            .toEpochMilli();

                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return ((TimestampNtz) value)
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();

                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return ((TimestampLtz) value).getEpochMillisecond();

                case BIGINT:
                    // Assume BIGINT is milliseconds timestamp
                    return (Long) value;

                default:
                    LOG.warn(
                            "Unsupported partition column type for timestamp extraction: {}",
                            partitionType);
                    return -1;
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract timestamp from partition value: {}", e.getMessage());
            return -1;
        }
    }

    /**
     * Parse partition string value to milliseconds timestamp based on AutoPartitionTimeUnit.
     *
     * <p>The string format depends on the time unit:
     *
     * <ul>
     *   <li>HOUR: "yyyyMMddHH" (e.g., "2025050808")
     *   <li>DAY: "yyyyMMdd" (e.g., "20250508")
     *   <li>MONTH: "yyyyMM" (e.g., "202505")
     *   <li>YEAR: "yyyy" (e.g., "2025")
     *   <li>QUARTER: "yyyyQ#" (e.g., "2025Q2")
     * </ul>
     *
     * @param partitionString the partition string value
     * @return the timestamp in milliseconds, or -1 if parsing fails
     */
    private long parsePartitionStringToTimestamp(String partitionString) {
        AutoPartitionTimeUnit unit = timeUnit;
        if (unit == null || partitionString == null || partitionString.isEmpty()) {
            return -1;
        }
        ZoneId zoneId = timeZone != null ? timeZone : ZoneOffset.UTC;

        try {
            switch (unit) {
                case HOUR:
                    DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
                    LocalDateTime hourDateTime =
                            LocalDateTime.parse(partitionString, hourFormatter);
                    return hourDateTime.atZone(zoneId).toInstant().toEpochMilli();

                case DAY:
                    DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                    LocalDate dayDate = LocalDate.parse(partitionString, dayFormatter);
                    return dayDate.atStartOfDay(zoneId).toInstant().toEpochMilli();

                case MONTH:
                    LocalDate monthDate =
                            LocalDate.parse(
                                    partitionString + "01",
                                    DateTimeFormatter.ofPattern("yyyyMMdd"));
                    return monthDate.atStartOfDay(zoneId).toInstant().toEpochMilli();

                case YEAR:
                    LocalDate yearDate =
                            LocalDate.parse(
                                    partitionString + "0101",
                                    DateTimeFormatter.ofPattern("yyyyMMdd"));
                    return yearDate.atStartOfDay(zoneId).toInstant().toEpochMilli();

                case QUARTER:
                    int qIndex = partitionString.indexOf('Q');
                    if (qIndex > 0) {
                        int year = Integer.parseInt(partitionString.substring(0, qIndex));
                        int quarter = Integer.parseInt(partitionString.substring(qIndex + 1));
                        int month = (quarter - 1) * 3 + 1;
                        LocalDate quarterDate = LocalDate.of(year, month, 1);
                        return quarterDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
                    }
                    return -1;

                default:
                    LOG.warn("Unsupported AutoPartitionTimeUnit for string parsing: {}", unit);
                    return -1;
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to parse partition string '{}' with time unit {}: {}",
                    partitionString,
                    unit,
                    e.getMessage());
            return -1;
        }
    }

    /**
     * Finalizes batch processing by synchronizing all buckets to the same offset level. This method
     * ensures that all buckets have consistent offset progression while minimizing the number of
     * empty row writes.
     *
     * <p>The targetOffset parameter should be the last processed offset (batch.lastLogOffset()).
     * Since synchronizeAllBucketsToOffset expands Range to targetOffset+1, the final Range
     * endOffset will equal batch.nextLogOffset(), correctly covering all processed offsets.
     *
     * @param targetOffset the last processed offset in the batch (batch.lastLogOffset())
     * @param batchStartOffset the start offset of the batch
     * @throws Exception if an error occurs during finalization
     */
    public void finalizeBatch(long targetOffset, long batchStartOffset) throws Exception {
        // Write empty rows to all buckets to ensure they are synchronized to the same offset level
        // This single write per batch replaces multiple empty row writes during batch processing
        cacheManager.synchronizeAllBucketsToOffset(indexTableId, targetOffset, batchStartOffset);
    }

    private IndexedRow createIndexedRow(LogRecord walRecord, long ttlTimestampMillis) {
        indexedRowEncoder.startNewRow();

        int fieldIndex = 0;

        for (int dataColumnIndex : indexFieldIndicesOfDataRow) {
            if (dataColumnIndex >= 0) {
                Object value = fieldGetters[dataColumnIndex].getFieldOrNull(walRecord.getRow());
                indexedRowEncoder.encodeField(fieldIndex++, value);
            }
        }

        if (kvTtlEnabled) {
            // Append system TTL timestamp column (epoch millis). If partition time cannot be
            // derived, use Long.MAX_VALUE to indicate "never expire" semantics.
            indexedRowEncoder.encodeField(fieldIndex, ttlTimestampMillis);
        }
        return indexedRowEncoder.finishRow();
    }

    /**
     * Checks if any index column in the given row has a NULL value.
     *
     * <p>This method is used as a defensive check to detect NULL values in index columns at
     * runtime. While schema validation should prevent NULL values in index columns, this serves as
     * a last line of defense.
     *
     * @param row the data row to check
     * @return true if any index column has a NULL value, false otherwise
     */
    private boolean hasNullIndexColumn(InternalRow row) {
        for (int dataColumnIndex : indexFieldIndicesOfDataRow) {
            if (dataColumnIndex >= 0 && fieldGetters[dataColumnIndex] != null) {
                if (fieldGetters[dataColumnIndex].getFieldOrNull(row) == null) {
                    return true;
                }
            }
        }
        return false;
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
