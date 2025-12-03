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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.write.ClientColumnLockManager;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the primary key table. */
class UpsertWriterImpl extends AbstractTableWriter implements UpsertWriter {
    private static final Logger LOG = LoggerFactory.getLogger(UpsertWriterImpl.class);
    private static final UpsertResult UPSERT_SUCCESS = new UpsertResult();
    private static final DeleteResult DELETE_SUCCESS = new DeleteResult();

    private final TableInfo tableInfo;
    private final KeyEncoder primaryKeyEncoder;
    private final @Nullable int[] targetColumns;
    private final @Nullable ClientColumnLockManager columnLockManager;

    // same to primaryKeyEncoder if the bucket key is the same to the primary key
    private final KeyEncoder bucketKeyEncoder;

    private final KvFormat kvFormat;
    private final RowEncoder rowEncoder;
    private final FieldGetter[] fieldGetters;

    // flag to control whether to use overwrite mode for all subsequent writes
    private volatile boolean overwriteMode = false;

    // Column lock management
    private final @Nullable ClientColumnLockManager.LockKey lockKey;
    private final @Nullable String lockOwnerId;

    UpsertWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            @Nullable int[] partialUpdateColumns,
            WriterClient writerClient,
            @Nullable ClientColumnLockManager columnLockManager) {
        super(tablePath, tableInfo, writerClient);
        this.tableInfo = tableInfo;
        RowType rowType = tableInfo.getRowType();
        sanityCheck(rowType, tableInfo.getPrimaryKeys(), partialUpdateColumns);

        this.targetColumns = partialUpdateColumns;
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        // encode primary key using physical primary key
        this.primaryKeyEncoder =
                KeyEncoder.of(rowType, tableInfo.getPhysicalPrimaryKeys(), lakeFormat);
        this.bucketKeyEncoder =
                tableInfo.isDefaultBucketKey()
                        ? primaryKeyEncoder
                        : KeyEncoder.of(rowType, tableInfo.getBucketKeys(), lakeFormat);

        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.rowEncoder = RowEncoder.create(kvFormat, rowType);
        this.fieldGetters = InternalRow.createFieldGetters(rowType);

        // Initialize column lock management
        this.lockOwnerId = writerClient.getLockOwnerId();
        this.columnLockManager = columnLockManager;

        // Check if column lock is required and acquire it
        boolean columnLockRequired =
                tableInfo.getTableConfig().get(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED);
        if (columnLockRequired) {
            if (lockOwnerId == null) {
                throw new FlussRuntimeException(
                        String.format(
                                "Table %s requires column locks, but WriterClient was not configured with a lock owner ID. "
                                        + "Please set '%s' when creating the WriterClient.",
                                tablePath, ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID.key()));
            }

            if (columnLockManager == null) {
                throw new FlussRuntimeException(
                        String.format(
                                "Table %s requires column locks, but column lock manager could not be created.",
                                tablePath));
            }

            // Acquire column lock for the target columns
            // Note: Primary key columns are excluded from locking as they are always required
            // for partial updates and do not cause conflicts
            List<String> pkNames = tableInfo.getPrimaryKeys();
            int[] primaryKeyIndexes = new int[pkNames.size()];
            for (int i = 0; i < pkNames.size(); i++) {
                primaryKeyIndexes[i] = rowType.getFieldIndex(pkNames.get(i));
            }
            int[] lockColumns = filterOutPrimaryKeyColumns(targetColumns, primaryKeyIndexes);

            try {
                // For non-partitioned tables, partitionId is null
                // For partitioned tables, we will acquire locks for all partitions that are written
                // For now, we acquire the lock at table level (partitionId = null)
                // TODO: Optimize to acquire locks per partition when writing to partitioned tables
                this.lockKey =
                        columnLockManager
                                .acquireLock(
                                        tableInfo.getTableId(),
                                        null, // null means table-level lock for now
                                        lockOwnerId,
                                        tableInfo.getSchemaId(),
                                        lockColumns,
                                        null)
                                .get();
                LOG.info(
                        "Acquired column lock for table {} with lockColumns {} (filtered from targetColumns {}) and owner ID {}",
                        tablePath,
                        lockColumns != null ? java.util.Arrays.toString(lockColumns) : "all",
                        targetColumns != null ? java.util.Arrays.toString(targetColumns) : "all",
                        lockOwnerId);
            } catch (Exception e) {
                // Unwrap ExecutionException to get the root cause
                Throwable cause = e;
                if (e instanceof java.util.concurrent.ExecutionException && e.getCause() != null) {
                    cause = e.getCause();
                }
                String errorMessage =
                        String.format(
                                "Failed to acquire column lock for table %s with owner ID %s: %s",
                                tablePath, lockOwnerId, cause.getMessage());
                throw new FlussRuntimeException(errorMessage, cause);
            }
        } else {
            this.lockKey = null;
        }
    }

    private static void sanityCheck(
            RowType rowType, List<String> primaryKeys, @Nullable int[] targetColumns) {
        // skip check when target columns is null
        if (targetColumns == null) {
            return;
        }
        BitSet targetColumnsSet = new BitSet();
        for (int targetColumnIndex : targetColumns) {
            targetColumnsSet.set(targetColumnIndex);
        }

        BitSet pkColumnSet = new BitSet();
        // check the target columns contains the primary key
        for (String key : primaryKeys) {
            int pkIndex = rowType.getFieldIndex(key);
            if (!targetColumnsSet.get(pkIndex)) {
                throw new IllegalArgumentException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                rowType.project(targetColumns).getFieldNames(), primaryKeys));
            }
            pkColumnSet.set(pkIndex);
        }

        // check the columns not in targetColumns should be nullable
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            // column not in primary key
            if (!pkColumnSet.get(i)) {
                // the column should be nullable
                if (!rowType.getTypeAt(i).isNullable()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partial Update requires all columns except primary key to be nullable, but column %s is NOT NULL.",
                                    rowType.getFieldNames().get(i)));
                }
            }
        }
    }

    /**
     * Filter out primary key columns from target columns for lock acquisition.
     *
     * <p>Primary key columns are always required for partial updates (to locate the row) but don't
     * need to participate in lock conflict detection since they cannot be updated.
     *
     * @param targetColumns the target columns to write (can be null for all columns)
     * @param primaryKeyIndexes the indexes of primary key columns
     * @return filtered column indexes excluding primary keys, or null if targetColumns is null
     */
    private static @Nullable int[] filterOutPrimaryKeyColumns(
            @Nullable int[] targetColumns, int[] primaryKeyIndexes) {
        if (targetColumns == null) {
            // null means all columns, keep as null
            return null;
        }

        // Create a set of primary key indexes for quick lookup
        BitSet pkSet = new BitSet();
        for (int pkIdx : primaryKeyIndexes) {
            pkSet.set(pkIdx);
        }

        // Filter out primary key columns
        int nonPkCount = 0;
        for (int col : targetColumns) {
            if (!pkSet.get(col)) {
                nonPkCount++;
            }
        }

        // If all columns are primary keys, return null (lock all non-PK columns)
        if (nonPkCount == 0) {
            return null;
        }

        int[] result = new int[nonPkCount];
        int idx = 0;
        for (int col : targetColumns) {
            if (!pkSet.get(col)) {
                result[idx++] = col;
            }
        }

        return result;
    }

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    @Override
    public CompletableFuture<UpsertResult> upsert(InternalRow row) {
        checkFieldCount(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forUpsert(
                        getPhysicalPath(row), encodeRow(row), key, bucketKey, targetColumns);
        return send(record).thenApply(ignored -> UPSERT_SUCCESS);
    }

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    @Override
    public CompletableFuture<DeleteResult> delete(InternalRow row) {
        checkFieldCount(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forDelete(getPhysicalPath(row), key, bucketKey, targetColumns);
        return send(record).thenApply(ignored -> DELETE_SUCCESS);
    }

    @Override
    public void setOverwriteMode(boolean overwriteMode) {
        // If the overwrite mode is changing, we need to close and flush all in-progress batches
        // to ensure batches with the old mode are sent before creating new batches with the new
        // mode
        if (this.overwriteMode != overwriteMode) {
            writerClient.closeInProgressBatchesAndFlush();
            // Set the overwrite mode in the writer client's accumulator
            writerClient.setOverwriteMode(overwriteMode);
        }
        this.overwriteMode = overwriteMode;
    }

    @Override
    public Map<TableBucket, Long> bucketsAckStatus() {
        // Get bucket offset tracker from writer client
        // The tracker maintains the latest acknowledged offset for each bucket written by this
        // writer
        try {
            Map<TableBucket, Long> allOffsets =
                    writerClient.getBucketOffsetTracker().getAllOffsets();
            // Filter to only include buckets for this table
            Map<TableBucket, Long> tableBuckets = new HashMap<>();
            long tableId = tableInfo.getTableId();
            for (Map.Entry<TableBucket, Long> entry : allOffsets.entrySet()) {
                TableBucket bucket = entry.getKey();
                if (bucket.getTableId() == tableId) {
                    tableBuckets.put(bucket, entry.getValue());
                }
            }
            return Collections.unmodifiableMap(tableBuckets);
        } catch (Exception e) {
            // If bucket offset tracker is not available, return empty map
            return Collections.emptyMap();
        }
    }

    private BinaryRow encodeRow(InternalRow row) {
        if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
            return (IndexedRow) row;
        } else if (kvFormat == KvFormat.COMPACTED && row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        // encode the row to target format
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return rowEncoder.finishRow();
    }

    @Override
    public void close() throws Exception {
        // Release column lock if it was acquired
        if (lockKey != null && lockOwnerId != null && columnLockManager != null) {
            try {
                columnLockManager.releaseLock(lockKey, lockOwnerId).get();
                LOG.info(
                        "Released column lock for table {} with owner ID {}",
                        tablePath,
                        lockOwnerId);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to release column lock for table {} with owner ID {}",
                        tablePath,
                        lockOwnerId,
                        e);
                // Don't throw exception here to allow other cleanup to proceed
            }
        }
    }
}
