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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.CompactedLogRecord;
import org.apache.fluss.record.DefaultKvRecord;
import org.apache.fluss.record.IndexedLogRecord;
import org.apache.fluss.record.KvMutationType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.protocol.MergeMode;

import javax.annotation.Nullable;

import static org.apache.fluss.record.DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A record to write to a table. It can represent an upsert operation, a delete operation, or an
 * append operation.
 */
@Internal
public final class WriteRecord {

    /** Create a write record for upsert operation and partial-upsert operation. */
    public static WriteRecord forUpsert(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            BinaryRow row,
            byte[] key,
            byte[] bucketKey,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns) {
        return forUpsert(
                tableInfo,
                tablePath,
                row,
                key,
                bucketKey,
                writeFormat,
                targetColumns,
                MergeMode.DEFAULT);
    }

    /** Create a write record for upsert operation with merge mode control. */
    public static WriteRecord forUpsert(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            BinaryRow row,
            byte[] key,
            byte[] bucketKey,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns,
            MergeMode mergeMode) {
        checkNotNull(row, "row must not be null");
        checkNotNull(key, "key must not be null");
        checkNotNull(bucketKey, "bucketKey must not be null");
        checkArgument(writeFormat.isKv(), "writeFormat must be a KV format");
        int estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, row) + RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tableInfo,
                tablePath,
                key,
                bucketKey,
                row,
                writeFormat,
                targetColumns,
                estimatedSizeInBytes,
                mergeMode,
                KvMutationType.UPSERT);
    }

    /** Create a write record for delete operation and partial-delete update. */
    public static WriteRecord forDelete(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            byte[] key,
            byte[] bucketKey,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns) {
        return forDelete(
                tableInfo,
                tablePath,
                key,
                bucketKey,
                writeFormat,
                targetColumns,
                MergeMode.DEFAULT);
    }

    /** Create a write record for delete operation with merge mode control. */
    public static WriteRecord forDelete(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            byte[] key,
            byte[] bucketKey,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns,
            MergeMode mergeMode) {
        checkNotNull(key, "key must not be null");
        checkNotNull(bucketKey, "bucketKey must not be null");
        checkArgument(writeFormat.isKv(), "writeFormat must be a KV format");
        int estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, null) + RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tableInfo,
                tablePath,
                key,
                bucketKey,
                null,
                writeFormat,
                targetColumns,
                estimatedSizeInBytes,
                mergeMode,
                KvMutationType.DELETE);
    }

    /** Create a write record for retract operation (inverse aggregation on aggregation tables). */
    public static WriteRecord forRetract(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            BinaryRow row,
            byte[] key,
            byte[] bucketKey,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns) {
        checkNotNull(row, "retract row must not be null");
        checkNotNull(key, "key must not be null");
        checkNotNull(bucketKey, "bucketKey must not be null");
        checkArgument(writeFormat.isKv(), "writeFormat must be a KV format");
        int estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, row) + RECORD_BATCH_HEADER_SIZE;
        return new WriteRecord(
                tableInfo,
                tablePath,
                key,
                bucketKey,
                row,
                writeFormat,
                targetColumns,
                estimatedSizeInBytes,
                // Retract always uses DEFAULT mode; OVERWRITE is for undo recovery only
                MergeMode.DEFAULT,
                KvMutationType.RETRACT);
    }

    /** Create a write record for append operation for indexed format. */
    public static WriteRecord forIndexedAppend(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            IndexedRow row,
            @Nullable byte[] bucketKey) {
        checkNotNull(row);
        int estimatedSizeInBytes =
                IndexedLogRecord.sizeOf(row) + recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE);
        return new WriteRecord(
                tableInfo,
                tablePath,
                null,
                bucketKey,
                row,
                WriteFormat.INDEXED_LOG,
                null,
                estimatedSizeInBytes,
                MergeMode.DEFAULT,
                null);
    }

    /** Creates a write record for append operation for Arrow format. */
    public static WriteRecord forArrowAppend(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            InternalRow row,
            @Nullable byte[] bucketKey) {
        checkNotNull(row);
        // the write row maybe GenericRow, can't estimate the size.
        // it is not necessary to estimate size for Arrow format.
        int estimatedSizeInBytes = -1;
        return new WriteRecord(
                tableInfo,
                tablePath,
                null,
                bucketKey,
                row,
                WriteFormat.ARROW_LOG,
                null,
                estimatedSizeInBytes,
                MergeMode.DEFAULT,
                null);
    }

    /** Creates a write record for append operation for Compacted format. */
    public static WriteRecord forCompactedAppend(
            TableInfo tableInfo,
            PhysicalTablePath tablePath,
            CompactedRow row,
            @Nullable byte[] bucketKey) {
        checkNotNull(row);
        int estimatedSizeInBytes =
                CompactedLogRecord.sizeOf(row) + recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE);
        return new WriteRecord(
                tableInfo,
                tablePath,
                null,
                bucketKey,
                row,
                WriteFormat.COMPACTED_LOG,
                null,
                estimatedSizeInBytes,
                MergeMode.DEFAULT,
                null);
    }

    // ------------------------------------------------------------------------------------------

    private final PhysicalTablePath physicalTablePath;

    private final @Nullable byte[] key;
    private final @Nullable byte[] bucketKey;
    private final @Nullable InternalRow row;
    private final WriteFormat writeFormat;

    // will be null if it's not for partial update
    private final @Nullable int[] targetColumns;
    private final int estimatedSizeInBytes;
    private final TableInfo tableInfo;

    /**
     * The merge mode for this record: DEFAULT (normal merge) or OVERWRITE (bypass merge engine).
     */
    private final MergeMode mergeMode;

    /** Normalized mutation type for KV writes. Null for append-only log writes. */
    private final @Nullable KvMutationType kvMutationType;

    private WriteRecord(
            TableInfo tableInfo,
            PhysicalTablePath physicalTablePath,
            @Nullable byte[] key,
            @Nullable byte[] bucketKey,
            @Nullable InternalRow row,
            WriteFormat writeFormat,
            @Nullable int[] targetColumns,
            int estimatedSizeInBytes,
            MergeMode mergeMode,
            @Nullable KvMutationType kvMutationType) {
        this.tableInfo = tableInfo;
        this.physicalTablePath = physicalTablePath;
        this.key = key;
        this.bucketKey = bucketKey;
        this.row = row;
        this.writeFormat = writeFormat;
        this.targetColumns = targetColumns;
        this.estimatedSizeInBytes = estimatedSizeInBytes;
        this.mergeMode = mergeMode;
        this.kvMutationType = kvMutationType;
        validateWriteSemantics();
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public @Nullable byte[] getKey() {
        return key;
    }

    public @Nullable byte[] getBucketKey() {
        return bucketKey;
    }

    public @Nullable InternalRow getRow() {
        return row;
    }

    @Nullable
    public int[] getTargetColumns() {
        return targetColumns;
    }

    public WriteFormat getWriteFormat() {
        return writeFormat;
    }

    public boolean isKvWrite() {
        return writeFormat.isKv();
    }

    /** Get the merge mode for this record. */
    public MergeMode getMergeMode() {
        return mergeMode;
    }

    /** Whether this record is a retract operation. */
    public boolean isRetract() {
        return kvMutationType == KvMutationType.RETRACT;
    }

    /** Returns the normalized mutation type for KV writes. Null for append-only log writes. */
    @Nullable
    public KvMutationType getKvMutationType() {
        return kvMutationType;
    }

    /** Returns the normalized mutation type for KV writes and fails fast for append-only writes. */
    public KvMutationType requireKvMutationType() {
        checkArgument(isKvWrite(), "kv mutation type is only available for kv write records");
        return checkNotNull(kvMutationType, "kv mutation type must not be null for kv write");
    }

    /** Returns the estimated size in bytes of the record with batch header. */
    public int getEstimatedSizeInBytes() {
        if (estimatedSizeInBytes < 0) {
            throw new IllegalStateException(
                    String.format(
                            "The estimated size in bytes is not supported for %s write format.",
                            writeFormat));
        }
        return estimatedSizeInBytes;
    }

    public int getSchemaId() {
        return tableInfo.getSchemaId();
    }

    private void validateWriteSemantics() {
        if (writeFormat.isKv()) {
            checkNotNull(kvMutationType, "kv mutation type must not be null for kv write");
            if (kvMutationType.requiresRowData()) {
                checkNotNull(
                        row, "row data must not be null for kv mutation type %s", kvMutationType);
            }
        } else {
            checkArgument(
                    kvMutationType == null,
                    "append-only write records must not carry kv mutation semantics");
        }
    }
}
