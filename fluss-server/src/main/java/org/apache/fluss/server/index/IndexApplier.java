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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.MapUtils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * IndexApplier is a stateful component held by each index table bucket's Leader Replica to manage
 * index data application and updates.
 *
 * <p>Core Features:
 *
 * <ul>
 *   <li>Applies index records from ReplicaFetcherThread to KvTablet's KvPreWriteBuffer
 *   <li>Writes index data to WAL and manages unified state tracking via IndexApplyStatus
 *   <li>Tracks index application progress for each data bucket with comprehensive status
 *       information
 *   <li>Provides index commit offsets for reporting progress to upstream data buckets
 * </ul>
 *
 * <p>State Management: Uses a unified Map&lt;TableBucket, IndexApplyStatus&gt; to track all status
 * information for each upstream data bucket, including last applied record ranges, offset mappings,
 * and commit progress. This design eliminates the need for separate offset mapping and applied
 * offset tracking.
 *
 * <p>Lifecycle: IndexApplier is initialized when a Replica becomes a Leader and destroyed when the
 * Replica is demoted or goes offline.
 *
 * <p>Thread Safety: This class provides thread-safe operations using locks to protect internal
 * state variables and ensure atomic index application and state updates.
 */
@Internal
public final class IndexApplier implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexApplier.class);

    private final KvTablet kvTablet;
    private final LogTablet logTablet;
    private final FatalErrorHandler fatalErrorHandler;
    private final RowType indexRowType;
    private final KeyEncoder indexKeyEncoder;

    // Lock for protecting internal state
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Internal state management - IndexApplyStatus for each data bucket
    // Key: TableBucket (upstream data bucket), Value: IndexApplyStatus (tracking apply progress)
    // IndexApplyStatus encapsulates all necessary state information including:
    // - last applied log records range in both data and index bucket address spaces
    // - index commit offset for reporting progress to upstream data buckets
    @GuardedBy("lock")
    private final Map<TableBucket, IndexApplyStatus> dataBucketStatusMap =
            MapUtils.newConcurrentHashMap();

    @GuardedBy("lock")
    private final Map<TableBucket, List<IndexApplyParams>> uncommittedApplyMap =
            Maps.newConcurrentMap();

    @GuardedBy("lock")
    private volatile boolean closed = false;

    /**
     * Creates a new IndexApplier with simplified parameters.
     *
     * @param kvTablet the KV tablet for index data storage
     * @param logTablet the log tablet for WAL operations
     * @param fatalErrorHandler the fatal error handler
     * @param indexTableSchema the schema of the index table
     */
    public IndexApplier(
            KvTablet kvTablet,
            LogTablet logTablet,
            FatalErrorHandler fatalErrorHandler,
            Schema indexTableSchema) {
        this.kvTablet = checkNotNull(kvTablet, "kvTablet cannot be null");
        this.logTablet = checkNotNull(logTablet, "logTablet cannot be null");
        this.fatalErrorHandler =
                checkNotNull(fatalErrorHandler, "fatalErrorHandler cannot be null");
        checkNotNull(indexTableSchema, "indexTableSchema cannot be null");

        // Extract RowType from Schema
        this.indexRowType = indexTableSchema.getRowType();

        // Create KeyEncoder for index table primary key
        this.indexKeyEncoder =
                KeyEncoder.of(this.indexRowType, indexTableSchema.getPrimaryKeyColumnNames(), null);

        LOG.info("IndexApplier initialized for index bucket {}", kvTablet.getTableBucket());
    }

    /**
     * Apply index records from the specified parameters. This is the main interface for processing
     * index data from ReplicaFetcherThread.
     *
     * @param records the index records (MemoryLogRecords) containing data from a specific data
     *     bucket
     * @param startOffset the start offset of the WAL address space (inclusive)
     * @param endOffset the end offset of the WAL address space (exclusive)
     * @param dataBucket the upstream data table bucket that generated these index records
     * @return the new applied (not committed) offset after processing
     * @throws Exception if an error occurs during index application
     */
    public long applyIndexRecords(
            MemoryLogRecords records, long startOffset, long endOffset, TableBucket dataBucket)
            throws Exception {
        checkNotNull(records, "records cannot be null");
        checkNotNull(dataBucket, "dataBucket cannot be null");

        LOG.info(
                "indexBucket {} applying index records from range [{}, {}) of data bucket {}, {} bytes of index data",
                kvTablet.getTableBucket(),
                startOffset,
                endOffset,
                dataBucket,
                records.sizeInBytes());

        if (startOffset > endOffset) {
            throw new IllegalArgumentException(
                    "Start offset must be less than end offset. "
                            + "startOffset: "
                            + startOffset
                            + ", endOffset: "
                            + endOffset);
        }

        if (closed) {
            LOG.warn("IndexApplier is closed, skipping index application");
            return startOffset;
        }

        return inWriteLock(
                lock,
                () -> {
                    try {
                        return doApplyIndexRecords(records, startOffset, endOffset, dataBucket);
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to apply index records from range [{}, {})",
                                startOffset,
                                endOffset,
                                e);
                        fatalErrorHandler.onFatalError(e);
                        throw e;
                    }
                });
    }

    /**
     * Get the index commit offset for the specified data bucket. This represents the progress of
     * index application for reporting to upstream data buckets.
     *
     * @param dataBucket the data table bucket
     * @return the index commit offset, or -1 if no progress has been made for this data bucket
     */
    public long getIndexCommitOffset(TableBucket dataBucket) {
        return getOrInitIndexApplyStatus(dataBucket).getIndexCommitDataOffset();
    }

    private IndexApplyStatus getOrCreateIndexApplyStatusNoLocked(TableBucket dataBucket) {
        IndexApplyStatus status = dataBucketStatusMap.get(dataBucket);
        if (status != null) {
            return status;
        }
        status = new IndexApplyStatus(0, 0, 0);
        dataBucketStatusMap.put(dataBucket, status);
        return status;
    }

    public IndexApplyStatus getOrInitIndexApplyStatusLocked(TableBucket dataBucket) {
        return inWriteLock(lock, () -> getOrCreateIndexApplyStatusNoLocked(dataBucket));
    }

    /**
     * Get the IndexApplyStatus for the specified data bucket. This contains complete state
     * information including fetch offsets and commit offsets.
     *
     * @param dataBucket the data table bucket
     * @return the IndexApplyStatus, or null if no previous apply status exists for this data bucket
     */
    private @Nullable IndexApplyStatus getIndexApplyStatusLocked(TableBucket dataBucket) {
        return inReadLock(lock, () -> dataBucketStatusMap.get(dataBucket));
    }

    public IndexApplyStatus getOrInitIndexApplyStatus(TableBucket dataBucket) {
        IndexApplyStatus status = getIndexApplyStatusLocked(dataBucket);
        if (null == status) {
            return getOrInitIndexApplyStatusLocked(dataBucket);
        }
        return status;
    }

    /**
     * Update index commit data offset for all data buckets based on the high watermark. This method
     * calculates the correct indexCommitDataOffset value for each data bucket when the high
     * watermark is updated.
     *
     * @param highWatermark the current high watermark value
     */
    public void onUpdateHighWatermark(long highWatermark) {
        inWriteLock(
                lock,
                () -> {
                    if (closed) {
                        LOG.warn("IndexApplier is closed, skipping high watermark update");
                        return;
                    }

                    for (Map.Entry<TableBucket, List<IndexApplyParams>> entry :
                            uncommittedApplyMap.entrySet()) {
                        TableBucket dataBucket = entry.getKey();
                        List<IndexApplyParams> uncommittedApplies = entry.getValue();
                        IndexApplyStatus currentStatus =
                                getOrCreateIndexApplyStatusNoLocked(dataBucket);
                        long currentBucketIndexCommitOffset =
                                currentStatus.getIndexCommitDataOffset();
                        while (!uncommittedApplies.isEmpty()) {
                            IndexApplyParams applyToCheck = uncommittedApplies.get(0);
                            if (applyToCheck.isRecordsEmpty()) {
                                currentBucketIndexCommitOffset =
                                        applyToCheck.getDataBucketEndOffset();
                                uncommittedApplies.remove(0);
                                continue;
                            } else if (highWatermark > applyToCheck.getIndexBucketStartOffset()) {
                                if (highWatermark >= applyToCheck.getIndexBucketEndOffset()) {
                                    currentBucketIndexCommitOffset =
                                            applyToCheck.getDataBucketEndOffset();
                                    uncommittedApplies.remove(0);
                                    continue;
                                } else {
                                    currentBucketIndexCommitOffset =
                                            applyToCheck.getDataBucketEndOffset()
                                                    - (applyToCheck.getIndexBucketEndOffset()
                                                            - highWatermark);
                                }
                            }
                            break;
                        }
                        dataBucketStatusMap.put(
                                dataBucket,
                                new IndexApplyStatus(
                                        currentStatus.getLastApplyRecordsDataEndOffset(),
                                        currentStatus.getLastApplyRecordsIndexEndOffset(),
                                        currentBucketIndexCommitOffset));
                        LOG.debug(
                                "Updated indexCommitDataOffset for data bucket {}: hw={}, newIndexCommitDataOffset={}",
                                dataBucket,
                                highWatermark,
                                currentBucketIndexCommitOffset);
                    }
                });
    }

    @Override
    public void close() {
        inWriteLock(
                lock,
                () -> {
                    if (!closed) {
                        closed = true;
                        // Keep internal state for read operations after close
                        // dataBucketStatusMap.clear(); // Don't clear to maintain state
                        LOG.info(
                                "IndexApplier closed for index bucket {}",
                                kvTablet.getTableBucket());
                    }
                });
    }

    // ================================================================================================
    // Internal Implementation Methods
    // ================================================================================================

    /**
     * Check if the MemoryLogRecords is empty (contains no actual log records).
     *
     * @param records the MemoryLogRecords to check
     * @return true if records is empty, false otherwise
     */
    private boolean isRecordsEmpty(LogRecords records) {
        // Check if it's the static EMPTY instance
        if (records == MemoryLogRecords.EMPTY) {
            return true;
        }
        // Check all batches for record count
        for (LogRecordBatch batch : records.batches()) {
            if (batch.getRecordCount() > 0) {
                return false;
            }
        }
        // All batches have zero records
        return true;
    }

    private void logErrorForInvalidApplyRange(
            TableBucket dataBucket,
            long startOffset,
            long endOffset,
            long lastApplyRecordsDataEndOffset) {
        if (startOffset <= lastApplyRecordsDataEndOffset) {
            LOG.error(
                    "IndexSegment already applied for data bucket {}: segmentRange=[{}, {}), "
                            + "currentApplied={}",
                    dataBucket,
                    startOffset,
                    endOffset,
                    lastApplyRecordsDataEndOffset);
        } else {
            LOG.error(
                    "Gap detected for IndexSegment {}: segmentStart={}, expected={}, "
                            + "skipping processing",
                    dataBucket,
                    startOffset,
                    lastApplyRecordsDataEndOffset);
        }
    }

    private long doApplyIndexRecords(
            MemoryLogRecords records, long startOffset, long endOffset, TableBucket dataBucket)
            throws Exception {
        TableBucket indexBucket = kvTablet.getTableBucket();

        // Get current status for this data bucket
        IndexApplyStatus currentStatus = getOrCreateIndexApplyStatusNoLocked(dataBucket);

        // Check if the segment is adjacent to the last applied records
        if (startOffset != currentStatus.getLastApplyRecordsDataEndOffset()) {
            logErrorForInvalidApplyRange(
                    dataBucket,
                    startOffset,
                    endOffset,
                    currentStatus.getLastApplyRecordsDataEndOffset());
            return currentStatus.getLastApplyRecordsDataEndOffset();
        }

        boolean isRecordsEmpty = isRecordsEmpty(records);
        List<IndexApplyParams> uncommittedApplies =
                uncommittedApplyMap.computeIfAbsent(dataBucket, tb -> new ArrayList<>());
        if (!isRecordsEmpty) {
            LogAppendInfo appendInfo = logTablet.appendAsLeader(records);
            processIndexRecordsToPreWriteBuffer(records, appendInfo);
            long indexBucketStartOffset = appendInfo.firstOffset();
            long indexBucketEndOffset = appendInfo.lastOffset() + 1;
            uncommittedApplies.add(
                    new IndexApplyParams(
                            endOffset, indexBucketStartOffset, indexBucketEndOffset, false));
            dataBucketStatusMap.put(
                    dataBucket,
                    new IndexApplyStatus(
                            endOffset,
                            indexBucketEndOffset,
                            currentStatus.getIndexCommitDataOffset()));
            LOG.info(
                    "Applied index records from data range [{}, {}) to index bucket {}, "
                            + "index bucket range: [{}:{}), currentIndexCommitDataOffset={}",
                    startOffset,
                    endOffset,
                    indexBucket,
                    indexBucketStartOffset,
                    indexBucketEndOffset,
                    currentStatus.getIndexCommitDataOffset());
        } else if (!uncommittedApplies.isEmpty()) {
            uncommittedApplies.add(
                    new IndexApplyParams(
                            endOffset,
                            logTablet.localLogEndOffset(),
                            logTablet.localLogEndOffset(),
                            true));
            dataBucketStatusMap.put(
                    dataBucket,
                    new IndexApplyStatus(
                            endOffset,
                            logTablet.localLogEndOffset(),
                            currentStatus.getIndexCommitDataOffset()));
        } else {
            dataBucketStatusMap.put(
                    dataBucket,
                    new IndexApplyStatus(endOffset, logTablet.localLogEndOffset(), endOffset));
        }
        return endOffset;
    }

    /**
     * Process index records and apply them to KV pre-write buffer using the allocated index bucket
     * offsets.
     *
     * @param records the index records to process
     * @param appendInfo the log append info containing allocated index bucket offsets
     * @throws Exception if processing fails
     */
    private void processIndexRecordsToPreWriteBuffer(
            MemoryLogRecords records, LogAppendInfo appendInfo) throws Exception {
        long currentIndexOffset = appendInfo.firstOffset();

        // Create read context for processing index records
        LogRecordReadContext readContext =
                LogRecordReadContext.createIndexedReadContext(indexRowType, 1);

        for (LogRecordBatch batch : records.batches()) {
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();

                    // Process the index record and apply to KvPreWriteBuffer
                    // Use the sequentially allocated index bucket offset
                    processIndexRecordToPreWriteBuffer(
                            batch.schemaId(), record, currentIndexOffset);
                    currentIndexOffset++;
                }
            }
        }
    }

    /**
     * Process a single index record and apply it to the KV pre-write buffer.
     *
     * @param record the index record to process
     * @param indexLogOffset the allocated log offset in index bucket address space
     * @throws Exception if processing fails
     */
    private void processIndexRecordToPreWriteBuffer(
            short schemaId, LogRecord record, long indexLogOffset) throws Exception {
        // Extract key and value from the index record
        IndexedRow indexedRow = (IndexedRow) record.getRow();
        // For index table, the key should be: [index columns] + [primary key columns]
        byte[] key = indexKeyEncoder.encodeKey(indexedRow);
        // For index table, the value is the complete IndexedRow
        byte[] valueBytes = ValueEncoder.encodeValue(schemaId, indexedRow);
        // Apply to KV pre-write buffer using the allocated index bucket offset
        // This leverages the fact that index records contain UB records and can be directly applied
        kvTablet.putToPreWriteBuffer(key, valueBytes, indexLogOffset);

        LOG.debug(
                "Index record applied to pre-write buffer: indexLogOffset={}, keySize={}, valueSize={}",
                indexLogOffset,
                key.length,
                valueBytes.length);
    }

    // ================================================================================================
    // Data Classes
    // ================================================================================================

    /** Status tracking for index application progress for a specific data bucket. */
    public static final class IndexApplyStatus {

        private final long lastApplyRecordsDataEndOffset;
        private final long lastApplyRecordsIndexEndOffset;
        private final long indexCommitDataOffset;

        public IndexApplyStatus(
                long lastApplyRecordsDataEndOffset,
                long lastApplyRecordsIndexEndOffset,
                long indexCommitDataOffset) {
            this.lastApplyRecordsDataEndOffset = lastApplyRecordsDataEndOffset;
            this.lastApplyRecordsIndexEndOffset = lastApplyRecordsIndexEndOffset;
            this.indexCommitDataOffset = indexCommitDataOffset;
        }

        public long getLastApplyRecordsDataEndOffset() {
            return lastApplyRecordsDataEndOffset;
        }

        public long getLastApplyRecordsIndexEndOffset() {
            return lastApplyRecordsIndexEndOffset;
        }

        public long getIndexCommitDataOffset() {
            return indexCommitDataOffset;
        }

        @Override
        public String toString() {
            return String.format(
                    "IndexApplyStatus{lastLogRecordsDataRange=[,%d), lastLogRecordsIndexRange=[,%d), indexCommitDataOffset=%d}",
                    lastApplyRecordsDataEndOffset,
                    lastApplyRecordsIndexEndOffset,
                    indexCommitDataOffset);
        }
    }

    private static final class IndexApplyParams {
        private final long dataBucketEndOffset;
        private final long indexBucketStartOffset;
        private final long indexBucketEndOffset;
        private final boolean isRecordsEmpty;

        public IndexApplyParams(
                long dataBucketEndOffset,
                long indexBucketStartOffset,
                long indexBucketEndOffset,
                boolean isRecordsEmpty) {
            this.dataBucketEndOffset = dataBucketEndOffset;
            this.indexBucketStartOffset = indexBucketStartOffset;
            this.indexBucketEndOffset = indexBucketEndOffset;
            this.isRecordsEmpty = isRecordsEmpty;
        }

        public long getDataBucketEndOffset() {
            return dataBucketEndOffset;
        }

        public boolean isRecordsEmpty() {
            return isRecordsEmpty;
        }

        public long getIndexBucketStartOffset() {
            return indexBucketStartOffset;
        }

        public long getIndexBucketEndOffset() {
            return indexBucketEndOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof IndexApplyParams)) {
                return false;
            }

            IndexApplyParams that = (IndexApplyParams) o;

            return new EqualsBuilder()
                    .append(dataBucketEndOffset, that.dataBucketEndOffset)
                    .append(indexBucketStartOffset, that.indexBucketStartOffset)
                    .append(indexBucketEndOffset, that.indexBucketEndOffset)
                    .append(isRecordsEmpty, that.isRecordsEmpty)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(dataBucketEndOffset)
                    .append(indexBucketStartOffset)
                    .append(indexBucketEndOffset)
                    .append(isRecordsEmpty)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "IndexApplyParams{"
                    + "dataBucketEndOffset="
                    + dataBucketEndOffset
                    + ", indexBucketStartOffset="
                    + indexBucketStartOffset
                    + ", indexBucketEndOffset="
                    + indexBucketEndOffset
                    + ", isEmpty="
                    + isRecordsEmpty
                    + '}';
        }
    }
}
