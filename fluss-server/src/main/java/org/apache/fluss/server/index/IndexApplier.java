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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.StateDefs;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.TsValueEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.state.BucketStateManager;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final RowType indexRowType;
    private final KeyEncoder indexKeyEncoder;
    private final TabletServerMetricGroup serverMetricGroup;

    // Lock for protecting internal state
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Track uncommitted applies for indexCommitDataOffset calculation
    // This map records the mapping between index bucket offsets and data bucket offsets
    // for each apply operation, enabling partial commit calculation
    @GuardedBy("lock")
    private final Map<TableBucket, List<IndexApplyParams>> uncommittedApplyMap =
            Maps.newConcurrentMap();

    // Cache for indexCommitDataOffset calculated from uncommittedApplyMap and highwatermark
    // This is a computed view and does not need persistence - it can be reconstructed
    // after failover by replaying highwatermark updates
    @GuardedBy("lock")
    private final Map<TableBucket, Long> indexCommitDataOffsetMap = Maps.newConcurrentMap();

    @GuardedBy("lock")
    private volatile boolean closed = false;

    /**
     * Creates a new IndexApplier with simplified parameters.
     *
     * @param kvTablet the KV tablet for index data storage
     * @param logTablet the log tablet for WAL operations
     * @param indexTableSchema the schema of the index table
     * @param serverMetricGroup the server metric group for metrics recording
     */
    public IndexApplier(
            KvTablet kvTablet,
            LogTablet logTablet,
            Schema indexTableSchema,
            TabletServerMetricGroup serverMetricGroup) {
        this.kvTablet = checkNotNull(kvTablet, "kvTablet cannot be null");
        this.logTablet = checkNotNull(logTablet, "logTablet cannot be null");
        checkNotNull(indexTableSchema, "indexTableSchema cannot be null");
        this.serverMetricGroup =
                checkNotNull(serverMetricGroup, "serverMetricGroup cannot be null");

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

        // Debug: Check timestamp in received records
        if (LOG.isDebugEnabled()) {
            for (LogRecordBatch batch : records.batches()) {
                LOG.debug(
                        "IndexApplier received batch with commitTimestamp={}, records={}, magic={}, schemaId={}",
                        batch.commitTimestamp(),
                        batch.getRecordCount(),
                        batch.magic(),
                        batch.schemaId());
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "indexBucket {} applying index records from range [{}, {}) of data bucket {}, {} bytes of index data",
                    kvTablet.getTableBucket(),
                    startOffset,
                    endOffset,
                    dataBucket,
                    records.sizeInBytes());
        }

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
                        return applyIndexRecordsInternal(
                                records, startOffset, endOffset, dataBucket);
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to apply index records from range [{}, {})",
                                startOffset,
                                endOffset,
                                e);
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

    /**
     * Get the IndexApplyStatus for the specified data bucket.
     *
     * <p>This method combines persisted state (lastApplyDataOffset from StateBucketManager) with
     * computed state (indexCommitDataOffset from in-memory calculation based on uncommittedApplyMap
     * and highwatermark).
     *
     * @param dataBucket the data table bucket
     * @return the IndexApplyStatus with current state
     */
    public IndexApplyStatus getOrInitIndexApplyStatus(TableBucket dataBucket) {
        return inReadLock(
                lock,
                () -> {
                    // Read uncommitted state (latest progress) from StateBucketManager
                    // This is persisted and survives failover
                    BucketStateManager.StateValueWithOffset uncommittedStateValue =
                            logTablet.getStateWithOffset(
                                    StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataBucket, false);

                    long lastApplyDataOffset =
                            (uncommittedStateValue != null)
                                    ? (Long) uncommittedStateValue.getValue()
                                    : 0L;

                    // lastApplyRecordsIndexEndOffset is always the current logEndOffset
                    long lastApplyIndexOffset = logTablet.localLogEndOffset();

                    // indexCommitDataOffset is computed from uncommittedApplyMap and highwatermark
                    // It does not need persistence - after failover, it will be recalculated
                    // when onUpdateHighWatermark is called
                    long indexCommitDataOffset =
                            indexCommitDataOffsetMap.getOrDefault(dataBucket, 0L);

                    return new IndexApplyStatus(
                            lastApplyDataOffset, lastApplyIndexOffset, indexCommitDataOffset);
                });
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
                        IndexApplyStatus currentStatus = getOrInitIndexApplyStatus(dataBucket);
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
                        indexCommitDataOffsetMap.put(dataBucket, currentBucketIndexCommitOffset);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Updated indexCommitDataOffset for data bucket {}: hw={}, newIndexCommitDataOffset={}",
                                    dataBucket,
                                    highWatermark,
                                    currentBucketIndexCommitOffset);
                        }
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

    private long applyIndexRecordsInternal(
            MemoryLogRecords records, long startOffset, long endOffset, TableBucket dataBucket)
            throws Exception {
        TableBucket indexBucket = kvTablet.getTableBucket();

        // Get current status for this data bucket from StateBucketManager
        IndexApplyStatus currentStatus = getOrInitIndexApplyStatus(dataBucket);

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

        // Extract timestamps BEFORE appending to log tablet, as the commitTimestamp
        // in LogRecordBatch will be overwritten to the write time by LogTablet
        List<Long> originalTimestamps = null;
        if (!isRecordsEmpty) {
            originalTimestamps = extractTimestampsFromRecords(records);
        }

        // Append records to log tablet
        // State changes in records (generated by IndexCache) will be automatically
        // applied to StateBucketManager by LogTablet
        LogAppendInfo appendInfo = logTablet.appendAsLeader(records);

        List<IndexApplyParams> uncommittedApplies =
                uncommittedApplyMap.computeIfAbsent(dataBucket, tb -> new ArrayList<>());

        if (!isRecordsEmpty) {
            // Write index records to KV pre-write buffer with original timestamps
            writeIndexRecordsToPreWriteBuffer(records, appendInfo, originalTimestamps);
            long indexBucketStartOffset = appendInfo.firstOffset();
            long indexBucketEndOffset = appendInfo.lastOffset() + 1;
            uncommittedApplies.add(
                    new IndexApplyParams(
                            endOffset, indexBucketStartOffset, indexBucketEndOffset, false));

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Applied index records from data range [{}, {}) to index bucket {}, "
                                + "index bucket range: [{}:{})",
                        startOffset,
                        endOffset,
                        indexBucket,
                        indexBucketStartOffset,
                        indexBucketEndOffset);
            }
        } else {
            // For empty records with stateChangeLogs (generated by IndexCache)
            long indexBucketStartOffset = appendInfo.firstOffset();
            long indexBucketEndOffset = appendInfo.lastOffset();
            if (!uncommittedApplies.isEmpty()) {
                // If there are pending uncommitted applies, add this empty record to the queue
                uncommittedApplies.add(
                        new IndexApplyParams(
                                endOffset, indexBucketEndOffset, indexBucketEndOffset, true));
            } else {
                // If no pending applies, empty records can be committed directly
                indexCommitDataOffsetMap.put(dataBucket, endOffset);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Applied empty index records from data range [{}, {}) to index bucket {}, "
                                + "index bucket offset range: [{}:{})",
                        startOffset,
                        endOffset,
                        indexBucket,
                        indexBucketStartOffset,
                        indexBucketEndOffset);
            }
        }
        return endOffset;
    }

    /**
     * Extract timestamps from all records in the MemoryLogRecords BEFORE writing to LogTablet. This
     * is necessary because LogTablet will overwrite the batch's commitTimestamp.
     *
     * @param records the index records
     * @return list of original timestamps for each record
     */
    private List<Long> extractTimestampsFromRecords(MemoryLogRecords records) {
        List<Long> timestamps = new ArrayList<>();
        LogRecordReadContext readContext =
                LogRecordReadContext.createIndexedReadContext(indexRowType, 1);

        for (LogRecordBatch batch : records.batches()) {
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();
                    timestamps.add(record.timestamp());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted {} timestamps from records", timestamps.size());
        }

        return timestamps;
    }

    /**
     * Process index records and apply them to KV pre-write buffer using the allocated index bucket
     * offsets.
     *
     * @param records the index records to process
     * @param appendInfo the log append info containing allocated index bucket offsets
     * @param originalTimestamps the original timestamps extracted before log append
     */
    private void writeIndexRecordsToPreWriteBuffer(
            MemoryLogRecords records, LogAppendInfo appendInfo, List<Long> originalTimestamps) {
        long currentIndexOffset = appendInfo.firstOffset();
        int totalRecordCount = 0;
        int timestampIndex = 0;

        // Create read context for processing index records
        LogRecordReadContext readContext =
                LogRecordReadContext.createIndexedReadContext(indexRowType, 1);

        for (LogRecordBatch batch : records.batches()) {

            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();

                    // Process the index record and apply to KvPreWriteBuffer
                    // Use the sequentially allocated index bucket offset
                    // Use original timestamp from extracted list instead of record.timestamp()
                    // which was overwritten by LogTablet
                    long originalTimestamp = originalTimestamps.get(timestampIndex);
                    writeIndexRecordToPreWriteBuffer(
                            batch.schemaId(), record, currentIndexOffset, originalTimestamp);
                    currentIndexOffset++;
                    totalRecordCount++;
                    timestampIndex++;
                }
            }
        }

        // Record the batch size metric
        serverMetricGroup.indexApplyBatchSizeHistogram().update(totalRecordCount);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processed {} index records in batch for index bucket {}",
                    totalRecordCount,
                    kvTablet.getTableBucket());
        }
    }

    /**
     * Process a single index record and apply it to the KV pre-write buffer.
     *
     * <p>This method handles different changeTypes correctly:
     *
     * <ul>
     *   <li>INSERT / UPDATE_AFTER: PUT operation to insert or update the index entry
     *   <li>UPDATE_BEFORE: DELETE operation to remove the old index entry (before key change)
     *   <li>DELETE: DELETE operation to remove the index entry
     * </ul>
     *
     * @param schemaId the schema ID
     * @param record the index record to process
     * @param indexLogOffset the allocated log offset in index bucket address space
     * @param originalTimestamp the original timestamp extracted before log append
     */
    private void writeIndexRecordToPreWriteBuffer(
            short schemaId, LogRecord record, long indexLogOffset, long originalTimestamp) {
        // Extract changeType and row from the index record
        ChangeType changeType = record.getChangeType();
        IndexedRow indexedRow = (IndexedRow) record.getRow();

        // For index table, the key should be: [index columns] + [primary key columns]
        byte[] key = indexKeyEncoder.encodeKey(indexedRow);

        // Handle different changeTypes with appropriate operations
        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
            case UPDATE_AFTER:
                // For APPEND_ONLY, INSERT and UPDATE_AFTER, we need to PUT the index entry
                byte[] valueBytes;
                if (kvTablet.shouldUseTsEncoding()) {
                    // Value format with timestamp: [timestamp(8)][schemaId(2)][row bytes]
                    // Use the original timestamp extracted before LogTablet append
                    // record.timestamp() is no longer valid as it was overwritten by LogTablet
                    valueBytes =
                            TsValueEncoder.encodeValue(originalTimestamp, schemaId, indexedRow);
                } else {
                    // Normal value format: [schemaId(2)][row bytes]
                    valueBytes = ValueEncoder.encodeValue(schemaId, indexedRow);
                }
                kvTablet.putToPreWriteBufferSafety(key, valueBytes, indexLogOffset);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Index entry PUT to pre-write buffer: changeType={}, indexLogOffset={}, keySize={}, valueSize={}, timestamp={}",
                            changeType,
                            indexLogOffset,
                            key.length,
                            valueBytes.length,
                            originalTimestamp);
                }
                break;

            case UPDATE_BEFORE:
            case DELETE:
                // For UPDATE_BEFORE and DELETE, we need to DELETE the index entry
                // Passing null as value indicates deletion
                kvTablet.putToPreWriteBufferSafety(key, null, indexLogOffset);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Index entry DELETE from pre-write buffer: changeType={}, indexLogOffset={}, keySize={}",
                            changeType,
                            indexLogOffset,
                            key.length);
                }
                break;

            default:
                throw new UnsupportedOperationException(
                        "Unsupported changeType: "
                                + changeType
                                + " at index log offset: "
                                + indexLogOffset);
        }
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
