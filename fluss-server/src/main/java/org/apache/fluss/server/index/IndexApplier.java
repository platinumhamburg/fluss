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
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * Applies index records from upstream data buckets to the index table's KV storage.
 *
 * <p>IndexApplier is held by each index table bucket's Leader Replica and manages:
 *
 * <ul>
 *   <li>Applying index records from ReplicaFetcherThread to KvTablet's KvPreWriteBuffer
 *   <li>Writing index data to WAL
 *   <li>Tracking index application progress via KvTablet's indexReplicationOffsets
 *   <li>Providing index commit offsets for progress reporting
 * </ul>
 *
 * <h2>State Management</h2>
 *
 * <p>Uses KvTablet's indexReplicationOffsets (persisted via TabletState/CompletedSnapshot):
 *
 * <ul>
 *   <li>After applying records, updates kvTablet.updateIndexReplicationOffset()
 *   <li>Reads progress via kvTablet.getIndexReplicationOffset()
 *   <li>State is automatically persisted with TabletState snapshots
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>All public methods are thread-safe using read-write locks.
 */
@Internal
public final class IndexApplier implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexApplier.class);

    // ==================== Dependencies ====================

    private final KvTablet kvTablet;
    private final LogTablet logTablet;
    private final Runnable highWatermarkAdvancer;
    private final SchemaGetter schemaGetter;
    private final TableMetricGroup tableMetricGroup;
    private final TabletServerMetricGroup serverMetricGroup;

    // ==================== Encoding ====================

    private final KeyEncoder indexKeyEncoder;
    private final @Nullable TtlHandler ttlHandler;

    private final Map<Short, LogRecordReadContext> readContextCache = new HashMap<>();

    // ==================== State ====================

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private volatile boolean closed = false;

    // ==================== Constructor ====================

    public IndexApplier(
            KvTablet kvTablet,
            LogTablet logTablet,
            Runnable highWatermarkAdvancer,
            SchemaGetter schemaGetter,
            TableMetricGroup tableMetricGroup,
            TabletServerMetricGroup serverMetricGroup) {
        this.kvTablet = checkNotNull(kvTablet, "kvTablet");
        this.logTablet = checkNotNull(logTablet, "logTablet");
        this.highWatermarkAdvancer = checkNotNull(highWatermarkAdvancer, "highWatermarkAdvancer");
        this.schemaGetter = checkNotNull(schemaGetter, "schemaGetter");
        this.tableMetricGroup = checkNotNull(tableMetricGroup, "tableMetricGroup");
        this.serverMetricGroup = checkNotNull(serverMetricGroup, "serverMetricGroup");

        Schema indexTableSchema = schemaGetter.getLatestSchemaInfo().getSchema();
        RowType initRowType = indexTableSchema.getRowType();
        this.indexKeyEncoder =
                KeyEncoder.of(initRowType, indexTableSchema.getPrimaryKeyColumnNames(), null);
        this.ttlHandler = kvTablet.kvTtlEnabled() ? new TtlHandler(initRowType) : null;

        LOG.info("IndexApplier initialized for index bucket {}", kvTablet.getTableBucket());
    }

    // ==================== Public API ====================

    /**
     * Applies index records from an upstream data bucket.
     *
     * @param records the index records to apply
     * @param startOffset start offset in data bucket address space (inclusive)
     * @param endOffset end offset in data bucket address space (exclusive)
     * @param dataBucket the upstream data bucket
     * @return the new applied offset after processing
     */
    public long applyIndexRecords(
            MemoryLogRecords records, long startOffset, long endOffset, TableBucket dataBucket)
            throws Exception {
        checkNotNull(records, "records");
        checkNotNull(dataBucket, "dataBucket");
        checkArgument(startOffset <= endOffset, "startOffset must be <= endOffset");

        if (LOG.isDebugEnabled()) {
            for (LogRecordBatch batch : records.batches()) {
                LOG.debug(
                        "Received batch: commitTimestamp={}, records={}, schemaId={}",
                        batch.commitTimestamp(),
                        batch.getRecordCount(),
                        batch.schemaId());
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Applying index records from data bucket {} range [{}, {}), {} bytes",
                    dataBucket,
                    startOffset,
                    endOffset,
                    records.sizeInBytes());
        }

        return inWriteLock(
                lock,
                () -> {
                    // Check closed inside the write lock to prevent TOCTOU race
                    // with concurrent close() that clears readContextCache.
                    if (closed) {
                        LOG.warn("IndexApplier is closed, skipping index application");
                        return startOffset;
                    }

                    IndexApplyStatus currentStatus = readStatusFromKvTablet(dataBucket);

                    // Validate offset continuity.
                    // On mismatch, we intentionally return without applying and without
                    // throwing. The fetch loop in IndexFetcherThread uses
                    // lastApplyRecordsDataEndOffset as the next fetchOffset, so the
                    // correct range will be re-requested on the next cycle — both gap
                    // and already-applied cases self-heal automatically.
                    if (startOffset != currentStatus.getLastApplyRecordsDataEndOffset()) {
                        long expected = currentStatus.getLastApplyRecordsDataEndOffset();
                        if (startOffset <= expected) {
                            LOG.error(
                                    "Segment already applied for {}: range=[{}, {}), applied={}",
                                    dataBucket,
                                    startOffset,
                                    endOffset,
                                    expected);
                        } else {
                            LOG.error(
                                    "Gap detected for {}: start={}, expected={}",
                                    dataBucket,
                                    startOffset,
                                    expected);
                        }
                        return currentStatus.getLastApplyRecordsDataEndOffset();
                    }

                    try {
                        boolean hasRecords = hasActualRecords(records);
                        if (hasRecords) {
                            MemoryLogRecords recordsForKv = copyRecords(records);
                            LogAppendInfo appendInfo = logTablet.appendAsLeader(records);
                            updateMetrics(records, appendInfo);
                            writeToPreWriteBuffer(recordsForKv, appendInfo);
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        "Applied empty records from data range [{}, {}), offset updated in KvTablet",
                                        startOffset,
                                        endOffset);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to apply index records from range [{}, {})",
                                startOffset,
                                endOffset,
                                e);
                        throw e;
                    }

                    kvTablet.updateIndexReplicationOffset(dataBucket, endOffset);
                    highWatermarkAdvancer.run();
                    return endOffset;
                });
    }

    /**
     * Gets the index commit offset for a data bucket.
     *
     * @param dataBucket the data bucket
     * @return the committed offset, or 0 if no progress
     */
    public long getIndexCommitOffset(TableBucket dataBucket) {
        return kvTablet.getIndexReplicationOffset(dataBucket);
    }

    /**
     * Gets the current apply status for a data bucket.
     *
     * @param dataBucket the data bucket
     * @return the current status
     */
    public IndexApplyStatus getOrInitIndexApplyStatus(TableBucket dataBucket) {
        return inReadLock(lock, () -> readStatusFromKvTablet(dataBucket));
    }

    /**
     * Resets the apply status for a data bucket to a new offset. Used when the fetch offset is out
     * of range and needs to be reset to coordinate with the leader's data range.
     *
     * @param dataBucket the upstream data bucket
     * @param newOffset the new offset to reset to
     */
    public void resetApplyStatus(TableBucket dataBucket, long newOffset) {
        inWriteLock(
                lock,
                () -> {
                    long oldOffset = kvTablet.getIndexReplicationOffset(dataBucket);
                    kvTablet.updateIndexReplicationOffset(dataBucket, newOffset);
                    LOG.info(
                            "Reset index apply status for data bucket {} on index bucket {}: "
                                    + "offset {} -> {}",
                            dataBucket,
                            kvTablet.getTableBucket(),
                            oldOffset,
                            newOffset);
                });
    }

    @Override
    public void close() {
        inWriteLock(
                lock,
                () -> {
                    if (!closed) {
                        closed = true;
                        for (LogRecordReadContext ctx : readContextCache.values()) {
                            try {
                                ctx.close();
                            } catch (Exception e) {
                                LOG.warn("Error closing cached LogRecordReadContext", e);
                            }
                        }
                        readContextCache.clear();
                        LOG.info(
                                "IndexApplier closed for index bucket {}",
                                kvTablet.getTableBucket());
                    }
                });
    }

    // ==================== Core Logic ====================

    private IndexApplyStatus readStatusFromKvTablet(TableBucket dataBucket) {
        long lastApplyDataOffset = kvTablet.getIndexReplicationOffset(dataBucket);
        long lastApplyIndexOffset = logTablet.localLogEndOffset();
        // With KvTablet-based tracking, the commit offset equals the apply offset
        // since the state is always up-to-date in memory
        return new IndexApplyStatus(lastApplyDataOffset, lastApplyIndexOffset);
    }

    // ==================== Record Processing ====================

    private void writeToPreWriteBuffer(MemoryLogRecords records, LogAppendInfo appendInfo) {
        long currentOffset = appendInfo.firstOffset();
        int recordCount = 0;

        for (LogRecordBatch batch : records.batches()) {
            short schemaId = batch.schemaId();
            LogRecordReadContext readContext =
                    readContextCache.computeIfAbsent(
                            schemaId,
                            id -> {
                                SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
                                return LogRecordReadContext.createIndexedReadContext(
                                        schemaInfo.getSchema().getRowType(),
                                        schemaInfo.getSchemaId(),
                                        schemaGetter);
                            });
            try (CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    writeRecordToPreWriteBuffer(batch.schemaId(), record, currentOffset);
                    currentOffset++;
                    recordCount++;
                }
            }
        }

        serverMetricGroup.indexApplyBatchSizeHistogram().update(recordCount);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processed {} index records for bucket {}",
                    recordCount,
                    kvTablet.getTableBucket());
        }
    }

    private void writeRecordToPreWriteBuffer(short schemaId, LogRecord record, long indexOffset) {
        ChangeType changeType = record.getChangeType();
        IndexedRow row = (IndexedRow) record.getRow();
        byte[] key = indexKeyEncoder.encodeKey(row);

        if (isDeleteOperation(changeType)) {
            kvTablet.putToPreWriteBufferSafely(key, null, indexOffset);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "DELETE: changeType={}, offset={}, keySize={}",
                        changeType,
                        indexOffset,
                        key.length);
            }
        } else if (isPutOperation(changeType)) {
            byte[] value = encodeValue(schemaId, row);
            kvTablet.putToPreWriteBufferSafely(key, value, indexOffset);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "PUT: changeType={}, offset={}, keySize={}, valueSize={}",
                        changeType,
                        indexOffset,
                        key.length,
                        value.length);
            }
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported changeType: " + changeType + " at offset: " + indexOffset);
        }
    }

    private byte[] encodeValue(short schemaId, IndexedRow row) {
        if (ttlHandler != null) {
            long ttlTimestamp = ttlHandler.extractTtlTimestamp(row);
            return ValueEncoder.encodeValueWithLongPrefix(
                    ttlTimestamp, schemaId, row, kvTablet.getCompactionFilterConfig());
        }
        return ValueEncoder.encodeValue(schemaId, row);
    }

    private static boolean isPutOperation(ChangeType changeType) {
        return changeType == ChangeType.APPEND_ONLY
                || changeType == ChangeType.INSERT
                || changeType == ChangeType.UPDATE_AFTER;
    }

    private static boolean isDeleteOperation(ChangeType changeType) {
        return changeType == ChangeType.UPDATE_BEFORE || changeType == ChangeType.DELETE;
    }

    // ==================== Helper Methods ====================

    private static boolean hasActualRecords(LogRecords records) {
        if (records == MemoryLogRecords.EMPTY) {
            return false;
        }
        for (LogRecordBatch batch : records.batches()) {
            if (batch.getRecordCount() > 0) {
                return true;
            }
        }
        return false;
    }

    private static MemoryLogRecords copyRecords(MemoryLogRecords records) {
        byte[] copy = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), copy, 0, records.sizeInBytes());
        return MemoryLogRecords.pointToBytes(copy, 0, copy.length);
    }

    private void updateMetrics(MemoryLogRecords records, LogAppendInfo appendInfo) {
        tableMetricGroup.incKvBytesIn(records.sizeInBytes());
        tableMetricGroup.incKvMessageIn(appendInfo.numMessages());
        tableMetricGroup.incLogBytesIn(appendInfo.validBytes());
        tableMetricGroup.incLogMessageIn(appendInfo.numMessages());
    }

    // ==================== Inner Classes ====================

    /** Handles TTL timestamp extraction from index rows. */
    private static final class TtlHandler {
        private final InternalRow.FieldGetter ttlFieldGetter;

        TtlHandler(RowType indexRowType) {
            int ttlFieldIndex = indexRowType.getFieldIndex(TableDescriptor.INDEX_TTL_COLUMN_NAME);
            if (ttlFieldIndex < 0) {
                throw new IllegalStateException(
                        "KV TTL enabled but missing TTL column '"
                                + TableDescriptor.INDEX_TTL_COLUMN_NAME
                                + "'");
            }
            if (indexRowType.getTypeAt(ttlFieldIndex).getTypeRoot() != DataTypeRoot.BIGINT) {
                throw new IllegalStateException(
                        "TTL column '"
                                + TableDescriptor.INDEX_TTL_COLUMN_NAME
                                + "' must be BIGINT");
            }
            this.ttlFieldGetter =
                    InternalRow.createFieldGetter(
                            indexRowType.getTypeAt(ttlFieldIndex), ttlFieldIndex);
        }

        long extractTtlTimestamp(IndexedRow row) {
            Object ttlValue = ttlFieldGetter.getFieldOrNull(row);
            if (ttlValue == null) {
                throw new IllegalStateException(
                        "TTL column '" + TableDescriptor.INDEX_TTL_COLUMN_NAME + "' is null");
            }
            long ttlTs = (Long) ttlValue;
            if (ttlTs < 0) {
                throw new IllegalStateException("Invalid TTL timestamp (must be >= 0): " + ttlTs);
            }
            return ttlTs;
        }
    }

    /** Status tracking for index application progress. */
    public static final class IndexApplyStatus {
        private final long lastApplyRecordsDataEndOffset;
        private final long lastApplyRecordsIndexEndOffset;

        public IndexApplyStatus(
                long lastApplyRecordsDataEndOffset, long lastApplyRecordsIndexEndOffset) {
            this.lastApplyRecordsDataEndOffset = lastApplyRecordsDataEndOffset;
            this.lastApplyRecordsIndexEndOffset = lastApplyRecordsIndexEndOffset;
        }

        public long getLastApplyRecordsDataEndOffset() {
            return lastApplyRecordsDataEndOffset;
        }

        public long getLastApplyRecordsIndexEndOffset() {
            return lastApplyRecordsIndexEndOffset;
        }

        @Override
        public String toString() {
            return String.format(
                    "IndexApplyStatus{dataEndOffset=%d, indexEndOffset=%d}",
                    lastApplyRecordsDataEndOffset, lastApplyRecordsIndexEndOffset);
        }
    }
}
