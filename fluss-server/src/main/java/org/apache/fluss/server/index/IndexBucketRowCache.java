/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultStateChangeLogsBuilder;
import org.apache.fluss.record.IndexedLogRecord;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.record.StateChangeLogs;
import org.apache.fluss.record.StateDefs;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.indexed.IndexedRow;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * IndexBucketRowCache manages cached index data for a single bucket using a range-based
 * architecture.
 *
 * <p>This cache is designed for efficient index row storage and retrieval with support for:
 *
 * <ul>
 *   <li>Range-based continuous address space management
 *   <li>Sparse indexing support through offset-only writes
 *   <li>Automatic range merging for adjacent segments
 *   <li>Memory-efficient storage using MemorySegment pools
 *   <li>Accurate LogRecords generation with proper batch headers
 * </ul>
 */
@Internal
public class IndexBucketRowCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBucketRowCache.class);

    /** ReadWrite lock to protect concurrent operations. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final MemorySegmentPool memoryPool;
    private final NavigableMap<Long, OffsetRange> offsetRanges = new TreeMap<>();

    /** Cache for the last accessed range to optimize consecutive operations. */
    private volatile OffsetRange lastReadRange = null;

    private volatile boolean closed = false;

    private final TableBucket dataBucket;

    private final TableBucket indexBucket;

    public IndexBucketRowCache(
            TableBucket dataBucket, TableBucket indexBucket, MemorySegmentPool memoryPool) {
        this.dataBucket = dataBucket;
        this.indexBucket = indexBucket;
        this.memoryPool = memoryPool;
    }

    /**
     * Writes an IndexedRow to the cache for a specific logOffset with optional automatic gap
     * filling capability. This method also supports writing empty rows for sparse indexing by
     * passing null values for changeType and indexedRow.
     *
     * @param logOffset the log offset
     * @param changeType the change type of the row (null for empty row)
     * @param indexedRow the IndexedRow to cache (null for empty row)
     * @param batchStartOffset the minimum offset for gap filling
     * @param timestamp the partition column timestamp (milliseconds), -1 if no time partitioning
     */
    public void writeIndexedRow(
            long logOffset,
            ChangeType changeType,
            IndexedRow indexedRow,
            long batchStartOffset,
            @Nullable Long timestamp)
            throws IOException {
        inWriteLock(
                lock,
                () -> {
                    if (closed) {
                        throw new IllegalStateException("IndexBucketRowCache is closed");
                    }

                    // Determine if this is an empty row write
                    boolean isEmptyRowWrite = (changeType == null && indexedRow == null);

                    // Find appropriate range using TreeMap floor entry (largest range <= logOffset)
                    Map.Entry<Long, OffsetRange> floorEntry = offsetRanges.floorEntry(logOffset);
                    OffsetRange targetRange;

                    if (floorEntry != null) {
                        OffsetRange candidateRange = floorEntry.getValue();

                        if (logOffset == candidateRange.getEndOffset()) {
                            // Adjacent write
                            targetRange = candidateRange;
                        } else if (logOffset > candidateRange.getEndOffset()) {
                            if (candidateRange.getEndOffset() < batchStartOffset) {
                                targetRange =
                                        createNewRangeForOffsetRange(batchStartOffset, logOffset);
                            } else {
                                candidateRange.expandRangeBoundaryToOffset(logOffset);
                                targetRange = candidateRange;
                            }
                        } else {
                            // Data already exists in a range, ignore
                            if (LOG.isTraceEnabled()) {
                                LOG.trace(
                                        "Index bucket {} skipping logOffset {} (already in range [{}, {}))",
                                        indexBucket,
                                        logOffset,
                                        candidateRange.getStartOffset(),
                                        candidateRange.getEndOffset());
                            }
                            return;
                        }
                    } else {
                        targetRange = createNewRangeForOffsetRange(batchStartOffset, logOffset);
                    }

                    // Write data to the target range (only if not empty row)
                    if (!isEmptyRowWrite) {
                        targetRange.writeIndexedRow(logOffset, changeType, indexedRow, timestamp);
                    } else {
                        targetRange.expandRangeBoundaryToOffset(logOffset + 1);
                    }

                    // Check for auto-merging with the next range
                    checkAndMergeWithNextRange(targetRange);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(
                                "Index bucket {}: {} for logOffset {} to range [{}, {}), total ranges: {}",
                                indexBucket,
                                isEmptyRowWrite ? "Expanded range" : "Wrote IndexedRow",
                                logOffset,
                                targetRange.getStartOffset(),
                                targetRange.getEndOffset(),
                                offsetRanges.size());
                    }
                });
    }

    /**
     * Checks if there is an OffsetRange containing the specified offset.
     *
     * @param offset the offset to check
     * @return true if a range contains this offset, false otherwise
     */
    public boolean hasRangeContaining(long offset) {
        return inReadLock(
                lock,
                () -> {
                    if (closed) {
                        return false;
                    }
                    return findRangeContaining(offset) != null;
                });
    }

    /**
     * Gets LogRecords for a range of offsets with accurate metadata. This method directly
     * constructs LogRecordBatch headers using complete offset and record information, eliminating
     * the need for tricky estimations.
     *
     * <p>Single Range Constraint: The query range must be within a single OffsetRange to ensure
     * logical address continuity and accurate LogRecordBatch construction.
     *
     * @param startOffset the start offset (inclusive)
     * @param maxEndOffset the end offset (exclusive)
     * @return LogRecords with accurate headers, or empty LogRecords if no data in range
     * @throws IllegalArgumentException if the query spans multiple ranges
     */
    public LogRecords readIndexLogRecords(long startOffset, long maxEndOffset) {
        return inReadLock(
                lock,
                () -> {
                    if (closed) {
                        return createEmptyLogRecords();
                    }

                    if (startOffset >= maxEndOffset) {
                        return createEmptyLogRecords();
                    }

                    if (lastReadRange != null && lastReadRange.contains(startOffset)) {
                        return lastReadRange.getRangeRecords(startOffset, maxEndOffset, dataBucket);
                    }

                    // Find the range containing the start offset using optimized method
                    OffsetRange containingRange = findRangeContaining(startOffset);

                    lastReadRange = containingRange;

                    if (containingRange == null) {
                        return createEmptyLogRecords();
                    }

                    if (!containingRange.contains(startOffset)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Start offset %d is not within the containing range [%d, %d).",
                                        startOffset,
                                        containingRange.getStartOffset(),
                                        containingRange.getEndOffset()));
                    }

                    // Calculate the actual end offset - use the smaller of expectedEndOffset or
                    // range
                    // boundary
                    long actualEndOffset = Math.min(maxEndOffset, containingRange.getEndOffset());

                    // Get LogRecords from the single range with accurate metadata
                    return containingRange.getRangeRecords(
                            startOffset, actualEndOffset, dataBucket);
                });
    }

    /**
     * Returns the total number of cached IndexedRow entries across all ranges.
     *
     * @return total cached entries count
     */
    public int totalEntries() {
        return inReadLock(
                lock,
                () -> {
                    if (closed) {
                        return 0;
                    }

                    return offsetRanges.values().stream().mapToInt(OffsetRange::entriesCount).sum();
                });
    }

    /**
     * Returns the total number of MemorySegments used across all ranges.
     *
     * @return total memory segment count
     */
    public int getMemorySegmentCount() {
        return inReadLock(
                lock,
                () -> {
                    if (closed) {
                        return 0;
                    }

                    return offsetRanges.values().stream()
                            .mapToInt(OffsetRange::getMemorySegmentCount)
                            .sum();
                });
    }

    /**
     * Gets the underlying memory pool for testing purposes.
     *
     * @return the memory segment pool
     */
    @VisibleForTesting
    public MemorySegmentPool getMemoryPool() {
        return memoryPool;
    }

    /**
     * Gets the number of cached hot data bytes in the specified range. This method counts actual
     * IndexedRow bytes that are already cached (hot data), excluding any gaps that would need to be
     * loaded from WAL.
     *
     * <p>This method is used to determine if there are enough hot data bytes to satisfy a
     * minBucketFetchBytes requirement without loading additional data from WAL.
     *
     * @param startOffset the start offset (inclusive)
     * @param maxEndOffset the end offset (exclusive)
     * @return the size of hot data bytes in the range
     */
    public int getCachedRecordBytesInRange(long startOffset, long maxEndOffset) {
        return inReadLock(
                lock,
                () -> {
                    if (closed || startOffset >= maxEndOffset) {
                        return 0;
                    }
                    return this.offsetRanges
                            .subMap(startOffset, true, maxEndOffset, false)
                            .values()
                            .stream()
                            .map(r -> r.getEntryBytesInRange(startOffset, maxEndOffset))
                            .reduce(Integer::sum)
                            .orElse(0);
                });
    }

    /**
     * Returns whether this cache is empty (contains no IndexedRow data).
     *
     * @return true if cache contains no data
     */
    public boolean isEmpty() {
        return totalEntries() == 0;
    }

    /**
     * Gets the minimum cached offset (start offset of the first range).
     *
     * @return the minimum cached offset, or -1 if cache is empty
     */
    public long getMinCachedOffset() {
        return inReadLock(
                lock,
                () -> {
                    if (offsetRanges.isEmpty()) {
                        return -1L;
                    }
                    return offsetRanges.firstKey();
                });
    }

    /**
     * Returns the TableBucket information for index bucket.
     *
     * @return the index bucket
     */
    public TableBucket getIndexBucket() {
        return indexBucket;
    }

    /**
     * Cleanup cached data below the given horizon offset.
     *
     * @param cleanupHorizonOffset the horizon offset - data below this will be cleaned up
     */
    public void garbageCollectBelowOffset(long cleanupHorizonOffset) {
        inWriteLock(
                lock,
                () -> {
                    if (closed) {
                        return;
                    }

                    List<Long> rangesToRemove = new ArrayList<>();
                    int totalRemovedSize = 0;
                    for (Map.Entry<Long, OffsetRange> entry : offsetRanges.entrySet()) {
                        OffsetRange range = entry.getValue();
                        int removedSize = range.cleanupBelowHorizon(cleanupHorizonOffset);
                        totalRemovedSize += removedSize;

                        // Only remove ranges that are strictly below the horizon
                        // (endOffset < cleanupHorizonOffset)
                        // Ranges with endOffset >= cleanupHorizonOffset should be kept even if
                        // empty, as they represent synchronized offset progress and are needed
                        // to avoid triggering unnecessary cold data loading
                        if (range.isEmpty() && range.getEndOffset() < cleanupHorizonOffset) {
                            rangesToRemove.add(entry.getKey());
                        }
                    }

                    // Remove empty ranges that are completely below horizon
                    for (Long rangeKey : rangesToRemove) {
                        OffsetRange range = offsetRanges.remove(rangeKey);
                        if (range != null) {
                            try {
                                range.close();
                            } catch (IOException e) {
                                LOG.error("Error closing range", e);
                            }
                        }
                    }
                    if (totalRemovedSize > 0 || !rangesToRemove.isEmpty()) {
                        LOG.debug(
                                "IndexBucketRowCache({} -> {}) cleaned up data below horizon {}, reclaimed {} bytes, removed {} empty ranges. Current ranges: {}",
                                dataBucket,
                                indexBucket,
                                cleanupHorizonOffset,
                                totalRemovedSize,
                                rangesToRemove.size(),
                                offsetRanges.size());
                    }
                });
    }

    @Override
    public void close() throws IOException {
        inWriteLock(
                lock,
                () -> {
                    if (closed) {
                        return;
                    }
                    closed = true;

                    // Clear cache
                    lastReadRange = null;

                    // Close all ranges
                    for (OffsetRange range : offsetRanges.values()) {
                        try {
                            range.close();
                        } catch (Exception e) {
                            LOG.warn("Error closing offset range", e);
                        }
                    }
                    offsetRanges.clear();
                });
    }

    /** Optimized range finding using TreeMap's built-in indexing. */
    private OffsetRange findRangeContaining(long offset) {
        // Use TreeMap's floorEntry to find the largest start offset <= target offset
        Map.Entry<Long, OffsetRange> floorEntry = offsetRanges.floorEntry(offset);

        if (floorEntry != null) {
            OffsetRange candidate = floorEntry.getValue();
            if (candidate.contains(offset)) {
                return candidate;
            }
        }

        return null;
    }

    private void checkAndMergeWithNextRange(OffsetRange currentRange) {
        long currentEnd = currentRange.getEndOffset();
        Map.Entry<Long, OffsetRange> nextEntry = offsetRanges.ceilingEntry(currentEnd);

        if (nextEntry != null) {
            OffsetRange nextRange = nextEntry.getValue();
            if (nextRange.getStartOffset() == currentEnd) {
                // Ranges are adjacent - merge them
                currentRange.mergeWith(nextRange);
                OffsetRange removedRange = offsetRanges.remove(nextEntry.getKey());
                LOG.trace(
                        "Merged ranges: [{}, {}) + [{}, {}) = [{}, {})",
                        currentRange.getStartOffset(),
                        currentEnd,
                        nextRange.getStartOffset(),
                        nextRange.getEndOffset(),
                        currentRange.getStartOffset(),
                        currentRange.getEndOffset());
                try {
                    removedRange.close();
                } catch (IOException e) {
                    LOG.warn("Error closing merged range", e);
                }
            }
        }
    }

    /**
     * Creates a new OffsetRange that spans the specified offset range.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return the newly created OffsetRange
     */
    private OffsetRange createNewRangeForOffsetRange(long startOffset, long endOffset) {
        OffsetRange newRange = new OffsetRange(startOffset, memoryPool);
        offsetRanges.put(startOffset, newRange);
        // Directly set the end offset for the entire range
        newRange.expandRangeBoundaryToOffset(endOffset);
        return newRange;
    }

    /** Creates an empty LogRecords for cases where no data is available. */
    public static LogRecords createEmptyLogRecords() {
        try {
            // Create an empty BytesView and wrap it in BytesViewLogRecords
            BytesView emptyBytesView = new MultiBytesView.Builder().build();
            return new BytesViewLogRecords(emptyBytesView);
        } catch (Exception e) {
            // Fallback: create minimal valid LogRecords
            LOG.warn("Failed to create empty LogRecords", e);
            throw new RuntimeException("Failed to create empty LogRecords", e);
        }
    }

    /**
     * OffsetRange represents a continuous logical address space segment (left open, right closed)
     * within an IndexBucket.
     *
     * <p>This class maintains the "continuous address space" principle by ensuring that all offsets
     * within [startOffset, endOffset) are accounted for, even if some don't have actual IndexedRow
     * data (sparse indexing).
     *
     * <p>Key features: - Strict adjacency enforcement for data writes - Memory-efficient storage
     * using MemorySegment pools - Support for both dense and sparse indexing patterns - Automatic
     * cleanup and resource management
     */
    private static class OffsetRange implements Closeable {
        private final long startOffset;
        private long endOffset;
        private final MemorySegmentPool memoryPool;
        private final List<CachePage> cachePages = new ArrayList<>();
        private final RowCacheIndex localIndex;
        private volatile boolean closed = false;

        OffsetRange(long startOffset, MemorySegmentPool memoryPool) {
            this.startOffset = startOffset;
            this.endOffset = startOffset;
            this.memoryPool = memoryPool;
            this.localIndex = new RowCacheIndex();
        }

        public int getEntryBytesInRange(long startOffset, long endOffset) {
            return localIndex.getEntriesInRange(startOffset, endOffset).values().stream()
                    .mapToInt(RowCacheIndexEntry::getRowLength)
                    .sum();
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public boolean contains(long offset) {
            return startOffset <= offset && offset < endOffset;
        }

        /**
         * Expands the range boundary to the specified end offset for batch operations. This method
         * allows efficient expansion of the range to cover a continuous offset range without
         * requiring adjacency checks for each individual offset.
         *
         * @param targetEndOffset the target end offset (exclusive) to expand the range to
         */
        public void expandRangeBoundaryToOffset(long targetEndOffset) {
            if (targetEndOffset < endOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot shrink range boundary. Current end: %d, Target end: %d",
                                endOffset, targetEndOffset));
            }

            // Extend the range boundary to the target end offset
            endOffset = targetEndOffset;
        }

        /**
         * Writes IndexedRow data to this range, extending the right boundary.
         *
         * @param logOffset the log offset
         * @param changeType the change type of the row
         * @param row the row to write
         * @param timestamp the partition column timestamp (milliseconds), -1 if no time
         *     partitioning
         * @return the number of bytes written
         */
        public int writeIndexedRow(
                long logOffset, ChangeType changeType, IndexedRow row, @Nullable Long timestamp)
                throws IOException {
            if (logOffset != endOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot write non-adjacent offset to range. Range end: %d, Offset: %d",
                                endOffset, logOffset));
            }
            int estimateSize = row.getSizeInBytes() + 1 + LENGTH_LENGTH;

            // Find or allocate segment for this write
            CachePage targetPage = findOrAllocateSegment(estimateSize);
            MemorySegment memorySegment = targetPage.getSegment();

            // Write data to segment
            int segmentOffset = targetPage.allocateSpace(estimateSize);

            MemorySegmentOutputView outputView = new MemorySegmentOutputView(memorySegment);
            outputView.setPosition(segmentOffset);
            int sizeInBytes = IndexedLogRecord.writeTo(outputView, changeType, row);

            // Update index with timestamp
            RowCacheIndexEntry entry =
                    new RowCacheIndexEntry(targetPage, segmentOffset, sizeInBytes, timestamp);
            localIndex.addIndexEntry(logOffset, entry);

            // Extend range boundary
            endOffset = logOffset + 1;
            return sizeInBytes;
        }

        /**
         * Gets LogRecords for the specified range within this OffsetRange with accurate metadata.
         * This method uses an efficient MemorySegment-based approach instead of processing
         * individual rows.
         *
         * <p>Algorithm: 1. Find boundary entries using getCeilingEntry/getFloorEntry 2. Determine
         * affected MemorySegment range 3. Create MemorySegmentBytesView for each complete segment
         * 4. Handle boundary segments with proper offset trimming
         *
         * @param start the start offset (inclusive)
         * @param end the end offset (exclusive)
         * @param dataBucket the data bucket for state change logs
         * @return LogRecords with accurate headers and metadata
         */
        public LogRecords getRangeRecords(long start, long end, TableBucket dataBucket) {
            try {

                // Calculate the number of records in the range
                NavigableMap<Long, RowCacheIndexEntry> entriesInRange =
                        localIndex.getEntriesInRange(start, end);

                int recordCount = entriesInRange.size();

                // Build stateChangeLogs to record data bucket offset mapping
                // This is required even when recordCount is 0 to ensure proper offset tracking
                DefaultStateChangeLogsBuilder stateBuilder = new DefaultStateChangeLogsBuilder();
                stateBuilder.addLog(
                        ChangeType.UPDATE_AFTER,
                        StateDefs.DATA_BUCKET_OFFSET_OF_INDEX,
                        dataBucket,
                        end);
                StateChangeLogs stateChangeLogs = stateBuilder.build();

                if (recordCount == 0) {
                    // Even when there are no records in this range, we still need to build
                    // a LogRecordBatch with stateChangeLogs to ensure the index bucket
                    // can properly advance its offset tracking
                    BytesView recordBatchBytesView;
                    try (MemoryLogRecordsIndexedBuilder builder =
                            MemoryLogRecordsIndexedBuilder.builder(
                                    SchemaInfo.DEFAULT_SCHEMA_ID,
                                    new ArrayList<>(),
                                    0,
                                    false,
                                    stateChangeLogs)) {
                        recordBatchBytesView = builder.build();
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(
                                "Built empty LogRecords with stateChangeLogs for range [{}, {})",
                                start,
                                end);
                    }

                    return new BytesViewLogRecords(recordBatchBytesView);
                }

                RowCacheIndexEntry firstIndexEntry = null;
                RowCacheIndexEntry lastIndexEntry = null;
                List<CachePage> involvedPages = new ArrayList<>();
                CachePage lastPage = null;
                for (Map.Entry<Long, RowCacheIndexEntry> entry : entriesInRange.entrySet()) {
                    if (firstIndexEntry == null) {
                        firstIndexEntry = entry.getValue();
                    }
                    CachePage page = entry.getValue().getPage();
                    if (page != lastPage) {
                        involvedPages.add(page);
                        lastPage = page;
                    }
                    lastIndexEntry = entry.getValue();
                }

                List<MemorySegmentBytesView> segmentBytesViews =
                        assembleCachedPagesToSegmentBytesViews(
                                involvedPages, firstIndexEntry, lastIndexEntry);

                checkArgument(!segmentBytesViews.isEmpty(), "No segments found for range");

                // Calculate batch timestamp from partition column timestamps
                long batchTimestamp = calculateBatchTimestamp(entriesInRange);

                // Build LogRecords using MemoryLogRecordsIndexedBuilder with
                // preWrittenByteViews
                // Use default schema ID since index tables don't allow schema changes
                BytesView recordBatchBytesView;
                try (MemoryLogRecordsIndexedBuilder builder =
                        MemoryLogRecordsIndexedBuilder.builder(
                                SchemaInfo.DEFAULT_SCHEMA_ID,
                                segmentBytesViews,
                                recordCount,
                                false,
                                stateChangeLogs)) {
                    // Set commitTimestamp before building to ensure it's included in the batch
                    // header
                    if (batchTimestamp > 0) {
                        builder.setCommitTimestamp(batchTimestamp);
                    }
                    recordBatchBytesView = builder.build();
                }

                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Built LogRecords for range [{}, {}) using {} MemorySegments with {} bytes, batchTimestamp: {}",
                            start,
                            end,
                            segmentBytesViews.size(),
                            recordBatchBytesView.getBytesLength(),
                            batchTimestamp);
                }

                return new BytesViewLogRecords(recordBatchBytesView);

            } catch (Exception e) {
                LOG.warn("Failed to build LogRecords for range [{}, {})", start, end, e);
                return createEmptyLogRecords();
            }
        }

        /**
         * Calculate the representative timestamp for a batch of index entries. Uses the maximum
         * timestamp to ensure data is not deleted prematurely (conservative strategy).
         *
         * @param entries the map of offset to RowCacheIndexEntry
         * @return the batch timestamp in milliseconds, or -1 if no valid timestamps
         *     (non-partitioned tables or all timestamps are null)
         */
        private long calculateBatchTimestamp(Map<Long, RowCacheIndexEntry> entries) {
            if (entries.isEmpty()) {
                return 0;
            }

            return entries.values().stream()
                    .map(RowCacheIndexEntry::getTimestamp)
                    .filter(ts -> ts != null && ts > 0)
                    .mapToLong(Long::longValue)
                    .max()
                    .orElse(0);
        }

        private List<MemorySegmentBytesView> assembleCachedPagesToSegmentBytesViews(
                List<CachePage> cachedPages,
                RowCacheIndexEntry firstIndexEntry,
                RowCacheIndexEntry lastIndexEntry) {
            List<MemorySegmentBytesView> segmentBytesViews = new ArrayList<>();
            if (cachedPages.size() == 1) {
                int startOffset = firstIndexEntry.getSegmentOffset();
                int endOffset = lastIndexEntry.getSegmentOffset() + lastIndexEntry.getRowLength();
                MemorySegmentBytesView segmentView =
                        new MemorySegmentBytesView(
                                cachePages.get(0).segment,
                                firstIndexEntry.getSegmentOffset(),
                                endOffset - startOffset);
                segmentBytesViews.add(segmentView);
            } else if (cachedPages.size() > 1) {
                int firstViewStartOffset = firstIndexEntry.getSegmentOffset();
                int firstViewEndOffset = cachePages.get(0).nextOffset;
                MemorySegmentBytesView firstSegmentView =
                        new MemorySegmentBytesView(
                                cachePages.get(0).segment,
                                firstIndexEntry.getSegmentOffset(),
                                firstViewEndOffset - firstViewStartOffset);
                segmentBytesViews.add(firstSegmentView);

                if (cachedPages.size() > 2) {
                    for (int i = 1; i < cachedPages.size() - 1; i++) {
                        CachePage page = cachePages.get(i);
                        MemorySegmentBytesView segmentView =
                                new MemorySegmentBytesView(
                                        page.getSegment(), 0, page.getNextOffset());
                        segmentBytesViews.add(segmentView);
                    }
                }

                int lastViewStartOffset = 0;
                int lastViewEndOffset =
                        lastIndexEntry.segmentOffset + lastIndexEntry.getRowLength();
                MemorySegmentBytesView lastSegmentView =
                        new MemorySegmentBytesView(
                                cachePages.get(cachedPages.size() - 1).segment,
                                lastViewStartOffset,
                                lastViewEndOffset - lastViewStartOffset);
                segmentBytesViews.add(lastSegmentView);
            }
            return segmentBytesViews;
        }

        /**
         * Merges another OffsetRange into this range.
         *
         * @param other the other range to merge
         */
        public void mergeWith(OffsetRange other) {
            if (this.endOffset != other.startOffset) {
                throw new IllegalArgumentException(
                        "Cannot merge non-adjacent ranges: "
                                + this.endOffset
                                + " != "
                                + other.startOffset);
            }

            // Extend this range to include the other range
            this.endOffset = other.endOffset;

            // Transfer memory segments from other to this range
            this.cachePages.addAll(other.cachePages);

            // Clear the memory segments from the other range to transfer ownership
            // This prevents the other range from releasing these segments when it's closed
            other.cachePages.clear();

            this.localIndex.addAllIndexEntries(other.localIndex.getAllEntries());

            // Clear the local index from the other range to complete the ownership transfer
            other.localIndex.clear();
        }

        public int cleanupBelowHorizon(long horizonOffset) {
            if (closed || cachePages.isEmpty()) {
                return 0;
            }

            Set<CachePage> pagesToReclaim = localIndex.removeEntriesBelowHorizon(horizonOffset);

            int totalReclaimed = 0;
            Iterator<CachePage> iterator = cachePages.iterator();
            while (iterator.hasNext()) {
                CachePage page = iterator.next();
                if (pagesToReclaim.contains(page)) {
                    totalReclaimed += page.getSegment().size();
                    memoryPool.returnPage(page.getSegment());
                    iterator.remove();
                    pagesToReclaim.remove(page);
                }
            }

            return totalReclaimed;
        }

        public int entriesCount() {
            return localIndex.size();
        }

        /**
         * Gets the number of MemorySegments used by this range.
         *
         * @return the number of memory segments
         */
        public int getMemorySegmentCount() {
            return cachePages.size();
        }

        public boolean isEmpty() {
            return cachePages.isEmpty();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            closed = true;

            // Release memory segments back to the pool
            try {
                if (!cachePages.isEmpty()) {
                    memoryPool.returnAll(
                            cachePages.stream()
                                    .map(CachePage::getSegment)
                                    .collect(Collectors.toList()));
                    LOG.trace(
                            "Returned {} MemorySegments to pool during OffsetRange close",
                            cachePages.size());
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to return memory segments to pool during close: {}",
                        e.getMessage());
            } finally {
                cachePages.clear();
                localIndex.clear();
            }
        }

        private CachePage findOrAllocateSegment(int requiredSize) throws IOException {
            // Check if the last segment has enough space
            if (!cachePages.isEmpty()) {
                int lastIndex = cachePages.size() - 1;
                CachePage lastPage = cachePages.get(lastIndex);
                if (lastPage.hasSpace(requiredSize)) {
                    return lastPage;
                }
            }

            // Allocate a new segment
            MemorySegment newSegment = memoryPool.nextSegment();
            if (newSegment == null) {
                throw new EOFException("Cannot allocate new memory segment from pool");
            }

            CachePage page = new CachePage(newSegment);
            cachePages.add(page);

            return page;
        }

        /** Metadata for tracking allocation within a MemorySegment. */
        public static class CachePage {
            private final MemorySegment segment;
            private int nextOffset = 0;

            CachePage(MemorySegment segment) {
                this.segment = segment;
            }

            public boolean hasSpace(int requiredSize) {
                return nextOffset + requiredSize <= segment.size();
            }

            public int allocateSpace(int size) {
                int allocatedOffset = nextOffset;
                nextOffset += size;
                return allocatedOffset;
            }

            public int getNextOffset() {
                return nextOffset;
            }

            public MemorySegment getSegment() {
                return segment;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (!(o instanceof CachePage)) {
                    return false;
                }

                CachePage page = (CachePage) o;

                return new EqualsBuilder()
                        .append(nextOffset, page.nextOffset)
                        .append(segment, page.segment)
                        .isEquals();
            }

            @Override
            public int hashCode() {
                return new HashCodeBuilder(17, 37).append(segment).append(nextOffset).toHashCode();
            }

            @Override
            public String toString() {
                return "CachePage{" + "segment=" + segment + ", nextOffset=" + nextOffset + '}';
            }
        }
    }

    /**
     * RowCacheIndex provides efficient mapping from logOffset to memory locations within a
     * Range-based architecture.
     *
     * <p>This class maintains a single key data structure: - offsetEntries: TreeMap for mapping
     * logOffset to RowCacheIndexEntry (memory location)
     *
     * <p>The index supports: - O(log n) range queries using TreeMap - Direct offset to memory
     * location mapping - Efficient cleanup based on commit horizon
     *
     * <p>In the Range-based architecture, status management is handled at the Range level, so this
     * class focuses purely on offset-to-memory-location mapping.
     *
     * <p>Thread Safety: This class is NOT thread-safe. External synchronization is required.
     */
    @Internal
    public static final class RowCacheIndex {

        /** Maps logOffset to memory location. */
        private final NavigableMap<Long, RowCacheIndexEntry> offsetEntries = new TreeMap<>();

        /**
         * Adds an RowCacheIndexEntry for a specific logOffset.
         *
         * @param logOffset the log offset
         * @param entry the index entry containing memory location
         */
        public void addIndexEntry(long logOffset, RowCacheIndexEntry entry) {
            offsetEntries.put(logOffset, entry);
        }

        /**
         * Adds multiple RowCacheIndexEntries.
         *
         * @param entries the entries to add
         */
        public void addAllIndexEntries(Map<Long, RowCacheIndexEntry> entries) {
            offsetEntries.putAll(entries);
        }

        /**
         * Gets all IndexEntries in the specified range [startOffset, endOffset).
         *
         * @param startOffset the start offset (inclusive)
         * @param endOffset the end offset (exclusive)
         * @return a navigable map of offsets to entries in the range
         */
        public NavigableMap<Long, RowCacheIndexEntry> getEntriesInRange(
                long startOffset, long endOffset) {
            return offsetEntries.subMap(startOffset, true, endOffset, false);
        }

        /**
         * Removes all entries with offsets less than the given horizon. This is used for cleanup
         * based on index commit horizon.
         *
         * @param horizon the commit horizon
         * @return the pages to reclaim
         */
        public Set<OffsetRange.CachePage> removeEntriesBelowHorizon(long horizon) {
            NavigableMap<Long, RowCacheIndexEntry> indexEntriesToRemove =
                    offsetEntries.headMap(horizon, false);
            if (indexEntriesToRemove.isEmpty()) {
                return Collections.emptySet();
            }

            // Collect all pages that have entries to be removed
            Set<OffsetRange.CachePage> affectedPages =
                    indexEntriesToRemove.values().stream()
                            .map(RowCacheIndexEntry::getPage)
                            .collect(Collectors.toSet());

            // Remove the entries from the index
            indexEntriesToRemove.clear();

            // Check which affected pages still have remaining entries and should be retained
            if (!offsetEntries.isEmpty()) {
                affectedPages.remove(offsetEntries.firstEntry().getValue().getPage());
            }

            return affectedPages;
        }

        /**
         * Gets the total number of indexed entries.
         *
         * @return the number of entries
         */
        public int size() {
            return offsetEntries.size();
        }

        /**
         * Checks if the index is empty.
         *
         * @return true if no entries exist
         */
        public boolean isEmpty() {
            return offsetEntries.isEmpty();
        }

        /** Clears all entries. */
        public void clear() {
            offsetEntries.clear();
        }

        /**
         * Gets all entries in the index.
         *
         * @return a navigable map of all offsets to entries
         */
        public NavigableMap<Long, RowCacheIndexEntry> getAllEntries() {
            return new TreeMap<>(offsetEntries);
        }

        @Override
        public String toString() {
            return String.format("RowCacheIndex{entries=%d}", offsetEntries.size());
        }
    }

    /**
     * RowCacheIndexEntry represents the memory location of an IndexedRow in the memory segments.
     * This is a compact data structure used in RowCacheIndex to minimize memory overhead.
     *
     * <p>Each RowCacheIndexEntry contains: - segmentIndex: The index of the memory segment
     * containing the row - segmentOffset: The offset within the memory segment where the row starts
     * - rowLength: The length of the serialized IndexedRow data - timestamp: The partition column
     * timestamp for TTL (milliseconds), null if no time partitioning (non-partitioned tables)
     */
    @Internal
    public static final class RowCacheIndexEntry {

        private final OffsetRange.CachePage page;
        private final int segmentOffset;
        private final int rowLength;
        @Nullable private final Long timestamp;

        /**
         * Creates a new RowCacheIndexEntry.
         *
         * @param page the cache page
         * @param segmentOffset the offset within the segment
         * @param rowLength the length of the row data
         * @param timestamp the partition column timestamp (milliseconds), null if no time
         *     partitioning (non-partitioned tables)
         */
        public RowCacheIndexEntry(
                OffsetRange.CachePage page,
                int segmentOffset,
                int rowLength,
                @Nullable Long timestamp) {
            if (segmentOffset < 0) {
                throw new IllegalArgumentException(
                        "Segment offset cannot be negative: " + segmentOffset);
            }
            if (rowLength <= 0) {
                throw new IllegalArgumentException("Row length must be positive: " + rowLength);
            }

            this.page = page;
            this.segmentOffset = segmentOffset;
            this.rowLength = rowLength;
            this.timestamp = timestamp;
        }

        public OffsetRange.CachePage getPage() {
            return page;
        }

        /**
         * Gets the offset within the memory segment.
         *
         * @return the segment offset
         */
        public int getSegmentOffset() {
            return segmentOffset;
        }

        /**
         * Gets the length of the row data.
         *
         * @return the row length in bytes
         */
        public int getRowLength() {
            return rowLength;
        }

        /**
         * Gets the partition column timestamp for TTL.
         *
         * @return the timestamp in milliseconds, or null if no time partitioning (non-partitioned
         *     tables)
         */
        @Nullable
        public Long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "RowCacheIndexEntry{"
                    + "page="
                    + page
                    + ", segmentOffset="
                    + segmentOffset
                    + ", rowLength="
                    + rowLength
                    + ", timestamp="
                    + timestamp
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RowCacheIndexEntry that = (RowCacheIndexEntry) o;
            return page == that.page
                    && segmentOffset == that.segmentOffset
                    && rowLength == that.rowLength
                    && (timestamp == null
                            ? that.timestamp == null
                            : timestamp.equals(that.timestamp));
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(page)
                    .append(segmentOffset)
                    .append(rowLength)
                    .append(timestamp)
                    .toHashCode();
        }
    }
}
