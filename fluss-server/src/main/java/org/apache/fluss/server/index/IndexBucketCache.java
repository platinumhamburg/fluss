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
 * IndexBucketCache manages cached index data for a single bucket using a range-based architecture.
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
public class IndexBucketCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBucketCache.class);

    /** ReadWrite lock to protect concurrent operations. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final MemorySegmentPool memoryPool;
    private final NavigableMap<Long, OffsetRange> offsetRanges = new TreeMap<>();

    /** Cache for the last accessed range to optimize consecutive operations. */
    private volatile OffsetRange lastReadRange = null;

    private volatile boolean closed = false;

    private final TableBucket dataBucket;

    private final TableBucket indexBucket;

    public IndexBucketCache(
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
                        throw new IllegalStateException("IndexBucketCache is closed");
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
                                        createNewEmptyRangeForOffsetRange(
                                                batchStartOffset, logOffset);
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
                        targetRange =
                                createNewEmptyRangeForOffsetRange(batchStartOffset, logOffset);
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
                    // range boundary
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
                    int cachedBytes = 0;

                    OffsetRange containingRange = findRangeContaining(startOffset);
                    if (containingRange != null) {
                        cachedBytes +=
                                containingRange.getEntryBytesInRange(startOffset, maxEndOffset);
                    }

                    for (Map.Entry<Long, OffsetRange> entry :
                            offsetRanges.tailMap(startOffset, false).entrySet()) {
                        if (entry.getKey() >= maxEndOffset) {
                            break;
                        }
                        cachedBytes +=
                                entry.getValue().getEntryBytesInRange(startOffset, maxEndOffset);
                    }

                    return cachedBytes;
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
     * Gets formatted offset range information for diagnostic purposes.
     *
     * @return formatted string with offset range layout, empty string if no ranges exist
     */
    public String getOffsetRangeInfo() {
        return inReadLock(
                lock,
                () -> {
                    if (offsetRanges.isEmpty()) {
                        return "";
                    }

                    StringBuilder sb = new StringBuilder();
                    for (OffsetRange range : offsetRanges.values()) {
                        sb.append("      Range [")
                                .append(range.getStartOffset())
                                .append(", ")
                                .append(range.getEndOffset())
                                .append("), entries=")
                                .append(range.entriesCount())
                                .append(", memSegments=")
                                .append(range.getMemorySegmentCount())
                                .append("\n");
                    }
                    return sb.toString();
                });
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
                                "IndexBucketCache({} -> {}) cleaned up data below horizon {}, reclaimed {} bytes, removed {} empty ranges. Current ranges: {}",
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

    private OffsetRange createNewEmptyRangeForOffsetRange(long startOffset, long endOffset) {
        OffsetRange newRange = new OffsetRange(startOffset, memoryPool);
        offsetRanges.put(startOffset, newRange);
        newRange.expandRangeBoundaryToOffset(endOffset);
        return newRange;
    }

    /** Creates an empty LogRecords for cases where no data is available. */
    public static LogRecords createEmptyLogRecords() {
        try {
            BytesView emptyBytesView = new MultiBytesView.Builder().build();
            return new BytesViewLogRecords(emptyBytesView);
        } catch (Exception e) {
            LOG.warn("Failed to create empty LogRecords", e);
            throw new RuntimeException("Failed to create empty LogRecords", e);
        }
    }

    /** OffsetRange represents a continuous logical address space segment within an IndexBucket. */
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

        public void expandRangeBoundaryToOffset(long targetEndOffset) {
            if (targetEndOffset < endOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot shrink range boundary. Current end: %d, Target end: %d",
                                endOffset, targetEndOffset));
            }
            endOffset = targetEndOffset;
        }

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

            CachePage targetPage = findOrAllocateSegment(estimateSize);
            MemorySegment memorySegment = targetPage.getSegment();

            int segmentOffset = targetPage.allocateSpace(estimateSize);

            MemorySegmentOutputView outputView = new MemorySegmentOutputView(memorySegment);
            outputView.setPosition(segmentOffset);
            int sizeInBytes = IndexedLogRecord.writeTo(outputView, changeType, row);

            RowCacheIndexEntry entry =
                    new RowCacheIndexEntry(targetPage, segmentOffset, sizeInBytes, timestamp);
            localIndex.addIndexEntry(logOffset, entry);

            endOffset = logOffset + 1;
            return sizeInBytes;
        }

        public LogRecords getRangeRecords(long start, long end, TableBucket dataBucket) {
            try {
                NavigableMap<Long, RowCacheIndexEntry> entriesInRange =
                        localIndex.getEntriesInRange(start, end);

                int recordCount = entriesInRange.size();

                DefaultStateChangeLogsBuilder stateBuilder = new DefaultStateChangeLogsBuilder();
                stateBuilder.addLog(
                        ChangeType.UPDATE_AFTER,
                        StateDefs.DATA_BUCKET_OFFSET_OF_INDEX,
                        dataBucket,
                        end);
                StateChangeLogs stateChangeLogs = stateBuilder.build();

                if (recordCount == 0) {
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

                long batchTimestamp = calculateBatchTimestamp(entriesInRange);

                BytesView recordBatchBytesView;
                try (MemoryLogRecordsIndexedBuilder builder =
                        MemoryLogRecordsIndexedBuilder.builder(
                                SchemaInfo.DEFAULT_SCHEMA_ID,
                                segmentBytesViews,
                                recordCount,
                                false,
                                stateChangeLogs)) {
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
                                cachedPages.get(0).segment,
                                firstIndexEntry.getSegmentOffset(),
                                endOffset - startOffset);
                segmentBytesViews.add(segmentView);
            } else if (cachedPages.size() > 1) {
                int firstViewStartOffset = firstIndexEntry.getSegmentOffset();
                int firstViewEndOffset = cachedPages.get(0).nextOffset;
                MemorySegmentBytesView firstSegmentView =
                        new MemorySegmentBytesView(
                                cachedPages.get(0).segment,
                                firstIndexEntry.getSegmentOffset(),
                                firstViewEndOffset - firstViewStartOffset);
                segmentBytesViews.add(firstSegmentView);

                if (cachedPages.size() > 2) {
                    for (int i = 1; i < cachedPages.size() - 1; i++) {
                        CachePage page = cachedPages.get(i);
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
                                cachedPages.get(cachedPages.size() - 1).segment,
                                lastViewStartOffset,
                                lastViewEndOffset - lastViewStartOffset);
                segmentBytesViews.add(lastSegmentView);
            }
            return segmentBytesViews;
        }

        public void mergeWith(OffsetRange other) {
            if (this.endOffset != other.startOffset) {
                throw new IllegalArgumentException(
                        "Cannot merge non-adjacent ranges: "
                                + this.endOffset
                                + " != "
                                + other.startOffset);
            }

            this.endOffset = other.endOffset;
            this.cachePages.addAll(other.cachePages);
            other.cachePages.clear();
            this.localIndex.addAllIndexEntries(other.localIndex.getAllEntries());
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
            if (!cachePages.isEmpty()) {
                int lastIndex = cachePages.size() - 1;
                CachePage lastPage = cachePages.get(lastIndex);
                if (lastPage.hasSpace(requiredSize)) {
                    return lastPage;
                }
            }

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

    /** RowCacheIndex provides efficient mapping from logOffset to memory locations. */
    @Internal
    public static final class RowCacheIndex {

        private final NavigableMap<Long, RowCacheIndexEntry> offsetEntries = new TreeMap<>();

        public void addIndexEntry(long logOffset, RowCacheIndexEntry entry) {
            offsetEntries.put(logOffset, entry);
        }

        public void addAllIndexEntries(Map<Long, RowCacheIndexEntry> entries) {
            offsetEntries.putAll(entries);
        }

        public NavigableMap<Long, RowCacheIndexEntry> getEntriesInRange(
                long startOffset, long endOffset) {
            return offsetEntries.subMap(startOffset, true, endOffset, false);
        }

        public Set<OffsetRange.CachePage> removeEntriesBelowHorizon(long horizon) {
            NavigableMap<Long, RowCacheIndexEntry> indexEntriesToRemove =
                    offsetEntries.headMap(horizon, false);
            if (indexEntriesToRemove.isEmpty()) {
                return Collections.emptySet();
            }

            Set<OffsetRange.CachePage> affectedPages =
                    indexEntriesToRemove.values().stream()
                            .map(RowCacheIndexEntry::getPage)
                            .collect(Collectors.toSet());

            indexEntriesToRemove.clear();

            if (!offsetEntries.isEmpty()) {
                Set<OffsetRange.CachePage> pagesInUse =
                        offsetEntries.values().stream()
                                .map(RowCacheIndexEntry::getPage)
                                .collect(Collectors.toSet());
                affectedPages.removeAll(pagesInUse);
            }

            return affectedPages;
        }

        public int size() {
            return offsetEntries.size();
        }

        public boolean isEmpty() {
            return offsetEntries.isEmpty();
        }

        public void clear() {
            offsetEntries.clear();
        }

        public NavigableMap<Long, RowCacheIndexEntry> getAllEntries() {
            return new TreeMap<>(offsetEntries);
        }

        @Override
        public String toString() {
            return String.format("RowCacheIndex{entries=%d}", offsetEntries.size());
        }
    }

    /** RowCacheIndexEntry represents the memory location of an IndexedRow. */
    @Internal
    public static final class RowCacheIndexEntry {

        private final OffsetRange.CachePage page;
        private final int segmentOffset;
        private final int rowLength;
        @Nullable private final Long timestamp;

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

        public int getSegmentOffset() {
            return segmentOffset;
        }

        public int getRowLength() {
            return rowLength;
        }

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
