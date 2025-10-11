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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.IndexedLogRecord;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.indexed.IndexedRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;

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

    private final MemorySegmentPool memoryPool;
    private final TreeMap<Long, OffsetRange> offsetRanges = new TreeMap<>();
    private volatile boolean closed = false;
    private int totalSize = 0;
    private long commitOffset = 0;

    public IndexBucketRowCache(MemorySegmentPool memoryPool) {
        this.memoryPool = memoryPool;
    }

    public void updateCommitOffset(long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public long getCommitOffset() {
        return commitOffset;
    }

    /**
     * Writes to cache for sparse indexing support - drives Range creation or expansion without
     * actual IndexedRow data.
     *
     * <p>This method is specifically designed to handle sparse indexing scenarios where certain
     * offsets may not have corresponding index data but still need to participate in the Range
     * structure management.
     *
     * <p>Key behaviors: - Ignores if logOffset already exists in any range to prevent duplicates -
     * Creates or expands OffsetRange to include the specified offset - Supports auto-merging of
     * adjacent ranges when boundaries meet - Uses the same Range-based adjacency strategy as the
     * main writeIndexedRow method
     *
     * @param logOffset the log offset to expand range for (without actual data)
     * @throws IOException if there's an error during range operations
     */
    public void writeEmptyRowForOffset(long logOffset) throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexBucketRowCache is closed");
        }

        // Check if logOffset already exists in any range - ignore if found
        if (containsOffset(logOffset)) {
            LOG.trace(
                    "Ignoring range expansion for existing logOffset {} in IndexBucketRowCache",
                    logOffset);
            return;
        }

        // Find appropriate range using TreeMap floor entry (largest range <= logOffset)
        Map.Entry<Long, OffsetRange> floorEntry = offsetRanges.floorEntry(logOffset);
        OffsetRange targetRange = null;

        if (floorEntry != null) {
            OffsetRange candidateRange = floorEntry.getValue();
            // Check if the offset is adjacent to the range's right boundary
            if (candidateRange.canAcceptOffset(logOffset)) {
                targetRange = candidateRange;
            }
        }

        // Create new range if no suitable range found
        if (targetRange == null) {
            targetRange = createNewRange(logOffset);
        }

        // Expand range boundary without writing actual data
        targetRange.expandRangeBoundary(logOffset);

        // Check for auto-merging with the next range
        checkAndMergeWithNextRange(targetRange);

        LOG.trace(
                "Expanded range for logOffset {} to range [{}, {}), total ranges: {}",
                logOffset,
                targetRange.getStartOffset(),
                targetRange.getEndOffset(),
                offsetRanges.size());
    }

    /**
     * Writes an IndexedRow to the cache for a specific logOffset using Range-based strict adjacency
     * mechanism.
     *
     * <p>Range-Based Write Strategy: - Find the largest range smaller than data offset using
     * TreeMap.floorEntry() - Check adjacency: data offset must equal range right boundary - Extend
     * existing range if adjacent, create new range if not adjacent - Auto-merge ranges when
     * boundaries meet after expansion
     *
     * <p>Duplication Handling: If the logOffset already exists in any range, the write is ignored
     * to prevent duplicates and maintain data consistency.
     *
     * @param logOffset the log offset
     * @param indexedRow the IndexedRow to cache
     * @throws IOException if an error occurs during writing
     */
    public void writeIndexedRow(long logOffset, ChangeType changeType, IndexedRow indexedRow)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexBucketRowCache is closed");
        }

        // Check if logOffset already exists across all ranges - ignore if found
        if (containsOffset(logOffset)) {
            LOG.trace("Ignoring write for existing logOffset {} in IndexBucketRowCache", logOffset);
            return;
        }

        // Find appropriate range using TreeMap floor entry (largest range <= logOffset)
        Map.Entry<Long, OffsetRange> floorEntry = offsetRanges.floorEntry(logOffset);
        OffsetRange targetRange = null;

        if (floorEntry != null) {
            OffsetRange candidateRange = floorEntry.getValue();
            // Check if the offset is adjacent to the range's right boundary
            if (candidateRange.canAcceptOffset(logOffset)) {
                targetRange = candidateRange;
            }
        }

        // Create new range if no suitable range found
        if (targetRange == null) {
            targetRange = createNewRange(logOffset);
        }

        // Write data to the target range
        totalSize += targetRange.writeIndexedRow(logOffset, changeType, indexedRow);

        // Check for auto-merging with the next range
        checkAndMergeWithNextRange(targetRange);

        LOG.trace(
                "Wrote IndexedRow for logOffset {} to range [{}, {}), total ranges: {}",
                logOffset,
                targetRange.getStartOffset(),
                targetRange.getEndOffset(),
                offsetRanges.size());
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
     * @param endOffset the end offset (exclusive)
     * @param schemaId the schema ID for the LogRecordBatch headers
     * @return LogRecords with accurate headers, or empty LogRecords if no data in range
     * @throws IllegalArgumentException if the query spans multiple ranges
     */
    public LogRecords getRangeLogRecords(long startOffset, long endOffset, short schemaId) {
        if (closed) {
            return createEmptyLogRecords();
        }

        if (startOffset >= endOffset) {
            return createEmptyLogRecords();
        }

        // Find the range containing the start offset
        OffsetRange containingRange = findRangeContaining(startOffset);
        if (containingRange == null) {
            return createEmptyLogRecords();
        }

        // Enforce single range constraint
        if (!containingRange.containsRange(startOffset, endOffset)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query range [%d, %d) spans multiple ranges or exceeds range boundary. "
                                    + "Found range [%d, %d). Single range queries only.",
                            startOffset,
                            endOffset,
                            containingRange.getStartOffset(),
                            containingRange.getEndOffset()));
        }

        // Get LogRecords from the single range with accurate metadata
        return containingRange.getRangeLogRecords(startOffset, endOffset, schemaId);
    }

    /**
     * Checks if the cache contains actual IndexedRow data for a specific offset.
     *
     * @param offset the offset to check
     * @return true if actual IndexedRow data exists at this offset
     */
    public boolean containsOffset(long offset) {
        if (closed) {
            return false;
        }
        OffsetRange containingRange = findRangeContaining(offset);
        if (containingRange == null) {
            return false;
        }
        // Check if there's actual IndexedRow data at this offset, not just range boundary
        return containingRange.hasDataAtOffset(offset);
    }

    /**
     * Returns the total number of cached IndexedRow entries across all ranges.
     *
     * @return total cached entries count
     */
    public int totalEntries() {
        if (closed) {
            return 0;
        }
        return offsetRanges.values().stream().mapToInt(OffsetRange::size).sum();
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
     * Returns the number of OffsetRange segments in this cache.
     *
     * @return the count of active ranges
     */
    public int getRangeCount() {
        return offsetRanges.size();
    }

    /**
     * Returns the total size in bytes of all cached data.
     *
     * @return total size in bytes
     */
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Gets all unloaded ranges within the specified range using Range-based gap analysis.
     *
     * <p>This method identifies gaps between existing OffsetRanges that need to be loaded from WAL,
     * supporting the two-phase fetch capability.
     *
     * <p>Algorithm: - Find all OffsetRanges that intersect with the query range - Identify gaps
     * between these ranges within the query bounds - Return gaps as OffsetRangeInfo objects for WAL
     * loading
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a list of OffsetRangeInfo representing gaps that need data loading
     */
    public List<OffsetRangeInfo> getUnloadedRanges(long startOffset, long endOffset) {
        if (closed) {
            return emptyList();
        }

        if (startOffset >= endOffset) {
            return emptyList();
        }

        List<OffsetRangeInfo> unloadedRanges = new ArrayList<>();

        if (offsetRanges.isEmpty()) {
            // No ranges exist, entire query range is unloaded
            unloadedRanges.add(new OffsetRangeInfo(startOffset, endOffset));
            return unloadedRanges;
        }

        // Find all ranges that might intersect with the query range
        List<OffsetRange> intersectingRanges = new ArrayList<>();
        for (OffsetRange range : offsetRanges.values()) {
            // Check if range intersects with [startOffset, endOffset)
            if (range.getStartOffset() < endOffset && range.getEndOffset() > startOffset) {
                intersectingRanges.add(range);
            }
        }

        if (intersectingRanges.isEmpty()) {
            // No existing ranges intersect with query range, entire range is unloaded
            unloadedRanges.add(new OffsetRangeInfo(startOffset, endOffset));
            return unloadedRanges;
        }

        // Sort ranges by start offset (should already be sorted, but ensure)
        intersectingRanges.sort((r1, r2) -> Long.compare(r1.getStartOffset(), r2.getStartOffset()));

        // Find gaps within the query range
        long currentOffset = startOffset;

        for (OffsetRange range : intersectingRanges) {
            long rangeStart = Math.max(range.getStartOffset(), startOffset);
            long rangeEnd = Math.min(range.getEndOffset(), endOffset);

            // Check for gap before this range
            if (currentOffset < rangeStart) {
                unloadedRanges.add(new OffsetRangeInfo(currentOffset, rangeStart));
            }

            // Move current offset past this range
            currentOffset = Math.max(currentOffset, rangeEnd);
        }

        // Check for gap after all ranges
        if (currentOffset < endOffset) {
            unloadedRanges.add(new OffsetRangeInfo(currentOffset, endOffset));
        }

        LOG.trace(
                "Found {} unloaded ranges within [{}, {}) with {} existing ranges",
                unloadedRanges.size(),
                startOffset,
                endOffset,
                intersectingRanges.size());

        return unloadedRanges;
    }

    /**
     * Cleanup cached data below the given horizon offset.
     *
     * @param horizonOffset the horizon offset - data below this will be cleaned up
     * @throws IOException if cleanup operation fails
     */
    public void cleanupBelowHorizon(long horizonOffset) throws IOException {
        if (closed) {
            return;
        }

        List<Long> rangesToRemove = new ArrayList<>();
        int totalRemovedSize = 0;
        for (Map.Entry<Long, OffsetRange> entry : offsetRanges.entrySet()) {
            OffsetRange range = entry.getValue();
            int removedSize = range.cleanupBelowHorizon(horizonOffset);
            totalRemovedSize += removedSize;

            if (range.isEmpty()) {
                rangesToRemove.add(entry.getKey());
            }
        }

        // Update total size
        totalSize -= totalRemovedSize;

        // Remove empty ranges
        for (Long rangeKey : rangesToRemove) {
            OffsetRange range = offsetRanges.remove(rangeKey);
            if (range != null) {
                range.close();
            }
        }

        LOG.debug(
                "Cleaned up data below horizon {}, reclaimed {} bytes, removed {} empty ranges. Current ranges: {}",
                horizonOffset,
                totalRemovedSize,
                rangesToRemove.size(),
                offsetRanges.size());
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        // Close all ranges
        for (OffsetRange range : offsetRanges.values()) {
            try {
                range.close();
            } catch (Exception e) {
                LOG.warn("Error closing offset range", e);
            }
        }

        offsetRanges.clear();
        totalSize = 0;

        LOG.debug("IndexBucketRowCache closed successfully");
    }

    // Internal helper methods

    private OffsetRange findRangeContaining(long offset) {
        for (OffsetRange range : offsetRanges.values()) {
            if (range.contains(offset)) {
                return range;
            }
        }
        return null;
    }

    private OffsetRange createNewRange(long startOffset) {
        OffsetRange newRange = new OffsetRange(startOffset, memoryPool);
        offsetRanges.put(startOffset, newRange);
        return newRange;
    }

    private void checkAndMergeWithNextRange(OffsetRange currentRange) {
        // Check if current range can be merged with the next range
        long currentEnd = currentRange.getEndOffset();
        Map.Entry<Long, OffsetRange> nextEntry = offsetRanges.ceilingEntry(currentEnd);

        if (nextEntry != null) {
            OffsetRange nextRange = nextEntry.getValue();
            if (nextRange.getStartOffset() == currentEnd) {
                // Ranges are adjacent - merge them
                currentRange.mergeWith(nextRange);
                offsetRanges.remove(nextEntry.getKey());
                try {
                    nextRange.close();
                } catch (IOException e) {
                    LOG.warn("Error closing merged range", e);
                }

                LOG.trace(
                        "Merged ranges: [{}, {}) + [{}, {}) = [{}, {})",
                        currentRange.getStartOffset(),
                        currentEnd,
                        nextRange.getStartOffset(),
                        nextRange.getEndOffset(),
                        currentRange.getStartOffset(),
                        currentRange.getEndOffset());
            }
        }
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
        private final List<MemorySegment> memorySegments = new ArrayList<>();
        private final List<SegmentMetadata> segmentMetadataList = new ArrayList<>();
        private final RowCacheIndex localIndex;

        OffsetRange(long startOffset, MemorySegmentPool memoryPool) {
            this.startOffset = startOffset;
            this.endOffset = startOffset;
            this.memoryPool = memoryPool;
            this.localIndex = new RowCacheIndex();
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public boolean contains(long offset) {
            return offset >= startOffset && offset < endOffset;
        }

        public boolean containsRange(long start, long end) {
            return start >= startOffset && end <= endOffset;
        }

        public boolean canAcceptOffset(long offset) {
            return offset == endOffset;
        }

        /**
         * Checks if there's actual IndexedRow data at the specified offset.
         *
         * @param offset the offset to check
         * @return true if actual IndexedRow data exists at this offset
         */
        public boolean hasDataAtOffset(long offset) {
            return localIndex.getIndexEntry(offset) != null;
        }

        /**
         * Expands the range boundary to include the given offset without writing actual data.
         *
         * @param logOffset the offset to expand the range to
         */
        public void expandRangeBoundary(long logOffset) {
            if (logOffset != endOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot expand range with non-adjacent offset. Range end: %d, Offset: %d",
                                endOffset, logOffset));
            }

            // Simply extend the range boundary without writing data
            endOffset = logOffset + 1;
        }

        /**
         * Writes IndexedRow data to this range, extending the right boundary.
         *
         * @param logOffset the log offset
         * @param changeType the change type of the row
         * @param row the row to write
         * @return the number of bytes written
         * @throws IOException if an error occurs during writing
         */
        public int writeIndexedRow(long logOffset, ChangeType changeType, IndexedRow row)
                throws IOException {
            if (logOffset != endOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot write non-adjacent offset to range. Range end: %d, Offset: %d",
                                endOffset, logOffset));
            }
            int estimateSize = row.getSizeInBytes() + 1 + LENGTH_LENGTH;

            // Find or allocate segment for this write
            SegmentMetadata targetSegment = findOrAllocateSegment(estimateSize);
            MemorySegment memorySegment = memorySegments.get(targetSegment.getSegmentIndex());

            // Write data to segment
            int segmentOffset = targetSegment.allocateSpace(estimateSize);

            MemorySegmentOutputView outputView = new MemorySegmentOutputView(memorySegment);
            outputView.setPosition(segmentOffset);
            int sizeInBytes = IndexedLogRecord.writeTo(outputView, changeType, row);

            // Update index
            RowCacheIndexEntry entry =
                    new RowCacheIndexEntry(
                            targetSegment.getSegmentIndex(), segmentOffset, sizeInBytes);
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
         * @param schemaId the schema ID for LogRecordBatch headers
         * @return LogRecords with accurate headers and metadata
         */
        public LogRecords getRangeLogRecords(long start, long end, short schemaId) {
            // Find the first entry >= start offset
            Map.Entry<Long, RowCacheIndexEntry> firstEntry = localIndex.getCeilingEntry(start);
            if (firstEntry == null) {
                return createEmptyLogRecords();
            }

            // Find the last entry < end offset
            Map.Entry<Long, RowCacheIndexEntry> lastEntry = localIndex.getFloorEntry(end - 1);
            if (lastEntry == null || lastEntry.getKey() < start) {
                return createEmptyLogRecords();
            }

            try {
                RowCacheIndexEntry firstIndexEntry = firstEntry.getValue();
                RowCacheIndexEntry lastIndexEntry = lastEntry.getValue();

                int firstSegmentIndex = firstIndexEntry.getSegmentIndex();
                int lastSegmentIndex = lastIndexEntry.getSegmentIndex();

                List<MemorySegmentBytesView> segmentBytesViews = new ArrayList<>();

                if (firstSegmentIndex == lastSegmentIndex) {
                    // All data in single segment - create one BytesView with proper boundaries
                    MemorySegment segment = memorySegments.get(firstSegmentIndex);
                    int startOffset = firstIndexEntry.getSegmentOffset();
                    int endOffset =
                            lastIndexEntry.getSegmentOffset() + lastIndexEntry.getRowLength();

                    MemorySegmentBytesView segmentView =
                            new MemorySegmentBytesView(
                                    segment, startOffset, endOffset - startOffset);
                    segmentBytesViews.add(segmentView);

                } else {
                    // Multiple segments involved

                    // First segment - from firstEntry to end of segment
                    MemorySegment firstSegment = memorySegments.get(firstSegmentIndex);
                    SegmentMetadata firstSegmentMeta = segmentMetadataList.get(firstSegmentIndex);
                    int firstStartOffset = firstIndexEntry.getSegmentOffset();
                    int firstEndOffset =
                            firstSegmentMeta.getNextOffset(); // End of used space in segment

                    if (firstEndOffset > firstStartOffset) {
                        MemorySegmentBytesView firstSegmentView =
                                new MemorySegmentBytesView(
                                        firstSegment,
                                        firstStartOffset,
                                        firstEndOffset - firstStartOffset);
                        segmentBytesViews.add(firstSegmentView);
                    }

                    // Middle segments - complete segments
                    for (int segIndex = firstSegmentIndex + 1;
                            segIndex < lastSegmentIndex;
                            segIndex++) {
                        MemorySegment segment = memorySegments.get(segIndex);
                        SegmentMetadata segmentMeta = segmentMetadataList.get(segIndex);

                        if (segmentMeta.getNextOffset() > 0) {
                            MemorySegmentBytesView segmentView =
                                    new MemorySegmentBytesView(
                                            segment, 0, segmentMeta.getNextOffset());
                            segmentBytesViews.add(segmentView);
                        }
                    }

                    // Last segment - from beginning to lastEntry end
                    MemorySegment lastSegment = memorySegments.get(lastSegmentIndex);
                    int lastEndOffset =
                            lastIndexEntry.getSegmentOffset() + lastIndexEntry.getRowLength();

                    if (lastEndOffset > 0) {
                        MemorySegmentBytesView lastSegmentView =
                                new MemorySegmentBytesView(lastSegment, 0, lastEndOffset);
                        segmentBytesViews.add(lastSegmentView);
                    }
                }

                if (segmentBytesViews.isEmpty()) {
                    return createEmptyLogRecords();
                }

                // Calculate the number of records in the range
                int recordCount = localIndex.getEntriesInRange(start, end).size();

                // Build LogRecords using MemoryLogRecordsIndexedBuilder with preWrittenByteViews
                BytesView recordBatchBytesView =
                        MemoryLogRecordsIndexedBuilder.builder(
                                        schemaId,
                                        // preWritten mode
                                        segmentBytesViews,
                                        recordCount,
                                        false)
                                .build();

                LOG.trace(
                        "Built LogRecords for range [{}, {}) using {} MemorySegments with {} bytes",
                        start,
                        end,
                        segmentBytesViews.size(),
                        recordBatchBytesView.getBytesLength());

                return new BytesViewLogRecords(recordBatchBytesView);

            } catch (Exception e) {
                LOG.warn("Failed to build LogRecords for range [{}, {})", start, end, e);
                return createEmptyLogRecords();
            }
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

            // Merge memory segments
            for (MemorySegment segment : other.memorySegments) {
                this.memorySegments.add(segment);
            }

            // Merge segment metadata with updated indices
            int baseIndex = this.segmentMetadataList.size();
            for (SegmentMetadata metadata : other.segmentMetadataList) {
                this.segmentMetadataList.add(metadata);
            }

            // Merge indices with updated segment references
            NavigableMap<Long, RowCacheIndexEntry> otherEntries = other.localIndex.getAllEntries();
            for (Map.Entry<Long, RowCacheIndexEntry> entry : otherEntries.entrySet()) {
                RowCacheIndexEntry indexEntry = entry.getValue();
                // Update segment index to reflect the merged segments
                RowCacheIndexEntry updatedEntry =
                        new RowCacheIndexEntry(
                                indexEntry.getSegmentIndex() + baseIndex,
                                indexEntry.getSegmentOffset(),
                                indexEntry.getRowLength());
                this.localIndex.addIndexEntry(entry.getKey(), updatedEntry);
            }
        }

        public int cleanupBelowHorizon(long horizonOffset) {
            if (memorySegments.isEmpty()) {
                return 0;
            }

            List<Integer> segmentsToRemove = new ArrayList<>();

            // Find segments that can be completely removed (all data < horizonOffset)
            for (int i = 0; i < segmentMetadataList.size(); i++) {
                Long maxOffset = localIndex.getMaxOffsetForSegment(i);
                if (maxOffset != null && maxOffset < horizonOffset) {
                    segmentsToRemove.add(i);
                }
            }

            if (segmentsToRemove.isEmpty()) {
                // Still need to remove individual entries below horizon - calculate size
                NavigableMap<Long, RowCacheIndexEntry> entriesToRemove =
                        localIndex.getEntriesInRange(Long.MIN_VALUE, horizonOffset);
                int removedSize = 0;
                for (RowCacheIndexEntry entry : entriesToRemove.values()) {
                    removedSize += entry.getRowLength();
                }
                localIndex.removeEntriesBelowHorizon(horizonOffset);
                return removedSize;
            }

            // Calculate size of data to be removed
            int removedSize = 0;
            for (int segmentIndex : segmentsToRemove) {
                // Sum up all row lengths in this segment
                NavigableMap<Long, RowCacheIndexEntry> allEntries = localIndex.getAllEntries();
                for (RowCacheIndexEntry entry : allEntries.values()) {
                    if (entry.getSegmentIndex() == segmentIndex) {
                        removedSize += entry.getRowLength();
                    }
                }
            }

            // Remove segments from back to front to avoid index shifting issues during removal
            Collections.reverse(segmentsToRemove);

            for (int segmentIndex : segmentsToRemove) {
                // Return memory segment to pool
                MemorySegment segmentToRemove = memorySegments.get(segmentIndex);
                memoryPool.returnPage(segmentToRemove);

                // Remove from lists
                memorySegments.remove(segmentIndex);
                segmentMetadataList.remove(segmentIndex);

                LOG.trace(
                        "Removed MemorySegment {} from OffsetRange, maxOffset was < horizon {}",
                        segmentIndex,
                        horizonOffset);
            }

            // Update index after segment removal
            if (!segmentsToRemove.isEmpty()) {
                Collections.reverse(segmentsToRemove); // Back to ascending order

                // Remove index entries for deleted segments and update remaining segment indices
                for (int i = 0; i < segmentsToRemove.size(); i++) {
                    int removedSegmentIndex = segmentsToRemove.get(i);

                    // Remove all entries for this segment
                    localIndex.removeEntriesForSegment(removedSegmentIndex);

                    // Adjust segment indices for all remaining segments after this removed one
                    // We subtract (i+1) because we've already removed i segments before this one
                    localIndex.adjustSegmentIndices(removedSegmentIndex - i, 1);
                }

                LOG.debug(
                        "Cleaned up {} MemorySegments below horizon {}, {} segments remaining",
                        segmentsToRemove.size(),
                        horizonOffset,
                        memorySegments.size());
            }

            // Clean up any remaining individual entries that are below horizon
            localIndex.removeEntriesBelowHorizon(horizonOffset);

            return removedSize;
        }

        public int size() {
            return localIndex.size();
        }

        public boolean isEmpty() {
            return localIndex.isEmpty();
        }

        @Override
        public void close() throws IOException {
            // Release memory segments back to the pool
            memoryPool.returnAll(memorySegments);
            memorySegments.clear();
            segmentMetadataList.clear();
            localIndex.clear();
        }

        private SegmentMetadata findOrAllocateSegment(int requiredSize) throws IOException {
            // Check if the last segment has enough space
            if (!segmentMetadataList.isEmpty()) {
                SegmentMetadata lastSegment =
                        segmentMetadataList.get(segmentMetadataList.size() - 1);
                if (lastSegment.hasSpace(requiredSize)) {
                    return lastSegment;
                }
            }

            // Allocate a new segment
            MemorySegment newSegment = memoryPool.nextSegment();
            if (newSegment == null) {
                throw new IOException("Cannot allocate new memory segment from pool");
            }

            memorySegments.add(newSegment);
            SegmentMetadata metadata = new SegmentMetadata(segmentMetadataList.size(), newSegment);
            segmentMetadataList.add(metadata);

            return metadata;
        }

        /** Metadata for tracking allocation within a MemorySegment. */
        private static class SegmentMetadata {
            private final int segmentIndex;
            private final MemorySegment segment;
            private int nextOffset = 0;

            SegmentMetadata(int segmentIndex, MemorySegment segment) {
                this.segmentIndex = segmentIndex;
                this.segment = segment;
            }

            public int getSegmentIndex() {
                return segmentIndex;
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
        }
    }
}
