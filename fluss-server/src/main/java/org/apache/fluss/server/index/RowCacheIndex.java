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

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * RowCacheIndex provides efficient mapping from logOffset to memory locations within a Range-based
 * architecture.
 *
 * <p>This class maintains a single key data structure: - offsetEntries: TreeMap for mapping
 * logOffset to RowCacheIndexEntry (memory location)
 *
 * <p>The index supports: - O(log n) range queries using TreeMap - Direct offset to memory location
 * mapping - Efficient cleanup based on commit horizon
 *
 * <p>In the Range-based architecture, status management is handled at the Range level, so this
 * class focuses purely on offset-to-memory-location mapping.
 *
 * <p>Thread Safety: This class is NOT thread-safe. External synchronization is required.
 */
@Internal
public final class RowCacheIndex {

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
     * Gets the RowCacheIndexEntry for a specific logOffset.
     *
     * @param logOffset the log offset
     * @return the RowCacheIndexEntry, or null if not found
     */
    public RowCacheIndexEntry getIndexEntry(long logOffset) {
        return offsetEntries.get(logOffset);
    }

    /**
     * Checks if a specific logOffset exists in the index.
     *
     * @param logOffset the log offset to check
     * @return true if the offset exists, false otherwise
     */
    public boolean containsOffset(long logOffset) {
        return offsetEntries.containsKey(logOffset);
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
     * Gets the maximum offset for entries in a specific segment.
     *
     * @param segmentIndex the segment index
     * @return the maximum offset in the segment, or null if no entries found
     */
    public Long getMaxOffsetForSegment(int segmentIndex) {
        Long maxOffset = null;
        for (java.util.Map.Entry<Long, RowCacheIndexEntry> entry : offsetEntries.entrySet()) {
            if (entry.getValue().getSegmentIndex() == segmentIndex) {
                maxOffset = entry.getKey();
            }
        }
        return maxOffset;
    }

    /**
     * Removes all entries for a specific segment index.
     *
     * @param segmentIndex the segment index
     * @return the number of entries removed
     */
    public int removeEntriesForSegment(int segmentIndex) {
        java.util.List<Long> toRemove = new java.util.ArrayList<>();
        for (java.util.Map.Entry<Long, RowCacheIndexEntry> entry : offsetEntries.entrySet()) {
            if (entry.getValue().getSegmentIndex() == segmentIndex) {
                toRemove.add(entry.getKey());
            }
        }
        for (Long offset : toRemove) {
            offsetEntries.remove(offset);
        }
        return toRemove.size();
    }

    /**
     * Updates segment indices for all entries with segmentIndex >= baseIndex by subtracting
     * adjustment.
     *
     * @param baseIndex the minimum segment index to update
     * @param adjustment the amount to subtract from segment indices
     */
    public void adjustSegmentIndices(int baseIndex, int adjustment) {
        java.util.Map<Long, RowCacheIndexEntry> updatedEntries = new java.util.HashMap<>();
        for (java.util.Map.Entry<Long, RowCacheIndexEntry> entry : offsetEntries.entrySet()) {
            RowCacheIndexEntry indexEntry = entry.getValue();
            if (indexEntry.getSegmentIndex() >= baseIndex) {
                RowCacheIndexEntry updatedEntry =
                        new RowCacheIndexEntry(
                                indexEntry.getSegmentIndex() - adjustment,
                                indexEntry.getSegmentOffset(),
                                indexEntry.getRowLength());
                updatedEntries.put(entry.getKey(), updatedEntry);
            }
        }
        // Update entries in offsetEntries
        for (java.util.Map.Entry<Long, RowCacheIndexEntry> entry : updatedEntries.entrySet()) {
            offsetEntries.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Removes all entries with offsets less than the given horizon. This is used for cleanup based
     * on index commit horizon.
     *
     * @param horizon the commit horizon
     * @return the number of entries removed
     */
    public int removeEntriesBelowHorizon(long horizon) {
        NavigableMap<Long, RowCacheIndexEntry> toRemove = offsetEntries.headMap(horizon, false);
        int removedCount = toRemove.size();
        toRemove.clear();
        return removedCount;
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
     * Gets the ceiling entry for the given offset (first entry >= offset).
     *
     * @param offset the offset to search from
     * @return the ceiling entry, or null if not found
     */
    public java.util.Map.Entry<Long, RowCacheIndexEntry> getCeilingEntry(long offset) {
        return offsetEntries.ceilingEntry(offset);
    }

    /**
     * Gets the floor entry for the given offset (last entry <= offset).
     *
     * @param offset the offset to search from
     * @return the floor entry, or null if not found
     */
    public java.util.Map.Entry<Long, RowCacheIndexEntry> getFloorEntry(long offset) {
        return offsetEntries.floorEntry(offset);
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
