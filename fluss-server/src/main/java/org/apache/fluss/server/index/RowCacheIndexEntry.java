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

/**
 * RowCacheIndexEntry represents the memory location of an IndexedRow in the memory segments. This
 * is a compact data structure used in RowCacheIndex to minimize memory overhead.
 *
 * <p>Each RowCacheIndexEntry contains: - segmentIndex: The index of the memory segment containing
 * the row - segmentOffset: The offset within the memory segment where the row starts - rowLength:
 * The length of the serialized IndexedRow data
 */
@Internal
public final class RowCacheIndexEntry {

    private final int segmentIndex;
    private final int segmentOffset;
    private final int rowLength;

    /**
     * Creates a new RowCacheIndexEntry.
     *
     * @param segmentIndex the index of the memory segment
     * @param segmentOffset the offset within the segment
     * @param rowLength the length of the row data
     */
    public RowCacheIndexEntry(int segmentIndex, int segmentOffset, int rowLength) {
        if (segmentIndex < 0) {
            throw new IllegalArgumentException("Segment index cannot be negative: " + segmentIndex);
        }
        if (segmentOffset < 0) {
            throw new IllegalArgumentException(
                    "Segment offset cannot be negative: " + segmentOffset);
        }
        if (rowLength <= 0) {
            throw new IllegalArgumentException("Row length must be positive: " + rowLength);
        }

        this.segmentIndex = segmentIndex;
        this.segmentOffset = segmentOffset;
        this.rowLength = rowLength;
    }

    /**
     * Gets the index of the memory segment.
     *
     * @return the segment index
     */
    public int getSegmentIndex() {
        return segmentIndex;
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
     * Calculates the end offset within the segment.
     *
     * @return the end offset (segmentOffset + rowLength)
     */
    public int getEndOffset() {
        return segmentOffset + rowLength;
    }

    @Override
    public String toString() {
        return String.format(
                "RowCacheIndexEntry{segmentIndex=%d, segmentOffset=%d, rowLength=%d}",
                segmentIndex, segmentOffset, rowLength);
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
        return segmentIndex == that.segmentIndex
                && segmentOffset == that.segmentOffset
                && rowLength == that.rowLength;
    }

    @Override
    public int hashCode() {
        int result = segmentIndex;
        result = 31 * result + segmentOffset;
        result = 31 * result + rowLength;
        return result;
    }
}
