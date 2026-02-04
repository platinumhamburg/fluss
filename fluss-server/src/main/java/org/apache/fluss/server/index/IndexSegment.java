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
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;

/**
 * IndexSegment represents index records mapped to a specific bucket within a continuous WAL address
 * space.
 *
 * <p>This class encapsulates a segment of index records that correspond to a range of WAL offsets
 * [startOffset, endOffset) where startOffset is inclusive and endOffset is exclusive.
 *
 * <p>The memory lifecycle of the records is managed by the IndexCache system, eliminating the need
 * for self-managed memory cleanup within IndexSegment.
 */
@Internal
public final class IndexSegment {

    /** Status of the data in this segment. */
    public enum DataStatus {
        /** Data is loaded and available in the segment (may be empty but range is loaded). */
        LOADED,
        /** Data not yet loaded from WAL (needs cold loading). */
        NOT_READY
    }

    private final long startOffset;
    private final long endOffset;
    private final LogRecords records;
    private final DataStatus status;

    /**
     * Creates a new IndexSegment.
     *
     * @param startOffset the start offset of the WAL address space (inclusive)
     * @param endOffset the end offset of the WAL address space (exclusive)
     * @param records the log records for this segment (memory lifecycle managed by IndexCache)
     * @param status the status of the data in this segment
     */
    public IndexSegment(long startOffset, long endOffset, LogRecords records, DataStatus status) {
        if (startOffset > endOffset) {
            throw new IllegalArgumentException(
                    "Start offset must be less or equal than end offset. "
                            + "startOffset: "
                            + startOffset
                            + ", endOffset: "
                            + endOffset);
        }
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.records = records;
        this.status = status;
    }

    /**
     * Gets the start offset of the WAL address space (inclusive).
     *
     * @return the start offset
     */
    public long getStartOffset() {
        return startOffset;
    }

    /**
     * Gets the end offset of the WAL address space (exclusive).
     *
     * @return the end offset
     */
    public long getEndOffset() {
        return endOffset;
    }

    /**
     * Gets the log records for this segment.
     *
     * @return the log records
     */
    public LogRecords getRecords() {
        return records;
    }

    /**
     * Gets the status of the data in this segment.
     *
     * @return the data status
     */
    public DataStatus getStatus() {
        return status;
    }

    /**
     * Returns the size of the offset range covered by this segment.
     *
     * @return the number of offsets in the range (endOffset - startOffset)
     */
    public long size() {
        return endOffset - startOffset;
    }

    /**
     * Checks if the given offset is within this segment's range.
     *
     * @param offset the offset to check
     * @return true if the offset is within the range [startOffset, endOffset)
     */
    public boolean contains(long offset) {
        return offset >= startOffset && offset < endOffset;
    }

    public boolean isEmpty() {
        return startOffset == endOffset;
    }

    /**
     * Creates an empty segment indicating no data to fetch at this offset.
     *
     * @param endOffset the end offset
     * @return an empty IndexSegment with LOADED status (loaded but empty)
     */
    public static IndexSegment createEmptySegment(long endOffset) {
        return new IndexSegment(endOffset, endOffset, MemoryLogRecords.EMPTY, DataStatus.LOADED);
    }

    /**
     * Creates a segment indicating data is not ready (not loaded in cache yet).
     *
     * @param startOffset the start offset of the requested range
     * @param endOffset the end offset of the requested range
     * @return an IndexSegment with NOT_READY status
     */
    public static IndexSegment createNotReadySegment(long startOffset, long endOffset) {
        return new IndexSegment(
                startOffset, endOffset, MemoryLogRecords.EMPTY, DataStatus.NOT_READY);
    }

    @Override
    public String toString() {
        return String.format(
                "IndexSegment{startOffset=%d, endOffset=%d, status=%s, records=%s}",
                startOffset, endOffset, status, records.getClass().getSimpleName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexSegment that = (IndexSegment) o;
        return startOffset == that.startOffset
                && endOffset == that.endOffset
                && status == that.status
                && records.equals(that.records);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(startOffset);
        result = 31 * result + Long.hashCode(endOffset);
        result = 31 * result + status.hashCode();
        result = 31 * result + records.hashCode();
        return result;
    }
}
