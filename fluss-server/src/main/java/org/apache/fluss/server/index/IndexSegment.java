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

    private final long startOffset;
    private final long endOffset;
    private final LogRecords records;

    /**
     * Creates a new IndexSegment.
     *
     * @param startOffset the start offset of the WAL address space (inclusive)
     * @param endOffset the end offset of the WAL address space (exclusive)
     * @param records the log records for this segment (memory lifecycle managed by IndexCache)
     */
    public IndexSegment(long startOffset, long endOffset, LogRecords records) {
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
     * Returns the size of the offset range covered by this segment.
     *
     * @return the number of offsets in the range (endOffset - startOffset - 1)
     */
    public long size() {
        return endOffset - startOffset - 1;
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

    public static IndexSegment createEmptySegment(long endOffset) {
        return new IndexSegment(endOffset, endOffset, MemoryLogRecords.EMPTY);
    }

    @Override
    public String toString() {
        return String.format(
                "IndexSegment{startOffset=%d, endOffset=%d, records=%s}",
                startOffset, endOffset, records.getClass().getSimpleName());
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
                && records.equals(that.records);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(startOffset);
        result = 31 * result + Long.hashCode(endOffset);
        result = 31 * result + records.hashCode();
        return result;
    }
}
