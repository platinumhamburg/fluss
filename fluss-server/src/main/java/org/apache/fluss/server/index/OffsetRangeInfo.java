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
 * OffsetRangeInfo represents a continuous offset range that needs to be loaded from WAL in the
 * Range-based IndexBucketRowCache architecture.
 *
 * <p>This class is a simplified replacement for OffsetRangeInfo, focusing only on representing
 * ranges that need data loading without complex status management.
 *
 * <p>The range follows the standard [startOffset, endOffset) semantic: - startOffset is inclusive -
 * endOffset is exclusive
 */
@Internal
public final class OffsetRangeInfo {

    private final long startOffset;
    private final long endOffset;

    /**
     * Creates a new OffsetRangeInfo.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     */
    public OffsetRangeInfo(long startOffset, long endOffset) {
        if (startOffset >= endOffset) {
            throw new IllegalArgumentException(
                    String.format(
                            "Start offset must be less than end offset. startOffset: %d, endOffset: %d",
                            startOffset, endOffset));
        }
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    /**
     * Gets the start offset (inclusive).
     *
     * @return the start offset
     */
    public long getStartOffset() {
        return startOffset;
    }

    /**
     * Gets the end offset (exclusive).
     *
     * @return the end offset
     */
    public long getEndOffset() {
        return endOffset;
    }

    /**
     * Gets the size of this range.
     *
     * @return the range size (endOffset - startOffset)
     */
    public long getSize() {
        return endOffset - startOffset;
    }

    /**
     * Checks if this range contains the specified offset.
     *
     * @param offset the offset to check
     * @return true if the offset is within this range [startOffset, endOffset)
     */
    public boolean contains(long offset) {
        return offset >= startOffset && offset < endOffset;
    }

    @Override
    public String toString() {
        return String.format("OffsetRangeInfo[%d, %d)", startOffset, endOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OffsetRangeInfo that = (OffsetRangeInfo) obj;
        return startOffset == that.startOffset && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(startOffset) * 31 + Long.hashCode(endOffset);
    }
}
