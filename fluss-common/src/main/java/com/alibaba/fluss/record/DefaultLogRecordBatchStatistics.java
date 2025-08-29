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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinaryRowData;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.RowType;

import java.util.Arrays;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * Byte view implementation of LogRecordBatchStatistics that provides zero-copy access to statistics
 * data without creating heap objects or copying data. Uses IndexedRow for better performance.
 * Supports schema-aware format with partial column statistics.
 */
public class DefaultLogRecordBatchStatistics implements LogRecordBatchStatistics {

    private final MemorySegment segment;
    private final int position;
    private final int size;
    private final RowType rowType;
    private final int schemaId;

    private final int[] statsIndexMapping;

    private final Long[] statsNullCounts;

    // Offsets for min/max values in the byte array
    private final int minValuesOffset;
    private final int maxValuesOffset;
    private final int minValuesSize;
    private final int maxValuesSize;

    private InternalRow cachedMinRow;
    private InternalRow cachedMaxRow;
    private Long[] cachedNullCounts;

    private final int[] reversedStatsIndexMapping;

    /** Constructor for schema-aware statistics. */
    public DefaultLogRecordBatchStatistics(
            MemorySegment segment,
            int position,
            int size,
            RowType rowType,
            int schemaId,
            Long[] nullCounts,
            int minValuesOffset,
            int maxValuesOffset,
            int minValuesSize,
            int maxValuesSize,
            int[] statsIndexMapping) {
        this.segment = segment;
        this.position = position;
        this.size = size;
        this.rowType = rowType;
        this.schemaId = schemaId;
        this.statsNullCounts = nullCounts;
        this.minValuesOffset = minValuesOffset;
        this.maxValuesOffset = maxValuesOffset;
        this.minValuesSize = minValuesSize;
        this.maxValuesSize = maxValuesSize;
        this.statsIndexMapping = statsIndexMapping;
        this.reversedStatsIndexMapping = new int[rowType.getFieldCount()];
        Arrays.fill(this.reversedStatsIndexMapping, -1);
        for (int statsIndex = 0; statsIndex < statsIndexMapping.length; statsIndex++) {
            this.reversedStatsIndexMapping[statsIndexMapping[statsIndex]] = statsIndex;
        }
    }

    @Override
    public InternalRow getMinValues() {
        if (minValuesSize <= 0) {
            return null;
        }

        // Return cached row if already created
        if (cachedMinRow != null) {
            return cachedMinRow;
        }

        BinaryRowData minRow = new BinaryRowData(rowType.getFieldCount());
        minRow.pointTo(segment, position + minValuesOffset, minValuesSize);

        this.cachedMinRow = new FullRowWrapper(minRow, reversedStatsIndexMapping);
        return this.cachedMinRow;
    }

    @Override
    public InternalRow getMaxValues() {
        if (maxValuesSize <= 0) {
            return null;
        }

        // Return cached row if already created
        if (cachedMaxRow != null) {
            return cachedMaxRow;
        }

        BinaryRowData maxRow = new BinaryRowData(rowType.getFieldCount());
        maxRow.pointTo(segment, position + maxValuesOffset, maxValuesSize);

        this.cachedMaxRow = new FullRowWrapper(maxRow, reversedStatsIndexMapping);
        return this.cachedMaxRow;
    }

    @Override
    public Long[] getNullCounts() {
        if (cachedNullCounts != null) {
            return cachedNullCounts;
        }
        cachedNullCounts = new Long[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (this.reversedStatsIndexMapping[i] >= 0) {
                cachedNullCounts[i] = statsNullCounts[reversedStatsIndexMapping[i]];
            } else {
                cachedNullCounts[i] = -1L;
            }
        }
        return cachedNullCounts;
    }

    @Override
    public boolean hasColumnStatistics(int fieldIndex) {
        return reversedStatsIndexMapping[fieldIndex] != -1;
    }

    /**
     * Get the null count for a specific field.
     *
     * @param fieldIndex The field index
     * @return The null count for the field
     */
    public long getNullCount(int fieldIndex) {
        return statsNullCounts[fieldIndex] != null ? statsNullCounts[fieldIndex].longValue() : 0L;
    }

    /**
     * Get the underlying memory segment.
     *
     * @return The memory segment
     */
    public MemorySegment getSegment() {
        return segment;
    }

    /**
     * Get the position in the memory segment.
     *
     * @return The position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Get the total size of the statistics data.
     *
     * @return The size in bytes
     */
    public int getSize() {
        return size;
    }

    /**
     * Check if minimum values are available.
     *
     * @return true if minimum values are available
     */
    public boolean hasMinValues() {
        return minValuesSize > 0;
    }

    /**
     * Check if maximum values are available.
     *
     * @return true if maximum values are available
     */
    public boolean hasMaxValues() {
        return maxValuesSize > 0;
    }

    /**
     * Get the row type for this statistics.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    @Override
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DefaultLogRecordBatchStatistics{");
        sb.append(", hasMinValues=").append(hasMinValues());
        sb.append(", hasMaxValues=").append(hasMaxValues());
        sb.append(", size=").append(size);
        if (statsIndexMapping != null) {
            sb.append(", statisticsColumns=").append(Arrays.toString(statsIndexMapping));
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultLogRecordBatchStatistics that = (DefaultLogRecordBatchStatistics) o;

        if (position != that.position) {
            return false;
        }
        if (size != that.size) {
            return false;
        }
        if (minValuesOffset != that.minValuesOffset) {
            return false;
        }
        if (maxValuesOffset != that.maxValuesOffset) {
            return false;
        }
        if (minValuesSize != that.minValuesSize) {
            return false;
        }
        if (maxValuesSize != that.maxValuesSize) {
            return false;
        }
        if (!Arrays.equals(statsIndexMapping, that.statsIndexMapping)) {
            return false;
        }
        if (!Arrays.equals(statsNullCounts, that.statsNullCounts)) {
            return false;
        }
        if (!rowType.equals(that.rowType)) {
            return false;
        }
        return segment.equals(that.segment);
    }

    @Override
    public int hashCode() {
        int result = segment.hashCode();
        result = 31 * result + position;
        result = 31 * result + size;
        result = 31 * result + rowType.hashCode();
        result = 31 * result + Arrays.hashCode(statsIndexMapping);
        result = 31 * result + Arrays.hashCode(statsNullCounts);
        result = 31 * result + minValuesOffset;
        result = 31 * result + maxValuesOffset;
        result = 31 * result + minValuesSize;
        result = 31 * result + maxValuesSize;
        return result;
    }

    private static class FullRowWrapper implements InternalRow {

        private final InternalRow internalRow;

        private final int[] reversedStatsIndexMapping;

        FullRowWrapper(InternalRow internalRow, int[] reversedStatsIndexMapping) {
            this.internalRow = internalRow;
            this.reversedStatsIndexMapping = reversedStatsIndexMapping;
        }

        private void ensureColumnExists(int pos) {
            checkArgument(
                    pos >= 0 && pos < reversedStatsIndexMapping.length,
                    "Column index out of range.");
            checkArgument(
                    this.reversedStatsIndexMapping[pos] >= 0,
                    "Column index not available in underlying row data.");
        }

        @Override
        public int getFieldCount() {
            return reversedStatsIndexMapping.length;
        }

        @Override
        public boolean isNullAt(int pos) {
            ensureColumnExists(pos);
            return internalRow.isNullAt(reversedStatsIndexMapping[pos]);
        }

        @Override
        public boolean getBoolean(int pos) {
            ensureColumnExists(pos);
            return internalRow.getBoolean(reversedStatsIndexMapping[pos]);
        }

        @Override
        public byte getByte(int pos) {
            ensureColumnExists(pos);
            return internalRow.getByte(reversedStatsIndexMapping[pos]);
        }

        @Override
        public short getShort(int pos) {
            ensureColumnExists(pos);
            return internalRow.getShort(reversedStatsIndexMapping[pos]);
        }

        @Override
        public int getInt(int pos) {
            ensureColumnExists(pos);
            return internalRow.getInt(reversedStatsIndexMapping[pos]);
        }

        @Override
        public long getLong(int pos) {
            ensureColumnExists(pos);
            return internalRow.getLong(reversedStatsIndexMapping[pos]);
        }

        @Override
        public float getFloat(int pos) {
            ensureColumnExists(pos);
            return internalRow.getFloat(reversedStatsIndexMapping[pos]);
        }

        @Override
        public double getDouble(int pos) {
            ensureColumnExists(pos);
            return internalRow.getDouble(reversedStatsIndexMapping[pos]);
        }

        @Override
        public BinaryString getChar(int pos, int length) {
            ensureColumnExists(pos);
            return internalRow.getChar(reversedStatsIndexMapping[pos], length);
        }

        @Override
        public BinaryString getString(int pos) {
            ensureColumnExists(pos);
            return internalRow.getString(reversedStatsIndexMapping[pos]);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            ensureColumnExists(pos);
            return internalRow.getDecimal(reversedStatsIndexMapping[pos], precision, scale);
        }

        @Override
        public TimestampNtz getTimestampNtz(int pos, int precision) {
            ensureColumnExists(pos);
            return internalRow.getTimestampNtz(reversedStatsIndexMapping[pos], precision);
        }

        @Override
        public TimestampLtz getTimestampLtz(int pos, int precision) {
            ensureColumnExists(pos);
            return internalRow.getTimestampLtz(reversedStatsIndexMapping[pos], precision);
        }

        @Override
        public byte[] getBinary(int pos, int length) {
            ensureColumnExists(pos);
            return internalRow.getBinary(reversedStatsIndexMapping[pos], length);
        }

        @Override
        public byte[] getBytes(int pos) {
            ensureColumnExists(pos);
            return internalRow.getBytes(reversedStatsIndexMapping[pos]);
        }
    }
}
