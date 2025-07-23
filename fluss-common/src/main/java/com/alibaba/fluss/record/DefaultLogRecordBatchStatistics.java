package com.alibaba.fluss.record;

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalRow;

/** Default implementation of {@link LogRecordBatchStatistics}. */
public class DefaultLogRecordBatchStatistics implements LogRecordBatchStatistics {

    private final long rowCount;
    private final InternalRow minValues;
    private final InternalRow maxValues;
    private final InternalArray nullCounts;

    public DefaultLogRecordBatchStatistics(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        this.rowCount = rowCount;
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.nullCounts = nullCounts;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public InternalRow getMinValues() {
        return minValues;
    }

    @Override
    public InternalRow getMaxValues() {
        return maxValues;
    }

    @Override
    public InternalArray getNullCounts() {
        return nullCounts;
    }

    @Override
    public String toString() {
        return "DefaultLogRecordBatchStatistics{"
                + "rowCount="
                + rowCount
                + ", minValues="
                + minValues
                + ", maxValues="
                + maxValues
                + ", nullCounts="
                + nullCounts
                + '}';
    }
}
