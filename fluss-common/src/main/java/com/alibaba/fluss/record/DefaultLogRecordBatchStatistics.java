package com.alibaba.fluss.record;

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalRow;

/** Default implementation of {@link LogRecordBatchStatistics}. */
public class DefaultLogRecordBatchStatistics implements LogRecordBatchStatistics {
    @Override
    public long getRowCount() {
        return 0;
    }

    @Override
    public InternalRow getMinValues() {
        return null;
    }

    @Override
    public InternalRow getMaxValues() {
        return null;
    }

    @Override
    public InternalArray getNullCounts() {
        return null;
    }
}
