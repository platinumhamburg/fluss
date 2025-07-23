package com.alibaba.fluss.record;

import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalRow;

/** Statistics infomation of {@link LogRecordBatch LogRecordBatch}. */
public interface LogRecordBatchStatistics {

    long getRowCount();

    InternalRow getMinValues();

    InternalRow getMaxValues();

    InternalArray getNullCounts();
}
