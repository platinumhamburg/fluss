/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericArray;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogRecordBatchStatistics} and {@link LogRecordBatchStatisticsSerializer}. */
public class LogRecordBatchStatisticsTest {

    @Test
    public void testStatisticsCollectorBasic() {
        RowType rowType =
                DataTypes.ROW(new IntType(false), new StringType(false), new BooleanType(false));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process some rows
        collector.processRow(
                GenericRow.of(new Object[] {1, BinaryString.fromString("hello"), true}));
        collector.processRow(
                GenericRow.of(new Object[] {2, BinaryString.fromString("world"), false}));
        collector.processRow(
                GenericRow.of(new Object[] {3, BinaryString.fromString("test"), true}));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();
        assertThat(stats.getRowCount()).isEqualTo(3);

        // Check min values
        assertThat(stats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(stats.getMinValues().getBoolean(2)).isEqualTo(false);

        // Check max values
        assertThat(stats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(stats.getMaxValues().getString(1).toString()).isEqualTo("world");
        assertThat(stats.getMaxValues().getBoolean(2)).isEqualTo(true);

        // Check null counts (should be 0 for all fields)
        assertThat(stats.getNullCounts().getLong(0)).isEqualTo(0);
        assertThat(stats.getNullCounts().getLong(1)).isEqualTo(0);
        assertThat(stats.getNullCounts().getLong(2)).isEqualTo(0);
    }

    @Test
    public void testStatisticsCollectorWithNulls() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with some null values
        Object[] row1 = new Object[2];
        row1[0] = 1;
        row1[1] = BinaryString.fromString("hello");
        collector.processRow(GenericRow.of(row1));

        Object[] row2 = new Object[2];
        row2[0] = null;
        row2[1] = BinaryString.fromString("world");
        collector.processRow(GenericRow.of(row2));

        Object[] row3 = new Object[2];
        row3[0] = 3;
        row3[1] = null;
        collector.processRow(GenericRow.of(row3));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();
        assertThat(stats.getRowCount()).isEqualTo(3);

        // Check null counts
        assertThat(stats.getNullCounts().getLong(0)).isEqualTo(1); // one null in first column
        assertThat(stats.getNullCounts().getLong(1)).isEqualTo(1); // one null in second column

        // Check min/max values (should only consider non-null values)
        assertThat(stats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(stats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(stats.getMaxValues().getString(1).toString()).isEqualTo("world");
    }

    @Test
    public void testStatisticsSerializationDeserialization() throws IOException {
        RowType rowType =
                DataTypes.ROW(new IntType(false), new StringType(false), new BooleanType(false));

        // Create statistics
        InternalRow minValues =
                GenericRow.of(new Object[] {1, BinaryString.fromString("hello"), false});
        InternalRow maxValues =
                GenericRow.of(new Object[] {3, BinaryString.fromString("world"), true});
        GenericArray nullCounts = new GenericArray(new long[] {0, 0, 0});

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(3, minValues, maxValues, nullCounts);

        // Serialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        assertThat(serialized).isNotEmpty();

        // Deserialize
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);
        assertThat(deserializedStats).isNotNull();
        assertThat(deserializedStats.getRowCount()).isEqualTo(originalStats.getRowCount());

        // Check min values
        assertThat(deserializedStats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(deserializedStats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(deserializedStats.getMinValues().getBoolean(2)).isEqualTo(false);

        // Check max values
        assertThat(deserializedStats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(deserializedStats.getMaxValues().getString(1).toString()).isEqualTo("world");
        assertThat(deserializedStats.getMaxValues().getBoolean(2)).isEqualTo(true);

        // Check null counts
        assertThat(deserializedStats.getNullCounts().getLong(0)).isEqualTo(0);
        assertThat(deserializedStats.getNullCounts().getLong(1)).isEqualTo(0);
        assertThat(deserializedStats.getNullCounts().getLong(2)).isEqualTo(0);
    }

    @Test
    public void testEmptyStatisticsSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT());

        // Test null statistics
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(null, rowType);
        assertThat(serialized).isEmpty();

        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);
        assertThat(deserializedStats).isNull();

        // Test empty data
        LogRecordBatchStatistics emptyStats =
                LogRecordBatchStatisticsSerializer.deserialize(new byte[0], rowType);
        assertThat(emptyStats).isNull();
    }
}
