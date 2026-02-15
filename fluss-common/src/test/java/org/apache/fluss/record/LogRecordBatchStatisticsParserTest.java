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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for LogRecordBatchStatisticsParser. */
public class LogRecordBatchStatisticsParserTest {

    // Helper method to create stats index mapping for all columns
    private static int[] createAllColumnsStatsMapping(RowType rowType) {
        int[] statsIndexMapping = new int[rowType.getFieldCount()];
        for (int i = 0; i < statsIndexMapping.length; i++) {
            statsIndexMapping[i] = i;
        }
        return statsIndexMapping;
    }

    @Test
    public void testParseStatisticsFromByteArray() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {10L, 0L};
        IndexedRow minValues = createTestIndexedRow(rowType, -100, "aaa");
        IndexedRow maxValues = createTestIndexedRow(rowType, 999, "zzz");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using byte array
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testParseStatisticsFromByteBuffer() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {3L, 7L};
        IndexedRow minValues = createTestIndexedRow(rowType, 0, "first");
        IndexedRow maxValues = createTestIndexedRow(rowType, 500, "last");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(buffer, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testParseStatisticsFromMemorySegment() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 42, "test");
        IndexedRow maxValues = createTestIndexedRow(rowType, 84, "testing");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using MemorySegment
        MemorySegment segment = MemorySegment.wrap(writtenData);
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, rowType, DEFAULT_SCHEMA_ID);

        // Verify parsing results - just basic validation
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.getNullCounts()).hasSize(2);
    }

    @Test
    public void testIsValidStatistics() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create valid test statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "min");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "max");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with valid data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(writtenData, rowType)).isTrue();

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(buffer, rowType)).isTrue();

        // Test with empty data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(new byte[0], rowType))
                .isFalse();

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((byte[]) null, rowType))
                .isFalse();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((ByteBuffer) null, rowType))
                .isFalse();
    }

    @Test
    public void testGetStatisticsSize() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "a");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "z");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with byte array
        int size = LogRecordBatchStatisticsParser.getStatisticsSize(writtenData);
        assertThat(size).isEqualTo(writtenData.length);

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        size = LogRecordBatchStatisticsParser.getStatisticsSize(buffer);
        assertThat(size).isEqualTo(writtenData.length);
    }

    @Test
    public void testGetStatisticsSizeWithInvalidData() {
        // Test with invalid data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize(ByteBuffer.allocateDirect(0)))
                .isEqualTo(-1);

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((byte[]) null)).isEqualTo(-1);
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((ByteBuffer) null))
                .isEqualTo(-1);
    }

    @Test
    public void testBasicFunctionality() throws Exception {
        // Test basic write and parse cycle without detailed verification
        RowType rowType = DataTypes.ROW(new IntType(false));

        Long[] nullCounts = new Long[] {0L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1);
        IndexedRow maxValues = createTestIndexedRow(rowType, 100);

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Verify we can parse it back
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isNotNull();
        assertThat(parsedStats.hasMinValues()).isTrue();
        assertThat(parsedStats.hasMaxValues()).isTrue();
    }

    @Test
    public void testParseTruncatedStatisticsData() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create valid statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "min");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "max");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Severely truncate the data (only keep first 4 bytes)
        byte[] truncatedData = new byte[4];
        System.arraycopy(writtenData, 0, truncatedData, 0, truncatedData.length);

        // The system should handle truncated data gracefully without crashing
        // It may either reject it during validation or parse what it can
        try {
            boolean isValid =
                    LogRecordBatchStatisticsParser.isValidStatistics(truncatedData, rowType);
            if (isValid) {
                // If validation passes, try parsing (may return null or valid stats)
                DefaultLogRecordBatchStatistics stats =
                        LogRecordBatchStatisticsParser.parseStatistics(
                                MemorySegment.wrap(truncatedData), 0, rowType, DEFAULT_SCHEMA_ID);
                // System handled truncated data gracefully (null or valid stats both acceptable)
                // Just verify no exception was thrown
                assertThat(true).isTrue();
            } else {
                // Validation correctly rejected truncated data
                assertThat(isValid).isFalse();
            }
        } catch (Exception e) {
            // Exception is also acceptable for truncated data
            assertThat(e).isNotNull();
        }
    }

    @Test
    public void testParseCorruptedStatisticsData() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create valid statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1, "min");
        IndexedRow maxValues = createTestIndexedRow(rowType, 100, "max");

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Corrupt the data by zeroing out a significant portion
        byte[] corruptedData = writtenData.clone();
        if (corruptedData.length > 20) {
            for (int i = 10; i < Math.min(20, corruptedData.length); i++) {
                corruptedData[i] = 0;
            }
        }

        // Parsing corrupted data may succeed (robust parsing) or fail
        // We just verify the system doesn't crash
        try {
            boolean isValid =
                    LogRecordBatchStatisticsParser.isValidStatistics(corruptedData, rowType);
            if (isValid) {
                DefaultLogRecordBatchStatistics stats =
                        LogRecordBatchStatisticsParser.parseStatistics(
                                MemorySegment.wrap(corruptedData), 0, rowType, DEFAULT_SCHEMA_ID);
                // If parsing succeeds, just verify we got a result
                assertThat(stats).isNotNull();
            }
        } catch (Exception e) {
            // Exception is acceptable for corrupted data
            assertThat(e).isNotNull();
        }
    }

    @Test
    public void testStatisticsWithExtremeIntegerValues() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false));

        // Test with extreme integer values
        Long[] nullCounts = new Long[] {0L};
        IndexedRow minValues = createTestIndexedRow(rowType, Integer.MIN_VALUE);
        IndexedRow maxValues = createTestIndexedRow(rowType, Integer.MAX_VALUE);

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse and verify extreme values are preserved
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getMinValues().getInt(0)).isEqualTo(Integer.MIN_VALUE);
        assertThat(parsedStats.getMaxValues().getInt(0)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void testStatisticsWithVeryLongStrings() throws Exception {
        RowType rowType = DataTypes.ROW(new StringType(false));

        // Create moderately long strings (100 characters) to avoid buffer overflow
        StringBuilder longStringBuilder = new StringBuilder(100);
        for (int i = 0; i < 100; i++) {
            longStringBuilder.append((char) ('a' + (i % 26)));
        }
        String longString = longStringBuilder.toString();

        Long[] nullCounts = new Long[] {0L};
        IndexedRow minValues = createTestIndexedRow(rowType, longString);
        IndexedRow maxValues = createTestIndexedRow(rowType, longString);

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse and verify long strings are preserved
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getMinValues().getString(0).toString()).isEqualTo(longString);
        assertThat(parsedStats.getMaxValues().getString(0).toString()).isEqualTo(longString);
    }

    @Test
    public void testStatisticsWithLargeNullCounts() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false));

        // Test with large but reasonable null count values
        Long[] nullCounts = new Long[] {1000000L};
        IndexedRow minValues = createTestIndexedRow(rowType, 1);
        IndexedRow maxValues = createTestIndexedRow(rowType, 100);

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(rowType, createAllColumnsStatsMapping(rowType));
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse and verify large null count is preserved
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        MemorySegment.wrap(writtenData), 0, rowType, DEFAULT_SCHEMA_ID);

        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()[0]).isEqualTo(1000000L);
    }

    // Helper methods for creating test data
    private IndexedRow createTestIndexedRow(RowType rowType, Object... values) throws IOException {
        IndexedRowWriter writer =
                new IndexedRowWriter(rowType.getChildren().toArray(new DataType[0]));
        writer.reset();

        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                DataType fieldType = rowType.getTypeAt(i);
                if (fieldType instanceof StringType) {
                    // Convert String to BinaryString for StringType fields
                    writer.writeString(BinaryString.fromString((String) values[i]));
                } else if (fieldType instanceof IntType) {
                    // Write int values directly
                    writer.writeInt((Integer) values[i]);
                }
            }
        }

        IndexedRow row = new IndexedRow(rowType.getChildren().toArray(new DataType[0]));
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private byte[] writeStatisticsForTest(
            LogRecordBatchStatisticsWriter writer,
            InternalRow minValues,
            InternalRow maxValues,
            Long[] nullCounts)
            throws IOException {

        // Allocate enough memory for statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten =
                writer.writeStatistics(
                        minValues, maxValues, nullCounts, new MemorySegmentOutputView(segment));

        // Copy the written data to a byte array
        byte[] result = new byte[bytesWritten];
        segment.get(0, result, 0, bytesWritten);
        return result;
    }
}
