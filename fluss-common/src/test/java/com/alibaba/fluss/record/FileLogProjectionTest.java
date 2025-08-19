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

import com.alibaba.fluss.exception.InvalidColumnProjectionException;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.alibaba.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static com.alibaba.fluss.record.LogRecordReadContext.createArrowReadContext;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FileLogProjection}. */
class FileLogProjectionTest {

    private @TempDir File tempDir;

    // TODO: add tests for nested types
    @Test
    void testSetCurrentProjection() {
        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {0, 2});
        FileLogProjection.ProjectionInfo info1 = projection.currentProjection;
        assertThat(info1).isNotNull();
        assertThat(info1.nodesProjection.stream().toArray()).isEqualTo(new int[] {0, 2});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info1.buffersProjection.stream().toArray()).isEqualTo(new int[] {0, 1, 5, 6, 7});
        assertThat(projection.projectionsCache).hasSize(1);
        assertThat(projection.projectionsCache.get(1L)).isSameAs(info1);

        projection.setCurrentProjection(
                2L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {1});
        FileLogProjection.ProjectionInfo info2 = projection.currentProjection;
        assertThat(info2).isNotNull();
        assertThat(info2.nodesProjection.stream().toArray()).isEqualTo(new int[] {1});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info2.buffersProjection.stream().toArray()).isEqualTo(new int[] {2, 3, 4});
        assertThat(projection.projectionsCache).hasSize(2);
        assertThat(projection.projectionsCache.get(2L)).isSameAs(info2);

        projection.setCurrentProjection(
                1L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {0, 2});
        assertThat(projection.currentProjection).isNotNull().isSameAs(info1);

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA1_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {1}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The schema and projection should be identical for the same table id.");
    }

    @Test
    void testIllegalSetCurrentProjection() {
        FileLogProjection projection = new FileLogProjection();

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {3}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("Projected fields [3] is out of bound for schema with 3 fields.");

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {1, 0}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The projection indexes should be in field order, but is [1, 0]");

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {0, 0, 0}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage(
                        "The projection indexes should not contain duplicated fields, but is [0, 0, 0]");
    }

    static Stream<Arguments> projectedFieldsArgs() {
        return Stream.of(
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V0),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V0),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V0),
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V1),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V1),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V1));
    }

    @ParameterizedTest
    @MethodSource("projectedFieldsArgs")
    void testProject(int[] projectedFields, byte recordBatchMagic) throws Exception {
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        List<Object[]> results =
                doProjection(
                        new FileLogProjection(),
                        fileLogRecords,
                        TestData.DATA1_ROW_TYPE,
                        projectedFields,
                        Integer.MAX_VALUE);
        List<Object[]> allData = new ArrayList<>();
        allData.addAll(TestData.DATA1);
        allData.addAll(TestData.ANOTHER_DATA1);
        List<Object[]> expected = new ArrayList<>();
        assertThat(results.size()).isEqualTo(allData.size());
        for (Object[] data : allData) {
            Object[] objs = new Object[projectedFields.length];
            for (int j = 0; j < projectedFields.length; j++) {
                objs[j] = data[projectedFields[j]];
            }
            expected.add(objs);
        }
        assertEquals(results, expected);
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testIllegalByteOrder(byte recordBatchMagic) throws Exception {
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        FileLogProjection projection = new FileLogProjection();
        // overwrite the wrong decoding byte order endian
        projection.getLogHeaderBuffer(recordBatchMagic).order(ByteOrder.BIG_ENDIAN);
        // should throw exception.
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        projection,
                                        fileLogRecords,
                                        TestData.DATA1_ROW_TYPE,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Failed to read `arrow header` from file channel");
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testProjectSizeLimited(byte recordBatchMagic) throws Exception {
        List<Object[]> allData = new ArrayList<>();
        allData.addAll(TestData.DATA1);
        allData.addAll(TestData.ANOTHER_DATA1);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        int totalSize = fileLogRecords.sizeInBytes();
        boolean hasEmpty = false;
        boolean hasHalf = false;
        boolean hasFull = false;
        for (int i = 4; i >= 1; i--) {
            int maxBytes = totalSize / i;
            List<Object[]> results =
                    doProjection(
                            new FileLogProjection(),
                            fileLogRecords,
                            TestData.DATA1_ROW_TYPE,
                            new int[] {0, 1},
                            maxBytes);
            if (results.isEmpty()) {
                hasEmpty = true;
            } else if (results.size() == TestData.DATA1.size()) {
                hasHalf = true;
                assertEquals(results, TestData.DATA1);
            } else if (results.size() == allData.size()) {
                hasFull = true;
                assertEquals(results, allData);
            } else {
                fail("Unexpected result size: " + results.size());
            }
        }
        assertThat(hasEmpty).isTrue();
        assertThat(hasHalf).isTrue();
        assertThat(hasFull).isTrue();
    }

    @SafeVarargs
    private final FileLogRecords createFileLogRecords(
            byte recordBatchMagic, RowType rowType, List<Object[]>... inputs) throws Exception {
        FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
        long offsetBase = 0L;
        for (List<Object[]> input : inputs) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            rowType,
                            DEFAULT_SCHEMA_ID,
                            offsetBase,
                            System.currentTimeMillis(),
                            recordBatchMagic,
                            input,
                            LogFormat.ARROW));
            offsetBase += input.size();
        }
        fileLogRecords.flush();
        return fileLogRecords;
    }

    private List<Object[]> doProjection(
            FileLogProjection projection,
            FileLogRecords fileLogRecords,
            RowType rowType,
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        projection.setCurrentProjection(1L, rowType, DEFAULT_COMPRESSION, projectedFields);
        RowType projectedType = rowType.project(projectedFields);
        LogRecords project =
                projection.project(
                        fileLogRecords.channel(), 0, fileLogRecords.sizeInBytes(), fetchMaxBytes);
        assertThat(project.sizeInBytes()).isLessThanOrEqualTo(fetchMaxBytes);
        List<Object[]> results = new ArrayList<>();
        long expectedOffset = 0L;
        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch batch : project.batches()) {
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isEqualTo(expectedOffset);
                        assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                        InternalRow row = record.getRow();
                        assertThat(row.getFieldCount()).isEqualTo(projectedFields.length);
                        Object[] objs = new Object[projectedFields.length];
                        for (int i = 0; i < projectedFields.length; i++) {
                            if (row.isNullAt(i)) {
                                objs[i] = null;
                                continue;
                            }
                            switch (projectedType.getTypeAt(i).getTypeRoot()) {
                                case INTEGER:
                                    objs[i] = row.getInt(i);
                                    break;
                                case STRING:
                                    objs[i] = row.getString(i).toString();
                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                            "Unsupported type: " + projectedType.getTypeAt(i));
                            }
                        }
                        results.add(objs);
                        expectedOffset++;
                    }
                }
            }
        }
        return results;
    }

    private static void assertEquals(List<Object[]> actual, List<Object[]> expected) {
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatch(byte recordBatchMagic) throws Exception {
        // Create test data with multiple batches
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2); // Use DATA2 which has 3 columns: a(int), b(string),
        // c(string)

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 2}); // Project columns a and c

        // Get the first batch
        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();

        // Perform projection
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        // Verify the projected bytes are not empty
        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected data by reading it back
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        // Verify projected row has correct number of fields
                        assertThat(row.getFieldCount()).isEqualTo(2);

                        // Verify the projected fields contain correct data
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.DATA2.get(recordCount)[0]); // column a
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[2]); // column c

                        recordCount++;
                    }
                    // Verify we got all records from the original batch
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchEmptyBatch(byte recordBatchMagic) throws Exception {
        // Create an empty batch (this would be a CDC batch with no records)
        FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "empty.tmp"));

        // Create an empty memory log records
        MemoryLogRecords emptyRecords =
                createRecordsWithoutBaseLogOffset(
                        TestData.DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        System.currentTimeMillis(),
                        recordBatchMagic,
                        new ArrayList<>(), // Empty data
                        LogFormat.ARROW);

        fileLogRecords.append(emptyRecords);
        fileLogRecords.flush();

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L, TestData.DATA1_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {0});

        // Get the batch (should be empty)
        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();

        // Perform projection on empty batch
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        // Should return empty bytes for empty batch
        assertThat(projectedBytes.getBytesLength()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchSingleColumn(byte recordBatchMagic) throws Exception {
        // Test projection to single column
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {1}); // Project only column b

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify single column projection
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(1);
                        assertThat(row.getString(0).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchAllColumns(byte recordBatchMagic) throws Exception {
        // Test projection to all columns (should be equivalent to no projection)
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 1, 2}); // Project all columns

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify all columns projection
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(3);
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.DATA2.get(recordCount)[0]); // column a
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b
                        assertThat(row.getString(2).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[2]); // column c

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @Test
    void testProjectRecordBatchNoProjectionSet() {
        FileLogProjection projection = new FileLogProjection();

        // Create a mock batch
        FileLogRecords fileLogRecords = null;
        try {
            fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
            FileLogInputStream.FileChannelLogRecordBatch batch =
                    fileLogRecords.batchIterator(0).next();

            // Should throw exception when no projection is set
            assertThatThrownBy(() -> projection.projectRecordBatch(batch))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("There is no projection registered yet.");
        } catch (Exception e) {
            // Expected for empty file
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchMultipleBatches(byte recordBatchMagic) throws Exception {
        // Test projection across multiple batches
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA1_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0}); // Project only column a

        // Test projection on first batch
        FileLogInputStream.FileChannelLogRecordBatch firstBatch =
                fileLogRecords.batchIterator(0).next();
        BytesView firstProjectedBytes = projection.projectRecordBatch(firstBatch);
        assertThat(firstProjectedBytes.getBytesLength()).isGreaterThan(0);

        // Test projection on second batch
        FileLogInputStream.FileChannelLogRecordBatch secondBatch =
                fileLogRecords
                        .batchIterator(firstBatch.position() + firstBatch.sizeInBytes())
                        .next();
        BytesView secondProjectedBytes = projection.projectRecordBatch(secondBatch);
        assertThat(secondProjectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify both projections work correctly
        LogRecords firstProjectedRecords = new BytesViewLogRecords(firstProjectedBytes);
        LogRecords secondProjectedRecords = new BytesViewLogRecords(secondProjectedBytes);
        RowType projectedType = TestData.DATA1_ROW_TYPE.project(new int[] {0});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            // Verify first batch
            int firstBatchCount = 0;
            for (LogRecordBatch projectedBatch : firstProjectedRecords.batches()) {
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();
                        assertThat(row.getInt(0)).isEqualTo(TestData.DATA1.get(firstBatchCount)[0]);
                        firstBatchCount++;
                    }
                }
            }
            assertThat(firstBatchCount).isEqualTo(TestData.DATA1.size());

            // Verify second batch
            int secondBatchCount = 0;
            for (LogRecordBatch projectedBatch : secondProjectedRecords.batches()) {
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.ANOTHER_DATA1.get(secondBatchCount)[0]);
                        secondBatchCount++;
                    }
                }
            }
            assertThat(secondBatchCount).isEqualTo(TestData.ANOTHER_DATA1.size());
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchStatisticsClearing(byte recordBatchMagic) throws Exception {
        // Test that statistics are properly cleared during projection for V2+ versions
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 1}); // Project columns a and b

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch has statistics cleared
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that statistics are not available in projected batch
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.DATA2.get(recordCount)[0]); // column a
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testProjectRecordBatchNoStatisticsClearing(byte recordBatchMagic) throws Exception {
        // Test that statistics clearing only happens for V2+ versions
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 1}); // Project columns a and b

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch for V0/V1 versions (no statistics to clear)
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // For V0/V1, statistics should be 0 (not supported)
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.DATA2.get(recordCount)[0]); // column a
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V2})
    void testProjectStatisticsClearing(byte recordBatchMagic) throws Exception {
        // Test that statistics are properly cleared during project() method for V2+ versions
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2); // Multiple batches

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 2}); // Project columns a and c

        // Use project() method instead of projectRecordBatch()
        BytesViewLogRecords projectedRecords =
                projection.project(
                        fileLogRecords.channel(),
                        0,
                        fileLogRecords.sizeInBytes(),
                        Integer.MAX_VALUE);

        assertThat(projectedRecords.sizeInBytes()).isGreaterThan(0);

        // Verify all projected batches have statistics cleared
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that statistics are not available in projected batch
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        // Verify projected columns contain correct data
                        assertThat(row.getInt(0)).isNotNull(); // column a should be int
                        assertThat(row.getString(1).toString())
                                .isNotNull(); // column c should be string
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchWithStatisticsFlag(byte recordBatchMagic) throws Exception {
        // Test projection when original batch has statistics flag set
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {1}); // Project only column b

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch has statistics cleared even if original had statistics
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that statistics are cleared in projected batch
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(1);
                        assertThat(row.getString(0).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchCrcIntegrityAfterStatisticsTruncation(byte recordBatchMagic)
            throws Exception {
        // Test that projection with statistics clearing maintains CRC integrity
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 1}); // Project columns a and b

        FileLogInputStream.FileChannelLogRecordBatch batch = fileLogRecords.batchIterator(0).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch maintains CRC integrity after statistics truncation
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that the projected batch is valid (CRC check passes)
                assertThat(projectedBatch.isValid()).isTrue();

                // Verify that statistics are cleared in projected batch
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct and can be read
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        assertThat(row.getInt(0))
                                .isEqualTo(TestData.DATA2.get(recordCount)[0]); // column a
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[1]); // column b

                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(TestData.DATA2.size());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V2})
    void testProjectCrcIntegrityAfterStatisticsTruncation(byte recordBatchMagic) throws Exception {
        // Test that project() method with statistics clearing maintains CRC integrity
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2); // Multiple batches

        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L,
                TestData.DATA2_ROW_TYPE,
                DEFAULT_COMPRESSION,
                new int[] {0, 2}); // Project columns a and c

        // Use project() method which should clear statistics and recalculate CRC
        BytesViewLogRecords projectedRecords =
                projection.project(
                        fileLogRecords.channel(),
                        0,
                        fileLogRecords.sizeInBytes(),
                        Integer.MAX_VALUE);

        assertThat(projectedRecords.sizeInBytes()).isGreaterThan(0);

        // Verify all projected batches maintain CRC integrity
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that each projected batch is valid (CRC check passes)
                assertThat(projectedBatch.isValid()).isTrue();

                // Verify that statistics are cleared in projected batch
                assertThat(projectedBatch.statisticsSizeInBytes()).isEqualTo(0);

                // Verify the projected data is correct and can be read
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        // Verify projected columns contain correct data
                        assertThat(row.getInt(0)).isNotNull(); // column a should be int
                        assertThat(row.getString(1).toString())
                                .isNotNull(); // column c should be string
                    }
                }
            }
        }
    }
}
