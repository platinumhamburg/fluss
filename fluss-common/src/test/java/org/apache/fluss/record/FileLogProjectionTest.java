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

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V3_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.record.LogRecordReadContext.createArrowReadContext;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FileLogProjection}. */
class FileLogProjectionTest {

    private @TempDir File tempDir;
    private TestingSchemaGetter testingSchemaGetter;

    @BeforeEach
    void beforeEach() {
        testingSchemaGetter = new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA, 1));
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA2_SCHEMA, 2));
    }

    @Test
    void testSetCurrentProjection() throws Exception {
        short schemaId = (short) 2;
        FileLogRecords recordsOfData2RowType =
                createFileLogRecords(
                        schemaId,
                        LOG_MAGIC_VALUE_V1,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2);

        ProjectionPushdownCache cache = new ProjectionPushdownCache();
        FileLogProjection projection = new FileLogProjection(cache);
        // get schema during running.
        doProjection(
                1L,
                schemaId,
                projection,
                recordsOfData2RowType,
                new int[] {0, 2},
                recordsOfData2RowType.sizeInBytes());
        assertThat(cache.projectionCache.size()).isEqualTo(1);
        FileLogProjection.ProjectionInfo info1 =
                cache.getProjectionInfo(1L, schemaId, new int[] {0, 2});
        assertThat(info1).isNotNull();
        assertThat(info1.nodesProjection.stream().toArray()).isEqualTo(new int[] {0, 2});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info1.buffersProjection.stream().toArray()).isEqualTo(new int[] {0, 1, 5, 6, 7});

        doProjection(
                2L,
                schemaId,
                projection,
                recordsOfData2RowType,
                new int[] {1},
                recordsOfData2RowType.sizeInBytes());
        assertThat(cache.projectionCache.size()).isEqualTo(2);
        FileLogProjection.ProjectionInfo info2 =
                cache.getProjectionInfo(2L, schemaId, new int[] {1});
        assertThat(info2).isNotNull();
        assertThat(info2.nodesProjection.stream().toArray()).isEqualTo(new int[] {1});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info2.buffersProjection.stream().toArray()).isEqualTo(new int[] {2, 3, 4});
    }

    @Test
    void testIllegalSetCurrentProjection() throws Exception {
        long tableId = 1L;
        short schemaId = (short) 2;
        FileLogRecords recordsOfData2RowType =
                createFileLogRecords(
                        schemaId,
                        LOG_MAGIC_VALUE_V1,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2);
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {3},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("Projected fields [3] is out of bound for schema with 3 fields.");

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {1, 0},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The projection indexes should be in field order, but is [1, 0]");

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {0, 0, 0},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage(
                        "The projection indexes should not contain duplicated fields, but is [0, 0, 0]");
    }

    @Test
    void testProjectionOldDataWithNewSchema() throws Exception {
        // Currently, we only support add column at last.
        short schemaId = 1;
        try (FileLogRecords records =
                createFileLogRecords(
                        schemaId, LOG_MAGIC_VALUE_V1, TestData.DATA1_ROW_TYPE, TestData.DATA1)) {

            ProjectionPushdownCache cache = new ProjectionPushdownCache();
            FileLogProjection projection = new FileLogProjection(cache);
            assertThat(
                            doProjection(
                                    2L,
                                    2,
                                    projection,
                                    records,
                                    new int[] {1},
                                    records.sizeInBytes()))
                    .containsExactly(
                            new Object[] {"a"},
                            new Object[] {"b"},
                            new Object[] {"c"},
                            new Object[] {"d"},
                            new Object[] {"e"},
                            new Object[] {"f"},
                            new Object[] {"g"},
                            new Object[] {"h"},
                            new Object[] {"i"},
                            new Object[] {"j"});

            assertThatThrownBy(
                            () ->
                                    doProjection(
                                            1L,
                                            2,
                                            projection,
                                            records,
                                            new int[] {0, 2},
                                            records.sizeInBytes()))
                    .isInstanceOf(InvalidColumnProjectionException.class)
                    .hasMessage(
                            "Projected fields [0, 2] is out of bound for schema with 2 fields.");
        }
    }

    static Stream<Arguments> projectedFieldsArgs() {
        return Stream.of(
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V0, (short) 1),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V0, (short) 1),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V0, (short) 1),
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V1, (short) 1),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V1, (short) 1),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V1, (short) 1),
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V0, (short) 2),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V0, (short) 2),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V0, (short) 2),
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V1, (short) 2),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V1, (short) 2),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V1, (short) 2));
    }

    @ParameterizedTest
    @MethodSource("projectedFieldsArgs")
    void testProject(int[] projectedFields, byte recordBatchMagic, short schemaId)
            throws Exception {
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA1_SCHEMA, schemaId));
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaId,
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        List<Object[]> results =
                doProjection(
                        1L,
                        schemaId,
                        new FileLogProjection(new ProjectionPushdownCache()),
                        fileLogRecords,
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
        final FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        // overwrite the wrong decoding byte order endian
        projection.getLogHeaderBuffer().order(ByteOrder.BIG_ENDIAN);
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        projection,
                                        fileLogRecords,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("negative declared length");

        // The BIG_ENDIAN of schema will be read as 256 wronly. In this case, we use 256 to skip it.
        short schemaId = 256;
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA2_SCHEMA, schemaId));
        final FileLogRecords fileLogRecords2 =
                createFileLogRecords(
                        schemaId,
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        1L,
                                        schemaId,
                                        projection,
                                        fileLogRecords2,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("negative declared length");
        fileLogRecords2.close();
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
                            new FileLogProjection(new ProjectionPushdownCache()),
                            fileLogRecords,
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

    @Test
    void testReadLogHeaderFullyOrFail() throws Exception {
        ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V1_RECORD_BATCH_HEADER_SIZE);

        // only V1 log header, should read fully
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V1, V1_RECORD_BATCH_HEADER_SIZE)) {
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // V1 log header with data, should read fully
        try (FileLogRecords fileLogRecords = createFileWithLogHeader(LOG_MAGIC_VALUE_V1, 100)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // only v0 log header, should only read 48 bytes
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V0, V0_RECORD_BATCH_HEADER_SIZE)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isTrue();
            assertThat(logHeaderBuffer.position()).isEqualTo(V0_RECORD_BATCH_HEADER_SIZE);
        }

        // v0 log header with data, should read fully
        try (FileLogRecords fileLogRecords = createFileWithLogHeader(LOG_MAGIC_VALUE_V0, 100)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // v1 log header incomplete, should throw exception
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V1, V0_RECORD_BATCH_HEADER_SIZE)) {
            logHeaderBuffer.rewind();
            assertThatThrownBy(
                            () ->
                                    FileLogProjection.readLogHeaderFullyOrFail(
                                            fileLogRecords.channel(), logHeaderBuffer, 0),
                            "Should throw exception if the log header is incomplete")
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining(
                            "Expected to read 52 bytes, but reached end of file after reading 48 bytes.");
        }

        // v0 log header incomplete, should throw exception
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V0, V0_RECORD_BATCH_HEADER_SIZE - 1)) {
            logHeaderBuffer.rewind();
            assertThatThrownBy(
                            () ->
                                    FileLogProjection.readLogHeaderFullyOrFail(
                                            fileLogRecords.channel(), logHeaderBuffer, 0),
                            "Should throw exception if the log header is incomplete")
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining(
                            "Expected to read 48 bytes, but reached end of file after reading 47 bytes.");
        }

        ByteBuffer v3HeaderBuffer = ByteBuffer.allocate(V3_RECORD_BATCH_HEADER_SIZE);
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(
                        LOG_MAGIC_VALUE_V2, LogRecordBatchFormat.V2_RECORD_BATCH_HEADER_SIZE)) {
            FileLogProjection.readLogHeaderFullyOrFail(fileLogRecords.channel(), v3HeaderBuffer, 0);
            assertThat(v3HeaderBuffer.position())
                    .isEqualTo(LogRecordBatchFormat.V2_RECORD_BATCH_HEADER_SIZE);
        }

        v3HeaderBuffer.rewind();
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V3, V3_RECORD_BATCH_HEADER_SIZE)) {
            FileLogProjection.readLogHeaderFullyOrFail(fileLogRecords.channel(), v3HeaderBuffer, 0);
            assertThat(v3HeaderBuffer.hasRemaining()).isFalse();
        }

        v3HeaderBuffer.rewind();
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V3, V3_RECORD_BATCH_HEADER_SIZE - 1)) {
            assertThatThrownBy(
                            () ->
                                    FileLogProjection.readLogHeaderFullyOrFail(
                                            fileLogRecords.channel(), v3HeaderBuffer, 0))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("Expected to read 68 bytes")
                    .hasMessageContaining("after reading 67 bytes");
        }
    }

    @Test
    void testV3WriterKeyAndLongSequenceSurviveProjection() throws Exception {
        WriterKey writerKey = new WriterKey(17L, Long.MIN_VALUE | 3L);
        long sequence = (long) Integer.MAX_VALUE + 17L;
        MemoryLogRecords records;
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L,
                            DEFAULT_SCHEMA_ID,
                            1024,
                            TestData.DATA1_ROW_TYPE,
                            DEFAULT_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.fencedBuilder(
                            DEFAULT_SCHEMA_ID,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(1024)),
                            false,
                            null);
            builder.append(ChangeType.APPEND_ONLY, row(new Object[] {1, "a"}));
            builder.setFencedWriterState(writerKey, sequence);
            builder.close();
            records = MemoryLogRecords.pointToBytesView(builder.build());
        }

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "v3-projection.log"))) {
            fileLogRecords.append(records);
            fileLogRecords.flush();
            FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
            projection.setCurrentProjection(
                    1L, testingSchemaGetter, DEFAULT_COMPRESSION, new int[] {0});

            LogRecordBatch projectedBatch =
                    projection
                            .project(
                                    fileLogRecords.channel(),
                                    0,
                                    fileLogRecords.sizeInBytes(),
                                    Integer.MAX_VALUE)
                            .batches()
                            .iterator()
                            .next();
            assertThat(projectedBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
            assertThat(projectedBatch.idempotenceProtocolVersion()).isEqualTo(1);
            assertThat(projectedBatch.fencedWriterKey()).isEqualTo(writerKey);
            assertThat(projectedBatch.fencedSequence()).isEqualTo(sequence);
            assertThat(projectedBatch.isValid()).isTrue();
            projectedBatch.ensureValid();
        }
    }

    @Test
    void testProjectionRejectsV3DeclaredSizeSmallerThanFixedHeader() throws Exception {
        ByteBuffer corruptHeader =
                ByteBuffer.allocate(V3_RECORD_BATCH_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        corruptHeader.putInt(
                LENGTH_OFFSET, V3_RECORD_BATCH_HEADER_SIZE - LogRecordBatchFormat.LOG_OVERHEAD - 1);
        corruptHeader.put(MAGIC_OFFSET, LOG_MAGIC_VALUE_V3);
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "undersized-v3-projection.log"))) {
            fileLogRecords.channel().write(corruptHeader);
            fileLogRecords.flush();
            FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());

            assertThatThrownBy(
                            () ->
                                    projection.project(
                                            fileLogRecords.channel(),
                                            0,
                                            (int) fileLogRecords.channel().size(),
                                            Integer.MAX_VALUE))
                    .isInstanceOf(CorruptMessageException.class)
                    .hasMessageContaining("v3")
                    .hasMessageContaining("smaller");
        }
    }

    @Test
    void testProjectionRejectsMalformedLayoutBeforeReadingFollowingBatchBytes() throws Exception {
        assertMalformedV3LayoutRejected(
                V3_RECORD_BATCH_HEADER_SIZE, -1, 0, true, "statisticsLength");
        assertMalformedV3LayoutRejected(V3_RECORD_BATCH_HEADER_SIZE, 0, -1, true, "recordCount");
        assertMalformedV3LayoutRejected(V3_RECORD_BATCH_HEADER_SIZE, 1, 0, true, "header-only");
        assertMalformedV3LayoutRejected(V3_RECORD_BATCH_HEADER_SIZE, 0, 1, true, "header-only");
        assertMalformedV3LayoutRejected(80, Integer.MAX_VALUE, 0, true, "overflow");
        assertMalformedV3LayoutRejected(80, 0, Integer.MAX_VALUE, false, "overflow");
        assertMalformedV3LayoutRejected(70, 0, 0, true, "Arrow header");
    }

    @ParameterizedTest
    @ValueSource(
            bytes = {
                LOG_MAGIC_VALUE_V0,
                LOG_MAGIC_VALUE_V1,
                LOG_MAGIC_VALUE_V2,
                LOG_MAGIC_VALUE_V3
            })
    void testRangeProjectionPreservesOutputBeforeIncompleteTail(byte tailMagic) throws Exception {
        try (FileLogRecords records =
                createFileLogRecords(
                        LOG_MAGIC_VALUE_V0,
                        TestData.DATA1_ROW_TYPE,
                        java.util.Collections.singletonList(new Object[] {1, "complete"}))) {
            int completeEnd = records.sizeInBytes();
            byte[] expected = projectToBytes(records, completeEnd);
            int headerSize = LogRecordBatchFormat.recordBatchHeaderSize(tailMagic);
            int validDeclaredLength = headerSize - LogRecordBatchFormat.LOG_OVERHEAD;

            for (int physicalSize = LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
                    physicalSize < headerSize;
                    physicalSize++) {
                records.channel().truncate(completeEnd);
                records.channel()
                        .write(
                                rawBatchTail(physicalSize, validDeclaredLength, tailMagic),
                                completeEnd);
                assertThat(projectToBytes(records, (int) records.channel().size()))
                        .isEqualTo(expected);
            }

            records.channel().truncate(completeEnd);
            records.channel()
                    .write(
                            rawBatchTail(headerSize, validDeclaredLength + 8, tailMagic),
                            completeEnd);
            assertThat(projectToBytes(records, (int) records.channel().size())).isEqualTo(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(
            bytes = {
                LOG_MAGIC_VALUE_V0,
                LOG_MAGIC_VALUE_V1,
                LOG_MAGIC_VALUE_V2,
                LOG_MAGIC_VALUE_V3
            })
    void testRangeProjectionRejectsInvalidDeclarationFromCommonPrefix(byte magic) throws Exception {
        int headerSize = LogRecordBatchFormat.recordBatchHeaderSize(magic);
        assertProjectionDeclarationRejected(magic, -1, "negative");
        assertProjectionDeclarationRejected(magic, Integer.MAX_VALUE, "overflow");
        assertProjectionDeclarationRejected(
                magic, headerSize - LogRecordBatchFormat.LOG_OVERHEAD - 1, "smaller");
    }

    @Test
    void testRangeProjectionRejectsUnknownMagicFromCommonPrefix() throws Exception {
        assertProjectionDeclarationRejected((byte) 99, 0, "Unsupported log magic");
    }

    @Test
    void testProjectRecordBatchRetainsStrictFullBatchContract() throws Exception {
        ByteBuffer header =
                rawBatchTail(
                        V3_RECORD_BATCH_HEADER_SIZE,
                        V3_RECORD_BATCH_HEADER_SIZE - LogRecordBatchFormat.LOG_OVERHEAD,
                        LOG_MAGIC_VALUE_V3);
        try (FileLogRecords records =
                FileLogRecords.open(new File(tempDir, "strict-project-record-batch.log"))) {
            records.channel().write(header);
            FileLogInputStream.FileChannelLogRecordBatch batch =
                    new FileLogInputStream.FileChannelLogRecordBatch(
                            0L,
                            LOG_MAGIC_VALUE_V3,
                            records,
                            0,
                            V3_RECORD_BATCH_HEADER_SIZE - LogRecordBatchFormat.LOG_OVERHEAD);
            records.channel().truncate(V3_RECORD_BATCH_HEADER_SIZE - 1L);

            assertThatThrownBy(
                            () ->
                                    new FileLogProjection(new ProjectionPushdownCache())
                                            .projectRecordBatch(batch))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("Expected to read 68 bytes");

            int declaredLength =
                    V3_RECORD_BATCH_HEADER_SIZE - LogRecordBatchFormat.LOG_OVERHEAD + 8;
            records.channel().truncate(0);
            records.channel()
                    .write(
                            rawBatchTail(
                                    V3_RECORD_BATCH_HEADER_SIZE,
                                    declaredLength,
                                    LOG_MAGIC_VALUE_V3));
            FileLogInputStream.FileChannelLogRecordBatch payloadTruncatedBatch =
                    new FileLogInputStream.FileChannelLogRecordBatch(
                            0L, LOG_MAGIC_VALUE_V3, records, 0, declaredLength);

            assertThatThrownBy(
                            () ->
                                    new FileLogProjection(new ProjectionPushdownCache())
                                            .projectRecordBatch(payloadTruncatedBatch))
                    .isInstanceOf(CorruptMessageException.class)
                    .hasMessageContaining("exceeds physical end");
        }
    }

    @Test
    void testProjectionRejectsOverlappingArrowBufferSpans() throws Exception {
        List<ArrowFieldNode> nodes = List.of(new ArrowFieldNode(1, 0), new ArrowFieldNode(1, 0));
        List<ArrowBuffer> buffers =
                List.of(
                        new ArrowBuffer(0, 16),
                        new ArrowBuffer(8, 8),
                        new ArrowBuffer(16, 8),
                        new ArrowBuffer(24, 8),
                        new ArrowBuffer(32, 8));
        assertMalformedArrowMetadataRejected(nodes, buffers, "overlap");
    }

    @Test
    void testProjectionRejectsMissingRequiredArrowNodesAndBuffers() throws Exception {
        List<ArrowBuffer> completeBuffers =
                List.of(
                        new ArrowBuffer(0, 8),
                        new ArrowBuffer(8, 8),
                        new ArrowBuffer(16, 8),
                        new ArrowBuffer(24, 8),
                        new ArrowBuffer(32, 8));
        assertMalformedArrowMetadataRejected(
                List.of(new ArrowFieldNode(1, 0)), completeBuffers, "node count");
        assertMalformedArrowMetadataRejected(
                List.of(new ArrowFieldNode(1, 0), new ArrowFieldNode(1, 0)),
                completeBuffers.subList(0, 4),
                "buffer count");
    }

    private byte[] projectToBytes(FileLogRecords records, int end) throws Exception {
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L, testingSchemaGetter, DEFAULT_COMPRESSION, new int[] {0});
        BytesView projected =
                projection.project(records.channel(), 0, end, Integer.MAX_VALUE).getBytesView();
        ByteBuf buffer = projected.getByteBuf();
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return bytes;
    }

    private void assertProjectionDeclarationRejected(byte magic, int declaredLength, String message)
            throws Exception {
        try (FileLogRecords records =
                FileLogRecords.open(new File(tempDir, UUID.randomUUID() + ".log"))) {
            records.channel()
                    .write(
                            rawBatchTail(
                                    LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC,
                                    declaredLength,
                                    magic));
            assertThatThrownBy(
                            () ->
                                    new FileLogProjection(new ProjectionPushdownCache())
                                            .project(
                                                    records.channel(),
                                                    0,
                                                    (int) records.channel().size(),
                                                    Integer.MAX_VALUE))
                    .isInstanceOf(CorruptMessageException.class)
                    .hasMessageContaining(message);
        }
    }

    private void assertMalformedArrowMetadataRejected(
            List<ArrowFieldNode> nodes, List<ArrowBuffer> buffers, String message)
            throws Exception {
        try (FileLogRecords records = createFileWithArrowMetadata(nodes, buffers, 40)) {
            FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
            projection.setCurrentProjection(
                    1L, testingSchemaGetter, DEFAULT_COMPRESSION, new int[] {0});
            assertThatThrownBy(
                            () ->
                                    projection.project(
                                            records.channel(),
                                            0,
                                            (int) records.channel().size(),
                                            Integer.MAX_VALUE))
                    .isInstanceOf(CorruptMessageException.class)
                    .hasMessageContaining(message);
        }
    }

    private FileLogRecords createFileWithArrowMetadata(
            List<ArrowFieldNode> nodes, List<ArrowBuffer> buffers, int bodyLength)
            throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ArrowUtils.serializeArrowRecordBatchMetadata(
                new WriteChannel(Channels.newChannel(output)),
                1L,
                nodes,
                buffers,
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                bodyLength);
        byte[] metadata = output.toByteArray();
        int batchSize = V0_RECORD_BATCH_HEADER_SIZE + metadata.length + bodyLength;
        ByteBuffer batch = ByteBuffer.allocate(batchSize).order(ByteOrder.LITTLE_ENDIAN);
        batch.putInt(LENGTH_OFFSET, batchSize - LogRecordBatchFormat.LOG_OVERHEAD);
        batch.put(MAGIC_OFFSET, LOG_MAGIC_VALUE_V0);
        batch.putShort(schemaIdOffset(LOG_MAGIC_VALUE_V0), (short) DEFAULT_SCHEMA_ID);
        batch.put(attributeOffset(LOG_MAGIC_VALUE_V0), DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK);
        batch.putInt(recordsCountOffset(LOG_MAGIC_VALUE_V0), 1);
        batch.position(V0_RECORD_BATCH_HEADER_SIZE);
        batch.put(metadata);
        batch.position(batchSize);
        batch.flip();

        FileLogRecords records = FileLogRecords.open(new File(tempDir, UUID.randomUUID() + ".log"));
        records.channel().write(batch);
        return records;
    }

    private static ByteBuffer rawBatchTail(int physicalSize, int declaredLength, byte magic) {
        ByteBuffer tail = ByteBuffer.allocate(physicalSize).order(ByteOrder.LITTLE_ENDIAN);
        tail.putInt(LENGTH_OFFSET, declaredLength);
        tail.put(MAGIC_OFFSET, magic);
        return tail;
    }

    private void assertMalformedV3LayoutRejected(
            int declaredSize,
            int statisticsLength,
            int recordCount,
            boolean appendOnly,
            String message)
            throws Exception {
        ByteBuffer bytesAfterHeader =
                ByteBuffer.allocate(declaredSize + 64).order(ByteOrder.LITTLE_ENDIAN);
        bytesAfterHeader.putInt(LENGTH_OFFSET, declaredSize - LogRecordBatchFormat.LOG_OVERHEAD);
        bytesAfterHeader.put(MAGIC_OFFSET, LOG_MAGIC_VALUE_V3);
        bytesAfterHeader.putShort(schemaIdOffset(LOG_MAGIC_VALUE_V3), (short) DEFAULT_SCHEMA_ID);
        bytesAfterHeader.put(
                attributeOffset(LOG_MAGIC_VALUE_V3),
                appendOnly ? DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK : 0);
        bytesAfterHeader.putInt(recordsCountOffset(LOG_MAGIC_VALUE_V3), recordCount);
        bytesAfterHeader.putInt(statisticsLengthOffset(LOG_MAGIC_VALUE_V3), statisticsLength);

        try (FileLogRecords records =
                FileLogRecords.open(new File(tempDir, UUID.randomUUID() + ".log"))) {
            records.channel().write(bytesAfterHeader);
            FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
            projection.setCurrentProjection(
                    1L, testingSchemaGetter, DEFAULT_COMPRESSION, new int[] {0});

            assertThatThrownBy(
                            () ->
                                    projection.project(
                                            records.channel(),
                                            0,
                                            (int) records.channel().size(),
                                            Integer.MAX_VALUE))
                    .isInstanceOf(CorruptMessageException.class)
                    .hasMessageContaining(message);
        }
    }

    private FileLogRecords createFileWithLogHeader(byte magic, int length) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(MAGIC_OFFSET);
        buffer.put(magic);
        buffer.position(length);
        buffer.flip();
        File file = new File(tempDir, UUID.randomUUID() + ".log");
        FileLogRecords fileLogRecords = FileLogRecords.open(file);
        fileLogRecords.channel().write(buffer);
        fileLogRecords.flush();
        return fileLogRecords;
    }

    @SafeVarargs
    final FileLogRecords createFileLogRecords(
            byte recordBatchMagic, RowType rowType, List<Object[]>... inputs) throws Exception {
        return createFileLogRecords(DEFAULT_SCHEMA_ID, recordBatchMagic, rowType, inputs);
    }

    @SafeVarargs
    final FileLogRecords createFileLogRecords(
            int schemaId, byte recordBatchMagic, RowType rowType, List<Object[]>... inputs)
            throws Exception {
        FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
        long offsetBase = 0L;
        for (List<Object[]> input : inputs) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            rowType,
                            schemaId,
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
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        return doProjection(
                1L, DEFAULT_SCHEMA_ID, projection, fileLogRecords, projectedFields, fetchMaxBytes);
    }

    private List<Object[]> doProjection(
            long tableId,
            int schemaId,
            FileLogProjection projection,
            FileLogRecords fileLogRecords,
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        projection.setCurrentProjection(
                tableId, testingSchemaGetter, DEFAULT_COMPRESSION, projectedFields);
        LogRecords project =
                projection.project(
                        fileLogRecords.channel(), 0, fileLogRecords.sizeInBytes(), fetchMaxBytes);
        assertThat(project.sizeInBytes()).isLessThanOrEqualTo(fetchMaxBytes);
        RowType rowType = testingSchemaGetter.getSchema(schemaId).getRowType();
        RowType projectedType = rowType.project(projectedFields);
        List<Object[]> results = new ArrayList<>();
        long expectedOffset = 0L;
        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, schemaId, testingSchemaGetter, true)) {
            for (LogRecordBatch batch : project.batches()) {
                assertThat(batch.isValid()).isTrue();
                batch.ensureValid();
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
        // Use schemaId=2 which corresponds to DATA2_SCHEMA (3 columns) in testingSchemaGetter
        int schemaIdForData2 = 2;

        // Create test data with multiple batches
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaIdForData2,
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2); // Use DATA2 which has 3 columns: a(int), b(string),
        // c(string)

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {0, 2}); // Project columns a and c

        // Get the first batch
        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();

        // Perform projection
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        // Verify the projected bytes are not empty
        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected data by reading it back
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, schemaIdForData2, testingSchemaGetter)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                assertThat(projectedBatch.isValid()).isTrue();
                projectedBatch.ensureValid();
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

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L, testingSchemaGetter, DEFAULT_COMPRESSION, new int[] {0});

        // Get the batch (should be empty)
        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();

        // Perform projection on empty batch
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        // Should return empty bytes for empty batch
        assertThat(projectedBytes.getBytesLength()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectRecordBatchSingleColumn(byte recordBatchMagic) throws Exception {
        // Use schemaId=2 which corresponds to DATA2_SCHEMA (3 columns) in testingSchemaGetter
        int schemaIdForData2 = 2;

        // Test projection to single column
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaIdForData2,
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2);

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {1}); // Project only column b

        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify single column projection
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, schemaIdForData2, testingSchemaGetter)) {
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
        // Use schemaId=2 which corresponds to DATA2_SCHEMA (3 columns) in testingSchemaGetter
        int schemaIdForData2 = 2;

        // Test projection to all columns (should be equivalent to no projection)
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaIdForData2,
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2);

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {0, 1, 2}); // Project all columns

        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify all columns projection
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1, 2});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, schemaIdForData2, testingSchemaGetter)) {
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
    void testProjectRecordBatchNoProjectionSet() throws Exception {
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());

        // Create a file with actual data so we can get a valid batch
        FileLogRecords fileLogRecords =
                createFileLogRecords(LOG_MAGIC_VALUE_V2, TestData.DATA2_ROW_TYPE, TestData.DATA2);
        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();

        // Should throw exception when no projection is set
        // getOrCreateProjectionInfo returns null because no projection has been registered,
        // causing a NullPointerException downstream. This is an implementation detail —
        // the key contract is that calling projectRecordBatch without setting a projection fails.
        assertThatThrownBy(() -> projection.projectRecordBatch(batch))
                .isInstanceOf(Exception.class);
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

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {0}); // Project only column a

        // Test projection on first batch
        FileLogInputStream.FileChannelLogRecordBatch firstBatch =
                fileLogRecords.batchIterator(0, -1).next();
        BytesView firstProjectedBytes = projection.projectRecordBatch(firstBatch);
        assertThat(firstProjectedBytes.getBytesLength()).isGreaterThan(0);

        // Test projection on second batch
        FileLogInputStream.FileChannelLogRecordBatch secondBatch =
                fileLogRecords
                        .batchIterator(firstBatch.position() + firstBatch.sizeInBytes(), -1)
                        .next();
        BytesView secondProjectedBytes = projection.projectRecordBatch(secondBatch);
        assertThat(secondProjectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify both projections work correctly
        LogRecords firstProjectedRecords = new BytesViewLogRecords(firstProjectedBytes);
        LogRecords secondProjectedRecords = new BytesViewLogRecords(secondProjectedBytes);
        RowType projectedType = TestData.DATA1_ROW_TYPE.project(new int[] {0});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID, testingSchemaGetter)) {
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

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {0, 1}); // Project columns a and b

        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch has statistics cleared
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID, testingSchemaGetter)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that statistics are not available in projected batch
                assertThat(projectedBatch.getStatistics(context)).isEmpty();

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
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0})
    void testProjectRecordBatchNoStatisticsClearing(byte recordBatchMagic) throws Exception {
        // Test that statistics clearing only happens for V1+ versions (V0 has no statistics)
        FileLogRecords fileLogRecords =
                createFileLogRecords(recordBatchMagic, TestData.DATA2_ROW_TYPE, TestData.DATA2);

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                new int[] {0, 1}); // Project columns a and b

        FileLogInputStream.FileChannelLogRecordBatch batch =
                fileLogRecords.batchIterator(0, -1).next();
        BytesView projectedBytes = projection.projectRecordBatch(batch);

        assertThat(projectedBytes.getBytesLength()).isGreaterThan(0);

        // Verify the projected batch for V0/V1 versions (no statistics to clear)
        LogRecords projectedRecords = new BytesViewLogRecords(projectedBytes);
        RowType projectedType = TestData.DATA2_ROW_TYPE.project(new int[] {0, 1});

        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID, testingSchemaGetter)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // For V0/V1, statistics should be 0 (not supported)
                assertThat(projectedBatch.getStatistics(context)).isEmpty();

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
        // Use schemaId=2 which corresponds to DATA2_SCHEMA (3 columns) in testingSchemaGetter
        int schemaIdForData2 = 2;

        // Test that statistics are properly cleared during project() method for V2+ versions
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaIdForData2,
                        recordBatchMagic,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2); // Multiple batches

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                testingSchemaGetter,
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
                createArrowReadContext(projectedType, schemaIdForData2, testingSchemaGetter)) {
            for (LogRecordBatch projectedBatch : projectedRecords.batches()) {
                // Verify that statistics are not available in projected batch
                assertThat(projectedBatch.getStatistics(context)).isEmpty();

                // Verify the projected data is correct
                try (CloseableIterator<LogRecord> records = projectedBatch.records(context)) {
                    int recordCount = 0;
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        InternalRow row = record.getRow();

                        assertThat(row.getFieldCount()).isEqualTo(2);
                        // Verify projected columns contain correct data (columns a and c)
                        assertThat(row.getInt(0)).isEqualTo(TestData.DATA2.get(recordCount)[0]);
                        assertThat(row.getString(1).toString())
                                .isEqualTo(TestData.DATA2.get(recordCount)[2]);
                        recordCount++;
                    }
                }
            }
        }
    }
}
