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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    private static final WriterKey PROGRESS_WRITER_KEY = new WriterKey(17L, Long.MIN_VALUE | 3L);
    private static final long WRITER_PROGRESS = (long) Integer.MAX_VALUE + 17L;

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testExistingWriterAccessorsRemainCompact(byte magic) throws Exception {
        LogRecordBatch batch = buildOrdinaryBatch(magic, 7L, Integer.MAX_VALUE);
        assertThat(batch.writerId()).isEqualTo(7L);
        assertThat(batch.batchSequence()).isEqualTo(Integer.MAX_VALUE);
        assertThat(batch.idempotenceProtocolVersion()).isZero();
        assertThatThrownBy(batch::writerKey).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(batch::writerProgress).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testV0AndV2HeadersRemainByteExact() throws Exception {
        assertThat(batchBytes(buildOrdinaryRecords(LOG_MAGIC_VALUE_V0, 7L, Integer.MAX_VALUE)))
                .isEqualTo(
                        parseHex(
                                "000000000000000024000000000000000000000000ec1d59fa010000000000000700000000000000ffffff7f00000000"));
        assertThat(batchBytes(buildOrdinaryRecords(LOG_MAGIC_VALUE_V2, 7L, Integer.MAX_VALUE)))
                .isEqualTo(
                        parseHex(
                                "00000000000000002c000000020000000000000000ffffffffadc5a64e010000000000000700000000000000ffffff7f0000000000000000"));
    }

    @Test
    void testV3WriterKeyAndLongProgressSurviveMemoryRoundTrip() throws Exception {
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.progressBuilder(
                        schemaId, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), false);
        builder.setWriterProgress(PROGRESS_WRITER_KEY, WRITER_PROGRESS);

        LogRecordBatch batch =
                MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
        assertThat(batch.idempotenceProtocolVersion()).isEqualTo(1);
        assertThat(batch.writerKey()).isEqualTo(PROGRESS_WRITER_KEY);
        assertThat(batch.writerProgress()).isEqualTo(WRITER_PROGRESS);
        assertThatThrownBy(batch::writerId).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(batch::batchSequence).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRowBuilderRewritesBuiltHeaderAfterProgressChanges() throws Exception {
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.progressBuilder(
                        schemaId, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), false);
        builder.setWriterProgress(PROGRESS_WRITER_KEY, WRITER_PROGRESS);
        LogRecordBatch first =
                MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
        long firstChecksum = first.checksum();

        WriterKey replacementKey = new WriterKey(-9L, 27L);
        long replacementProgress = Long.MAX_VALUE;
        builder.setWriterProgress(replacementKey, replacementProgress);
        LogRecordBatch rebuilt =
                MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();

        assertThat(rebuilt.writerKey()).isEqualTo(replacementKey);
        assertThat(rebuilt.writerProgress()).isEqualTo(replacementProgress);
        assertThat(rebuilt.checksum()).isNotEqualTo(firstChecksum);
        rebuilt.ensureValid();
    }

    @Test
    void testV3RequiresWriterKeyAndAcceptsLongMaxProgress() throws Exception {
        MemoryLogRecordsCompactedBuilder missingKeyBuilder =
                MemoryLogRecordsCompactedBuilder.progressBuilder(
                        schemaId, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), false);
        assertThatThrownBy(missingKeyBuilder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("writer key");

        MemoryLogRecordsCompactedBuilder maxProgressBuilder =
                MemoryLogRecordsCompactedBuilder.progressBuilder(
                        schemaId, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), false);
        maxProgressBuilder.setWriterProgress(PROGRESS_WRITER_KEY, Long.MAX_VALUE);
        LogRecordBatch batch =
                MemoryLogRecords.pointToBytesView(maxProgressBuilder.build())
                        .batches()
                        .iterator()
                        .next();
        assertThat(batch.writerProgress()).isEqualTo(Long.MAX_VALUE);
        assertThatThrownBy(() -> maxProgressBuilder.setWriterProgress(PROGRESS_WRITER_KEY, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testExistingBuilderFactoryCannotCreateV3() throws Exception {
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        schemaId, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), false);
        LogRecordBatch batch =
                MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
        assertThat(batch.magic()).isLessThan(LOG_MAGIC_VALUE_V3);
        assertThat(LogRecordBatch.CURRENT_LOG_MAGIC_VALUE).isEqualTo(LOG_MAGIC_VALUE_V0);
    }

    private LogRecordBatch buildOrdinaryBatch(byte magic, long writerId, int sequence)
            throws Exception {
        return buildOrdinaryRecords(magic, writerId, sequence).batches().iterator().next();
    }

    private MemoryLogRecords buildOrdinaryRecords(byte magic, long writerId, int sequence)
            throws Exception {
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        builder.setWriterState(writerId, sequence);
        return MemoryLogRecords.pointToBytesView(builder.build());
    }

    private static byte[] batchBytes(MemoryLogRecords records) {
        byte[] bytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), bytes, 0, bytes.length);
        return bytes;
    }

    private static byte[] parseHex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testRecordBatchSize(byte magic) throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(magic, TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testIndexedRowWriteAndReadBatch(byte magic) throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(ChangeType.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(allRowType).build(), schemaId));
        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                allRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }

        builder.close();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testNoRecordAppend(byte magic) throws Exception {
        // 1. no record append with baseOffset as 0.
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(baseRowType).build(), schemaId));
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }

        // 2. no record append with baseOffset as 100.
        builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        100L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));
        memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(100);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(101);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(100);

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }
}
