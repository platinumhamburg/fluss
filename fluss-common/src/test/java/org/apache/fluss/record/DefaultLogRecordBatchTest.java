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
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
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
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testIndexedRowWriteAndReadBatch(byte magic) throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        List<IndexedRow> rows = new ArrayList<>();
        MemoryLogRecords memoryLogRecords;

        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100))) {
            for (int i = 0; i < recordNumber; i++) {
                IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
                builder.append(ChangeType.INSERT, row);
                rows.add(row);
            }
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }
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
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testNoRecordAppend(byte magic) throws Exception {
        // 1. no record append with baseOffset as 0.
        MemoryLogRecords memoryLogRecords;
        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100))) {
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }
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
        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        100L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100))) {
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }
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

    @Test
    void testV3FormatWithStateChangeLogs() throws Exception {
        // Create state change logs
        DefaultStateChangeLogsBuilder logsBuilder = new DefaultStateChangeLogsBuilder();
        org.apache.fluss.metadata.TableBucket testKey1 =
                new org.apache.fluss.metadata.TableBucket(1L, 0);
        org.apache.fluss.metadata.TableBucket testKey2 =
                new org.apache.fluss.metadata.TableBucket(1L, 1);
        org.apache.fluss.metadata.TableBucket testKey3 =
                new org.apache.fluss.metadata.TableBucket(1L, 2);
        logsBuilder.addLog(
                ChangeType.INSERT, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, testKey1, 100L);
        logsBuilder.addLog(
                ChangeType.UPDATE_AFTER, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, testKey2, 200L);
        logsBuilder.addLog(
                ChangeType.DELETE, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, testKey3, null);
        DefaultStateChangeLogs stateLogs = logsBuilder.build();

        // Create batch with state change logs using V3 format
        int recordNumber = 10;
        List<IndexedRow> rows = new ArrayList<>();
        MemoryLogRecords memoryLogRecords;

        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        LOG_MAGIC_VALUE_V3,
                        new UnmanagedPagedOutputView(1024),
                        stateLogs)) {
            for (int i = 0; i < recordNumber; i++) {
                IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
                builder.append(ChangeType.INSERT, row);
                rows.add(row);
            }
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        // Verify batch properties
        assertThat(logRecordBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        logRecordBatch.ensureValid();

        // Verify state change logs are present
        Optional<StateChangeLogs> stateChangeLogsOpt = logRecordBatch.stateChangeLogs();
        assertThat(stateChangeLogsOpt).isPresent();

        StateChangeLogs retrievedLogs = stateChangeLogsOpt.get();
        List<StateChangeLog> logs = new ArrayList<>();
        retrievedLogs.iters().forEachRemaining(logs::add);

        assertThat(logs).hasSize(3);
        assertThat(logs.get(0).getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(logs.get(0).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logs.get(0).getKey()).isEqualTo(testKey1);
        assertThat(logs.get(0).getValue()).isEqualTo(100L);

        assertThat(logs.get(1).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logs.get(1).getKey()).isEqualTo(testKey2);
        assertThat(logs.get(1).getValue()).isEqualTo(200L);

        assertThat(logs.get(2).getChangeType()).isEqualTo(ChangeType.DELETE);
        assertThat(logs.get(2).getKey()).isEqualTo(testKey3);
        assertThat(logs.get(2).getValue()).isNull();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testNonV3FormatHasNoStateChangeLogs(byte magic) throws Exception {
        // Create batch with non-V3 format
        int recordNumber = 10;
        MemoryLogRecords memoryLogRecords;

        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100))) {
            for (int i = 0; i < recordNumber; i++) {
                IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
                builder.append(ChangeType.INSERT, row);
            }
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        // Verify no state change logs for non-V3 formats
        Optional<StateChangeLogs> stateChangeLogsOpt = logRecordBatch.stateChangeLogs();
        assertThat(stateChangeLogsOpt).isEmpty();
    }
}
