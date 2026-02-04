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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordReadContext.createIndexedReadContext;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemoryLogRecordsIndexedBuilder} with preWrittenBytesView and stateChangeLogs. */
class MemoryLogRecordsIndexedBuilderTest {

    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter(
                    DEFAULT_SCHEMA_ID,
                    Schema.newBuilder()
                            .fromRowType(TestInternalRowGenerator.createAllRowType())
                            .build());

    @Test
    void testPreWrittenBytesViewWithStateChangeLogs() throws Exception {
        // Step 1: Create some test data (indexed rows)
        int recordCount = 5;
        List<IndexedRow> testRows = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            testRows.add(TestInternalRowGenerator.genIndexedRowForAllType());
        }

        // Step 2: Write rows to get preWrittenBytesView
        // We need to write the rows data first (without header) to simulate IndexCache scenario
        // For each row, we need to serialize it and collect the bytes
        // In real scenario, IndexCache already has the serialized row data
        // Here we simulate by writing rows to a temporary builder and extracting the data
        UnmanagedPagedOutputView tempOutputView = new UnmanagedPagedOutputView(1024);

        for (int i = 0; i < recordCount; i++) {
            IndexedLogRecord.writeTo(tempOutputView, ChangeType.APPEND_ONLY, testRows.get(i));
        }

        // Get the written bytes as preWrittenBytesView (excluding header)
        List<MemorySegmentBytesView> preWrittenBytesView = tempOutputView.getWrittenSegments();

        // Step 3: Create StateChangeLogs
        DefaultStateChangeLogsBuilder stateBuilder = new DefaultStateChangeLogsBuilder();
        TableBucket dataBucket1 = new TableBucket(DATA1_TABLE_ID, 0);
        TableBucket dataBucket2 = new TableBucket(DATA1_TABLE_ID, 1);

        // Simulate index application progress tracking
        stateBuilder.addLog(
                ChangeType.UPDATE_AFTER, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataBucket1, 100L);
        stateBuilder.addLog(
                ChangeType.UPDATE_AFTER, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, dataBucket2, 200L);

        StateChangeLogs stateChangeLogs = stateBuilder.build();

        // Step 4: Use preWrittenBytesView + stateChangeLogs to build MemoryLogRecords
        MemoryLogRecords memoryLogRecords;
        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        DEFAULT_SCHEMA_ID,
                        preWrittenBytesView,
                        recordCount,
                        true, // appendOnly
                        stateChangeLogs)) {

            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        // Step 5: Parse and verify the LogRecordBatch
        assertThat(memoryLogRecords).isNotNull();
        assertThat(memoryLogRecords.batches()).hasSize(1);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isNotNull();

        // Verify basic batch properties
        assertThat(batch.getRecordCount()).isEqualTo(recordCount);
        // When stateChangeLogs is provided, format should be V3
        assertThat(batch.magic()).isEqualTo(LogRecordBatchFormat.LOG_MAGIC_VALUE_V3);
        batch.ensureValid();

        // Step 6: Verify StateChangeLogs are correctly embedded and can be parsed
        Optional<StateChangeLogs> stateChangeLogsOpt = batch.stateChangeLogs();
        assertThat(stateChangeLogsOpt).isPresent();

        StateChangeLogs retrievedLogs = stateChangeLogsOpt.get();
        List<StateChangeLog> logs = new ArrayList<>();
        retrievedLogs.iters().forEachRemaining(logs::add);

        assertThat(logs).hasSize(2);

        // Verify first state change log
        assertThat(logs.get(0).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logs.get(0).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logs.get(0).getKey()).isEqualTo(dataBucket1);
        assertThat(logs.get(0).getValue()).isEqualTo(100L);

        // Verify second state change log
        assertThat(logs.get(1).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logs.get(1).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logs.get(1).getKey()).isEqualTo(dataBucket2);
        assertThat(logs.get(1).getValue()).isEqualTo(200L);

        // Step 7: Verify the records themselves can still be read correctly
        LogRecordReadContext readContext =
                createIndexedReadContext(
                        TestInternalRowGenerator.createAllRowType(),
                        DEFAULT_SCHEMA_ID,
                        SCHEMA_GETTER);
        try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
            int count = 0;
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record).isNotNull();
                count++;
            }
            assertThat(count).isEqualTo(recordCount);
        }
    }

    @Test
    void testPreWrittenBytesViewWithoutStateChangeLogs() throws Exception {
        // Test backward compatibility: preWrittenBytesView without stateChangeLogs should still
        // work
        int recordCount = 3;
        List<IndexedRow> testRows = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            testRows.add(TestInternalRowGenerator.genIndexedRowForAllType());
        }

        // Write rows to get preWrittenBytesView
        UnmanagedPagedOutputView tempOutputView = new UnmanagedPagedOutputView(1024);

        for (int i = 0; i < recordCount; i++) {
            IndexedLogRecord.writeTo(tempOutputView, ChangeType.APPEND_ONLY, testRows.get(i));
        }

        List<MemorySegmentBytesView> preWrittenBytesView = tempOutputView.getWrittenSegments();

        // Build without stateChangeLogs
        MemoryLogRecords memoryLogRecords;
        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        DEFAULT_SCHEMA_ID, preWrittenBytesView, recordCount, true)) {
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        // Verify
        assertThat(memoryLogRecords).isNotNull();
        assertThat(memoryLogRecords.batches()).hasSize(1);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch.getRecordCount()).isEqualTo(recordCount);
        batch.ensureValid();

        // Should have no stateChangeLogs
        assertThat(batch.stateChangeLogs()).isEmpty();

        // Records should be readable
        LogRecordReadContext readContext =
                createIndexedReadContext(
                        TestInternalRowGenerator.createAllRowType(),
                        DEFAULT_SCHEMA_ID,
                        SCHEMA_GETTER);
        try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertThat(count).isEqualTo(recordCount);
        }
    }
}
