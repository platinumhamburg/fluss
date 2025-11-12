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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileLogInputStream}. */
public class FileLogInputStreamTest extends LogTestBase {
    private @TempDir File tempDir;

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testWriteTo(byte recordBatchMagic) throws Exception {
        try (FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(recordBatchMagic);

            LogRecordBatch recordBatch = batch.loadFullBatch();

            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, schemaId);
                    CloseableIterator<LogRecord> iterator = recordBatch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                LogRecord record = iterator.next();
                assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                assertThat(iterator.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testStateChangeLogsWithV3Format() throws Exception {
        // Create state change logs
        DefaultStateChangeLogsBuilder logsBuilder = new DefaultStateChangeLogsBuilder();
        org.apache.fluss.metadata.TableBucket key1 =
                new org.apache.fluss.metadata.TableBucket(1L, 0);
        org.apache.fluss.metadata.TableBucket key2 =
                new org.apache.fluss.metadata.TableBucket(1L, 1);
        org.apache.fluss.metadata.TableBucket key3 =
                new org.apache.fluss.metadata.TableBucket(1L, 2);
        logsBuilder.addLog(ChangeType.INSERT, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        logsBuilder.addLog(
                ChangeType.UPDATE_AFTER, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        logsBuilder.addLog(ChangeType.DELETE, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, key3, null);
        DefaultStateChangeLogs stateLogs = logsBuilder.build();

        // Create records with state change logs using V3 format
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test-v3.tmp"))) {
            MemoryLogRecords memoryRecords = createRecordsWithStateChangeLogs(stateLogs);
            fileLogRecords.append(memoryRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);

            // Test stateChangeLogs method - zero-copy read
            Optional<StateChangeLogs> stateChangeLogsOpt = batch.stateChangeLogs();
            assertThat(stateChangeLogsOpt).isPresent();

            StateChangeLogs stateChangeLogs = stateChangeLogsOpt.get();
            Iterator<StateChangeLog> iterator = stateChangeLogs.iters();

            List<StateChangeLog> logs = new ArrayList<>();
            iterator.forEachRemaining(logs::add);

            assertThat(logs).hasSize(3);

            // Verify first log
            assertThat(logs.get(0).getChangeType()).isEqualTo(ChangeType.INSERT);
            assertThat(logs.get(0).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
            assertThat(logs.get(0).getKey()).isEqualTo(key1);
            assertThat(logs.get(0).getValue()).isEqualTo(100L);

            // Verify second log
            assertThat(logs.get(1).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
            assertThat(logs.get(1).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
            assertThat(logs.get(1).getKey()).isEqualTo(key2);
            assertThat(logs.get(1).getValue()).isEqualTo(200L);

            // Verify third log
            assertThat(logs.get(2).getChangeType()).isEqualTo(ChangeType.DELETE);
            assertThat(logs.get(2).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
            assertThat(logs.get(2).getKey()).isEqualTo(key3);
            assertThat(logs.get(2).getValue()).isNull();

            // Verify zero-copy - multiple calls should work
            Optional<StateChangeLogs> stateChangeLogsOpt2 = batch.stateChangeLogs();
            assertThat(stateChangeLogsOpt2).isPresent();
            StateChangeLogs stateChangeLogs2 = stateChangeLogsOpt2.get();
            Iterator<StateChangeLog> iterator2 = stateChangeLogs2.iters();
            List<StateChangeLog> logs2 = new ArrayList<>();
            iterator2.forEachRemaining(logs2::add);
            assertThat(logs2).hasSize(3);
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testStateChangeLogsWithNonV3Format(byte recordBatchMagic) throws Exception {
        // Non-V3 formats should not have state change logs
        try (FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            Optional<StateChangeLogs> stateChangeLogsOpt = batch.stateChangeLogs();
            assertThat(stateChangeLogsOpt).isEmpty();
        }
    }

    @Test
    void testStateChangeLogsWithV3FormatButEmptyLogs() throws Exception {
        // V3 format but with no state change logs
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test-v3-empty.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            LOG_MAGIC_VALUE_V3,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            Optional<StateChangeLogs> stateChangeLogsOpt = batch.stateChangeLogs();
            assertThat(stateChangeLogsOpt).isEmpty();
        }
    }

    private MemoryLogRecords createRecordsWithStateChangeLogs(StateChangeLogs stateChangeLogs)
            throws Exception {
        org.apache.fluss.memory.UnmanagedPagedOutputView outputView =
                new org.apache.fluss.memory.UnmanagedPagedOutputView(1024 * 1024);
        org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator allocator =
                new org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator(
                        Integer.MAX_VALUE);
        org.apache.fluss.row.arrow.ArrowWriterPool pool =
                new org.apache.fluss.row.arrow.ArrowWriterPool(allocator);
        org.apache.fluss.row.arrow.ArrowWriter arrowWriter =
                pool.getOrCreateWriter(
                        1L,
                        schemaId,
                        Integer.MAX_VALUE,
                        baseRowType,
                        ArrowCompressionInfo.NO_COMPRESSION);

        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        0L, LOG_MAGIC_VALUE_V3, schemaId, arrowWriter, outputView, stateChangeLogs);

        InternalRow row = GenericRow.of(0, org.apache.fluss.row.BinaryString.fromString("abc"));
        builder.append(ChangeType.APPEND_ONLY, row);
        org.apache.fluss.record.bytesview.MultiBytesView bytesView = builder.build();
        builder.close();
        pool.close();
        allocator.close();
        return MemoryLogRecords.pointToByteBuffer(bytesView.getByteBuf().nioBuffer());
    }
}
