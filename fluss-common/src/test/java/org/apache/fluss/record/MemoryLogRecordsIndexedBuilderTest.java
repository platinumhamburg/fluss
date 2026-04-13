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
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.LogRecordReadContext.createIndexedReadContext;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemoryLogRecordsIndexedBuilder} with preWrittenBytesView. */
class MemoryLogRecordsIndexedBuilderTest {

    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter(
                    DEFAULT_SCHEMA_ID,
                    Schema.newBuilder()
                            .fromRowType(TestInternalRowGenerator.createAllRowType())
                            .build());

    @Test
    void testPreWrittenBytesView() throws Exception {
        int recordCount = 5;
        List<IndexedRow> testRows = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            testRows.add(TestInternalRowGenerator.genIndexedRowForAllType());
        }

        UnmanagedPagedOutputView tempOutputView = new UnmanagedPagedOutputView(1024);
        for (int i = 0; i < recordCount; i++) {
            IndexedLogRecord.writeTo(tempOutputView, ChangeType.APPEND_ONLY, testRows.get(i));
        }

        List<MemorySegmentBytesView> preWrittenBytesView = tempOutputView.getWrittenSegments();

        MemoryLogRecords memoryLogRecords;
        try (MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        DEFAULT_SCHEMA_ID, preWrittenBytesView, recordCount, true)) {
            memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        assertThat(memoryLogRecords).isNotNull();
        assertThat(memoryLogRecords.batches()).hasSize(1);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isNotNull();
        assertThat(batch.getRecordCount()).isEqualTo(recordCount);
        batch.ensureValid();

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
}
