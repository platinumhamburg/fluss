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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.testutils.DataTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MemorySegmentLogInputStream}. */
public class MemorySegmentLogInputStreamTest {

    @Test
    void testNextBatch() throws Exception {
        // gen normal batch.
        MemoryLogRecords memoryLogRecords = DataTestUtils.genMemoryLogRecordsByObject(DATA1);
        Iterator<LogRecordBatch> iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isTrue();

        // gen empty batch.
        memoryLogRecords = MemoryLogRecords.EMPTY;
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with invalid header size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[LOG_OVERHEAD - 1]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with invalid header size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[11]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with not enough size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[LOG_OVERHEAD]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with enough size.
        MemorySegment memory = MemorySegment.allocateHeapMemory(100);
        memory.put(MAGIC_OFFSET, CURRENT_LOG_MAGIC_VALUE);
        memory.putInt(
                LENGTH_OFFSET, V0_RECORD_BATCH_HEADER_SIZE - LogRecordBatchFormat.LOG_OVERHEAD);
        memoryLogRecords = MemoryLogRecords.pointToBytes(memory.getHeapMemory());
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isTrue();
    }

    @Test
    void testRejectsMalformedBatchHeadersBeforeReturningBatch() {
        assertCorruptBatch(
                rawBatch(LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC, 0, (byte) 99),
                "Unsupported log magic");
        assertCorruptBatch(
                rawBatch(V0_RECORD_BATCH_HEADER_SIZE, 0, (byte) 99), "Unsupported log magic");
    }

    @ParameterizedTest
    @ValueSource(
            bytes = {
                LOG_MAGIC_VALUE_V0,
                LOG_MAGIC_VALUE_V1,
                LOG_MAGIC_VALUE_V2,
                LOG_MAGIC_VALUE_V3
            })
    void testIncompletePhysicalTailsReturnNoBatch(byte magic) {
        int headerSize = LogRecordBatchFormat.recordBatchHeaderSize(magic);
        int validDeclaredLength = headerSize - LOG_OVERHEAD;
        for (int physicalSize = LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
                physicalSize < headerSize;
                physicalSize++) {
            assertThat(getIterator(rawBatch(physicalSize, validDeclaredLength, magic)).hasNext())
                    .isFalse();
        }
        assertThat(getIterator(rawBatch(headerSize, validDeclaredLength + 8, magic)).hasNext())
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(
            bytes = {
                LOG_MAGIC_VALUE_V0,
                LOG_MAGIC_VALUE_V1,
                LOG_MAGIC_VALUE_V2,
                LOG_MAGIC_VALUE_V3
            })
    void testInvalidDeclarationsAreCorruptEvenWithOnlyCommonPrefix(byte magic) {
        int headerSize = LogRecordBatchFormat.recordBatchHeaderSize(magic);
        assertCorruptBatch(
                rawBatch(LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC, -1, magic), "negative");
        assertCorruptBatch(
                rawBatch(
                        LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC,
                        Integer.MAX_VALUE,
                        magic),
                "overflow");
        assertCorruptBatch(
                rawBatch(
                        LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC,
                        headerSize - LOG_OVERHEAD - 1,
                        magic),
                "smaller");
    }

    private static MemoryLogRecords rawBatch(int physicalSize, int declaredLength, byte magic) {
        ByteBuffer buffer = ByteBuffer.allocate(physicalSize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(LENGTH_OFFSET, declaredLength);
        buffer.put(MAGIC_OFFSET, magic);
        return MemoryLogRecords.pointToBytes(buffer.array());
    }

    private static void assertCorruptBatch(MemoryLogRecords records, String message) {
        Iterator<LogRecordBatch> iterator = getIterator(records);
        assertThatThrownBy(iterator::hasNext)
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining(message);
    }

    private static Iterator<LogRecordBatch> getIterator(MemoryLogRecords memoryLogRecords) {
        return memoryLogRecords.batches().iterator();
    }
}
