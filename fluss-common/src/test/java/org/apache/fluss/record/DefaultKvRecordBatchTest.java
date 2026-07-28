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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultKvRecordBatch}. */
class DefaultKvRecordBatchTest extends KvTestBase {

    private static final byte[] EXPECTED_V0_FIXTURE =
            new byte[] {
                (byte) 0x18,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x13,
                (byte) 0xe7,
                (byte) 0x9d,
                (byte) 0xcf,
                (byte) 0x01,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x21,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0xff,
                (byte) 0xff,
                (byte) 0xff,
                (byte) 0x7f,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0x00
            };

    @Test
    void testV0HeaderAndAccessorsRemainByteCompatible() throws Exception {
        byte[] bytes = buildV0BatchBytes(33L, Integer.MAX_VALUE);
        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes));

        assertThat(batch.getClass()).isEqualTo(DefaultKvRecordBatch.class);
        assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V0);
        assertThat(batch.idempotenceProtocolVersion()).isZero();
        assertThat(batch.writerId()).isEqualTo(33L);
        assertThat(batch.batchSequence()).isEqualTo(Integer.MAX_VALUE);
        assertThat(DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE).isEqualTo(28);
        assertThat(bytes).isEqualTo(EXPECTED_V0_FIXTURE);
        assertThatThrownBy(batch::writerKey).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(batch::writerProgress).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writeAndReadBatch() throws Exception {
        int recordNumber = 100;
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        List<byte[]> keys = new ArrayList<>();
        List<CompactedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = new byte[] {(byte) i, (byte) i};
            CompactedRow row =
                    i % 2 == 1 ? null : TestInternalRowGenerator.genCompactedRowForAllType();
            builder.append(key, row);
            keys.add(key);
            rows.add(row);
        }

        KvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(builder.build());
        kvRecords.ensureValid();

        // verify the header info
        assertThat(kvRecords.getRecordCount()).isEqualTo(recordNumber);
        assertThat(kvRecords.magic()).isEqualTo(magic);
        assertThat(kvRecords.isValid()).isTrue();
        assertThat(kvRecords.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        for (KvRecord record :
                kvRecords.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA)))) {
            assertThat(keyToBytes(record)).isEqualTo(keys.get(i));
            assertThat(record.getRow()).isEqualTo(rows.get(i));
            i++;
        }

        builder.close();
    }

    private static byte[] buildV0BatchBytes(long writerId, int batchSequence) throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);
        builder.setWriterState(writerId, batchSequence);
        ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        builder.close();
        return bytes;
    }
}
