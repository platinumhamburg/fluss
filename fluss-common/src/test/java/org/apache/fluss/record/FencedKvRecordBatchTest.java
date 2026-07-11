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
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FencedKvRecordBatch}. */
class FencedKvRecordBatchTest {

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, Integer.MAX_VALUE, 2147483648L, Long.MAX_VALUE})
    void testV1WriterKeyAndSequenceRoundTrip(long sequence) throws Exception {
        WriterKey writerKey = new WriterKey(33L, Long.MIN_VALUE | 7L);
        KvRecordBatch batch =
                KvRecordBatchReader.pointToByteBuffer(
                        ByteBuffer.wrap(buildV1Batch(writerKey, sequence)));

        assertThat(batch).isInstanceOf(FencedKvRecordBatch.class);
        assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V1);
        assertThat(batch.idempotenceProtocolVersion()).isEqualTo(1);
        assertThat(batch.fencedWriterKey()).isEqualTo(writerKey);
        assertThat(batch.fencedSequence()).isEqualTo(sequence);
        assertThat(FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE).isEqualTo(40);
        batch.ensureValid();
    }

    @Test
    void testV1HeaderUsesFencedLayout() throws Exception {
        WriterKey writerKey = new WriterKey(33L, Long.MIN_VALUE | 7L);
        byte[] bytes = buildV1Batch(writerKey, Long.MAX_VALUE);
        ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        assertThat(bytes).hasSize(40);
        assertThat(header.getInt(0)).isEqualTo(36);
        assertThat(header.get(4)).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V1);
        assertThat(header.getShort(9)).isEqualTo((short) 1);
        assertThat(header.get(11)).isZero();
        assertThat(header.getLong(12)).isEqualTo(writerKey.high());
        assertThat(header.getLong(20)).isEqualTo(writerKey.low());
        assertThat(header.getLong(28)).isEqualTo(Long.MAX_VALUE);
        assertThat(header.getInt(36)).isZero();
    }

    @Test
    void testV1CrcCoversFencedHeaderFields() throws Exception {
        byte[] bytes = buildV1Batch(new WriterKey(33L, Long.MIN_VALUE | 7L), 1L);
        bytes[12] ^= 1;

        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes));

        assertThat(batch.isValid()).isFalse();
    }

    @Test
    void testV1RejectsNegativeSequence() {
        assertThatThrownBy(
                        () ->
                                FencedKvRecordBatchBuilder.builder(
                                                1,
                                                1024,
                                                new UnmanagedPagedOutputView(100),
                                                KvFormat.COMPACTED)
                                        .setWriterState(new WriterKey(1L, 2L), -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testV1RequiresWriterStateBeforeBuild() {
        assertThatThrownBy(
                        () ->
                                FencedKvRecordBatchBuilder.builder(
                                                1,
                                                1024,
                                                new UnmanagedPagedOutputView(100),
                                                KvFormat.COMPACTED)
                                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReaderRejectsUnknownMagicBeforeHeaderAccess() {
        byte[] bytes = minimumBatchWithMagic((byte) 2);

        assertThatThrownBy(() -> KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes)))
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("Unsupported KV batch magic 2");
    }

    private static byte[] buildV1Batch(WriterKey writerKey, long sequence) throws Exception {
        FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);
        builder.setWriterState(writerKey, sequence);
        ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        builder.close();
        return bytes;
    }

    private static byte[] minimumBatchWithMagic(byte magic) {
        return ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN).putInt(1).put(magic).array();
    }
}
