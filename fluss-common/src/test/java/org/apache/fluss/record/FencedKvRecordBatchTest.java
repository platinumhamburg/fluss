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
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.utils.crc.Crc32C;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
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

        assertReaderRejects(ByteBuffer.wrap(bytes), "Unsupported KV batch magic 2");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    void testReaderRejectsTruncatedCommonPrefixWithoutAdvancing(int prefixSize) {
        assertReaderRejects(ByteBuffer.allocate(prefixSize), "minimum length and magic prefix");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE})
    void testReaderRejectsInvalidDeclaredLengthWithoutAdvancing(int declaredLength) {
        assertReaderRejects(
                minimumBatchWithLengthAndMagic(declaredLength, KvRecordBatch.KV_MAGIC_VALUE_V1),
                "Invalid KV batch length " + declaredLength);
    }

    @Test
    void testReaderRejectsV1HeaderShorterThanFortyBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE - 1);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0, buffer.capacity() - Integer.BYTES);
        buffer.put(4, KvRecordBatch.KV_MAGIC_VALUE_V1);

        assertReaderRejects(buffer, "smaller than magic 1 header 40");
    }

    @Test
    void testReaderRejectsDeclaredBatchBeyondRemainingBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0, FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE);
        buffer.put(4, KvRecordBatch.KV_MAGIC_VALUE_V1);

        assertReaderRejects(buffer, "exceeds remaining bytes 40");
    }

    @Test
    void testReaderHonorsNonZeroPositionAndConstrainedLimit() throws Exception {
        byte[] batchBytes = buildV1Batch(new WriterKey(3L, 4L), 5L);
        byte[] framedBytes = new byte[batchBytes.length + 9];
        System.arraycopy(batchBytes, 0, framedBytes, 3, batchBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(framedBytes);
        buffer.position(3);
        buffer.limit(3 + batchBytes.length);

        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(buffer);

        assertThat(batch.fencedWriterKey()).isEqualTo(new WriterKey(3L, 4L));
        assertThat(batch.fencedSequence()).isEqualTo(5L);
        assertThat(buffer.position()).isEqualTo(3);
        assertThat(buffer.limit()).isEqualTo(3 + batchBytes.length);
    }

    @Test
    void testReaderDoesNotAdvanceReadOnlyHeapBufferWhenCopying() throws Exception {
        byte[] batchBytes = buildV1Batch(new WriterKey(6L, 7L), 8L);
        byte[] framedBytes = new byte[batchBytes.length + 5];
        System.arraycopy(batchBytes, 0, framedBytes, 2, batchBytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(framedBytes).asReadOnlyBuffer();
        buffer.position(2);
        buffer.limit(2 + batchBytes.length);
        assertThat(buffer.hasArray()).isFalse();
        assertThat(buffer.isDirect()).isFalse();

        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(buffer);

        assertThat(batch.fencedWriterKey()).isEqualTo(new WriterKey(6L, 7L));
        assertThat(batch.fencedSequence()).isEqualTo(8L);
        assertThat(buffer.position()).isEqualTo(2);
        assertThat(buffer.limit()).isEqualTo(2 + batchBytes.length);
    }

    @Test
    void testReaderRejectsRecordCountWithoutPayload() throws Exception {
        byte[] bytes = buildV1Batch(new WriterKey(1L, 2L), 3L);
        putInt(bytes, FencedKvRecordBatch.RECORDS_COUNT_OFFSET, 1);
        updateCrc(bytes, bytes.length);

        assertReaderRejects(ByteBuffer.wrap(bytes), "record count 1 does not fit payload size 0");
    }

    @Test
    void testReaderRejectsNegativeRecordCount() throws Exception {
        byte[] bytes = buildV1Batch(new WriterKey(1L, 2L), 3L);
        putInt(bytes, FencedKvRecordBatch.RECORDS_COUNT_OFFSET, -1);
        updateCrc(bytes, bytes.length);

        assertReaderRejects(ByteBuffer.wrap(bytes), "negative record count -1");
    }

    @Test
    void testRecordCannotConsumeBytesBeyondDeclaredBatchEnd() throws Exception {
        byte[] validRecordBatch = buildV1BatchWithKeys(new byte[] {42});
        int declaredSize = validRecordBatch.length - 1;
        byte[] bytesWithSentinel = Arrays.copyOf(validRecordBatch, validRecordBatch.length + 4);
        Arrays.fill(
                bytesWithSentinel, validRecordBatch.length, bytesWithSentinel.length, (byte) 99);
        putInt(bytesWithSentinel, FencedKvRecordBatch.LENGTH_OFFSET, declaredSize - Integer.BYTES);
        updateCrc(bytesWithSentinel, declaredSize);
        KvRecordBatch batch =
                KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytesWithSentinel));
        batch.ensureValid();

        Iterator<KvRecord> records = batch.records(readContext()).iterator();

        assertThatThrownBy(records::next)
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("crosses declared batch end");
    }

    @Test
    void testRecordCountMustConsumeEntireDeclaredPayload() throws Exception {
        byte[] bytes = buildV1BatchWithKeys(new byte[] {1}, new byte[] {2});
        putInt(bytes, FencedKvRecordBatch.RECORDS_COUNT_OFFSET, 1);
        updateCrc(bytes, bytes.length);
        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes));
        batch.ensureValid();

        Iterator<KvRecord> records = batch.records(readContext()).iterator();

        assertThatThrownBy(records::next)
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("does not end at declared batch end");
    }

    @Test
    void testSetWriterStateAfterBuildRebuildsHeader() throws Exception {
        FencedKvRecordBatchBuilder builder = newBuilder();
        builder.setWriterState(new WriterKey(1L, 2L), 3L);
        BytesView firstBuild = builder.build();

        builder.setWriterState(new WriterKey(4L, 5L), 6L);
        BytesView secondBuild = builder.build();
        KvRecordBatch batch =
                KvRecordBatchReader.pointToByteBuffer(secondBuild.getByteBuf().nioBuffer());

        assertThat(secondBuild).isNotSameAs(firstBuild);
        assertThat(batch.fencedWriterKey()).isEqualTo(new WriterKey(4L, 5L));
        assertThat(batch.fencedSequence()).isEqualTo(6L);
        batch.ensureValid();
        builder.close();
    }

    private static byte[] buildV1Batch(WriterKey writerKey, long sequence) throws Exception {
        FencedKvRecordBatchBuilder builder = newBuilder();
        builder.setWriterState(writerKey, sequence);
        ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        builder.close();
        return bytes;
    }

    private static byte[] buildV1BatchWithKeys(byte[]... keys) throws Exception {
        FencedKvRecordBatchBuilder builder = newBuilder();
        builder.setWriterState(new WriterKey(1L, 2L), 3L);
        for (byte[] key : keys) {
            builder.append(key, null);
        }
        ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        builder.close();
        return bytes;
    }

    private static FencedKvRecordBatchBuilder newBuilder() {
        return FencedKvRecordBatchBuilder.builder(
                1, Integer.MAX_VALUE, new UnmanagedPagedOutputView(100), KvFormat.COMPACTED);
    }

    private static KvRecordBatch.ReadContext readContext() {
        return KvRecordReadContext.createReadContext(
                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
    }

    private static void assertReaderRejects(ByteBuffer buffer, String message) {
        int position = buffer.position();
        int limit = buffer.limit();
        assertThatThrownBy(() -> KvRecordBatchReader.pointToByteBuffer(buffer))
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining(message);
        assertThat(buffer.position()).isEqualTo(position);
        assertThat(buffer.limit()).isEqualTo(limit);
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
    }

    private static void updateCrc(byte[] bytes, int declaredSize) {
        long crc =
                Crc32C.compute(
                        bytes,
                        FencedKvRecordBatch.SCHEMA_ID_OFFSET,
                        declaredSize - FencedKvRecordBatch.SCHEMA_ID_OFFSET);
        putInt(bytes, FencedKvRecordBatch.CRC_OFFSET, (int) crc);
    }

    private static byte[] minimumBatchWithMagic(byte magic) {
        return ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN).putInt(1).put(magic).array();
    }

    private static ByteBuffer minimumBatchWithLengthAndMagic(int declaredLength, byte magic) {
        ByteBuffer buffer = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(declaredLength).put(magic).flip();
        return buffer;
    }
}
