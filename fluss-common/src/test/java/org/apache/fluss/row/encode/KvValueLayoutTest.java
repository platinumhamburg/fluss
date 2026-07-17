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

package org.apache.fluss.row.encode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordBatch;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_1;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the versioned KV value layout. */
class KvValueLayoutTest {

    @Test
    void testLayoutByKvFormatVersion() {
        KvValueLayout version1 = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_1);
        KvValueLayout version2 = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_2);
        KvValueLayout version3 = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

        assertThat(version1.schemaIdOffset()).isZero();
        assertThat(version1.schemaIdLength()).isEqualTo(2);
        assertThat(version1.hasValueTag()).isFalse();
        assertThat(version1.valueTagLength()).isZero();
        assertThat(version1.rowPayloadOffset()).isEqualTo(2);

        assertThat(version2.schemaIdOffset()).isZero();
        assertThat(version2.schemaIdLength()).isEqualTo(2);
        assertThat(version2.hasValueTag()).isFalse();
        assertThat(version2.valueTagLength()).isZero();
        assertThat(version2.rowPayloadOffset()).isEqualTo(2);

        assertThat(version3.schemaIdOffset()).isZero();
        assertThat(version3.schemaIdLength()).isEqualTo(2);
        assertThat(version3.hasValueTag()).isTrue();
        assertThat(version3.valueTagOffset()).isEqualTo(2);
        assertThat(version3.valueTagLength()).isEqualTo(8);
        assertThat(version3.rowPayloadOffset()).isEqualTo(10);

        assertThatThrownBy(() -> KvValueLayout.forKvFormatVersion(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported KV format version 0");
        assertThatThrownBy(() -> KvValueLayout.forKvFormatVersion(4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported KV format version 4");
        assertThatThrownBy(version1::valueTagOffset).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> version3.rowPayloadLength(9))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testVersion3ValueTagUsesBigEndianBytes() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long valueTag = 0x0102030405060708L;
        ValueEncoder encoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, ignored -> valueTag);

        byte[] encoded = encoder.encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

        assertThat(
                        Arrays.copyOfRange(
                                encoded,
                                layout.valueTagOffset(),
                                layout.valueTagOffset() + layout.valueTagLength()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
        assertThat(layout.readValueTag(MemorySegment.wrap(encoded))).isEqualTo(valueTag);
    }

    @Test
    void testSchemaIdUsesLittleEndianBytes() {
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);
        byte[] value = new byte[layout.rowPayloadOffset()];

        layout.writeSchemaId(value, (short) 0x0102);

        assertThat(value[0]).isEqualTo((byte) 0x02);
        assertThat(value[1]).isEqualTo((byte) 0x01);
        assertThat(layout.readSchemaId(MemorySegment.wrap(value))).isEqualTo((short) 0x0102);
    }

    @Test
    void testValueTagProviderMustMatchLayout() {
        assertThatThrownBy(
                        () -> ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_2, ignored -> 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be null");
        assertThatThrownBy(() -> ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be non-null");
    }

    @Test
    void testVersionedEncodeDecodeRoundTrip() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        BinaryValue value = new BinaryValue(DEFAULT_SCHEMA_ID, row);

        for (int version : new int[] {KV_FORMAT_VERSION_2, KV_FORMAT_VERSION_3}) {
            ValueEncoder encoder =
                    ValueEncoder.forKvFormatVersion(
                            version, version == KV_FORMAT_VERSION_3 ? ignored -> 11L : null);
            BinaryValue decoded =
                    new ValueDecoder(
                                    new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                    KvFormat.COMPACTED,
                                    KvValueLayout.forKvFormatVersion(version))
                            .decodeValue(encoder.encodeValue(value));

            assertThat(decoded.schemaId).isEqualTo(DEFAULT_SCHEMA_ID);
            assertThat(decoded.row.getInt(0)).isEqualTo(1);
            assertThat(decoded.row.getString(1).toString()).isEqualTo("a");
        }
    }

    @Test
    void testValueTagWriteChecksBounds() {
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

        assertThatThrownBy(() -> layout.writeValueTag(new byte[9], 1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> layout.writeValueTag(new byte[10], -1, 1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> layout.writeSchemaId(new byte[1], (short) 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testReadContextDefaultsToVersion2Layout() {
        ValueRecordBatch.ReadContext defaultReadContext = schemaId -> null;

        assertThat(defaultReadContext.getKvValueLayout())
                .isSameAs(KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_2));
    }

    @Test
    void testVersion3ValueRecordBatchUsesLayoutForEveryRecord() throws Exception {
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(
                encodeVersion3Value(compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}), 100L));
        builder.append(
                encodeVersion3Value(compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b"}), 101L));

        List<ValueRecord> records =
                StreamSupport.stream(
                                builder.build()
                                        .records(
                                                ValueRecordReadContext.createReadContext(
                                                        new TestingSchemaGetter(
                                                                DEFAULT_SCHEMA_ID, DATA1_SCHEMA),
                                                        KvFormat.COMPACTED,
                                                        KvValueLayout.forKvFormatVersion(
                                                                KV_FORMAT_VERSION_3)))
                                        .spliterator(),
                                false)
                        .collect(Collectors.toList());

        assertThat(records).hasSize(2);
        assertThat(records.get(0).schemaId()).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(records.get(0).getRow().getInt(0)).isEqualTo(1);
        assertThat(records.get(0).getRow().getString(1).toString()).isEqualTo("a");
        assertThat(records.get(1).schemaId()).isEqualTo(DEFAULT_SCHEMA_ID);
        assertThat(records.get(1).getRow().getInt(0)).isEqualTo(2);
        assertThat(records.get(1).getRow().getString(1).toString()).isEqualTo("b");
    }

    @Test
    void testFixedSchemaDecoderUsesVersion3Layout() {
        byte[] encoded =
                encodeVersion3Value(compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}), 100L);
        FixedSchemaDecoder decoder =
                new FixedSchemaDecoder(
                        KvFormat.COMPACTED,
                        DATA1_SCHEMA,
                        KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3));

        InternalRow decoded = decoder.decode(MemorySegment.wrap(encoded));

        assertThat(decoded.getInt(0)).isEqualTo(1);
        assertThat(decoded.getString(1).toString()).isEqualTo("a");
    }

    private static byte[] encodeVersion3Value(BinaryRow row, long valueTag) {
        return ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, ignored -> valueTag)
                .encodeValue(new BinaryValue(DEFAULT_SCHEMA_ID, row));
    }
}
