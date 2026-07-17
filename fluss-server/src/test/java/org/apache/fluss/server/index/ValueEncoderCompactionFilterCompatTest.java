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

package org.apache.fluss.server.index;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.AlignedRowEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the shared V3 byte layout consumed by Java and the native compaction filter. */
class ValueEncoderCompactionFilterCompatTest {

    private static final short TEST_SCHEMA_ID = 7;
    private static final long TEST_TAG = 0x0102030405060708L;

    @Test
    void testEncodedValueMatchesFloorSetFilterLayout() throws Exception {
        DataType[] fieldTypes = new DataType[] {DataTypes.INT()};
        BinaryRow testRow;
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 42);
            testRow = encoder.finishRow();
        }

        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);
        ValueEncoder encoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, row -> TEST_TAG);
        byte[] encoded = encoder.encodeValue(new BinaryValue(TEST_SCHEMA_ID, testRow));
        MemorySegment segment = MemorySegment.wrap(encoded);

        assertThat(encoded).hasSize(layout.rowPayloadOffset() + testRow.getSizeInBytes());
        assertThat(layout.readSchemaId(segment)).isEqualTo(TEST_SCHEMA_ID);
        assertThat(layout.readValueTag(segment)).isEqualTo(TEST_TAG);
        assertThat(Arrays.copyOfRange(encoded, layout.valueTagOffset(), layout.rowPayloadOffset()))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8);

        byte[] expectedRowBytes = new byte[testRow.getSizeInBytes()];
        testRow.copyTo(expectedRowBytes, 0);
        assertThat(Arrays.copyOfRange(encoded, layout.rowPayloadOffset(), encoded.length))
                .isEqualTo(expectedRowBytes);
        assertThat(layout.valueTagOffset()).isEqualTo(layout.schemaIdLength());
    }
}
