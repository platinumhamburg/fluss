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

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.AlignedRowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.UnsafeUtils;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.row.encode.ValueEncoder.SCHEMA_ID_LENGTH;
import static org.apache.fluss.row.encode.ValueEncoder.TAG_LENGTH;
import static org.apache.fluss.row.encode.ValueEncoder.TAG_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the byte layout produced by {@link ValueEncoder#createValue(short, BinaryRow)} in
 * v3 format is exactly what the C++ {@code FloorSetCompactionFilter} reads at its hard-coded
 * offsets.
 *
 * <p>Contract under test (from RocksDB fork's FloorSetCompactionFilter.cc):
 *
 * <pre>
 *   uint64_t tag = DecodeFixed64(value.data() + tag_offset);
 *   // tag_offset is passed from Java as ValueEncoder.TAG_OFFSET (=2)
 * </pre>
 *
 * <p>This test does NOT depend on the RocksDB fork JAR and can run in any CI environment. If the
 * Java encoding layout ever drifts from the C++ read convention, this test fails immediately.
 */
class ValueEncoderCompactionFilterCompatTest {

    private static final short TEST_SCHEMA_ID = 7;
    private static final long TEST_TAG = 123456789L;

    @Test
    void testEncodedValueTagIsReadableAtFloorSetFilterOffset() throws Exception {
        // (1) Create a BinaryRow with a single INT field — content is irrelevant; we only care
        // about the value header layout (schemaId + tag) and that the row body follows immediately.
        DataType[] fieldTypes = new DataType[] {DataTypes.INT()};
        BinaryRow testRow;
        try (AlignedRowEncoder encoder = new AlignedRowEncoder(fieldTypes)) {
            encoder.startNewRow();
            encoder.encodeField(0, 42);
            testRow = encoder.finishRow();
        }
        int rowSize = testRow.getSizeInBytes();

        // (2) Create a v3 encoder with a fixed tag extractor.
        ValueEncoder valueEncoder = ValueEncoder.forVersion(3, row -> TEST_TAG);

        // (3) Encode via the instance API (the production path).
        BinaryValue binaryValue = valueEncoder.createValue(TEST_SCHEMA_ID, testRow);
        byte[] encoded = binaryValue.encodeValue();

        // ---- Assertions: verify the byte-level contract with FloorSetCompactionFilter ----

        // Total length = schemaId(2) + tag(8) + row body
        assertThat(encoded.length)
                .as("v3 value length = SCHEMA_ID_LENGTH + TAG_LENGTH + row body")
                .isEqualTo(SCHEMA_ID_LENGTH + TAG_LENGTH + rowSize);

        // SchemaId at offset 0, native byte order (little-endian on x86/ARM).
        short readSchemaId = UnsafeUtils.getShort(encoded, 0);
        assertThat(readSchemaId)
                .as("schemaId must be at offset 0 in native byte order")
                .isEqualTo(TEST_SCHEMA_ID);

        // Tag at TAG_OFFSET (=2), native byte order — this is the exact offset the C++
        // FloorSetCompactionFilter uses: DecodeFixed64(value.data() + tag_offset).
        // DecodeFixed64 reads in little-endian (native) order on x86/ARM, matching Java's
        // Unsafe.putLong which also uses native order.
        long readTag = UnsafeUtils.getLong(encoded, TAG_OFFSET);
        assertThat(readTag)
                .as(
                        "tag must be at TAG_OFFSET (%d) in native byte order, "
                                + "matching C++ DecodeFixed64(value.data() + %d)",
                        TAG_OFFSET, TAG_OFFSET)
                .isEqualTo(TEST_TAG);

        // Row body starts at offset SCHEMA_ID_LENGTH + TAG_LENGTH (= 10).
        byte[] expectedRowBytes = new byte[rowSize];
        testRow.copyTo(expectedRowBytes, 0);
        byte[] actualRowBytes = new byte[rowSize];
        System.arraycopy(encoded, SCHEMA_ID_LENGTH + TAG_LENGTH, actualRowBytes, 0, rowSize);
        assertThat(actualRowBytes)
                .as("BinaryRow body must start at offset %d", SCHEMA_ID_LENGTH + TAG_LENGTH)
                .isEqualTo(expectedRowBytes);

        // Cross-check: TAG_OFFSET must equal SCHEMA_ID_LENGTH (the tag immediately follows
        // schemaId). This guards against accidental insertion of padding between the two.
        assertThat(TAG_OFFSET)
                .as("TAG_OFFSET must equal SCHEMA_ID_LENGTH (no padding between schemaId and tag)")
                .isEqualTo(SCHEMA_ID_LENGTH);
    }
}
