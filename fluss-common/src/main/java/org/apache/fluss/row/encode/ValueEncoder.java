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

import org.apache.fluss.metadata.CompactionFilterConfig;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.UnsafeUtils;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public class ValueEncoder {

    public static final int SCHEMA_ID_LENGTH = 2;

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value to be expected persisted
     * to kv store.
     *
     * <p>The encoded value format is: [schemaId(2 bytes)][row data]
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        byte[] values = new byte[SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putShort(values, 0, schemaId);
        row.copyTo(values, SCHEMA_ID_LENGTH);
        return values;
    }

    /**
     * Encode the {@code row} with a {@code schemaId} and an 8-byte long prefix based on the
     * compaction filter configuration.
     *
     * <p>The encoded value format depends on the compaction filter configuration:
     *
     * <ul>
     *   <li>No prefix: [schemaId(2 bytes)][row data]
     *   <li>With 8-byte prefix: [prefix(8 bytes)][schemaId(2 bytes)][row data]
     * </ul>
     *
     * @param prefixValue the prefix value (e.g., timestamp for TTL compaction filter)
     * @param schemaId the schema id of the row
     * @param row the row to encode
     * @param compactionFilterConfig the compaction filter configuration
     */
    public static byte[] encodeValueWithLongPrefix(
            long prefixValue,
            short schemaId,
            BinaryRow row,
            CompactionFilterConfig compactionFilterConfig) {
        int prefixLength = compactionFilterConfig.getPrefixLength();
        if (prefixLength == 0) {
            return encodeValue(schemaId, row);
        }

        // Assert that prefix length is 8 bytes for long encoding
        if (prefixLength != 8) {
            throw new IllegalArgumentException(
                    "Prefix length must be 8 bytes for long prefix encoding, but got: "
                            + prefixLength);
        }

        // Encode with 8-byte prefix
        byte[] values = new byte[prefixLength + SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        // Encode prefix as big-endian for compaction filter compatibility
        UnsafeUtils.putLongBigEndian(values, 0, prefixValue);
        UnsafeUtils.putShort(values, prefixLength, schemaId);
        row.copyTo(values, prefixLength + SCHEMA_ID_LENGTH);
        return values;
    }
}
