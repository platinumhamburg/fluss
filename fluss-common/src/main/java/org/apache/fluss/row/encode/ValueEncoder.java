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

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.UnsafeUtils;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;

/**
 * An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store.
 *
 * <p>This class provides both static utility methods (for direct encoding) and a version-aware
 * instance API via {@link #forVersion(int, ToLongFunction)}. The instance API encapsulates the
 * format version and tag provider so that callers don't need to know whether v2 or v3 encoding is
 * required.
 */
public class ValueEncoder {

    public static final int SCHEMA_ID_LENGTH = 2;

    /** Length of the tag field in v3 value format. */
    public static final int TAG_LENGTH = 8;

    /**
     * Byte offset of the tag field within the encoded v3 value. Equals {@link #SCHEMA_ID_LENGTH}
     * because the tag immediately follows the 2-byte schemaId.
     */
    public static final int TAG_OFFSET = SCHEMA_ID_LENGTH;

    private final int kvFormatVersion;
    @Nullable private final ToLongFunction<BinaryRow> tagProvider;

    private ValueEncoder(int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> tagProvider) {
        this.kvFormatVersion = kvFormatVersion;
        this.tagProvider = tagProvider;
    }

    /**
     * Creates a version-aware ValueEncoder instance.
     *
     * <p>Invariant: {@code kvFormatVersion >= 3} iff {@code tagProvider != null}.
     *
     * @param kvFormatVersion the KV format version of the table
     * @param tagProvider function to extract the tag from a row (required for v3, null for v2)
     */
    public static ValueEncoder forVersion(
            int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> tagProvider) {
        if (kvFormatVersion >= KV_FORMAT_VERSION_3 && tagProvider == null) {
            throw new IllegalArgumentException(
                    "tagProvider must be non-null for kvFormatVersion >= 3");
        }
        if (kvFormatVersion < KV_FORMAT_VERSION_3 && tagProvider != null) {
            throw new IllegalArgumentException("tagProvider must be null for kvFormatVersion < 3");
        }
        return new ValueEncoder(kvFormatVersion, tagProvider);
    }

    /**
     * Creates a BinaryValue for the given schemaId and row, encoding with the format version bound
     * to this encoder. For v3, the tag is automatically extracted via the tagProvider.
     */
    public BinaryValue createValue(short schemaId, BinaryRow row) {
        if (kvFormatVersion >= KV_FORMAT_VERSION_3) {
            long tag = tagProvider.applyAsLong(row);
            return new BinaryValue(schemaId, tag, row);
        }
        return new BinaryValue(schemaId, row);
    }

    // ---- Static utility methods (used by BinaryValue.encodeValue() and low-level paths) ----

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value (v2 format): {@code
     * [schemaId(2)][BinaryRow]}.
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
     * Encode the {@code row} with a {@code schemaId} and a {@code tag} to a byte array value (v3
     * format): {@code [schemaId(2)][tag(8)][BinaryRow]}.
     *
     * <p>The tag is written in native byte order (little-endian on x86/ARM), matching the RocksDB
     * C++ {@code DecodeFixed64} convention used by {@code FloorSetCompactionFilter}.
     *
     * @param schemaId the schema id of the row
     * @param tag the 8-byte tag value (e.g. partitionId for partition TTL)
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, long tag, BinaryRow row) {
        byte[] values = new byte[SCHEMA_ID_LENGTH + TAG_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putShort(values, 0, schemaId);
        UnsafeUtils.putLong(values, SCHEMA_ID_LENGTH, tag);
        row.copyTo(values, SCHEMA_ID_LENGTH + TAG_LENGTH);
        return values;
    }
}
