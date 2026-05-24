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

package org.apache.fluss.utils;

import java.util.Collections;
import java.util.Set;

/** Utility constants and helpers shared by the Global Secondary Index Table layer. */
public final class IndexTableUtils {

    /**
     * Single system column appended to partitioned Index Tables. Carries the source main
     * table's {@code partitionId} (Big-Endian int64) so that partition tombstone cleanup can
     * filter dead rows without consulting external state.
     */
    public static final String PARTITION_ID_SYSTEM_COLUMN = "__partition_id";

    /**
     * Reserved system column names that user-defined schemas must not declare on a main table
     * (otherwise Index Table derivation would collide). Index Tables themselves may add these
     * columns at derive time; user-facing schemas must keep their column namespace clean.
     */
    public static final Set<String> RESERVED_INDEX_SYSTEM_COLUMNS =
            Collections.singleton(PARTITION_ID_SYSTEM_COLUMN);

    /** Fixed-width Big-Endian int64 prefix for partitioned Index Table values. */
    public static final int PARTITION_ID_PREFIX_SIZE = 8;

    /**
     * Separator between main table name and index name in Index Table paths.
     *
     * <p>Chosen so the composed Index Table name passes {@code TablePath.detectInvalidName}
     * (which restricts table names to {@code [a-zA-Z0-9_-]}) while remaining unambiguous: user
     * index names are validated by {@code Schema.Index} to match {@code ^[A-Za-z0-9_]+$} AND to
     * forbid {@code __}, so a name {@code <main>__<index>} unambiguously decomposes back to its
     * parts.
     */
    public static final String INDEX_TABLE_NAME_SEPARATOR = "__";

    private IndexTableUtils() {}

    /** Builds the Index Table name from a main table name and an index name. */
    public static String indexTableName(String mainTableName, String indexName) {
        return mainTableName + INDEX_TABLE_NAME_SEPARATOR + indexName;
    }

    /**
     * Prepends the 8-byte Big-Endian int64 partitionId to the given value body.
     *
     * @return a new byte array of length {@code 8 + valueBody.length}
     */
    public static byte[] prependPartitionIdPrefix(long partitionId, byte[] valueBody) {
        if (valueBody == null) {
            throw new IllegalArgumentException("valueBody must not be null");
        }
        byte[] out = new byte[PARTITION_ID_PREFIX_SIZE + valueBody.length];
        out[0] = (byte) (partitionId >>> 56);
        out[1] = (byte) (partitionId >>> 48);
        out[2] = (byte) (partitionId >>> 40);
        out[3] = (byte) (partitionId >>> 32);
        out[4] = (byte) (partitionId >>> 24);
        out[5] = (byte) (partitionId >>> 16);
        out[6] = (byte) (partitionId >>> 8);
        out[7] = (byte) partitionId;
        System.arraycopy(valueBody, 0, out, PARTITION_ID_PREFIX_SIZE, valueBody.length);
        return out;
    }

    /**
     * Decodes the first 8 bytes of {@code value} as a Big-Endian int64 partitionId.
     *
     * @throws IllegalArgumentException if {@code value} is null or shorter than 8 bytes
     */
    public static long decodePartitionIdPrefix(byte[] value) {
        if (value == null || value.length < PARTITION_ID_PREFIX_SIZE) {
            throw new IllegalArgumentException(
                    "Partitioned Index Table value must be at least "
                            + PARTITION_ID_PREFIX_SIZE
                            + " bytes for the partitionId prefix");
        }
        return ((long) (value[0] & 0xFF) << 56)
                | ((long) (value[1] & 0xFF) << 48)
                | ((long) (value[2] & 0xFF) << 40)
                | ((long) (value[3] & 0xFF) << 32)
                | ((long) (value[4] & 0xFF) << 24)
                | ((long) (value[5] & 0xFF) << 16)
                | ((long) (value[6] & 0xFF) << 8)
                | ((long) value[7] & 0xFF);
    }

    /**
     * Returns the value body without the 8-byte partitionId prefix.
     *
     * @throws IllegalArgumentException if {@code value} is null or shorter than 8 bytes
     */
    public static byte[] stripPartitionIdPrefix(byte[] value) {
        if (value == null || value.length < PARTITION_ID_PREFIX_SIZE) {
            throw new IllegalArgumentException(
                    "Partitioned Index Table value must be at least "
                            + PARTITION_ID_PREFIX_SIZE
                            + " bytes");
        }
        byte[] body = new byte[value.length - PARTITION_ID_PREFIX_SIZE];
        System.arraycopy(value, PARTITION_ID_PREFIX_SIZE, body, 0, body.length);
        return body;
    }
}
