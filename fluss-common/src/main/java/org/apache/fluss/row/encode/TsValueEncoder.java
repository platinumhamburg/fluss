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

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.UnsafeUtils;

/**
 * An encoder to encode {@link BinaryRow} with a schema id and log offset as value to be stored in
 * kv store.
 *
 * <p>The encoded value format is: [logOffset(8 bytes)][schemaId(2 bytes)][row data]
 */
public class TsValueEncoder {

    static final int TS_LENGTH = 8;

    static final int SCHEMA_ID_LENGTH = 2;

    /**
     * Encode the {@code row} with a {@code schemaId} and {@code logOffset} to a byte array value to
     * be stored in kv store.
     *
     * @param ts the log offset of the row
     * @param schemaId the schema id of the row
     * @param row the row to encode
     * @return the encoded byte array with format: [logOffset(8 bytes)][schemaId(2 bytes)][row data]
     */
    public static byte[] encodeValue(long ts, short schemaId, BinaryRow row) {
        byte[] values = new byte[TS_LENGTH + SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putLong(values, 0, ts);
        UnsafeUtils.putShort(values, TS_LENGTH, schemaId);
        row.copyTo(values, TS_LENGTH + SCHEMA_ID_LENGTH);
        return values;
    }
}
