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
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;

import static org.apache.fluss.row.encode.TsValueEncoder.TS_LENGTH;
import static org.apache.fluss.row.encode.ValueEncoder.SCHEMA_ID_LENGTH;

/** A decoder to decode byte array values that were encoded by {@link TsValueEncoder}. */
public class TsValueDecoder {

    private final RowDecoder rowDecoder;

    public TsValueDecoder(RowDecoder rowDecoder) {
        this.rowDecoder = rowDecoder;
    }

    public RowDecoder getRowDecoder() {
        return rowDecoder;
    }

    /**
     * A decoder to decode byte array values that were encoded by {@link TsValueEncoder}.
     *
     * <p>The decoder extracts the schema id, ts and {@link BinaryRow} from the encoded value.
     */
    public TsValue decodeValue(byte[] valueBytes) {
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        // Use big-endian to match TsValueEncoder's encoding
        long ts = memorySegment.getLongBigEndian(0);
        short schemaId = memorySegment.getShort(TS_LENGTH);
        BinaryRow row =
                rowDecoder.decode(
                        memorySegment,
                        TS_LENGTH + SCHEMA_ID_LENGTH,
                        valueBytes.length - SCHEMA_ID_LENGTH - TS_LENGTH);
        return new TsValue(ts, schemaId, row);
    }

    /**
     * The schema id, log offset and {@link BinaryRow} stored as the value of kv store.
     *
     * <p>This represents the decoded data from values encoded by {@link TsValueEncoder}.
     */
    public static class TsValue {
        public final long ts;
        public final short schemaId;
        public final BinaryRow row;

        private TsValue(long ts, short schemaId, BinaryRow row) {
            this.ts = ts;
            this.schemaId = schemaId;
            this.row = row;
        }
    }
}
