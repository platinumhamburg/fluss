/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueEncoder;

import java.util.Objects;

/** A value of key-value pair that contains schema id and binary row. */
public class BinaryValue {

    public final short schemaId;
    public final BinaryRow row;
    private final long tag;
    private final boolean hasTag;

    /** Creates a v2 value (no tag prefix). */
    public BinaryValue(short schemaId, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
        this.tag = 0;
        this.hasTag = false;
    }

    /** Creates a v3 value with an 8-byte tag prefix (e.g. partitionId for partition TTL). */
    public BinaryValue(short schemaId, long tag, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
        this.tag = tag;
        this.hasTag = true;
    }

    /**
     * Creates a new BinaryValue with a different schemaId and row, preserving the tag attribute
     * from this value. This is used by mergers/updaters to propagate the tag through merge
     * operations without leaking format version knowledge into business logic.
     */
    public BinaryValue withRow(short schemaId, BinaryRow row) {
        return hasTag ? new BinaryValue(schemaId, tag, row) : new BinaryValue(schemaId, row);
    }

    /**
     * Encode the value to a byte array to be persisted to kv store.
     *
     * <p>v2 format: {@code [schemaId(2)][BinaryRow]}<br>
     * v3 format: {@code [schemaId(2)][tag(8)][BinaryRow]}
     */
    public byte[] encodeValue() {
        return hasTag
                ? ValueEncoder.encodeValue(schemaId, tag, row)
                : ValueEncoder.encodeValue(schemaId, row);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryValue that = (BinaryValue) o;
        return schemaId == that.schemaId
                && hasTag == that.hasTag
                && tag == that.tag
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, hasTag, tag, row);
    }

    @Override
    public String toString() {
        if (hasTag) {
            return "BinaryValue{schemaId=" + schemaId + ", tag=" + tag + ", row=" + row + '}';
        }
        return "BinaryValue{schemaId=" + schemaId + ", row=" + row + '}';
    }
}
