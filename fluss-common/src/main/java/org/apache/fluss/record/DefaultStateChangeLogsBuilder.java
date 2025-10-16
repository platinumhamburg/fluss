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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for constructing {@link DefaultStateChangeLogs}.
 *
 * <p>This builder helps construct state change logs in memory with the following layout:
 *
 * <pre>
 * +--------------+--------------+--------------+--------------+-----+
 * | Version      | Row Count    | AlignedRow 1 | AlignedRow 2 | ... |
 * | (1 byte)     | (4 bytes)    | (variable)   | (variable)   |     |
 * +--------------+--------------+--------------+--------------+-----+
 * </pre>
 *
 * <p>Each AlignedRow contains 4 fields:
 *
 * <ul>
 *   <li>Field 0: ChangeType (byte)
 *   <li>Field 1: StateDef (int, StateDefs ID)
 *   <li>Field 2: Key (String)
 *   <li>Field 3: Value (String, nullable)
 * </ul>
 */
public class DefaultStateChangeLogsBuilder {

    private static final byte VERSION_1 = 1;
    private static final int VERSION_SIZE = 1;
    private static final int FIELD_COUNT = 4;
    private static final int ROW_COUNT_SIZE = 4;
    private static final int HEADER_SIZE = VERSION_SIZE + ROW_COUNT_SIZE;
    private static final int ROW_SIZE_PREFIX = 4;

    private final List<byte[]> rowDataList;
    private int totalSize;

    public DefaultStateChangeLogsBuilder() {
        this.rowDataList = new ArrayList<>();
        this.totalSize = HEADER_SIZE;
    }

    /**
     * Adds a state change log entry.
     *
     * @param changeType the type of change
     * @param stateDef the state definition (StateDefs enum)
     * @param key the key of the state (as Object)
     * @param value the value of the state (as Object), can be null for deletion
     * @throws IllegalArgumentException if key or value types don't match the expected types in
     *     stateDef
     */
    public void addLog(ChangeType changeType, StateDefs stateDef, Object key, Object value) {
        // Validate types
        if (stateDef != null) {
            // Validate key type
            if (key != null) {
                Class<?> expectedKeyType = stateDef.getKeyType();
                if (!expectedKeyType.isInstance(key)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Key type mismatch for state definition %s: expected %s, but got %s",
                                    stateDef.name(),
                                    expectedKeyType.getName(),
                                    key.getClass().getName()));
                }
            }

            // Validate value type (value can be null for delete operations)
            if (value != null) {
                Class<?> expectedValueType = stateDef.getValueType();
                if (!expectedValueType.isInstance(value)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Value type mismatch for state definition %s: expected %s, but got %s",
                                    stateDef.name(),
                                    expectedValueType.getName(),
                                    value.getClass().getName()));
                }
            }
        }

        // Create an AlignedRow with 4 fields
        AlignedRow row = new AlignedRow(FIELD_COUNT);
        AlignedRowWriter writer = new AlignedRowWriter(row, 256);
        writer.reset();

        // Field 0: ChangeType
        if (changeType != null) {
            writer.writeByte(0, changeType.toByteValue());
        } else {
            writer.setNullAt(0);
        }

        // Field 1: StateDef (store as int ID)
        if (stateDef != null) {
            writer.writeInt(1, stateDef.getId());
        } else {
            writer.setNullAt(1);
        }

        // Field 2: Key (serialize Object to String)
        if (key != null && stateDef != null) {
            String serializedKey = stateDef.getKeySerializer().serialize(key);
            writer.writeString(2, BinaryString.fromString(serializedKey));
        } else {
            writer.setNullAt(2);
        }

        // Field 3: Value (serialize Object to String)
        if (value != null && stateDef != null) {
            String serializedValue = stateDef.getValueSerializer().serialize(value);
            writer.writeString(3, BinaryString.fromString(serializedValue));
        } else {
            writer.setNullAt(3);
        }

        writer.complete();

        // Serialize the row to byte array
        int rowSize = row.getSizeInBytes();
        byte[] rowData = new byte[rowSize];
        row.getSegments()[0].get(row.getOffset(), rowData, 0, rowSize);

        rowDataList.add(rowData);
        totalSize += ROW_SIZE_PREFIX + rowSize;
    }

    /**
     * Gets the total size in bytes of the state change logs data.
     *
     * @return the total size in bytes
     */
    public int sizeInBytes() {
        return totalSize;
    }

    /**
     * Gets the number of state change log entries.
     *
     * @return the number of entries
     */
    public int getRowCount() {
        return rowDataList.size();
    }

    /**
     * Builds and returns a DefaultStateChangeLogs instance backed by a memory segment.
     *
     * @return the constructed DefaultStateChangeLogs
     */
    public DefaultStateChangeLogs build() {
        byte[] data = new byte[totalSize];
        MemorySegment segment = MemorySegment.wrap(data);

        int offset = 0;

        // Write version
        segment.put(offset, VERSION_1);
        offset += VERSION_SIZE;

        // Write row count
        segment.putInt(offset, rowDataList.size());
        offset += ROW_COUNT_SIZE;

        // Write each row with its size prefix
        for (byte[] rowData : rowDataList) {
            segment.putInt(offset, rowData.length);
            offset += ROW_SIZE_PREFIX;
            segment.put(offset, rowData, 0, rowData.length);
            offset += rowData.length;
        }

        DefaultStateChangeLogs logs = new DefaultStateChangeLogs();
        logs.pointTo(segment, 0, totalSize);
        return logs;
    }

    /** Resets the builder to its initial state, allowing it to be reused. */
    public void reset() {
        rowDataList.clear();
        totalSize = HEADER_SIZE;
    }
}
