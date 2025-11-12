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
import org.apache.fluss.memory.MemorySegmentWritableOutputView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.aligned.AlignedRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Default implementation of StateChangeLogs.
 *
 * <p>This class provides zero-copy read access to state change logs stored in a memory segment. The
 * data layout is:
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
public class DefaultStateChangeLogs implements StateChangeLogs {

    private static final byte VERSION_1 = 1;
    private static final int VERSION_SIZE = 1;
    private static final int ROW_COUNT_SIZE = 4;
    private static final int HEADER_SIZE = VERSION_SIZE + ROW_COUNT_SIZE;
    private static final int FIELD_COUNT = 4;

    private MemorySegment segment;
    private int offset;
    private int size;
    private int rowCount;

    /** Points this instance to a memory region containing state change logs. */
    public void pointTo(MemorySegment memorySegment, int offset, int size) {
        this.segment = memorySegment;
        this.offset = offset;
        this.size = size;
        if (size >= HEADER_SIZE) {
            byte version = segment.get(offset);
            if (version != VERSION_1) {
                throw new IllegalArgumentException(
                        "Unsupported state change logs version: " + version);
            }
            this.rowCount = segment.getInt(offset + VERSION_SIZE);
        } else {
            this.rowCount = 0;
        }
    }

    @Override
    public int sizeInBytes() {
        return size;
    }

    @Override
    public Iterator<StateChangeLog> iters() {
        if (rowCount == 0 || segment == null) {
            return Collections.emptyIterator();
        }

        List<StateChangeLog> logs = new ArrayList<>(rowCount);
        int currentOffset = offset + HEADER_SIZE;

        for (int i = 0; i < rowCount; i++) {
            // Read the size of the current AlignedRow
            if (currentOffset + 4 > offset + size) {
                break;
            }
            int rowSize = segment.getInt(currentOffset);
            currentOffset += 4;

            if (currentOffset + rowSize > offset + size) {
                break;
            }

            // Create an AlignedRow pointing to the current position
            AlignedRow row = new AlignedRow(FIELD_COUNT);
            row.pointTo(segment, currentOffset, rowSize);

            // Create StateChangeLog from the AlignedRow
            logs.add(new AlignedStateChangeLog(row));

            currentOffset += rowSize;
        }

        return logs.iterator();
    }

    @Override
    public void writeTo(MemorySegmentWritableOutputView outputView) {
        try {
            if (segment == null || size == 0) {
                // Write version and zero row count for empty logs
                outputView.writeByte(VERSION_1);
                outputView.writeInt(0);
                return;
            }
            outputView.write(segment, offset, size);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write state change logs", e);
        }
    }

    /**
     * Implementation of StateChangeLog backed by AlignedRow.
     *
     * <p>This provides a view over the underlying AlignedRow without copying data.
     */
    private static class AlignedStateChangeLog implements StateChangeLog {
        private final AlignedRow row;

        AlignedStateChangeLog(AlignedRow row) {
            this.row = row;
        }

        @Override
        public ChangeType getChangeType() {
            if (row.isNullAt(0)) {
                return null;
            }
            return ChangeType.fromByteValue(row.getByte(0));
        }

        @Override
        public StateDefs getStateDef() {
            if (row.isNullAt(1)) {
                return null;
            }
            int stateDefId = row.getInt(1);
            return StateDefs.fromId(stateDefId);
        }

        @Override
        public Object getKey() {
            if (row.isNullAt(2)) {
                return null;
            }
            // Get the StateDef to use its key deserializer
            StateDefs stateDef = getStateDef();
            if (stateDef == null) {
                return null;
            }
            BinaryString binaryString = row.getString(2);
            if (binaryString == null) {
                return null;
            }
            // Deserialize the string key to the appropriate type
            String stringKey = binaryString.toString();
            return stateDef.getKeySerializer().deserialize(stringKey);
        }

        @Override
        public Object getValue() {
            if (row.isNullAt(3)) {
                return null;
            }
            // Get the StateDef to use its value deserializer
            StateDefs stateDef = getStateDef();
            if (stateDef == null) {
                return null;
            }
            BinaryString binaryString = row.getString(3);
            if (binaryString == null) {
                return null;
            }
            // Deserialize the string value to the appropriate type
            String stringValue = binaryString.toString();
            return stateDef.getValueSerializer().deserialize(stringValue);
        }
    }
}
