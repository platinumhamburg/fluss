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

package com.alibaba.fluss.row.compacted;

import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;

/** A decoder for {@link CompactedRow}. */
public class CompactedRowDeserializer {
    private final CompactedRowReader.FieldReader[] readers;
    private final DataType[] types;

    public CompactedRowDeserializer(DataType[] types) {
        this.types = types;
        this.readers = new CompactedRowReader.FieldReader[types.length];
        for (int i = 0; i < types.length; i++) {
            // Don't need to copy to nullable because decode method checks value is null or not
            readers[i] = CompactedRowReader.createFieldReader(types[i]);
        }
    }

    public void deserialize(CompactedRowReader reader, GenericRow output) {
        for (int i = 0; i < readers.length; i++) {
            DataType type = types[i];
            if (type.getTypeRoot() == DataTypeRoot.ROW) {
                handleNestedRow(reader, output, i, type);
            } else {
                output.setField(i, readers[i].readField(reader, i));
            }
        }
    }

    public DataType[] getTypes() {
        return types;
    }

    private void handleNestedRow(
            CompactedRowReader reader, GenericRow output, int fieldIndex, DataType rowType) {
        DataType[] subTypes = rowType.getChildren().toArray(new DataType[0]);
        CompactedRowDeserializer nestedDeserializer = new CompactedRowDeserializer(subTypes);

        CompactedRow row = (CompactedRow) readers[fieldIndex].readField(reader, fieldIndex);
        reader.pointTo(
                row.getSegment(), row.getOffset(), row.getOffset() + 1, row.getSizeInBytes());

        GenericRow nestedRow = new GenericRow(subTypes.length);
        nestedDeserializer.deserialize(reader, nestedRow);
        output.setField(fieldIndex, nestedRow);
    }
}
