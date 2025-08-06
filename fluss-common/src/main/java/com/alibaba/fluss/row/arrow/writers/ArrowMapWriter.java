/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.arrow.writers;

import com.alibaba.fluss.row.DataGetters;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.MapVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.ArrowUtils;

import java.util.List;

/** ArrowMapWriter. */
public class ArrowMapWriter extends ArrowFieldWriter<DataGetters> {

    private final ArrowFieldWriter<DataGetters> keyWriter;
    private final ArrowFieldWriter<DataGetters> valueWriter;
    private int offset;

    public ArrowMapWriter(
            ValueVector valueVector,
            ArrowFieldWriter<DataGetters> keyWriter,
            ArrowFieldWriter<DataGetters> valueWriter) {
        super(valueVector);
        this.keyWriter = keyWriter;
        this.valueWriter = valueWriter;
    }

    public static ArrowFieldWriter<DataGetters> forField(
            ValueVector valueVector, DataType keyType, DataType valueType) {
        MapVector mapVector = (MapVector) valueVector;
        mapVector.reAlloc();
        FieldVector dataVector = mapVector.getDataVector();
        List<FieldVector> keyValueVectors = dataVector.getChildrenFromFields();
        ArrowFieldWriter<DataGetters> keyWriter =
                ArrowUtils.createArrowFieldWriter(keyValueVectors.get(0), keyType);
        ArrowFieldWriter<DataGetters> valueWriter =
                ArrowUtils.createArrowFieldWriter(keyValueVectors.get(1), valueType);
        return new ArrowMapWriter(valueVector, keyWriter, valueWriter);
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        InternalMap map = row.getMap(ordinal);
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        MapVector mapVector = (MapVector) getValueVector();
        StructVector structVector = (StructVector) mapVector.getDataVector();

        mapVector.startNewValue(rowIndex);
        for (int mapIndex = 0; mapIndex < map.size(); mapIndex++) {
            int fieldIndex = offset + mapIndex;
            keyWriter.write(fieldIndex, keyArray, mapIndex, handleSafe);
            valueWriter.write(fieldIndex, valueArray, mapIndex, handleSafe);
            structVector.setIndexDefined(fieldIndex);
        }
        offset += map.size();
        mapVector.endValue(rowIndex, map.size());
    }
}
