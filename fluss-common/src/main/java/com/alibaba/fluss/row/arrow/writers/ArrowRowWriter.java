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
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;

import java.util.List;

/** ArrowRowWriter. */
public class ArrowRowWriter extends ArrowFieldWriter<DataGetters> {
    private final FieldVector fieldVector;
    private final ArrowFieldWriter<DataGetters>[] fieldWriters;

    public ArrowRowWriter(FieldVector fieldVector, ArrowFieldWriter<DataGetters>[] fieldWriters) {
        super(fieldVector);
        this.fieldVector = fieldVector;
        this.fieldWriters = fieldWriters;
    }

    @SuppressWarnings("unchecked")
    public static ArrowFieldWriter<DataGetters> forField(FieldVector fieldVector, RowType rowType) {
        List<FieldVector> children = fieldVector.getChildrenFromFields();
        ArrowFieldWriter<DataGetters>[] fieldWriters = new ArrowFieldWriter[children.size()];
        for (int i = 0; i < children.size(); i++) {
            fieldWriters[i] =
                    ArrowUtils.createArrowFieldWriter(children.get(i), rowType.getTypeAt(i));
        }
        return new ArrowRowWriter(fieldVector, fieldWriters);
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        int fieldCount = fieldWriters.length;
        InternalRow internalRow = row.getRow(ordinal, fieldCount);
        StructVector structVector = (StructVector) fieldVector;
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(rowIndex, internalRow, i, handleSafe);
        }
        structVector.setIndexDefined(rowIndex);
    }
}
