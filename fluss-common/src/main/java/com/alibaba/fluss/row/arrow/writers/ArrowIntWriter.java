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

package com.alibaba.fluss.row.arrow.writers;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.DataGetters;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;

/** {@link ArrowFieldWriter} for Int. */
@Internal
public class ArrowIntWriter extends ArrowFieldWriter<DataGetters> {

    public static ArrowIntWriter forField(IntVector intVector) {
        return new ArrowIntWriter(intVector);
    }

    private ArrowIntWriter(IntVector intVector) {
        super(intVector);
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        IntVector vector = (IntVector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else if (handleSafe) {
            vector.setSafe(getCount(), readInt(row, ordinal));
        } else {
            vector.set(getCount(), readInt(row, ordinal));
        }
    }

    private boolean isNullAt(DataGetters row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    int readInt(DataGetters row, int ordinal) {
        return row.getInt(ordinal);
    }
}
