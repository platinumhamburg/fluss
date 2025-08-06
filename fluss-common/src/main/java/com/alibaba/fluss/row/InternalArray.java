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

package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeChecks;

import javax.annotation.Nullable;

import java.io.Serializable;

import static com.alibaba.fluss.types.DataTypeChecks.getLength;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * @see GenericArray
 * @since 0.6
 */
@PublicEvolving
public interface InternalArray extends DataGetters {
    /** Returns the number of elements in this array. */
    int size();

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    byte[] getBinary(int pos);

    boolean[] toBooleanArray();

    byte[] toByteArray();

    short[] toShortArray();

    int[] toIntArray();

    long[] toLongArray();

    float[] toFloatArray();

    double[] toDoubleArray();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param fieldType the element type of the array
     */
    static ElementGetter createElementGetter(DataType fieldType, int fieldPos) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int bytesLength = getLength(fieldType);
                elementGetter = row -> row.getChar(fieldPos, bytesLength);
                break;
            case VARCHAR:
            case STRING:
                elementGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                elementGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                final int binaryLength = getLength(fieldType);
                elementGetter = row -> row.getBinary(fieldPos, binaryLength);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                elementGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                elementGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
                elementGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                elementGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                elementGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                elementGetter = row -> row.getTimestampNtz(fieldPos, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                elementGetter = row -> row.getTimestampLtz(fieldPos, timestampLtzPrecision);
                break;
            case ARRAY:
                elementGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                elementGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
                final int rowFieldCount = DataTypeChecks.getFieldCount(fieldType);
                elementGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), InternalArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!fieldType.isNullable()) {
            return elementGetter;
        }
        return (row) -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return elementGetter.getElementOrNull(row);
        };
    }

    /** Accessor for getting the elements of an array during runtime. */
    interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(InternalArray array);
    }
}
