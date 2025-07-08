package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * @see GenericArray
 * @since 0.4.0
 */
@PublicEvolving
public interface InternalArray extends DataGetters {

    /** Returns the number of elements in this array. */
    int size();

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

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
     * @param elementType the element type of the array
     */
    static ElementGetter createElementGetter(DataType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case STRING:
                elementGetter = InternalArray::getString;
                break;
            case BOOLEAN:
                elementGetter = InternalArray::getBoolean;
                break;
            case BINARY:
            case BYTES:
                elementGetter = InternalArray::getBinary;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                elementGetter =
                        (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = InternalArray::getByte;
                break;
            case SMALLINT:
                elementGetter = InternalArray::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = InternalArray::getInt;
                break;
            case BIGINT:
                elementGetter = InternalArray::getLong;
                break;
            case FLOAT:
                elementGetter = InternalArray::getFloat;
                break;
            case DOUBLE:
                elementGetter = InternalArray::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                elementGetter =
                        (array, pos) -> array.getTimestampNtz(pos, getPrecision(elementType));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                elementGetter =
                        (array, pos) -> array.getTimestampLtz(pos, getPrecision(elementType));
                break;
            case ARRAY:
                elementGetter = InternalArray::getArray;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(),
                                InternalArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * Accessor for getting the elements of an array during runtime.
     *
     * @see #createElementGetter(DataType)
     */
    interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(InternalArray array, int pos);
    }
}
