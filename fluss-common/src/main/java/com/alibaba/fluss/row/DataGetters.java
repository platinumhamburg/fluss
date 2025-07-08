package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Getters to get data.
 *
 * @since 0.4.0
 */
@PublicEvolving
public interface DataGetters {

    /** Returns true if the element is null at the given position. */
    boolean isNullAt(int pos);

    /** Returns the boolean value at the given position. */
    boolean getBoolean(int pos);

    /** Returns the byte value at the given position. */
    byte getByte(int pos);

    /** Returns the short value at the given position. */
    short getShort(int pos);

    /** Returns the integer value at the given position. */
    int getInt(int pos);

    /** Returns the long value at the given position. */
    long getLong(int pos);

    /** Returns the float value at the given position. */
    float getFloat(int pos);

    /** Returns the double value at the given position. */
    double getDouble(int pos);

    /** Returns the string value at the given position. */
    BinaryString getString(int pos);

    /**
     * Returns the decimal value at the given position.
     *
     * <p>The precision and scale are required to determine whether the decimal value was stored in
     * a compact representation (see {@link Decimal}).
     */
    Decimal getDecimal(int pos, int precision, int scale);

    /**
     * Returns the timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link TimestampNtz}).
     */
    TimestampNtz getTimestampNtz(int pos, int precision);

    /**
     * Returns the timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link TimestampLtz}).
     */
    TimestampLtz getTimestampLtz(int pos, int precision);

    /** Returns the binary value at the given position. */
    byte[] getBinary(int pos);

    /** Returns the array value at the given position. */
    InternalArray getArray(int pos);

    /**
     * Returns the row value at the given position.
     *
     * <p>The number of fields is required to correctly extract the row.
     */
    InternalRow getRow(int pos, int numFields);
}
