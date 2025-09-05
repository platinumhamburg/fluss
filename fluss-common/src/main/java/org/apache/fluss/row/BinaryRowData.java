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

package org.apache.fluss.row;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.nio.ByteOrder;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * An implementation of {@link InternalRow} which is backed by {@link MemorySegment} instead of
 * Object. It can significantly reduce the serialization/deserialization of Java objects.
 *
 * <p>A Row has two part: Fixed-length part and variable-length part.
 *
 * <p>Fixed-length part contains 1 byte header and null bit set and field values. Null bit set is
 * used for null tracking and is aligned to 8-byte word boundaries. `Field values` holds
 * fixed-length primitive types and variable-length values which can be stored in 8 bytes inside. If
 * it do not fit the variable-length field, then store the length and offset of variable-length
 * part.
 *
 * <p>Fixed-length part will certainly fall into a MemorySegment, which will speed up the read and
 * write of field. During the write phase, if the target memory segment has less space than fixed
 * length part size, we will skip the space. So the number of fields in a single Row cannot exceed
 * the capacity of a single MemorySegment, if there are too many fields, we suggest that user set a
 * bigger pageSize of MemorySegment.
 *
 * <p>Variable-length part may fall into multiple MemorySegments.
 */
@Internal
public final class BinaryRowData extends BinarySection
        implements BinaryRow, NullAwareGetters, DataSetters {

    private static final long serialVersionUID = 1L;

    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
    private static final long FIRST_BYTE_ZERO = LITTLE_ENDIAN ? ~0xFFL : ~(0xFFL << 56L);
    public static final int HEADER_SIZE_IN_BITS = 8;

    public static final BinaryRowData EMPTY_ROW = new BinaryRowData(0);

    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
    }

    public static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    }

    public static int calculateFixPartSizeInBytes(int arity) {
        return calculateBitSetWidthInBytes(arity) + 8 * arity;
    }

    private final int arity;
    private final int nullBitsSizeInBytes;

    public BinaryRowData(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
    }

    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    public int getFixedLengthPartSize() {
        return nullBitsSizeInBytes + 8 * arity;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    public void setTotalSize(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.bitGet(segments[0], offset, pos + HEADER_SIZE_IN_BITS);
    }

    private void setNotNullAt(int i) {
        assertIndexIsValid(i);
        BinarySegmentUtils.bitUnSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
    }

    @Override
    public void setNullAt(int i) {
        assertIndexIsValid(i);
        BinarySegmentUtils.bitSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
        // We must set the fixed length part zero.
        // 1.Only int/long/boolean...(Fix length type) will invoke this setNullAt.
        // 2.Set to zero in order to equals and hash operation bytes calculation.
        segments[0].putLong(getFieldOffset(i), 0);
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putInt(getFieldOffset(pos), value);
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putLong(getFieldOffset(pos), value);
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putDouble(getFieldOffset(pos), value);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // compact format
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // zero-out the bytes
            BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
            BinarySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // keep the offset for future update
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // Write the bytes to the variable length portion.
                BinarySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    @Override
    public void setTimestampLtz(int pos, TimestampLtz value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampLtz.isCompact(precision)) {
            setLong(pos, value.getEpochMillisecond());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to the variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getEpochMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    public void setTimestampNtz(int pos, TimestampNtz value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampNtz.isCompact(precision)) {
            setLong(pos, value.getMillisecond());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to the variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putBoolean(getFieldOffset(pos), value);
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putShort(getFieldOffset(pos), value);
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].put(getFieldOffset(pos), value);
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putFloat(getFieldOffset(pos), value);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getBoolean(getFieldOffset(pos));
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return segments[0].get(getFieldOffset(pos));
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getShort(getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getInt(getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getLong(getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getFloat(getFieldOffset(pos));
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getDouble(getFieldOffset(pos));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    segments[0].getLong(getFieldOffset(pos)), precision, scale);
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndSize = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readDecimalData(
                segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(segments[0].getLong(getFieldOffset(pos)));
        }
        final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readTimestampLtzData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(segments[0].getLong(fieldOffset));
        }
        final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readTimestampNtzData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public byte[] getBytes(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    /** The bit is 1 when the field is null. Default is 0. */
    public boolean anyNull() {
        // Skip the header.
        if ((segments[0].getLong(0) & FIRST_BYTE_ZERO) != 0) {
            return true;
        }
        for (int i = 8; i < nullBitsSizeInBytes; i += 8) {
            if (segments[0].getLong(i) != 0) {
                return true;
            }
        }
        return false;
    }

    public boolean anyNull(int[] fields) {
        for (int field : fields) {
            if (isNullAt(field)) {
                return true;
            }
        }
        return false;
    }

    public BinaryRowData copy() {
        return copy(new BinaryRowData(arity));
    }

    public BinaryRowData copy(BinaryRowData reuse) {
        return copyInternal(reuse);
    }

    private BinaryRowData copyInternal(BinaryRowData reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    public void clear() {
        segments = null;
        offset = 0;
        sizeInBytes = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // both BinaryRow and NestedRow have the same memory format
        if (!(o instanceof BinaryRow)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    public static BinaryRowData singleColumn(@Nullable Integer i) {
        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (i == null) {
            writer.setNullAt(0);
        } else {
            writer.writeInt(0, i);
        }
        writer.complete();
        return row;
    }

    public static BinaryRowData singleColumn(@Nullable String string) {
        BinaryString binaryString = string == null ? null : BinaryString.fromString(string);
        return singleColumn(binaryString);
    }

    public static BinaryRowData singleColumn(@Nullable BinaryString string) {
        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (string == null) {
            writer.setNullAt(0);
        } else {
            writer.writeString(0, string);
        }
        writer.complete();
        return row;
    }

    /**
     * If it is a fixed-length field, we can call this BinaryRowData's setXX method for in-place
     * updates. If it is variable-length field, can't use this method, because the underlying data
     * is stored continuously.
     */
    public static boolean isInFixedLengthPart(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            case DECIMAL:
                return Decimal.isCompact(((DecimalType) type).getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampLtz.isCompact(((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampNtz.isCompact(((LocalZonedTimestampType) type).getPrecision());
            default:
                return false;
        }
    }

    @Override
    public void copyTo(byte[] dst, int dstOffset) {
        if (segments.length == 1) {
            segments[0].get(offset, dst, dstOffset, sizeInBytes);
        } else {
            BinarySegmentUtils.copyToBytes(segments, offset, dst, dstOffset, sizeInBytes);
        }
    }
}
