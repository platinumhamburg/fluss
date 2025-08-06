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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinarySegmentUtils;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.io.Serializable;

import static com.alibaba.fluss.row.BinaryRow.calculateBitSetWidthInBytes;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;

/**
 * Deserializes a {@link InternalRow} in an decoded way. In order to save more space, int and long
 * are written in variable length (lengths of strings, binaries, etc. are also written in this way).
 *
 * <p>NOTE: read from byte[] instead of {@link MemorySegment} can be a bit more efficient.
 *
 * <p>See {@link CompactedRowWriter}.
 */
public class CompactedRowReader {

    private final int fieldCount;
    // Including null bits.
    private final int headerSizeInBytes;
    private final DataType[] types;

    private MemorySegment segment;
    private MemorySegment[] segments;
    private int offset;
    private int position;
    private int limit;

    public CompactedRowReader(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public CompactedRowReader(DataType[] types) {
        this.types = types;
        this.fieldCount = types.length;
        this.headerSizeInBytes = calculateBitSetWidthInBytes(fieldCount);
    }

    public CompactedRowReader(int fieldCount) {
        this.types = null;
        this.fieldCount = fieldCount;
        this.headerSizeInBytes = calculateBitSetWidthInBytes(fieldCount);
    }

    public void pointTo(MemorySegment segment, int offset, int length) {
        pointTo(segment, offset, offset + headerSizeInBytes, offset + length);
    }

    void pointTo(MemorySegment segment, int offset, int position, int limit) {
        if (segment != this.segment) {
            this.segment = segment;
            this.segments = new MemorySegment[] {segment};
        }
        this.offset = offset;
        this.position = position;
        this.limit = limit;
    }

    public boolean isNullAt(int pos) {
        return BinarySegmentUtils.bitGet(segment, offset, pos);
    }

    public boolean readBoolean() {
        return segment.getBoolean(position++);
    }

    public byte readByte() {
        return segment.get(position++);
    }

    public short readShort() {
        short value = segment.getShort(position);
        position += 2;
        return value;
    }

    /** See {@link #readLong()}. */
    public int readInt() {
        int tempPos = position;
        int x;
        if ((x = segment.get(tempPos++)) >= 0) {
            position = tempPos;
            return x;
        } else if ((x ^= (segment.get(tempPos++) << 7)) < 0) {
            x ^= (~0 << 7);
        } else if ((x ^= (segment.get(tempPos++) << 14)) >= 0) {
            x ^= (~0 << 7) ^ (~0 << 14);
        } else if ((x ^= (segment.get(tempPos++) << 21)) < 0) {
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
        } else {
            int y = segment.get(tempPos++);
            x ^= y << 28;
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
        }
        position = tempPos;
        return x;
    }

    public long readLong() {
        // Influenced by Protobuf CodedInputStream.
        // Implementation notes:
        //
        // Optimized for one-byte values, expected to be common.
        // The particular code below was selected from various candidates
        // empirically.
        //
        // Sign extension of (signed) Java bytes is usually a nuisance, but
        // we exploit it here to more easily obtain the sign of bytes read.
        // Instead of cleaning up the sign extension bits by masking eagerly,
        // we delay until we find the final (positive) byte, when we clear all
        // accumulated bits with one xor.  We depend on javac to constant fold.
        fastPath:
        {
            int tempPos = position;
            if (limit == tempPos) {
                break fastPath; // illegal, throws exception
            }
            final MemorySegment segment = this.segment;
            long x;
            int y;
            if ((y = segment.get(tempPos++)) >= 0) {
                position = tempPos;
                return y;
            } else if (limit - tempPos < 9) {
                break fastPath;
            } else if ((y ^= (segment.get(tempPos++) << 7)) < 0) {
                x = y ^ (~0 << 7);
            } else if ((y ^= (segment.get(tempPos++) << 14)) >= 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14));
            } else if ((y ^= (segment.get(tempPos++) << 21)) < 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
            } else if ((x = y ^ ((long) segment.get(tempPos++) << 28)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
            } else if ((x ^= ((long) segment.get(tempPos++) << 35)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
            } else if ((x ^= ((long) segment.get(tempPos++) << 42)) >= 0L) {
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42);
            } else if ((x ^= ((long) segment.get(tempPos++) << 49)) < 0L) {
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42)
                                ^ (~0L << 49);
            } else {
                x ^= ((long) segment.get(tempPos++) << 56);
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42)
                                ^ (~0L << 49)
                                ^ (~0L << 56);
                if (x < 0L) {
                    if (segment.get(tempPos++) < 0L) {
                        break fastPath; // illegal, throws exception
                    }
                }
            }
            position = tempPos;
            return x;
        }
        return readLongSlowPath();
    }

    public float readFloat() {
        float value = segment.getFloat(position);
        position += 4;
        return value;
    }

    public double readDouble() {
        double value = segment.getDouble(position);
        position += 8;
        return value;
    }

    public BinaryString readString() {
        int length = readInt();
        BinaryString string = BinaryString.fromAddress(segments, position, length);
        position += length;
        return string;
    }

    public Decimal readDecimal(int precision, int scale) {
        return Decimal.isCompact(precision)
                ? Decimal.fromUnscaledLong(readLong(), precision, scale)
                : Decimal.fromUnscaledBytes(readBytes(), precision, scale);
    }

    public TimestampLtz readTimestampLtz(int precision) {
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(readLong());
        }
        long milliseconds = readLong();
        int nanosOfMillisecond = readInt();
        return TimestampLtz.fromEpochMillis(milliseconds, nanosOfMillisecond);
    }

    public TimestampNtz readTimestampNtz(int precision) {
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(readLong());
        }
        long milliseconds = readLong();
        int nanosOfMillisecond = readInt();
        return TimestampNtz.fromMillis(milliseconds, nanosOfMillisecond);
    }

    public byte[] readBytes() {
        int length = readInt();
        return readBytesInternal(length);
    }

    // ----------------------- internal methods -------------------------------
    byte[] readBytesInternal(int length) {
        byte[] bytes = new byte[length];
        segment.get(position, bytes, 0, length);
        position += length;
        return bytes;
    }

    private long readLongSlowPath() {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            final byte b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new RuntimeException("Invalid input stream.");
    }

    /**
     * Creates an accessor for reading elements.
     *
     * @param fieldType the element type of the row
     */
    static FieldReader createFieldReader(DataType fieldType) {
        final FieldReader fieldReader;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                fieldReader = (reader, pos) -> reader.readString();
                break;
            case BOOLEAN:
                fieldReader = (reader, pos) -> reader.readBoolean();
                break;
            case BINARY:
            case BYTES:
                fieldReader = (reader, pos) -> reader.readBytes();
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldReader = (reader, pos) -> reader.readDecimal(decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldReader = (reader, pos) -> reader.readByte();
                break;
            case SMALLINT:
                fieldReader = (reader, pos) -> reader.readShort();
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldReader = (reader, pos) -> reader.readInt();
                break;
            case BIGINT:
                fieldReader = (reader, pos) -> reader.readLong();
                break;
            case FLOAT:
                fieldReader = (reader, pos) -> reader.readFloat();
                break;
            case DOUBLE:
                fieldReader = (reader, pos) -> reader.readDouble();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestampNtz(timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestampLtz(timestampLtzPrecision);
                break;
            case ARRAY:
                fieldReader = (reader, pos) -> reader.readArray();
                break;
            case MULTISET:
            case MAP:
                fieldReader = (reader, pos) -> reader.readMap();
                break;
            case ROW:
                DataType[] fieldTypes =
                        ((RowType) fieldType).getChildren().toArray(new DataType[0]);
                fieldReader = (reader, pos) -> reader.readRow(fieldTypes);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for IndexedRow: " + fieldType);
        }
        if (!fieldType.isNullable()) {
            return fieldReader;
        }
        return (reader, pos) -> {
            if (reader.isNullAt(pos)) {
                return null;
            }
            return fieldReader.readField(reader, pos);
        };
    }

    public InternalArray readArray() {
        int length = readInt();
        InternalArray array = BinarySegmentUtils.readArrayData(segments, position, length);
        position += length;
        return array;
    }

    public InternalMap readMap() {
        int length = readInt();
        InternalMap map = BinarySegmentUtils.readMapData(segments, position, length);
        position += length;
        return map;
    }

    public InternalRow readRow() {
        return readRow(types);
    }

    public InternalRow readRow(DataType[] types) {
        int length = readInt();
        InternalRow row;
        row =
                BinarySegmentUtils.readCompactedRowData(
                        segments, fieldCount, position, length, types);
        position += length;
        return row;
    }

    /**
     * Accessor for reading the field of a row during runtime.
     *
     * @see #createFieldReader(DataType)
     */
    interface FieldReader extends Serializable {
        Object readField(CompactedRowReader reader, int pos);
    }
}
