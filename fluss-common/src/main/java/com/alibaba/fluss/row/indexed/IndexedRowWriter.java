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

package com.alibaba.fluss.row.indexed;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentWritable;
import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.BinaryArray;
import com.alibaba.fluss.row.BinaryMap;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.BinarySegmentUtils;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.BinaryWriter;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.serializer.InternalArraySerializer;
import com.alibaba.fluss.row.serializer.InternalMapSerializer;
import com.alibaba.fluss.row.serializer.InternalRowSerializer;
import com.alibaba.fluss.row.serializer.InternalSerializers;
import com.alibaba.fluss.row.serializer.Serializer;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.UnsafeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

import static com.alibaba.fluss.row.BinaryRow.calculateBitSetWidthInBytes;
import static com.alibaba.fluss.types.DataTypeChecks.getLength;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;

/** Writer for {@link IndexedRow}. */
@Internal
public class IndexedRowWriter extends OutputStream implements BinaryWriter, MemorySegmentWritable {

    private final int nullBitsSizeInBytes;
    private final int variableColumnLengthListInBytes;
    // nullBitSet size + variable column length list size.
    private final int headerSizeInBytes;

    private byte[] buffer;
    private MemorySegment segment;
    private int position;
    private int variableLengthPosition;

    public IndexedRowWriter(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public IndexedRowWriter(DataType[] types) {
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(types.length);
        this.variableColumnLengthListInBytes =
                IndexedRow.calculateVariableColumnLengthListSize(types);
        this.headerSizeInBytes = nullBitsSizeInBytes + variableColumnLengthListInBytes;
        this.position = headerSizeInBytes;
        // begin from nullBitsSizeInBytes.
        this.variableLengthPosition = nullBitsSizeInBytes;

        setBuffer(new byte[Math.max(64, headerSizeInBytes)]);
    }

    public void reset() {
        this.position = headerSizeInBytes;
        this.variableLengthPosition = nullBitsSizeInBytes;
        for (int i = 0; i < headerSizeInBytes; i++) {
            buffer[i] = 0;
        }
    }

    /** Default not null. */
    public void setNullAt(int pos) {
        UnsafeUtils.bitSet(buffer, 0, pos);
    }

    public void writeBoolean(boolean value) {
        ensureCapacity(1);
        UnsafeUtils.putBoolean(buffer, position++, value);
    }

    public void writeByte(byte value) {
        ensureCapacity(1);
        UnsafeUtils.putByte(buffer, position++, value);
    }

    public void writeShort(short value) {
        ensureCapacity(2);
        UnsafeUtils.putShort(buffer, position, value);
        position += 2;
    }

    public void writeInt(int value) {
        ensureCapacity(4);
        UnsafeUtils.putInt(buffer, position, value);
        position += 4;
    }

    public void writeLong(long value) {
        ensureCapacity(8);
        UnsafeUtils.putLong(buffer, position, value);
        position += 8;
    }

    public void writeFloat(float value) {
        ensureCapacity(4);
        UnsafeUtils.putFloat(buffer, position, value);
        position += 4;
    }

    public void writeDouble(double value) {
        ensureCapacity(8);
        UnsafeUtils.putDouble(buffer, position, value);
        position += 8;
    }

    public void writeChar(BinaryString value, int length) {
        writeChar(value.toString(), length);
    }

    public void writeString(BinaryString value) {
        int length = value.getSizeInBytes();
        // write var length in variable column length list.
        writeVarLengthToVarLengthList(length);
        if (value.getSegments() == null) {
            writeChar(value, length);
        } else {
            int offset = value.getOffset();
            MemorySegment segment0 = value.getSegments()[0];
            if (offset + length <= segment0.size()) {
                write(segment0, offset, length);
            } else {
                byte[] bytes = BinarySegmentUtils.allocateReuseBytes(length);
                BinarySegmentUtils.copyToBytes(value.getSegments(), offset, bytes, 0, length);
                write(bytes, 0, length);
            }
        }
    }

    private void writeChar(String string, int length) {
        byte[] bytes = new byte[length];
        BinaryString.encodeUTF8(string, bytes);
        write(bytes, 0, length);
    }

    public void writeBinary(byte[] value, int length) {
        if (value.length > length) {
            throw new IllegalArgumentException();
        }
        byte[] newByte = new byte[length];
        System.arraycopy(value, 0, newByte, 0, value.length);
        write(newByte, 0, length);
    }

    public void writeBytes(byte[] value) {
        writeVarLengthToVarLengthList(value.length);
        write(value, 0, value.length);
    }

    public void writeDecimal(Decimal value, int precision) {
        if (Decimal.isCompact(precision)) {
            writeLong(value.toUnscaledLong());
        } else {
            writeBytes(value.toUnscaledBytes());
        }
    }

    public void writeTimestampNtz(TimestampNtz value, int precision) {
        if (TimestampNtz.isCompact(precision)) {
            writeLong(value.getMillisecond());
        } else {
            writeLong(value.getMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    public void writeTimestampLtz(TimestampLtz value, int precision) {
        if (TimestampLtz.isCompact(precision)) {
            writeLong(value.getEpochMillisecond());
        } else {
            writeLong(value.getEpochMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    public void writeArray(InternalArray value, InternalArraySerializer serializer) {
        BinaryArray binaryArray = serializer.toBinaryArray(value);
        MemorySegment[] segments = binaryArray.getSegments();
        int offset = binaryArray.getOffset();
        int length = binaryArray.getSizeInBytes();

        write(length, segments, offset);
    }

    public void writeMap(InternalMap value, InternalMapSerializer serializer) {
        BinaryMap binaryMap = serializer.toBinaryMap(value);
        MemorySegment[] segments = binaryMap.getSegments();
        int offset = binaryMap.getOffset();
        int length = binaryMap.getSizeInBytes();

        write(length, segments, offset);
    }

    public void writeRow(InternalRow value, InternalRowSerializer serializer) {
        BinaryRow binaryRow = serializer.toBinaryRow(value);
        MemorySegment[] segments = binaryRow.getSegments();
        int offset = binaryRow.getOffset();
        int length = binaryRow.getSizeInBytes();

        write(length, segments, offset);
    }

    @Override
    public void complete() {}

    @Override
    public void write(int b) {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] value, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(value, off, buffer, position, len);
        position += len;
    }

    @Override
    public void write(MemorySegment segment, int off, int len) {
        ensureCapacity(len);
        segment.get(off, this.buffer, this.position, len);
        this.position += len;
    }

    /**
     * write bytes to buffer. Used for complex types such as: array, map, row.
     *
     * @param length in bytes.
     * @param segments memory segments.
     * @param offset offset in memory segment.
     */
    private void write(int length, MemorySegment[] segments, int offset) {
        // write var length in variable column length list.
        writeVarLengthToVarLengthList(length);

        if (offset + length <= segments[0].size()) {
            write(segments[0], offset, length);
        } else {
            byte[] bytes = BinarySegmentUtils.allocateReuseBytes(length);
            BinarySegmentUtils.copyToBytes(segments, offset, bytes, 0, length);
            write(bytes, 0, length);
        }
    }

    public byte[] buffer() {
        return buffer;
    }

    @Override
    public MemorySegment segment() {
        return segment;
    }

    @Override
    public int position() {
        return position;
    }

    // ----------------------- internal methods -------------------------------

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
    }

    private void ensureCapacity(int size) {
        if (buffer.length - position < size) {
            grow(size);
        }
    }

    private void grow(int minCapacityAdd) {
        int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
        setBuffer(Arrays.copyOf(this.buffer, newLen));
    }

    private void writeVarLengthToVarLengthList(int length) {
        if (variableLengthPosition - nullBitsSizeInBytes + 4 > variableColumnLengthListInBytes) {
            throw new IllegalArgumentException();
        }
        UnsafeUtils.putInt(buffer, variableLengthPosition, length);
        variableLengthPosition += 4;
    }

    // ------------------------------------------------------------------------------------------

    public static void serializeIndexedRow(IndexedRow row, OutputView target) throws IOException {
        int sizeInBytes = row.getSizeInBytes();
        if (target instanceof MemorySegmentWritable) {
            ((MemorySegmentWritable) target).write(row.getSegment(), row.getOffset(), sizeInBytes);
        } else {
            byte[] bytes = BinarySegmentUtils.allocateReuseBytes(sizeInBytes);
            row.getSegment().get(row.getOffset(), bytes, 0, sizeInBytes);
            target.write(bytes, 0, sizeInBytes);
        }
    }

    /**
     * Creates an accessor for writing the elements of an indexed row writer during runtime.
     *
     * @param fieldType the field type of the indexed row
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int charLength = getLength(fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeChar((BinaryString) value, charLength);
                break;
            case STRING:
                fieldWriter = (writer, pos, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeBinary((byte[]) value, binaryLength);
                break;
            case BYTES:
                fieldWriter = (writer, pos, value) -> writer.writeBytes((byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeDecimal((Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, pos, value) -> writer.writeByte((byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, pos, value) -> writer.writeShort((short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldWriter = (writer, pos, value) -> writer.writeInt((int) value);
                break;
            case BIGINT:
                fieldWriter = (writer, pos, value) -> writer.writeLong((long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, pos, value) -> writer.writeFloat((float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, pos, value) -> writer.writeDouble((double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampNtz(
                                        (TimestampNtz) value, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampLtz(
                                        (TimestampLtz) value, timestampLtzPrecision);
                break;
            case ARRAY:
                final Serializer<InternalArray> arraySerializer =
                        InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeArray(
                                        (InternalArray) value,
                                        (InternalArraySerializer) arraySerializer);
                break;
            case MULTISET:
            case MAP:
                Serializer<InternalMap> mapSerializer = InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeMap(
                                        (InternalMap) value, (InternalMapSerializer) mapSerializer);
                break;
            case ROW:
                Serializer<InternalRow> rowSerializer = IndexedRowSerializer.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeRow(
                                        (InternalRow) value, (InternalRowSerializer) rowSerializer);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for IndexedRow: " + fieldType);
        }

        if (!fieldType.isNullable()) {
            return fieldWriter;
        }
        return (writer, pos, value) -> {
            if (value == null) {
                writer.setNullAt(pos);
            } else {
                fieldWriter.writeField(writer, pos, value);
            }
        };
    }

    /** Accessor for writing the elements of an indexed row writer during runtime. */
    public interface FieldWriter extends Serializable {
        void writeField(IndexedRowWriter writer, int pos, Object value);
    }
}
