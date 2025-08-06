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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.BinarySegmentUtils;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.MurmurHashUtils;

/**
 * An implementation of {@link InternalRow} which is backed by {@link MemorySegment} instead of
 * Object. Data are stored in this row with a binary format, but unlike {@link IndexedRow}, it does
 * not have the ability of random access.
 *
 * <p>In order to reduce the storage space, the data is stored together in a compact way. Data
 * doesn't reserve space. It takes up as much space as it should. Int and Long are coded by a
 * variable length (See {@link CompactedRowWriter}).
 *
 * <p>Also has null bit set part, to null values, only bit will be set on the head, and there is no
 * content in the data part. For example:
 *
 * <ul>
 *   <li>Row(1, 2) will stored as 4 bytes: ROW_KIND + NULL_BIT_SET + 1 + 2.
 *   <li>Row(null, 2) will stored as 3 bytes: ROW_KIND + NULL_BIT_SET(First bit is 1) + 2.
 * </ul>
 *
 * <p>In order to be read correctly, when reading occurs (lazy), deserialization will be triggered
 * to generate a {@link GenericRow}.
 *
 * <p>Tradeoff: Sacrifice cpu for space. As long as there is a read, all fields are accessed.
 */
public class CompactedRow implements BinaryRow {

    private final int arity;
    private MemorySegment segment;
    private MemorySegment[] segments;
    private int offset;
    private int sizeInBytes;

    // For decode
    private boolean decoded;
    private GenericRow decodedRow;
    private CompactedRowReader reader;
    private final CompactedRowDeserializer deserializer;

    public CompactedRow(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public CompactedRow(DataType[] types) {
        this(types.length, new CompactedRowDeserializer(types));
    }

    public CompactedRow(int arity, CompactedRowDeserializer deserializer) {
        this.arity = arity;
        this.deserializer = deserializer;
    }

    public static CompactedRow from(
            DataType[] dataTypes, byte[] dataBytes, CompactedRowDeserializer compactedDecoder) {
        MemorySegment memorySegment = MemorySegment.wrap(dataBytes);
        CompactedRow compactedRow = new CompactedRow(dataTypes.length, compactedDecoder);
        compactedRow.pointTo(memorySegment, 0, memorySegment.size());
        return compactedRow;
    }

    /**
     * Copies the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @param dst The memory into which the bytes will be copied.
     * @param dstOffset The copying offset in the destination memory.
     * @throws IndexOutOfBoundsException Thrown, if too large that the bytes of the row exceed the
     *     amount of memory between the dstOffset and the dst array's end.
     */
    @Override
    public void copyTo(byte[] dst, int dstOffset) {
        segment.get(offset, dst, dstOffset, sizeInBytes);
    }

    @Override
    public CompactedRow copy() {
        return copy(new CompactedRow(arity, deserializer));
    }

    public CompactedRow copy(CompactedRow from) {
        byte[] newBuffer = new byte[sizeInBytes];
        segment.get(offset, newBuffer, 0, sizeInBytes);
        from.pointTo(MemorySegment.wrap(newBuffer), 0, sizeInBytes);
        return from;
    }

    public void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.segments = new MemorySegment[] {segment};
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.decoded = false;
    }

    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        this.segment = segments[0];
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.decoded = false;
    }

    @Override
    public MemorySegment[] getSegments() {
        return segments;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public MemorySegment getSegment() {
        return segment;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    @VisibleForTesting
    public InternalRow decodedRow(int pos) {
        if (!decoded) {
            deserialize(pos);
        }
        return decodedRow;
    }

    private void ensureReaderInitialized(int pos) {
        if (reader == null) {
            reader = new CompactedRowReader(deserializer.getTypes());
        }
    }

    private void deserialize(int pos) {
        if (decodedRow == null) {
            decodedRow = new GenericRow(arity);
        }
        ensureReaderInitialized(pos);
        reader.pointTo(segment, offset, sizeInBytes);
        deserializer.deserialize(reader, decodedRow);
        decoded = true;
        // for GC friendly
        reader = null;
    }

    @Override
    public boolean isNullAt(int pos) {
        return BinarySegmentUtils.bitGet(segment, offset, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return decodedRow(pos).getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return decodedRow(pos).getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return decodedRow(pos).getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return decodedRow(pos).getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return decodedRow(pos).getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return decodedRow(pos).getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return decodedRow(pos).getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return decodedRow(pos).getChar(pos, length);
    }

    @Override
    public BinaryString getString(int pos) {
        return decodedRow(pos).getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return decodedRow(pos).getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return decodedRow(pos).getTimestampNtz(pos, precision);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return decodedRow(pos).getTimestampLtz(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return decodedRow(pos).getBinary(pos, length);
    }

    @Override
    public byte[] getBytes(int pos) {
        return decodedRow(pos).getBytes(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return decodedRow(pos).getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return decodedRow(pos).getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (decoded && decodedRow != null) {
            return decodedRow.getRow(pos, numFields);
        }
        ensureReaderInitialized(pos);
        reader.pointTo(segment, offset, sizeInBytes);
        return reader.readRow();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CompactedRow)) {
            return false;
        }
        CompactedRow that = (CompactedRow) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }
}
