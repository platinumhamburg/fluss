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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.utils.MurmurHashUtils;
import org.apache.fluss.utils.VarLengthUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is an immutable kv record. Different from {@link IndexedLogRecord}, it isn't designed
 * for persistence. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Flags => int8 (present only when batch attribute HAS_RECORD_FLAGS is set; bit 0 =
 *       isRetract: 1 means retract record, 0 means normal record)
 *   <li>KeyLength => unsigned varint
 *   <li>Key => bytes
 *   <li>Row => {@link BinaryRow}
 * </ul>
 *
 * <p>When the row is null, no any byte will be remained after Key.
 *
 * @since 0.1
 */
@PublicEvolving
public class DefaultKvRecord implements KvRecord {

    static final int LENGTH_LENGTH = 4;
    static final int FLAGS_LENGTH = 1;
    /** Bit 0 of the flags byte: indicates this is a retract record. */
    static final byte RETRACT_FLAG = 0x01;

    private final RowDecoder rowDecoder;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    private ByteBuffer key;
    private BinaryRow value;
    private boolean retract;

    private DefaultKvRecord(RowDecoder rowDecoder) {
        this.rowDecoder = rowDecoder;
    }

    private void pointTo(
            MemorySegment segment, int offset, int sizeInBytes, boolean hasRecordFlags) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        try {
            readKeyAndRow(hasRecordFlags);
        } catch (IOException e) {
            throw new InvalidRecordException("Found invalid kv record structure.", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKvRecord that = (DefaultKvRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    /**
     * Convenience overload that writes with record flags enabled (matching {@link
     * KvRecordBatchEncoding#WITH_RECORD_FLAGS}).
     */
    public static int writeTo(OutputView outputView, byte[] key, @Nullable BinaryRow row)
            throws IOException {
        return writeTo(outputView, key, row, false, true);
    }

    public static int writeTo(
            OutputView outputView,
            byte[] key,
            @Nullable BinaryRow row,
            boolean isRetract,
            boolean hasRecordFlags)
            throws IOException {
        int sizeInBytes = sizeWithoutLength(key, row, hasRecordFlags);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        if (hasRecordFlags) {
            // write flags byte: bit 0 = isRetract
            outputView.writeByte(isRetract ? RETRACT_FLAG : 0);
        }

        // write key length, unsigned var int;
        VarLengthUtils.writeUnsignedVarInt(key.length, outputView);
        // write the real key
        outputView.write(key);

        if (row != null) {
            // write internal row, which is the value.
            serializeInternalRow(outputView, row);
        }
        return sizeInBytes + LENGTH_LENGTH;
    }

    public static KvRecord readFrom(
            MemorySegment segment,
            int position,
            short schemaId,
            KvRecordBatch.ReadContext readContext,
            boolean hasRecordFlags) {
        int sizeInBytes = segment.getInt(position);
        DefaultKvRecord kvRecord = new DefaultKvRecord(readContext.getRowDecoder(schemaId));
        kvRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH, hasRecordFlags);
        return kvRecord;
    }

    /** Calculate the size of the kv record write to batch, including {@link #LENGTH_LENGTH}. */
    public static int sizeOf(byte[] key, @Nullable BinaryRow row, boolean hasRecordFlags) {
        return sizeWithoutLength(key, row, hasRecordFlags) + LENGTH_LENGTH;
    }

    /**
     * Convenience overload that computes size with record flags included (matching {@link
     * KvRecordBatchEncoding#WITH_RECORD_FLAGS}).
     */
    public static int sizeOf(byte[] key, @Nullable BinaryRow row) {
        return sizeOf(key, row, true);
    }

    private static int sizeWithoutLength(
            byte[] key, @Nullable BinaryRow row, boolean hasRecordFlags) {
        return (hasRecordFlags ? FLAGS_LENGTH : 0)
                + VarLengthUtils.sizeOfUnsignedVarInt(key.length)
                + key.length
                + (row == null ? 0 : row.getSizeInBytes());
    }

    private void readKeyAndRow(boolean hasRecordFlags) throws IOException {
        int currentOffset;
        if (hasRecordFlags) {
            // new format: read flags byte (immediately after the length field)
            byte flags = segment.get(offset + LENGTH_LENGTH);
            retract = (flags & RETRACT_FLAG) != 0;
            currentOffset = offset + LENGTH_LENGTH + FLAGS_LENGTH;
        } else {
            // legacy format: no flags byte, retract is always false
            retract = false;
            currentOffset = offset + LENGTH_LENGTH;
        }
        // now, read key;
        // read the length of key size
        int keyLength = VarLengthUtils.readUnsignedVarInt(segment, currentOffset);
        int bytesForKeyLength = VarLengthUtils.sizeOfUnsignedVarInt(keyLength);
        // seek to the position of real key
        currentOffset += bytesForKeyLength;
        key = segment.wrap(currentOffset, keyLength);

        // seek to the position of value
        currentOffset += keyLength;
        // now, read value;
        int nonValueLength = currentOffset - this.offset;
        int valueLength = sizeInBytes - nonValueLength;
        if (valueLength == 0) {
            value = null;
        } else {
            value = rowDecoder.decode(segment, currentOffset, valueLength);
        }
    }

    @Override
    public ByteBuffer getKey() {
        return key;
    }

    @Override
    public @Nullable BinaryRow getRow() {
        return value;
    }

    @Override
    public boolean isRetract() {
        return retract;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    private static void serializeInternalRow(OutputView outputView, InternalRow internalRow)
            throws IOException {
        if (internalRow instanceof IndexedRow) {
            IndexedRow indexedRow = (IndexedRow) internalRow;
            IndexedRowWriter.serializeIndexedRow(indexedRow, outputView);
        } else if (internalRow instanceof CompactedRow) {
            CompactedRow compactedRow = (CompactedRow) internalRow;
            CompactedRowWriter.serializeCompactedRow(compactedRow, outputView);
        } else {
            throw new IllegalArgumentException(
                    "No such internal row serializer for: "
                            + internalRow.getClass().getSimpleName());
        }
    }
}
