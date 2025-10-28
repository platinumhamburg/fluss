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
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.MurmurHashUtils;
import org.apache.fluss.utils.crc.Crc32C;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.COMMIT_TIMESTAMP_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.extendPropertiesDataOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.extendPropertiesLengthOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.leaderEpochOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.writeClientIdOffset;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * LogRecordBatch implementation for different magic version.
 *
 * <p>To learn more about the recordBatch format, see {@link LogRecordBatchFormat}. Supported
 * recordBatch format:
 *
 * <ul>
 *   <li>V0 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V0}
 *   <li>V1 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V1}
 *   <li>V3 => {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V3}
 * </ul>
 *
 * @since 0.1
 */
// TODO rename to MemoryLogRecordBatch
@PublicEvolving
public class DefaultLogRecordBatch implements LogRecordBatch {
    public static final byte APPEND_ONLY_FLAG_MASK = 0x01;

    private MemorySegment segment;
    private int position;
    private byte magic;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
        this.magic = segment.get(position + MAGIC_OFFSET);
    }

    public void setBaseLogOffset(long baseLogOffset) {
        segment.putLong(position + BASE_OFFSET_OFFSET, baseLogOffset);
    }

    @Override
    public byte magic() {
        return magic;
    }

    @Override
    public long commitTimestamp() {
        return segment.getLong(position + COMMIT_TIMESTAMP_OFFSET);
    }

    public void setCommitTimestamp(long timestamp) {
        segment.putLong(position + COMMIT_TIMESTAMP_OFFSET, timestamp);
    }

    public void setLeaderEpoch(int leaderEpoch) {
        if (magic >= LOG_MAGIC_VALUE_V1) {
            segment.putInt(position + leaderEpochOffset(magic), leaderEpoch);
        } else {
            throw new UnsupportedOperationException(
                    "Set leader epoch is not supported for magic v" + magic + " record batch");
        }
    }

    @Override
    public long writerId() {
        return segment.getLong(position + writeClientIdOffset(magic));
    }

    @Override
    public int batchSequence() {
        return segment.getInt(position + batchSequenceOffset(magic));
    }

    @Override
    public int leaderEpoch() {
        if (magic >= LOG_MAGIC_VALUE_V1) {
            return segment.getInt(position + leaderEpochOffset(magic));
        } else {
            return NO_LEADER_EPOCH;
        }
    }

    @Override
    public void ensureValid() {
        int sizeInBytes = sizeInBytes();
        if (sizeInBytes < recordBatchHeaderSize(magic)) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + recordBatchHeaderSize(magic)
                            + ")");
        }

        if (!isValid()) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (stored crc = "
                            + checksum()
                            + ", computed crc = "
                            + computeChecksum()
                            + ")");
        }
    }

    @Override
    public boolean isValid() {
        return sizeInBytes() >= recordBatchHeaderSize(magic) && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        ByteBuffer buffer = segment.wrap(position, sizeInBytes());
        int schemaIdOffset = schemaIdOffset(magic);
        return Crc32C.compute(buffer, schemaIdOffset, sizeInBytes() - schemaIdOffset);
    }

    private byte attributes() {
        // note we're not using the byte of attributes now.
        return segment.get(attributeOffset(magic) + position);
    }

    @Override
    public long nextLogOffset() {
        return lastLogOffset() + 1;
    }

    @Override
    public long checksum() {
        return segment.getUnsignedInt(crcOffset(magic) + position);
    }

    @Override
    public short schemaId() {
        return segment.getShort(schemaIdOffset(magic) + position);
    }

    @Override
    public long baseLogOffset() {
        return segment.getLong(BASE_OFFSET_OFFSET + position);
    }

    @Override
    public long lastLogOffset() {
        return baseLogOffset() + lastOffsetDelta();
    }

    private int lastOffsetDelta() {
        return segment.getInt(lastOffsetDeltaOffset(magic) + position);
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + segment.getInt(LENGTH_OFFSET + position);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(position + recordsCountOffset(magic));
    }

    /**
     * Returns the extend properties length for V3 format.
     *
     * @return the length of extend properties data, 0 for non-V3 formats
     */
    public int getExtendPropertiesLength() {
        if (magic == LOG_MAGIC_VALUE_V3) {
            return segment.getInt(position + extendPropertiesLengthOffset(magic));
        }
        return 0;
    }

    /**
     * Returns the extend properties data for V3 format.
     *
     * @return the extend properties data as byte array, empty for non-V3 formats
     */
    public byte[] getExtendPropertiesData() {
        if (magic == LOG_MAGIC_VALUE_V3) {
            int length = getExtendPropertiesLength();
            if (length > 0) {
                byte[] data = new byte[length];
                int dataOffset = position + extendPropertiesDataOffset(magic);
                segment.get(dataOffset, data);
                return data;
            }
        }
        return new byte[0];
    }

    /** Returns the start offset of records data, handling V3 format with extend properties. */
    private int getRecordsStartOffset() {
        if (magic == LOG_MAGIC_VALUE_V3) {
            int extendPropertiesLength = getExtendPropertiesLength();
            return position + recordBatchHeaderSize(magic) + extendPropertiesLength;
        } else {
            return position + recordBatchHeaderSize(magic);
        }
    }

    @Override
    public CloseableIterator<LogRecord> records(ReadContext context) {
        if (getRecordCount() == 0) {
            return CloseableIterator.emptyIterator();
        }

        int schemaId = schemaId();
        long timestamp = commitTimestamp();
        LogFormat logFormat = context.getLogFormat();
        RowType rowType = context.getRowType(schemaId);
        switch (logFormat) {
            case ARROW:
                return columnRecordIterator(
                        rowType,
                        context.getVectorSchemaRoot(schemaId),
                        context.getBufferAllocator(),
                        timestamp);
            case INDEXED:
                return rowRecordIterator(rowType, timestamp);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
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

        DefaultLogRecordBatch that = (DefaultLogRecordBatch) o;
        int sizeInBytes = sizeInBytes();
        return sizeInBytes == that.sizeInBytes()
                && segment.equalTo(that.segment, position, that.position, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, position, sizeInBytes());
    }

    private CloseableIterator<LogRecord> rowRecordIterator(RowType rowType, long timestamp) {
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        return new LogRecordIterator() {
            int position = getRecordsStartOffset();
            int rowId = 0;

            @Override
            protected LogRecord readNext(long baseOffset) {
                IndexedLogRecord logRecord =
                        IndexedLogRecord.readFrom(
                                segment, position, baseOffset + rowId, timestamp, fieldTypes);
                rowId++;
                position += logRecord.getSizeInBytes();
                return logRecord;
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {}
        };
    }

    private CloseableIterator<LogRecord> columnRecordIterator(
            RowType rowType, VectorSchemaRoot root, BufferAllocator allocator, long timestamp) {
        boolean isAppendOnly = (attributes() & APPEND_ONLY_FLAG_MASK) > 0;
        if (isAppendOnly) {
            // append only batch, no change type vector,
            // the start of the arrow data is the beginning of the batch records
            int recordsStartOffset = getRecordsStartOffset();
            int arrowOffset = recordsStartOffset;
            int arrowLength = sizeInBytes() - (recordsStartOffset - position);
            ArrowReader reader =
                    ArrowUtils.createArrowReader(
                            segment, arrowOffset, arrowLength, root, allocator, rowType);
            return new ArrowLogRecordIterator(reader, timestamp) {
                @Override
                protected ChangeType getChangeType(int rowId) {
                    return ChangeType.APPEND_ONLY;
                }
            };
        } else {
            // with change type, decode the change type vector first,
            // the arrow data starts after the change type vector
            int changeTypeOffset = getChangeTypeOffset();
            ChangeTypeVector changeTypeVector =
                    new ChangeTypeVector(segment, changeTypeOffset, getRecordCount());
            int arrowOffset = changeTypeOffset + changeTypeVector.sizeInBytes();
            int arrowLength = sizeInBytes() - (arrowOffset - position);
            ArrowReader reader =
                    ArrowUtils.createArrowReader(
                            segment, arrowOffset, arrowLength, root, allocator, rowType);
            return new ArrowLogRecordIterator(reader, timestamp) {
                @Override
                protected ChangeType getChangeType(int rowId) {
                    return changeTypeVector.getChangeType(rowId);
                }
            };
        }
    }

    /** Returns the offset of the change type vector, handling V3 format with extend properties. */
    private int getChangeTypeOffset() {
        if (magic == LOG_MAGIC_VALUE_V3) {
            int extendPropertiesLength = getExtendPropertiesLength();
            return position + recordBatchHeaderSize(magic) + extendPropertiesLength;
        } else {
            return position + arrowChangeTypeOffset(magic);
        }
    }

    /** The basic implementation for Arrow log record iterator. */
    private abstract class ArrowLogRecordIterator extends LogRecordIterator {
        private final ArrowReader reader;
        private final long timestamp;
        private int rowId = 0;

        private ArrowLogRecordIterator(ArrowReader reader, long timestamp) {
            this.reader = reader;
            this.timestamp = timestamp;
        }

        protected abstract ChangeType getChangeType(int rowId);

        @Override
        public boolean hasNext() {
            return rowId < reader.getRowCount();
        }

        @Override
        protected LogRecord readNext(long baseOffset) {
            LogRecord record =
                    new GenericRecord(
                            baseOffset + rowId,
                            timestamp,
                            getChangeType(rowId),
                            reader.read(rowId));
            rowId++;
            return record;
        }

        @Override
        protected boolean ensureNoneRemaining() {
            return true;
        }

        @Override
        public void close() {
            reader.close();
        }
    }

    /** Default log record iterator. */
    private abstract class LogRecordIterator implements CloseableIterator<LogRecord> {
        private final long baseOffset;
        private final int numRecords;
        private int readRecords = 0;

        public LogRecordIterator() {
            this.baseOffset = baseLogOffset();
            int numRecords = getRecordCount();
            if (numRecords < 0) {
                throw new IllegalArgumentException(
                        "Found invalid record count "
                                + numRecords
                                + " in magic v"
                                + magic
                                + " batch");
            }
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public LogRecord next() {
            if (readRecords >= numRecords) {
                throw new NoSuchElementException();
            }

            readRecords++;
            LogRecord rec = readNext(baseOffset);
            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining()) {
                    throw new IllegalArgumentException(
                            "Incorrect declared batch size, records still remaining in file");
                }
            }
            return rec;
        }

        protected abstract LogRecord readNext(long baseOffset);

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
