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
import org.apache.fluss.utils.crc.Crc32C;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * KV record batch implementation for magic 1 with a 128-bit writer key and 64-bit cumulative writer
 * progress.
 *
 * <p>The CRC covers the bytes from schema id through the end of the batch, matching the magic-0
 * batch CRC coverage.
 */
@PublicEvolving
public class ProgressKvRecordBatch implements KvRecordBatch {

    static final int LENGTH_LENGTH = 4;
    static final int MAGIC_LENGTH = 1;
    static final int CRC_LENGTH = 4;
    static final int SCHEMA_ID_LENGTH = 2;
    static final int ATTRIBUTES_LENGTH = 1;
    static final int WRITER_KEY_PART_LENGTH = 8;
    static final int WRITER_PROGRESS_LENGTH = 8;
    static final int RECORDS_COUNT_LENGTH = 4;

    static final int LENGTH_OFFSET = 0;
    static final int MAGIC_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int SCHEMA_ID_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTES_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    static final int WRITER_KEY_HIGH_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    static final int WRITER_KEY_LOW_OFFSET = WRITER_KEY_HIGH_OFFSET + WRITER_KEY_PART_LENGTH;
    static final int WRITER_PROGRESS_OFFSET = WRITER_KEY_LOW_OFFSET + WRITER_KEY_PART_LENGTH;
    static final int RECORDS_COUNT_OFFSET = WRITER_PROGRESS_OFFSET + WRITER_PROGRESS_LENGTH;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_HEADER_SIZE = RECORDS_OFFSET;

    public static final int KV_OVERHEAD = LENGTH_LENGTH;

    private MemorySegment segment;
    private int position;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    @Override
    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_HEADER_SIZE && checksum() == computeChecksum();
    }

    @Override
    public void ensureValid() {
        int sizeInBytes = sizeInBytes();
        if (sizeInBytes < RECORD_BATCH_HEADER_SIZE) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + RECORD_BATCH_HEADER_SIZE
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
    public long checksum() {
        return segment.getUnsignedInt(position + CRC_OFFSET);
    }

    @Override
    public short schemaId() {
        return segment.getShort(position + SCHEMA_ID_OFFSET);
    }

    @Override
    public byte magic() {
        return segment.get(position + MAGIC_OFFSET);
    }

    @Override
    public long writerId() {
        throw new UnsupportedOperationException(
                "Cumulative-progress batch has no contiguous-sequence writer id");
    }

    @Override
    public int batchSequence() {
        throw new UnsupportedOperationException(
                "Cumulative-progress batch has no contiguous batch sequence");
    }

    @Override
    public int idempotenceProtocolVersion() {
        return 1;
    }

    @Override
    public WriterKey writerKey() {
        return new WriterKey(
                segment.getLong(position + WRITER_KEY_HIGH_OFFSET),
                segment.getLong(position + WRITER_KEY_LOW_OFFSET));
    }

    @Override
    public long writerProgress() {
        return segment.getLong(position + WRITER_PROGRESS_OFFSET);
    }

    @Override
    public int sizeInBytes() {
        return KV_OVERHEAD + segment.getInt(position + LENGTH_OFFSET);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(position + RECORDS_COUNT_OFFSET);
    }

    @Override
    public Iterable<KvRecord> records(ReadContext readContext) {
        return () -> iterator(readContext);
    }

    private Iterator<KvRecord> iterator(ReadContext readContext) {
        validateRecordCountAndPayloadSize();
        int recordCount = getRecordCount();
        if (recordCount == 0) {
            return Collections.emptyIterator();
        }
        return new Iterator<KvRecord>() {
            private final short schemaId = schemaId();
            private final int batchEnd = position + sizeInBytes();
            private int currentPosition = position + RECORD_BATCH_HEADER_SIZE;
            private int readRecordCount;

            @Override
            public boolean hasNext() {
                return readRecordCount < recordCount;
            }

            @Override
            public KvRecord next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                int recordSize = validateRecordBounds(currentPosition, batchEnd);
                KvRecord kvRecord =
                        DefaultKvRecord.readFrom(segment, currentPosition, schemaId, readContext);
                currentPosition += recordSize;
                readRecordCount++;
                if (readRecordCount == recordCount && currentPosition != batchEnd) {
                    throw new CorruptMessageException(
                            "KV record payload does not end at declared batch end");
                }
                return kvRecord;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    void validateRecordCountAndPayloadSize() {
        int recordCount = getRecordCount();
        if (recordCount < 0) {
            throw new CorruptMessageException(
                    "Cumulative-progress KV batch has negative record count " + recordCount);
        }
        int payloadSize = sizeInBytes() - RECORD_BATCH_HEADER_SIZE;
        long minimumPayloadSize = (long) recordCount * (DefaultKvRecord.LENGTH_LENGTH + 1L);
        if (minimumPayloadSize > payloadSize) {
            throw new CorruptMessageException(
                    "Cumulative-progress KV batch record count "
                            + recordCount
                            + " does not fit payload size "
                            + payloadSize);
        }
        if (recordCount == 0 && payloadSize != 0) {
            throw new CorruptMessageException(
                    "Cumulative-progress KV batch record count 0 does not match payload size "
                            + payloadSize);
        }
    }

    private int validateRecordBounds(int recordPosition, int batchEnd) {
        if (recordPosition > batchEnd - DefaultKvRecord.LENGTH_LENGTH) {
            throw new CorruptMessageException("KV record length crosses declared batch end");
        }
        int recordBodySize = segment.getInt(recordPosition);
        if (recordBodySize < 1) {
            throw new CorruptMessageException("KV record has invalid body size " + recordBodySize);
        }
        long recordEnd = (long) recordPosition + DefaultKvRecord.LENGTH_LENGTH + recordBodySize;
        if (recordEnd > batchEnd) {
            throw new CorruptMessageException("KV record crosses declared batch end");
        }
        validateKeyBounds(recordPosition + DefaultKvRecord.LENGTH_LENGTH, (int) recordEnd);
        return DefaultKvRecord.LENGTH_LENGTH + recordBodySize;
    }

    private void validateKeyBounds(int keyLengthPosition, int recordEnd) {
        long keyLength = 0;
        int shift = 0;
        int currentPosition = keyLengthPosition;
        while (true) {
            if (currentPosition >= recordEnd) {
                throw new CorruptMessageException(
                        "KV record key length crosses declared record end");
            }
            int currentByte = segment.get(currentPosition++) & 0xff;
            keyLength |= (long) (currentByte & 0x7f) << shift;
            if ((currentByte & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 28) {
                throw new CorruptMessageException("KV record has invalid key length");
            }
        }
        if (keyLength > Integer.MAX_VALUE || keyLength > recordEnd - currentPosition) {
            throw new CorruptMessageException("KV record key crosses declared record end");
        }
    }

    private long computeChecksum() {
        ByteBuffer buffer = segment.wrap(position, sizeInBytes());
        return Crc32C.compute(buffer, SCHEMA_ID_OFFSET, sizeInBytes() - SCHEMA_ID_OFFSET);
    }

    public static ProgressKvRecordBatch pointToMemory(MemorySegment segment, int position) {
        ProgressKvRecordBatch batch = new ProgressKvRecordBatch();
        batch.pointTo(segment, position);
        return batch;
    }
}
