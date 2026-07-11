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

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.memory.MemorySegment;

import static org.apache.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class MemorySegmentLogInputStream implements LogInputStream<LogRecordBatch> {
    private final MemorySegment memorySegment;

    private int currentPosition;
    private int remaining;

    MemorySegmentLogInputStream(MemorySegment memorySegment, int basePosition, int sizeInBytes) {
        this.memorySegment = memorySegment;
        this.currentPosition = basePosition;
        this.remaining = sizeInBytes;
    }

    public LogRecordBatch nextBatch() {
        Integer batchSize = nextBatchSize();
        if (batchSize == null || remaining < batchSize) {
            return null;
        }

        DefaultLogRecordBatch logRecords = new DefaultLogRecordBatch();
        logRecords.pointTo(memorySegment, currentPosition);

        currentPosition += batchSize;
        remaining -= batchSize;
        return logRecords;
    }

    /** Validates the header of the next batch and returns batch size. */
    private Integer nextBatchSize() {
        if (remaining < HEADER_SIZE_UP_TO_MAGIC) {
            return null;
        }

        int recordSize = memorySegment.getInt(currentPosition + LENGTH_OFFSET);
        byte magic = memorySegment.get(currentPosition + MAGIC_OFFSET);
        final int minimumHeaderSize;
        try {
            minimumHeaderSize = recordBatchHeaderSize(magic);
        } catch (IllegalArgumentException e) {
            throw new CorruptMessageException(
                    "Unsupported log magic " + Byte.toUnsignedInt(magic), e);
        }

        if (recordSize < 0) {
            throw new CorruptMessageException(
                    "Record batch has negative declared length " + recordSize);
        }
        long batchSize = (long) LOG_OVERHEAD + recordSize;
        if (batchSize > Integer.MAX_VALUE) {
            throw new CorruptMessageException(
                    "Record batch declared size overflow: " + batchSize);
        }
        if (batchSize < minimumHeaderSize) {
            throw new CorruptMessageException(
                    "Record batch declared size "
                            + batchSize
                            + " is smaller than fixed header "
                            + minimumHeaderSize);
        }
        if (remaining < minimumHeaderSize) {
            return null;
        }
        return (int) batchSize;
    }
}
