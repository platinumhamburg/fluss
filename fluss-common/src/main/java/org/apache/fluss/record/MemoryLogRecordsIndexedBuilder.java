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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.utils.crc.Crc32C;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Default builder for {@link MemoryLogRecords} of log records in {@link LogFormat#INDEXED} format.
 */
public class MemoryLogRecordsIndexedBuilder implements AutoCloseable {
    private static final int BUILDER_DEFAULT_OFFSET = 0;

    private final long baseLogOffset;
    private final int schemaId;
    // The max bytes can be appended.
    private final int writeLimit;
    private final byte magic;
    private final AbstractPagedOutputView pagedOutputView;
    private final MemorySegment firstSegment;
    private final boolean appendOnly;

    private BytesView builtBuffer = null;
    private long writerId;
    private int batchSequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private volatile boolean isClosed;
    private boolean aborted = false;

    private MemoryLogRecordsIndexedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly) {
        this.appendOnly = appendOnly;
        checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.baseLogOffset = baseLogOffset;
        this.schemaId = schemaId;
        this.writeLimit = writeLimit;
        this.magic = magic;
        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        this.writerId = NO_WRITER_ID;
        this.batchSequence = NO_BATCH_SEQUENCE;
        this.currentRecordNumber = 0;
        this.isClosed = false;

        // We don't need to write header information while the builder creating,
        // we'll skip it first.
        this.pagedOutputView.setPosition(recordBatchHeaderSize(magic));
        this.sizeInBytes = recordBatchHeaderSize(magic);
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, boolean appendOnly) {
        return new MemoryLogRecordsIndexedBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                writeLimit,
                CURRENT_LOG_MAGIC_VALUE,
                outputView,
                appendOnly);
    }

    @VisibleForTesting
    public static MemoryLogRecordsIndexedBuilder builder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView outputView)
            throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                baseLogOffset, schemaId, writeLimit, magic, outputView, false);
    }

    /**
     * Check if we have room for a new record containing the given row. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(IndexedRow row) {
        return sizeInBytes + IndexedLogRecord.sizeOf(row) <= writeLimit;
    }

    public void append(ChangeType changeType, IndexedRow row) throws Exception {
        appendRecord(changeType, row);
    }

    private void appendRecord(ChangeType changeType, IndexedRow row) throws IOException {
        if (aborted) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsIndexedBuilder has already been aborted");
        }

        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsBuilder is closed for record appends");
        }
        if (appendOnly && changeType != ChangeType.APPEND_ONLY) {
            throw new IllegalArgumentException(
                    "Only append-only change type is allowed for append-only arrow log builder, but got "
                            + changeType);
        }

        int recordByteSizes = IndexedLogRecord.writeTo(pagedOutputView, changeType, row);
        currentRecordNumber++;
        sizeInBytes += recordByteSizes;
    }

    public BytesView build() throws IOException {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }

        if (builtBuffer != null) {
            return builtBuffer;
        }

        writeBatchHeader();
        builtBuffer =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                        .build();
        return builtBuffer;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        // trigger to rewrite batch header
        this.builtBuffer = null;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public long writerId() {
        return writerId;
    }

    public int batchSequence() {
        return batchSequence;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void abort() {
        aborted = true;
    }

    @Override
    public void close() throws IOException {
        if (aborted) {
            throw new IllegalStateException(
                    "Cannot close MemoryLogRecordsIndexedBuilder as it has already been aborted");
        }

        isClosed = true;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    // ----------------------- internal methods -------------------------------
    private void writeBatchHeader() throws IOException {
        // pagedOutputView doesn't support seek to previous segment,
        // so we create a new output view on the first segment
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.setPosition(0);
        // update header.
        outputView.writeLong(baseLogOffset);
        outputView.writeInt(sizeInBytes - BASE_OFFSET_LENGTH - LENGTH_LENGTH);
        outputView.writeByte(magic);

        // write empty timestamp which will be overridden on server side
        outputView.writeLong(0);

        // write empty leaderEpoch which will be overridden on server side
        if (magic >= LOG_MAGIC_VALUE_V1) {
            outputView.writeInt(NO_LEADER_EPOCH);
        }

        // write empty crc first.
        outputView.writeUnsignedInt(0);

        outputView.writeShort((short) schemaId);
        // write attributes (currently only appendOnly flag)
        outputView.writeBoolean(appendOnly);
        // skip write attribute byte for now.
        outputView.setPosition(lastOffsetDeltaOffset(magic));
        if (currentRecordNumber > 0) {
            outputView.writeInt(currentRecordNumber - 1);
        } else {
            // If there is no record, we write 0 for filed lastOffsetDelta, see the comments about
            // the field 'lastOffsetDelta' in DefaultLogRecordBatch.
            outputView.writeInt(0);
        }
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(currentRecordNumber);

        // Update crc.
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), schemaIdOffset(magic));
        outputView.setPosition(crcOffset(magic));
        outputView.writeUnsignedInt(crc);
    }
}
