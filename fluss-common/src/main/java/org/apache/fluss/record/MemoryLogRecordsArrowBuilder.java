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
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.utils.crc.Crc32C;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Builder for {@link MemoryLogRecords} of log records in {@link LogFormat#ARROW} format. */
public class MemoryLogRecordsArrowBuilder implements AutoCloseable {
    private static final int BUILDER_DEFAULT_OFFSET = 0;

    private final long baseLogOffset;
    private final int schemaId;
    private final byte magic;
    private final ArrowWriter arrowWriter;
    private final long writerEpoch;
    private final ChangeTypeVectorWriter changeTypeWriter;
    private final MemorySegment firstSegment;
    private final AbstractPagedOutputView pagedOutputView;
    private final boolean appendOnly;

    private volatile MultiBytesView bytesView = null;

    private long writerId;
    private int batchSequence;
    private int estimatedSizeInBytes;
    private int recordCount;
    private volatile boolean isClosed;
    private boolean reCalculateSizeInBytes = false;
    private boolean resetBatchHeader = false;
    private boolean aborted = false;

    // V3 extend properties
    private byte[] extendPropertiesData = new byte[0];

    private MemoryLogRecordsArrowBuilder(
            long baseLogOffset,
            int schemaId,
            byte magic,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView pagedOutputView,
            byte[] extendPropertiesData,
            boolean appendOnly) {
        this.appendOnly = appendOnly;
        checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.baseLogOffset = baseLogOffset;
        this.schemaId = schemaId;
        this.magic = magic;
        this.arrowWriter = checkNotNull(arrowWriter);
        this.writerEpoch = arrowWriter.getEpoch();

        this.writerId = NO_WRITER_ID;
        this.batchSequence = NO_BATCH_SEQUENCE;
        this.isClosed = false;

        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        int changeTypeOffset = getChangeTypeWriterOffset();
        checkArgument(
                firstSegment.size() >= changeTypeOffset,
                "The size of first segment of pagedOutputView is too small, need at least "
                        + changeTypeOffset
                        + " bytes.");
        this.changeTypeWriter = new ChangeTypeVectorWriter(firstSegment, changeTypeOffset);
        this.extendPropertiesData = extendPropertiesData;
        this.estimatedSizeInBytes =
                recordBatchHeaderSize(magic)
                        + (extendPropertiesData == null ? 0 : extendPropertiesData.length);
        this.recordCount = 0;
    }

    @VisibleForTesting
    public static MemoryLogRecordsArrowBuilder builder(
            long baseLogOffset,
            byte magic,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            byte[] extendPropertiesData) {
        return new MemoryLogRecordsArrowBuilder(
                baseLogOffset,
                schemaId,
                magic,
                arrowWriter,
                outputView,
                extendPropertiesData,
                false);
    }

    /** Builder with limited write size and the memory segment used to serialize records. */
    public static MemoryLogRecordsArrowBuilder builder(
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            byte[] extendPropertiesData,
            boolean appendOnly) {
        return new MemoryLogRecordsArrowBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                CURRENT_LOG_MAGIC_VALUE,
                arrowWriter,
                outputView,
                extendPropertiesData,
                appendOnly);
    }

    public MultiBytesView build() throws IOException {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }

        if (bytesView != null) {
            if (resetBatchHeader) {
                writeBatchHeader();
                resetBatchHeader = false;
            }
            return bytesView;
        }

        // serialize the arrow batch to dynamically allocated memory segments
        int arrowDataOffset = getChangeTypeWriterOffset() + changeTypeWriter.sizeInBytes();
        arrowWriter.serializeToOutputView(pagedOutputView, arrowDataOffset);
        recordCount = arrowWriter.getRecordsCount();

        bytesView =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                        .build();
        arrowWriter.recycle(writerEpoch);

        writeBatchHeader();
        return bytesView;
    }

    /** Check if the builder is full. */
    public boolean isFull() {
        return arrowWriter.isFull();
    }

    /**
     * Try to append a record to the builder. Return true if the record is appended successfully,
     * false if the builder is full.
     */
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        if (aborted) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsArrowBuilder has already been aborted");
        }

        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsArrowBuilder is closed for record appends");
        }
        if (appendOnly && changeType != ChangeType.APPEND_ONLY) {
            throw new IllegalArgumentException(
                    "Only append-only change type is allowed for append-only arrow log builder, but got "
                            + changeType);
        }

        arrowWriter.writeRow(row);
        if (!appendOnly) {
            changeTypeWriter.writeChangeType(changeType);
        }
        reCalculateSizeInBytes = true;
    }

    public long writerId() {
        return writerId;
    }

    public int batchSequence() {
        return batchSequence;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        // trigger to rewrite batch header when next build.
        this.resetBatchHeader = true;
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void abort() {
        arrowWriter.recycle(writerEpoch);
        aborted = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws Exception {
        if (aborted) {
            throw new IllegalStateException(
                    "Cannot close MemoryLogRecordsArrowBuilder as it has already been aborted");
        }

        if (isClosed) {
            return;
        }

        isClosed = true;

        // Build arrowBatch when batch close to recycle arrow writer.
        build();
    }

    public void recycleArrowWriter() {
        arrowWriter.recycle(writerEpoch);
    }

    public int estimatedSizeInBytes() {
        if (bytesView != null) {
            // accurate total size in bytes (compressed if compression is enabled)
            return bytesView.getBytesLength();
        }

        if (reCalculateSizeInBytes) {
            // make size in bytes up-to-date
            estimatedSizeInBytes =
                    getChangeTypeWriterOffset()
                            + changeTypeWriter.sizeInBytes()
                            + arrowWriter.estimatedSizeInBytes();
        }

        reCalculateSizeInBytes = false;
        return estimatedSizeInBytes;
    }

    // ----------------------- internal methods -------------------------------

    /**
     * Returns the offset where the change type writer should start, considering V3 extend
     * properties.
     */
    private int getChangeTypeWriterOffset() {
        if (magic == LOG_MAGIC_VALUE_V3) {
            return recordBatchHeaderSize(magic)
                    + (null == extendPropertiesData ? 0 : extendPropertiesData.length);
        } else {
            return arrowChangeTypeOffset(magic);
        }
    }

    private void writeBatchHeader() throws IOException {
        // pagedOutputView doesn't support seek to previous segment,
        // so we create a new output view on the first segment
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.setPosition(0);
        // update header.
        outputView.writeLong(baseLogOffset);
        outputView.writeInt(bytesView.getBytesLength() - BASE_OFFSET_LENGTH - LENGTH_LENGTH);
        outputView.writeByte(magic);

        // write empty timestamp which will be overridden on server side
        outputView.writeLong(0);

        // write empty leaderEpoch which will be overridden on server side
        if (magic >= LOG_MAGIC_VALUE_V1) {
            outputView.writeInt(NO_LEADER_EPOCH);
        }

        // write empty crc first.
        outputView.writeUnsignedInt(0);
        // write schema id
        outputView.writeShort((short) schemaId);
        // write attributes (currently only appendOnly flag)
        outputView.writeBoolean(appendOnly);
        // write lastOffsetDelta
        if (recordCount > 0) {
            outputView.writeInt(recordCount - 1);
        } else {
            // If there is no record, we write 0 for filed lastOffsetDelta, see the comments about
            // the field 'lastOffsetDelta' in DefaultLogRecordBatch.
            outputView.writeInt(0);
        }
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(recordCount);

        // Write extend properties for V3 format
        if (magic == LOG_MAGIC_VALUE_V3) {
            outputView.writeInt(extendPropertiesData.length);
            if (extendPropertiesData.length > 0) {
                outputView.write(extendPropertiesData);
            }
        }

        // Update crc.
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), schemaIdOffset(magic));
        outputView.setPosition(crcOffset(magic));
        outputView.writeUnsignedInt(crc);
    }

    @VisibleForTesting
    int getWriteLimitInBytes() {
        return arrowWriter.getWriteLimitInBytes();
    }
}
