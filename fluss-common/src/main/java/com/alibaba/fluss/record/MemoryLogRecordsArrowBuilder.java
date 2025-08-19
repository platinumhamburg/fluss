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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.utils.crc.Crc32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.alibaba.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static com.alibaba.fluss.record.LogRecordBatchFormat.BASE_OFFSET_LENGTH;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static com.alibaba.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static com.alibaba.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static com.alibaba.fluss.record.LogRecordBatchFormat.STATISTICS_FLAG_MASK;
import static com.alibaba.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.crcOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static com.alibaba.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Builder for {@link MemoryLogRecords} of log records in {@link LogFormat#ARROW} format. */
public class MemoryLogRecordsArrowBuilder implements AutoCloseable {
    private static final int BUILDER_DEFAULT_OFFSET = 0;
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLogRecordsArrowBuilder.class);

    private final long baseLogOffset;
    private final int schemaId;
    private final byte magic;
    private final ArrowWriter arrowWriter;
    private final long writerEpoch;
    private final ChangeTypeVectorWriter changeTypeWriter;
    private final MemorySegment firstSegment;
    private final AbstractPagedOutputView pagedOutputView;
    private final boolean appendOnly;
    private final LogRecordBatchStatisticsCollector statisticsCollector;

    private volatile MultiBytesView bytesView = null;

    private long writerId;
    private int batchSequence;
    private int estimatedSizeInBytes;
    private int recordCount;
    private volatile boolean isClosed;
    private boolean reCalculateSizeInBytes = false;
    private boolean resetBatchHeader = false;
    private boolean aborted = false;
    private int statisticsLength = 0;

    private MemoryLogRecordsArrowBuilder(
            long baseLogOffset,
            int schemaId,
            byte magic,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly,
            LogRecordBatchStatisticsCollector statisticsCollector) {
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
        // For V2, we use the minimal arrow change type offset (without statistics)
        int arrowChangeTypeOffset = arrowChangeTypeOffset(magic);
        checkArgument(
                firstSegment.size() >= arrowChangeTypeOffset,
                "The size of first segment of pagedOutputView is too small, need at least "
                        + arrowChangeTypeOffset
                        + " bytes.");
        this.changeTypeWriter = new ChangeTypeVectorWriter(firstSegment, arrowChangeTypeOffset);
        this.estimatedSizeInBytes = recordBatchHeaderSize(magic);
        this.recordCount = 0;
        this.statisticsCollector = statisticsCollector;
    }

    @VisibleForTesting
    public static MemoryLogRecordsArrowBuilder builder(
            long baseLogOffset,
            byte magic,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            LogRecordBatchStatisticsCollector statisticsCollector) {
        return new MemoryLogRecordsArrowBuilder(
                baseLogOffset,
                schemaId,
                magic,
                arrowWriter,
                outputView,
                false,
                statisticsCollector);
    }

    @VisibleForTesting
    public static MemoryLogRecordsArrowBuilder builder(
            long baseLogOffset,
            byte magic,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView) {
        return new MemoryLogRecordsArrowBuilder(
                baseLogOffset, schemaId, magic, arrowWriter, outputView, false, null);
    }

    /** Builder with limited write size and the memory segment used to serialize records. */
    public static MemoryLogRecordsArrowBuilder builder(
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            boolean appendOnly,
            LogRecordBatchStatisticsCollector statisticsCollector) {
        return new MemoryLogRecordsArrowBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                CURRENT_LOG_MAGIC_VALUE,
                arrowWriter,
                outputView,
                appendOnly,
                statisticsCollector);
    }

    /** Builder with limited write size and the memory segment used to serialize records. */
    public static MemoryLogRecordsArrowBuilder builder(
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            boolean appendOnly) {
        return new MemoryLogRecordsArrowBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                CURRENT_LOG_MAGIC_VALUE,
                arrowWriter,
                outputView,
                appendOnly,
                null);
    }

    public MultiBytesView build() throws IOException {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }

        if (bytesView != null && !resetBatchHeader) {
            // If bytesView already exists and no header reset is needed, we don't need to rebuild
            return bytesView;
        }

        if (bytesView != null && resetBatchHeader) {
            // If bytesView exists but header needs to be reset, only rewrite the header
            writeBatchHeader(); // Use the stored statisticsLength
            resetBatchHeader = false;
            return bytesView;
        }

        // serialize the arrow batch to dynamically allocated memory segments
        // Calculate arrow offset based on magic version
        int arrowOffset = LogRecordBatchFormat.arrowChangeTypeOffset(magic);
        if (!appendOnly) {
            // For non-append-only, arrow data starts after changeType data
            arrowOffset += changeTypeWriter.sizeInBytes();
        }

        int arrowBytesWritten = arrowWriter.serializeToOutputView(pagedOutputView, arrowOffset);
        recordCount = arrowWriter.getRecordsCount();

        // For V2, append statistics after records if available
        statisticsLength = 0;
        if (magic >= LogRecordBatchFormat.LOG_MAGIC_VALUE_V2 && statisticsCollector != null) {
            try {
                // Write statistics directly to the current output position
                // This avoids the cross-segment issue by using OutputView's automatic segment
                // management
                statisticsLength = statisticsCollector.writeStatistics(pagedOutputView);
            } catch (Exception e) {
                LOG.error("Failed to serialize statistics for record batch", e);
                // If serialization fails, continue without statistics
            }
        }

        // Reset the statistics collector for reuse
        if (statisticsCollector != null) {
            statisticsCollector.reset();
        }

        bytesView =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                        .build();
        arrowWriter.recycle(writerEpoch);

        // Write header with correct statistics length after all data is written
        writeBatchHeader();

        // Reset the flag after header is written
        resetBatchHeader = false;

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
        // Collect statistics for the row if enabled
        if (statisticsCollector != null) {
            statisticsCollector.processRow(row);
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
            int baseSize;
            if (appendOnly) {
                // For append-only, arrow data starts at V2_RECORDS_OFFSET
                baseSize =
                        LogRecordBatchFormat.V2_RECORDS_OFFSET + arrowWriter.estimatedSizeInBytes();
            } else {
                // For non-append-only, arrow data starts after changeType data
                baseSize =
                        LogRecordBatchFormat.V2_RECORDS_OFFSET
                                + changeTypeWriter.sizeInBytes()
                                + arrowWriter.estimatedSizeInBytes();
            }

            // For V2, add estimated statistics size after records
            if (magic >= LogRecordBatchFormat.LOG_MAGIC_VALUE_V2 && statisticsCollector != null) {
                try {
                    // Create a temporary output view to estimate size
                    MemorySegment tempSegment = MemorySegment.wrap(new byte[1024]);
                    MemorySegmentOutputView tempOutputView =
                            new MemorySegmentOutputView(tempSegment);
                    int size = statisticsCollector.writeStatistics(tempOutputView);
                    baseSize += size;
                } catch (Exception e) {
                    LOG.error("Failed to estimate statistics size for record batch", e);
                    // If serialization fails, skip statistics size estimation
                }
            }

            estimatedSizeInBytes = baseSize;
        }

        reCalculateSizeInBytes = false;
        return estimatedSizeInBytes;
    }

    // ----------------------- internal methods -------------------------------
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

        // write attributes (appendOnly flag and statistics flag)
        byte attributes = 0;
        if (appendOnly) {
            attributes |= 0x01; // set appendOnly flag
        }

        // Set statistics flag if statistics length > 0
        if (statisticsLength > 0) {
            attributes |= STATISTICS_FLAG_MASK; // set statistics flag
        }

        outputView.writeByte(attributes);

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

        // For V2, write statistics length (statistics data is appended after records)
        if (magic >= LogRecordBatchFormat.LOG_MAGIC_VALUE_V2) {
            outputView.writeInt(statisticsLength);
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
