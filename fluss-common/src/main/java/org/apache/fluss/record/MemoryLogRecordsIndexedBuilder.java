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
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.utils.crc.Crc32C;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;

/**
 * Default builder for {@link MemoryLogRecords} of log records in {@link
 * org.apache.fluss.metadata.LogFormat#INDEXED} format.
 */
public class MemoryLogRecordsIndexedBuilder extends MemoryLogRecordsRowBuilder<IndexedRow> {

    @Nullable private final List<MemorySegmentBytesView> preWrittenBytesView;
    private final int preWrittenRecordsNumber;
    @Nullable private final StateChangeLogs stateChangeLogs;
    private final int stateChangeLogsSizeInBytes;
    private long commitTimestamp = 0L;

    private MemoryLogRecordsIndexedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly) {
        super(baseLogOffset, schemaId, writeLimit, magic, pagedOutputView, appendOnly);
        this.preWrittenBytesView = null;
        this.preWrittenRecordsNumber = 0;
        this.stateChangeLogs = null;
        this.stateChangeLogsSizeInBytes = 0;
    }

    private MemoryLogRecordsIndexedBuilder(
            int schemaId,
            List<MemorySegmentBytesView> preWrittenBytesView,
            int preWrittenRecordsNumber,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly,
            @Nullable StateChangeLogs stateChangeLogs) {
        super(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                Integer.MAX_VALUE, // no write limit for pre-written mode
                magic,
                pagedOutputView,
                appendOnly);
        this.preWrittenBytesView = preWrittenBytesView;
        this.preWrittenRecordsNumber = preWrittenRecordsNumber;
        this.stateChangeLogs = stateChangeLogs;
        this.stateChangeLogsSizeInBytes =
                null == stateChangeLogs ? 0 : stateChangeLogs.sizeInBytes();
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
     * Creates a builder with pre-written bytes view and state change logs.
     *
     * @param schemaId the schema ID
     * @param preWrittenBytesView the pre-written bytes view containing serialized records
     * @param preWrittenRecordsNumber the number of records in the pre-written bytes view
     * @param appendOnly whether this is append-only
     * @param stateChangeLogs the state change logs to include (nullable)
     * @return a new builder instance
     */
    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId,
            List<MemorySegmentBytesView> preWrittenBytesView,
            int preWrittenRecordsNumber,
            boolean appendOnly,
            @Nullable StateChangeLogs stateChangeLogs) {
        // When stateChangeLogs is provided, use V3 format
        byte magic = stateChangeLogs != null ? LOG_MAGIC_VALUE_V3 : CURRENT_LOG_MAGIC_VALUE;
        return new MemoryLogRecordsIndexedBuilder(
                schemaId,
                preWrittenBytesView,
                preWrittenRecordsNumber,
                magic,
                new PreWrittenOutputView(preWrittenBytesView, magic),
                appendOnly,
                stateChangeLogs);
    }

    /**
     * Creates a builder with pre-written bytes view without state change logs (for backward
     * compatibility).
     *
     * @param schemaId the schema ID
     * @param preWrittenBytesView the pre-written bytes view containing serialized records
     * @param preWrittenRecordsNumber the number of records in the pre-written bytes view
     * @param appendOnly whether this is append-only
     * @return a new builder instance
     */
    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId,
            List<MemorySegmentBytesView> preWrittenBytesView,
            int preWrittenRecordsNumber,
            boolean appendOnly) {
        return builder(schemaId, preWrittenBytesView, preWrittenRecordsNumber, appendOnly, null);
    }

    /**
     * Sets the commit timestamp for this batch. This is only effective when using
     * preWrittenBytesView mode.
     *
     * @param timestamp the commit timestamp to set
     */
    public void setCommitTimestamp(long timestamp) {
        this.commitTimestamp = timestamp;
    }

    @Override
    public BytesView build() throws IOException {
        if (preWrittenBytesView != null) {
            return buildFromPreWritten();
        }
        return super.build();
    }

    /**
     * Build MemoryLogRecords from pre-written bytes view (for IndexCache scenario).
     *
     * <p>This method is used when the record data has already been serialized, and we only need to
     * add the batch header and optionally state change logs.
     */
    private BytesView buildFromPreWritten() throws IOException {
        if (preWrittenBytesView == null) {
            throw new IllegalStateException("preWrittenBytesView is null");
        }

        // Calculate total size: header + state change logs + pre-written data
        // Note: batchHeaderSize already includes BASE_OFFSET_LENGTH and LENGTH_LENGTH
        int batchHeaderSize = recordBatchHeaderSize(magic);
        int fullHeaderSize = batchHeaderSize + stateChangeLogsSizeInBytes;
        int dataSize = 0;
        for (MemorySegmentBytesView view : preWrittenBytesView) {
            dataSize += view.getBytesLength();
        }

        // Write batch header (including base offset and length)
        MemorySegment headerSegment = MemorySegment.allocateHeapMemory(fullHeaderSize);
        MemorySegmentOutputView headerView = new MemorySegmentOutputView(headerSegment);

        // Base offset
        headerView.writeLong(baseLogOffset);
        // Batch length (excluding base offset and length field itself)
        // batchHeaderSize includes BASE_OFFSET_LENGTH and LENGTH_LENGTH, so subtract them
        int batchLengthValue =
                (batchHeaderSize - BASE_OFFSET_LENGTH - LENGTH_LENGTH)
                        + stateChangeLogsSizeInBytes
                        + dataSize;
        headerView.writeInt(batchLengthValue);
        // Magic
        headerView.writeByte(magic);
        // Commit timestamp (use set value if provided, otherwise 0)
        headerView.writeLong(commitTimestamp);
        // Leader epoch (will be overridden on server side)
        if (magic >= LOG_MAGIC_VALUE_V1) {
            headerView.writeInt(NO_LEADER_EPOCH);
        }

        // CRC placeholder (will be computed later)
        int crcPosition = headerView.getPosition();
        headerView.writeUnsignedInt(0);

        // Schema ID
        headerView.writeShort((short) schemaId);
        // Attributes (appendOnly flag)
        headerView.writeBoolean(appendOnly);

        // Last offset delta
        if (preWrittenRecordsNumber > 0) {
            headerView.writeInt(preWrittenRecordsNumber - 1);
        } else {
            headerView.writeInt(0);
        }
        // Writer ID
        headerView.writeLong(NO_WRITER_ID);
        // Batch sequence
        headerView.writeInt(NO_BATCH_SEQUENCE);
        // Record count
        headerView.writeInt(preWrittenRecordsNumber);

        // Write state change logs size and content for V3 (after all other fields)
        if (magic == LOG_MAGIC_VALUE_V3) {
            headerView.writeInt(stateChangeLogsSizeInBytes);
            if (stateChangeLogsSizeInBytes > 0 && stateChangeLogs != null) {
                stateChangeLogs.writeTo(headerView);
            }
        }

        // Build the complete bytes view
        List<MemorySegmentBytesView> allViews = new ArrayList<>();
        allViews.add(new MemorySegmentBytesView(headerSegment, 0, fullHeaderSize));
        allViews.addAll(preWrittenBytesView);

        MultiBytesView.Builder builder = MultiBytesView.builder();
        builder.addMemorySegmentByteViewList(allViews);
        BytesView completeView = builder.build();

        // Compute and update CRC
        long crc = Crc32C.compute(allViews, schemaIdOffset(magic));
        headerView.setPosition(crcPosition);
        headerView.writeUnsignedInt(crc);

        return completeView;
    }

    @Override
    protected int sizeOf(IndexedRow row) {
        return IndexedLogRecord.sizeOf(row);
    }

    @Override
    protected int writeRecord(ChangeType changeType, IndexedRow row) throws IOException {
        return IndexedLogRecord.writeTo(pagedOutputView, changeType, row);
    }

    /** A minimal output view for pre-written bytes view mode. */
    private static class PreWrittenOutputView extends AbstractPagedOutputView {
        private final List<MemorySegmentBytesView> segments;

        PreWrittenOutputView(List<MemorySegmentBytesView> segments, byte magic) {
            super(
                    segments.isEmpty()
                            // Allocate enough space for the header + 1 to avoid setPosition()
                            // overflow
                            // since setPosition checks (size <= position)
                            ? MemorySegment.allocateHeapMemory(recordBatchHeaderSize(magic) + 1)
                            : segments.get(0).getMemorySegment(),
                    segments.isEmpty()
                            ? recordBatchHeaderSize(magic) + 1
                            : segments.get(0).getMemorySegment().size());
            this.segments = segments;
        }

        @Override
        protected MemorySegment nextSegment() throws IOException {
            throw new UnsupportedOperationException(
                    "PreWrittenOutputView does not support writing");
        }

        @Override
        public List<MemorySegment> allocatedPooledSegments() {
            return java.util.Collections.emptyList();
        }

        @Override
        public List<MemorySegmentBytesView> getWrittenSegments() {
            return segments;
        }
    }
}
