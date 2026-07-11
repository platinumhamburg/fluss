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

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.utils.crc.Crc32C;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.record.FencedKvRecordBatch.CRC_OFFSET;
import static org.apache.fluss.record.FencedKvRecordBatch.LENGTH_LENGTH;
import static org.apache.fluss.record.FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.FencedKvRecordBatch.SCHEMA_ID_OFFSET;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Builder for magic-1 {@link FencedKvRecordBatch} memory bytes. */
public class FencedKvRecordBatchBuilder implements AutoCloseable {

    private final int schemaId;
    private final int writeLimit;
    private final AbstractPagedOutputView pagedOutputView;
    private final MemorySegment firstSegment;
    private final KvFormat kvFormat;

    private BytesView builtBuffer;
    private WriterKey writerKey;
    private long sequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private boolean writerStateSet;
    private volatile boolean isClosed;
    private boolean aborted;

    private FencedKvRecordBatchBuilder(
            int schemaId,
            int writeLimit,
            AbstractPagedOutputView pagedOutputView,
            KvFormat kvFormat) {
        checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.schemaId = schemaId;
        this.writeLimit = writeLimit;
        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        this.kvFormat = kvFormat;
        this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
        pagedOutputView.setPosition(RECORD_BATCH_HEADER_SIZE);
    }

    public static FencedKvRecordBatchBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, KvFormat kvFormat) {
        return new FencedKvRecordBatchBuilder(schemaId, writeLimit, outputView, kvFormat);
    }

    /** Returns whether there is room for the supplied record. */
    public boolean hasRoomFor(byte[] key, @Nullable BinaryRow row) {
        return sizeInBytes + DefaultKvRecord.sizeOf(key, row) <= writeLimit;
    }

    /** Appends a key and optional row. A null row represents a delete. */
    public void append(byte[] key, @Nullable BinaryRow row) throws IOException {
        if (aborted) {
            throw new IllegalStateException(
                    "Tried to append a record, but FencedKvRecordBatchBuilder has already been aborted");
        }
        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to put a record, but FencedKvRecordBatchBuilder is closed for record puts.");
        }
        int recordBytes = DefaultKvRecord.writeTo(pagedOutputView, key, validateRowFormat(row));
        currentRecordNumber++;
        if (currentRecordNumber == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Maximum number of records per batch exceeded, max records: "
                            + Integer.MAX_VALUE);
        }
        sizeInBytes += recordBytes;
    }

    /** Sets the opaque writer identity and non-negative fenced sequence for this batch. */
    public void setWriterState(WriterKey writerKey, long sequence) {
        if (writerKey == null) {
            throw new NullPointerException("writerKey must not be null");
        }
        checkArgument(sequence >= 0, "fenced sequence must be non-negative");
        this.writerKey = writerKey;
        this.sequence = sequence;
        this.writerStateSet = true;
    }

    /** Builds the fenced KV batch. */
    public BytesView build() throws IOException {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        if (!writerStateSet) {
            throw new IllegalStateException("Fenced KV batch requires writer state before build");
        }
        if (builtBuffer == null) {
            writeBatchHeader();
            builtBuffer =
                    MultiBytesView.builder()
                            .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                            .build();
        }
        return builtBuffer;
    }

    public void abort() {
        aborted = true;
    }

    @Override
    public void close() {
        if (aborted) {
            throw new IllegalStateException(
                    "Cannot close FencedKvRecordBatchBuilder as it has already been aborted");
        }
        isClosed = true;
    }

    private void writeBatchHeader() throws IOException {
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.writeInt(sizeInBytes - LENGTH_LENGTH);
        outputView.writeByte(KvRecordBatch.KV_MAGIC_VALUE_V1);
        outputView.writeUnsignedInt(0);
        outputView.writeShort((short) schemaId);
        outputView.writeByte(0);
        outputView.writeLong(writerKey.high());
        outputView.writeLong(writerKey.low());
        outputView.writeLong(sequence);
        outputView.writeInt(currentRecordNumber);
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), SCHEMA_ID_OFFSET);
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(crc);
    }

    private BinaryRow validateRowFormat(BinaryRow row) {
        if (row == null) {
            return null;
        }
        if (kvFormat == KvFormat.COMPACTED && row instanceof CompactedRow) {
            return row;
        }
        if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
            return row;
        }
        if (kvFormat == KvFormat.ALIGNED && row instanceof AlignedRow) {
            return row;
        }
        throw new IllegalArgumentException(
                "The row to be appended to kv record batch with "
                        + kvFormat
                        + " format has unsupported type "
                        + row.getClass().getSimpleName());
    }
}
