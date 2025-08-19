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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecordBatchStatisticsCollector;
import com.alibaba.fluss.record.MemoryLogRecordsArrowBuilder;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * A batch of log records managed in ARROW format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public class ArrowLogWriteBatch extends WriteBatch {
    private final MemoryLogRecordsArrowBuilder recordsBuilder;
    private final AbstractPagedOutputView outputView;
    private final LogRecordBatchStatisticsCollector statisticsCollector;

    public ArrowLogWriteBatch(
            int bucketId,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            long createdMs) {
        super(bucketId, physicalTablePath, createdMs);
        this.outputView = outputView;
        this.statisticsCollector = new LogRecordBatchStatisticsCollector(arrowWriter.getSchema());
        this.recordsBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        schemaId, arrowWriter, outputView, true, statisticsCollector);
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        InternalRow row = writeRecord.getRow();
        checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");
        checkArgument(writeRecord.getKey() == null, "key must be null for log record");
        checkNotNull(row != null, "row must not be null for log record");
        checkNotNull(callback, "write callback must be not null");
        if (recordsBuilder.isClosed() || recordsBuilder.isFull()) {
            return false;
        } else {
            recordsBuilder.append(ChangeType.APPEND_ONLY, row);
            recordCount++;
            callbacks.add(callback);
            return true;
        }
    }

    @Override
    public BytesView build() {
        try {
            return recordsBuilder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to build memory log records.", e);
        }
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        reopened = false;
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    @Override
    public List<MemorySegment> pooledMemorySegments() {
        return outputView.allocatedPooledSegments();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return recordsBuilder.writerId();
    }

    @Override
    public int batchSequence() {
        return recordsBuilder.batchSequence();
    }

    @Override
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }
}
