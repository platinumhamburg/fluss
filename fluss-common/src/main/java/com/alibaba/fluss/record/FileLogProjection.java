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
import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.exception.InvalidColumnProjectionException;
import com.alibaba.fluss.record.FileLogInputStream.FileChannelLogRecordBatch;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.Buffer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;
import com.alibaba.fluss.utils.types.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static com.alibaba.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static com.alibaba.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.attributeOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static com.alibaba.fluss.utils.FileUtils.readFullyOrFail;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** Column projection util on Arrow format {@link FileLogRecords}. */
public class FileLogProjection {

    // see the arrow binary message format in the page:
    // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    private static final int ARROW_IPC_CONTINUATION_LENGTH = 4;
    private static final int ARROW_IPC_METADATA_SIZE_OFFSET = ARROW_IPC_CONTINUATION_LENGTH;
    private static final int ARROW_IPC_METADATA_SIZE_LENGTH = 4;
    private static final int ARROW_HEADER_SIZE =
            ARROW_IPC_CONTINUATION_LENGTH + ARROW_IPC_METADATA_SIZE_LENGTH;

    final Map<Long, ProjectionInfo> projectionsCache = new HashMap<>();
    ProjectionInfo currentProjection;

    // shared resources for multiple projections
    private final ByteArrayOutputStream outputStream;
    private final WriteChannel writeChannel;

    /** Buffer to read log records batch header up to magic. */
    private final ByteBuffer logHeaderUpToMagicBuffer =
            ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /** Buffer to read log records batch header for magic version 0. */
    private final ByteBuffer logHeaderBufferForMagicV0 =
            ByteBuffer.allocate(recordBatchHeaderSize(LOG_MAGIC_VALUE_V0));

    /** Buffer to read log records batch header for magic version 1. */
    private final ByteBuffer logHeaderBufferForMagicV1 =
            ByteBuffer.allocate(recordBatchHeaderSize(LOG_MAGIC_VALUE_V1));

    /** Buffer to read log records batch header for magic version 1. */
    private final ByteBuffer logHeaderBufferForMagicV2 =
            ByteBuffer.allocate(recordBatchHeaderSize(LOG_MAGIC_VALUE_V2));

    private final ByteBuffer arrowHeaderBuffer = ByteBuffer.allocate(ARROW_HEADER_SIZE);
    private ByteBuffer arrowMetadataBuffer;

    public FileLogProjection() {
        this.outputStream = new ByteArrayOutputStream();
        this.writeChannel = new WriteChannel(Channels.newChannel(outputStream));
        // fluss use little endian for encoding log records batch
        this.logHeaderUpToMagicBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.logHeaderBufferForMagicV0.order(ByteOrder.LITTLE_ENDIAN);
        this.logHeaderBufferForMagicV1.order(ByteOrder.LITTLE_ENDIAN);
        this.logHeaderBufferForMagicV2.order(ByteOrder.LITTLE_ENDIAN);
        // arrow force use little endian to encode int32 values
        this.arrowHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public void setCurrentProjection(
            long tableId,
            RowType schema,
            ArrowCompressionInfo compressionInfo,
            int[] selectedFields) {
        if (projectionsCache.containsKey(tableId)) {
            // the schema and projection should identical for the same table id.
            currentProjection = projectionsCache.get(tableId);
            if (!Arrays.equals(currentProjection.selectedFields, selectedFields)
                    || !currentProjection.schema.equals(schema)) {
                throw new InvalidColumnProjectionException(
                        "The schema and projection should be identical for the same table id.");
            }
            return;
        }

        // initialize the projection util information
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema);
        BitSet selection = toBitSet(arrowSchema.getFields().size(), selectedFields);
        List<Tuple2<Field, Boolean>> flattenedFields = new ArrayList<>();
        flattenFields(arrowSchema.getFields(), selection, flattenedFields);
        int totalFieldNodes = flattenedFields.size();
        int[] bufferLayoutCount = new int[totalFieldNodes];
        BitSet nodesProjection = new BitSet(totalFieldNodes);
        int totalBuffers = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            Field fieldNode = flattenedFields.get(i).f0;
            boolean selected = flattenedFields.get(i).f1;
            nodesProjection.set(i, selected);
            bufferLayoutCount[i] = TypeLayout.getTypeBufferCount(fieldNode.getType());
            totalBuffers += bufferLayoutCount[i];
        }
        BitSet buffersProjection = new BitSet(totalBuffers);
        int bufferIndex = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            if (nodesProjection.get(i)) {
                buffersProjection.set(bufferIndex, bufferIndex + bufferLayoutCount[i]);
            }
            bufferIndex += bufferLayoutCount[i];
        }

        Schema projectedArrowSchema = ArrowUtils.toArrowSchema(schema.project(selectedFields));
        ArrowBodyCompression bodyCompression =
                CompressionUtil.createBodyCompression(compressionInfo.createCompressionCodec());
        int metadataLength =
                ArrowUtils.estimateArrowMetadataLength(projectedArrowSchema, bodyCompression);
        currentProjection =
                new ProjectionInfo(
                        nodesProjection,
                        buffersProjection,
                        bufferIndex,
                        schema,
                        metadataLength,
                        bodyCompression,
                        selectedFields);
        projectionsCache.put(tableId, currentProjection);
    }

    public BytesView projectRecordBatch(FileChannelLogRecordBatch batch) throws IOException {
        checkNotNull(currentProjection, "There is no projection registered yet.");
        FileChannel channel = batch.fileRecords.channel();
        int position = batch.position();
        int end = position + batch.sizeInBytes();

        MultiBytesView.Builder builder = MultiBytesView.builder();

        // read log header up to magic
        logHeaderUpToMagicBuffer.rewind();
        readFullyOrFail(channel, logHeaderUpToMagicBuffer, position, "log header up to magic");

        logHeaderUpToMagicBuffer.rewind();
        byte magic = logHeaderUpToMagicBuffer.get(MAGIC_OFFSET);
        ByteBuffer logHeaderBuffer = getLogHeaderBuffer(magic);

        // read log header
        logHeaderBuffer.rewind();
        readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
        if (position > end - batchSizeInBytes) {
            // the remaining bytes in the file are not enough to read a full batch
            return builder.build();
        }

        // Return null if meets empty batch. The empty batch was generated when build cdc log batch
        // when there
        // is no cdc log generated for this kv batch. See the comments about the field
        // 'lastOffsetDelta' in DefaultLogRecordBatch.
        if (batchSizeInBytes == recordBatchHeaderSize(magic)) {
            return builder.build();
        }

        boolean isAppendOnly =
                (logHeaderBuffer.get(attributeOffset(magic)) & APPEND_ONLY_FLAG_MASK) > 0;

        final int changeTypeBytes;
        final long arrowHeaderOffset;
        if (isAppendOnly) {
            changeTypeBytes = 0;
            arrowHeaderOffset = position + recordBatchHeaderSize(magic);
        } else {
            changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(magic));
            arrowHeaderOffset = position + recordBatchHeaderSize(magic) + changeTypeBytes;
        }

        // read arrow header
        arrowHeaderBuffer.rewind();
        readFullyOrFail(channel, arrowHeaderBuffer, arrowHeaderOffset, "arrow header");
        arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
        int arrowMetadataSize = arrowHeaderBuffer.getInt();

        resizeArrowMetadataBuffer(arrowMetadataSize);
        arrowMetadataBuffer.rewind();
        readFullyOrFail(
                channel,
                arrowMetadataBuffer,
                arrowHeaderOffset + ARROW_HEADER_SIZE,
                "arrow metadata");

        arrowMetadataBuffer.rewind();
        Message metadata = Message.getRootAsMessage(arrowMetadataBuffer);
        ProjectedArrowBatch projectedArrowBatch =
                projectArrowBatch(
                        metadata,
                        currentProjection.nodesProjection,
                        currentProjection.buffersProjection,
                        currentProjection.bufferCount);
        long arrowBodyLength = projectedArrowBatch.bodyLength();

        int newBatchSizeInBytes =
                recordBatchHeaderSize(magic)
                        + changeTypeBytes
                        + currentProjection.arrowMetadataLength
                        + (int) arrowBodyLength; // safe to cast to int

        // 3. create new arrow batch metadata which already projected.
        byte[] headerMetadata =
                serializeArrowRecordBatchMetadata(
                        projectedArrowBatch, arrowBodyLength, currentProjection.bodyCompression);
        checkState(
                headerMetadata.length == currentProjection.arrowMetadataLength,
                "Invalid metadata length");

        // 4. update and copy log batch header
        logHeaderBuffer.position(LENGTH_OFFSET);
        logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);
        logHeaderBuffer.rewind();
        // the logHeader can't be reused, as it will be sent to network
        byte[] logHeader = new byte[recordBatchHeaderSize(magic)];
        logHeaderBuffer.get(logHeader);

        // 5. build log records
        builder.addBytes(logHeader);
        if (!isAppendOnly) {
            builder.addBytes(channel, position + arrowChangeTypeOffset(magic), changeTypeBytes);
        }
        builder.addBytes(headerMetadata);
        final long bufferOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
        projectedArrowBatch.buffers.forEach(
                b -> builder.addBytes(channel, bufferOffset + b.getOffset(), (int) b.getSize()));
        return builder.build();
    }

    /**
     * Project the log records to a subset of fields and the size of returned log records shouldn't
     * exceed maxBytes.
     *
     * @return the projected records.
     */
    public BytesViewLogRecords project(FileChannel channel, int start, int end, int maxBytes)
            throws IOException {
        checkNotNull(currentProjection, "There is no projection registered yet.");
        MultiBytesView.Builder builder = MultiBytesView.builder();
        int position = start;
        while (maxBytes > HEADER_SIZE_UP_TO_MAGIC) {
            if (position >= end - HEADER_SIZE_UP_TO_MAGIC) {
                // the remaining bytes in the file are not enough to read a batch header up to
                // magic.
                return new BytesViewLogRecords(builder.build());
            }

            // read log header up to magic
            logHeaderUpToMagicBuffer.rewind();
            readFullyOrFail(channel, logHeaderUpToMagicBuffer, position, "log header up to magic");

            logHeaderUpToMagicBuffer.rewind();
            byte magic = logHeaderUpToMagicBuffer.get(MAGIC_OFFSET);

            int recordBatchHeaderSize = recordBatchHeaderSize(magic);
            if (position >= end - recordBatchHeaderSize) {
                // the remaining bytes in the file are not enough to read a batch header
                return new BytesViewLogRecords(builder.build());
            }

            ByteBuffer logHeaderBuffer = getLogHeaderBuffer(magic);
            // read log header
            logHeaderBuffer.rewind();
            readFullyOrFail(channel, logHeaderBuffer, position, "log header");

            logHeaderBuffer.rewind();
            int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
            if (position > end - batchSizeInBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // Skip empty batch. The empty batch was generated when build cdc log batch when there
            // is no cdc log generated for this kv batch. See the comments about the field
            // 'lastOffsetDelta' in DefaultLogRecordBatch.
            if (batchSizeInBytes == recordBatchHeaderSize) {
                position += batchSizeInBytes;
                continue;
            }

            boolean isAppendOnly =
                    (logHeaderBuffer.get(attributeOffset(magic)) & APPEND_ONLY_FLAG_MASK) > 0;

            final int changeTypeBytes;
            final long arrowHeaderOffset;
            if (isAppendOnly) {
                changeTypeBytes = 0;
                arrowHeaderOffset = position + recordBatchHeaderSize;
            } else {
                changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(magic));
                arrowHeaderOffset = position + recordBatchHeaderSize + changeTypeBytes;
            }

            // read arrow header
            arrowHeaderBuffer.rewind();
            readFullyOrFail(channel, arrowHeaderBuffer, arrowHeaderOffset, "arrow header");
            arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
            int arrowMetadataSize = arrowHeaderBuffer.getInt();

            resizeArrowMetadataBuffer(arrowMetadataSize);
            arrowMetadataBuffer.rewind();
            readFullyOrFail(
                    channel,
                    arrowMetadataBuffer,
                    arrowHeaderOffset + ARROW_HEADER_SIZE,
                    "arrow metadata");

            arrowMetadataBuffer.rewind();
            Message metadata = Message.getRootAsMessage(arrowMetadataBuffer);
            ProjectedArrowBatch projectedArrowBatch =
                    projectArrowBatch(
                            metadata,
                            currentProjection.nodesProjection,
                            currentProjection.buffersProjection,
                            currentProjection.bufferCount);
            long arrowBodyLength = projectedArrowBatch.bodyLength();

            int newBatchSizeInBytes =
                    recordBatchHeaderSize
                            + changeTypeBytes
                            + currentProjection.arrowMetadataLength
                            + (int) arrowBodyLength; // safe to cast to int
            if (newBatchSizeInBytes > maxBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // 3. create new arrow batch metadata which already projected.
            byte[] headerMetadata =
                    serializeArrowRecordBatchMetadata(
                            projectedArrowBatch,
                            arrowBodyLength,
                            currentProjection.bodyCompression);
            checkState(
                    headerMetadata.length == currentProjection.arrowMetadataLength,
                    "Invalid metadata length");

            // 4. update and copy log batch header
            logHeaderBuffer.position(LENGTH_OFFSET);
            logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);
            logHeaderBuffer.rewind();
            // the logHeader can't be reused, as it will be sent to network
            byte[] logHeader = new byte[recordBatchHeaderSize];
            logHeaderBuffer.get(logHeader);

            // 5. build log records
            builder.addBytes(logHeader);
            if (!isAppendOnly) {
                builder.addBytes(channel, position + arrowChangeTypeOffset(magic), changeTypeBytes);
            }
            builder.addBytes(headerMetadata);
            final long bufferOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
            projectedArrowBatch.buffers.forEach(
                    b ->
                            builder.addBytes(
                                    channel, bufferOffset + b.getOffset(), (int) b.getSize()));

            maxBytes -= newBatchSizeInBytes;
            position += batchSizeInBytes;
        }

        return new BytesViewLogRecords(builder.build());
    }

    private ProjectedArrowBatch projectArrowBatch(
            Message metadata, BitSet nodesProjection, BitSet buffersProjection, int bufferCount) {
        List<ArrowFieldNode> newNodes = new ArrayList<>();
        List<ArrowBuffer> newBufferLayouts = new ArrayList<>();
        List<ArrowBuffer> selectedBuffers = new ArrayList<>();
        RecordBatch recordBatch = (RecordBatch) metadata.header(new RecordBatch());
        long numRecords = recordBatch.length();
        for (int i = nodesProjection.nextSetBit(0); i >= 0; i = nodesProjection.nextSetBit(i + 1)) {
            FieldNode node = recordBatch.nodes(i);
            newNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
        }
        long bodyLength = metadata.bodyLength();
        long newOffset = 0L;
        for (int i = buffersProjection.nextSetBit(0);
                i >= 0;
                i = buffersProjection.nextSetBit(i + 1)) {
            Buffer buf = recordBatch.buffers(i);
            long nextOffset =
                    i < bufferCount - 1 ? recordBatch.buffers(i + 1).offset() : bodyLength;
            long paddedLength = nextOffset - buf.offset();
            selectedBuffers.add(new ArrowBuffer(buf.offset(), paddedLength));
            newBufferLayouts.add(new ArrowBuffer(newOffset, buf.length()));
            newOffset += paddedLength;
        }

        return new ProjectedArrowBatch(numRecords, newNodes, newBufferLayouts, selectedBuffers);
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch}. This avoids to create an instance of {@link
     * ArrowRecordBatch}.
     *
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    private byte[] serializeArrowRecordBatchMetadata(
            ProjectedArrowBatch batch, long arrowBodyLength, ArrowBodyCompression bodyCompression)
            throws IOException {
        outputStream.reset();
        ArrowUtils.serializeArrowRecordBatchMetadata(
                writeChannel,
                batch.numRecords,
                batch.nodes,
                batch.buffersLayout,
                bodyCompression,
                arrowBodyLength);
        return outputStream.toByteArray();
    }

    private void resizeArrowMetadataBuffer(int metadataSize) {
        if (arrowMetadataBuffer == null || arrowMetadataBuffer.capacity() < metadataSize) {
            arrowMetadataBuffer = ByteBuffer.allocate(metadataSize);
            arrowMetadataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            arrowMetadataBuffer.limit(metadataSize);
        }
    }

    /** Flatten fields by a pre-order depth-first traversal of the fields in the schema. */
    private void flattenFields(
            List<Field> arrowFields,
            BitSet selectedFields,
            List<Tuple2<Field, Boolean>> flattenedFields) {
        for (int i = 0; i < arrowFields.size(); i++) {
            Field field = arrowFields.get(i);
            boolean selected = selectedFields.get(i);
            flattenedFields.add(Tuple2.of(field, selected));
            List<Field> children = field.getChildren();
            flattenFields(children, fillBitSet(children.size(), selected), flattenedFields);
        }
    }

    private static BitSet toBitSet(int length, int[] selectedIndexes) {
        BitSet bitset = new BitSet(length);
        int prev = -1;
        for (int i : selectedIndexes) {
            if (i < prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should be in field order, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i == prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should not contain duplicated fields, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i >= length) {
                throw new InvalidColumnProjectionException(
                        "Projected fields "
                                + Arrays.toString(selectedIndexes)
                                + " is out of bound for schema with "
                                + length
                                + " fields.");
            }
            bitset.set(i);
            prev = i;
        }
        return bitset;
    }

    private static BitSet fillBitSet(int length, boolean value) {
        BitSet bitset = new BitSet(length);
        if (value) {
            bitset.set(0, length);
        } else {
            bitset.clear();
        }
        return bitset;
    }

    @VisibleForTesting
    ByteBuffer getLogHeaderBuffer(byte magic) {
        if (magic == LOG_MAGIC_VALUE_V0) {
            return logHeaderBufferForMagicV0;
        } else if (magic == LOG_MAGIC_VALUE_V1) {
            return logHeaderBufferForMagicV1;
        } else if (magic == LOG_MAGIC_VALUE_V2) {
            return logHeaderBufferForMagicV2;
        } else {
            throw new IllegalArgumentException("Unknown magic value: " + magic);
        }
    }

    static final class ProjectionInfo {
        final BitSet nodesProjection;
        final BitSet buffersProjection;
        final int bufferCount;
        final RowType schema;
        final int arrowMetadataLength;
        final ArrowBodyCompression bodyCompression;
        final int[] selectedFields;

        private ProjectionInfo(
                BitSet nodesProjection,
                BitSet buffersProjection,
                int bufferCount,
                RowType schema,
                int arrowMetadataLength,
                ArrowBodyCompression bodyCompression,
                int[] selectedFields) {
            this.nodesProjection = nodesProjection;
            this.buffersProjection = buffersProjection;
            this.bufferCount = bufferCount;
            this.schema = schema;
            this.arrowMetadataLength = arrowMetadataLength;
            this.bodyCompression = bodyCompression;
            this.selectedFields = selectedFields;
        }
    }

    /** Metadata of a projected arrow record batch. */
    public static final class ProjectedArrowBatch {
        /** Number of records. */
        final long numRecords;

        /** The projected nodes of {@link ArrowRecordBatch#getNodes()}. */
        final List<ArrowFieldNode> nodes;

        /** The new buffer layouts of the {@link #buffers}. */
        final List<ArrowBuffer> buffersLayout;

        /** The projected buffer positions of {@link ArrowRecordBatch#getBuffers()}. */
        final List<ArrowBuffer> buffers;

        public ProjectedArrowBatch(
                long numRecords,
                List<ArrowFieldNode> nodes,
                List<ArrowBuffer> buffersLayout,
                List<ArrowBuffer> buffers) {
            this.numRecords = numRecords;
            this.nodes = nodes;
            this.buffersLayout = buffersLayout;
            this.buffers = buffers;
        }

        public long bodyLength() {
            long bodyLength = 0;
            for (ArrowBuffer buffer : buffers) {
                bodyLength += buffer.getSize();
            }
            return bodyLength;
        }
    }
}
