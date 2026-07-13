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

import java.nio.ByteBuffer;

/** Parses a KV record batch after dispatching to the format selected by its magic byte. */
public final class KvRecordBatchReader {

    private static final int MINIMUM_PREFIX_SIZE = 5;

    private KvRecordBatchReader() {}

    /** Points a KV record batch at the remaining contents of the supplied buffer. */
    public static KvRecordBatch pointToByteBuffer(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining < MINIMUM_PREFIX_SIZE) {
            throw new CorruptMessageException(
                    "KV batch is smaller than the minimum length and magic prefix");
        }

        MemorySegment segment;
        int position;
        if (buffer.isDirect()) {
            segment = MemorySegment.wrapOffHeapMemory(buffer);
            position = buffer.position();
        } else if (buffer.hasArray()) {
            segment = MemorySegment.wrap(buffer.array());
            position = buffer.arrayOffset() + buffer.position();
        } else {
            byte[] bytes = new byte[remaining];
            buffer.duplicate().get(bytes);
            segment = MemorySegment.wrap(bytes);
            position = 0;
        }

        int declaredLength = segment.getInt(position);
        byte magic = segment.get(position + DefaultKvRecordBatch.MAGIC_OFFSET);
        int headerSize;
        if (magic == KvRecordBatch.KV_MAGIC_VALUE_V0) {
            headerSize = DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        } else if (magic == KvRecordBatch.KV_MAGIC_VALUE_V1) {
            headerSize = FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        } else {
            throw new CorruptMessageException(
                    "Unsupported KV batch magic " + Byte.toUnsignedInt(magic));
        }

        if (declaredLength < 0
                || declaredLength > Integer.MAX_VALUE - DefaultKvRecordBatch.KV_OVERHEAD) {
            throw new CorruptMessageException("Invalid KV batch length " + declaredLength);
        }
        int sizeInBytes = DefaultKvRecordBatch.KV_OVERHEAD + declaredLength;
        if (sizeInBytes < headerSize) {
            throw new CorruptMessageException(
                    "KV batch size "
                            + sizeInBytes
                            + " is smaller than magic "
                            + Byte.toUnsignedInt(magic)
                            + " header "
                            + headerSize);
        }
        if (sizeInBytes > remaining) {
            throw new CorruptMessageException(
                    "KV batch size " + sizeInBytes + " exceeds remaining bytes " + remaining);
        }

        if (magic == KvRecordBatch.KV_MAGIC_VALUE_V0) {
            return DefaultKvRecordBatch.pointToMemory(segment, position);
        }
        FencedKvRecordBatch batch = FencedKvRecordBatch.pointToMemory(segment, position);
        batch.validateRecordCountAndPayloadSize();
        return batch;
    }
}
