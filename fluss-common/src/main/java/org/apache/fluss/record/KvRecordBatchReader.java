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
        if (buffer.remaining() < MINIMUM_PREFIX_SIZE) {
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
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            segment = MemorySegment.wrap(bytes);
            position = 0;
        }

        int sizeInBytes = DefaultKvRecordBatch.KV_OVERHEAD + segment.getInt(position);
        byte magic = segment.get(position + DefaultKvRecordBatch.MAGIC_OFFSET);
        KvRecordBatch batch;
        int headerSize;
        if (magic == KvRecordBatch.KV_MAGIC_VALUE_V0) {
            batch = DefaultKvRecordBatch.pointToMemory(segment, position);
            headerSize = DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        } else if (magic == KvRecordBatch.KV_MAGIC_VALUE_V1) {
            batch = FencedKvRecordBatch.pointToMemory(segment, position);
            headerSize = FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        } else {
            throw new CorruptMessageException(
                    "Unsupported KV batch magic " + Byte.toUnsignedInt(magic));
        }

        if (sizeInBytes < headerSize || sizeInBytes > buffer.remaining()) {
            throw new CorruptMessageException(
                    "KV batch has invalid size "
                            + sizeInBytes
                            + " for magic "
                            + Byte.toUnsignedInt(magic));
        }
        return batch;
    }
}
