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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the V0 {@link KvRecordBatchBuilder}. */
class KvRecordBatchBuilderTest {

    @Test
    void testCurrentBuilderStillProducesV0() throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        1,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);
        builder.setWriterState(33L, Integer.MAX_VALUE);
        ByteBuffer buffer = builder.build().getByteBuf().nioBuffer();
        KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(buffer);

        assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V0);
        assertThat(batch.idempotenceProtocolVersion()).isZero();

        builder.close();
    }
}
