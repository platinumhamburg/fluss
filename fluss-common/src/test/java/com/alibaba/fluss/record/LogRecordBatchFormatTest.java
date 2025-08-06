/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static com.alibaba.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.NO_LEADER_EPOCH;
import static com.alibaba.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.attributeOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.commitTimestampOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.crcOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.leaderEpochOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static com.alibaba.fluss.record.LogRecordBatchFormat.writeClientIdOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogRecordBatchFormat}. */
public class LogRecordBatchFormatTest {

    @Test
    void testCommonParam() {
        assertThat(LENGTH_OFFSET).isEqualTo(8);
        assertThat(MAGIC_OFFSET).isEqualTo(12);
        assertThat(LOG_OVERHEAD).isEqualTo(12);
        assertThat(HEADER_SIZE_UP_TO_MAGIC).isEqualTo(13);
    }

    @Test
    void testLogRecordBatchFormatForMagicV0() {
        byte magic = (byte) 0;
        assertThat(commitTimestampOffset(magic)).isEqualTo(13);
        assertThat(leaderEpochOffset(magic)).isEqualTo(-1).isEqualTo(NO_LEADER_EPOCH);
        assertThat(crcOffset(magic)).isEqualTo(21);
        assertThat(schemaIdOffset(magic)).isEqualTo(25);
        assertThat(attributeOffset(magic)).isEqualTo(27);
        assertThat(lastOffsetDeltaOffset(magic)).isEqualTo(28);
        assertThat(writeClientIdOffset(magic)).isEqualTo(32);
        assertThat(batchSequenceOffset(magic)).isEqualTo(40);
        assertThat(recordsCountOffset(magic)).isEqualTo(44);
        assertThat(recordBatchHeaderSize(magic)).isEqualTo(48);
        assertThat(arrowChangeTypeOffset(magic)).isEqualTo(48);
    }

    @Test
    void testLogRecordBatchFormatForMagicV1() {
        byte magic = (byte) 1;
        assertThat(commitTimestampOffset(magic)).isEqualTo(13);
        assertThat(leaderEpochOffset(magic)).isEqualTo(21);
        assertThat(crcOffset(magic)).isEqualTo(25);
        assertThat(schemaIdOffset(magic)).isEqualTo(29);
        assertThat(attributeOffset(magic)).isEqualTo(31);
        assertThat(lastOffsetDeltaOffset(magic)).isEqualTo(32);
        assertThat(writeClientIdOffset(magic)).isEqualTo(36);
        assertThat(batchSequenceOffset(magic)).isEqualTo(44);
        assertThat(recordsCountOffset(magic)).isEqualTo(48);
        assertThat(recordBatchHeaderSize(magic)).isEqualTo(52);
        assertThat(arrowChangeTypeOffset(magic)).isEqualTo(52);
    }
}
