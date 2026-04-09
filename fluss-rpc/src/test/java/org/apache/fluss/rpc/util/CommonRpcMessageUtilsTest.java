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

package org.apache.fluss.rpc.util;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CommonRpcMessageUtils}.
 *
 * <p>Specifically verifies the deserialization of FetchLogResponse when both records and
 * filteredEndOffset are present — a scenario that occurs when server-side filter pushdown matches
 * some batches but filters out trailing batches.
 */
class CommonRpcMessageUtilsTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, null, 0);
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");

    /**
     * Reproduces the bug where client discards records when filteredEndOffset is also present.
     *
     * <p>Scenario: Server-side filter matches some batches (records non-empty) but trailing batches
     * are filtered out (filteredEndOffset set). The server sends BOTH records and filteredEndOffset
     * in the response. The client must preserve both — records for consumption, filteredEndOffset
     * for offset advancement past filtered trailing batches.
     *
     * <p>Bug: The client deserialization in {@link
     * CommonRpcMessageUtils#getFetchLogResultForBucket} treats records and filteredEndOffset as
     * mutually exclusive. When filteredEndOffset is present, it constructs a
     * FetchLogResultForBucket with records=null, discarding all matched data.
     */
    @Test
    void testGetFetchLogResultPreservesRecordsWhenFilteredEndOffsetPresent() {
        // Simulate a server response with BOTH records and filteredEndOffset.
        // This happens when some batches match the filter (included in records)
        // but trailing batches are filtered out (filteredEndOffset points past them).
        byte[] recordBytes = new byte[] {0x01, 0x02, 0x03, 0x04};
        long highWatermark = 100L;
        long filteredEndOffset = 50L;

        PbFetchLogRespForBucket respForBucket =
                new PbFetchLogRespForBucket()
                        .setBucketId(0)
                        .setHighWatermark(highWatermark)
                        .setLogStartOffset(0L)
                        .setRecords(recordBytes)
                        .setFilteredEndOffset(filteredEndOffset);

        // Verify the PB message has both fields set
        assertThat(respForBucket.hasRecords()).isTrue();
        assertThat(respForBucket.hasFilteredEndOffset()).isTrue();

        FetchLogResultForBucket result =
                CommonRpcMessageUtils.getFetchLogResultForBucket(
                        TABLE_BUCKET, TABLE_PATH, respForBucket);

        // The result MUST contain records — they should not be discarded
        assertThat(result.records())
                .as(
                        "Records must not be discarded when filteredEndOffset is also present. "
                                + "The server sends both when some batches match the filter "
                                + "but trailing batches are filtered out.")
                .isNotNull();
        assertThat(result.records()).isNotEqualTo(MemoryLogRecords.EMPTY);
        assertThat(result.records().sizeInBytes()).isEqualTo(recordBytes.length);

        // filteredEndOffset must also be preserved
        assertThat(result.hasFilteredEndOffset()).isTrue();
        assertThat(result.getFilteredEndOffset()).isEqualTo(filteredEndOffset);

        // highWatermark must be correct
        assertThat(result.getHighWatermark()).isEqualTo(highWatermark);
    }

    /** Verifies that filteredEndOffset-only responses (empty records) still work correctly. */
    @Test
    void testGetFetchLogResultWithFilteredEndOffsetOnly() {
        // When ALL batches are filtered out, server sends filteredEndOffset with empty records.
        long highWatermark = 100L;
        long filteredEndOffset = 50L;

        PbFetchLogRespForBucket respForBucket =
                new PbFetchLogRespForBucket()
                        .setBucketId(0)
                        .setHighWatermark(highWatermark)
                        .setLogStartOffset(0L)
                        .setRecords(new byte[0])
                        .setFilteredEndOffset(filteredEndOffset);

        FetchLogResultForBucket result =
                CommonRpcMessageUtils.getFetchLogResultForBucket(
                        TABLE_BUCKET, TABLE_PATH, respForBucket);

        assertThat(result.hasFilteredEndOffset()).isTrue();
        assertThat(result.getFilteredEndOffset()).isEqualTo(filteredEndOffset);
    }

    /** Verifies that normal responses (records only, no filter) still work correctly. */
    @Test
    void testGetFetchLogResultWithRecordsOnly() {
        byte[] recordBytes = new byte[] {0x01, 0x02, 0x03, 0x04};
        long highWatermark = 100L;

        PbFetchLogRespForBucket respForBucket =
                new PbFetchLogRespForBucket()
                        .setBucketId(0)
                        .setHighWatermark(highWatermark)
                        .setLogStartOffset(0L)
                        .setRecords(recordBytes);

        FetchLogResultForBucket result =
                CommonRpcMessageUtils.getFetchLogResultForBucket(
                        TABLE_BUCKET, TABLE_PATH, respForBucket);

        assertThat(result.records()).isNotNull();
        assertThat(result.records().sizeInBytes()).isEqualTo(recordBytes.length);
        assertThat(result.hasFilteredEndOffset()).isFalse();
    }
}
