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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexTableUtilsTest {

    @Test
    void testPartitionIdSystemColumnNameIsUnderscored() {
        assertThat(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN).isEqualTo("__partition_id");
    }

    @Test
    void testReservedIndexSystemColumnSetContainsOnlyPartitionId() {
        assertThat(IndexTableUtils.RESERVED_INDEX_SYSTEM_COLUMNS)
                .containsExactly("__partition_id");
    }

    @Test
    void testEncodeDecodePartitionIdPrefixIsBigEndian8Bytes() {
        long pid = 0x0102030405060708L;
        byte[] body = new byte[] {(byte) 0xAA, (byte) 0xBB};
        byte[] encoded = IndexTableUtils.prependPartitionIdPrefix(pid, body);

        assertThat(encoded).hasSize(10);
        assertThat(encoded[0]).isEqualTo((byte) 0x01);
        assertThat(encoded[7]).isEqualTo((byte) 0x08);
        assertThat(encoded[8]).isEqualTo((byte) 0xAA);
        assertThat(encoded[9]).isEqualTo((byte) 0xBB);

        assertThat(IndexTableUtils.decodePartitionIdPrefix(encoded)).isEqualTo(pid);
        assertThat(IndexTableUtils.stripPartitionIdPrefix(encoded)).isEqualTo(body);
    }

    @Test
    void testDecodeThrowsWhenValueShorterThanPrefix() {
        assertThatThrownBy(() -> IndexTableUtils.decodePartitionIdPrefix(new byte[] {0, 1, 2}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDecodeThrowsWhenValueIsNull() {
        assertThatThrownBy(() -> IndexTableUtils.decodePartitionIdPrefix(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testStripThrowsWhenValueShorterThanPrefix() {
        assertThatThrownBy(() -> IndexTableUtils.stripPartitionIdPrefix(new byte[] {0, 1}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPrependRejectsNullValueBody() {
        assertThatThrownBy(() -> IndexTableUtils.prependPartitionIdPrefix(1L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptyBodyStillRoundTrips() {
        byte[] encoded = IndexTableUtils.prependPartitionIdPrefix(42L, new byte[0]);
        assertThat(IndexTableUtils.decodePartitionIdPrefix(encoded)).isEqualTo(42L);
        assertThat(IndexTableUtils.stripPartitionIdPrefix(encoded)).isEmpty();
    }

    @Test
    void testNegativePartitionIdRoundTripsAsBigEndian() {
        long pid = -1L;
        byte[] encoded = IndexTableUtils.prependPartitionIdPrefix(pid, new byte[] {1});
        // All 8 prefix bytes are 0xFF for -1
        for (int i = 0; i < 8; i++) {
            assertThat(encoded[i]).isEqualTo((byte) 0xFF);
        }
        assertThat(IndexTableUtils.decodePartitionIdPrefix(encoded)).isEqualTo(pid);
    }

    @Test
    void testPartitionIdPrefixSizeIs8() {
        assertThat(IndexTableUtils.PARTITION_ID_PREFIX_SIZE).isEqualTo(8);
    }
}
