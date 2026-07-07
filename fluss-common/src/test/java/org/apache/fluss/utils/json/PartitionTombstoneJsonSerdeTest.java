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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.PartitionTombstone;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionTombstoneJsonSerde}. */
class PartitionTombstoneJsonSerdeTest {

    @Test
    void testRoundTrip() {
        PartitionTombstone original =
                new PartitionTombstone(5L, new HashSet<>(Arrays.asList(7L, 9L)), 3L);
        byte[] bytes =
                JsonSerdeUtils.writeValueAsBytes(original, PartitionTombstoneJsonSerde.INSTANCE);
        PartitionTombstone decoded =
                JsonSerdeUtils.readValue(bytes, PartitionTombstoneJsonSerde.INSTANCE);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void testEmptyRoundTrip() {
        byte[] bytes =
                JsonSerdeUtils.writeValueAsBytes(
                        PartitionTombstone.EMPTY, PartitionTombstoneJsonSerde.INSTANCE);
        PartitionTombstone decoded =
                JsonSerdeUtils.readValue(bytes, PartitionTombstoneJsonSerde.INSTANCE);
        assertThat(decoded).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testEmittedJsonContainsExpectedFields() {
        PartitionTombstone original =
                new PartitionTombstone(5L, new HashSet<>(Arrays.asList(7L, 9L)), 3L);
        byte[] bytes =
                JsonSerdeUtils.writeValueAsBytes(original, PartitionTombstoneJsonSerde.INSTANCE);
        String json = new String(bytes, StandardCharsets.UTF_8);
        assertThat(json).contains("\"floor\":5");
        assertThat(json).contains("\"explicit_set\":[");
        assertThat(json).contains("\"tombstone_version\":3");
    }

    @Test
    void testEmptySetSerializesAsEmptyArray() {
        byte[] bytes =
                JsonSerdeUtils.writeValueAsBytes(
                        PartitionTombstone.EMPTY, PartitionTombstoneJsonSerde.INSTANCE);
        String json = new String(bytes, StandardCharsets.UTF_8);
        assertThat(json).contains("\"floor\":-1");
        assertThat(json).contains("\"explicit_set\":[]");
        assertThat(json).contains("\"tombstone_version\":0");
    }
}
