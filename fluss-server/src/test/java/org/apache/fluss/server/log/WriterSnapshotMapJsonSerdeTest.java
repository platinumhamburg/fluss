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

package org.apache.fluss.server.log;

import org.apache.fluss.record.WriterKey;
import org.apache.fluss.server.log.WriterStateManager.FencedWriterSnapshotEntry;
import org.apache.fluss.server.log.WriterStateManager.FencedWriterSnapshotMap;
import org.apache.fluss.server.log.WriterStateManager.WriterSnapshotEntry;
import org.apache.fluss.server.log.WriterStateManager.WriterSnapshotMap;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.server.log.WriterStateManager.WriterSnapshotMapJsonSerde}. */
public class WriterSnapshotMapJsonSerdeTest extends JsonSerdeTestBase<WriterSnapshotMap> {

    public WriterSnapshotMapJsonSerdeTest() {
        super(WriterStateManager.WriterSnapshotMapJsonSerde.INSTANCE);
    }

    @Override
    protected WriterSnapshotMap[] createObjects() {
        List<WriterSnapshotEntry> entries =
                Arrays.asList(
                        new WriterSnapshotEntry(1001, 23, 100, 1000, 2000),
                        new WriterSnapshotEntry(1001, 25, 200, 3000, 4000),
                        new WriterSnapshotEntry(1002, 33, 300, 4000, 5000));
        WriterSnapshotMap map = new WriterSnapshotMap(entries);
        return new WriterSnapshotMap[] {map};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"writer_id_entries\":["
                    + "{\"writer_id\":1001,\"last_batch_sequence\":23,\"last_batch_base_offset\":100,\"offset_delta\":1000,\"last_batch_timestamp\":2000},"
                    + "{\"writer_id\":1001,\"last_batch_sequence\":25,\"last_batch_base_offset\":200,\"offset_delta\":3000,\"last_batch_timestamp\":4000},"
                    + "{\"writer_id\":1002,\"last_batch_sequence\":33,\"last_batch_base_offset\":300,\"offset_delta\":4000,\"last_batch_timestamp\":5000}]}"
        };
    }

    @Test
    void testFencedSnapshotV2ExactSchemaAndRoundTrip() {
        FencedWriterSnapshotMap expected =
                new FencedWriterSnapshotMap(
                        Collections.singletonList(
                                new FencedWriterSnapshotEntry(
                                        new WriterKey(Long.MAX_VALUE, Long.MIN_VALUE | 3L),
                                        (long) Integer.MAX_VALUE + 1L,
                                        Long.MAX_VALUE - 1L,
                                        42L)));

        byte[] json =
                JsonSerdeUtils.writeValueAsBytes(
                        expected, WriterStateManager.FencedWriterSnapshotMapJsonSerde.INSTANCE);

        assertThat(new String(json, StandardCharsets.UTF_8))
                .isEqualTo(
                        "{\"version\":2,\"kv_idempotence_protocol_version\":1,\"writer_entries\":[{"
                                + "\"writer_key_high\":9223372036854775807,"
                                + "\"writer_key_low\":-9223372036854775805,"
                                + "\"last_sequence\":2147483648,"
                                + "\"last_target_wal_offset\":9223372036854775806,"
                                + "\"last_timestamp\":42}]}");
        assertThat(
                        JsonSerdeUtils.readValue(
                                json, WriterStateManager.FencedWriterSnapshotMapJsonSerde.INSTANCE))
                .isEqualTo(expected);
    }

    @Test
    void testFencedSnapshotRejectsWrongOrMissingProtocol() {
        assertInvalidFencedSnapshot(
                "{\"version\":1,\"kv_idempotence_protocol_version\":1,\"writer_entries\":[]}");
        assertInvalidFencedSnapshot("{\"version\":2,\"writer_entries\":[]}");
        assertInvalidFencedSnapshot(
                "{\"version\":2,\"kv_idempotence_protocol_version\":0,\"writer_entries\":[]}");
    }

    @Test
    void testFencedSnapshotRejectsDuplicateWriterKeys() {
        String entry =
                "{\"writer_key_high\":4,\"writer_key_low\":5,\"last_sequence\":100,"
                        + "\"last_target_wal_offset\":10,\"last_timestamp\":1}";
        assertInvalidFencedSnapshot(
                "{\"version\":2,\"kv_idempotence_protocol_version\":1,\"writer_entries\":["
                        + entry
                        + ","
                        + entry
                        + "]}");
    }

    @Test
    void testFencedSnapshotRejectsNegativeSequence() {
        assertInvalidFencedSnapshot(
                validFencedSnapshotEntry()
                        .replace("\"last_sequence\":100", "\"last_sequence\":-1"));
    }

    @Test
    void testFencedSnapshotRejectsMissingOrMalformedFields() {
        assertInvalidFencedSnapshot(
                validFencedSnapshotEntry().replace("\"writer_key_high\":4,", ""));
        assertInvalidFencedSnapshot(
                validFencedSnapshotEntry()
                        .replace("\"writer_key_low\":5", "\"writer_key_low\":\"5\""));
        assertInvalidFencedSnapshot(
                validFencedSnapshotEntry()
                        .replace("\"last_timestamp\":1", "\"last_timestamp\":1.5"));
    }

    private static String validFencedSnapshotEntry() {
        return "{\"version\":2,\"kv_idempotence_protocol_version\":1,\"writer_entries\":[{"
                + "\"writer_key_high\":4,\"writer_key_low\":5,\"last_sequence\":100,"
                + "\"last_target_wal_offset\":10,\"last_timestamp\":1}]}";
    }

    private static void assertInvalidFencedSnapshot(String json) {
        assertThatThrownBy(
                        () ->
                                JsonSerdeUtils.readValue(
                                        json.getBytes(StandardCharsets.UTF_8),
                                        WriterStateManager.FencedWriterSnapshotMapJsonSerde
                                                .INSTANCE))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
