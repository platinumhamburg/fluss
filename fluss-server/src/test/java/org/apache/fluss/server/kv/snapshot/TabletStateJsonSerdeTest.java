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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletStateJsonSerde}. */
class TabletStateJsonSerdeTest extends JsonSerdeTestBase<TabletState> {

    protected TabletStateJsonSerdeTest() {
        super(TabletStateJsonSerde.INSTANCE);
    }

    @Override
    protected void assertEquals(TabletState actual, TabletState expected) {
        assertThat(actual.getRowCount()).isEqualTo(expected.getRowCount());
        assertThat(actual.getAutoIncIDRanges()).isEqualTo(expected.getAutoIncIDRanges());
        assertThat(actual.getIndexReplicationOffsets())
                .isEqualTo(expected.getIndexReplicationOffsets());
    }

    @Override
    protected TabletState[] createObjects() {
        // Empty tablet state
        TabletState ts1 = new TabletState(0, null, null, null);

        // With row count only
        TabletState ts2 = new TabletState(0, 1234L, null, null);

        // With row count and auto-inc ID ranges
        TabletState ts3 =
                new TabletState(
                        0,
                        5678L,
                        Collections.singletonList(new AutoIncIDRange(2, 10000, 20000)),
                        null);

        // With index replication offsets (non-partitioned)
        Map<TableBucket, Long> offsets4 = new HashMap<>();
        offsets4.put(new TableBucket(1, 0), 100L);
        TabletState ts4 = new TabletState(0, null, null, offsets4);

        // With index replication offsets (partitioned)
        Map<TableBucket, Long> offsets5 = new HashMap<>();
        offsets5.put(new TableBucket(1, 10L, 0), 200L);
        TabletState ts5 = new TabletState(0, null, null, offsets5);

        return new TabletState[] {ts1, ts2, ts3, ts4, ts5};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{}",
            "{\"row_count\":1234}",
            "{\"row_count\":5678,\"auto_inc_id_range\":[{\"column_id\":2,\"start\":10000,\"end\":20000}]}",
            "{\"index_replication_offsets\":[{\"table_id\":1,\"bucket_id\":0,\"offset\":100}]}",
            "{\"index_replication_offsets\":[{\"table_id\":1,\"partition_id\":10,\"bucket_id\":0,\"offset\":200}]}"
        };
    }

    @Test
    void testDeserializeFromOldMetadataFormat() {
        // Simulate parsing TabletState fields from an old _METADATA file that contains
        // extra fields (version, table_id, etc.) mixed with TabletState fields.
        // TabletStateJsonSerde should tolerate unknown fields and extract only what it needs.
        String oldMetadataJson =
                "{\"version\":1,"
                        + "\"table_id\":2,\"bucket_id\":0,"
                        + "\"snapshot_id\":2,"
                        + "\"snapshot_location\":\"oss://bucket/snapshot\","
                        + "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[],"
                        + "\"private_file_handles\":[],"
                        + "\"snapshot_incremental_size\":0},"
                        + "\"log_offset\":20,"
                        + "\"row_count\":999,"
                        + "\"auto_inc_id_range\":[{\"column_id\":1,\"start\":100,\"end\":200}],"
                        + "\"index_replication_offsets\":[{\"table_id\":1,\"bucket_id\":0,\"offset\":50}]}";

        TabletState tabletState =
                TabletStateJsonSerde.fromJson(oldMetadataJson.getBytes(StandardCharsets.UTF_8));

        assertThat(tabletState.getRowCount()).isEqualTo(999L);
        assertThat(tabletState.getAutoIncIDRanges()).hasSize(1);
        assertThat(tabletState.getAutoIncIDRanges().get(0).getColumnId()).isEqualTo(1);
        assertThat(tabletState.getAutoIncIDRanges().get(0).getStart()).isEqualTo(100L);
        assertThat(tabletState.getAutoIncIDRanges().get(0).getEnd()).isEqualTo(200L);
        assertThat(tabletState.getIndexReplicationOffsets()).hasSize(1);
        assertThat(tabletState.getIndexReplicationOffsets().get(new TableBucket(1, 0)))
                .isEqualTo(50L);
    }

    @Test
    void testDeserializeLegacyIndexOffsetFormat() {
        // Legacy format uses JSON object with string keys
        String legacyJson =
                "{\"row_count\":100," + "\"index_replication_offsets\":{\"1:0\":50,\"1:10:2\":75}}";

        TabletState tabletState =
                TabletStateJsonSerde.fromJson(legacyJson.getBytes(StandardCharsets.UTF_8));

        assertThat(tabletState.getRowCount()).isEqualTo(100L);
        assertThat(tabletState.getIndexReplicationOffsets()).hasSize(2);
        assertThat(tabletState.getIndexReplicationOffsets().get(new TableBucket(1, 0)))
                .isEqualTo(50L);
        assertThat(tabletState.getIndexReplicationOffsets().get(new TableBucket(1, 10L, 2)))
                .isEqualTo(75L);
    }
}
