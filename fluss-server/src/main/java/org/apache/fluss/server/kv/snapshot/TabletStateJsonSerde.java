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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Json serializer and deserializer for {@link TabletState}.
 *
 * <p>This serde uses the same JSON field names as {@link CompletedSnapshotJsonSerde} for the
 * TabletState fields (row_count, auto_inc_id_range, index_replication_offsets), so it can also
 * parse old {@code _METADATA} files that contain these fields mixed with snapshot metadata.
 */
public class TabletStateJsonSerde
        implements JsonSerializer<TabletState>, JsonDeserializer<TabletState> {

    public static final TabletStateJsonSerde INSTANCE = new TabletStateJsonSerde();

    // Reuse the same JSON field names from CompletedSnapshotJsonSerde
    private static final String ROW_COUNT = "row_count";
    private static final String AUTO_INC_ID_RANGE = "auto_inc_id_range";
    private static final String AUTO_INC_COLUMN_ID = "column_id";
    private static final String AUTO_INC_ID_START = "start";
    private static final String AUTO_INC_ID_END = "end";
    private static final String INDEX_REPLICATION_OFFSETS = "index_replication_offsets";
    private static final String INDEX_OFFSET_TABLE_ID = "table_id";
    private static final String INDEX_OFFSET_PARTITION_ID = "partition_id";
    private static final String INDEX_OFFSET_BUCKET_ID = "bucket_id";
    private static final String INDEX_OFFSET_OFFSET = "offset";

    @Override
    public void serialize(TabletState tabletState, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // serialize row count if exists
        if (tabletState.getRowCount() != null) {
            generator.writeNumberField(ROW_COUNT, tabletState.getRowCount());
        }

        // serialize auto-increment id range for each auto-increment column
        if (tabletState.getAutoIncIDRanges() != null
                && !tabletState.getAutoIncIDRanges().isEmpty()) {
            generator.writeArrayFieldStart(AUTO_INC_ID_RANGE);
            for (AutoIncIDRange autoIncIDRange : tabletState.getAutoIncIDRanges()) {
                generator.writeStartObject();
                generator.writeNumberField(AUTO_INC_COLUMN_ID, autoIncIDRange.getColumnId());
                generator.writeNumberField(AUTO_INC_ID_START, autoIncIDRange.getStart());
                generator.writeNumberField(AUTO_INC_ID_END, autoIncIDRange.getEnd());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }

        // serialize index replication offsets if exists
        if (tabletState.getIndexReplicationOffsets() != null
                && !tabletState.getIndexReplicationOffsets().isEmpty()) {
            generator.writeArrayFieldStart(INDEX_REPLICATION_OFFSETS);
            for (Map.Entry<TableBucket, Long> entry :
                    tabletState.getIndexReplicationOffsets().entrySet()) {
                TableBucket tb = entry.getKey();
                generator.writeStartObject();
                generator.writeNumberField(INDEX_OFFSET_TABLE_ID, tb.getTableId());
                if (tb.getPartitionId() != null) {
                    generator.writeNumberField(INDEX_OFFSET_PARTITION_ID, tb.getPartitionId());
                }
                generator.writeNumberField(INDEX_OFFSET_BUCKET_ID, tb.getBucket());
                generator.writeNumberField(INDEX_OFFSET_OFFSET, entry.getValue());
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }

        generator.writeEndObject();
    }

    @Override
    public TabletState deserialize(JsonNode node) {
        Long rowCount = null;
        if (node.has(ROW_COUNT)) {
            rowCount = node.get(ROW_COUNT).asLong();
        }

        List<AutoIncIDRange> autoIncIDRanges = null;
        if (node.has(AUTO_INC_ID_RANGE)) {
            autoIncIDRanges = new ArrayList<>();
            for (JsonNode autoIncIDRangeNode : node.get(AUTO_INC_ID_RANGE)) {
                int columnId = autoIncIDRangeNode.get(AUTO_INC_COLUMN_ID).asInt();
                long start = autoIncIDRangeNode.get(AUTO_INC_ID_START).asLong();
                long end = autoIncIDRangeNode.get(AUTO_INC_ID_END).asLong();
                autoIncIDRanges.add(new AutoIncIDRange(columnId, start, end));
            }
        }

        Map<TableBucket, Long> indexReplicationOffsets = null;
        if (node.has(INDEX_REPLICATION_OFFSETS)) {
            JsonNode offsetsNode = node.get(INDEX_REPLICATION_OFFSETS);
            Map<TableBucket, Long> offsets = new HashMap<>();
            if (offsetsNode.isArray()) {
                for (JsonNode entryNode : offsetsNode) {
                    long tableId = entryNode.get(INDEX_OFFSET_TABLE_ID).asLong();
                    Long partId =
                            entryNode.has(INDEX_OFFSET_PARTITION_ID)
                                    ? entryNode.get(INDEX_OFFSET_PARTITION_ID).asLong()
                                    : null;
                    int bucketId = entryNode.get(INDEX_OFFSET_BUCKET_ID).asInt();
                    long offset = entryNode.get(INDEX_OFFSET_OFFSET).asLong();
                    offsets.put(new TableBucket(tableId, partId, bucketId), offset);
                }
            } else {
                // Legacy format: JSON object with string keys
                offsetsNode
                        .fields()
                        .forEachRemaining(
                                entry -> {
                                    String key = entry.getKey();
                                    long offset = entry.getValue().asLong();
                                    offsets.put(parseLegacyBucketKey(key), offset);
                                });
            }
            indexReplicationOffsets = offsets;
        }

        // flushedLogOffset is not stored in _TABLET_STATE; use 0 as default
        return new TabletState(0, rowCount, autoIncIDRanges, indexReplicationOffsets);
    }

    /**
     * Parses a legacy string key format into a {@link TableBucket}. The legacy format is
     * "tableId:partitionId:bucketId" for partitioned tables or "tableId:bucketId" for
     * non-partitioned tables.
     */
    private static TableBucket parseLegacyBucketKey(String key) {
        String[] parts = key.split(":");
        if (parts.length == 3) {
            return new TableBucket(
                    Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
        } else if (parts.length == 2) {
            return new TableBucket(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
        } else {
            throw new IllegalArgumentException(
                    "Invalid legacy bucket key format: '"
                            + key
                            + "', expected 'tableId:bucketId' or 'tableId:partitionId:bucketId'");
        }
    }

    /** Serialize the {@link TabletState} to json bytes. */
    public static byte[] toJson(TabletState tabletState) {
        return JsonSerdeUtils.writeValueAsBytes(tabletState, INSTANCE);
    }

    /** Deserialize the json bytes to {@link TabletState}. */
    public static TabletState fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }
}
