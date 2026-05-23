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

package org.apache.fluss.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.SchemaJsonSerde;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end Foundation integration test for FIP V2.
 *
 * <p>Validates that Schema.index DDL + namespaced properties + descriptor derivation + TableInfo
 * back-link accessors compose into a coherent Index Table foundation.
 */
class IndexTableFoundationITCase {

    @Test
    void testSchemaIndexPlusNamespacedPropertiesRoundTrip() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .index("idx_user", "user_id")
                        .build();

        TableDescriptor main =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_user"), "8")
                        .property(
                                ConfigOptions.SECONDARY_INDEX_VISIBILITY,
                                SecondaryIndexVisibility.SYNC)
                        .build();

        TableDescriptor derived =
                TableDescriptor.deriveIndexTableDescriptor(main, 100L, "tdb.orders", "idx_user");

        assertThat(derived.isIndexTable()).isTrue();
        assertThat(derived.getKvFormat()).isEqualTo(KvFormat.COMPACTED);
        assertThat(derived.getLogFormat()).isEqualTo(LogFormat.COMPACTED);
        assertThat(derived.getChangelogImage()).isEqualTo(ChangelogImage.WAL);
        assertThat(derived.getTableDistribution()).isPresent();
        assertThat(derived.getTableDistribution().get().getBucketCount()).hasValue(8);
        assertThat(derived.getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .contains(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);

        long now = System.currentTimeMillis();
        TableInfo info =
                TableInfo.of(
                        TablePath.of("tdb", "orders$idx_user"),
                        200L,
                        1,
                        derived,
                        null,
                        now,
                        now);

        assertThat(info.isIndexTable()).isTrue();
        assertThat(info.getMainTableId()).hasValue(100L);
        assertThat(info.getMainTableName()).hasValue("tdb.orders");
    }

    @Test
    void testLegacyAliasYieldsIdxDefaultNamespacedProperties() {
        Map<String, String> input = new HashMap<>();
        input.put("table.secondary-index.columns", "user_id");

        Map<String, String> normalized = TableDescriptor.translateLegacyIndexProperties(input);

        assertThat(normalized)
                .doesNotContainKey("table.secondary-index.columns")
                .containsEntry("secondary-index.idx_default.columns", "user_id");
    }

    @Test
    void testSchemaJsonSerdeRoundTripPreservesIndexes() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .column("r", DataTypes.INT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .index("idx_r", "r")
                        .build();

        byte[] bytes = JsonSerdeUtils.writeValueAsBytes(schema, SchemaJsonSerde.INSTANCE);
        Schema decoded = JsonSerdeUtils.readValue(bytes, SchemaJsonSerde.INSTANCE);

        assertThat(decoded.getIndexes()).isEqualTo(schema.getIndexes());
    }
}
