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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end Foundation integration test for FIP V2.
 *
 * <p>Validates that Schema.index DDL + namespaced properties + TableInfo back-link accessors
 * compose into a coherent Index Table foundation.
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
                        .property(ConfigOptions.INDEX_VISIBILITY, IndexVisibility.SYNC)
                        .build();

        Schema indexSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        new Schema.Column("user_id", DataTypes.BIGINT()),
                                        new Schema.Column("order_id", DataTypes.BIGINT()),
                                        new Schema.Column("dt", DataTypes.STRING()),
                                        new Schema.Column(
                                                IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                                                DataTypes.BIGINT().copy(false))))
                        .primaryKey("user_id", "order_id", "dt")
                        .build();
        TableDescriptor derived =
                TableDescriptor.builder()
                        .schema(indexSchema)
                        .kvFormat(KvFormat.ALIGNED)
                        .logFormat(LogFormat.COMPACTED)
                        .changelogImage(ChangelogImage.WAL)
                        .distributedBy(8, "user_id")
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, 100L)
                        .build();

        assertThat(derived.isIndexTable()).isTrue();
        assertThat(derived.getKvFormat()).isEqualTo(KvFormat.ALIGNED);
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
                        TablePath.of("tdb", IndexTableUtils.indexTableName("orders", "idx_user")),
                        200L,
                        1,
                        derived,
                        null,
                        now,
                        now);

        assertThat(info.isIndexTable()).isTrue();
        assertThat(info.getMainTableId()).hasValue(100L);
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
