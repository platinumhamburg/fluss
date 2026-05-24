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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IndexTableAutoDerive}. */
class IndexTableAutoDeriveTest {

    private static final TablePath MAIN_PATH = TablePath.of("db", "orders");
    private static final long MAIN_TABLE_ID = 42L;

    @Test
    void testDeriveProducesOneTablePerIndex() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("region_id", DataTypes.BIGINT())
                        .primaryKey("order_id")
                        .index("idx_user", "user_id")
                        .index("idx_region", "region_id")
                        .build();
        TableDescriptor main = TableDescriptor.builder().schema(schema).distributedBy(4).build();

        List<TableDescriptor> derived =
                IndexTableAutoDerive.deriveIndexTables(main, MAIN_TABLE_ID, MAIN_PATH);

        assertThat(derived).hasSize(2);
        // back-link properties identify the index by main-table name + tableId; the derived
        // descriptor itself does not carry the index name. The order matches Schema.getIndexes().
        assertThat(derived)
                .allSatisfy(
                        d -> {
                            assertThat(d.getProperties())
                                    .containsEntry(
                                            ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID.key(),
                                            String.valueOf(MAIN_TABLE_ID))
                                    .containsEntry(
                                            ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_NAME.key(),
                                            "db.orders");
                        });
        // the schemas distinguish the two index tables: idx_user has user_id, idx_region has
        // region_id.
        assertThat(derived.get(0).getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .contains("user_id");
        assertThat(derived.get(1).getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .contains("region_id");
    }

    @Test
    void testDeriveProducesEmptyListForNoIndex() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();
        TableDescriptor main = TableDescriptor.builder().schema(schema).distributedBy(2).build();

        List<TableDescriptor> derived =
                IndexTableAutoDerive.deriveIndexTables(main, MAIN_TABLE_ID, MAIN_PATH);

        assertThat(derived).isEmpty();
    }

    @Test
    void testDerivedDescriptorsHavePartitionIdColumn() {
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
                        .distributedBy(4, "order_id")
                        .build();

        List<TableDescriptor> derived =
                IndexTableAutoDerive.deriveIndexTables(main, MAIN_TABLE_ID, MAIN_PATH);

        assertThat(derived).hasSize(1);
        assertThat(derived.get(0).getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .contains(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
    }

    @Test
    void testDerivedDescriptorsAreIndexTableType() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .build();
        TableDescriptor main = TableDescriptor.builder().schema(schema).distributedBy(3).build();

        List<TableDescriptor> derived =
                IndexTableAutoDerive.deriveIndexTables(main, MAIN_TABLE_ID, MAIN_PATH);

        assertThat(derived).hasSize(1);
        assertThat(derived.get(0).isIndexTable()).isTrue();
        assertThat(main.isIndexTable()).isFalse();
    }
}
