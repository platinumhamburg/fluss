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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CoordinatorService#indexTablePathsFor(TablePath, Schema)} — the helper that
 * computes the set of derived Index Table paths to cascade-drop when the main table is dropped.
 */
class CoordinatorServiceCascadeDropTest {

    private static final TablePath MAIN_PATH = TablePath.of("db", "orders");

    @Test
    void testReturnsOneIndexPathPerSchemaIndexInOrder() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("region_id", DataTypes.BIGINT())
                        .primaryKey("order_id")
                        .index("idx_user", "user_id")
                        .index("idx_region", "region_id")
                        .build();

        List<TablePath> paths = CoordinatorService.indexTablePathsFor(MAIN_PATH, schema);

        assertThat(paths)
                .containsExactly(
                        TablePath.of(
                                "db",
                                IndexTableUtils.indexTableName("orders", "idx_user")),
                        TablePath.of(
                                "db",
                                IndexTableUtils.indexTableName("orders", "idx_region")));
    }

    @Test
    void testReturnsEmptyForSchemaWithoutIndexes() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        List<TablePath> paths = CoordinatorService.indexTablePathsFor(MAIN_PATH, schema);

        assertThat(paths).isEmpty();
    }

    @Test
    void testIndexPathsLiveInMainTableDatabase() {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.BIGINT())
                        .column("v", DataTypes.BIGINT())
                        .primaryKey("k")
                        .index("idx_v", "v")
                        .build();
        TablePath mainPath = TablePath.of("warehouse", "events");

        List<TablePath> paths = CoordinatorService.indexTablePathsFor(mainPath, schema);

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0).getDatabaseName()).isEqualTo("warehouse");
        assertThat(paths.get(0).getTableName())
                .isEqualTo(IndexTableUtils.indexTableName("events", "idx_v"));
    }
}
