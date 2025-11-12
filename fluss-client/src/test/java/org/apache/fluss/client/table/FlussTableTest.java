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

package org.apache.fluss.client.table;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FlussTable}. */
class FlussTableTest {

    private TableInfo primaryKeyTableInfo;
    private TableInfo logTableInfo;

    @BeforeEach
    void setUp() {
        // Create a primary key table schema
        Schema primaryKeySchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor primaryKeyDescriptor =
                TableDescriptor.builder().schema(primaryKeySchema).distributedBy(3, "id").build();
        primaryKeyTableInfo =
                TableInfo.of(
                        new TablePath("test", "pk_table"),
                        1L,
                        0,
                        primaryKeyDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        // Create a log table schema (no primary key)
        Schema logSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor logDescriptor =
                TableDescriptor.builder().schema(logSchema).distributedBy(3).build();
        logTableInfo =
                TableInfo.of(
                        new TablePath("test", "log_table"),
                        2L,
                        0,
                        logDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
    }

    @Test
    void testIndexTableDetection() throws Exception {
        // Test index table name detection
        TablePath indexTablePath1 = new TablePath("test", "__pk_table__index__name_idx");
        try (FlussTable indexTable1 =
                new FlussTable(null, indexTablePath1, primaryKeyTableInfo, new Configuration())) {
            assertThat(indexTable1.isIndexTable()).isTrue();
        }

        TablePath indexTablePath2 = new TablePath("db", "__users__index__email_status_idx");
        try (FlussTable indexTable2 =
                new FlussTable(null, indexTablePath2, primaryKeyTableInfo, new Configuration())) {
            assertThat(indexTable2.isIndexTable()).isTrue();
        }

        // Test regular table names (should not be detected as index tables)
        TablePath regularTablePath1 = new TablePath("test", "regular_table");
        try (FlussTable regularTable1 =
                new FlussTable(null, regularTablePath1, primaryKeyTableInfo, new Configuration())) {
            assertThat(regularTable1.isIndexTable()).isFalse();
        }

        TablePath regularTablePath2 = new TablePath("test", "__not_index_table__something");
        try (FlussTable regularTable2 =
                new FlussTable(null, regularTablePath2, primaryKeyTableInfo, new Configuration())) {
            assertThat(regularTable2.isIndexTable()).isFalse();
        }

        TablePath regularTablePath3 = new TablePath("test", "table__index");
        try (FlussTable regularTable3 =
                new FlussTable(null, regularTablePath3, primaryKeyTableInfo, new Configuration())) {
            assertThat(regularTable3.isIndexTable()).isFalse();
        }

        TablePath regularTablePath4 = new TablePath("test", "index__table");
        try (FlussTable regularTable4 =
                new FlussTable(null, regularTablePath4, primaryKeyTableInfo, new Configuration())) {
            assertThat(regularTable4.isIndexTable()).isFalse();
        }
    }

    @Test
    void testIndexTableNewUpsertThrowsException() throws Exception {
        TablePath indexTablePath = new TablePath("test", "__pk_table__index__name_idx");
        try (FlussTable indexTable =
                new FlussTable(null, indexTablePath, primaryKeyTableInfo, new Configuration())) {
            // Should throw exception when trying to create upsert writer for index table
            assertThatThrownBy(() -> indexTable.newUpsert())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Index table")
                    .hasMessageContaining("doesn't support UpsertWriter")
                    .hasMessageContaining("internal roles");
        }
    }

    @Test
    void testIndexTableNewAppendThrowsException() throws Exception {
        TablePath indexTablePath = new TablePath("test", "__log_table__index__name_idx");
        try (FlussTable indexTable =
                new FlussTable(null, indexTablePath, logTableInfo, new Configuration())) {
            // Should throw exception when trying to create append writer for index table
            assertThatThrownBy(() -> indexTable.newAppend())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Index table")
                    .hasMessageContaining("doesn't support AppendWriter")
                    .hasMessageContaining("internal roles");
        }
    }
}
