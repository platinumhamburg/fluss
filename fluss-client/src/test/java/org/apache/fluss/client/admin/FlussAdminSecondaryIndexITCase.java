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

package org.apache.fluss.client.admin;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for secondary index DDL operations via FlussAdmin. Migrated from V1
 * FlussAdminITCase and adapted to V2 index table naming and schema.
 */
class FlussAdminSecondaryIndexITCase extends ClientToServerITCaseBase {

    private static final String DB = "test_db_admin_idx";

    @Test
    void testCreateTableWithGlobalSecondaryIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_table_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("city", DataTypes.STRING())
                        .primaryKey("id")
                        .index("name_idx", "name")
                        .index("age_city_idx", "age", "city")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .comment("test table with global secondary index")
                        .distributedBy(3, "id")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("name_idx"), "3")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("age_city_idx"), "3")
                        .build();

        createTable(tablePath, descriptor, true);

        // Verify main table
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.getSchemaId()).isEqualTo(1);
        assertThat(mainTableInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(2);

        // Verify index tables (V2 naming: mainTable__indexName)
        TablePath nameIndexTablePath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName("test_table_with_index", "name_idx"));
        TablePath ageCityIndexTablePath =
                TablePath.of(
                        DB,
                        IndexTableUtils.indexTableName("test_table_with_index", "age_city_idx"));

        assertThat(admin.tableExists(nameIndexTablePath).get()).isTrue();
        TableInfo nameIndexInfo = admin.getTableInfo(nameIndexTablePath).get();
        assertThat(nameIndexInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_2);
        assertThat(nameIndexInfo.toTableDescriptor().getSchema().getPrimaryKeyColumnNames())
                .containsExactly("name", "id");
        assertThat(
                        nameIndexInfo
                                .toTableDescriptor()
                                .getTableDistribution()
                                .get()
                                .getBucketCount()
                                .get())
                .isEqualTo(3);

        assertThat(admin.tableExists(ageCityIndexTablePath).get()).isTrue();
        TableInfo ageCityIndexInfo = admin.getTableInfo(ageCityIndexTablePath).get();
        assertThat(ageCityIndexInfo.toTableDescriptor().getSchema().getPrimaryKeyColumnNames())
                .containsExactly("age", "city", "id");
    }

    @Test
    void testCreatePartitionedTableWithGlobalSecondaryIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partitioned_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .primaryKey("id", "region")
                        .index("name_idx", "name")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .comment("partitioned table with index")
                        .distributedBy(3, "id")
                        .partitionedBy("region")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("name_idx"), "3")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        createTable(tablePath, descriptor, true);

        // Verify main table is partitioned
        TableInfo mainTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(mainTableInfo.toTableDescriptor().isPartitioned()).isTrue();
        assertThat(mainTableInfo.toTableDescriptor().getPartitionKeys()).containsExactly("region");
        assertThat(mainTableInfo.toTableDescriptor().getSchema().getIndexes()).hasSize(1);

        // Verify index table exists and is NOT partitioned
        TablePath nameIndexTablePath =
                TablePath.of(
                        DB,
                        IndexTableUtils.indexTableName("test_partitioned_with_index", "name_idx"));
        assertThat(admin.tableExists(nameIndexTablePath).get()).isTrue();
        TableInfo indexInfo = admin.getTableInfo(nameIndexTablePath).get();
        assertThat(indexInfo.getTableConfig().getKvFormatVersion())
                .hasValue(ConfigOptions.KV_FORMAT_VERSION_3);
        assertThat(indexInfo.toTableDescriptor().isPartitioned()).isFalse();

        // V2: partitioned index table has __partition_id system column
        assertThat(indexInfo.toTableDescriptor().getSchema().getColumnNames())
                .contains(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
    }

    @Test
    void testUserCannotCreateInternalSecondaryIndexTable() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_user_created_internal_index");
        admin.createDatabase(DB, org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, true).get();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, 1L)
                        .build();

        assertThatThrownBy(() -> admin.createTable(tablePath, descriptor, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("internal secondary index table");

        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    @Test
    void testCreateTableRollsBackWhenDerivedIndexTableNameExists() throws Exception {
        TablePath mainTablePath = TablePath.of(DB, "test_index_table_name_collision");
        TablePath existingIndexTablePath =
                TablePath.of(
                        DB,
                        IndexTableUtils.indexTableName(mainTablePath.getTableName(), "idx_name"));

        TableDescriptor existingDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(1, "id")
                        .build();
        createTable(existingIndexTablePath, existingDescriptor, true);

        TableDescriptor mainDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id")
                                        .index("idx_name", "name")
                                        .build())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_name"), "1")
                        .build();

        assertThatThrownBy(() -> admin.createTable(mainTablePath, mainDescriptor, false).get())
                .cause()
                .isInstanceOf(TableAlreadyExistException.class);

        assertThat(admin.tableExists(mainTablePath).get()).isFalse();
        assertThat(admin.tableExists(existingIndexTablePath).get()).isTrue();
    }

    @Test
    void testDropTableWithGlobalSecondaryIndexAutoDeletesIndexTables() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_table_to_drop_with_index");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .index("name_idx", "name")
                        .index("age_idx", "age")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("name_idx"), "3")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("age_idx"), "3")
                        .build();

        createTable(tablePath, descriptor, true);

        assertThat(admin.tableExists(tablePath).get()).isTrue();

        TablePath nameIndexPath =
                TablePath.of(
                        DB,
                        IndexTableUtils.indexTableName(
                                "test_table_to_drop_with_index", "name_idx"));
        TablePath ageIndexPath =
                TablePath.of(
                        DB,
                        IndexTableUtils.indexTableName("test_table_to_drop_with_index", "age_idx"));

        assertThat(admin.tableExists(nameIndexPath).get()).isTrue();
        assertThat(admin.tableExists(ageIndexPath).get()).isTrue();

        // Drop main table
        admin.dropTable(tablePath, false).get();

        // Verify cascade delete
        assertThat(admin.tableExists(tablePath).get()).isFalse();
        assertThat(admin.tableExists(nameIndexPath).get()).isFalse();
        assertThat(admin.tableExists(ageIndexPath).get()).isFalse();
    }

    @Test
    void testDropTableWithIndexIgnoreIfNotExists() throws Exception {
        // Ensure the database exists so the drop goes through to table-level check
        admin.createDatabase(DB, org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, true).get();
        TablePath nonExistentPath = TablePath.of(DB, "non_existent_table_with_index");
        admin.dropTable(nonExistentPath, true).get();
        assertThat(admin.tableExists(nonExistentPath).get()).isFalse();
    }

    @Test
    void testSchemaEvolutionRejectedForTablesWithSecondaryIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_schema_evolution_rejected");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index("idx_name", "name")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_name"), "3")
                        .build();

        createTable(tablePath, descriptor, true);

        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                tablePath,
                                                Arrays.asList(
                                                        TableChange.addColumn(
                                                                "extra",
                                                                DataTypes.STRING(),
                                                                null,
                                                                TableChange.ColumnPosition.last())),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("secondary indexes");

        TablePath indexTablePath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName(tablePath.getTableName(), "idx_name"));
        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                indexTablePath,
                                                Arrays.asList(
                                                        TableChange.addColumn(
                                                                "extra",
                                                                DataTypes.STRING(),
                                                                null,
                                                                TableChange.ColumnPosition.last())),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("internal secondary index tables");
    }

    @Test
    void testCreatePartitionWithInvalidTimeFormatForTableWithIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_invalid_partition_format");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .index("idx_name", Arrays.asList("name"))
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_name"), "1")
                        .build();

        createTable(tablePath, descriptor, true);

        // Invalid format
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath,
                                                newPartitionSpec("dt", "invalid_date"),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class);

        // Wrong format (YYYY-MM-DD vs YYYYMMDD)
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath,
                                                newPartitionSpec("dt", "2025-01-21"),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class);

        // Valid future date should succeed
        String validDate = LocalDate.now().plusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        admin.createPartition(tablePath, newPartitionSpec("dt", validDate), false).get();
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        assertThat(
                        partitions.stream()
                                .map(PartitionInfo::getPartitionName)
                                .collect(Collectors.toList()))
                .contains(validDate);
    }
}
