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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexTableUtils}. */
class IndexTableUtilsTest {

    private static final int DEFAULT_BUCKET_NUMBER = 3;

    @Test
    void testGenerateIndexTablePath() {
        TablePath mainTablePath = TablePath.of("test_db", "test_table");
        String indexName = "test_index";

        TablePath indexTablePath = IndexTableUtils.generateIndexTablePath(mainTablePath, indexName);

        assertThat(indexTablePath.getDatabaseName()).isEqualTo("test_db");
        assertThat(indexTablePath.getTableName()).isEqualTo("__test_table__index__test_index");
    }

    @Test
    void testGenerateIndexTablePathWithComplexNames() {
        TablePath mainTablePath = TablePath.of("my_database", "user_orders");
        String indexName = "user_email_idx";

        TablePath indexTablePath = IndexTableUtils.generateIndexTablePath(mainTablePath, indexName);

        assertThat(indexTablePath.getDatabaseName()).isEqualTo("my_database");
        assertThat(indexTablePath.getTableName()).isEqualTo("__user_orders__index__user_email_idx");
    }

    @Test
    void testCreateIndexTableDescriptorWithSingleIndexColumn() {
        // use existing test data
        Schema mainSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("a")
                        .index("idx_b", "b")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(3, "a").build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify schema
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("b", "a", "__offset");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("b", "a");
        assertThat(indexSchema.getPrimaryKey().get().getConstraintName()).isEqualTo("pk_idx_b");

        // verify __offset column
        Schema.Column offsetColumn = indexSchema.getColumns().get(2);
        assertThat(offsetColumn.getName()).isEqualTo("__offset");
        assertThat(offsetColumn.getDataType()).isEqualTo(DataTypes.BIGINT());

        // verify distribution
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketKeys())
                .containsExactly("b");
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(3);

        // verify not partitioned
        assertThat(indexTableDescriptor.isPartitioned()).isFalse();
    }

    @Test
    void testCreateIndexTableDescriptorWithMultipleIndexColumns() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .index("name_email_idx", Arrays.asList("name", "email"))
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(4, "id").build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify schema - should include index columns + primary key columns + __offset
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("name", "email", "id", "__offset");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("name", "email", "id");

        // verify distribution - bucket key should be index columns
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketKeys())
                .containsExactly("name", "email");
    }

    @Test
    void testCreateIndexTableDescriptorWithPartitionedMainTable() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("created_date", DataTypes.STRING())
                        .primaryKey("id", "region") // partition keys must be subset of primary key
                        .index("name_idx", "name")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "id") // bucket key cannot include partition columns
                        .partitionedBy("region") // only use region as partition key
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify schema - should include index columns + primary key columns + partition columns +
        // __offset
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames())
                .containsExactly("name", "id", "region", "__offset");

        // verify partitioned
        assertThat(indexTableDescriptor.isPartitioned()).isTrue();
        assertThat(indexTableDescriptor.getPartitionKeys()).containsExactly("region");
    }

    @Test
    void testCreateIndexTableDescriptorWithCustomIndexBucketCount() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA2_SCHEMA).index("b_idx", "b").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "a")
                        .property(ConfigOptions.TABLE_INDEX_BUCKET_NUM.key(), "5")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify custom bucket count is used
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(5);
    }

    @Test
    void testCreateIndexTableDescriptorWithDefaultBucketCount() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA1_SCHEMA_PK).index("b_idx", "b").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(2, "a").build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify main table's bucket count is used since no custom index bucket count
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(2);
    }

    @Test
    void testCreateIndexTableDescriptorWithIndexColumnOverlapsPrimaryKey() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("status", DataTypes.STRING())
                        .primaryKey("id", "status")
                        .index("id_status_idx", Arrays.asList("id", "status"))
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, Arrays.asList("id", "status"))
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify schema - should not duplicate columns and only include necessary columns +
        // __offset
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("id", "status", "__offset");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("id", "status");
    }

    @Test
    void testCreateIndexTableDescriptorPreservesMainTableProperties() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA2_SCHEMA).index("c_idx", "c").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "a")
                        .property("custom.property", "test_value")
                        .property("another.property", "another_value")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // verify properties are preserved
        assertThat(indexTableDescriptor.getProperties())
                .containsEntry("custom.property", "test_value");
        assertThat(indexTableDescriptor.getProperties())
                .containsEntry("another.property", "another_value");
    }

    @Test
    void testIndexTableDescriptorColumnOrder() {
        // test that columns appear in correct order: index columns first, then primary key columns,
        // then partition columns
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .primaryKey("id", "region") // partition key must be part of primary key
                        .index("email_idx", "email")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "id") // bucket key cannot include partition columns
                        .partitionedBy("region")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                IndexTableUtils.createIndexTableDescriptor(
                        mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

        // columns should be ordered as: email (index), id (pk), region (partition), __offset
        // (system)
        Schema indexSchema = indexTableDescriptor.getSchema();
        List<String> columnNames = indexSchema.getColumnNames();

        // verify column order: index columns first, then primary key columns, then partition
        // columns, then system columns
        assertThat(columnNames).containsExactly("email", "id", "region", "__offset");

        // verify primary key order: email (index column) + id, region (primary key columns)
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("email", "id", "region");
    }

    @Test
    void testIndexTableDescriptorContainsOffsetColumn() {
        // test that all index tables contain the __offset system column
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index("name_idx", "name")
                        .index("email_idx", "email")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(3, "id").build();

        // test both indexes
        for (Schema.Index index : mainSchema.getIndexes()) {
            TableDescriptor indexTableDescriptor =
                    IndexTableUtils.createIndexTableDescriptor(
                            mainTableDescriptor, index, DEFAULT_BUCKET_NUMBER);

            Schema indexSchema = indexTableDescriptor.getSchema();
            List<Schema.Column> columns = indexSchema.getColumns();

            // verify __offset column exists and is the last column
            Schema.Column lastColumn = columns.get(columns.size() - 1);
            assertThat(lastColumn.getName()).isEqualTo("__offset");
            assertThat(lastColumn.getDataType()).isEqualTo(DataTypes.BIGINT());

            // verify __offset is not part of primary key
            assertThat(indexSchema.getPrimaryKeyColumnNames()).doesNotContain("__offset");
        }
    }

    @Test
    void testIsIndexTable() {
        // test valid index table names
        assertThat(IndexTableUtils.isIndexTable("__test_table__index__test_idx")).isTrue();
        assertThat(IndexTableUtils.isIndexTable("__user_orders__index__email_idx")).isTrue();
        assertThat(IndexTableUtils.isIndexTable("__my_table__index__multi_col_idx")).isTrue();

        // test invalid index table names
        assertThat(IndexTableUtils.isIndexTable("test_table")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("_test_table")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("__test_table")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("test_table_index")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("__test_table_idx")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("")).isFalse();
        assertThat(IndexTableUtils.isIndexTable("__")).isFalse();
    }

    @Test
    void testExtractMainTableName() {
        // test valid index table names
        assertThat(IndexTableUtils.extractMainTableName("__test_table__index__test_idx"))
                .isEqualTo("test_table");
        assertThat(IndexTableUtils.extractMainTableName("__user_orders__index__email_idx"))
                .isEqualTo("user_orders");
        assertThat(
                        IndexTableUtils.extractMainTableName(
                                "__my_complex_table_name__index__multi_col_idx"))
                .isEqualTo("my_complex_table_name");

        // test invalid index table names
        assertThat(IndexTableUtils.extractMainTableName("test_table")).isNull();
        assertThat(IndexTableUtils.extractMainTableName("__test_table")).isNull();
        assertThat(IndexTableUtils.extractMainTableName("test_table__index__idx")).isNull();
        assertThat(IndexTableUtils.extractMainTableName("")).isNull();
    }

    @Test
    void testExtractIndexName() {
        // test valid index table names
        assertThat(IndexTableUtils.extractIndexName("__test_table__index__test_idx"))
                .isEqualTo("test_idx");
        assertThat(IndexTableUtils.extractIndexName("__user_orders__index__email_idx"))
                .isEqualTo("email_idx");
        assertThat(IndexTableUtils.extractIndexName("__my_table__index__multi_column_index"))
                .isEqualTo("multi_column_index");

        // test edge cases
        assertThat(IndexTableUtils.extractIndexName("__table__index__idx_with_index_in_name"))
                .isEqualTo("idx_with_index_in_name");

        // test invalid index table names
        assertThat(IndexTableUtils.extractIndexName("test_table")).isNull();
        assertThat(IndexTableUtils.extractIndexName("__test_table")).isNull();
        assertThat(IndexTableUtils.extractIndexName("test_table__index__")).isNull();
        assertThat(IndexTableUtils.extractIndexName("")).isNull();
    }

    @Test
    void testIndexTableUtilityMethodsIntegration() {
        // test that all utility methods work together correctly
        TablePath mainTablePath = TablePath.of("test_db", "user_table");
        String indexName = "email_index";

        // generate index table path
        TablePath indexTablePath = IndexTableUtils.generateIndexTablePath(mainTablePath, indexName);
        String indexTableName = indexTablePath.getTableName();

        // verify the generated name is recognized as an index table
        assertThat(IndexTableUtils.isIndexTable(indexTableName)).isTrue();

        // verify we can extract the original main table name
        assertThat(IndexTableUtils.extractMainTableName(indexTableName)).isEqualTo("user_table");

        // verify we can extract the original index name
        assertThat(IndexTableUtils.extractIndexName(indexTableName)).isEqualTo("email_index");
    }
}
