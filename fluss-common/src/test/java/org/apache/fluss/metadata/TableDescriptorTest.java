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

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.metadata.TableDescriptor}. */
class TableDescriptorTest {

    private static final int DEFAULT_INDEX_BUCKET_NUMBER = 3;
    private static final long MAIN_TABLE_ID = 1001L;
    private static final String MAIN_TABLE_NAME = "test_table";

    private static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .column("f0", DataTypes.STRING())
                    .column("f1", DataTypes.BIGINT())
                    .column("f3", DataTypes.STRING())
                    .primaryKey("f0", "f3")
                    .build();

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigBuilder.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigBuilder.key("b").intType().noDefaultValue();

    @Test
    void testBasic() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .build();

        assertThat(descriptor.getSchema()).isEqualTo(SCHEMA_1);
        assertThat(descriptor.isPartitioned()).isTrue();
        assertThat(descriptor.getPartitionKeys()).hasSize(1);
        assertThat(descriptor.getPartitionKeys().get(0)).isEqualTo("f0");
        assertThat(descriptor.getProperties()).hasSize(0);
        assertThat(descriptor.getComment().orElse(null)).isEqualTo("Test Comment");

        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testProperties() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().build())
                        .property(OPTION_A, false)
                        .property(OPTION_B, 42)
                        .property("c", "C")
                        .customProperty("d", "D")
                        .build();

        assertThat(descriptor.getProperties()).hasSize(3);
        assertThat(descriptor.getProperties().get("a")).isEqualTo("false");
        assertThat(descriptor.getProperties().get("b")).isEqualTo("42");
        assertThat(descriptor.getProperties().get("c")).isEqualTo("C");
        assertThat(descriptor.getProperties().get("d")).isNull();
        assertThat(descriptor.getCustomProperties().get("d")).isEqualTo("D");
    }

    @Test
    void testDistribution() {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(SCHEMA_1).distributedBy(10, "f0", "f3").build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).hasSize(2);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f0", "f3");
        assertThat(descriptor.isDefaultBucketKey()).isTrue();

        // a subset of primary key
        descriptor = TableDescriptor.builder().schema(SCHEMA_1).distributedBy(10, "f3").build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isFalse();

        // default bucket key for partitioned table
        descriptor = TableDescriptor.builder().schema(SCHEMA_1).partitionedBy("f0").build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isTrue();

        // test subset of primary key for partitioned table
        descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .distributedBy(10, "f3")
                        .build();
        distribution = descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).isEqualTo(Collections.singletonList("f3"));
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
    }

    @Test
    void testSchemaWithoutPrimaryKeyAndDistributionWithEmptyBucketKeys() {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build();
        final TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(12).build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(12);
        assertThat(distribution.get().getBucketKeys()).hasSize(0);
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
    }

    @Test
    void testPrimaryKeyDifferentWithBucketKeys() {
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .distributedBy(12, "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. "
                                + "The primary keys are [f0, f3], the partition keys are [], "
                                + "but the user-defined bucket keys are [f1].");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .distributedBy(12, "f0", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. "
                                + "The primary keys are [f0, f3], the partition keys are [], "
                                + "but the user-defined bucket keys are [f0, f1].");

        // bucket key shouldn't include partition key
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0")
                                        .distributedBy(3, "f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket key [f0, f3] shouldn't include any column in partition keys [f0].");
    }

    @Test
    void testSchemaWithPrimaryKeysButDistributionIsNull() {
        // primary key is "f0", but distribution is null, we will use primary key as bucket key.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).isEmpty();
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testSchemaWithPrimaryKeysButDistributionWithEmptyBucketKey() {
        // primary key is "f0", but bucket key is empty, we will use primary key as bucket key.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .distributedBy(10)
                        .comment("Test Comment")
                        .build();
        Optional<TableDescriptor.TableDistribution> distribution =
                descriptor.getTableDistribution();
        assertThat(distribution).isPresent();
        assertThat(distribution.get().getBucketCount()).hasValue(10);
        assertThat(distribution.get().getBucketKeys()).hasSize(1);
        assertThat(distribution.get().getBucketKeys().get(0)).isEqualTo("f3");
    }

    @Test
    void testWithProperties() {
        final TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(SCHEMA_1)
                        .partitionedBy("f0")
                        .comment("Test Comment")
                        .property(OPTION_A, true)
                        .build();

        final TableDescriptor copy = descriptor.withProperties(new HashMap<>());
        assertThat(copy.isPartitioned()).isEqualTo(descriptor.isPartitioned());
        assertThat(copy.getTableDistribution()).isEqualTo(descriptor.getTableDistribution());
        assertThat(copy.getComment()).isEqualTo(descriptor.getComment());
        assertThat(copy.getPartitionKeys()).isEqualTo(descriptor.getPartitionKeys());
        assertThat(copy.getSchema()).isEqualTo(descriptor.getSchema());
        assertThat(copy.getProperties()).hasSize(0);
    }

    @Test
    void testInvalidTableDescriptor() {
        // schema without primary key.
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build();
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .partitionedBy("unknown_p")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Partition key 'unknown_p' does not exist in the schema.");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .distributedBy(12, "unknown_f")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket key 'unknown_f' does not exist in the schema.");

        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(
                                                Schema.newBuilder()
                                                        .column("id", DataTypes.INT())
                                                        .column("dt", DataTypes.STRING())
                                                        .column("a", DataTypes.BIGINT())
                                                        .column("ts", DataTypes.TIMESTAMP())
                                                        .primaryKey("id")
                                                        .build())
                                        .partitionedBy("dt")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Partitioned Primary Key Table requires partition key [dt] is a subset of the primary key [id].");
    }

    @Test
    void testPartitionedTable() {
        // will partitioned keys are equal to primary keys, will no bucket key.
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Primary Key constraint [f0, f3] should not be same with partition fields [f0, f3].");

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(SCHEMA_1).partitionedBy("f0").build();
        assertThat(tableDescriptor.getTableDistribution().get().getBucketKeys())
                .isEqualTo(Collections.singletonList("f3"));

        // bucket key contains partitioned key, throw exception
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(SCHEMA_1)
                                        .partitionedBy("f0")
                                        .distributedBy(1, "f0", "f3")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Bucket key [f0, f3] shouldn't include any column in partition keys [f0].");
    }

    @Test
    void testInvalidListaggParameterEmptyDelimiter() {
        // LISTAGG with empty delimiter - should fail
        assertThatThrownBy(() -> AggFunctions.LISTAGG("").validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a non-empty string");
    }

    @Test
    void testInvalidListaggParameterUnknownParameter() {
        // LISTAGG with unknown parameter - should fail
        Map<String, String> params = new HashMap<>();
        params.put("unknown_param", "value");

        assertThatThrownBy(() -> AggFunctions.of(AggFunctionType.LISTAGG, params).validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown_param")
                .hasMessageContaining("not supported");
    }

    @Test
    void testInvalidSumFunctionWithParameters() {
        // SUM function does not accept parameters - should fail
        Map<String, String> params = new HashMap<>();
        params.put("some_param", "value");

        assertThatThrownBy(() -> AggFunctions.of(AggFunctionType.SUM, params).validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("some_param")
                .hasMessageContaining("not supported");
    }

    @Test
    void testIndexesWithPrimaryKeyTable() {
        // Test that indexes work with primary key tables
        final Schema schemaWithIndex =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .primaryKey("f0", "f2")
                        .index("idx1", "f1")
                        .build();

        TableDescriptor descriptor = TableDescriptor.builder().schema(schemaWithIndex).build();

        assertThat(descriptor.getSchema().getIndexes()).hasSize(1);
        assertThat(descriptor.getSchema().getIndexes().get(0).getIndexName()).isEqualTo("idx1");
        assertThat(descriptor.getSchema().getIndexes().get(0).getColumnNames())
                .containsExactly("f1");
    }

    @Test
    void testIndexesWithoutPrimaryKeyTable() {
        // Test that indexes are not allowed without primary key
        final Schema schemaWithIndexNoPK =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .index("idx1", "f1")
                        .build();

        assertThatThrownBy(() -> TableDescriptor.builder().schema(schemaWithIndexNoPK).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Global secondary indexes are only supported for primary key tables");
    }

    @Test
    void testMultipleIndexes() {
        // Test multiple indexes
        final Schema schemaWithMultipleIndexes =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .column("f3", DataTypes.INT())
                        .primaryKey("f0", "f2")
                        .index("idx1", "f1")
                        .index("idx2", "f3")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schemaWithMultipleIndexes).build();

        assertThat(descriptor.getSchema().getIndexes()).hasSize(2);
        assertThat(descriptor.getSchema().getIndexes().get(0).getIndexName()).isEqualTo("idx1");
        assertThat(descriptor.getSchema().getIndexes().get(1).getIndexName()).isEqualTo("idx2");
    }

    // ==================== Tests for TableDescriptor.forIndexTable ====================

    @Test
    void testForIndexTableWithSingleIndexColumn() {
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
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify schema
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("b", "a");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("b", "a");
        assertThat(indexSchema.getPrimaryKey().get().getConstraintName()).isEqualTo("pk_idx_b");

        // verify distribution
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketKeys())
                .containsExactly("b");
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(3);

        // verify not partitioned
        assertThat(indexTableDescriptor.isPartitioned()).isFalse();

        // verify mainTableId is set
        assertThat(indexTableDescriptor.getMainTableId().isPresent()).isTrue();
        assertThat(indexTableDescriptor.getMainTableId().getAsLong()).isEqualTo(MAIN_TABLE_ID);
    }

    @Test
    void testForIndexTableWithMultipleIndexColumns() {
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
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify schema - should include index columns + primary key columns
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("name", "email", "id");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("name", "email", "id");

        // verify distribution - bucket key should be index columns
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketKeys())
                .containsExactly("name", "email");
    }

    @Test
    void testForIndexTableWithPartitionedMainTable() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("created_date", DataTypes.STRING())
                        .primaryKey("id", "region")
                        .index("name_idx", "name")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "id")
                        .partitionedBy("region")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify schema - should include index columns + primary key columns
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("name", "id", "region");

        // verify index table is NOT partitioned
        assertThat(indexTableDescriptor.isPartitioned()).isFalse();
    }

    @Test
    void testForIndexTableWithCustomBucketCount() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA2_SCHEMA).index("b_idx", "b").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "a")
                        .property(ConfigOptions.TABLE_SECONDARY_INDEX_BUCKET_NUM.key(), "5")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify custom bucket count is used
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(5);
    }

    @Test
    void testForIndexTableWithDefaultBucketCount() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA1_SCHEMA_PK).index("b_idx", "b").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(2, "a").build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify default bucket count is used
        assertThat(indexTableDescriptor.getTableDistribution().get().getBucketCount().get())
                .isEqualTo(DEFAULT_INDEX_BUCKET_NUMBER);
    }

    @Test
    void testForIndexTableWithIndexColumnPartiallyOverlapsPrimaryKey() {
        // Test that index columns can partially overlap with primary key columns
        // and the index table correctly handles deduplication
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("status", DataTypes.STRING())
                        .primaryKey("id", "status")
                        .index("id_name_idx", Arrays.asList("id", "name"))
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, Arrays.asList("id", "status"))
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify schema - should not duplicate columns, id appears only once
        Schema indexSchema = indexTableDescriptor.getSchema();
        assertThat(indexSchema.getColumnNames()).containsExactly("id", "name", "status");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("id", "name", "status");
    }

    @Test
    void testForIndexTablePreservesReplicationFactor() {
        Schema mainSchema =
                Schema.newBuilder().fromSchema(DATA2_SCHEMA).index("c_idx", "c").build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "a")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "5")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // verify replication factor is preserved
        assertThat(indexTableDescriptor.getProperties())
                .containsEntry(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "5");
    }

    @Test
    void testForIndexTableColumnOrder() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .primaryKey("id", "region")
                        .index("email_idx", "email")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .distributedBy(3, "id")
                        .partitionedBy("region")
                        .build();

        Schema.Index index = mainSchema.getIndexes().get(0);

        TableDescriptor indexTableDescriptor =
                TableDescriptor.forIndexTable(
                        mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

        // columns should be ordered as: email (index), id (pk), region (partition)
        Schema indexSchema = indexTableDescriptor.getSchema();
        List<String> columnNames = indexSchema.getColumnNames();

        assertThat(columnNames).containsExactly("email", "id", "region");
        assertThat(indexSchema.getPrimaryKeyColumnNames()).containsExactly("email", "id", "region");
    }

    @Test
    void testForIndexTableDoesNotContainOffsetColumn() {
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

        for (Schema.Index index : mainSchema.getIndexes()) {
            TableDescriptor indexTableDescriptor =
                    TableDescriptor.forIndexTable(
                            mainTableDescriptor, index, MAIN_TABLE_ID, MAIN_TABLE_NAME);

            Schema indexSchema = indexTableDescriptor.getSchema();
            assertThat(indexSchema.getColumnNames()).doesNotContain("__offset");
        }
    }
}
