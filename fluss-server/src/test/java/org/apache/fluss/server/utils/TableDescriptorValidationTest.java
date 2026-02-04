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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    private static final int MAX_BUCKET_NUM = 1000;

    @Test
    void testIndexOnNonPartitionedTableAllowed() {
        // Non-partitioned table with index should be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .index("name_idx", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }

    @Test
    void testIndexOnPartitionedTableWithAutoPartitionAllowed() {
        // Partitioned table with auto-partition enabled and index should be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .index("name_idx", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }

    @Test
    void testIndexOnPartitionedTableWithoutAutoPartitionNotAllowed() {
        // Partitioned table without auto-partition enabled and with index should NOT be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .index("name_idx", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        // auto-partition is NOT enabled (default is false)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Global secondary indexes are not supported on partitioned tables without auto-partition enabled")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key());
    }

    @Test
    void testIndexOnPartitionedTableWithAutoPartitionExplicitlyDisabledNotAllowed() {
        // Partitioned table with auto-partition explicitly disabled and with index should NOT be
        // allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "region")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .index("name_idx", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("region")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Global secondary indexes are not supported on partitioned tables without auto-partition enabled");
    }

    @Test
    void testPartitionedTableWithoutIndexAllowed() {
        // Partitioned table without index should be allowed regardless of auto-partition setting
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        // auto-partition is NOT enabled
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }

    @Test
    void testMultipleIndexesOnPartitionedTableWithAutoPartitionAllowed() {
        // Partitioned table with auto-partition enabled and multiple indexes should be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .index("name_idx", "name")
                        .index("age_idx", "age")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }

    @Test
    void testMultipleIndexesOnPartitionedTableWithoutAutoPartitionNotAllowed() {
        // Partitioned table without auto-partition and with multiple indexes should NOT be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .index("name_idx", "name")
                        .index("age_idx", "age")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Global secondary indexes are not supported on partitioned tables without auto-partition enabled");
    }

    @Test
    void testIndexColumnsEqualToPrimaryKeyNotAllowed() {
        // Index columns that are exactly the same as primary key columns should NOT be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .index("id_idx", "id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("cannot be exactly the same as primary key columns")
                .hasMessageContaining("Primary key already provides unique lookup capability");
    }

    @Test
    void testIndexColumnsEqualToCompositePrimaryKeyNotAllowed() {
        // Index columns that are exactly the same as composite primary key columns should NOT be
        // allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "status")
                        .column("id", DataTypes.INT())
                        .column("status", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .index("id_status_idx", "id", "status")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("cannot be exactly the same as primary key columns");
    }

    @Test
    void testIndexColumnsEqualToPartitionColumnsNotAllowed() {
        // Index columns that are exactly the same as partition columns should NOT be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .index("dt_idx", "dt")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("cannot be exactly the same as partition columns")
                .hasMessageContaining("not suitable for index lookup");
    }

    @Test
    void testIndexColumnsPartiallyOverlapPrimaryKeyAllowed() {
        // Index columns that partially overlap with primary key columns should be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "status")
                        .column("id", DataTypes.INT())
                        .column("status", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .index("id_name_idx", "id", "name")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }

    @Test
    void testIndexColumnsPartiallyOverlapPartitionColumnsAllowed() {
        // Index columns that partially overlap with partition columns should be allowed
        Schema schema =
                Schema.newBuilder()
                        .primaryKey("id", "dt")
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .index("name_dt_idx", "name", "dt")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        tableDescriptor, MAX_BUCKET_NUM))
                .doesNotThrowAnyException();
    }
}
