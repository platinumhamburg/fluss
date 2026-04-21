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

    private static final int MAX_BUCKET_NUM = 128;

    @Test
    void testSecondaryIndexCannotMatchPrimaryKeyColumns() {
        TableDescriptor descriptor =
                baseTableBuilder(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id")
                                        .secondaryIndex("id_idx", "id")
                                        .build())
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("cannot be exactly the same as primary key columns");
    }

    @Test
    void testSecondaryIndexCannotMatchPartitionColumns() {
        TableDescriptor descriptor =
                baseTableBuilder(
                                Schema.newBuilder()
                                        .column("dt", DataTypes.STRING())
                                        .column("id", DataTypes.BIGINT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("dt", "id")
                                        .secondaryIndex("dt_idx", "dt")
                                        .build())
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("cannot be exactly the same as partition columns");
    }

    @Test
    void testPartitionedSecondaryIndexRequiresAutoPartition() {
        TableDescriptor descriptor =
                baseTableBuilder(
                                Schema.newBuilder()
                                        .column("dt", DataTypes.STRING())
                                        .column("id", DataTypes.BIGINT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("dt", "id")
                                        .secondaryIndex("name_idx", "name")
                                        .build())
                        .partitionedBy("dt")
                        .build();

        assertThatThrownBy(() -> validate(descriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Secondary indexes are not supported on partitioned tables")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key());
    }

    @Test
    void testPartitionedSecondaryIndexWithAutoPartitionEnabledIsValid() {
        TableDescriptor descriptor =
                baseTableBuilder(
                                Schema.newBuilder()
                                        .column("dt", DataTypes.STRING())
                                        .column("id", DataTypes.BIGINT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("dt", "id")
                                        .secondaryIndex("name_idx", "name")
                                        .build())
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .build();

        assertThatCode(() -> validate(descriptor)).doesNotThrowAnyException();
    }

    private static void validate(TableDescriptor descriptor) {
        TableDescriptorValidation.validateTableDescriptor(descriptor, MAX_BUCKET_NUM, null);
    }

    private static TableDescriptor.Builder baseTableBuilder(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3, "id")
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1);
    }
}
