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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TombstonedPartitionDiscriminator}. */
class TombstonedPartitionDiscriminatorTest {

    private static final TablePath INDEX_TABLE_PATH =
            TablePath.of("test_db", IndexTableUtils.indexTableName("orders", "idx_user"));

    @Test
    void testForIndexTableRejectsMissingKvFormatVersion() {
        assertThatThrownBy(
                        () ->
                                TombstonedPartitionDiscriminator.forIndexTable(
                                        partitionedIndexTableInfoWithoutKvFormatVersion(),
                                        /* metadataCache= */ null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kvFormatVersion");
    }

    @Test
    void testForIndexTableRejectsTooOldKvFormatVersion() {
        assertThatThrownBy(
                        () ->
                                TombstonedPartitionDiscriminator.forIndexTable(
                                        partitionedIndexTableInfoWithKvFormatVersion(
                                                ConfigOptions.KV_FORMAT_VERSION_2),
                                        /* metadataCache= */ null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kvFormatVersion");
    }

    @Test
    void testForIndexTableAcceptsKvFormatVersion3() {
        assertThat(
                        TombstonedPartitionDiscriminator.forIndexTable(
                                partitionedIndexTableInfoWithKvFormatVersion(
                                        ConfigOptions.KV_FORMAT_VERSION_3),
                                /* metadataCache= */ null))
                .isNotNull();
    }

    private static TableInfo partitionedIndexTableInfoWithoutKvFormatVersion() {
        return partitionedIndexTableInfo(null);
    }

    private static TableInfo partitionedIndexTableInfoWithKvFormatVersion(int kvFormatVersion) {
        return partitionedIndexTableInfo(kvFormatVersion);
    }

    private static TableInfo partitionedIndexTableInfo(Integer kvFormatVersion) {
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(
                        partitionedMainTableDescriptor(), 1001L, "test_db.orders", "idx_user");
        Map<String, String> properties = new HashMap<>(indexDescriptor.getProperties());
        if (kvFormatVersion == null) {
            properties.remove(ConfigOptions.TABLE_KV_FORMAT_VERSION.key());
        } else {
            properties.put(
                    ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), String.valueOf(kvFormatVersion));
        }

        return TableInfo.of(
                INDEX_TABLE_PATH,
                2002L,
                0,
                indexDescriptor.withProperties(properties),
                "file:///tmp/remote",
                1L,
                1L);
    }

    private static TableDescriptor partitionedMainTableDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .index("idx_user", "user_id")
                        .build();

        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy("dt")
                .distributedBy(1, "order_id")
                .property(ConfigOptions.secondaryIndexBucketNumKey("idx_user"), "1")
                .build();
    }
}
