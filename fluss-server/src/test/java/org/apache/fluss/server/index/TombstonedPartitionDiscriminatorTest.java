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
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    void testForIndexTableRejectsUnknownNewerKvFormatVersion() {
        assertThatThrownBy(
                        () ->
                                TombstonedPartitionDiscriminator.forIndexTable(
                                        partitionedIndexTableInfoWithKvFormatVersion(4),
                                        /* metadataCache= */ null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kvFormatVersion 3");
    }

    @Test
    void testTagOnlyValueUsesBigEndianLayoutAndShortValuesFailOpen() {
        TabletServerMetadataCache metadataCache = mock(TabletServerMetadataCache.class);
        when(metadataCache.getPartitionTombstone(1001L))
                .thenReturn(new PartitionTombstone(-1L, Collections.singleton(42L), 1L));
        TombstonedPartitionDiscriminator discriminator =
                TombstonedPartitionDiscriminator.forIndexTable(
                        partitionedIndexTableInfoWithKvFormatVersion(KV_FORMAT_VERSION_3),
                        metadataCache);
        KvValueLayout layout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);
        byte[] tagOnlyValue = new byte[layout.rowPayloadOffset()];
        layout.writeValueTag(tagOnlyValue, 42L);

        assertThat(discriminator.isTombstoned(tagOnlyValue)).isTrue();
        layout.writeValueTag(tagOnlyValue, 43L);
        assertThat(discriminator.isTombstoned(tagOnlyValue)).isFalse();
        assertThat(discriminator.isTombstoned(null)).isFalse();
        for (int length = 0; length < layout.rowPayloadOffset(); length++) {
            assertThat(discriminator.isTombstoned(new byte[length]))
                    .as("value length %s", length)
                    .isFalse();
        }
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
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                java.util.Collections.singletonList("user_id"),
                                IndexVisibility.SYNC,
                                1)
                        .build();

        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy("dt")
                .distributedBy(1, "order_id")
                .build();
    }
}
