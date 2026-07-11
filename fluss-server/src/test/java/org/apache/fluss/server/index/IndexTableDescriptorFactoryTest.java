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
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IndexTableDescriptorFactory}. */
class IndexTableDescriptorFactoryTest {

    @Test
    void testDerivePartitionedIndexTableUsesAlignedWalAndAddsPartitionId() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Collections.singletonList("user_id"),
                                IndexVisibility.SYNC,
                                16)
                        .build();

        TableDescriptor mainDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        TableDescriptor derived =
                IndexTableDescriptorFactory.derive(
                        mainDescriptor,
                        /* mainTableId= */ 1234L,
                        /* mainTableName= */ "tdb.orders",
                        "idx_user");

        assertThat(derived.getKvFormat()).isEqualTo(KvFormat.ALIGNED);
        assertThat(derived.getLogFormat()).isEqualTo(LogFormat.COMPACTED);
        assertThat(derived.getChangelogImage()).isEqualTo(ChangelogImage.WAL);

        assertThat(derived.getTableDistribution()).isPresent();
        assertThat(derived.getTableDistribution().get().getBucketCount()).hasValue(16);

        assertThat(derived.isIndexTable()).isTrue();
        assertThat(derived.getProperties())
                .containsEntry(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID.key(), "1234");
        assertThat(derived.getProperties())
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key());

        Schema dSchema = derived.getSchema();
        assertThat(dSchema.getPrimaryKey()).isPresent();
        assertThat(dSchema.getPrimaryKey().get().getColumnNames())
                .containsExactly("user_id", "order_id", "dt");
        assertThat(dSchema.getPrimaryKey().get().getColumnNames())
                .doesNotContain(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        assertThat(dSchema.getPrimaryKey().get().getColumnNames())
                .doesNotContain("__source_offset", "__index_deleted");
        assertThat(dSchema.getColumns())
                .extracting(Schema.Column::getName)
                .contains(
                        IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                        "__source_offset",
                        "__index_deleted");
    }

    @Test
    void testDeriveNonPartitionedIndexTableOmitsPartitionIdColumn() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .build();

        TableDescriptor main = TableDescriptor.builder().schema(mainSchema).build();
        TableDescriptor d = IndexTableDescriptorFactory.derive(main, 1L, "db.t", "idx_u");

        assertThat(d.getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .doesNotContain(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        assertThat(d.getSchema().getColumns())
                .extracting(Schema.Column::getName)
                .contains("__source_offset", "__index_deleted");
        assertThat(d.isIndexTable()).isTrue();
    }

    @Test
    void testDeriveIndexTableUsesVersionedTombstoneMergeContract() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .build();

        TableDescriptor main = TableDescriptor.builder().schema(mainSchema).build();
        TableDescriptor d = IndexTableDescriptorFactory.derive(main, 1L, "db.t", "idx_u");

        assertThat(d.getProperties())
                .containsEntry(
                        ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.VERSIONED.name())
                .containsEntry(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(), "__source_offset")
                .containsEntry(
                        ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), DeleteBehavior.IGNORE.name());
    }

    @Test
    void testDeriveSetsKvFormatVersionByPartitioning() {
        Schema nonPartitionedSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .build();
        TableDescriptor nonPartitioned =
                TableDescriptor.builder().schema(nonPartitionedSchema).build();
        TableDescriptor nonPartitionedIndex =
                IndexTableDescriptorFactory.derive(nonPartitioned, 1L, "db.t", "idx_u");

        assertThat(nonPartitionedIndex.getProperties())
                .containsEntry(
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(ConfigOptions.KV_FORMAT_VERSION_2));

        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .index("idx_u", "u")
                        .build();
        TableDescriptor partitioned =
                TableDescriptor.builder().schema(partitionedSchema).partitionedBy("dt").build();
        TableDescriptor partitionedIndex =
                IndexTableDescriptorFactory.derive(partitioned, 2L, "db.pt", "idx_u");

        assertThat(partitionedIndex.getProperties())
                .containsEntry(
                        ConfigOptions.TABLE_KV_FORMAT_VERSION.key(),
                        String.valueOf(ConfigOptions.KV_FORMAT_VERSION_3));
    }

    @Test
    void testDeriveUsesInheritedBucketCountWhenNotOverridden() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_u", "u")
                        .build();

        TableDescriptor main =
                TableDescriptor.builder().schema(mainSchema).distributedBy(7, "id").build();

        TableDescriptor d = IndexTableDescriptorFactory.derive(main, 1L, "db.t", "idx_u");

        assertThat(d.getTableDistribution()).isPresent();
        assertThat(d.getTableDistribution().get().getBucketCount()).hasValue(7);
    }

    @Test
    void testDeriveThrowsForUnknownIndexName() {
        Schema mainSchema =
                Schema.newBuilder().column("id", DataTypes.BIGINT()).primaryKey("id").build();
        TableDescriptor main = TableDescriptor.builder().schema(mainSchema).build();

        assertThatThrownBy(() -> IndexTableDescriptorFactory.derive(main, 1L, "db.t", "nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void testDerivedIndexTableBucketKeyIsIdxColsOnly() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Collections.singletonList("user_id"),
                                IndexVisibility.SYNC,
                                8)
                        .build();
        TableDescriptor main =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .partitionedBy("dt")
                        .build();

        TableDescriptor derived =
                IndexTableDescriptorFactory.derive(main, 100L, "tdb.orders", "idx_user");

        assertThat(derived.getTableDistribution()).isPresent();
        assertThat(derived.getTableDistribution().get().getBucketKeys()).containsExactly("user_id");
    }
}
