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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

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
                        .index("idx_user", "user_id")
                        .build();

        TableDescriptor mainDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .partitionedBy("dt")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_user"), "16")
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

        Schema dSchema = derived.getSchema();
        assertThat(dSchema.getPrimaryKey()).isPresent();
        assertThat(dSchema.getPrimaryKey().get().getColumnNames())
                .containsExactly("user_id", "order_id", "dt");
        assertThat(dSchema.getPrimaryKey().get().getColumnNames())
                .doesNotContain(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        assertThat(dSchema.getColumns())
                .extracting(Schema.Column::getName)
                .contains(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
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
        assertThat(d.isIndexTable()).isTrue();
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
                        .index("idx_user", "user_id")
                        .build();
        TableDescriptor main =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .partitionedBy("dt")
                        .property(ConfigOptions.secondaryIndexBucketNumKey("idx_user"), "8")
                        .build();

        TableDescriptor derived =
                IndexTableDescriptorFactory.derive(main, 100L, "tdb.orders", "idx_user");

        assertThat(derived.getTableDistribution()).isPresent();
        assertThat(derived.getTableDistribution().get().getBucketKeys()).containsExactly("user_id");
    }
}
