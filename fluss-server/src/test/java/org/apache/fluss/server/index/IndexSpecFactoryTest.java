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

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Tests for {@link IndexSpecFactory}. */
class IndexSpecFactoryTest {

    private static final int TARGET_BUCKET_COUNT = 7;

    @Test
    void testBuildIndexSpecsPreservesPerIndexVisibility() {
        TablePath mainPath = TablePath.of("db", "users");
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("email", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_sync",
                                IndexType.SECONDARY,
                                Collections.singletonList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_async",
                                IndexType.SECONDARY,
                                Collections.singletonList("name"),
                                IndexVisibility.ASYNC,
                                5)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "id").build();
        TableInfo mainInfo = tableInfo(mainPath, 1L, 1, mainDescriptor);

        FakeMetadataCache metadataCache = new FakeMetadataCache();
        metadataCache.add(
                TablePath.of("db", IndexTableUtils.indexTableName("users", "idx_sync")),
                11L,
                2,
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.users", "idx_sync"));
        metadataCache.add(
                TablePath.of("db", IndexTableUtils.indexTableName("users", "idx_async")),
                12L,
                3,
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.users", "idx_async"));

        List<IndexSpec> specs =
                IndexSpecFactory.buildIndexSpecs(
                        mainInfo, new TableBucket(mainInfo.getTableId(), 0), metadataCache);

        assertThat(specs)
                .extracting(IndexSpec::getIndexName, IndexSpec::getVisibility)
                .containsExactly(
                        tuple("idx_sync", IndexVisibility.SYNC),
                        tuple("idx_async", IndexVisibility.ASYNC));
        assertThat(specs).extracting(IndexSpec::isSync).containsExactly(true, false);
    }

    @Test
    void testEncodeNonPartitionedEntryUsesCompletePhysicalPrimaryKey() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("base_id", DataTypes.BIGINT())
                        .column("idx", DataTypes.BIGINT())
                        .column("payload", DataTypes.BIGINT())
                        .primaryKey("base_id")
                        .index(
                                "idx_value",
                                IndexType.SECONDARY,
                                Collections.singletonList("idx"),
                                IndexVisibility.SYNC,
                                TARGET_BUCKET_COUNT)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "base_id").build();
        TableInfo mainInfo = tableInfo(TablePath.of("db", "records"), 1L, 1, mainDescriptor);
        FakeMetadataCache metadataCache = new FakeMetadataCache();
        metadataCache.add(
                TablePath.of("db", "records__idx_value"),
                11L,
                2,
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.records", "idx_value"));

        IndexSpec spec =
                IndexSpecFactory.buildIndexSpecs(
                                mainInfo, new TableBucket(mainInfo.getTableId(), 0), metadataCache)
                        .get(0);
        GenericRow sourceRow = GenericRow.of(8L, 41L, 999L);
        IndexSpec.IndexEntry entry = spec.encodeEntry(sourceRow);
        GenericRow physicalRow = GenericRow.of(41L, 8L);
        RowType sourceRowType =
                RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT());
        RowType physicalRowType = RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT());
        byte[] expectedKey =
                new CompactedKeyEncoder(physicalRowType, new int[] {0, 1}).encodeKey(physicalRow);
        int expectedBucket = bucketFor(sourceRowType, new int[] {1}, sourceRow);
        int fullPhysicalKeyBucket = bucketFor(physicalRowType, new int[] {0, 1}, physicalRow);

        assertThat(entry.key()).containsExactly(expectedKey);
        assertThat(entry.value().getFieldCount()).isEqualTo(2);
        assertThat(entry.value().getLong(0)).isEqualTo(41L);
        assertThat(entry.value().getLong(1)).isEqualTo(8L);
        assertThat(entry.targetBucket()).isEqualTo(expectedBucket);
        assertThat(fullPhysicalKeyBucket)
                .as("the chosen row distinguishes index-only routing from physical-PK routing")
                .isNotEqualTo(expectedBucket);
        assertThat(spec.encodeEntry(GenericRow.of(9L, 41L, 1000L)).targetBucket())
                .as("base primary key does not affect the exact index-only target bucket")
                .isEqualTo(expectedBucket);
    }

    @Test
    void testEncodePartitionedEntryIncludesConstantPartitionIdInKeyAndValue() {
        Schema mainSchema =
                Schema.newBuilder()
                        .column("idx", DataTypes.BIGINT())
                        .column("partition_key", DataTypes.BIGINT())
                        .column("base_id", DataTypes.BIGINT())
                        .column("payload", DataTypes.BIGINT())
                        .primaryKey("partition_key", "base_id")
                        .index(
                                "idx_value",
                                IndexType.SECONDARY,
                                Collections.singletonList("idx"),
                                IndexVisibility.SYNC,
                                TARGET_BUCKET_COUNT)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder()
                        .schema(mainSchema)
                        .partitionedBy("partition_key")
                        .distributedBy(1, "base_id")
                        .build();
        TableInfo mainInfo = tableInfo(TablePath.of("db", "records"), 1L, 1, mainDescriptor);
        FakeMetadataCache metadataCache = new FakeMetadataCache();
        metadataCache.add(
                TablePath.of("db", "records__idx_value"),
                11L,
                2,
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.records", "idx_value"));

        IndexSpec partition123Spec =
                IndexSpecFactory.buildIndexSpecs(
                                mainInfo,
                                new TableBucket(mainInfo.getTableId(), 123L, 0),
                                metadataCache)
                        .get(0);
        IndexSpec partition456Spec =
                IndexSpecFactory.buildIndexSpecs(
                                mainInfo,
                                new TableBucket(mainInfo.getTableId(), 456L, 0),
                                metadataCache)
                        .get(0);
        GenericRow sourceRow = GenericRow.of(41L, 5L, 7L, 999L);
        IndexSpec.IndexEntry entry = partition123Spec.encodeEntry(sourceRow);
        GenericRow physicalRow = GenericRow.of(41L, 5L, 7L, 123L);
        RowType sourceRowType =
                RowType.of(
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT());
        RowType physicalRowType =
                RowType.of(
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT());
        byte[] expectedKey =
                new CompactedKeyEncoder(physicalRowType, new int[] {0, 1, 2, 3})
                        .encodeKey(physicalRow);
        int expectedBucket = bucketFor(sourceRowType, new int[] {0}, sourceRow);
        int fullPhysicalKeyBucket = bucketFor(physicalRowType, new int[] {0, 1, 2, 3}, physicalRow);

        assertThat(entry.key()).containsExactly(expectedKey);
        assertThat(entry.value().getFieldCount()).isEqualTo(4);
        assertThat(entry.value().getLong(0)).isEqualTo(41L);
        assertThat(entry.value().getLong(1)).isEqualTo(5L);
        assertThat(entry.value().getLong(2)).isEqualTo(7L);
        assertThat(entry.value().getLong(3)).isEqualTo(123L);
        assertThat(entry.targetBucket()).isEqualTo(expectedBucket);
        assertThat(fullPhysicalKeyBucket)
                .as("the chosen row distinguishes index-only routing from physical-PK routing")
                .isNotEqualTo(expectedBucket);
        IndexSpec.IndexEntry differentBaseKey =
                partition123Spec.encodeEntry(GenericRow.of(41L, 6L, 8L, 1000L));
        assertThat(differentBaseKey.key()).isNotEqualTo(entry.key());
        assertThat(differentBaseKey.targetBucket())
                .as("base primary-key columns do not affect the exact target bucket")
                .isEqualTo(expectedBucket);
        IndexSpec.IndexEntry differentPartition = partition456Spec.encodeEntry(sourceRow);
        assertThat(differentPartition.key()).isNotEqualTo(expectedKey);
        assertThat(differentPartition.targetBucket())
                .as("the partition discriminator does not affect the exact target bucket")
                .isEqualTo(expectedBucket);
    }

    @Test
    void testRejectsTableAtDerivedPathWithoutIndexOwnership() {
        TablePath mainPath = TablePath.of("db", "users");
        TableDescriptor mainDescriptor = singleIndexMainDescriptor(3);
        TableInfo mainInfo = tableInfo(mainPath, 1L, 1, mainDescriptor);
        FakeMetadataCache metadataCache = new FakeMetadataCache();
        TablePath indexPath = TablePath.of("db", "users__idx_email");
        metadataCache.add(indexPath, 11L, 2, mainDescriptor);

        assertThatThrownBy(
                        () ->
                                IndexSpecFactory.buildIndexSpecs(
                                        mainInfo,
                                        new TableBucket(mainInfo.getTableId(), 0),
                                        metadataCache))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(indexPath.toString())
                .hasMessageContaining("Index Table");
    }

    @Test
    void testRejectsIndexTableWithWrongMainBackLink() {
        TablePath mainPath = TablePath.of("db", "users");
        TableDescriptor mainDescriptor = singleIndexMainDescriptor(3);
        TableInfo mainInfo = tableInfo(mainPath, 1L, 1, mainDescriptor);
        FakeMetadataCache metadataCache = new FakeMetadataCache();
        TablePath indexPath = TablePath.of("db", "users__idx_email");
        metadataCache.add(
                indexPath,
                11L,
                2,
                IndexTableDescriptorFactory.derive(mainDescriptor, 99L, "db.users", "idx_email"));

        assertThatThrownBy(
                        () ->
                                IndexSpecFactory.buildIndexSpecs(
                                        mainInfo,
                                        new TableBucket(mainInfo.getTableId(), 0),
                                        metadataCache))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(indexPath.toString())
                .hasMessageContaining("main table 1");
    }

    @Test
    void testRejectsIndexTableWithUnexpectedBucketCount() {
        TablePath mainPath = TablePath.of("db", "users");
        TableDescriptor mainDescriptor = singleIndexMainDescriptor(3);
        TableInfo mainInfo = tableInfo(mainPath, 1L, 1, mainDescriptor);
        FakeMetadataCache metadataCache = new FakeMetadataCache();
        TablePath indexPath = TablePath.of("db", "users__idx_email");
        metadataCache.add(
                indexPath,
                11L,
                2,
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.users", "idx_email")
                        .withBucketCount(4));

        assertThatThrownBy(
                        () ->
                                IndexSpecFactory.buildIndexSpecs(
                                        mainInfo,
                                        new TableBucket(mainInfo.getTableId(), 0),
                                        metadataCache))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(indexPath.toString())
                .hasMessageContaining("bucket count 3")
                .hasMessageContaining("but found 4");
    }

    private static TableDescriptor singleIndexMainDescriptor(int indexBucketCount) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Collections.singletonList("email"),
                                IndexVisibility.SYNC,
                                indexBucketCount)
                        .build();
        return TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
    }

    private static int bucketFor(RowType rowType, int[] columnIndices, GenericRow row) {
        byte[] encodedColumns = new CompactedKeyEncoder(rowType, columnIndices).encodeKey(row);
        return FlussBucketingFunction.bucketForRowKey(encodedColumns, TARGET_BUCKET_COUNT);
    }

    private static TableInfo tableInfo(
            TablePath tablePath, long tableId, int schemaId, TableDescriptor descriptor) {
        return TableInfo.of(tablePath, tableId, schemaId, descriptor, "/tmp", 0L, 0L);
    }

    private static final class FakeMetadataCache extends TabletServerMetadataCache {
        private final Map<TablePath, TableMetadata> metadataByPath = new HashMap<>();

        private FakeMetadataCache() {
            super(null);
        }

        private void add(
                TablePath tablePath, long tableId, int schemaId, TableDescriptor descriptor) {
            metadataByPath.put(
                    tablePath,
                    new TableMetadata(
                            tableInfo(tablePath, tableId, schemaId, descriptor),
                            Collections.emptyList()));
        }

        @Override
        public OptionalLong getTableId(TablePath tablePath) {
            TableMetadata metadata = metadataByPath.get(tablePath);
            return metadata == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(metadata.getTableInfo().getTableId());
        }

        @Override
        public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
            return Optional.ofNullable(metadataByPath.get(tablePath));
        }
    }
}
