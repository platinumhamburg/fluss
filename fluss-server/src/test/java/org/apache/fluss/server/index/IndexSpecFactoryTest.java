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
import static org.assertj.core.api.Assertions.tuple;

/** Tests for {@link IndexSpecFactory}. */
class IndexSpecFactoryTest {

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
                        .index("idx_value", "idx")
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
        GenericRow sourceRow = GenericRow.of(7L, 41L, 999L);
        IndexSpec.IndexEntry entry = spec.encodeEntry(sourceRow);
        GenericRow physicalRow = GenericRow.of(41L, 7L);
        byte[] expectedKey =
                new CompactedKeyEncoder(
                                RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                new int[] {0, 1})
                        .encodeKey(physicalRow);

        assertThat(entry.key()).containsExactly(expectedKey);
        assertThat(entry.value().getFieldCount()).isEqualTo(2);
        assertThat(entry.value().getLong(0)).isEqualTo(41L);
        assertThat(entry.value().getLong(1)).isEqualTo(7L);
        assertThat(entry.targetBucket()).isBetween(0, 2);
        assertThat(spec.encodeEntry(GenericRow.of(8L, 41L, 1000L)).targetBucket())
                .as("bucket hash uses index columns only")
                .isEqualTo(entry.targetBucket());
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
                        .index("idx_value", "idx")
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

        IndexSpec spec =
                IndexSpecFactory.buildIndexSpecs(
                                mainInfo,
                                new TableBucket(mainInfo.getTableId(), 123L, 0),
                                metadataCache)
                        .get(0);
        IndexSpec.IndexEntry entry = spec.encodeEntry(GenericRow.of(41L, 5L, 7L, 999L));
        GenericRow physicalRow = GenericRow.of(41L, 5L, 7L, 123L);
        byte[] expectedKey =
                new CompactedKeyEncoder(
                                RowType.of(
                                        DataTypes.BIGINT(),
                                        DataTypes.BIGINT(),
                                        DataTypes.BIGINT(),
                                        DataTypes.BIGINT()),
                                new int[] {0, 1, 2, 3})
                        .encodeKey(physicalRow);

        assertThat(entry.key()).containsExactly(expectedKey);
        assertThat(entry.value().getFieldCount()).isEqualTo(4);
        assertThat(entry.value().getLong(0)).isEqualTo(41L);
        assertThat(entry.value().getLong(1)).isEqualTo(5L);
        assertThat(entry.value().getLong(2)).isEqualTo(7L);
        assertThat(entry.value().getLong(3)).isEqualTo(123L);
        assertThat(entry.targetBucket()).isBetween(0, 2);
        IndexSpec.IndexEntry differentBaseKey = spec.encodeEntry(GenericRow.of(41L, 6L, 8L, 1000L));
        assertThat(differentBaseKey.key()).isNotEqualTo(entry.key());
        assertThat(differentBaseKey.targetBucket())
                .as("bucket hash excludes base primary key and partition id")
                .isEqualTo(entry.targetBucket());
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
