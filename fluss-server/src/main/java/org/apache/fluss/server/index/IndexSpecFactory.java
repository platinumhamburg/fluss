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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory for building {@link IndexSpec} instances from main-table metadata and the server-side
 * metadata cache.
 *
 * <p>Extracted from {@code Replica} so that the spec-building logic (schema resolution, encoder
 * construction, bucket assignment) lives in one place rather than being scattered across a
 * 2600-line God Class.
 *
 * <p>Caller contract: the {@link IndexSpec.EntryEncoder} instances produced by this factory are
 * {@link NotThreadSafe} because they capture a mutable {@link RowEncoder}. The caller (typically
 * {@link IndexReplicator}) must guarantee single-threaded invocation.
 */
@Internal
public final class IndexSpecFactory {

    private IndexSpecFactory() {}

    /**
     * Builds the list of {@link IndexSpec}s for every secondary index declared on the main table.
     *
     * @param mainTableInfo the main (data) table's info
     * @param mainTableBucket the bucket of the main table replica that will drive the replicators
     * @param metadataCache server-wide metadata cache for resolving index-table IDs and bucket
     *     counts
     * @return an unmodifiable list of specs; empty if the main table has no indexes
     */
    public static List<IndexSpec> buildIndexSpecs(
            TableInfo mainTableInfo,
            TableBucket mainTableBucket,
            TabletServerMetadataCache metadataCache) {

        Schema schema = mainTableInfo.getSchema();
        List<Schema.Index> indexes = schema.getIndexes();
        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        int[] basePkColumnIndices = schema.getPrimaryKeyIndexes();
        boolean partitioned = mainTableInfo.isPartitioned();
        RowType mainRowType = mainTableInfo.getRowType();

        Long partitionId = partitioned ? mainTableBucket.getPartitionId() : null;

        List<IndexSpec> specs = new ArrayList<>(indexes.size());
        for (Schema.Index index : indexes) {
            specs.add(
                    buildOneSpec(
                            index,
                            schema,
                            mainTableInfo,
                            mainRowType,
                            basePkColumnIndices,
                            partitioned,
                            partitionId,
                            metadataCache));
        }
        return Collections.unmodifiableList(specs);
    }

    // ---- internal helpers ----

    private static IndexSpec buildOneSpec(
            Schema.Index index,
            Schema schema,
            TableInfo mainTableInfo,
            RowType mainRowType,
            int[] basePkColumnIndices,
            boolean partitioned,
            Long partitionId,
            TabletServerMetadataCache metadataCache) {

        int[] idxColumnIndices = schema.getColumnIndexes(index.getColumnNames());
        int[] indexValueColumnIndices =
                composeIndexValueColumnIndices(idxColumnIndices, basePkColumnIndices);

        String indexName = index.getIndexName();
        ResolvedIndexTable indexTable = resolveIndexTable(mainTableInfo, index, metadataCache);
        long indexTableId = indexTable.tableId;
        int indexBucketCount = indexTable.bucketCount;
        int indexSchemaId = indexTable.schemaId;

        CompactedKeyEncoder bucketKeyEncoder =
                new CompactedKeyEncoder(mainRowType, idxColumnIndices);
        FlussBucketingFunction bucketingFunction = new FlussBucketingFunction();

        int storedColCount = indexValueColumnIndices.length;
        int totalColCount = storedColCount + (partitioned ? 1 : 0);
        DataType[] valueFieldTypes = new DataType[totalColCount];
        InternalRow.FieldGetter[] valueFieldGetters = new InternalRow.FieldGetter[storedColCount];
        for (int i = 0; i < storedColCount; i++) {
            int idxInMain = indexValueColumnIndices[i];
            DataType type = mainRowType.getTypeAt(idxInMain);
            valueFieldTypes[i] = type;
            valueFieldGetters[i] = InternalRow.createFieldGetter(type, idxInMain);
        }
        if (partitioned) {
            valueFieldTypes[storedColCount] = DataTypes.BIGINT().copy(false);
        }

        KvFormat indexKvFormat = KvFormat.COMPACTED;
        RowEncoder valueRowEncoder = RowEncoder.create(indexKvFormat, valueFieldTypes);
        int[] physicalPkIndices = new int[totalColCount];
        for (int i = 0; i < totalColCount; i++) {
            physicalPkIndices[i] = i;
        }
        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(RowType.of(valueFieldTypes), physicalPkIndices);

        IndexSpec.EntryEncoder entryEncoder =
                row -> {
                    valueRowEncoder.startNewRow();
                    for (int i = 0; i < valueFieldGetters.length; i++) {
                        valueRowEncoder.encodeField(i, valueFieldGetters[i].getFieldOrNull(row));
                    }
                    if (partitioned) {
                        valueRowEncoder.encodeField(storedColCount, partitionId);
                    }
                    BinaryRow value = valueRowEncoder.finishRow();
                    int targetBucket =
                            bucketingFunction.bucketing(
                                    bucketKeyEncoder.encodeKey(row), indexBucketCount);
                    return new IndexSpec.IndexEntry(
                            keyEncoder.encodeKey(value), value, targetBucket);
                };

        return new IndexSpec(
                index.getIndexName(),
                index.getVisibility(),
                indexTableId,
                indexSchemaId,
                indexKvFormat,
                idxColumnIndices,
                entryEncoder);
    }

    /**
     * Compose index-value column indices: idx columns followed by base-PK columns, deduplicated.
     */
    static int[] composeIndexValueColumnIndices(int[] idxColumnIndices, int[] basePkColumnIndices) {
        Set<Integer> seen = new HashSet<>();
        int[] tmp = new int[idxColumnIndices.length + basePkColumnIndices.length];
        int len = 0;
        for (int i : idxColumnIndices) {
            if (seen.add(i)) {
                tmp[len++] = i;
            }
        }
        for (int i : basePkColumnIndices) {
            if (seen.add(i)) {
                tmp[len++] = i;
            }
        }
        int[] out = new int[len];
        System.arraycopy(tmp, 0, out, 0, len);
        return out;
    }

    private static TablePath indexTablePathFor(TableInfo mainTableInfo, String indexName) {
        return TablePath.of(
                mainTableInfo.getTablePath().getDatabaseName(),
                IndexTableUtils.indexTableName(
                        mainTableInfo.getTableId(), indexName));
    }

    private static ResolvedIndexTable resolveIndexTable(
            TableInfo mainTableInfo, Schema.Index index, TabletServerMetadataCache metadataCache) {
        TablePath path = indexTablePathFor(mainTableInfo, index.getIndexName());
        long cachedTableId =
                metadataCache
                        .getTableId(path)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Index Table " + path + " not in cache"));
        TableMetadata metadata =
                metadataCache
                        .getTableMetadata(path)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Index Table " + path + " metadata not in cache"));
        TableInfo indexTableInfo = metadata.getTableInfo();
        if (cachedTableId != indexTableInfo.getTableId()) {
            throw invalidMetadata(
                    path,
                    "table id "
                            + cachedTableId
                            + " from the path cache, but metadata contains "
                            + indexTableInfo.getTableId());
        }
        validateIndexTable(mainTableInfo, index, path, indexTableInfo);
        return new ResolvedIndexTable(
                indexTableInfo.getTableId(),
                indexTableInfo.getNumBuckets(),
                indexTableInfo.getSchemaId());
    }

    private static void validateIndexTable(
            TableInfo mainTableInfo, Schema.Index index, TablePath path, TableInfo indexTableInfo) {
        if (!indexTableInfo.isIndexTable()) {
            throw invalidMetadata(path, "a table that is not an Index Table");
        }
        if (indexTableInfo.getMainTableId().getAsLong() != mainTableInfo.getTableId()) {
            throw invalidMetadata(
                    path,
                    "an Index Table owned by main table "
                            + mainTableInfo.getTableId()
                            + " but found owner "
                            + indexTableInfo.getMainTableId().getAsLong());
        }

        TableDescriptor expected =
                IndexTableDescriptorFactory.derive(
                        mainTableInfo.toTableDescriptor(),
                        mainTableInfo.getTableId(),
                        mainTableInfo.getTablePath().toString(),
                        index.getIndexName());
        int expectedBucketCount =
                expected.getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(() -> invalidMetadata(path, "no derived bucket count"));
        if (indexTableInfo.getNumBuckets() != expectedBucketCount) {
            throw invalidMetadata(
                    path,
                    "bucket count "
                            + expectedBucketCount
                            + " but found "
                            + indexTableInfo.getNumBuckets());
        }
        if (!indexTableInfo.getBucketKeys().equals(expected.getBucketKeys())) {
            throw invalidMetadata(
                    path,
                    "bucket keys "
                            + expected.getBucketKeys()
                            + " but found "
                            + indexTableInfo.getBucketKeys());
        }
        if (!indexTableInfo.getSchema().equals(expected.getSchema())) {
            throw invalidMetadata(path, "a schema different from the derived Index Table schema");
        }
        if (indexTableInfo.getTableConfig().getKvFormat() != expected.getKvFormat()) {
            throw invalidMetadata(
                    path,
                    "KV format "
                            + expected.getKvFormat()
                            + " but found "
                            + indexTableInfo.getTableConfig().getKvFormat());
        }
        LogFormat actualLogFormat = indexTableInfo.getTableConfig().getLogFormat();
        if (actualLogFormat != expected.getLogFormat()) {
            throw invalidMetadata(
                    path,
                    "log format " + expected.getLogFormat() + " but found " + actualLogFormat);
        }
        ChangelogImage actualChangelogImage = indexTableInfo.getTableConfig().getChangelogImage();
        if (actualChangelogImage != expected.getChangelogImage()) {
            throw invalidMetadata(
                    path,
                    "changelog image "
                            + expected.getChangelogImage()
                            + " but found "
                            + actualChangelogImage);
        }
        int expectedKvFormatVersion =
                Integer.parseInt(
                        expected.getProperties().get(ConfigOptions.TABLE_KV_FORMAT_VERSION.key()));
        int actualKvFormatVersion = indexTableInfo.getTableConfig().getKvFormatVersion().orElse(0);
        if (actualKvFormatVersion != expectedKvFormatVersion) {
            throw invalidMetadata(
                    path,
                    "KV format version "
                            + expectedKvFormatVersion
                            + " but found "
                            + actualKvFormatVersion);
        }
        if (indexTableInfo.getKvIdempotenceProtocol()
                != KvIdempotenceProtocol.CUMULATIVE_PROGRESS) {
            throw invalidMetadata(path, "the cumulative progress protocol");
        }
    }

    private static IllegalStateException invalidMetadata(TablePath path, String detail) {
        return new IllegalStateException(
                "Invalid metadata for Index Table " + path + ": expected " + detail);
    }

    private static final class ResolvedIndexTable {
        private final long tableId;
        private final int bucketCount;
        private final int schemaId;

        private ResolvedIndexTable(long tableId, int bucketCount, int schemaId) {
            this.tableId = tableId;
            this.bucketCount = bucketCount;
            this.schemaId = schemaId;
        }
    }
}
