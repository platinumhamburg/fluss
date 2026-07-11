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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
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
import java.util.function.ToLongFunction;

/**
 * Factory for building {@link IndexSpec} instances from main-table metadata and the server-side
 * metadata cache.
 *
 * <p>Extracted from {@code Replica} so that the spec-building logic (schema resolution, encoder
 * construction, bucket assignment) lives in one place rather than being scattered across a
 * 2600-line God Class.
 *
 * <p>Caller contract: the {@link IndexSpec.ValueEncoder} instances produced by this factory are
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

        ToLongFunction<InternalRow> partitionIdResolver =
                partitioned ? buildPartitionIdResolver(mainTableBucket) : null;

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
                            partitionIdResolver,
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
            ToLongFunction<InternalRow> partitionIdResolver,
            TabletServerMetadataCache metadataCache) {

        int[] idxColumnIndices = schema.getColumnIndexes(index.getColumnNames());
        int[] indexValueColumnIndices =
                composeIndexValueColumnIndices(idxColumnIndices, basePkColumnIndices);

        String indexName = index.getIndexName();
        long indexTableId = resolveIndexTableId(mainTableInfo, indexName, metadataCache);
        int indexBucketCount = resolveIndexBucketCount(mainTableInfo, indexName, metadataCache);
        int indexSchemaId = resolveIndexSchemaId(mainTableInfo, indexName, metadataCache);

        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(mainRowType, indexValueColumnIndices);

        CompactedKeyEncoder bucketKeyEncoder =
                new CompactedKeyEncoder(mainRowType, idxColumnIndices);
        FlussBucketingFunction bucketingFunction = new FlussBucketingFunction();

        int storedColCount = indexValueColumnIndices.length;
        int partitionColumnCount = partitioned ? 1 : 0;
        int sourceOffsetPosition = storedColCount + partitionColumnCount;
        int deletedMarkerPosition = sourceOffsetPosition + 1;
        int totalColCount = deletedMarkerPosition + 1;
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
        valueFieldTypes[sourceOffsetPosition] = DataTypes.BIGINT().copy(false);
        valueFieldTypes[deletedMarkerPosition] = DataTypes.BOOLEAN().copy(false);

        KvFormat indexKvFormat = partitioned ? KvFormat.ALIGNED : KvFormat.COMPACTED;
        RowEncoder valueRowEncoder = RowEncoder.create(indexKvFormat, valueFieldTypes);

        IndexSpec.ValueEncoder valueEncoder =
                (row, sourceOffset, deleted) -> {
                    valueRowEncoder.startNewRow();
                    for (int i = 0; i < valueFieldGetters.length; i++) {
                        valueRowEncoder.encodeField(i, valueFieldGetters[i].getFieldOrNull(row));
                    }
                    if (partitioned) {
                        long pid = partitionIdResolver.applyAsLong(row);
                        valueRowEncoder.encodeField(storedColCount, pid);
                    }
                    valueRowEncoder.encodeField(sourceOffsetPosition, sourceOffset);
                    valueRowEncoder.encodeField(deletedMarkerPosition, deleted);
                    return valueRowEncoder.finishRow();
                };

        return new IndexSpec(
                index.getIndexName(),
                index.getVisibility(),
                indexTableId,
                indexSchemaId,
                indexKvFormat,
                idxColumnIndices,
                keyEncoder,
                valueEncoder,
                row ->
                        bucketingFunction.bucketing(
                                bucketKeyEncoder.encodeKey(row), indexBucketCount));
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

    private static ToLongFunction<InternalRow> buildPartitionIdResolver(
            TableBucket mainTableBucket) {
        long partitionId = mainTableBucket.getPartitionId();
        return row -> partitionId;
    }

    private static TablePath indexTablePathFor(TableInfo mainTableInfo, String indexName) {
        return TablePath.of(
                mainTableInfo.getTablePath().getDatabaseName(),
                IndexTableUtils.indexTableName(
                        mainTableInfo.getTablePath().getTableName(), indexName));
    }

    private static long resolveIndexTableId(
            TableInfo mainTableInfo, String indexName, TabletServerMetadataCache metadataCache) {
        TablePath path = indexTablePathFor(mainTableInfo, indexName);
        return metadataCache
                .getTableId(path)
                .orElseThrow(
                        () -> new IllegalStateException("Index Table " + path + " not in cache"));
    }

    private static int resolveIndexBucketCount(
            TableInfo mainTableInfo, String indexName, TabletServerMetadataCache metadataCache) {
        TablePath path = indexTablePathFor(mainTableInfo, indexName);
        return metadataCache
                .getTableMetadata(path)
                .map(tm -> tm.getTableInfo().getNumBuckets())
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Index Table " + path + " bucket count not in cache"));
    }

    private static int resolveIndexSchemaId(
            TableInfo mainTableInfo, String indexName, TabletServerMetadataCache metadataCache) {
        TablePath path = indexTablePathFor(mainTableInfo, indexName);
        return metadataCache
                .getTableMetadata(path)
                .map(tm -> tm.getTableInfo().getSchemaId())
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Index Table " + path + " schema id not in cache"));
    }
}
