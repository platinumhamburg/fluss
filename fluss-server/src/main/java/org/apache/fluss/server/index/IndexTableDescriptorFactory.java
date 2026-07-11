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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Factory for descriptors of system-managed secondary index tables. */
@Internal
public final class IndexTableDescriptorFactory {

    private IndexTableDescriptorFactory() {}

    /**
     * Derives the {@link TableDescriptor} for the system-managed index table backing the named
     * secondary index on {@code mainDescriptor}.
     */
    public static TableDescriptor derive(
            TableDescriptor mainDescriptor,
            long mainTableId,
            String mainTableName,
            String indexName) {
        Schema.Index index =
                mainDescriptor.getSchema().getIndexes().stream()
                        .filter(i -> i.getIndexName().equals(indexName))
                        .findFirst()
                        .orElse(null);
        checkArgument(index != null, "Unknown index '%s' on table %s", indexName, mainTableName);

        boolean partitioned = mainDescriptor.isPartitioned();
        Optional<Schema.PrimaryKey> mainPk = mainDescriptor.getSchema().getPrimaryKey();
        checkState(
                mainPk.isPresent(),
                "Indexed main table '%s' must have a primary key",
                mainTableName);
        List<String> basePk = mainPk.get().getColumnNames();

        Set<String> seen = new LinkedHashSet<>();
        List<Schema.Column> columns = new ArrayList<>();
        for (String c : index.getColumnNames()) {
            if (seen.add(c)) {
                columns.add(new Schema.Column(c, lookupColumnType(mainDescriptor.getSchema(), c)));
            }
        }
        for (String c : basePk) {
            if (seen.add(c)) {
                columns.add(new Schema.Column(c, lookupColumnType(mainDescriptor.getSchema(), c)));
            }
        }
        if (partitioned) {
            columns.add(
                    new Schema.Column(
                            IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN,
                            DataTypes.BIGINT().copy(false)));
        }
        columns.add(
                new Schema.Column(
                        IndexTableUtils.SOURCE_OFFSET_SYSTEM_COLUMN,
                        DataTypes.BIGINT().copy(false)));
        columns.add(
                new Schema.Column(
                        IndexTableUtils.INDEX_DELETED_SYSTEM_COLUMN,
                        DataTypes.BOOLEAN().copy(false)));
        List<String> idxPk = new ArrayList<>(seen);
        Schema derivedSchema = Schema.newBuilder().fromColumns(columns).primaryKey(idxPk).build();

        Integer bucketCount =
                index.getBucketCount()
                        .orElseGet(
                                () ->
                                        mainDescriptor
                                                .getTableDistribution()
                                                .flatMap(
                                                        TableDescriptor.TableDistribution
                                                                ::getBucketCount)
                                                .orElse(null));

        TableDescriptor.Builder b =
                TableDescriptor.builder()
                        .schema(derivedSchema)
                        .kvFormat(partitioned ? KvFormat.ALIGNED : KvFormat.COMPACTED)
                        .logFormat(LogFormat.COMPACTED)
                        .changelogImage(ChangelogImage.WAL)
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, mainTableId)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.VERSIONED)
                        .property(
                                ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN,
                                IndexTableUtils.SOURCE_OFFSET_SYSTEM_COLUMN)
                        .property(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.IGNORE)
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                partitioned
                                        ? ConfigOptions.KV_FORMAT_VERSION_3
                                        : ConfigOptions.KV_FORMAT_VERSION_2);
        if (bucketCount != null) {
            // Bucket key is idxCols only, matching prefix lookup routing and push-side bucketing.
            b.distributedBy(bucketCount, new ArrayList<>(index.getColumnNames()));
        }
        return b.build();
    }

    private static DataType lookupColumnType(Schema schema, String column) {
        Schema.Column col =
                schema.getColumns().stream()
                        .filter(c -> c.getName().equals(column))
                        .findFirst()
                        .orElse(null);
        checkArgument(col != null, "Unknown column '%s'", column);
        return col.getDataType();
    }
}
