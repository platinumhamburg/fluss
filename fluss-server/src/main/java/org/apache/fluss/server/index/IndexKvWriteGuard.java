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
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.server.kv.KvWriteGuard;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Validates source-partition identity for physical Index Table V1 mutations. */
public final class IndexKvWriteGuard implements KvWriteGuard {

    private final long mainTableId;
    private final boolean partitioned;
    private final int partitionIdPosition;
    private final int partitionIdKeyPosition;
    private final TabletServerMetadataCache metadataCache;
    @Nullable private final KeyDecoder keyDecoder;

    public IndexKvWriteGuard(TableInfo tableInfo, TabletServerMetadataCache metadataCache) {
        checkArgument(tableInfo.isIndexTable(), "KV write guard requires an Index Table");
        this.mainTableId = tableInfo.getMainTableId().getAsLong();
        this.metadataCache = metadataCache;

        Schema schema = tableInfo.getSchema();
        this.partitionIdPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.partitioned = partitionIdPosition >= 0;
        if (partitioned) {
            List<String> primaryKeys =
                    schema.getPrimaryKey()
                            .orElseThrow(IllegalArgumentException::new)
                            .getColumnNames();
            this.partitionIdKeyPosition = primaryKeys.size() - 1;
            checkArgument(
                    IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN.equals(
                            primaryKeys.get(partitionIdKeyPosition)),
                    "Partition ID must be the final physical Index Table primary key field");
            int kvFormatVersion =
                    tableInfo
                            .getTableConfig()
                            .getKvFormatVersion()
                            .orElse(ConfigOptions.KV_FORMAT_VERSION_2);
            this.keyDecoder =
                    KeyDecoder.ofPrimaryKeyDecoder(
                            schema.getRowType(),
                            primaryKeys,
                            (short) kvFormatVersion,
                            tableInfo.getTableConfig().getDataLakeFormat().orElse(null),
                            false);
        } else {
            this.partitionIdKeyPosition = -1;
            this.keyDecoder = null;
        }
    }

    @Override
    public Decision beforeWriterState(WriterKey writerKey) {
        IndexWriterKey.SourceBucket source = IndexWriterKey.decode(writerKey);
        OptionalLong partitionId = source.getPartitionId();
        if (!partitioned) {
            checkArgument(
                    !partitionId.isPresent(),
                    "Unpartitioned Index Table writer must not contain a partition ID");
            return Decision.APPLY;
        }

        checkArgument(
                partitionId.isPresent(),
                "Partitioned Index Table writer must contain a partition ID");
        PartitionTombstone tombstone =
                metadataCache
                        .getInitializedPartitionTombstone(mainTableId)
                        .orElseThrow(
                                () ->
                                        new StaleMetadataException(
                                                "Partition tombstone baseline is not initialized for "
                                                        + mainTableId));
        return tombstone.isTombstoned(partitionId.getAsLong()) ? Decision.NO_OP : Decision.APPLY;
    }

    @Override
    public void validateRecord(WriterKey writerKey, byte[] key, @Nullable BinaryRow value) {
        IndexWriterKey.SourceBucket source = IndexWriterKey.decode(writerKey);
        OptionalLong partitionId = source.getPartitionId();
        if (!partitioned) {
            checkArgument(
                    !partitionId.isPresent(),
                    "Unpartitioned Index Table writer must not contain a partition ID");
            return;
        }

        checkArgument(
                partitionId.isPresent(),
                "Partitioned Index Table writer must contain a partition ID");
        long expectedPartitionId = partitionId.getAsLong();
        InternalRow physicalKey = keyDecoder.decodeKey(key);
        long keyPartitionId = physicalKey.getLong(partitionIdKeyPosition);
        checkArgument(
                keyPartitionId == expectedPartitionId,
                "Physical key partition ID %s does not match writer partition ID %s",
                keyPartitionId,
                expectedPartitionId);
        if (value != null) {
            long valuePartitionId = value.getLong(partitionIdPosition);
            checkArgument(
                    valuePartitionId == expectedPartitionId,
                    "Index row partition ID %s does not match writer partition ID %s",
                    valuePartitionId,
                    expectedPartitionId);
        }
    }
}
