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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import org.fluss.rocksdb.AbstractCompactionFilterFactory;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Judges whether a partitioned Index Table value row belongs to a tombstoned partition.
 *
 * <p>Named "Discriminator" (not "Predicate") deliberately: this is the <em>judge</em> that reads
 * the tag (partition ID) from the v3 value format and consults the cluster-wide {@link
 * PartitionTombstone}. It is not a {@code Predicate<byte[]>} implementation — callers wire one of
 * its methods into the three filter paths (write-time value filter, prefix-lookup filter, RocksDB
 * compaction filter) themselves.
 *
 * <p>Construction is gated by {@link #forIndexTable(TableInfo, TabletServerMetadataCache)}: only
 * partitioned Index Tables that carry the {@code __partition_id} system column and a resolvable
 * main-table back-link yield an instance; all other call sites get {@code null} and skip the
 * tombstone path entirely.
 *
 * <p>Thread-safe: {@link #isTombstoned(byte[])} reads the tag at a fixed offset without any shared
 * mutable state, and the metadata-cache lookup is itself concurrent-safe.
 */
@Internal
final class TombstonedPartitionDiscriminator {

    private static final KvValueLayout INDEX_VALUE_LAYOUT =
            KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);

    private final long mainTableId;
    private final int partitionIdPosition;
    private final TabletServerMetadataCache metadataCache;

    private TombstonedPartitionDiscriminator(
            long mainTableId, int partitionIdPosition, TabletServerMetadataCache metadataCache) {
        this.mainTableId = mainTableId;
        this.partitionIdPosition = partitionIdPosition;
        this.metadataCache = metadataCache;
    }

    /**
     * Builds a discriminator iff this table is a partitioned Index Table with a resolvable main
     * table back-link and a {@code __partition_id} system column; returns {@code null} otherwise.
     */
    @Nullable
    static TombstonedPartitionDiscriminator forIndexTable(
            TableInfo tableInfo, TabletServerMetadataCache metadataCache) {
        if (!tableInfo.isIndexTable()) {
            return null;
        }
        Schema schema = tableInfo.getSchema();
        int pidPos = schema.getColumnNames().indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        if (pidPos < 0) {
            return null;
        }
        // Defensive guard: partitioned index tables MUST use v3 format to carry the tag.
        int kvFormatVersion =
                tableInfo
                        .getTableConfig()
                        .getKvFormatVersion()
                        .orElse(ConfigOptions.KV_FORMAT_VERSION_2);
        checkArgument(
                kvFormatVersion == KV_FORMAT_VERSION_3,
                "Partitioned Index Table must use kvFormatVersion 3, but got %s",
                kvFormatVersion);
        return new TombstonedPartitionDiscriminator(
                tableInfo.getMainTableId().getAsLong(), pidPos, metadataCache);
    }

    /**
     * Returns {@code true} when the encoded v3 value bytes contain a tag (partition ID) that is
     * tombstoned. The tag is read through the version 3 layout without deserializing the row.
     */
    boolean isTombstoned(byte[] valueBytes) {
        if (valueBytes == null || valueBytes.length < INDEX_VALUE_LAYOUT.rowPayloadOffset()) {
            return false;
        }
        long tag = INDEX_VALUE_LAYOUT.readValueTag(MemorySegment.wrap(valueBytes));
        return currentTombstone().isTombstoned(tag);
    }

    /** Fast-path guard: {@code true} only when the current tombstone has at least one entry. */
    boolean hasTombstonedPartitions() {
        return !currentTombstone().isEmpty();
    }

    /**
     * Wires the compaction-filter factory with a live supplier of the current {@link
     * PartitionTombstone}. For v3 tables, uses {@link FloorSetCompactionFilterFactory} which reads
     * the tag at a fixed offset; no schema metadata is needed by the native filter.
     */
    AbstractCompactionFilterFactory<?> createCompactionFilterFactory() {
        return new FloorSetCompactionFilterFactory(this::currentTombstone);
    }

    /**
     * Returns a tag extractor function for v3 value encoding. The extractor reads the partitionId
     * from the row at the known position.
     */
    ToLongFunction<BinaryRow> createTagExtractor() {
        final int pos = partitionIdPosition;
        return row -> row.getLong(pos);
    }

    /**
     * Diagnostic accessor used by {@code IndexReplicationSupervisor} when logging filter installation.
     */
    long mainTableId() {
        return mainTableId;
    }

    private PartitionTombstone currentTombstone() {
        return metadataCache.getPartitionTombstone(mainTableId);
    }
}
