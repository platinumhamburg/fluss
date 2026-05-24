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
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;

/**
 * Apply-path drop predicate for partitioned Index Tables (FIP V2 §2.9.8).
 *
 * <p>Given a raw Index Table value of the form {@code [8B partitionId prefix | body]} (Plan 1
 * P1T7), returns {@code true} if the source main-table partition is tombstoned and the record
 * should therefore be dropped before being applied to the Index Bucket.
 *
 * <p>DELETE acks ({@code valueBytes == null}) are always kept so that delete propagation stays
 * idempotent. Values shorter than the 8-byte prefix (e.g. non-partitioned Index Tables) are also
 * kept — this filter only fires on partitioned Index Table values.
 */
@Internal
public final class PartitionTombstoneFilter {

    private PartitionTombstoneFilter() {}

    /**
     * Returns {@code true} if the record should be DROPPED because its source partition has been
     * tombstoned.
     *
     * @param valueBytes raw Index Table value bytes; {@code null} for DELETE acks
     * @param mainTableId source main table id (looked up via {@code TableInfo.getMainTableId()} on
     *     the Index Table being applied)
     * @param metadataCache TabletServer metadata cache holding the propagated tombstone state
     */
    public static boolean shouldDrop(
            @Nullable byte[] valueBytes,
            long mainTableId,
            TabletServerMetadataCache metadataCache) {
        if (valueBytes == null) {
            return false;
        }
        if (valueBytes.length < IndexTableUtils.PARTITION_ID_PREFIX_SIZE) {
            // Not a partitioned Index Table value; no tombstone applies.
            return false;
        }
        long partitionId = IndexTableUtils.decodePartitionIdPrefix(valueBytes);
        PartitionTombstone tombstone = metadataCache.getPartitionTombstone(mainTableId);
        return tombstone.isTombstoned(partitionId);
    }
}
