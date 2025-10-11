/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.index.IndexCache.IndexCacheFetchParam;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * IndexRowCache manages row-level index caches organized by index bucket.
 *
 * <p>Storage Model: - bucketRowCaches: Map&lt;TableBucket, IndexBucketRowCache&gt; - Row-level
 * caches organized by index bucket - Each IndexBucketRowCache maintains independent MemorySegment
 * space and RowCacheIndex - Unified MemorySegmentPool for memory segment allocation and recycling
 *
 * <p>Core Features: - Manages multiple IndexBucketRowCache instances for different index buckets -
 * Provides unified interface for writing and reading IndexedRow data - Handles cleanup and memory
 * management across all bucket caches - Supports batch operations for improved performance
 *
 * <p>Thread Safety: This class is NOT thread-safe. External synchronization is required.
 */
@Internal
public final class IndexRowCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRowCache.class);

    private final LogTablet logTablet;

    /** Map from index bucket to its row cache. */
    private final Map<TablePartitionId, IndexBucketRowCache[]> bucketRowCaches =
            MapUtils.newConcurrentHashMap();

    /** Whether this cache manager has been closed. */
    private boolean closed = false;

    /**
     * Creates a new IndexRowCache.
     *
     * @param memoryPool the memory segment pool for allocating memory
     * @param logTablet the log tablet for accessing high watermark
     */
    public IndexRowCache(
            MemorySegmentPool memoryPool,
            LogTablet logTablet,
            Map<TablePartitionId, Integer> indexBucketDistribution) {
        this.logTablet = logTablet;
        for (Map.Entry<TablePartitionId, Integer> entry : indexBucketDistribution.entrySet()) {
            TablePartitionId tablePartitionId = entry.getKey();
            int bucketCount = entry.getValue();
            IndexBucketRowCache[] bucketCaches = new IndexBucketRowCache[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                bucketCaches[i] = new IndexBucketRowCache(memoryPool);
            }
            bucketRowCaches.put(tablePartitionId, bucketCaches);
        }
        LOG.info("IndexRowCache initialized with memory pool page size: {}", memoryPool.pageSize());
    }

    /**
     * Updates the commit offset for a specific index bucket.
     *
     * @param tablePartitionId the index table
     * @param bucketId the index bucket ID
     * @param commitOffset the commit offset
     */
    public void updateCommitOffset(
            TablePartitionId tablePartitionId, int bucketId, long commitOffset) {
        getBucketRowCache(tablePartitionId, bucketId).updateCommitOffset(commitOffset);
    }

    /**
     * Returns the min commit offset for the entire index row cache.
     *
     * @return the commit offset
     */
    public long getCommitHorizon() {
        return bucketRowCaches.values().stream()
                .map(
                        caches ->
                                Arrays.stream(caches)
                                        .map(IndexBucketRowCache::getCommitOffset)
                                        .min(Long::compareTo)
                                        .orElse(0L))
                .min(Long::compareTo)
                .orElse(0L);
    }

    /**
     * Writes an empty row to the cache for a specific index bucket and logOffset.
     *
     * @param tablePartitionId the index table
     * @param logOffset the log offset
     * @throws IOException if an error occurs during writing
     */
    public void writeEmptyRows(TablePartitionId tablePartitionId, long logOffset)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }
        IndexBucketRowCache[] indexBucketCaches = getIndexBucketRowCaches(tablePartitionId);
        for (IndexBucketRowCache bucketCache : indexBucketCaches) {
            bucketCache.writeEmptyRowForOffset(logOffset);
        }
    }

    /**
     * Writes an IndexedRow to the cache for a specific index bucket and logOffset.
     *
     * @param tablePartitionId the index table
     * @param bucketId the index bucket ID
     * @param logOffset the log offset
     * @param indexedRow the IndexedRow to cache
     * @throws IOException if an error occurs during writing
     */
    public void writeIndexedRow(
            TablePartitionId tablePartitionId,
            int bucketId,
            long logOffset,
            ChangeType changeType,
            IndexedRow indexedRow)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }
        checkArgument(
                changeType != null && indexedRow != null,
                "changeType and indexedRow cannot be null");
        IndexBucketRowCache[] indexBucketCaches = getIndexBucketRowCaches(tablePartitionId);
        checkArgument(
                bucketId >= 0 && bucketId < indexBucketCaches.length, "bucketId is out of range");
        for (int i = 0; i < indexBucketCaches.length; i++) {
            IndexBucketRowCache bucketCache = indexBucketCaches[i];
            if (i == bucketId) {
                bucketCache.writeIndexedRow(logOffset, changeType, indexedRow);
            } else {
                bucketCache.writeEmptyRowForOffset(logOffset);
            }
        }

        LOG.trace(
                "Wrote IndexedRow for index table {}, bucket {} with logOffset {}",
                tablePartitionId,
                bucketId,
                logOffset);
    }

    /**
     * Gets LogRecords for the specified offset range within the given index bucket with accurate
     * metadata. This method directly constructs LogRecordBatch headers using complete offset and
     * record information, eliminating the need for tricky estimations.
     *
     * <p>Note: The requested data range must form a continuous address space (or its subset) within
     * the cache. Queries spanning multiple ranges are not allowed and will throw an exception.
     *
     * @param tablePartitionId the index table
     * @param bucketId the index bucket ID
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @param schemaId the schema ID for LogRecordBatch headers
     * @return LogRecords with accurate headers, or empty LogRecords if bucket not found or no data
     *     in range
     */
    public LogRecords getRangeLogRecords(
            TablePartitionId tablePartitionId,
            int bucketId,
            long startOffset,
            long endOffset,
            short schemaId) {
        if (closed) {
            return createEmptyLogRecords();
        }

        IndexBucketRowCache bucketCache = getBucketRowCache(tablePartitionId, bucketId);
        if (bucketCache == null) {
            return createEmptyLogRecords();
        }

        return bucketCache.getRangeLogRecords(startOffset, endOffset, schemaId);
    }

    /**
     * Gets all unloaded ranges within the specified range for an index bucket using Range-based gap
     * analysis.
     *
     * <p>This method delegates to the IndexBucketRowCache to identify gaps between existing
     * OffsetRanges that need to be loaded from WAL, restoring the two-phase fetch capability.
     *
     * @param indexBucket the index bucket
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a list of OffsetRangeInfo representing gaps that need data loading
     */
    public List<OffsetRangeInfo> getUnloadedRanges(
            TableBucket indexBucket, long startOffset, long endOffset) {
        if (closed) {
            return emptyList();
        }
        TablePartitionId tablePartitionId = TablePartitionId.from(indexBucket);
        IndexBucketRowCache bucketCache =
                getBucketRowCache(tablePartitionId, indexBucket.getBucket());
        return bucketCache.getUnloadedRanges(startOffset, endOffset);
    }

    /**
     * Gets unloaded ranges for multiple IndexBuckets in a batch, optimized for efficient WAL
     * processing.
     *
     * <p>This method collects unloaded ranges from multiple IndexBuckets and returns them in a
     * OffsetRangeInfos structure that supports:
     *
     * <ul>
     *   <li>Global min/max offset calculation for single WAL read
     *   <li>Efficient lookup during WAL processing
     *   <li>Conditional data writing based on IndexBucket requirements
     * </ul>
     *
     * @param fetchRequests Map of IndexBucket to fetch parameters containing offset ranges
     * @return OffsetRangeInfos containing all unloaded ranges for efficient batch processing
     */
    public Map<TableBucket, List<OffsetRangeInfo>> getUnloadedRanges(
            Map<TableBucket, IndexCacheFetchParam> fetchRequests) {
        if (closed || fetchRequests == null || fetchRequests.isEmpty()) {
            return new HashMap<>();
        }

        Map<TableBucket, List<OffsetRangeInfo>> batchRanges = new HashMap<>();

        long startOffset =
                fetchRequests.values().stream()
                        .map(IndexCacheFetchParam::getFetchOffset)
                        .min(Long::compareTo)
                        .orElse(0L);
        long endOffset = logTablet.getHighWatermark();

        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();

            List<OffsetRangeInfo> offsetRangeInfos =
                    getUnloadedRanges(indexBucket, startOffset, endOffset);

            if (offsetRangeInfos.isEmpty()) {
                continue;
            }
            batchRanges.put(indexBucket, offsetRangeInfos);
        }

        LOG.debug("Collected batch unloaded ranges: {}", batchRanges);
        return batchRanges;
    }

    /**
     * Removes all entries with offsets below the given horizon for all index buckets.
     *
     * @param horizon the commit horizon
     */
    public void cleanupBelowHorizon(long horizon) throws IOException {
        if (closed) {
            return;
        }
        for (Map.Entry<TablePartitionId, IndexBucketRowCache[]> entry :
                bucketRowCaches.entrySet()) {
            for (IndexBucketRowCache cache : entry.getValue()) {
                cache.cleanupBelowHorizon(horizon);
            }
        }
    }

    /**
     * Gets the total number of cached entries across all index buckets.
     *
     * @return the total number of entries
     */
    public int getTotalEntries() {
        int totalEntries = 0;
        for (IndexBucketRowCache[] indexBucketRowCaches : bucketRowCaches.values()) {
            for (IndexBucketRowCache cache : indexBucketRowCaches) {
                totalEntries += cache.totalEntries();
            }
        }
        return totalEntries;
    }

    /**
     * Gets the number of index buckets with cached data.
     *
     * @return the number of cached buckets
     */
    public int getCachedBucketCount() {
        return bucketRowCaches.size();
    }

    /**
     * Checks if the cache manager has any cached data.
     *
     * @return true if no cached data exists
     */
    public boolean isEmpty() {
        return bucketRowCaches.isEmpty();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info(
                "Closing IndexRowCache with {} cached buckets, {} total entries",
                getCachedBucketCount(),
                getTotalEntries());

        // Close all bucket caches
        for (Map.Entry<TablePartitionId, IndexBucketRowCache[]> entry :
                bucketRowCaches.entrySet()) {
            for (int i = 0; i < entry.getValue().length; i++) {
                try {
                    IndexBucketRowCache cache = entry.getValue()[i];
                    cache.close();
                } catch (IOException e) {
                    LOG.warn(
                            "Error closing bucket cache for index table {}, bucket {}",
                            entry.getKey(),
                            i,
                            e);
                }
            }
        }
        bucketRowCaches.clear();

        closed = true;
        LOG.info("IndexRowCache closed successfully");
    }

    /** Creates an empty LogRecords for cases where no data is available. */
    private static LogRecords createEmptyLogRecords() {
        return IndexBucketRowCache.createEmptyLogRecords();
    }

    private IndexBucketRowCache[] getIndexBucketRowCaches(TablePartitionId tablePartitionId) {
        IndexBucketRowCache[] bucketCaches = bucketRowCaches.get(tablePartitionId);
        if (null == bucketCaches) {

            throw new IllegalArgumentException(
                    "IndexBucketRowCache not found for tablePartitionId: " + tablePartitionId);
        }

        return bucketCaches;
    }

    private IndexBucketRowCache getBucketRowCache(TablePartitionId tablePartitionId, int bucketId)
            throws IllegalArgumentException {
        IndexBucketRowCache[] bucketCaches = getIndexBucketRowCaches(tablePartitionId);
        if (bucketId < 0 || bucketId >= bucketCaches.length) {
            throw new IllegalArgumentException(
                    "Invalid bucketId: " + bucketId + " for tablePartitionId: " + tablePartitionId);
        }
        return bucketCaches[bucketId];
    }
}
