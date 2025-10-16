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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.index.IndexCache.IndexCacheFetchParam;
import org.apache.fluss.server.log.LogTablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    private final Map<Long, IndexBucketRowCache[]> bucketRowCaches = new HashMap<>();

    private final Map<Long, AtomicLong[]> bucketCommitOffsets = new HashMap<>();

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
            Map<Long, Integer> indexBucketDistribution) {
        this.logTablet = logTablet;
        for (Map.Entry<Long, Integer> entry : indexBucketDistribution.entrySet()) {
            Long indexTableId = entry.getKey();
            int bucketCount = entry.getValue();
            IndexBucketRowCache[] bucketCaches = new IndexBucketRowCache[bucketCount];
            AtomicLong[] bucketCommitOffsets = new AtomicLong[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                bucketCaches[i] = new IndexBucketRowCache(memoryPool);
                bucketCommitOffsets[i] = new AtomicLong(0L);
            }
            bucketRowCaches.put(indexTableId, bucketCaches);
            this.bucketCommitOffsets.put(indexTableId, bucketCommitOffsets);
        }
        LOG.info("IndexRowCache initialized with memory pool page size: {}", memoryPool.pageSize());
    }

    /**
     * Updates the commit offset for a specific index bucket.
     *
     * @param indexTableId the index table id
     * @param bucketId the index bucket ID
     * @param commitOffset the commit offset
     */
    public void updateCommitOffset(long indexTableId, int bucketId, long commitOffset) {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }
        getCommitOffsetsArray(indexTableId)[bucketId].set(commitOffset);
        getBucketRowCache(indexTableId, bucketId).garbageCollectBelowOffset(commitOffset);
    }

    /**
     * Returns the min commit offset for the entire index row cache.
     *
     * @return the commit offset
     */
    public long getCommitHorizon() {
        return bucketCommitOffsets.values().stream()
                .map(
                        offsets ->
                                Arrays.stream(offsets)
                                        .map(AtomicLong::get)
                                        .min(Long::compareTo)
                                        .orElse(0L))
                .min(Long::compareTo)
                .orElse(0L);
    }

    /**
     * Writes an empty row to the cache for a specific index bucket and logOffset.
     *
     * @param indexTableId the index table id
     * @param logOffset the log offset
     */
    public void writeEmptyRows(long indexTableId, long logOffset) {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }
        IndexBucketRowCache[] indexBucketCaches = getIndexBucketRowCaches(indexTableId);
        for (IndexBucketRowCache bucketCache : indexBucketCaches) {
            bucketCache.writeEmptyRowForOffset(logOffset);
        }
    }

    /**
     * Writes an IndexedRow to the cache for a specific index bucket and logOffset.
     *
     * <p>Thread Safety: This method is NOT thread-safe by itself. Callers MUST ensure proper
     * external synchronization. In IndexCache, this is protected by writeLock for both hot data
     * writes and cold data loading operations.
     *
     * @param indexTableId the index table id
     * @param bucketId the index bucket ID
     * @param logOffset the log offset
     * @param changeType the change type of the row
     * @param indexedRow the IndexedRow to cache
     * @throws IOException if an error occurs during writing
     * @throws IllegalStateException if the cache is closed
     */
    public void writeIndexedRow(
            long indexTableId,
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

        IndexBucketRowCache[] indexBucketCaches = getIndexBucketRowCaches(indexTableId);
        checkArgument(
                bucketId >= 0 && bucketId < indexBucketCaches.length,
                "bucketId %s is out of range [0, %s)",
                bucketId,
                indexBucketCaches.length);

        try {
            for (int i = 0; i < indexBucketCaches.length; i++) {
                IndexBucketRowCache bucketCache = indexBucketCaches[i];
                if (i == bucketId) {
                    bucketCache.writeIndexedRow(logOffset, changeType, indexedRow);
                } else {
                    bucketCache.writeEmptyRowForOffset(logOffset);
                }
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Wrote IndexedRow for index table {}, bucket {} with logOffset {}",
                        indexTableId,
                        bucketId,
                        logOffset);
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to write IndexedRow for index table {}, bucket {} at logOffset {}: {}",
                    indexTableId,
                    bucketId,
                    logOffset,
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    /**
     * Gets LogRecords for the specified offset range within the given index bucket with accurate
     * metadata. This method directly constructs LogRecordBatch headers using complete offset and
     * record information, eliminating the need for tricky estimations.
     *
     * <p>Note: The requested data range must form a continuous address space (or its subset) within
     * the cache. Queries spanning multiple ranges are not allowed and will throw an exception.
     *
     * @param indexTableId the id of index table
     * @param bucketId the index bucket ID
     * @param startOffset the start offset (inclusive)
     * @param expectedEndOffset the end offset (exclusive)
     * @return LogRecords with accurate headers, or empty LogRecords if bucket not found or no data
     *     in range
     */
    public LogRecords readIndexLogRecords(
            long indexTableId, int bucketId, long startOffset, long expectedEndOffset) {
        if (closed) {
            return createEmptyLogRecords();
        }

        IndexBucketRowCache bucketCache = getBucketRowCache(indexTableId, bucketId);
        if (bucketCache == null) {
            return createEmptyLogRecords();
        }

        return bucketCache.readIndexLogRecords(startOffset, expectedEndOffset);
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
        IndexBucketRowCache bucketCache =
                getBucketRowCache(indexBucket.getTableId(), indexBucket.getBucket());
        return bucketCache.getUnloadedRanges(startOffset, endOffset);
    }

    /**
     * Checks if the specified IndexBucket has sufficient hot data records to satisfy the
     * minBucketFetchRecords requirement without loading additional data from WAL.
     *
     * <p>This method is used in hotDataOnly mode to determine if a fetch request can be satisfied
     * immediately with cached data, or if it should return empty to trigger DelayedFetchIndex
     * processing.
     *
     * @param indexBucket the index bucket to check
     * @param startOffset the fetch start offset (inclusive)
     * @param minBucketFetchRecords the minimum number of records required
     * @return true if there are enough hot data records, false otherwise
     */
    public boolean hasSufficientHotData(
            TableBucket indexBucket, long startOffset, int minBucketFetchRecords) {
        if (closed || minBucketFetchRecords <= 0) {
            // No minimum requirement
            return true;
        }

        IndexBucketRowCache bucketCache =
                getBucketRowCache(indexBucket.getTableId(), indexBucket.getBucket());
        if (bucketCache == null) {
            return false;
        }

        long highWatermark = logTablet.getHighWatermark();
        if (startOffset >= highWatermark) {
            return false;
        }

        int hotDataRecords = bucketCache.getCachedRecordCountInRange(startOffset, highWatermark);
        return hotDataRecords >= minBucketFetchRecords;
    }

    /**
     * Checks if all specified IndexBuckets have sufficient hot data records to satisfy the
     * minBucketFetchRecords requirement.
     *
     * <p>This batch method is optimized for checking multiple buckets efficiently and is used to
     * determine whether a hotDataOnly fetch request should proceed or return empty to trigger
     * DelayedFetchIndex.
     *
     * @param fetchRequests Map of IndexBucket to fetch parameters
     * @return true if all buckets have sufficient hot data, false otherwise
     */
    public boolean hasSufficientHotDataForAll(
            Map<TableBucket, IndexCacheFetchParam> fetchRequests) {
        if (closed || fetchRequests == null || fetchRequests.isEmpty()) {
            return true;
        }

        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            IndexCacheFetchParam param = entry.getValue();

            // Only check if minFetchHotDataRecords is set (> 0)
            if (param.getMinFetchHotDataRecords() > 0) {
                if (!hasSufficientHotData(
                        indexBucket,
                        param.getFetchOffset(),
                        (int) param.getMinFetchHotDataRecords())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "IndexBucket {} does not have sufficient hot data for minBucketFetchRecords={}",
                                indexBucket,
                                param.getMinFetchHotDataRecords());
                    }
                    return false;
                }
            }
        }

        return true;
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

        long endOffset = logTablet.getHighWatermark();

        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            IndexCacheFetchParam param = entry.getValue();

            List<OffsetRangeInfo> offsetRangeInfos =
                    getUnloadedRanges(indexBucket, param.getFetchOffset(), endOffset);

            if (offsetRangeInfos.isEmpty()) {
                continue;
            }
            batchRanges.put(indexBucket, offsetRangeInfos);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Collected batch unloaded ranges: {}", batchRanges);
        }
        return batchRanges;
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
        for (Map.Entry<Long, IndexBucketRowCache[]> entry : bucketRowCaches.entrySet()) {
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

    private IndexBucketRowCache[] getIndexBucketRowCaches(long indexTableId) {
        IndexBucketRowCache[] bucketCaches = bucketRowCaches.get(indexTableId);
        if (null == bucketCaches) {
            throw new IllegalArgumentException(
                    "IndexBucketRowCache not found for indexTableId: " + indexTableId);
        }

        return bucketCaches;
    }

    private AtomicLong[] getCommitOffsetsArray(long indexTableId) {
        AtomicLong[] commitOffsets = bucketCommitOffsets.get(indexTableId);
        if (null == commitOffsets) {
            throw new IllegalArgumentException(
                    "IndexBucketRowCache not found for indexTableId: " + indexTableId);
        }
        return commitOffsets;
    }

    private IndexBucketRowCache getBucketRowCache(long indexTableId, int bucketId)
            throws IllegalArgumentException {
        IndexBucketRowCache[] bucketCaches = getIndexBucketRowCaches(indexTableId);
        if (bucketId < 0 || bucketId >= bucketCaches.length) {
            throw new IllegalArgumentException(
                    "Invalid bucketId: " + bucketId + " for indexTableId: " + indexTableId);
        }
        return bucketCaches[bucketId];
    }
}
