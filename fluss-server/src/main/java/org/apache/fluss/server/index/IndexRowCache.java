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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.log.LogTablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /** Result of reading index log records from cache. */
    public static final class ReadResult {
        private final LogRecords records;
        private final ReadStatus status;

        private ReadResult(LogRecords records, ReadStatus status) {
            this.records = records;
            this.status = status;
        }

        public LogRecords getRecords() {
            return records;
        }

        public ReadStatus getStatus() {
            return status;
        }

        /** Creates a successful read result with the given records. */
        public static ReadResult success(LogRecords records) {
            return new ReadResult(records, ReadStatus.LOADED);
        }

        /** Creates a not loaded result indicating the range is not in cache. */
        public static ReadResult notLoaded() {
            return new ReadResult(createEmptyLogRecords(), ReadStatus.NOT_LOADED);
        }
    }

    /** Status of reading index log records from cache. */
    public enum ReadStatus {
        /** Data is loaded and available (may be empty but the range is loaded). */
        LOADED,
        /** The requested range is not loaded in cache yet. */
        NOT_LOADED
    }

    /** Map from index bucket to its row cache. */
    private final Map<Long, IndexBucketRowCache[]> bucketRowCaches = new HashMap<>();

    /** Map from index table id to fetch states (including commit offsets) for each bucket. */
    private final Map<Long, RemoteFetchState[]> bucketFetchStates = new HashMap<>();

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
        for (Map.Entry<Long, Integer> entry : indexBucketDistribution.entrySet()) {
            Long indexTableId = entry.getKey();
            int bucketCount = entry.getValue();
            IndexBucketRowCache[] bucketCaches = new IndexBucketRowCache[bucketCount];
            RemoteFetchState[] fetchStates = new RemoteFetchState[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                bucketCaches[i] =
                        new IndexBucketRowCache(
                                logTablet.getTableBucket(),
                                new TableBucket(indexTableId, i),
                                memoryPool);
                fetchStates[i] = new RemoteFetchState();
            }
            bucketRowCaches.put(indexTableId, bucketCaches);
            this.bucketFetchStates.put(indexTableId, fetchStates);
        }
        LOG.info("IndexRowCache initialized with memory pool page size: {}", memoryPool.pageSize());
    }

    /**
     * Updates the commit offset and fetch offset for a specific index bucket.
     *
     * @param indexTableId the index table id
     * @param bucketId the index bucket ID
     * @param commitOffset the commit offset
     * @param fetchOffset the fetch offset being requested
     * @param leaderServerId the leader server id that this bucket is fetching from
     */
    public void updateCommitOffset(
            long indexTableId,
            int bucketId,
            long commitOffset,
            long fetchOffset,
            int leaderServerId) {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }
        RemoteFetchState[] states = getFetchStatesArray(indexTableId);
        states[bucketId].updateCommitOffset(commitOffset, leaderServerId);
        states[bucketId].recordFetchOffset(fetchOffset);
        getBucketRowCache(indexTableId, bucketId).garbageCollectBelowOffset(commitOffset);
    }

    /**
     * Returns the min commit offset for the entire index row cache.
     *
     * @return the commit offset
     */
    public long getCommitHorizon() {
        return bucketFetchStates.values().stream()
                .map(
                        states ->
                                Arrays.stream(states)
                                        .mapToLong(RemoteFetchState::getCommitOffset)
                                        .min()
                                        .orElse(-1L))
                .min(Long::compareTo)
                .orElse(-1L);
    }

    /**
     * Gets all index buckets with commit offset equal to -1.
     *
     * @return a map of index table id to list of bucket ids with commit offset -1
     */
    public Map<Long, List<Integer>> getBucketsWithUncommittedOffsets() {
        Map<Long, List<Integer>> result = new HashMap<>();
        for (Map.Entry<Long, RemoteFetchState[]> entry : bucketFetchStates.entrySet()) {
            Long indexTableId = entry.getKey();
            RemoteFetchState[] states = entry.getValue();
            List<Integer> uncommittedBuckets = new ArrayList<>();
            for (int i = 0; i < states.length; i++) {
                if (states[i].getCommitOffset() == -1L) {
                    uncommittedBuckets.add(i);
                }
            }
            if (!uncommittedBuckets.isEmpty()) {
                result.put(indexTableId, uncommittedBuckets);
            }
        }
        return result;
    }

    /**
     * Gets detailed diagnostic information for index buckets that are blocking the commit horizon.
     * Only returns information for buckets whose commit offset equals the current commit horizon,
     * as these are the ones preventing progress.
     *
     * @return a list of diagnostic strings for buckets blocking the commit horizon
     */
    public List<String> getBlockingBucketDiagnostics() {
        List<String> diagnostics = new ArrayList<>();
        long commitHorizon = getCommitHorizon();

        // Only report buckets that are at the commit horizon (blocking progress)
        for (Map.Entry<Long, RemoteFetchState[]> entry : bucketFetchStates.entrySet()) {
            Long indexTableId = entry.getKey();
            RemoteFetchState[] states = entry.getValue();
            for (int i = 0; i < states.length; i++) {
                if (states[i].getCommitOffset() == commitHorizon) {
                    diagnostics.add(states[i].getDiagnosticString(indexTableId, i));
                }
            }
        }
        return diagnostics;
    }

    /**
     * Gets the minimum cache watermark across all index buckets. This represents the smallest start
     * offset of cached data across all IndexBuckets.
     *
     * @return the minimum cache watermark, or -1 if no data is cached
     */
    public long getMinCacheWatermark() {
        if (closed) {
            return -1L;
        }

        long minWatermark = Long.MAX_VALUE;
        boolean hasData = false;

        for (IndexBucketRowCache[] bucketCaches : bucketRowCaches.values()) {
            for (IndexBucketRowCache cache : bucketCaches) {
                long minOffset = cache.getMinCachedOffset();
                if (minOffset >= 0) {
                    hasData = true;
                    minWatermark = Math.min(minWatermark, minOffset);
                }
            }
        }

        return hasData ? minWatermark : -1L;
    }

    /**
     * Writes an IndexedRow only to the target bucket without writing empty rows to other buckets.
     * This method is used during batch processing to reduce unnecessary empty row writes.
     *
     * @param indexTableId the index table id
     * @param bucketId the index bucket ID
     * @param logOffset the log offset
     * @param changeType the change type of the row
     * @param indexedRow the IndexedRow to cache
     * @param batchStartOffset the minimum offset for gap filling
     * @param timestamp the partition column timestamp (milliseconds), -1 if no time partitioning
     * @throws IOException if an error occurs during writing
     * @throws IllegalStateException if the cache is closed
     */
    public void writeIndexedRowToTargetBucket(
            long indexTableId,
            int bucketId,
            long logOffset,
            ChangeType changeType,
            IndexedRow indexedRow,
            long batchStartOffset,
            @Nullable Long timestamp)
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

        IndexBucketRowCache targetBucketCache = indexBucketCaches[bucketId];

        while (true) {
            try {
                // Only write to the target bucket
                targetBucketCache.writeIndexedRow(
                        logOffset, changeType, indexedRow, batchStartOffset, timestamp);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Wrote IndexedRow to target bucket for index table {}, bucket {} with logOffset {}, timestamp {}",
                            indexTableId,
                            bucketId,
                            logOffset,
                            timestamp);
                }
                return;
            } catch (EOFException eofException) {
                if (LOG.isInfoEnabled()) {
                    LOG.trace(
                            "EOFException happened during writing IndexedRow to target bucket for index table {}, bucket {} at logOffset {}: {}, that meens memory pressure is too high, backoff and retry.",
                            indexTableId,
                            bucketId,
                            logOffset,
                            eofException.getMessage(),
                            eofException);
                }
            }
        }
    }

    /**
     * Synchronizes all buckets to the specified offset by writing empty rows where necessary. This
     * method is used at the end of batch processing to ensure all buckets have consistent offset
     * progression while minimizing the total number of empty row writes.
     *
     * @param indexTableId the index table id
     * @param targetOffset the target offset to synchronize all buckets to
     * @param batchStartOffset the minimum offset for gap filling
     * @throws IOException if an error occurs during synchronization
     * @throws IllegalStateException if the cache is closed
     */
    public void synchronizeAllBucketsToOffset(
            long indexTableId, long targetOffset, long batchStartOffset) throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexRowCache is closed");
        }

        IndexBucketRowCache[] indexBucketCaches = getIndexBucketRowCaches(indexTableId);

        // Diagnostic logging for debugging range gap issue
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Synchronizing all {} buckets for index table {} to targetOffset={}, batchStartOffset={}",
                    indexBucketCaches.length,
                    indexTableId,
                    targetOffset,
                    batchStartOffset);
        }

        try {
            // Write empty rows to all buckets to synchronize them to the target offset
            for (int bucketId = 0; bucketId < indexBucketCaches.length; bucketId++) {
                IndexBucketRowCache bucketCache = indexBucketCaches[bucketId];
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Synchronizing bucket {} for index table {} to offset {}, batchStartOffset: {}",
                            bucketId,
                            indexTableId,
                            targetOffset,
                            batchStartOffset);
                }
                bucketCache.writeIndexedRow(targetOffset, null, null, batchStartOffset, null);
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Synchronized all buckets for index table {} to offset {}",
                        indexTableId,
                        targetOffset);
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to synchronize buckets for index table {} to offset {}: {}",
                    indexTableId,
                    targetOffset,
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
     * @return ReadResult containing LogRecords and status indicating whether data is loaded or not
     */
    public ReadResult readIndexLogRecords(
            long indexTableId, int bucketId, long startOffset, long expectedEndOffset) {
        if (closed) {
            return ReadResult.notLoaded();
        }

        IndexBucketRowCache bucketCache = getBucketRowCache(indexTableId, bucketId);
        if (bucketCache == null) {
            return ReadResult.notLoaded();
        }

        // Check if there is a range containing the start offset
        boolean hasRange = bucketCache.hasRangeContaining(startOffset);
        if (!hasRange) {
            // Range not loaded in cache yet
            return ReadResult.notLoaded();
        }

        // Range exists, read the records (may be empty if no data in range)
        LogRecords records = bucketCache.readIndexLogRecords(startOffset, expectedEndOffset);
        return ReadResult.success(records);
    }

    public int getCachedBytesOfIndexBucketInRange(
            TableBucket indexBucket, long startOffset, long endOffset) {
        IndexBucketRowCache bucketCache =
                getBucketRowCache(indexBucket.getTableId(), indexBucket.getBucket());
        return bucketCache.getCachedRecordBytesInRange(startOffset, endOffset);
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

    /**
     * Gets the total number of memory segments used across all index buckets.
     *
     * @return the total memory segment count
     */
    public int getTotalMemorySegmentCount() {
        int totalSegmentCount = 0;
        for (IndexBucketRowCache[] indexBucketRowCaches : bucketRowCaches.values()) {
            for (IndexBucketRowCache cache : indexBucketRowCaches) {
                totalSegmentCount += cache.getMemorySegmentCount();
            }
        }
        return totalSegmentCount;
    }

    /**
     * Gets the commit offset for a specific index bucket.
     *
     * @param indexTableId the index table id
     * @param bucketId the bucket id
     * @return the commit offset, or -1 if not set
     */
    public long getCommitOffset(long indexTableId, int bucketId) {
        RemoteFetchState[] states = bucketFetchStates.get(indexTableId);
        if (states == null || bucketId >= states.length) {
            return -1L;
        }
        return states[bucketId].getCommitOffset();
    }

    /**
     * Gets offset range information for a specific index bucket.
     *
     * @param indexTableId the index table id
     * @param bucketId the bucket id
     * @return formatted string with offset range layout, empty string if no ranges exist
     */
    public String getIndexBucketRangeInfo(long indexTableId, int bucketId) {
        IndexBucketRowCache[] caches = bucketRowCaches.get(indexTableId);
        if (caches == null || bucketId >= caches.length) {
            return "";
        }

        IndexBucketRowCache cache = caches[bucketId];
        return cache.getOffsetRangeInfo();
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

    private RemoteFetchState[] getFetchStatesArray(long indexTableId) {
        RemoteFetchState[] fetchStates = bucketFetchStates.get(indexTableId);
        if (null == fetchStates) {
            throw new IllegalArgumentException(
                    "RemoteFetchState not found for indexTableId: " + indexTableId);
        }
        return fetchStates;
    }

    @VisibleForTesting
    IndexBucketRowCache getBucketRowCache(long indexTableId, int bucketId)
            throws IllegalArgumentException {
        IndexBucketRowCache[] bucketCaches = getIndexBucketRowCaches(indexTableId);
        if (bucketId < 0 || bucketId >= bucketCaches.length) {
            throw new IllegalArgumentException(
                    "Invalid bucketId: " + bucketId + " for indexTableId: " + indexTableId);
        }
        return bucketCaches[bucketId];
    }
}
