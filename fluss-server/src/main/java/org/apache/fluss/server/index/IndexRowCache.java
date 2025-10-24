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
import org.apache.fluss.server.log.LogTablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
        for (Map.Entry<Long, Integer> entry : indexBucketDistribution.entrySet()) {
            Long indexTableId = entry.getKey();
            int bucketCount = entry.getValue();
            IndexBucketRowCache[] bucketCaches = new IndexBucketRowCache[bucketCount];
            AtomicLong[] bucketCommitOffsets = new AtomicLong[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                bucketCaches[i] =
                        new IndexBucketRowCache(
                                logTablet.getTableBucket(),
                                new TableBucket(indexTableId, i),
                                memoryPool);
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
     * Writes an IndexedRow only to the target bucket without writing empty rows to other buckets.
     * This method is used during batch processing to reduce unnecessary empty row writes.
     *
     * @param indexTableId the index table id
     * @param bucketId the index bucket ID
     * @param logOffset the log offset
     * @param changeType the change type of the row
     * @param indexedRow the IndexedRow to cache
     * @param batchStartOffset the minimum offset for gap filling
     * @throws IOException if an error occurs during writing
     * @throws IllegalStateException if the cache is closed
     */
    public void writeIndexedRowToTargetBucket(
            long indexTableId,
            int bucketId,
            long logOffset,
            ChangeType changeType,
            IndexedRow indexedRow,
            long batchStartOffset)
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
                        logOffset, changeType, indexedRow, batchStartOffset);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Wrote IndexedRow to target bucket for index table {}, bucket {} with logOffset {}",
                            indexTableId,
                            bucketId,
                            logOffset);
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

        try {
            // Write empty rows to all buckets to synchronize them to the target offset
            for (IndexBucketRowCache bucketCache : indexBucketCaches) {
                bucketCache.writeIndexedRow(targetOffset, null, null, batchStartOffset);
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
