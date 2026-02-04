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
 * IndexBucketCacheManager manages row-level index caches organized by index bucket.
 *
 * <p>Storage Model: - bucketCaches: Map&lt;TableBucket, IndexBucketCache&gt; - Row-level caches
 * organized by index bucket - Each IndexBucketCache maintains independent MemorySegment space and
 * RowCacheIndex - Unified MemorySegmentPool for memory segment allocation and recycling
 *
 * <p>Core Features: - Manages multiple IndexBucketCache instances for different index buckets -
 * Provides unified interface for writing and reading IndexedRow data - Handles cleanup and memory
 * management across all bucket caches - Supports batch operations for improved performance
 *
 * <p>Thread Safety: This class is NOT thread-safe. External synchronization is required.
 */
@Internal
public final class IndexBucketCacheManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBucketCacheManager.class);

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

        public static ReadResult success(LogRecords records) {
            return new ReadResult(records, ReadStatus.LOADED);
        }

        public static ReadResult notLoaded() {
            return new ReadResult(createEmptyLogRecords(), ReadStatus.NOT_LOADED);
        }
    }

    /** Status of reading index log records from cache. */
    public enum ReadStatus {
        LOADED,
        NOT_LOADED
    }

    /** Map from index bucket to its row cache. */
    private final Map<Long, IndexBucketCache[]> bucketCaches = new HashMap<>();

    /** Map from index table id to fetch states (including commit offsets) for each bucket. */
    private final Map<Long, RemoteFetchState[]> bucketFetchStates = new HashMap<>();

    /** Whether this cache manager has been closed. */
    private boolean closed = false;

    public IndexBucketCacheManager(
            MemorySegmentPool memoryPool,
            LogTablet logTablet,
            Map<Long, Integer> indexBucketDistribution) {
        for (Map.Entry<Long, Integer> entry : indexBucketDistribution.entrySet()) {
            Long indexTableId = entry.getKey();
            int bucketCount = entry.getValue();
            IndexBucketCache[] caches = new IndexBucketCache[bucketCount];
            RemoteFetchState[] fetchStates = new RemoteFetchState[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                caches[i] =
                        new IndexBucketCache(
                                logTablet.getTableBucket(),
                                new TableBucket(indexTableId, i),
                                memoryPool);
                fetchStates[i] = new RemoteFetchState();
            }
            bucketCaches.put(indexTableId, caches);
            this.bucketFetchStates.put(indexTableId, fetchStates);
        }
        LOG.info(
                "IndexBucketCacheManager initialized with memory pool page size: {}",
                memoryPool.pageSize());
    }

    public void updateCommitOffset(
            long indexTableId,
            int bucketId,
            long commitOffset,
            long fetchOffset,
            int leaderServerId) {
        if (closed) {
            throw new IllegalStateException("IndexBucketCacheManager is closed");
        }
        RemoteFetchState[] states = getFetchStatesArray(indexTableId);
        states[bucketId].updateCommitOffset(commitOffset, leaderServerId);
        states[bucketId].recordFetchOffset(fetchOffset);
        getBucketCache(indexTableId, bucketId).garbageCollectBelowOffset(commitOffset);
    }

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

    public List<String> getBlockingBucketDiagnostics() {
        List<String> diagnostics = new ArrayList<>();
        long commitHorizon = getCommitHorizon();

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

    public long getMinCacheWatermark() {
        if (closed) {
            return -1L;
        }

        long minWatermark = Long.MAX_VALUE;
        boolean hasData = false;

        for (IndexBucketCache[] caches : bucketCaches.values()) {
            for (IndexBucketCache cache : caches) {
                long minOffset = cache.getMinCachedOffset();
                if (minOffset >= 0) {
                    hasData = true;
                    minWatermark = Math.min(minWatermark, minOffset);
                }
            }
        }

        return hasData ? minWatermark : -1L;
    }

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
            throw new IllegalStateException("IndexBucketCacheManager is closed");
        }
        checkArgument(
                changeType != null && indexedRow != null,
                "changeType and indexedRow cannot be null");

        IndexBucketCache targetBucketCache = getBucketCache(indexTableId, bucketId);

        while (true) {
            if (closed) {
                throw new IllegalStateException("IndexBucketCacheManager is closed");
            }
            try {
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
                            "EOFException happened during writing IndexedRow to target bucket for index table {}, bucket {} at logOffset {}: {}, that means memory pressure is too high, backoff and retry.",
                            indexTableId,
                            bucketId,
                            logOffset,
                            eofException.getMessage(),
                            eofException);
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while waiting for index cache memory",
                            interruptedException);
                }
            }
        }
    }

    public void synchronizeAllBucketsToOffset(
            long indexTableId, long targetOffset, long batchStartOffset) throws IOException {
        if (closed) {
            throw new IllegalStateException("IndexBucketCacheManager is closed");
        }

        IndexBucketCache[] indexBucketCaches = getIndexBucketCaches(indexTableId);

        LOG.debug(
                "Synchronizing all {} buckets for index table {} to targetOffset={}, batchStartOffset={}",
                indexBucketCaches.length,
                indexTableId,
                targetOffset,
                batchStartOffset);

        for (int bucketId = 0; bucketId < indexBucketCaches.length; bucketId++) {
            IndexBucketCache bucketCache = indexBucketCaches[bucketId];
            LOG.trace(
                    "Synchronizing bucket {} for index table {} to offset {}, batchStartOffset: {}",
                    bucketId,
                    indexTableId,
                    targetOffset,
                    batchStartOffset);
            bucketCache.writeIndexedRow(targetOffset, null, null, batchStartOffset, null);
        }

        LOG.trace(
                "Synchronized all buckets for index table {} to offset {}",
                indexTableId,
                targetOffset);
    }

    public ReadResult readIndexLogRecords(
            long indexTableId, int bucketId, long startOffset, long expectedEndOffset) {
        if (closed) {
            return ReadResult.notLoaded();
        }

        IndexBucketCache bucketCache = getBucketCache(indexTableId, bucketId);
        if (bucketCache == null) {
            return ReadResult.notLoaded();
        }

        boolean hasRange = bucketCache.hasRangeContaining(startOffset);
        if (!hasRange) {
            return ReadResult.notLoaded();
        }

        LogRecords records = bucketCache.readIndexLogRecords(startOffset, expectedEndOffset);
        return ReadResult.success(records);
    }

    public int getCachedBytesOfIndexBucketInRange(
            TableBucket indexBucket, long startOffset, long endOffset) {
        IndexBucketCache bucketCache =
                getBucketCache(indexBucket.getTableId(), indexBucket.getBucket());
        return bucketCache.getCachedRecordBytesInRange(startOffset, endOffset);
    }

    public int getTotalEntries() {
        int totalEntries = 0;
        for (IndexBucketCache[] caches : bucketCaches.values()) {
            for (IndexBucketCache cache : caches) {
                totalEntries += cache.totalEntries();
            }
        }
        return totalEntries;
    }

    public int getCachedBucketCount() {
        return bucketCaches.size();
    }

    public boolean isEmpty() {
        return bucketCaches.isEmpty();
    }

    public int getTotalMemorySegmentCount() {
        int totalSegmentCount = 0;
        for (IndexBucketCache[] caches : bucketCaches.values()) {
            for (IndexBucketCache cache : caches) {
                totalSegmentCount += cache.getMemorySegmentCount();
            }
        }
        return totalSegmentCount;
    }

    public long getCommitOffset(long indexTableId, int bucketId) {
        RemoteFetchState[] states = bucketFetchStates.get(indexTableId);
        if (states == null || bucketId >= states.length) {
            return -1L;
        }
        return states[bucketId].getCommitOffset();
    }

    public String getIndexBucketRangeInfo(long indexTableId, int bucketId) {
        IndexBucketCache[] caches = bucketCaches.get(indexTableId);
        if (caches == null || bucketId >= caches.length) {
            return "";
        }

        IndexBucketCache cache = caches[bucketId];
        return cache.getOffsetRangeInfo();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info(
                "Closing IndexBucketCacheManager with {} cached buckets, {} total entries",
                getCachedBucketCount(),
                getTotalEntries());

        for (Map.Entry<Long, IndexBucketCache[]> entry : bucketCaches.entrySet()) {
            for (int i = 0; i < entry.getValue().length; i++) {
                try {
                    IndexBucketCache cache = entry.getValue()[i];
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
        bucketCaches.clear();

        closed = true;
        LOG.info("IndexBucketCacheManager closed successfully");
    }

    private static LogRecords createEmptyLogRecords() {
        return IndexBucketCache.createEmptyLogRecords();
    }

    private IndexBucketCache[] getIndexBucketCaches(long indexTableId) {
        IndexBucketCache[] caches = bucketCaches.get(indexTableId);
        if (null == caches) {
            throw new IllegalArgumentException(
                    "IndexBucketCache not found for indexTableId: " + indexTableId);
        }
        return caches;
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
    IndexBucketCache getBucketCache(long indexTableId, int bucketId) {
        IndexBucketCache[] caches = getIndexBucketCaches(indexTableId);
        checkArgument(
                bucketId >= 0 && bucketId < caches.length,
                "Invalid bucketId: %s for indexTableId: %s (valid range: [0, %s))",
                bucketId,
                indexTableId,
                caches.length);
        return caches[bucketId];
    }
}
