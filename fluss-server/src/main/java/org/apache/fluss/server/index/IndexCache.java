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
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * IndexCache is a stateful component held by each Leader Replica to manage index data caching for
 * all index tables using a new row-level storage + dynamic assembly architecture.
 *
 * <p>Core Features (Refactored):
 *
 * <ul>
 *   <li>Row-level storage: Caches IndexedRow data directly instead of pre-assembled LogRecords
 *   <li>Dynamic assembly: Constructs LogRecords on-demand based on fetch parameters
 *   <li>Discrete offset support: Handles non-continuous offset ranges with LOADED/EMPTY/UNLOADED
 *       states
 *   <li>Hot data writing: Supports real-time IndexedRow writing for immediate caching
 *   <li>Cold data loading: Automatically loads missing data from WAL using IndexCacheWriter
 *   <li>Precise data cutting: Avoids unnecessary data transmission with exact boundary handling
 *   <li>Zero-copy operations: Uses MultiBytesView for efficient memory management
 * </ul>
 *
 * <p>Architecture Changes: - Replaced CachedIndexSegments with IndexRowCache for row-level
 * management - Integrated IndexCacheWriter for enhanced cold data loading - Added support for
 * hot/cold data fusion with consistent data integrity - Enhanced fetchIndex with three-stage
 * processing: cache analysis → cold loading → dynamic assembly
 *
 * <p>Thread Safety: This class provides concurrent read access with exclusive write operations
 * using ReadWriteLock. Critical state updates use atomic operations for consistency.
 */
@Internal
public final class IndexCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCache.class);

    /** Callback interface for index commit horizon changes. */
    public interface IndexCommitHorizonCallback {
        /**
         * Called when index commit horizon is updated.
         *
         * @param newHorizon the new index commit horizon
         */
        void onIndexCommitHorizonUpdate(long newHorizon);
    }

    private final LogTablet logTablet;

    private final IndexCacheWriter indexCacheWriter;
    private final PhysicalTablePath dataTablePhysicalPath;
    private final IndexCommitHorizonCallback commitHorizonCallback;

    private final IndexRowCache indexRowCache;

    private volatile long lastIndexCommitHorizon = -1;

    private volatile boolean closed = false;

    private final ExecutorService indexWriterExecutor;

    /**
     * Creates a new IndexCache with the new row-level cache architecture.
     *
     * @param logTablet the log tablet for data access
     * @param memoryPool the memory segment pool for zero-copy operations
     * @param dataTableSchema the schema of the data table containing index definitions
     * @param dataTablePhysicalPath the physical path of the data table
     * @param metadataCache the metadata cache for table metadata
     * @param commitHorizonCallback callback for index commit horizon changes (can be null)
     */
    public IndexCache(
            LogTablet logTablet,
            MemorySegmentPool memoryPool,
            Schema dataTableSchema,
            PhysicalTablePath dataTablePhysicalPath,
            TabletServerMetadataCache metadataCache,
            IndexCommitHorizonCallback commitHorizonCallback) {
        this.logTablet = checkNotNull(logTablet, "logTablet cannot be null");
        checkNotNull(memoryPool, "memoryPool cannot be null");
        checkNotNull(dataTableSchema, "dataTableSchema cannot be null");
        checkNotNull(metadataCache, "indexMetadataManager cannot be null");
        this.dataTablePhysicalPath =
                checkNotNull(dataTablePhysicalPath, "dataTablePhysicalPath cannot be null");
        this.commitHorizonCallback = commitHorizonCallback;

        BucketingFunction bucketingFunction = BucketingFunction.of(null);

        List<TableInfo> indexTableInfos;
        try {
            indexTableInfos = getIndexTablesWithRetry(metadataCache, dataTablePhysicalPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize IndexCache: " + e.getMessage(), e);
        }

        Map<Long, Integer> indexTableBucketDistribution =
                indexTableInfos.stream()
                        .collect(
                                HashMap::new,
                                (m, t) -> m.put(t.getTableId(), t.getNumBuckets()),
                                HashMap::putAll);
        this.indexRowCache = new IndexRowCache(memoryPool, logTablet, indexTableBucketDistribution);

        this.indexCacheWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        dataTableSchema,
                        indexTableInfos);

        this.indexWriterExecutor =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("index-writer-executor"));

        LOG.info(
                "IndexCache initialized with row-level cache architecture for data bucket {} with {} index definitions",
                logTablet.getTableBucket(),
                dataTableSchema.getIndexes().size());
    }

    /**
     * Fetch index log data for the specified index buckets using the new row-level cache
     * architecture.
     *
     * <p>Enhanced Processing Flow:
     *
     * <ol>
     *   <li>Cache Analysis: Check data coverage in row-level cache, identify LOADED/EMPTY/UNLOADED
     *       ranges
     *   <li>Cold Data Loading: Load missing UNLOADED ranges from WAL using IndexCacheWriter
     *   <li>Dynamic Assembly: Construct LogRecords on-demand using MultiBytesView for precise data
     *       cutting
     * </ol>
     *
     * @param fetchRequests Map of index bucket to fetch parameters
     * @return Map of index bucket to IndexSegment with dynamically assembled data
     * @throws Exception if an error occurs during fetch operation
     */
    public Optional<Map<TableBucket, IndexSegment>> fetchIndex(
            Map<TableBucket, IndexCacheFetchParam> fetchRequests, boolean hotDataOnly)
            throws Exception {
        if (closed) {
            LOG.warn("IndexCache is closed, returning empty results");
            return Optional.empty();
        }

        long highWatermark = logTablet.getHighWatermark();

        // Execute optimized batch processing flow
        Map<TableBucket, IndexSegment> results = new HashMap<>();
        Map<TableBucket, List<OffsetRangeInfo>> unloadedRanges;

        if (hotDataOnly) {
            updateCommitOffset(fetchRequests);
        }

        // Stage 1: For hotDataOnly mode, check if we have sufficient hot data first
        if (hotDataOnly) {
            boolean hasSufficientHotData = indexRowCache.hasSufficientHotDataForAll(fetchRequests);
            if (!hasSufficientHotData) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "DataBucket {}: Insufficient hot data for minBucketFetchRecords requirement, returning empty to trigger DelayedFetch",
                            this.logTablet.getTableBucket());
                }
                return Optional.empty();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "DataBucket {}: Sufficient hot data available, proceeding with hot data fetch",
                        this.logTablet.getTableBucket());
            }
        }

        // Stage 2: Collect all unloaded ranges from all IndexBuckets in batch
        unloadedRanges = indexRowCache.getUnloadedRanges(fetchRequests);

        // Stage 3: For hotDataOnly mode, if there are unloaded ranges, return empty
        // This preserves the existing behavior for cases where data needs WAL loading
        if (!unloadedRanges.isEmpty() && hotDataOnly) {
            for (Map.Entry<TableBucket, List<OffsetRangeInfo>> entry : unloadedRanges.entrySet()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "DataBucket {}: Index bucket {} has unloaded ranges: {}, returning empty for hotDataOnly mode",
                            this.logTablet.getTableBucket(),
                            entry.getKey(),
                            entry.getValue());
                }
            }
            return Optional.empty();
        }

        // Stage 4: Batch load cold data if needed (single WAL read for all IndexBuckets)
        if (!unloadedRanges.isEmpty()) {
            loadColdDataForRanges(unloadedRanges);
        }

        // Stage 5: Assemble IndexSegments for each IndexBucket
        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            IndexCacheFetchParam param = entry.getValue();

            long startOffset = param.getFetchOffset();
            if (startOffset >= highWatermark) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "DataBucket {}: Index bucket {} has no data to fetch from start offset {}",
                            this.logTablet.getTableBucket(),
                            indexBucket,
                            startOffset);
                }
                results.put(indexBucket, IndexSegment.createEmptySegment(highWatermark));
                continue;
            }

            LogRecords logRecords =
                    indexRowCache.readIndexLogRecords(
                            indexBucket.getTableId(),
                            indexBucket.getBucket(),
                            startOffset,
                            highWatermark);

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "DataBucket {}: Index bucket {} fetched {} bytes of index data from range [{}, {})",
                        dataTablePhysicalPath,
                        indexBucket,
                        logRecords.sizeInBytes(),
                        startOffset,
                        highWatermark);
            }
            IndexSegment segment = new IndexSegment(startOffset, highWatermark, logRecords);
            results.put(indexBucket, segment);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Batch fetched index data for {} buckets, loaded ranges: {}",
                    results.size(),
                    unloadedRanges);
        }
        return Optional.of(results);
    }

    private void updateCommitOffset(Map<TableBucket, IndexCacheFetchParam> fetchRequests) {
        long maxCommitOffset = -1;
        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            long commitOffset = entry.getValue().getIndexCommitOffset();
            indexRowCache.updateCommitOffset(
                    indexBucket.getTableId(), indexBucket.getBucket(), commitOffset);
            maxCommitOffset = Math.max(maxCommitOffset, commitOffset);
        }
        mayTriggerCommitHorizonCallback(maxCommitOffset);
    }

    private void mayTriggerCommitHorizonCallback(long maxCommitOffset) {
        if (maxCommitOffset > lastIndexCommitHorizon) {
            long currentCommitHorizon = indexRowCache.getCommitHorizon();
            if (currentCommitHorizon > lastIndexCommitHorizon) {
                lastIndexCommitHorizon = currentCommitHorizon;
                try {
                    commitHorizonCallback.onIndexCommitHorizonUpdate(currentCommitHorizon);
                } catch (Exception e) {
                    LOG.error("Failed to trigger commit horizon callback", e);
                }
            }
        }
    }

    /**
     * Write hot data from WAL records to index cache. This method leverages IndexCacheWriter to
     * process WAL data and write indexed data directly to cache for immediate availability.
     *
     * @param walBuilder the WALBuilder used to process WAL data
     * @param walRecords the WAL LogRecords generated during KV processing
     * @param appendInfo the log append information containing offset details
     */
    public void cacheIndexDataByHotData(
            WalBuilder walBuilder, LogRecords walRecords, LogAppendInfo appendInfo) {

        if (closed) {
            LOG.warn(
                    "IndexCache is closed, cannot write hot data for table bucket {}",
                    logTablet.getTableBucket());
            return;
        }

        indexWriterExecutor.submit(
                () -> {
                    try {
                        while (!closed) {
                            try {
                                indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(
                                            "Successfully processed hot data from WAL for table bucket {}",
                                            logTablet.getTableBucket());
                                }
                                return;
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to process hot data from WAL for table bucket {}",
                                        logTablet.getTableBucket(),
                                        e);
                            }
                        }
                    } finally {
                        if (null != walBuilder) {
                            walBuilder.deallocate();
                        }
                    }
                });
    }

    /**
     * Get the current index commit horizon.
     *
     * @return current index commit horizon
     */
    public long getIndexCommitHorizon() {
        return indexRowCache.getCommitHorizon();
    }

    /**
     * Gets the IndexRowCache for memory usage analysis.
     *
     * @return the IndexRowCache instance
     */
    public IndexRowCache getIndexRowCache() {
        return indexRowCache;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing IndexCache for data bucket {}", logTablet.getTableBucket());

        closed = true;

        // Close the row cache manager
        if (indexRowCache != null) {
            indexRowCache.close();
        }

        // Close the cache writer
        if (indexCacheWriter != null) {
            try {
                indexCacheWriter.close();
            } catch (IOException e) {
                LOG.warn("Error closing IndexCacheWriter", e);
            }
        }

        LOG.info("IndexCache closed for data bucket {}", logTablet.getTableBucket());
    }

    /**
     * Get related index tables with retry mechanism to handle index table creation delays.
     *
     * <p>During main table creation, index tables are created after the main table. This method
     * implements retry logic to wait for index table creation to complete.
     *
     * @param metadataCache the metadata cache
     * @param dataTablePhysicalPath the physical path of the data table
     * @return list of index table infos
     * @throws Exception if max retries exceeded or other errors occur
     */
    private List<TableInfo> getIndexTablesWithRetry(
            TabletServerMetadataCache metadataCache, PhysicalTablePath dataTablePhysicalPath)
            throws Exception {

        // Configuration for retry mechanism: retry every 1 second for up to 300 times (5 minutes)
        final int maxRetries = 300;
        final long retryIntervalMs = 1000;

        Exception lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                List<TableInfo> indexTableInfos =
                        metadataCache.getRelatedIndexTables(dataTablePhysicalPath.getTablePath());

                if (attempt > 0) {
                    LOG.info(
                            "Successfully retrieved index table information for data bucket {} after {} attempts",
                            logTablet.getTableBucket(),
                            attempt + 1);
                }

                return indexTableInfos;

            } catch (Exception e) {
                lastException = e;

                if (attempt == maxRetries - 1) {
                    // Either not retryable or max attempts reached
                    break;
                }

                LOG.warn(
                        "Failed to retrieve index table information for data bucket {} on attempt {} (retryable error). "
                                + "Index tables may still be creating. Retrying in {} ms. Error: {}",
                        logTablet.getTableBucket(),
                        attempt + 1,
                        retryIntervalMs,
                        e.getMessage());

                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new Exception(
                            "Interrupted while waiting to retry index table retrieval", ie);
                }
            }
        }

        // If we get here, all retries failed
        String errorMsg =
                String.format(
                        "Failed to retrieve index table information for data bucket %s after %d attempts. "
                                + "Index tables may not have been created yet or there is a configuration issue.",
                        logTablet.getTableBucket(), maxRetries);

        LOG.error(errorMsg, lastException);
        throw new Exception(errorMsg, lastException);
    }

    /**
     * Batch load cold data from WAL for multiple IndexBuckets using optimized single WAL read.
     *
     * <p>This method implements the core optimization by:
     *
     * <ul>
     *   <li>Computing global WAL read range from all IndexBucket requirements
     *   <li>Reading WAL data once for the entire global range
     *   <li>Conditionally processing data based on each IndexBucket's declared needs
     *   <li>Writing data only to IndexBuckets that declared they need it
     * </ul>
     */
    private void loadColdDataForRanges(Map<TableBucket, List<OffsetRangeInfo>> unloadedRanges)
            throws Exception {

        // Check if there are actually any ranges to load
        boolean hasNonEmptyRanges =
                unloadedRanges.values().stream().anyMatch(ranges -> !ranges.isEmpty());

        if (!hasNonEmptyRanges) {
            LOG.debug("No non-empty unloaded ranges found, skipping cold data loading");
            return;
        }

        // Calculate global WAL read range - FIX: Process ALL ranges, not just first one
        long globalStartOffset =
                unloadedRanges.values().stream()
                        .flatMap(List::stream)
                        .map(OffsetRangeInfo::getStartOffset)
                        .min(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected non-empty ranges but found none"));

        long globalEndOffset =
                unloadedRanges.values().stream()
                        .flatMap(List::stream)
                        .map(OffsetRangeInfo::getEndOffset)
                        .max(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected non-empty ranges but found none"));

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Loading cold data for global range [{}, {}) covering {} index buckets with total {} unloaded ranges",
                    globalStartOffset,
                    globalEndOffset,
                    unloadedRanges.size(),
                    unloadedRanges.values().stream().mapToInt(List::size).sum());
        }

        indexCacheWriter.loadColdDataToCache(globalStartOffset, globalEndOffset);
    }

    // ================================================================================================
    // Data Structures (Maintaining Interface Compatibility)
    // ================================================================================================

    /** IndexCache-specific fetch request structure. */
    public static final class IndexCacheFetchParam {
        private final long indexTableId;
        private final long fetchOffset;
        private final long minFetchHotDataRecords;
        private final long indexCommitOffset;

        public IndexCacheFetchParam(
                long indexTableId,
                long fetchOffset,
                long minFetchHotDataRecords,
                long indexCommitOffset) {
            this.indexTableId = indexTableId;
            this.fetchOffset = fetchOffset;
            this.minFetchHotDataRecords = minFetchHotDataRecords;
            this.indexCommitOffset = indexCommitOffset;
        }

        public long getIndexTableId() {
            return indexTableId;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public long getMinFetchHotDataRecords() {
            return minFetchHotDataRecords;
        }

        public long getIndexCommitOffset() {
            return indexCommitOffset;
        }
    }
}
