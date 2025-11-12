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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
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
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private final PendingWriteQueue pendingWriteQueue;

    // For cold data loading on failover - batch loading support
    private final long logEndOffsetOnLeaderStart;
    private final long coldLoadBatchSize;

    // Tracks the state of batch cold data loading
    private volatile long coldLoadTargetEndOffset = -1; // Final target offset to load to
    private volatile long coldLoadCurrentEndOffset = -1; // End offset of current batch
    private volatile boolean coldLoadInProgress = false; // Whether a batch is currently loading

    /**
     * Creates a new IndexCache with the new row-level cache architecture.
     *
     * @param logTablet the log tablet for data access
     * @param memoryPool the memory segment pool for zero-copy operations
     * @param dataTableSchema the schema of the data table containing index definitions
     * @param dataTablePhysicalPath the physical path of the data table
     * @param metadataCache the metadata cache for table metadata
     * @param commitHorizonCallback callback for index commit horizon changes (can be null)
     * @param dataBucketLogEndOffset the log end offset of the data bucket at the time when this
     *     index cache is created (for cold data loading boundary)
     * @param conf the server configuration
     */
    public IndexCache(
            LogTablet logTablet,
            MemorySegmentPool memoryPool,
            Schema dataTableSchema,
            PhysicalTablePath dataTablePhysicalPath,
            TabletServerMetadataCache metadataCache,
            IndexCommitHorizonCallback commitHorizonCallback,
            long dataBucketLogEndOffset,
            Configuration conf) {
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

        // Get main table info for TTL and partition configuration
        TableInfo mainTableInfo =
                metadataCache
                        .getTableMetadata(dataTablePhysicalPath.getTablePath())
                        .map(org.apache.fluss.server.metadata.TableMetadata::getTableInfo)
                        .orElse(null);

        this.indexCacheWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        dataTableSchema,
                        indexTableInfos,
                        mainTableInfo);

        // Record the data bucket's log end offset at leader start for cold data loading boundary
        // This represents the upper boundary of cold data that needs to be loaded if there's a gap
        this.logEndOffsetOnLeaderStart = dataBucketLogEndOffset;

        // Get cold load batch size from configuration
        MemorySize batchSize = conf.get(ConfigOptions.SERVER_INDEX_CACHE_COLD_LOAD_BATCH_SIZE);
        this.coldLoadBatchSize = batchSize.getBytes();

        // Initialize pending write queue to replace indexWriterExecutor
        this.pendingWriteQueue =
                new PendingWriteQueue(
                        logTablet.getTableBucket().toString(),
                        500, // queue capacity
                        3, // max retries
                        1000); // retry backoff ms

        LOG.info(
                "IndexCache initialized with row-level cache architecture for data bucket {} with {} index definitions, "
                        + "logEndOffsetOnLeaderStart: {}, coldLoadBatchSize: {} bytes",
                logTablet.getTableBucket(),
                dataTableSchema.getIndexes().size(),
                logEndOffsetOnLeaderStart,
                coldLoadBatchSize);
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
    public Tuple2<Integer, Optional<Map<TableBucket, IndexSegment>>> fetchIndex(
            Map<TableBucket, IndexCacheFetchParam> fetchRequests,
            long minAdvanceOffset,
            int maxBytes,
            boolean forceFetch)
            throws Exception {
        if (closed) {
            LOG.warn("IndexCache is closed, returning empty results");
            return Tuple2.of(0, Optional.empty());
        }

        long highWatermark = logTablet.getHighWatermark();

        Map<TableBucket, IndexSegment> results = new HashMap<>();

        updateCommitOffset(fetchRequests);

        long minFetchStartOffset =
                fetchRequests.values().stream()
                        .mapToLong(IndexCacheFetchParam::getFetchOffset)
                        .min()
                        .orElseThrow(() -> new IllegalStateException("No fetch requests provided"));

        if (!forceFetch) {
            if (highWatermark - minFetchStartOffset < minAdvanceOffset) {
                return Tuple2.of(0, Optional.empty());
            }
        }

        long currentEndOffset = minFetchStartOffset;
        int currentFetchBytes = 0;

        while (currentFetchBytes < maxBytes && currentEndOffset <= highWatermark) {
            currentEndOffset += minAdvanceOffset;
            for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                currentFetchBytes +=
                        indexRowCache.getCachedBytesOfIndexBucketInRange(
                                indexBucket, minFetchStartOffset, currentEndOffset);
            }
        }

        currentEndOffset = Math.min(currentEndOffset, highWatermark);

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

            IndexRowCache.ReadResult readResult =
                    indexRowCache.readIndexLogRecords(
                            indexBucket.getTableId(),
                            indexBucket.getBucket(),
                            startOffset,
                            currentEndOffset);

            IndexSegment segment;
            if (readResult.getStatus() == IndexRowCache.ReadStatus.NOT_LOADED) {
                // Data not loaded in cache yet, return NOT_READY segment
                segment = IndexSegment.createNotReadySegment(startOffset, currentEndOffset);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "DataBucket {}: Index bucket {} data not loaded for range [{}, {}), returning NOT_READY segment",
                            this.logTablet.getTableBucket(),
                            indexBucket,
                            startOffset,
                            currentEndOffset);
                }
            } else {
                // Data loaded, create segment with records (may be empty but range is loaded)
                LogRecords logRecords = readResult.getRecords();
                segment =
                        new IndexSegment(
                                startOffset,
                                currentEndOffset,
                                logRecords,
                                IndexSegment.DataStatus.LOADED);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "DataBucket {}: Index bucket {} fetched {} bytes of index data from range [{}, {}), highWatermark: {}",
                            dataTablePhysicalPath,
                            indexBucket,
                            logRecords.sizeInBytes(),
                            startOffset,
                            currentEndOffset,
                            highWatermark);
                }
            }
            results.put(indexBucket, segment);
        }

        return Tuple2.of(currentFetchBytes, Optional.of(results));
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "DataBucket {}({}): index commit horizon advanced to {}",
                            this.logTablet.getTableBucket(),
                            dataTablePhysicalPath,
                            currentCommitHorizon);
                }
                try {
                    commitHorizonCallback.onIndexCommitHorizonUpdate(currentCommitHorizon);
                } catch (Exception e) {
                    LOG.error("Failed to trigger commit horizon callback", e);
                }

                // Check if we need to trigger cold data loading after failover
                mayTriggerColdDataLoading(currentCommitHorizon);
            }
        }
    }

    /**
     * Checks if cold data loading should be triggered and submits the loading task if needed.
     *
     * <p>This method implements batch cold data loading to control memory usage. Cold data is
     * loaded in batches based on the configured batch size. Each batch is only loaded after the
     * previous batch has been consumed (indicated by indexCommitHorizon advancing).
     *
     * <p>Trigger conditions for new batch: 1. No batch currently in progress 2.
     * logEndOffsetOnLeaderStart > 0 (historical data exists) 3. IndexCommitHorizon <
     * logEndOffsetOnLeaderStart (not all historical data consumed) 4. Gap exists between cache and
     * consumption progress (minCacheWatermark > IndexCommitHorizon or cache is empty) 5. Current
     * batch consumed: currentCommitHorizon >= coldLoadCurrentEndOffset (or no batch loaded yet)
     *
     * <p>Note: IndexCommitHorizon >= 0 is always valid, including 0 which means downstream needs to
     * consume from offset 0.
     *
     * @param currentCommitHorizon the current index commit horizon
     */
    private void mayTriggerColdDataLoading(long currentCommitHorizon) {
        // If logEndOffsetOnLeaderStart <= 0, there is no historical data to load
        // All data written after becoming leader is hot data and already cached
        if (logEndOffsetOnLeaderStart <= 0) {
            return;
        }

        // If IndexCommitHorizon has reached or exceeded logEndOffsetOnLeaderStart,
        // it means all historical data that existed when becoming leader has been consumed
        // No need to load cold data anymore
        if (currentCommitHorizon >= logEndOffsetOnLeaderStart) {
            // Reset state if all cold data has been consumed
            if (coldLoadTargetEndOffset > 0) {
                LOG.info(
                        "DataBucket {}: All cold data has been consumed, resetting cold load state",
                        logTablet.getTableBucket());
                coldLoadTargetEndOffset = -1;
                coldLoadCurrentEndOffset = -1;
                coldLoadInProgress = false;
            }
            return;
        }

        // Check if a batch is currently being loaded
        if (coldLoadInProgress) {
            return;
        }

        // Check if there's a gap between cache and consumption progress
        long minCacheWatermark = indexRowCache.getMinCacheWatermark();

        // If no gap (cache is adjacent to or overlaps with commit horizon), no need to load
        if (minCacheWatermark >= 0 && minCacheWatermark <= currentCommitHorizon) {
            return;
        }

        // Check if previous batch has been consumed
        // If coldLoadCurrentEndOffset > 0, it means we have loaded a batch before
        // We should only load the next batch after the current one is consumed
        if (coldLoadCurrentEndOffset > 0 && currentCommitHorizon < coldLoadCurrentEndOffset) {
            // Previous batch not yet consumed, wait for it to be consumed
            return;
        }

        // Calculate the range for the next batch
        long loadStartOffset = currentCommitHorizon;

        // Determine the final target end offset if not set yet
        if (coldLoadTargetEndOffset < 0) {
            if (minCacheWatermark < 0) {
                // Cache is empty, target is logEndOffsetOnLeaderStart
                coldLoadTargetEndOffset = logEndOffsetOnLeaderStart;
            } else {
                // Cache has some data, target is min(logEndOffsetOnLeaderStart, minCacheWatermark)
                coldLoadTargetEndOffset = Math.min(logEndOffsetOnLeaderStart, minCacheWatermark);
            }
        }

        // Calculate batch end offset based on batch size
        // Use a simple heuristic: assume average record size of 1KB
        // This is a rough estimate - actual size may vary
        long estimatedRecordsInBatch = coldLoadBatchSize / 1024; // Assume 1KB per record
        long batchEndOffset =
                Math.min(loadStartOffset + estimatedRecordsInBatch, coldLoadTargetEndOffset);

        // Validate the range
        if (loadStartOffset >= batchEndOffset) {
            return;
        }

        // Mark batch as in progress
        coldLoadInProgress = true;
        coldLoadCurrentEndOffset = batchEndOffset;

        LOG.info(
                "DataBucket {}: Triggering batch cold data loading from WAL, batch range [{}, {}), "
                        + "target end offset: {}, batch size limit: {} bytes",
                logTablet.getTableBucket(),
                loadStartOffset,
                batchEndOffset,
                coldLoadTargetEndOffset,
                coldLoadBatchSize);

        // Create completion callback to handle batch completion
        ColdDataLoad.CompletionCallback callback =
                new ColdDataLoad.CompletionCallback() {
                    @Override
                    public void onComplete(long startOffset, long endOffset) {
                        LOG.info(
                                "DataBucket {}: Cold data loading batch completed successfully, range [{}, {})",
                                logTablet.getTableBucket(),
                                startOffset,
                                endOffset);
                        // Reset flag to allow next batch to be loaded
                        coldLoadInProgress = false;
                    }

                    @Override
                    public void onFailure(long startOffset, long endOffset, Exception cause) {
                        LOG.error(
                                "DataBucket {}: Cold data loading batch failed for range [{}, {})",
                                logTablet.getTableBucket(),
                                startOffset,
                                endOffset,
                                cause);
                        // Reset flag to allow retry
                        coldLoadInProgress = false;
                    }
                };

        // Submit cold data loading task for this batch
        ColdDataLoad coldDataLoad =
                new ColdDataLoad(indexCacheWriter, loadStartOffset, batchEndOffset, callback);
        boolean submitted = pendingWriteQueue.submit(coldDataLoad);

        if (!submitted) {
            LOG.error(
                    "DataBucket {}: Failed to submit cold data loading task for batch range [{}, {})",
                    logTablet.getTableBucket(),
                    loadStartOffset,
                    batchEndOffset);
            // Reset flag to allow retry
            coldLoadInProgress = false;
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
            if (walBuilder != null) {
                walBuilder.deallocate();
            }
            return;
        }

        // Create hot data write task and submit to pending write queue
        HotDataWrite hotDataWrite =
                new HotDataWrite(indexCacheWriter, walBuilder, walRecords, appendInfo);
        boolean submitted = pendingWriteQueue.submit(hotDataWrite);

        if (!submitted) {
            LOG.error(
                    "Failed to submit hot data write task for table bucket {}, range [{}, {}), queue may be full",
                    logTablet.getTableBucket(),
                    appendInfo.firstOffset(),
                    appendInfo.lastOffset() + 1);
            // Deallocate walBuilder if submission fails
            if (walBuilder != null) {
                walBuilder.deallocate();
            }
        }
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
     * Gets all index buckets with commit offset equal to -1.
     *
     * @return a map of index table id to list of bucket ids with commit offset -1
     */
    public Map<Long, List<Integer>> getBucketsWithUncommittedOffsets() {
        return indexRowCache.getBucketsWithUncommittedOffsets();
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

        // Close the pending write queue
        if (pendingWriteQueue != null) {
            pendingWriteQueue.close();
        }

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

    // ================================================================================================
    // Data Structures (Maintaining Interface Compatibility)
    // ================================================================================================

    /** IndexCache-specific fetch request structure. */
    public static final class IndexCacheFetchParam {
        private final long fetchOffset;
        private final long indexCommitOffset;

        public IndexCacheFetchParam(long fetchOffset, long indexCommitOffset) {
            this.fetchOffset = fetchOffset;
            this.indexCommitOffset = indexCommitOffset;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public long getIndexCommitOffset() {
            return indexCommitOffset;
        }
    }
}
