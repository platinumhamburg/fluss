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
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * hot/cold data fusion with consistent data integrity - Enhanced fetchIndexLogData with three-stage
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

    @SuppressWarnings("unused") // Used indirectly through indexRowCache and indexCacheWriter
    private final MemorySegmentPool memoryPool;

    private final IndexCacheWriter indexCacheWriter;
    private final List<Schema.Index> indexDefinitions;
    private final TabletServerMetadataCache tabletServerMetadataCache;
    private final PhysicalTablePath dataTablePhysicalPath;
    private final IndexCommitHorizonCallback commitHorizonCallback;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile long lastIndexCommitHorizon = -1;

    private final IndexRowCache indexRowCache;

    // Configuration and state
    private volatile boolean closed = false;

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
        this.memoryPool = checkNotNull(memoryPool, "memoryPool cannot be null");
        checkNotNull(dataTableSchema, "dataTableSchema cannot be null");
        this.tabletServerMetadataCache =
                checkNotNull(metadataCache, "indexMetadataManager cannot be null");
        this.dataTablePhysicalPath =
                checkNotNull(dataTablePhysicalPath, "dataTablePhysicalPath cannot be null");
        this.commitHorizonCallback = commitHorizonCallback;

        // Extract index definitions from the data table schema
        this.indexDefinitions = dataTableSchema.getIndexes();

        BucketingFunction bucketingFunction = BucketingFunction.of(null);

        List<TableInfo> indexTableInfos =
                metadataCache.getRelatedIndexTables(dataTablePhysicalPath.getTablePath());

        Map<TablePartitionId, Integer> indexTableBucketDistribution =
                indexTableInfos.stream()
                        .collect(
                                HashMap::new,
                                (m, t) ->
                                        m.put(
                                                TablePartitionId.of(t.getTableId(), null),
                                                t.getNumBuckets()),
                                HashMap::putAll);
        this.indexRowCache = new IndexRowCache(memoryPool, logTablet, indexTableBucketDistribution);

        this.indexCacheWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        dataTableSchema,
                        indexTableInfos);

        LOG.info(
                "IndexCache initialized with row-level cache architecture for data bucket {} with {} index definitions",
                logTablet.getTableBucket(),
                indexDefinitions.size());
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
    public Optional<Map<TableBucket, IndexSegment>> fetchIndexLogData(
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

        lock.readLock().lock();
        try {

            // Stage 1: Collect all unloaded ranges from all IndexBuckets in batch
            unloadedRanges = indexRowCache.getUnloadedRanges(fetchRequests);

            if (!unloadedRanges.isEmpty() && hotDataOnly) {
                for (Map.Entry<TableBucket, List<OffsetRangeInfo>> entry :
                        unloadedRanges.entrySet()) {
                    LOG.info(
                            "DataBucket {}: Index bucket {} has unloaded ranges: {}",
                            this.logTablet.getTableBucket(),
                            entry.getKey(),
                            entry.getValue());
                }
                return Optional.empty();
            }

            // Stage 2: Batch load cold data if needed (single WAL read for all IndexBuckets)
            if (!unloadedRanges.isEmpty()) {
                loadColdDataForRanges(unloadedRanges);
            }

            // Stage 3: Assemble IndexSegments for each IndexBucket
            for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                IndexCacheFetchParam param = entry.getValue();

                long startOffset = param.getFetchOffset();
                if (startOffset >= highWatermark) {
                    LOG.trace(
                            "DataBucket {}: Index bucket {} has no data to fetch from start offset {}",
                            this.logTablet.getTableBucket(),
                            indexBucket,
                            startOffset);
                    results.put(indexBucket, IndexSegment.createEmptySegment(highWatermark));
                    continue;
                }

                short schemaId = getSchemaIdForIndex(param.getIndexTableId());

                // Get LogRecords directly from IndexRowCache with accurate metadata
                // This eliminates the need for IndexLogRecordsBuilder's tricky estimations
                LogRecords logRecords =
                        indexRowCache.getRangeLogRecords(
                                TablePartitionId.from(indexBucket),
                                indexBucket.getBucket(),
                                startOffset,
                                highWatermark,
                                schemaId);

                LOG.trace(
                        "DataBucket {}: Index bucket {} fetched {} bytes of index data from range [{}, {})",
                        dataTablePhysicalPath,
                        indexBucket,
                        logRecords.sizeInBytes(),
                        startOffset,
                        highWatermark);
                IndexSegment segment = new IndexSegment(startOffset, highWatermark, logRecords);
                results.put(indexBucket, segment);
            }
        } finally {
            lock.readLock().unlock();
        }

        LOG.debug(
                "Batch fetched index data for {} buckets, loaded ranges: {}",
                results.size(),
                unloadedRanges);
        return Optional.of(results);
    }

    private void updateCommitOffset(Map<TableBucket, IndexCacheFetchParam> fetchRequests) {
        long maxOffset = -1;
        for (Map.Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            long commitOffset = entry.getValue().getIndexCommitOffset();
            indexRowCache.updateCommitOffset(
                    TablePartitionId.from(indexBucket), indexBucket.getBucket(), commitOffset);
            maxOffset = Math.max(maxOffset, commitOffset);
        }
        mayTriggerCommitHorizonCallback(maxOffset);
    }

    private void mayTriggerCommitHorizonCallback(long commitOffset) {
        if (commitOffset > lastIndexCommitHorizon) {
            long currentCommitHorizon = indexRowCache.getCommitHorizon();
            if (currentCommitHorizon > lastIndexCommitHorizon) {
                lastIndexCommitHorizon = currentCommitHorizon;
                try {
                    commitHorizonCallback.onIndexCommitHorizonUpdate(currentCommitHorizon);
                    maybeCleanupExpiredRecords();
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
     * @param walRecords the WAL LogRecords generated during KV processing
     * @param appendInfo the log append information containing offset details
     * @throws Exception if an error occurs during hot data processing
     */
    public void writeHotDataFromWAL(LogRecords walRecords, LogAppendInfo appendInfo)
            throws Exception {

        if (closed) {
            LOG.warn(
                    "IndexCache is closed, cannot write hot data for table bucket {}",
                    logTablet.getTableBucket());
            return;
        }

        lock.writeLock().lock();
        try {
            indexCacheWriter.writeHotDataFromWAL(walRecords, appendInfo);
            LOG.debug(
                    "Successfully processed hot data from WAL for table bucket {}",
                    logTablet.getTableBucket());

        } catch (Exception e) {
            LOG.warn(
                    "Failed to process hot data from WAL for table bucket {}",
                    logTablet.getTableBucket(),
                    e);
            throw e;
        } finally {
            lock.writeLock().unlock();
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

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing IndexCache for data bucket {}", logTablet.getTableBucket());

        lock.writeLock().lock();
        try {
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
        } finally {
            lock.writeLock().unlock();
        }

        LOG.info("IndexCache closed for data bucket {}", logTablet.getTableBucket());
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

        // Calculate global WAL read range
        long globalStartOffset =
                unloadedRanges.values().stream()
                        .filter(ranges -> !ranges.isEmpty())
                        .map(ranges -> ranges.get(0))
                        .map(OffsetRangeInfo::getStartOffset)
                        .min(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected non-empty ranges but found none"));

        long globalEndOffset =
                unloadedRanges.values().stream()
                        .filter(ranges -> !ranges.isEmpty())
                        .map(ranges -> ranges.get(0))
                        .map(OffsetRangeInfo::getEndOffset)
                        .min(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected non-empty ranges but found none"));

        indexCacheWriter.loadColdDataToCache(globalStartOffset, globalEndOffset);
    }

    /** Get schema ID for the specified index table. */
    private short getSchemaIdForIndex(long indexTableId) {
        try {
            // Get all index tables for the data table
            List<TableInfo> indexTables =
                    tabletServerMetadataCache.getRelatedIndexTables(
                            dataTablePhysicalPath.getTablePath());

            // Find the index table with the matching ID
            for (TableInfo indexTable : indexTables) {
                if (indexTable.getTableId() == indexTableId) {
                    return (short) indexTable.getSchemaId();
                }
            }

            LOG.warn(
                    "Index table metadata not found for table ID: {}, using default schema ID",
                    indexTableId);
            return 1;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to get schema ID for index table ID: {}, using default schema ID",
                    indexTableId,
                    e);
            return 1;
        }
    }

    private void maybeCleanupExpiredRecords() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }

            indexRowCache.cleanupBelowHorizon(lastIndexCommitHorizon);
        } catch (IOException e) {
            LOG.error("Error cleaning up expired records", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ================================================================================================
    // Data Structures (Maintaining Interface Compatibility)
    // ================================================================================================

    /** IndexCache-specific fetch request structure. */
    public static final class IndexCacheFetchParam {
        private final long indexTableId;
        private final long fetchOffset;
        private final long indexCommitOffset;

        public IndexCacheFetchParam(long indexTableId, long fetchOffset, long indexCommitOffset) {
            this.indexTableId = indexTableId;
            this.fetchOffset = fetchOffset;
            this.indexCommitOffset = indexCommitOffset;
        }

        public long getIndexTableId() {
            return indexTableId;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public long getIndexCommitOffset() {
            return indexCommitOffset;
        }
    }
}
