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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecords;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * IndexDataProducer is a stateful component held by each Leader Replica to manage index data
 * production for all index tables using a row-level storage + dynamic assembly architecture.
 *
 * <p>This class serves as the "producer" in the index replication producer-consumer model, where:
 *
 * <ul>
 *   <li>Producer (this class): Extracts index data from data table WAL and caches it
 *   <li>Consumer (IndexApplier): Applies index data to index table KV storage
 * </ul>
 *
 * <p>Core Features:
 *
 * <ul>
 *   <li>Row-level storage: Caches IndexedRow data directly instead of pre-assembled LogRecords
 *   <li>Dynamic assembly: Constructs LogRecords on-demand based on fetch parameters
 *   <li>Hot data writing: Supports real-time IndexedRow writing for immediate caching
 *   <li>Cold data loading: Automatically loads missing data from WAL using IndexDataExtractor
 *   <li>Precise data cutting: Avoids unnecessary data transmission with exact boundary handling
 *   <li>Zero-copy operations: Uses MultiBytesView for efficient memory management
 * </ul>
 *
 * <p>Thread Safety: This class provides concurrent read access with exclusive write operations
 * using ReadWriteLock. Critical state updates use atomic operations for consistency.
 */
@Internal
public final class IndexDataProducer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDataProducer.class);

    /** Callback interface for index commit horizon changes. */
    public interface IndexCommitHorizonCallback {
        void onIndexCommitHorizonUpdate(long newHorizon);
    }

    private final LogTablet logTablet;

    private final IndexDataExtractor dataExtractor;
    private final PhysicalTablePath dataTablePhysicalPath;
    private final IndexCommitHorizonCallback commitHorizonCallback;

    private final IndexBucketCacheManager cacheManager;

    private volatile long lastIndexCommitHorizon = -1;

    private volatile boolean closed = false;

    private final IndexWriteTaskQueue taskQueue;
    private final IndexWriteTaskScheduler taskScheduler;

    // For cold data loading on failover - batch loading support
    private final long logEndOffsetOnLeaderStart;
    private final long coldLoadBatchSize;

    // Tracks the state of batch cold data loading
    private volatile long coldLoadTargetEndOffset = -1;
    private volatile long coldLoadCurrentEndOffset = -1;
    private final AtomicBoolean coldLoadInProgress = new AtomicBoolean(false);

    public IndexDataProducer(
            LogTablet logTablet,
            MemorySegmentPool memoryPool,
            SchemaGetter schemaGetter,
            PhysicalTablePath dataTablePhysicalPath,
            TabletServerMetadataCache metadataCache,
            IndexCommitHorizonCallback commitHorizonCallback,
            long dataBucketLogEndOffset,
            IndexWriteTaskScheduler taskScheduler,
            Configuration conf) {
        this.logTablet = checkNotNull(logTablet, "logTablet cannot be null");
        checkNotNull(memoryPool, "memoryPool cannot be null");
        checkNotNull(schemaGetter, "schemaGetter cannot be null");
        checkNotNull(metadataCache, "metadataCache cannot be null");
        this.dataTablePhysicalPath =
                checkNotNull(dataTablePhysicalPath, "dataTablePhysicalPath cannot be null");
        this.commitHorizonCallback = commitHorizonCallback;

        Schema dataTableSchema = schemaGetter.getLatestSchemaInfo().getSchema();

        BucketingFunction bucketingFunction = BucketingFunction.of(null);

        List<TableInfo> indexTableInfos;
        try {
            indexTableInfos = getIndexTablesWithRetry(metadataCache, dataTablePhysicalPath);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize IndexDataProducer: " + e.getMessage(), e);
        }

        Map<Long, Integer> indexTableBucketDistribution =
                indexTableInfos.stream()
                        .collect(
                                HashMap::new,
                                (m, t) -> m.put(t.getTableId(), t.getNumBuckets()),
                                HashMap::putAll);
        this.cacheManager =
                new IndexBucketCacheManager(memoryPool, logTablet, indexTableBucketDistribution);

        TableInfo mainTableInfo =
                metadataCache
                        .getTableMetadata(dataTablePhysicalPath.getTablePath())
                        .map(org.apache.fluss.server.metadata.TableMetadata::getTableInfo)
                        .orElse(null);

        this.dataExtractor =
                new IndexDataExtractor(
                        logTablet,
                        cacheManager,
                        bucketingFunction,
                        schemaGetter,
                        indexTableInfos,
                        mainTableInfo);

        this.logEndOffsetOnLeaderStart = dataBucketLogEndOffset;

        MemorySize batchSize = conf.get(ConfigOptions.SERVER_INDEX_CACHE_COLD_LOAD_BATCH_SIZE);
        this.coldLoadBatchSize = batchSize.getBytes();

        this.taskScheduler = taskScheduler;
        this.taskQueue = new IndexWriteTaskQueue(logTablet.getTableBucket().toString(), 9192);
        this.taskScheduler.registerQueue(logTablet.getTableBucket(), taskQueue);

        LOG.info(
                "IndexDataProducer initialized for data bucket {} with {} index definitions, "
                        + "logEndOffsetOnLeaderStart: {}, coldLoadBatchSize: {} bytes",
                logTablet.getTableBucket(),
                dataTableSchema.getIndexes().size(),
                logEndOffsetOnLeaderStart,
                coldLoadBatchSize);
    }

    public Tuple2<Integer, Optional<Map<TableBucket, IndexSegment>>> fetchIndex(
            Map<TableBucket, FetchIndexParams> fetchRequests,
            long minAdvanceOffset,
            int maxBytes,
            boolean forceFetch)
            throws Exception {
        if (closed) {
            LOG.warn("IndexDataProducer is closed, returning empty results");
            return Tuple2.of(0, Optional.empty());
        }

        long highWatermark = logTablet.getHighWatermark();

        Map<TableBucket, IndexSegment> results = new HashMap<>();

        updateCommitOffset(fetchRequests);

        long minFetchStartOffset =
                fetchRequests.values().stream()
                        .mapToLong(FetchIndexParams::getFetchOffset)
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
            for (Map.Entry<TableBucket, FetchIndexParams> entry : fetchRequests.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                currentFetchBytes +=
                        cacheManager.getCachedBytesOfIndexBucketInRange(
                                indexBucket, minFetchStartOffset, currentEndOffset);
            }
        }

        currentEndOffset = Math.min(currentEndOffset, highWatermark);

        for (Map.Entry<TableBucket, FetchIndexParams> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            FetchIndexParams param = entry.getValue();

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

            IndexBucketCacheManager.ReadResult readResult =
                    cacheManager.readIndexLogRecords(
                            indexBucket.getTableId(),
                            indexBucket.getBucket(),
                            startOffset,
                            currentEndOffset);

            IndexSegment segment;
            if (readResult.getStatus() == IndexBucketCacheManager.ReadStatus.NOT_LOADED) {
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

    private void updateCommitOffset(Map<TableBucket, FetchIndexParams> fetchRequests) {
        long maxCommitOffset = -1;
        for (Map.Entry<TableBucket, FetchIndexParams> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            FetchIndexParams fetchParam = entry.getValue();
            long commitOffset = fetchParam.getIndexCommitOffset();
            long fetchOffset = fetchParam.getFetchOffset();
            int leaderServerId = fetchParam.getLeaderServerId();

            cacheManager.updateCommitOffset(
                    indexBucket.getTableId(),
                    indexBucket.getBucket(),
                    commitOffset,
                    fetchOffset,
                    leaderServerId);

            maxCommitOffset = Math.max(maxCommitOffset, commitOffset);
        }
        mayTriggerCommitHorizonCallback(maxCommitOffset);
    }

    private void mayTriggerCommitHorizonCallback(long maxCommitOffset) {
        if (maxCommitOffset > lastIndexCommitHorizon) {
            long currentCommitHorizon = cacheManager.getCommitHorizon();
            if (currentCommitHorizon > lastIndexCommitHorizon) {
                lastIndexCommitHorizon = currentCommitHorizon;
                LOG.debug(
                        "DataBucket {}({}): index commit horizon advanced to {}",
                        this.logTablet.getTableBucket(),
                        dataTablePhysicalPath,
                        currentCommitHorizon);
                if (commitHorizonCallback != null) {
                    try {
                        commitHorizonCallback.onIndexCommitHorizonUpdate(currentCommitHorizon);
                    } catch (Exception e) {
                        LOG.error("Failed to trigger commit horizon callback", e);
                    }
                }

                mayTriggerColdDataLoading(currentCommitHorizon);
            }
        }
    }

    private void mayTriggerColdDataLoading(long currentCommitHorizon) {
        if (logEndOffsetOnLeaderStart <= 0) {
            return;
        }

        if (currentCommitHorizon >= logEndOffsetOnLeaderStart) {
            if (coldLoadTargetEndOffset > 0) {
                LOG.info(
                        "DataBucket {}: All cold data has been consumed, resetting cold load state",
                        logTablet.getTableBucket());
                coldLoadTargetEndOffset = -1;
                coldLoadCurrentEndOffset = -1;
                coldLoadInProgress.set(false);
            }
            return;
        }

        if (coldLoadInProgress.get()) {
            return;
        }

        long minCacheWatermark = cacheManager.getMinCacheWatermark();

        if (minCacheWatermark >= 0 && minCacheWatermark <= currentCommitHorizon) {
            return;
        }

        if (coldLoadCurrentEndOffset > 0 && currentCommitHorizon < coldLoadCurrentEndOffset) {
            return;
        }

        if (coldLoadTargetEndOffset < 0) {
            if (minCacheWatermark < 0) {
                coldLoadTargetEndOffset = logEndOffsetOnLeaderStart;
            } else {
                coldLoadTargetEndOffset = Math.min(logEndOffsetOnLeaderStart, minCacheWatermark);
            }
        }

        long estimatedRecordsInBatch = coldLoadBatchSize / 1024;
        long batchEndOffset =
                Math.min(currentCommitHorizon + estimatedRecordsInBatch, coldLoadTargetEndOffset);

        if (currentCommitHorizon >= batchEndOffset) {
            return;
        }

        // Use compareAndSet to atomically check and set coldLoadInProgress
        // This prevents race condition where multiple threads could pass the check
        if (!coldLoadInProgress.compareAndSet(false, true)) {
            // Another thread already started cold data loading
            return;
        }

        coldLoadCurrentEndOffset = batchEndOffset;

        LOG.info(
                "DataBucket {}: Triggering batch cold data loading from WAL, batch range [{}, {}), "
                        + "target end offset: {}, batch size limit: {} bytes",
                logTablet.getTableBucket(),
                currentCommitHorizon,
                batchEndOffset,
                coldLoadTargetEndOffset,
                coldLoadBatchSize);

        ColdDataLoadTask.CompletionCallback callback =
                new ColdDataLoadTask.CompletionCallback() {
                    @Override
                    public void onComplete(long startOffset, long endOffset) {
                        LOG.info(
                                "DataBucket {}: Cold data loading batch completed successfully, range [{}, {})",
                                logTablet.getTableBucket(),
                                startOffset,
                                endOffset);
                        coldLoadInProgress.set(false);
                    }

                    @Override
                    public void onFailure(long startOffset, long endOffset, Exception cause) {
                        LOG.error(
                                "DataBucket {}: Cold data loading batch failed for range [{}, {})",
                                logTablet.getTableBucket(),
                                startOffset,
                                endOffset,
                                cause);
                        coldLoadInProgress.set(false);
                    }
                };

        ColdDataLoadTask coldDataLoadTask =
                new ColdDataLoadTask(dataExtractor, currentCommitHorizon, batchEndOffset, callback);
        boolean submitted = taskQueue.submit(coldDataLoadTask);

        if (!submitted) {
            LOG.error(
                    "DataBucket {}: Failed to submit cold data loading task for batch range [{}, {}), queue may be closed",
                    logTablet.getTableBucket(),
                    currentCommitHorizon,
                    batchEndOffset);
            coldLoadInProgress.set(false);
        }
    }

    public void cacheIndexDataByHotData(LogRecords walRecords, LogAppendInfo appendInfo) {

        if (closed) {
            LOG.warn(
                    "IndexDataProducer is closed, cannot write hot data for table bucket {}",
                    logTablet.getTableBucket());
            return;
        }

        HotDataWriteTask hotDataWriteTask =
                new HotDataWriteTask(dataExtractor, walRecords, appendInfo);
        boolean submitted = taskQueue.submit(hotDataWriteTask);

        if (!submitted) {
            LOG.error(
                    "Failed to submit hot data write task for table bucket {}, range [{}, {}), queue may be closed",
                    logTablet.getTableBucket(),
                    appendInfo.firstOffset(),
                    appendInfo.lastOffset() + 1);
        }
    }

    public long getIndexCommitHorizon() {
        return cacheManager.getCommitHorizon();
    }

    public IndexBucketCacheManager getCacheManager() {
        return cacheManager;
    }

    public String getIndexBucketDiagnosticInfo(
            TableBucket indexBucket, long requestedStartOffset, long requestedEndOffset) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n  --- Diagnostic for IndexBucket ").append(indexBucket).append(" ---\n");

        sb.append("  DataBucket: ").append(logTablet.getTableBucket()).append("\n");
        sb.append("  RequestedRange: [")
                .append(requestedStartOffset)
                .append(", ")
                .append(requestedEndOffset)
                .append(")\n");

        sb.append("  LogEndOffset: ").append(logTablet.localLogEndOffset()).append("\n");
        sb.append("  HighWatermark: ").append(logTablet.getHighWatermark()).append("\n");
        sb.append("  LogEndOffsetOnLeaderStart: ").append(logEndOffsetOnLeaderStart).append("\n");

        long commitOffset =
                cacheManager.getCommitOffset(indexBucket.getTableId(), indexBucket.getBucket());
        sb.append("  IndexBucketCommitOffset: ").append(commitOffset).append("\n");
        sb.append("  GlobalIndexCommitHorizon: ")
                .append(cacheManager.getCommitHorizon())
                .append("\n");

        String bucketRangeInfo =
                cacheManager.getIndexBucketRangeInfo(
                        indexBucket.getTableId(), indexBucket.getBucket());
        sb.append("  CachedRanges:\n");
        if (bucketRangeInfo.isEmpty()) {
            sb.append("    [EMPTY - No cached ranges for this bucket]\n");
        } else {
            sb.append(bucketRangeInfo);
        }

        sb.append("  GapAnalysis:\n");
        if (requestedStartOffset < logEndOffsetOnLeaderStart) {
            sb.append(
                    "    RequestedOffset < LogEndOffsetOnLeaderStart: YES (should be loadable from WAL)\n");
        } else {
            sb.append(
                    "    RequestedOffset >= LogEndOffsetOnLeaderStart: YES (hot data, check TaskQueue)\n");
        }

        sb.append("  ColdLoadState:\n");
        sb.append("    InProgress: ").append(coldLoadInProgress.get()).append("\n");
        sb.append("    CurrentEndOffset: ").append(coldLoadCurrentEndOffset).append("\n");
        sb.append("    TargetEndOffset: ").append(coldLoadTargetEndOffset).append("\n");

        if (requestedStartOffset >= coldLoadCurrentEndOffset
                && requestedStartOffset < logEndOffsetOnLeaderStart) {
            sb.append("    ⚠️  Requested data [")
                    .append(requestedStartOffset)
                    .append(", ")
                    .append(requestedEndOffset)
                    .append(") is beyond coldLoadCurrentEndOffset ")
                    .append(coldLoadCurrentEndOffset)
                    .append(", may need next batch\n");
        }

        int queueSize = taskQueue.getQueueSize();
        sb.append("  TaskQueue: queueSize=").append(queueSize);
        if (queueSize > 0) {
            sb.append(", may contain tasks for this range\n");
        } else {
            sb.append(", no pending tasks\n");
        }

        return sb.toString();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        LOG.info("Closing IndexDataProducer for data bucket {}", logTablet.getTableBucket());

        closed = true;

        if (taskQueue != null && taskScheduler != null) {
            taskScheduler.unregisterQueue(logTablet.getTableBucket());
            taskQueue.close();
        }

        if (cacheManager != null) {
            cacheManager.close();
        }

        if (dataExtractor != null) {
            try {
                dataExtractor.close();
            } catch (IOException e) {
                LOG.warn("Error closing IndexDataExtractor", e);
            }
        }

        LOG.info("IndexDataProducer closed for data bucket {}", logTablet.getTableBucket());
    }

    private List<TableInfo> getIndexTablesWithRetry(
            TabletServerMetadataCache metadataCache, PhysicalTablePath dataTablePhysicalPath)
            throws Exception {

        final int maxRetries = 300;
        final long retryIntervalMs = 1000;

        Exception lastException = null;
        TablePath dataTablePath = dataTablePhysicalPath.getTablePath();

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                List<TableInfo> indexTableInfos =
                        metadataCache.getRelatedIndexTables(dataTablePath);

                if (attempt > 0) {
                    LOG.info(
                            "Successfully retrieved index table information for data bucket {} after {} attempts",
                            logTablet.getTableBucket(),
                            attempt + 1);
                }

                return indexTableInfos;

            } catch (Exception e) {
                lastException = e;

                if (e instanceof TableNotExistException) {
                    LOG.error(
                            "Table does not exist (likely deleted during leader election). "
                                    + "Failing immediately to avoid deadlock. Data bucket: {}, Error: {}",
                            logTablet.getTableBucket(),
                            e.getMessage());
                    throw new Exception(
                            "Table does not exist for data bucket "
                                    + logTablet.getTableBucket()
                                    + ". Cannot initialize IndexDataProducer.",
                            e);
                }

                if (attempt == maxRetries - 1) {
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

        String errorMsg =
                String.format(
                        "Failed to retrieve index table information for data bucket %s after %d attempts. "
                                + "Index tables may not have been created yet or there is a configuration issue.",
                        logTablet.getTableBucket(), maxRetries);

        LOG.error(errorMsg, lastException);
        throw new Exception(errorMsg, lastException);
    }

    /** IndexDataProducer-specific fetch request structure. */
    public static final class FetchIndexParams {
        private final long fetchOffset;
        private final long indexCommitOffset;
        private final int leaderServerId;

        public FetchIndexParams(long fetchOffset, long indexCommitOffset, int leaderServerId) {
            this.fetchOffset = fetchOffset;
            this.indexCommitOffset = indexCommitOffset;
            this.leaderServerId = leaderServerId;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public long getIndexCommitOffset() {
            return indexCommitOffset;
        }

        public int getLeaderServerId() {
            return leaderServerId;
        }
    }
}
