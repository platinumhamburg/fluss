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
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    /** Estimated average size of a single WAL record in bytes, used for batch size calculation. */
    private static final long ESTIMATED_RECORD_SIZE_BYTES = 1024;

    /** Callback interface for index commit horizon changes. */
    public interface IndexCommitHorizonCallback {
        void onIndexCommitHorizonUpdate(long newHorizon);
    }

    private final LogTablet logTablet;

    private final IndexDataExtractor dataExtractor;
    private final PhysicalTablePath dataTablePhysicalPath;
    private final IndexCommitHorizonCallback commitHorizonCallback;

    private final IndexBucketCacheManager cacheManager;

    private final AtomicLong lastIndexCommitHorizon = new AtomicLong(-1);

    private volatile boolean closed = false;

    private final IndexWriteTaskQueue taskQueue;
    private final IndexWriteTaskScheduler taskScheduler;

    // For cold data loading on failover - batch loading support
    private final long logEndOffsetOnLeaderStart;
    private final long coldLoadBatchSize;

    // Tracks the state of batch cold data loading atomically
    private final AtomicReference<ColdLoadState> coldLoadState =
            new AtomicReference<>(ColdLoadState.IDLE);

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
        this.taskQueue = new IndexWriteTaskQueue(logTablet.getTableBucket().toString(), 8192);
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
            Map<TableBucket, IndexBucketFetchState> fetchRequests,
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
                        .mapToLong(IndexBucketFetchState::getFetchOffset)
                        .min()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No fetch requests provided for data bucket "
                                                        + logTablet.getTableBucket()));

        if (!forceFetch) {
            if (highWatermark - minFetchStartOffset < minAdvanceOffset) {
                // Not enough data to justify a batched fetch. However, if there IS data
                // available (HW > minFetchStartOffset), we must still fetch it to avoid
                // blocking index replication indefinitely. The minAdvanceOffset optimization
                // only applies when there is truly no new data.
                if (highWatermark <= minFetchStartOffset) {
                    return Tuple2.of(0, Optional.empty());
                }
                // Fall through to fetch with HW as the end offset
            }
        }

        long currentEndOffset = minFetchStartOffset;
        int currentFetchBytes = 0;

        while (currentFetchBytes < maxBytes && currentEndOffset <= highWatermark) {
            currentEndOffset += minAdvanceOffset;
            currentFetchBytes = 0;
            for (Map.Entry<TableBucket, IndexBucketFetchState> entry : fetchRequests.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                currentFetchBytes +=
                        cacheManager.getCachedBytesOfIndexBucketInRange(
                                indexBucket, minFetchStartOffset, currentEndOffset);
            }
        }

        currentEndOffset = Math.min(currentEndOffset, highWatermark);

        for (Map.Entry<TableBucket, IndexBucketFetchState> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            IndexBucketFetchState param = entry.getValue();

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

    private void updateCommitOffset(Map<TableBucket, IndexBucketFetchState> fetchRequests) {
        long maxCommitOffset = -1;
        for (Map.Entry<TableBucket, IndexBucketFetchState> entry : fetchRequests.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            IndexBucketFetchState fetchParam = entry.getValue();
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

    /**
     * Checks if the commit horizon has advanced and triggers callbacks + cold data loading.
     *
     * <p>This is the ONLY entry point for {@link #mayTriggerColdDataLoading}. It uses two gates to
     * ensure cold loading is only triggered when commitHorizon genuinely advances:
     *
     * <ul>
     *   <li>Gate α: maxCommitOffset > lastIndexCommitHorizon — quick check using the max
     *       commitOffset from the current fetch request batch
     *   <li>Gate β: currentCommitHorizon > lastIndexCommitHorizon — authoritative check using the
     *       global minimum across ALL index buckets (getCommitHorizon())
     * </ul>
     *
     * <p><b>One-shot behavior:</b> Once lastIndexCommitHorizon is set to currentCommitHorizon, Gate
     * α will block all subsequent calls until commitHorizon advances again. If commitHorizon cannot
     * advance (e.g., because a cache gap prevents fetcher progress), cold data loading will never
     * be re-triggered through this path.
     *
     * @param maxCommitOffset the maximum commitOffset from the current fetch request batch
     */
    private void mayTriggerCommitHorizonCallback(long maxCommitOffset) {
        // Gate α: Quick check — has any index bucket reported a higher offset than we've seen?
        long currentLastHorizon = lastIndexCommitHorizon.get();
        if (maxCommitOffset > currentLastHorizon) {
            // Gate β: Authoritative check — has the global minimum (commitHorizon) actually
            // advanced?
            long currentCommitHorizon = cacheManager.getCommitHorizon();
            if (currentCommitHorizon > currentLastHorizon) {
                // Atomically update only if no other thread has advanced it further
                if (!lastIndexCommitHorizon.compareAndSet(
                        currentLastHorizon, currentCommitHorizon)) {
                    return; // Another thread already advanced it
                }
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

    /**
     * Attempts to trigger cold data loading when the commit horizon advances.
     *
     * <p>Cold data loading fills the cache gap between commitHorizon and logEndOffsetOnLeaderStart.
     * This gap exists because data written to WAL before IndexDataProducer was set on KvTablet is
     * not captured by hot data caching.
     *
     * <p>Gate conditions (each returns early if true):
     *
     * <ul>
     *   <li>Gate A: commitHorizon not yet initialized (< 0)
     *   <li>Gate B: No cold data boundary (logEndOffsetOnLeaderStart <= 0)
     *   <li>Gate C: All cold data consumed (commitHorizon >= logEndOffsetOnLeaderStart)
     *   <li>Gate D: A cold load batch is already in progress
     *   <li>Gate E: Cache already covers commitHorizon (minCacheWatermark <= commitHorizon)
     *   <li>Gate F: Previous batch not yet consumed by fetchers
     *   <li>Gate G: Adjusted start offset >= batch end (no valid range to load)
     * </ul>
     *
     * <p><b>Important:</b> This method is currently only called from {@link
     * #mayTriggerCommitHorizonCallback}, which requires commitHorizon to advance (one-shot
     * trigger). If commitHorizon stops advancing (e.g., due to a cache gap that prevents fetcher
     * progress), this method will not be called again.
     */
    private void mayTriggerColdDataLoading(long currentCommitHorizon) {
        // Gate A: commitHorizon not yet initialized
        if (currentCommitHorizon < 0) {
            return;
        }

        // Gate B: No cold data boundary — leader started with empty WAL
        if (logEndOffsetOnLeaderStart <= 0) {
            return;
        }

        // Gate C: All cold data consumed — commitHorizon has caught up to or passed the
        // boundary between cold data (pre-leader-start) and hot data (post-leader-start)
        if (currentCommitHorizon >= logEndOffsetOnLeaderStart) {
            ColdLoadState current = coldLoadState.get();
            if (current.targetEndOffset > 0) {
                LOG.info(
                        "DataBucket {}: All cold data has been consumed, resetting cold load state",
                        logTablet.getTableBucket());
                coldLoadState.set(ColdLoadState.IDLE);
            }
            return;
        }

        // Snapshot the current state for all subsequent checks
        ColdLoadState current = coldLoadState.get();

        // Gate D: A cold load batch is already in progress — avoid concurrent loading
        if (current.inProgress) {
            return;
        }

        // Gate E: Cache already covers commitHorizon — no gap between cache and commitHorizon,
        // so fetchers can read from cache without cold loading
        long minCacheWatermark = cacheManager.getMinCacheWatermark();

        if (minCacheWatermark >= 0 && minCacheWatermark <= currentCommitHorizon) {
            return;
        }

        // Gate F: Previous cold load batch not yet consumed — fetchers haven't caught up to
        // the end of the last batch, so loading more would waste memory
        if (current.currentEndOffset > 0 && currentCommitHorizon < current.currentEndOffset) {
            return;
        }

        long targetEndOffset = current.targetEndOffset;
        if (targetEndOffset < 0) {
            if (minCacheWatermark < 0) {
                targetEndOffset = logEndOffsetOnLeaderStart;
            } else {
                targetEndOffset = Math.min(logEndOffsetOnLeaderStart, minCacheWatermark);
            }
        }

        long estimatedRecordsInBatch = coldLoadBatchSize / ESTIMATED_RECORD_SIZE_BYTES;
        long batchEndOffset =
                Math.min(currentCommitHorizon + estimatedRecordsInBatch, targetEndOffset);

        // Gate G: No valid range to load — WAL start offset has advanced past the batch end,
        // meaning the WAL segments for this range have been cleaned
        long logStartOffset = logTablet.logStartOffset();
        long adjustedStartOffset = Math.max(currentCommitHorizon, logStartOffset);
        if (adjustedStartOffset >= batchEndOffset) {
            return;
        }

        // Atomically transition from current (not-in-progress) to in-progress state.
        // This prevents race condition where multiple threads could pass the check.
        ColdLoadState next = new ColdLoadState(targetEndOffset, batchEndOffset, true);
        if (!coldLoadState.compareAndSet(current, next)) {
            // Another thread already modified the state
            return;
        }

        LOG.info(
                "DataBucket {}: Triggering batch cold data loading from WAL, batch range [{}, {}), "
                        + "target end offset: {}, batch size limit: {} bytes",
                logTablet.getTableBucket(),
                adjustedStartOffset,
                batchEndOffset,
                targetEndOffset,
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
                        // Use CAS loop to atomically transition from in-progress to idle
                        ColdLoadState prev;
                        ColdLoadState next;
                        do {
                            prev = coldLoadState.get();
                            next =
                                    new ColdLoadState(
                                            prev.targetEndOffset, prev.currentEndOffset, false);
                        } while (!coldLoadState.compareAndSet(prev, next));
                        // Re-evaluate cold data loading after batch completion.
                        // Without this, a race condition can cause permanent stall:
                        // if commitHorizon advanced while this batch was in-flight,
                        // the advancement signal was "consumed" by Gate A
                        // (lastIndexCommitHorizon was updated) but Gate B
                        // (coldLoadInProgress=true) blocked the next batch trigger.
                        // After this batch completes, no future fetchIndex() call
                        // will re-enter mayTriggerColdDataLoading because Gate A
                        // is permanently closed.
                        mayTriggerColdDataLoading(lastIndexCommitHorizon.get());
                    }

                    @Override
                    public void onFailure(long startOffset, long endOffset, Exception cause) {
                        LOG.error(
                                "DataBucket {}: Cold data loading batch failed for range [{}, {})",
                                logTablet.getTableBucket(),
                                startOffset,
                                endOffset,
                                cause);
                        // Use CAS loop to atomically reset the in-progress flag
                        ColdLoadState prev;
                        ColdLoadState next;
                        do {
                            prev = coldLoadState.get();
                            if (cause instanceof LogOffsetOutOfRangeException) {
                                // WAL segments were cleaned before we could read them.
                                // Reset cold load state so next commitHorizon advance can
                                // recalculate with current WAL bounds.
                                next = ColdLoadState.IDLE;
                            } else {
                                next =
                                        new ColdLoadState(
                                                prev.targetEndOffset, prev.currentEndOffset, false);
                            }
                        } while (!coldLoadState.compareAndSet(prev, next));
                        // Re-evaluate after failure as well, for the same reason
                        // as onComplete. The guards inside mayTriggerColdDataLoading
                        // will handle all edge cases safely.
                        mayTriggerColdDataLoading(lastIndexCommitHorizon.get());
                    }
                };

        ColdDataLoadTask coldDataLoadTask =
                new ColdDataLoadTask(dataExtractor, adjustedStartOffset, batchEndOffset, callback);
        boolean submitted = taskQueue.submit(coldDataLoadTask);

        if (!submitted) {
            LOG.error(
                    "DataBucket {}: Failed to submit cold data loading task for batch range [{}, {}), queue may be closed",
                    logTablet.getTableBucket(),
                    adjustedStartOffset,
                    batchEndOffset);
            ColdLoadState prevState, nextState;
            do {
                prevState = coldLoadState.get();
                nextState =
                        new ColdLoadState(
                                prevState.targetEndOffset, prevState.currentEndOffset, false);
            } while (!coldLoadState.compareAndSet(prevState, nextState));
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
        if (closed) {
            return -1;
        }
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
        ColdLoadState cls = coldLoadState.get();
        sb.append("    InProgress: ").append(cls.inProgress).append("\n");
        sb.append("    CurrentEndOffset: ").append(cls.currentEndOffset).append("\n");
        sb.append("    TargetEndOffset: ").append(cls.targetEndOffset).append("\n");

        if (requestedStartOffset >= cls.currentEndOffset
                && requestedStartOffset < logEndOffsetOnLeaderStart) {
            sb.append("    [WARNING] Requested data [")
                    .append(requestedStartOffset)
                    .append(", ")
                    .append(requestedEndOffset)
                    .append(") is beyond coldLoadCurrentEndOffset ")
                    .append(cls.currentEndOffset)
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
        // Set closed FIRST to prevent concurrent fetchIndex/cacheIndexDataByHotData
        // from operating on resources that are being closed.
        closed = true;

        LOG.info("Closing IndexDataProducer for data bucket {}", logTablet.getTableBucket());

        if (taskQueue != null && taskScheduler != null) {
            taskScheduler.unregisterQueue(logTablet.getTableBucket());
            taskQueue.close();
        }

        cacheManager.close();

        try {
            dataExtractor.close();
        } catch (IOException e) {
            LOG.warn("Error closing IndexDataExtractor", e);
        }

        LOG.info("IndexDataProducer closed for data bucket {}", logTablet.getTableBucket());
    }

    private List<TableInfo> getIndexTablesWithRetry(
            TabletServerMetadataCache metadataCache, PhysicalTablePath dataTablePhysicalPath)
            throws Exception {

        final int maxRetries = 30;
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

    /** Immutable snapshot of cold data loading state, swapped atomically via AtomicReference. */
    private static final class ColdLoadState {
        static final ColdLoadState IDLE = new ColdLoadState(-1, -1, false);

        /** The final target offset for all cold data loading (cold/hot boundary). */
        final long targetEndOffset;
        /** The end offset of the current in-flight batch. */
        final long currentEndOffset;
        /** Whether a cold load batch is currently being executed. */
        final boolean inProgress;

        ColdLoadState(long targetEndOffset, long currentEndOffset, boolean inProgress) {
            this.targetEndOffset = targetEndOffset;
            this.currentEndOffset = currentEndOffset;
            this.inProgress = inProgress;
        }
    }

    /** IndexDataProducer-specific fetch request structure. */
    public static final class IndexBucketFetchState {
        private final long fetchOffset;
        private final long indexCommitOffset;
        private final int leaderServerId;

        public IndexBucketFetchState(long fetchOffset, long indexCommitOffset, int leaderServerId) {
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
