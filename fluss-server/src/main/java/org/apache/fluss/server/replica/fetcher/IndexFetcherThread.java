/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchIndexRequest;
import org.apache.fluss.rpc.messages.PbFetchIndexReqForIndexTableBucket;
import org.apache.fluss.rpc.messages.PbFetchIndexReqForTableBucket;
import org.apache.fluss.server.index.IndexApplier;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;
import org.apache.fluss.utils.log.FairDataIndexTableBucketStatusMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.apache.fluss.config.ConfigOptions.INDEX_REPLICA_FETCH_MAX_BYTES;
import static org.apache.fluss.config.ConfigOptions.INDEX_REPLICA_FETCH_MIN_ADVANCE_OFFSET;
import static org.apache.fluss.config.ConfigOptions.INDEX_REPLICA_FETCH_WAIT_MAX_TIME;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Index fetcher thread to fetch data from leader. */
final class IndexFetcherThread extends ShutdownableThread {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFetcherThread.class);

    private final ReplicaManager replicaManager;
    private final LeaderEndpoint leader;
    private final int fetchBackOffMs;

    private final int maxFetchBytes;
    private final int minAdvanceOffset;
    private final int maxFetchWaitMs;

    /**
     * A fair status map to store index bucket fetch status. {@link DataIndexTableBucket} -> {@link
     * DataBucketIndexFetchStatus}.
     *
     * <p>Using this map instead of concurrent hash map to make sure each data-index bucket have the
     * same chance to be selected for index replication.
     */
    @GuardedBy("indexBucketStatusMapLock")
    private final FairDataIndexTableBucketStatusMap<DataBucketIndexFetchStatus>
            fairIndexBucketStatusMap = new FairDataIndexTableBucketStatusMap<>();

    private final Lock indexBucketStatusMapLock = new ReentrantLock();
    private final Condition indexBucketStatusMapCondition = indexBucketStatusMapLock.newCondition();

    /**
     * Thread-safe map to store failed data-index buckets and their failure reasons. This map is
     * polled by IndexFetcherManager during reconciliation to update goal states. Using a
     * ConcurrentHashMap allows the fetcher thread to record failures without blocking on
     * IndexFetcherManager's lock, avoiding deadlock.
     */
    private final Map<DataIndexTableBucket, String> invalidBuckets = new HashMap<>();

    private final TabletServerMetricGroup serverMetricGroup;

    public IndexFetcherThread(
            String name,
            ReplicaManager replicaManager,
            LeaderEndpoint leader,
            int fetchBackOffMs,
            Configuration config) {
        super(name, false);
        this.replicaManager = replicaManager;
        this.leader = leader;
        this.fetchBackOffMs = fetchBackOffMs;
        this.maxFetchBytes = (int) config.get(INDEX_REPLICA_FETCH_MAX_BYTES).getBytes();
        this.minAdvanceOffset = config.get(INDEX_REPLICA_FETCH_MIN_ADVANCE_OFFSET);
        this.maxFetchWaitMs = (int) config.get(INDEX_REPLICA_FETCH_WAIT_MAX_TIME).toMillis();
        this.serverMetricGroup = replicaManager.getServerMetricGroup();
    }

    public LeaderEndpoint getLeader() {
        return leader;
    }

    public int getBucketCount() {
        return fairIndexBucketStatusMap.size();
    }

    /**
     * Poll and clear failed buckets. This method is called by IndexFetcherManager during goal
     * reconciliation. Returns a snapshot of failed buckets and clears the internal map.
     *
     * @return map of failed data-index buckets to their failure reasons
     */
    public Map<DataIndexTableBucket, String> pollInvalidBuckets() {
        return inLock(
                indexBucketStatusMapLock,
                () -> {
                    if (invalidBuckets.isEmpty()) {
                        return Collections.emptyMap();
                    }
                    Map<DataIndexTableBucket, String> snapshot = new HashMap<>(invalidBuckets);
                    invalidBuckets.clear();
                    return snapshot;
                });
    }

    @Override
    public void doWork() {
        maybeFetch();
    }

    private void maybeFetch() {
        Optional<FetchIndexContext> fetchIndexContextOpt =
                inLock(
                        indexBucketStatusMapLock,
                        () -> {
                            Optional<FetchIndexContext> fetchLogContext = Optional.empty();
                            try {
                                fetchLogContext =
                                        buildFetchIndexContext(
                                                fairIndexBucketStatusMap.bucketStatusMap());
                                if (!fetchLogContext.isPresent()) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "No active index buckets available for fetching, backing off for {} ms",
                                                fetchBackOffMs);
                                    }
                                    indexBucketStatusMapCondition.await(
                                            fetchBackOffMs, TimeUnit.MILLISECONDS);
                                }
                            } catch (InterruptedException e) {
                                LOG.error("Interrupted while awaiting fetch back off ms.", e);
                            }
                            return fetchLogContext;
                        });

        fetchIndexContextOpt.ifPresent(this::processFetchIndexRequest);
    }

    void addIndexBuckets(
            Map<DataIndexTableBucket, IndexInitialFetchStatus> indexInitialFetchStatusMap)
            throws InterruptedException {
        indexBucketStatusMapLock.lockInterruptibly();
        try {
            indexInitialFetchStatusMap.forEach(
                    (dataIndexBucket, indexInitialFetchStatus) -> {
                        DataBucketIndexFetchStatus currentStatus =
                                fairIndexBucketStatusMap.statusValue(dataIndexBucket);
                        DataBucketIndexFetchStatus updatedStatus =
                                indexBucketFetchStatus(
                                        dataIndexBucket, indexInitialFetchStatus, currentStatus);

                        fairIndexBucketStatusMap.updateAndMoveToEnd(dataIndexBucket, updatedStatus);
                    });

            indexBucketStatusMapCondition.signalAll();
        } finally {
            indexBucketStatusMapLock.unlock();
        }
    }

    void removeIndexBuckets(Set<TableBucket> indexBuckets) {
        inLock(
                indexBucketStatusMapLock,
                () ->
                        fairIndexBucketStatusMap.removeIf(
                                dataIndexBucket ->
                                        indexBuckets.contains(dataIndexBucket.getIndexBucket())));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removed fetcher for index buckets {}", indexBuckets);
        }
    }

    public void removeIf(Predicate<DataIndexTableBucket> predicate) {
        inLock(indexBucketStatusMapLock, () -> fairIndexBucketStatusMap.removeIf(predicate));
    }

    /**
     * Internal method to delay index buckets without acquiring lock. Should only be called when
     * indexBucketStatusMapLock is already held.
     */
    private void delayIndexBucketsInternal(Set<DataIndexTableBucket> indexBuckets, long delay) {
        for (DataIndexTableBucket dataIndexBucket : indexBuckets) {
            DataBucketIndexFetchStatus currentIndexFetchStatus =
                    fairIndexBucketStatusMap.statusValue(dataIndexBucket);
            if (currentIndexFetchStatus != null && !currentIndexFetchStatus.isDelayed()) {
                // Updating the index bucket fetch status and moving to end with a new
                // DelayedItem.
                DataBucketIndexFetchStatus updatedIndexFetchStatus =
                        new DataBucketIndexFetchStatus(
                                currentIndexFetchStatus.getDataIndexTableBucket(),
                                currentIndexFetchStatus.indexTablePath(),
                                new DelayedItem(delay),
                                currentIndexFetchStatus.indexApplier());
                fairIndexBucketStatusMap.updateAndMoveToEnd(
                        dataIndexBucket, updatedIndexFetchStatus);
            }
        }
        indexBucketStatusMapCondition.signalAll();
    }

    private void processFetchIndexRequest(FetchIndexContext fetchIndexContext) {
        Set<DataIndexTableBucket> indexBucketsWithError = new HashSet<>();
        LeaderEndpoint.FetchIndexData responseData = null;
        FetchIndexRequest fetchIndexRequest = fetchIndexContext.getFetchIndexRequest();
        long fetchStartTime = System.nanoTime();

        // Record the number of buckets in this fetch request
        int bucketCount = fetchIndexContext.getRequestDataIndexBuckets().size();
        serverMetricGroup.indexFetchBucketsPerRequestHistogram().update(bucketCount);

        serverMetricGroup.indexFetchRequests().inc();
        try {

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Sending fetch index request with {} buckets to leader {}",
                        fetchIndexRequest.getReqForTableBucketsCount(),
                        leader.leaderServerId());
            }
            long startTime = System.currentTimeMillis();
            responseData = leader.fetchIndex(fetchIndexContext).get();
            long fetchLatencyMs = (System.nanoTime() - fetchStartTime) / 1_000_000;
            serverMetricGroup.indexFetchLatencyHistogram().update(fetchLatencyMs);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Received fetch index response from leader {} in {} ms, {} buckets processed",
                        leader.leaderServerId(),
                        System.currentTimeMillis() - startTime,
                        responseData.getFetchIndexLogResultMap().size());
            }
        } catch (Throwable t) {
            if (isRunning()) {
                Throwable e = ExceptionUtils.stripException(t, ExecutionException.class);
                if (e instanceof TimeoutException) {
                    LOG.warn("fetch index timeout from leader {}", leader.leaderServerId());
                } else {
                    LOG.warn(
                            "Error in response for fetch index request from leader {}",
                            leader.leaderServerId(),
                            t);
                }

                Set<DataIndexTableBucket> bucketsToRetry = new HashSet<>();
                for (DataIndexTableBucket bucket : fetchIndexContext.getRequestDataIndexBuckets()) {
                    // Only retry if the bucket still exists in our status map (not deleted)
                    if (fairIndexBucketStatusMap.statusValue(bucket) != null) {
                        bucketsToRetry.add(bucket);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Skipping retry for deleted index bucket: {}", bucket);
                        }
                    }
                }
                if (!bucketsToRetry.isEmpty()) {
                    indexBucketsWithError.addAll(bucketsToRetry);
                }
            }
        }

        if (responseData != null) {
            indexBucketStatusMapLock.lock();
            try {
                // Collect buckets that need to be marked as failed
                // These will be polled by IndexFetcherManager during reconciliation
                Map<DataIndexTableBucket, String> bucketsToInvalidate = new HashMap<>();

                handleFetchIndexLogResponse(
                        responseData.getFetchIndexLogResultMap(),
                        indexBucketsWithError,
                        bucketsToInvalidate);

                // Record failed buckets and remove them from status map
                // This prevents them from being fetched again until IndexFetcherManager
                // recreates the fetcher during reconciliation
                for (Map.Entry<DataIndexTableBucket, String> entry :
                        bucketsToInvalidate.entrySet()) {
                    DataIndexTableBucket bucket = entry.getKey();
                    String reason = entry.getValue();
                    invalidBuckets.put(bucket, reason);
                    // Remove from status map to stop further fetch attempts
                    fairIndexBucketStatusMap.remove(bucket);
                    LOG.info(
                            "Removed failed bucket {} from fetcher thread. Reason: {}. "
                                    + "IndexFetcherManager will recreate fetcher during reconciliation.",
                            bucket,
                            reason);
                }

                // Handle error buckets within the same lock to avoid race condition
                // This ensures delay status is set before next fetch cycle
                if (!indexBucketsWithError.isEmpty()) {
                    handleIndexBucketWithError(indexBucketsWithError);
                }
            } finally {
                // release buffer handle by fetchLogResponse.
                ByteBuf parsedByteBuf = responseData.getFetchIndexResponse().getParsedByteBuf();
                if (parsedByteBuf != null) {
                    parsedByteBuf.release();
                }
                indexBucketStatusMapLock.unlock();
            }
        }
    }

    private void handleFetchIndexLogResponse(
            Map<DataIndexTableBucket, FetchIndexLogResultForBucket> indexResponseData,
            Set<DataIndexTableBucket> indexBucketsWithError,
            Map<DataIndexTableBucket, String> bucketsToInvalidate) {
        indexResponseData.forEach(
                (dataIndexBucket, indexResultData) -> {
                    DataBucketIndexFetchStatus currentIndexFetchStatus =
                            fairIndexBucketStatusMap.statusValue(dataIndexBucket);
                    if (currentIndexFetchStatus == null
                            || !currentIndexFetchStatus.isReadyForFetch()) {
                        return;
                    }
                    switch (indexResultData.getError().error()) {
                        case NONE:
                            handleFetchIndexLogResponseOfSuccessBucket(
                                    dataIndexBucket, indexResultData);
                            break;
                        case LOG_OFFSET_OUT_OF_RANGE_EXCEPTION:
                            serverMetricGroup.indexFetchErrors().inc();
                            LOG.info(
                                    "Index offset out of range for data-index bucket {}, attempting automatic recovery",
                                    dataIndexBucket);
                            handleIndexOutOfRangeError(dataIndexBucket);
                            break;
                        case NOT_LEADER_OR_FOLLOWER:
                            serverMetricGroup.indexFetchErrors().inc();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        "Remote server is not the leader for index replica {}, which indicate "
                                                + "that the replica is being moved. Retrying after {} ms delay.",
                                        dataIndexBucket,
                                        fetchBackOffMs);
                            }
                            // Record bucket failure, will be polled by IndexFetcherManager
                            bucketsToInvalidate.put(
                                    dataIndexBucket,
                                    "NOT_LEADER_OR_FOLLOWER: Data bucket leader changed or is unavailable");
                            break;
                        case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                            LOG.info(
                                    "Index replica {} no longer exists, removing from fetcher",
                                    dataIndexBucket);
                            serverMetricGroup.indexFetchErrors().inc();
                            // Record bucket failure, will be polled by IndexFetcherManager
                            bucketsToInvalidate.put(
                                    dataIndexBucket,
                                    "UNKNOWN_TABLE_OR_BUCKET_EXCEPTION: Data bucket not found on leader");
                            break;
                        case FETCH_INDEX_EARLY_FIRE_EXCEPTION:
                            // This error indicates that either maxBytes limit was reached or
                            // delayed
                            // fetch wasn't required. We should delay to avoid busy loop when the
                            // server has no new index data available.
                            serverMetricGroup.indexFetchEarlyFire().inc();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        "Fetch index early fire for data-index bucket {}, delaying for {} ms",
                                        dataIndexBucket,
                                        fetchBackOffMs);
                            }
                            indexBucketsWithError.add(dataIndexBucket);
                            break;
                        default:
                            LOG.error(
                                    "Error in response for fetching index replica {}, error message is {}",
                                    dataIndexBucket,
                                    indexResultData.getError().message());
                            // Increment error metrics for index fetch failures
                            serverMetricGroup.indexFetchErrors().inc();
                            // Add index bucket to error set for retry
                            indexBucketsWithError.add(dataIndexBucket);
                    }
                });
    }

    private void handleFetchIndexLogResponseOfSuccessBucket(
            DataIndexTableBucket dataIndexBucket, FetchIndexLogResultForBucket indexResultData) {
        try {
            // Get current fetch offset from IndexApplier (Source of Truth)
            TableBucket dataBucket = dataIndexBucket.getDataBucket();
            TableBucket indexBucket = dataIndexBucket.getIndexBucket();
            Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
            IndexApplier indexApplier = indexReplica.getIndexApplier();

            if (indexApplier == null) {
                LOG.error(
                        "Index applier for index bucket {} not found, skip processing inde fetching result.",
                        indexBucket);
                return;
            }

            IndexApplier.IndexApplyStatus currentApplyStatus =
                    indexApplier.getOrInitIndexApplyStatus(dataBucket);

            long currentAppliedEndOffset = currentApplyStatus.getLastApplyRecordsDataEndOffset();

            // Process index fetch result - this involves writing index data to the index table
            long startOffset = indexResultData.getStartOffset();
            long endOffset = indexResultData.getEndOffset();
            boolean isDataReady = indexResultData.isDataReady();

            // Check if data is not ready (not loaded in cache yet)
            if (!isDataReady) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Index data not ready for data bucket {} -> index bucket {} range [{}, {}), will retry after {} ms delay",
                            dataBucket,
                            indexBucket,
                            startOffset,
                            endOffset,
                            fetchBackOffMs);
                }
                delayIndexBucketsInternal(Collections.singleton(dataIndexBucket), fetchBackOffMs);
                return;
            }

            if (startOffset != currentAppliedEndOffset) {
                LOG.warn(
                        "Index fetch offset mismatch for data bucket {} -> index bucket {}: expected start={}, got range [{}, {}), retrying after {} ms delay",
                        dataBucket,
                        indexBucket,
                        currentAppliedEndOffset,
                        startOffset,
                        endOffset,
                        fetchBackOffMs);
                delayIndexBucketsInternal(Collections.singleton(dataIndexBucket), fetchBackOffMs);
                return;
            } else if (startOffset == endOffset) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Received empty index segment for data bucket {} -> index bucket {} at offset {}, delayed for retry after {} ms delay",
                            dataBucket,
                            indexBucket,
                            startOffset,
                            fetchBackOffMs);
                }
                delayIndexBucketsInternal(Collections.singleton(dataIndexBucket), fetchBackOffMs);
                return;
            }

            MemoryLogRecords indexRecords = (MemoryLogRecords) indexResultData.recordsOrEmpty();
            boolean hasIndexData = indexRecords.sizeInBytes() > 0;

            // Apply index records directly through IndexApplier with WAL address space
            // range
            // Even if indexRecords is empty, we need to advance IndexApplier's state
            // to reflect the WAL address space progression
            indexApplier.applyIndexRecords(indexRecords, startOffset, endOffset, dataBucket);

            if (hasIndexData) {
                serverMetricGroup.replicationBytesIn().inc(indexRecords.sizeInBytes());
            }
            fairIndexBucketStatusMap.moveToEnd(dataIndexBucket);
            // Remove this redundant log as it duplicates information already logged above
        } catch (IndexOutOfBoundsException e) {
            LOG.error(
                    "Memory bounds error while processing index data for data-index bucket {}. This indicates corrupted index data or memory segment issues. "
                            + "Delaying for retry after {} ms. Error details: {}",
                    dataIndexBucket,
                    fetchBackOffMs,
                    e.getMessage(),
                    e);
            // Increment error metrics for memory-related index fetch failures
            serverMetricGroup.indexFetchErrors().inc();
            // Use internal method since we're already holding the lock
            delayIndexBucketsInternal(Collections.singleton(dataIndexBucket), fetchBackOffMs);
        } catch (Exception e) {
            LOG.error(
                    "Error while processing index data for data-index bucket {}, delayed for retry after {} ms.",
                    dataIndexBucket,
                    fetchBackOffMs,
                    e);
            // Increment error metrics for index fetch failures
            serverMetricGroup.indexFetchErrors().inc();
            // Use internal method since we're already holding the lock
            delayIndexBucketsInternal(Collections.singleton(dataIndexBucket), fetchBackOffMs);
        }
    }

    private void handleIndexBucketWithError(Set<DataIndexTableBucket> indexBuckets) {
        if (!indexBuckets.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Index fetch failed for {} buckets, retrying after {} ms delay: {}",
                        indexBuckets.size(),
                        fetchBackOffMs,
                        indexBuckets);
            }
            // Use internal method since we're already holding the lock
            delayIndexBucketsInternal(indexBuckets, fetchBackOffMs);
        }
    }

    /**
     * Handle index offset out of range error by resetting the fetch offset to a valid position.
     * This is similar to handleOutOfRangeError but specifically designed for index replication.
     */
    private void handleIndexOutOfRangeError(DataIndexTableBucket dataIndexBucket) {
        try {
            // Get current fetch offset from IndexApplier (Source of Truth) for logging
            TableBucket dataBucket = dataIndexBucket.getDataBucket();
            TableBucket indexBucket = dataIndexBucket.getIndexBucket();
            Replica indexReplica = replicaManager.getReplicaOrException(indexBucket);
            IndexApplier indexApplier = indexReplica.getIndexApplier();

            long currentFetchOffset = 0L;
            if (indexApplier != null) {
                IndexApplier.IndexApplyStatus currentApplyStatus =
                        indexApplier.getOrInitIndexApplyStatus(dataBucket);
                currentFetchOffset = currentApplyStatus.getLastApplyRecordsDataEndOffset() + 1;
            }

            DataBucketIndexFetchStatus newFetchStatus = fetchIndexOffsetAndReset(dataIndexBucket);
            fairIndexBucketStatusMap.updateAndMoveToEnd(dataIndexBucket, newFetchStatus);
            LOG.info(
                    "Index fetch reset for data-index bucket {} due to offset {} out of range, likely caused by leader change",
                    dataIndexBucket,
                    currentFetchOffset);
        } catch (Exception e) {
            LOG.error("Error getting index fetch offset for {} due to error", dataIndexBucket, e);
        }
    }

    /**
     * Handle an index bucket whose fetch offset is out of range and return a new fetch status. For
     * index replication, we don't need to truncate local data like regular log replication, but we
     * need to find a valid fetch offset from the data bucket's log.
     */
    private DataBucketIndexFetchStatus fetchIndexOffsetAndReset(
            DataIndexTableBucket dataIndexBucket) throws Exception {
        TableBucket dataBucket = dataIndexBucket.getDataBucket();

        // Get the data bucket replica to check its current state
        Replica dataReplica = replicaManager.getReplicaOrException(dataBucket);
        long dataReplicaEndOffset = dataReplica.getLocalLogEndOffset();

        /*
         * For index replication, we need to coordinate with the data bucket's log state.
         * The index fetch offset should be within the data bucket's log range.
         *
         * Unlike regular log replication, we don't truncate index data because:
         * 1. Index data is derived from data log, not authoritative
         * 2. Index buckets can be regenerated from data log if needed
         * 3. Index replication can start from any valid data log offset
         */
        long leaderDataEndOffset = leader.fetchLocalLogEndOffset(dataBucket).get();
        long leaderDataStartOffset = leader.fetchLocalLogStartOffset(dataBucket).get();

        // For index replication, we just need to coordinate the offset ranges but don't store
        // the actual fetch offset in DataBucketIndexFetchStatus anymore since IndexApplier
        // is the source of truth. We still do the validation checks and logging.
        if (leaderDataEndOffset < dataReplicaEndOffset) {
            // Leader's data is behind our local data (unclean leader election case)
            LOG.warn(
                    "Leader data end offset {} is behind local data end offset {} for data bucket {}. "
                            + "Index fetch will reset to coordinate with leader.",
                    leaderDataEndOffset,
                    dataReplicaEndOffset,
                    dataBucket);
        } else if (leaderDataStartOffset > dataReplicaEndOffset) {
            // Our local data is too far behind, start from leader's start offset
            LOG.warn(
                    "Local data end offset {} is behind leader's start offset {} for data bucket {}. "
                            + "Index fetch will reset to coordinate with leader.",
                    dataReplicaEndOffset,
                    leaderDataStartOffset,
                    dataBucket);
        }

        // For index buckets, we need to get the table path from index metadata
        // Try to get it from the current fetch status first
        DataBucketIndexFetchStatus currentStatus =
                fairIndexBucketStatusMap.statusValue(dataIndexBucket);
        TablePath indexTablePath =
                currentStatus != null
                        ? currentStatus.indexTablePath()
                        : null; // This should not happen in practice

        if (indexTablePath == null) {
            // Fallback: construct a reasonable table path
            // This is a safety measure, in normal cases currentStatus should exist
            LOG.warn("Could not find index table path or current status for {}", dataIndexBucket);
            throw new IllegalStateException(
                    String.format(
                            "Index table path or current status not found for data-index bucket %s",
                            dataIndexBucket));
        }

        return new DataBucketIndexFetchStatus(
                dataIndexBucket, indexTablePath, null, currentStatus.indexApplier());
    }

    /**
     * Returns initial index bucket fetch status based on current status and the provided {@link
     * IndexInitialFetchStatus}.
     */
    private DataBucketIndexFetchStatus indexBucketFetchStatus(
            DataIndexTableBucket dataIndexBucket,
            IndexInitialFetchStatus indexInitialFetchStatus,
            @Nullable DataBucketIndexFetchStatus currentFetchStatus) {
        if (currentFetchStatus != null) {
            return currentFetchStatus;
        } else {
            return new DataBucketIndexFetchStatus(
                    dataIndexBucket,
                    indexInitialFetchStatus.indexTablePath(),
                    null,
                    indexInitialFetchStatus.indexApplier());
        }
    }

    @Override
    public boolean initiateShutdown() {
        return super.initiateShutdown();
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        super.awaitShutdown();
        // We don't expect any exceptions here, but catch and log any errors to avoid failing the
        // caller, especially during shutdown. It is safe to catch the exception here without
        // causing correctness issue because we are going to shut down the thread and will not
        // re-use the leaderEndpoint anyway.
        try {
            leader.close();
        } catch (Throwable t) {
            LOG.error("Failed to close after shutting down replica fetcher thread.", t);
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        initiateShutdown();
        inLock(indexBucketStatusMapLock, indexBucketStatusMapCondition::signalAll);
        awaitShutdown();
    }

    private Optional<FetchIndexContext> buildFetchIndexContext(
            Map<DataIndexTableBucket, DataBucketIndexFetchStatus> indexBucketFetchStatusMap) {
        Map<Long, TablePath> tableIdToTablePath = new HashMap<>();
        FetchIndexRequest fetchRequest =
                new FetchIndexRequest()
                        .setMaxBytes(maxFetchBytes)
                        .setMinAdvanceOffset(minAdvanceOffset)
                        .setMaxWaitMs(maxFetchWaitMs);
        Set<DataIndexTableBucket> reqDataIndexBuckets = new HashSet<>();

        Map<TableBucket, Map<TableBucket, PbFetchIndexReqForIndexTableBucket>> indexBucketReqMap =
                new HashMap<>();
        for (Map.Entry<DataIndexTableBucket, DataBucketIndexFetchStatus>
                dataIndexTableBucketDataBucketIndexFetchStatusEntry :
                        indexBucketFetchStatusMap.entrySet()) {
            if (dataIndexTableBucketDataBucketIndexFetchStatusEntry.getValue().isReadyForFetch()) {
                DataIndexTableBucket dataIndexBucket =
                        dataIndexTableBucketDataBucketIndexFetchStatusEntry.getKey();
                DataBucketIndexFetchStatus indexFetchStatus =
                        dataIndexTableBucketDataBucketIndexFetchStatusEntry.getValue();
                PbFetchIndexReqForIndexTableBucket fetchIndexReqForIndexBucket =
                        new PbFetchIndexReqForIndexTableBucket()
                                .setTableId(dataIndexBucket.getIndexBucket().getTableId())
                                .setBucketId(dataIndexBucket.getIndexBucket().getBucket())
                                .setFetchOffset(indexFetchStatus.queryFetchOffset())
                                .setIndexCommitOffset(indexFetchStatus.queryIndexCommitOffset());
                if (dataIndexBucket.getIndexBucket().getPartitionId() != null) {
                    fetchIndexReqForIndexBucket.setPartitionId(
                            dataIndexBucket.getIndexBucket().getPartitionId());
                }
                indexBucketReqMap
                        .computeIfAbsent(dataIndexBucket.getDataBucket(), key -> Maps.newHashMap())
                        .put(dataIndexBucket.getIndexBucket(), fetchIndexReqForIndexBucket);
                reqDataIndexBuckets.add(dataIndexBucket);
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Index bucket {} is not ready for fetch, skipping",
                            dataIndexTableBucketDataBucketIndexFetchStatusEntry.getKey());
                }
            }
        }

        if (reqDataIndexBuckets.isEmpty()) {
            return Optional.empty();
        } else {
            List<PbFetchIndexReqForTableBucket> reqForTableBuckets = new ArrayList<>();
            for (Map.Entry<TableBucket, Map<TableBucket, PbFetchIndexReqForIndexTableBucket>>
                    entry : indexBucketReqMap.entrySet()) {
                TableBucket dataBucket = entry.getKey();
                PbFetchIndexReqForTableBucket reqForTableBucket =
                        new PbFetchIndexReqForTableBucket()
                                .setTableId(dataBucket.getTableId())
                                .setBucketId(dataBucket.getBucket());
                if (dataBucket.getPartitionId() != null) {
                    reqForTableBucket.setPartitionId(dataBucket.getPartitionId());
                }
                reqForTableBucket.addAllReqForIndexBuckets(entry.getValue().values());
                reqForTableBuckets.add(reqForTableBucket);
            }
            fetchRequest.addAllReqForTableBuckets(reqForTableBuckets);
            return Optional.of(
                    new FetchIndexContext(tableIdToTablePath, fetchRequest, reqDataIndexBuckets));
        }
    }
}
