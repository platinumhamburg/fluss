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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.index.IndexApplier;
import org.apache.fluss.server.metadata.MetadataProvider;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Manages index fetcher threads using a target-state driven approach with auto reconciliation
 * mechanism.
 */
@ThreadSafe
public class IndexFetcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFetcherManager.class);

    // Check targets every 5 seconds
    private static final long GOAL_CHECK_INTERVAL_MS = 5000L;

    // Timeout for CREATING state (30 seconds)
    private static final long CREATING_STATE_TIMEOUT_MS = 30000L;

    // Invalid leader server ID
    private static final int INVALID_LEADER_ID = -1;

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final int serverId;
    private final ReplicaManager replicaManager;
    private final TabletServerMetadataProvider metadataProvider;
    private final int numFetchersPerServer;
    private final Function<Integer, Optional<ServerNode>> serverNodeMetadataCache;
    private final long fetchBackoffMs;

    /**
     * Map of IndexBucket -> Map of DataBucket -> IndexFetcherTarget. This represents all the target
     * states we need to achieve.
     */
    @GuardedBy("lock")
    private final Map<TableBucket, Map<TableBucket, IndexFetcherTarget>> indexBucketTargets =
            new HashMap<>();

    /**
     * Map of (DataLeaderServerId, FetcherId) -> IndexFetcherThread. This represents the actual
     * running fetcher threads.
     */
    private final Map<ServerIdAndFetcherId, IndexFetcherThread> indexFetcherThreadMap =
            MapUtils.newConcurrentHashMap();

    /**
     * Map of DataIndexTableBucket -> ServerIdAndFetcherId. This helps quickly locate which fetcher
     * thread is handling a specific data-index bucket.
     */
    @GuardedBy("lock")
    private final Map<DataIndexTableBucket, ServerIdAndFetcherId> dataIndexBucketToFetcher =
            new HashMap<>();

    private final Object lock = new Object();

    private volatile boolean isRunning = false;
    private FetcherReconciliationThread fetcherReconciliationThread;

    public IndexFetcherManager(
            Configuration conf,
            RpcClient rpcClient,
            int serverId,
            ReplicaManager replicaManager,
            TabletServerMetadataProvider metadataProvider,
            Function<Integer, Optional<ServerNode>> serverNodeMetadataCache) {
        this.conf = conf;
        this.rpcClient = rpcClient;
        this.serverId = serverId;
        this.replicaManager = replicaManager;
        this.metadataProvider = metadataProvider;
        this.numFetchersPerServer = conf.getInt(ConfigOptions.LOG_REPLICA_FETCHER_NUMBER);
        this.serverNodeMetadataCache = serverNodeMetadataCache;
        this.fetchBackoffMs = conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL).toMillis();
    }

    /** Start the target reconciliation background thread. */
    public void startup() {
        synchronized (lock) {
            if (isRunning) {
                LOG.warn("IndexFetcherManager is already running, skip startup.");
                return;
            }
            isRunning = true;
            fetcherReconciliationThread = new FetcherReconciliationThread();
            fetcherReconciliationThread.start();
            LOG.info(
                    "IndexFetcherManager started with target check interval: {} ms",
                    GOAL_CHECK_INTERVAL_MS);
        }
    }

    /**
     * Add or update targets for an index bucket that became leader. This is called when an
     * IndexBucket becomes leader and needs to fetch from all DataBuckets.
     *
     * @param indexBucket the index bucket that became leader
     * @param dataIndexBucketTargetss map of DataIndexTableBucket to its target info (table path,
     *     index applier)
     */
    public void addOrUpdateTargetsForIndexBucket(
            TableBucket indexBucket,
            Map<DataIndexTableBucket, FetcherTargetInfo> dataIndexBucketTargetss) {
        if (dataIndexBucketTargetss.isEmpty()) {
            LOG.debug("No targets to add for index bucket {}", indexBucket);
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> targetsForIndexBucket =
                    indexBucketTargets.computeIfAbsent(indexBucket, k -> new HashMap<>());

            for (Map.Entry<DataIndexTableBucket, FetcherTargetInfo> entry :
                    dataIndexBucketTargetss.entrySet()) {
                DataIndexTableBucket dataIndexBucket = entry.getKey();
                FetcherTargetInfo fetcherTargetInfo = entry.getValue();
                TableBucket dataBucket = dataIndexBucket.getDataBucket();

                // Check if target already exists
                IndexFetcherTarget existingTarget = targetsForIndexBucket.get(dataBucket);

                if (existingTarget != null) {
                    // Target already exists, just reset to pending if it was failed
                    if (existingTarget.getState() == IndexFetcherTarget.State.FAILED) {
                        LOG.debug(
                                "Resetting failed target to pending for data-index bucket {}",
                                dataIndexBucket);
                        existingTarget.setState(IndexFetcherTarget.State.PENDING);
                        existingTarget.resetFailureCount();
                    }
                } else {
                    // New target - no need to specify leader, it will be queried dynamically
                    IndexFetcherTarget newTarget =
                            new IndexFetcherTarget(
                                    dataIndexBucket,
                                    fetcherTargetInfo.indexTablePath,
                                    fetcherTargetInfo.indexApplier);
                    targetsForIndexBucket.put(dataBucket, newTarget);
                    LOG.info("Added new target for data-index bucket {}", dataIndexBucket);
                }
            }

            // Trigger immediate reconciliation
            lock.notifyAll();
        }

        LOG.info(
                "Added/updated {} targets for index bucket {}",
                dataIndexBucketTargetss.size(),
                indexBucket);
    }

    /**
     * Remove all targets for an index bucket. This is called when an IndexBucket is no longer a
     * leader or is deleted.
     *
     * @param indexBucket the index bucket to remove
     */
    public void removeIndexBucket(TableBucket indexBucket) {
        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> removed = indexBucketTargets.remove(indexBucket);
            if (removed != null && !removed.isEmpty()) {
                LOG.info("Removed {} targets for index bucket {}", removed.size(), indexBucket);

                // Immediately remove buckets from fetcher threads and clean up mappings
                removeFetchers(removed.values(), indexBucket);

                // Trigger reconciliation to clean up idle fetchers
                lock.notifyAll();
            }
        }
    }

    /**
     * Remove targets for specific index buckets.
     *
     * @param indexBuckets set of index buckets to remove
     */
    public void removeIndexBuckets(Set<TableBucket> indexBuckets) {
        if (indexBuckets.isEmpty()) {
            return;
        }

        synchronized (lock) {
            for (TableBucket indexBucket : indexBuckets) {
                removeIndexBucket(indexBucket);
            }
        }
    }

    /**
     * Remove targets for specific data buckets within an index bucket. This is called when certain
     * data buckets (e.g., from deleted partitions) should no longer be fetched.
     *
     * @param indexBucket the index bucket
     * @param dataBucketsToRemove set of data buckets whose targets should be removed
     */
    public void removeDataBucketsFromIndexBucket(
            TableBucket indexBucket, Set<TableBucket> dataBucketsToRemove) {
        if (dataBucketsToRemove.isEmpty()) {
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> targetsForIndexBucket =
                    indexBucketTargets.get(indexBucket);
            if (targetsForIndexBucket != null) {
                List<IndexFetcherTarget> removedTargets = new ArrayList<>();
                for (TableBucket dataBucket : dataBucketsToRemove) {
                    IndexFetcherTarget removed = targetsForIndexBucket.remove(dataBucket);
                    if (removed != null) {
                        removedTargets.add(removed);
                    }
                }

                if (!removedTargets.isEmpty()) {
                    LOG.info(
                            "Removed {} data bucket targets from index bucket {}",
                            removedTargets.size(),
                            indexBucket);

                    // Immediately remove buckets from fetcher threads and clean up mappings
                    removeFetchers(removedTargets, indexBucket);

                    // Trigger reconciliation to clean up idle fetchers
                    lock.notifyAll();
                }

                // If no targets left for this index bucket, remove it completely
                if (targetsForIndexBucket.isEmpty()) {
                    indexBucketTargets.remove(indexBucket);
                    LOG.info("Removed index bucket {} as it has no more targets", indexBucket);
                }
            }
        }
    }

    /** Shutdown the manager and all running fetcher threads. */
    public void shutdown() throws InterruptedException {
        LOG.info("Shutting down IndexFetcherManager");

        synchronized (lock) {
            isRunning = false;
            lock.notifyAll();
        }

        // Shutdown target reconciliation thread
        if (fetcherReconciliationThread != null) {
            fetcherReconciliationThread.shutdown();
            fetcherReconciliationThread = null;
        }

        // Shutdown all fetcher threads
        closeAllFetchers();

        synchronized (lock) {
            indexBucketTargets.clear();
            dataIndexBucketToFetcher.clear();
        }

        LOG.info("IndexFetcherManager shutdown completed");
    }

    /**
     * Remove a specific data-index bucket from its current fetcher thread. This method should be
     * called while holding the lock.
     *
     * @param dataIndexBucket the data-index bucket to remove from its current fetcher
     */
    @GuardedBy("lock")
    private void removeFromCurrentFetcher(DataIndexTableBucket dataIndexBucket) {
        ServerIdAndFetcherId fetcherId = dataIndexBucketToFetcher.get(dataIndexBucket);

        if (fetcherId != null) {
            IndexFetcherThread fetcherThread = indexFetcherThreadMap.get(fetcherId);
            if (fetcherThread != null) {
                // Remove this specific data-index bucket from the fetcher thread
                fetcherThread.removeIf(dib -> dib.equals(dataIndexBucket));
                LOG.debug(
                        "Removed data-index bucket {} from fetcher thread (server={}, fetcher={}) due to fetch failure",
                        dataIndexBucket,
                        fetcherId.getServerId(),
                        fetcherId.getFetcherId());
            }

            // Clean up mapping
            dataIndexBucketToFetcher.remove(dataIndexBucket);
        }
    }

    /**
     * Remove targets from their corresponding fetcher threads and clean up mappings. This method
     * should be called while holding the lock.
     *
     * @param targetsToRemove collection of targets to remove
     * @param indexBucket the index bucket these targets belong to
     */
    @GuardedBy("lock")
    private void removeFetchers(
            Iterable<IndexFetcherTarget> targetsToRemove, TableBucket indexBucket) {
        // Group targets by fetcher thread
        Map<ServerIdAndFetcherId, Set<TableBucket>> fetcherToIndexBuckets = new HashMap<>();

        for (IndexFetcherTarget target : targetsToRemove) {
            DataIndexTableBucket dataIndexBucket = target.getDataIndexTableBucket();
            ServerIdAndFetcherId fetcherId = dataIndexBucketToFetcher.get(dataIndexBucket);

            if (fetcherId != null) {
                fetcherToIndexBuckets
                        .computeIfAbsent(fetcherId, k -> new HashSet<>())
                        .add(indexBucket);
            }

            // Clean up mapping immediately
            dataIndexBucketToFetcher.remove(dataIndexBucket);
        }

        // Remove buckets from each fetcher thread
        for (Map.Entry<ServerIdAndFetcherId, Set<TableBucket>> entry :
                fetcherToIndexBuckets.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            Set<TableBucket> indexBucketsToRemove = entry.getValue();

            IndexFetcherThread fetcherThread = indexFetcherThreadMap.get(fetcherId);
            if (fetcherThread != null) {
                fetcherThread.removeIndexBuckets(indexBucketsToRemove);
                LOG.debug(
                        "Removed index buckets {} from fetcher thread (server={}, fetcher={})",
                        indexBucketsToRemove,
                        fetcherId.getServerId(),
                        fetcherId.getFetcherId());
            }
        }
    }

    /**
     * Background thread that continuously reconciles the actual fetcher state with the target
     * state. It periodically checks all targets and creates/removes fetcher threads as needed.
     */
    private class FetcherReconciliationThread extends ShutdownableThread {
        FetcherReconciliationThread() {
            super("IndexFetcherTargetReconciliation", true);
        }

        @Override
        public void doWork() {
            try {
                synchronized (lock) {
                    if (!isRunning) {
                        return;
                    }

                    reconcileFetchers();

                    // Wait for next check interval or until notified
                    lock.wait(GOAL_CHECK_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                LOG.debug("Fetcher reconciliation thread interrupted", e);
                // Thread was interrupted during shutdown, exit gracefully
            } catch (Exception e) {
                LOG.error("Error during target reconciliation", e);
            }
        }
    }

    /**
     * Reconcile actual fetcher state with target state. This method should be called while holding
     * the lock.
     */
    @GuardedBy("lock")
    private void reconcileFetchers() {
        long currentTimeMs = System.currentTimeMillis();

        // 1. Process failures from fetcher threads
        int failedCount = processFetchFailures(currentTimeMs);

        // 2. Classify targets and prepare actions
        ReconciliationActions actions = classifyAndPrepareActions(currentTimeMs);

        // 3. Execute creation/updates
        executeFetcherCreations(actions.targetsToCreate, currentTimeMs);

        // NOTE: Intentionally NOT cleaning up idle fetchers to implement thread pooling.
        // IndexFetcherThreads are kept alive for reuse, avoiding the overhead of frequent
        // thread creation/destruction. The thread pool is bounded by
        // (numFetchersPerServer * number_of_data_leaders), which is a reasonable upper bound.
        // Threads will only be cleaned up during shutdown.

        // 4. Log reconciliation summary
        if (LOG.isDebugEnabled() && (failedCount > 0 || !actions.targetsToCreate.isEmpty())) {
            LOG.debug(
                    "Reconciliation completed: processed {} failures, {} targets to create/update, {} total fetcher threads",
                    failedCount,
                    actions.targetsToCreate.size(),
                    indexFetcherThreadMap.size());
        }
    }

    /**
     * Process failures reported by fetcher threads. Returns the count of failed buckets processed.
     */
    @GuardedBy("lock")
    private int processFetchFailures(long currentTimeMs) {
        int failedCount = 0;
        Set<DataIndexTableBucket> removedBuckets = new HashSet<>();

        for (IndexFetcherThread fetcherThread : indexFetcherThreadMap.values()) {
            Map<DataIndexTableBucket, String> failedBuckets = fetcherThread.pollInvalidBuckets();
            if (!failedBuckets.isEmpty()) {
                LOG.info(
                        "Processing {} fetch failures from fetcher thread {}",
                        failedBuckets.size(),
                        fetcherThread.getName());
            }
            for (Map.Entry<DataIndexTableBucket, String> entry : failedBuckets.entrySet()) {
                DataIndexTableBucket dataIndexBucket = entry.getKey();
                String reason = entry.getValue();
                TableBucket indexBucket = dataIndexBucket.getIndexBucket();
                TableBucket dataBucket = dataIndexBucket.getDataBucket();

                Map<TableBucket, IndexFetcherTarget> targetsForIndexBucket =
                        indexBucketTargets.get(indexBucket);
                if (targetsForIndexBucket != null) {
                    IndexFetcherTarget target = targetsForIndexBucket.get(dataBucket);
                    if (target != null && target.getState() == IndexFetcherTarget.State.RUNNING) {
                        LOG.info(
                                "Fetch failure for data-index bucket {}, marking for retry. Reason: {}",
                                dataIndexBucket,
                                reason);
                        target.markFailed(reason, currentTimeMs);

                        // Track buckets to remove (avoid duplicate removal)
                        removedBuckets.add(dataIndexBucket);
                        failedCount++;
                    }
                }
            }
        }

        // Remove all failed buckets from their fetchers once
        for (DataIndexTableBucket dataIndexBucket : removedBuckets) {
            removeFromCurrentFetcher(dataIndexBucket);
        }

        return failedCount;
    }

    /**
     * Classify all targets and prepare actions for reconciliation. Returns a ReconciliationActions
     * object containing targets to create.
     *
     * <p>Retry storm prevention is handled by the fixed backoff strategy in IndexFetcherTarget
     * (minimum 5 seconds for FAILED targets), so no additional rate limiting is needed here.
     */
    @GuardedBy("lock")
    private ReconciliationActions classifyAndPrepareActions(long currentTimeMs) {
        Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> targetsToCreate = new HashMap<>();

        // Iterate through all targets and classify them
        for (Map.Entry<TableBucket, Map<TableBucket, IndexFetcherTarget>> entry :
                indexBucketTargets.entrySet()) {
            for (IndexFetcherTarget target : entry.getValue().values()) {

                switch (target.getState()) {
                    case PENDING:
                    case FAILED:
                        handlePendingOrFailedFetcher(target, targetsToCreate, currentTimeMs);
                        break;

                    case RUNNING:
                        handleRunningFetcher(target);
                        break;

                    case CREATING:
                        handleCreatingFetcher(target, currentTimeMs);
                        break;
                }
            }
        }

        return new ReconciliationActions(targetsToCreate);
    }

    /**
     * Query the leader for a data bucket based on target state. For FAILED targets, this method
     * uses an adaptive strategy: starts with cache lookup, and if needed, queries ZooKeeper to get
     * fresh leader information or confirm bucket deletion.
     *
     * <p>This method uses optimized leader queries that avoid constructing intermediate
     * BucketMetadata objects, significantly reducing memory allocation overhead.
     *
     * @param target the target whose data bucket leader needs to be queried
     * @return leader query result containing the bucket leader info and whether ZK was queried
     */
    @GuardedBy("lock")
    private LeaderQueryResult queryLeaderForTarget(IndexFetcherTarget target) {
        MetadataProvider.BucketLeaderResult result;
        boolean queriedFromZK = false;

        if (target.getState() == IndexFetcherTarget.State.FAILED) {
            // For FAILED targets, check if we need to query ZK to get fresh data
            // First try cache (using optimized method)
            result = metadataProvider.getBucketLeaderIdFromCache(target.getDataBucket());

            // If cache shows the same leader we failed with (or unavailable),
            // query ZK to detect leader changes or confirm bucket deletion
            if (!result.isFound() || result.getLeaderId() == target.getLastKnownLeaderServerId()) {
                if (!result.isFound()) {
                    LOG.debug(
                            "Cached leader for data bucket {} is unavailable, querying ZooKeeper to verify if bucket exists",
                            target.getDataBucket());
                } else {
                    LOG.debug(
                            "Cached leader {} for data bucket {} matches last failed leader, querying ZooKeeper for fresh leader info",
                            result.getLeaderId(),
                            target.getDataBucket());
                }
                result = metadataProvider.getBucketLeaderIdFromZK(target.getDataBucket());
                queriedFromZK = true;
            }
        } else {
            // For PENDING targets, use cache (using optimized method)
            result = metadataProvider.getBucketLeaderIdFromCache(target.getDataBucket());
        }

        return new LeaderQueryResult(result, queriedFromZK);
    }

    /**
     * Handle a target in PENDING or FAILED state. May add it to targetsToCreate if ready for
     * (re)creation.
     */
    @GuardedBy("lock")
    private void handlePendingOrFailedFetcher(
            IndexFetcherTarget target,
            Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> targetsToCreate,
            long currentTimeMs) {

        // Check if ready for retry
        if (!target.isReadyForRetry(currentTimeMs, fetchBackoffMs)) {
            return;
        }

        // Query leader directly from MetadataProvider
        LeaderQueryResult queryResult = queryLeaderForTarget(target);
        MetadataProvider.BucketLeaderResult result = queryResult.result;
        boolean queriedFromZK = queryResult.queriedFromZK;

        // Handle case where leader is not available
        if (!result.isFound()) {
            handleLeaderNotAvailable(target, result, queriedFromZK, currentTimeMs);
            return;
        }

        int leaderId = result.getLeaderId();

        // Update target state with new leader information
        target.setLastKnownLeaderServerId(leaderId);

        // Log progress based on target state
        if (target.getState() == IndexFetcherTarget.State.PENDING) {
            LOG.info(
                    "Starting to fetch for data-index bucket {}, leader: {}",
                    target.getDataIndexTableBucket(),
                    leaderId);
        } else {
            LOG.info(
                    "Retrying failed target (attempt {}) for data-index bucket {}, current leader: {}{}. Last failure: {}",
                    target.getFailureCount() + 1,
                    target.getDataIndexTableBucket(),
                    leaderId,
                    queriedFromZK ? " (from ZooKeeper)" : "",
                    target.getLastFailureReason());
        }

        // Schedule target for creation/update
        ServerIdAndFetcherId fetcherId =
                new ServerIdAndFetcherId(leaderId, getFetcherId(target.getDataBucket()));
        targetsToCreate.computeIfAbsent(fetcherId, k -> new ArrayList<>()).add(target);
    }

    /**
     * Handle the case where leader is not available for a target. Marks the target as failed and
     * updates the timestamp.
     *
     * <p>Special handling for ZK-verified failures: When querying directly from ZooKeeper and the
     * bucket is not found, it means the table/partition has been deleted. This is a permanent
     * failure that should be cleaned up by removeDataBucketsFromIndexBucket or removeIndexBucket
     * calls from ReplicaManager.
     */
    @GuardedBy("lock")
    private void handleLeaderNotAvailable(
            IndexFetcherTarget target,
            MetadataProvider.BucketLeaderResult result,
            boolean queriedFromZK,
            long currentTimeMs) {

        String reason = result.getReason();

        // Log warning for permanent failures (NOT_FOUND from ZK) to help identify cleanup issues
        if (queriedFromZK && result.isNotFound()) {
            if (target.getFailureCount() > 0 && target.getFailureCount() % 20 == 0) {
                LOG.warn(
                        "Fetcher for data-index bucket {} has failed {} times. "
                                + "Data bucket appears to be deleted but target not yet cleaned up. "
                                + "Verify that partition/table deletion events are being processed correctly. Reason: {}",
                        target.getDataIndexTableBucket(),
                        target.getFailureCount(),
                        reason);
            }
        } else if (target.getState() == IndexFetcherTarget.State.PENDING && !queriedFromZK) {
            // Cache miss for PENDING target - this is normal, will retry
            LOG.debug(
                    "Data bucket {} leader not available in cache, will retry",
                    target.getDataBucket());
        }

        // Mark target as failed with the reason from BucketLeaderResult
        target.markFailed(reason, currentTimeMs);
    }

    /** Handle a target in RUNNING state. Checks for leader changes. */
    @GuardedBy("lock")
    private void handleRunningFetcher(IndexFetcherTarget target) {
        if (hasValidLeader(target)) {
            // Check if the leader has changed (using optimized method)
            MetadataProvider.BucketLeaderResult result =
                    metadataProvider.getBucketLeaderIdFromCache(target.getDataBucket());
            int lastKnownLeader = target.getLastKnownLeaderServerId();

            // If we can't query the current leader (not found or unavailable),
            // it might mean the leader has changed but metadata cache hasn't been updated yet.
            // In this case, we should also mark the target as failed to trigger reconnection.
            if (!result.isFound()) {
                LOG.info(
                        "Data bucket {} leader information not available in metadata cache (last known leader: {}), marking target as failed to trigger reconnection. Reason: {}",
                        target.getDataBucket(),
                        lastKnownLeader,
                        result.getReason());
                target.markFailed(
                        String.format(
                                "Data bucket leader information not available (last known: %d): %s",
                                lastKnownLeader, result.getReason()),
                        System.currentTimeMillis());

                // Remove from current fetcher
                removeFromCurrentFetcher(target.getDataIndexTableBucket());
                return;
            }

            int currentLeader = result.getLeaderId();
            if (currentLeader != lastKnownLeader) {
                // Leader has changed, mark target as failed to trigger reconnection
                LOG.info(
                        "Data bucket {} leader changed from {} to {}, marking target as failed to trigger reconnection",
                        target.getDataBucket(),
                        lastKnownLeader,
                        currentLeader);
                target.markFailed(
                        String.format(
                                "Data bucket leader changed from %d to %d",
                                lastKnownLeader, currentLeader),
                        System.currentTimeMillis());

                // Remove from current fetcher
                removeFromCurrentFetcher(target.getDataIndexTableBucket());
            }
        }
    }

    /** Handle a target in CREATING state. Checks for timeout and resets if stuck. */
    @GuardedBy("lock")
    private void handleCreatingFetcher(IndexFetcherTarget target, long currentTimeMs) {
        if (target.isCreatingTimeout(currentTimeMs, CREATING_STATE_TIMEOUT_MS)) {
            LOG.warn(
                    "Fetcher stuck in CREATING state for data-index bucket {}, resetting to FAILED",
                    target.getDataIndexTableBucket());
            target.markFailed(
                    "Creation timeout after " + CREATING_STATE_TIMEOUT_MS + "ms", currentTimeMs);
        }
    }

    /** Execute target creations/updates for all prepared targets. */
    @GuardedBy("lock")
    private void executeFetcherCreations(
            Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> targetsToCreate,
            long currentTimeMs) {
        for (Map.Entry<ServerIdAndFetcherId, List<IndexFetcherTarget>> entry :
                targetsToCreate.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            List<IndexFetcherTarget> targets = entry.getValue();

            try {
                createOrUpdateFetcher(fetcherId, targets, currentTimeMs);
            } catch (Exception e) {
                LOG.error(
                        "Failed to create/update fetcher for server {} fetcher {}",
                        fetcherId.getServerId(),
                        fetcherId.getFetcherId(),
                        e);
                // Mark all targets as failed
                for (IndexFetcherTarget target : targets) {
                    target.markFailed("Failed to create fetcher: " + e.getMessage(), currentTimeMs);
                }
            }
        }
    }

    /** Check if a target has a valid leader server ID. */
    private boolean hasValidLeader(IndexFetcherTarget target) {
        return target.getLastKnownLeaderServerId() > INVALID_LEADER_ID;
    }

    /** Helper class to hold reconciliation actions. */
    private static class ReconciliationActions {
        final Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> targetsToCreate;

        ReconciliationActions(Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> targetsToCreate) {
            this.targetsToCreate = targetsToCreate;
        }
    }

    /**
     * Create or update a fetcher thread for the given targets. The fetcherId contains the
     * dynamically queried leader server ID. This method should be called while holding the lock.
     *
     * @param fetcherId the target server and fetcher ID (server ID was dynamically queried)
     * @param targets the list of targets to be achieved by this fetcher
     * @param currentTimeMs current timestamp
     */
    @GuardedBy("lock")
    private void createOrUpdateFetcher(
            ServerIdAndFetcherId fetcherId, List<IndexFetcherTarget> targets, long currentTimeMs)
            throws Exception {

        // Mark targets as creating
        for (IndexFetcherTarget target : targets) {
            target.setState(IndexFetcherTarget.State.CREATING, currentTimeMs);
        }

        // Get or create fetcher thread
        IndexFetcherThread fetcherThread = indexFetcherThreadMap.get(fetcherId);

        if (fetcherThread == null) {
            // Create new fetcher thread
            fetcherThread =
                    createIndexFetcherThread(fetcherId.getFetcherId(), fetcherId.getServerId());
            indexFetcherThreadMap.put(fetcherId, fetcherThread);
            fetcherThread.start();
            LOG.info(
                    "Created new IndexFetcherThread for leader {} fetcher {} with {} targets",
                    fetcherId.getServerId(),
                    fetcherId.getFetcherId(),
                    targets.size());
        } else {
            // Verify fetcher is still targeting the same leader
            if (fetcherThread.getLeader().leaderServerId() != fetcherId.getServerId()) {
                LOG.warn(
                        "Fetcher thread leader mismatch: expected {}, actual {}. Recreating fetcher.",
                        fetcherId.getServerId(),
                        fetcherThread.getLeader().leaderServerId());
                try {
                    fetcherThread.shutdown();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while shutting down fetcher thread", e);
                }
                indexFetcherThreadMap.remove(fetcherId);

                // Create new fetcher thread
                fetcherThread =
                        createIndexFetcherThread(fetcherId.getFetcherId(), fetcherId.getServerId());
                indexFetcherThreadMap.put(fetcherId, fetcherThread);
                fetcherThread.start();
            }
        }

        // Prepare initial fetch status for all targets
        // Use the dynamically queried leader server ID from fetcherId
        Map<DataIndexTableBucket, IndexInitialFetchStatus> initialFetchStatusMap = new HashMap<>();
        for (IndexFetcherTarget target : targets) {
            // Query current state from IndexApplier's BucketStateManager
            IndexApplier.IndexApplyStatus applyStatus =
                    target.getIndexApplier().getOrInitIndexApplyStatus(target.getDataBucket());

            IndexInitialFetchStatus fetchStatus =
                    new IndexInitialFetchStatus(
                            target.getIndexBucket().getTableId(),
                            target.getIndexTablePath(),
                            // Use dynamically queried leader ID
                            fetcherId.getServerId(),
                            // Fetch from persisted
                            applyStatus.getLastApplyRecordsDataEndOffset(),
                            // state
                            // Index commit offset
                            applyStatus.getIndexCommitDataOffset(),
                            target.getIndexApplier());
            initialFetchStatusMap.put(target.getDataIndexTableBucket(), fetchStatus);
        }

        // Add buckets to fetcher thread
        try {
            fetcherThread.addIndexBuckets(initialFetchStatusMap);

            // Mark targets as running
            for (IndexFetcherTarget target : targets) {
                target.markRunning();
                dataIndexBucketToFetcher.put(target.getDataIndexTableBucket(), fetcherId);
                LOG.debug(
                        "Successfully started fetching for data-index bucket {}",
                        target.getDataIndexTableBucket());
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while adding buckets to fetcher thread", e);
            // Mark targets as failed
            for (IndexFetcherTarget target : targets) {
                target.markFailed(
                        "Interrupted while adding to fetcher: " + e.getMessage(), currentTimeMs);
            }
            throw e;
        }
    }

    /** Clean up fetcher threads that have no running targets. */
    @GuardedBy("lock")
    private void cleanupIdleFetchers(Set<ServerIdAndFetcherId> activeFetchers) {
        Set<ServerIdAndFetcherId> idleFetchers = new HashSet<>();

        for (Map.Entry<ServerIdAndFetcherId, IndexFetcherThread> entry :
                indexFetcherThreadMap.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            IndexFetcherThread fetcherThread = entry.getValue();

            if (!activeFetchers.contains(fetcherId) && fetcherThread.getBucketCount() == 0) {
                idleFetchers.add(fetcherId);
            }
        }

        // Remove idle fetchers
        for (ServerIdAndFetcherId fetcherId : idleFetchers) {
            IndexFetcherThread fetcherThread = indexFetcherThreadMap.remove(fetcherId);
            if (fetcherThread != null) {
                try {
                    fetcherThread.shutdown();
                    LOG.info(
                            "Shutdown idle IndexFetcherThread for leader {} fetcher {}",
                            fetcherId.getServerId(),
                            fetcherId.getFetcherId());
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while shutting down idle fetcher thread", e);
                }
            }
        }

        // Clean up dataIndexBucketToFetcher mappings for removed fetchers
        if (!idleFetchers.isEmpty()) {
            dataIndexBucketToFetcher
                    .entrySet()
                    .removeIf(entry -> idleFetchers.contains(entry.getValue()));
        }
    }

    private void closeAllFetchers() throws InterruptedException {
        List<IndexFetcherThread> threads = new ArrayList<>(indexFetcherThreadMap.values());

        // Remove all buckets from each fetcher thread before shutdown to ensure clean shutdown
        for (IndexFetcherThread thread : threads) {
            // Remove all data-index buckets from this fetcher thread
            thread.removeIf(dataIndexBucket -> true);
        }

        // Initiate shutdown for all threads
        for (IndexFetcherThread thread : threads) {
            thread.initiateShutdown();
        }

        // Wait for all threads to complete shutdown
        for (IndexFetcherThread thread : threads) {
            try {
                thread.shutdown();
            } catch (InterruptedException e) {
                LOG.error("Interrupted while shutting down fetcher thread", e);
                throw e;
            }
        }

        indexFetcherThreadMap.clear();
    }

    private int getFetcherId(TableBucket tableBucket) {
        return Math.abs(tableBucket.hashCode()) % numFetchersPerServer;
    }

    @VisibleForTesting
    IndexFetcherThread createIndexFetcherThread(int fetcherId, int leaderId) {
        String threadName = "IndexFetcherThread-" + fetcherId + "-" + leaderId;
        LeaderEndpoint leaderEndpoint = buildRemoteLogEndpoint(leaderId);
        return new IndexFetcherThread(
                threadName,
                replicaManager,
                leaderEndpoint,
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL).toMillis(),
                conf);
    }

    @VisibleForTesting
    RemoteLeaderEndpoint buildRemoteLogEndpoint(int leaderId) {
        return new RemoteLeaderEndpoint(
                conf,
                serverId,
                leaderId,
                GatewayClientProxy.createGatewayProxy(
                        () -> {
                            Optional<ServerNode> optionalServerNode =
                                    serverNodeMetadataCache.apply(leaderId);
                            if (optionalServerNode.isPresent()) {
                                return optionalServerNode.get();
                            } else {
                                // no available serverNode to connect, throw exception,
                                // fetch thread expects to retry
                                throw new RuntimeException(
                                        "ServerNode "
                                                + leaderId
                                                + " is not available in metadata cache.");
                            }
                        },
                        rpcClient,
                        TabletServerGateway.class));
    }

    /** Helper class to hold leader query results. */
    private static class LeaderQueryResult {
        final MetadataProvider.BucketLeaderResult result;
        final boolean queriedFromZK;

        LeaderQueryResult(MetadataProvider.BucketLeaderResult result, boolean queriedFromZK) {
            this.result = result;
            this.queriedFromZK = queriedFromZK;
        }
    }

    /**
     * Get all index fetcher targets (including RUNNING state). This is used for diagnostics and
     * monitoring.
     *
     * @return a map of IndexBucket -> list of all targets
     */
    public Map<TableBucket, List<IndexFetcherTargetStatus>> getAllFetcherTargetInfo() {
        Map<TableBucket, List<IndexFetcherTargetStatus>> result = new HashMap<>();
        synchronized (lock) {
            for (Map.Entry<TableBucket, Map<TableBucket, IndexFetcherTarget>> entry :
                    indexBucketTargets.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                Map<TableBucket, IndexFetcherTarget> targets = entry.getValue();

                List<IndexFetcherTargetStatus> allTargets = new ArrayList<>();
                for (IndexFetcherTarget target : targets.values()) {
                    allTargets.add(
                            new IndexFetcherTargetStatus(
                                    target.getDataIndexTableBucket(),
                                    target.getIndexTablePath(),
                                    target.getState(),
                                    target.getFailureCount(),
                                    target.getLastFailureReason(),
                                    target.getLastKnownLeaderServerId()));
                }

                if (!allTargets.isEmpty()) {
                    result.put(indexBucket, allTargets);
                }
            }
        }
        return result;
    }

    /**
     * Get all index fetcher targets that are not in RUNNING state. This is used for diagnostics and
     * monitoring.
     *
     * @return a map of IndexBucket -> list of non-running targets
     */
    public Map<TableBucket, List<IndexFetcherTargetStatus>> getNonRunningFetchers() {
        Map<TableBucket, List<IndexFetcherTargetStatus>> result = new HashMap<>();
        synchronized (lock) {
            for (Map.Entry<TableBucket, Map<TableBucket, IndexFetcherTarget>> entry :
                    indexBucketTargets.entrySet()) {
                TableBucket indexBucket = entry.getKey();
                Map<TableBucket, IndexFetcherTarget> targets = entry.getValue();

                List<IndexFetcherTargetStatus> nonRunningFetchers = new ArrayList<>();
                for (IndexFetcherTarget target : targets.values()) {
                    if (target.getState() != IndexFetcherTarget.State.RUNNING) {
                        nonRunningFetchers.add(
                                new IndexFetcherTargetStatus(
                                        target.getDataIndexTableBucket(),
                                        target.getIndexTablePath(),
                                        target.getState(),
                                        target.getFailureCount(),
                                        target.getLastFailureReason(),
                                        target.getLastKnownLeaderServerId()));
                    }
                }

                if (!nonRunningFetchers.isEmpty()) {
                    result.put(indexBucket, nonRunningFetchers);
                }
            }
        }
        return result;
    }

    /** Helper class to pass target information when adding targets. */
    public static class FetcherTargetInfo {
        private final TablePath indexTablePath;
        private final IndexApplier indexApplier;

        public FetcherTargetInfo(TablePath indexTablePath, IndexApplier indexApplier) {
            this.indexTablePath = indexTablePath;
            this.indexApplier = indexApplier;
        }

        public TablePath getIndexTablePath() {
            return indexTablePath;
        }
    }

    /** Information about a non-running index fetcher target for diagnostics. */
    public static class IndexFetcherTargetStatus {
        private final DataIndexTableBucket dataIndexTableBucket;
        private final TablePath indexTablePath;
        private final IndexFetcherTarget.State state;
        private final int failureCount;
        @Nullable private final String lastFailureReason;
        private final int lastKnownLeaderServerId;

        public IndexFetcherTargetStatus(
                DataIndexTableBucket dataIndexTableBucket,
                TablePath indexTablePath,
                IndexFetcherTarget.State state,
                int failureCount,
                @Nullable String lastFailureReason,
                int lastKnownLeaderServerId) {
            this.dataIndexTableBucket = dataIndexTableBucket;
            this.indexTablePath = indexTablePath;
            this.state = state;
            this.failureCount = failureCount;
            this.lastFailureReason = lastFailureReason;
            this.lastKnownLeaderServerId = lastKnownLeaderServerId;
        }

        public DataIndexTableBucket getDataIndexTableBucket() {
            return dataIndexTableBucket;
        }

        public TablePath getIndexTablePath() {
            return indexTablePath;
        }

        public IndexFetcherTarget.State getState() {

            return state;
        }

        public int getFailureCount() {
            return failureCount;
        }

        @Nullable
        public String getLastFailureReason() {
            return lastFailureReason;
        }

        public int getLastKnownLeaderServerId() {
            return lastKnownLeaderServerId;
        }
    }
}
