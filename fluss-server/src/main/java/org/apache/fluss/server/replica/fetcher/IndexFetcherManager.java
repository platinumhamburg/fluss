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
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Manages index fetcher threads using a goal-state driven approach with auto reconciliation
 * mechanism.
 */
@ThreadSafe
public class IndexFetcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFetcherManager.class);

    // Check goals every 5 seconds
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
     * Map of IndexBucket -> Map of DataBucket -> IndexFetcherGoal. This represents all the goal
     * states we need to achieve.
     */
    @GuardedBy("lock")
    private final Map<TableBucket, Map<TableBucket, IndexFetcherGoal>> indexBucketGoals =
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
    private GoalReconciliationThread goalReconciliationThread;

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

    /** Start the goal reconciliation background thread. */
    public void startup() {
        synchronized (lock) {
            if (isRunning) {
                LOG.warn("IndexFetcherManager is already running, skip startup.");
                return;
            }
            isRunning = true;
            goalReconciliationThread = new GoalReconciliationThread();
            goalReconciliationThread.start();
            LOG.info(
                    "IndexFetcherManager started with goal check interval: {} ms",
                    GOAL_CHECK_INTERVAL_MS);
        }
    }

    /**
     * Add or update goals for an index bucket that became leader. This is called when an
     * IndexBucket becomes leader and needs to fetch from all DataBuckets.
     *
     * @param indexBucket the index bucket that became leader
     * @param dataIndexBucketGoals map of DataIndexTableBucket to its goal info (table path, index
     *     applier)
     */
    public void addOrUpdateGoalsForIndexBucket(
            TableBucket indexBucket, Map<DataIndexTableBucket, GoalInfo> dataIndexBucketGoals) {
        if (dataIndexBucketGoals.isEmpty()) {
            LOG.debug("No goals to add for index bucket {}", indexBucket);
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherGoal> goalsForIndexBucket =
                    indexBucketGoals.computeIfAbsent(indexBucket, k -> new HashMap<>());

            for (Map.Entry<DataIndexTableBucket, GoalInfo> entry :
                    dataIndexBucketGoals.entrySet()) {
                DataIndexTableBucket dataIndexBucket = entry.getKey();
                GoalInfo goalInfo = entry.getValue();
                TableBucket dataBucket = dataIndexBucket.getDataBucket();

                // Check if goal already exists
                IndexFetcherGoal existingGoal = goalsForIndexBucket.get(dataBucket);

                if (existingGoal != null) {
                    // Goal already exists, just reset to pending if it was failed
                    if (existingGoal.getState() == IndexFetcherGoal.State.FAILED) {
                        LOG.debug(
                                "Resetting failed goal to pending for data-index bucket {}",
                                dataIndexBucket);
                        existingGoal.setState(IndexFetcherGoal.State.PENDING);
                        existingGoal.resetFailureCount();
                    }
                } else {
                    // New goal - no need to specify leader, it will be queried dynamically
                    IndexFetcherGoal newGoal =
                            new IndexFetcherGoal(
                                    dataIndexBucket,
                                    goalInfo.indexTablePath,
                                    goalInfo.indexApplier);
                    goalsForIndexBucket.put(dataBucket, newGoal);
                    LOG.info("Added new goal for data-index bucket {}", dataIndexBucket);
                }
            }

            // Trigger immediate reconciliation
            lock.notifyAll();
        }

        LOG.info(
                "Added/updated {} goals for index bucket {}",
                dataIndexBucketGoals.size(),
                indexBucket);
    }

    /**
     * Remove all goals for an index bucket. This is called when an IndexBucket is no longer a
     * leader or is deleted.
     *
     * @param indexBucket the index bucket to remove
     */
    public void removeIndexBucket(TableBucket indexBucket) {
        synchronized (lock) {
            Map<TableBucket, IndexFetcherGoal> removed = indexBucketGoals.remove(indexBucket);
            if (removed != null && !removed.isEmpty()) {
                LOG.info("Removed {} goals for index bucket {}", removed.size(), indexBucket);

                // Immediately remove buckets from fetcher threads and clean up mappings
                removeFetchers(removed.values(), indexBucket);

                // Trigger reconciliation to clean up idle fetchers
                lock.notifyAll();
            }
        }
    }

    /**
     * Remove goals for specific index buckets.
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
     * Remove goals for specific data buckets within an index bucket. This is called when certain
     * data buckets (e.g., from deleted partitions) should no longer be fetched.
     *
     * @param indexBucket the index bucket
     * @param dataBucketsToRemove set of data buckets whose goals should be removed
     */
    public void removeDataBucketsFromIndexBucket(
            TableBucket indexBucket, Set<TableBucket> dataBucketsToRemove) {
        if (dataBucketsToRemove.isEmpty()) {
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherGoal> goalsForIndexBucket =
                    indexBucketGoals.get(indexBucket);
            if (goalsForIndexBucket != null) {
                List<IndexFetcherGoal> removedGoals = new ArrayList<>();
                for (TableBucket dataBucket : dataBucketsToRemove) {
                    IndexFetcherGoal removed = goalsForIndexBucket.remove(dataBucket);
                    if (removed != null) {
                        removedGoals.add(removed);
                    }
                }

                if (!removedGoals.isEmpty()) {
                    LOG.info(
                            "Removed {} data bucket goals from index bucket {}",
                            removedGoals.size(),
                            indexBucket);

                    // Immediately remove buckets from fetcher threads and clean up mappings
                    removeFetchers(removedGoals, indexBucket);

                    // Trigger reconciliation to clean up idle fetchers
                    lock.notifyAll();
                }

                // If no goals left for this index bucket, remove it completely
                if (goalsForIndexBucket.isEmpty()) {
                    indexBucketGoals.remove(indexBucket);
                    LOG.info("Removed index bucket {} as it has no more goals", indexBucket);
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

        // Shutdown goal reconciliation thread
        if (goalReconciliationThread != null) {
            goalReconciliationThread.shutdown();
            goalReconciliationThread = null;
        }

        // Shutdown all fetcher threads
        closeAllFetchers();

        synchronized (lock) {
            indexBucketGoals.clear();
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
                        fetcherId.serverId,
                        fetcherId.fetcherId);
            }

            // Clean up mapping
            dataIndexBucketToFetcher.remove(dataIndexBucket);
        }
    }

    /**
     * Remove goals from their corresponding fetcher threads and clean up mappings. This method
     * should be called while holding the lock.
     *
     * @param goalsToRemove collection of goals to remove
     * @param indexBucket the index bucket these goals belong to
     */
    @GuardedBy("lock")
    private void removeFetchers(Iterable<IndexFetcherGoal> goalsToRemove, TableBucket indexBucket) {
        // Group goals by fetcher thread
        Map<ServerIdAndFetcherId, Set<TableBucket>> fetcherToIndexBuckets = new HashMap<>();

        for (IndexFetcherGoal goal : goalsToRemove) {
            DataIndexTableBucket dataIndexBucket = goal.getDataIndexTableBucket();
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
                        fetcherId.serverId,
                        fetcherId.fetcherId);
            }
        }
    }

    /**
     * Background thread that continuously reconciles the actual fetcher state with the goal state.
     * It periodically checks all goals and creates/removes fetcher threads as needed.
     */
    private class GoalReconciliationThread extends ShutdownableThread {
        GoalReconciliationThread() {
            super("IndexFetcherGoalReconciliation", true);
        }

        @Override
        public void doWork() {
            try {
                synchronized (lock) {
                    if (!isRunning) {
                        return;
                    }

                    reconcileGoals();

                    // Wait for next check interval or until notified
                    lock.wait(GOAL_CHECK_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                LOG.debug("Goal reconciliation thread interrupted", e);
                // Thread was interrupted during shutdown, exit gracefully
            } catch (Exception e) {
                LOG.error("Error during goal reconciliation", e);
            }
        }
    }

    /**
     * Reconcile actual fetcher state with goal state. This method should be called while holding
     * the lock.
     */
    @GuardedBy("lock")
    private void reconcileGoals() {
        long currentTimeMs = System.currentTimeMillis();

        // 1. Process failures from fetcher threads
        int failedCount = processFetchFailures(currentTimeMs);

        // 2. Classify goals and prepare actions
        ReconciliationActions actions = classifyAndPrepareActions(currentTimeMs);

        // 3. Execute creation/updates
        executeGoalCreations(actions.goalsToCreate, currentTimeMs);

        // 4. Clean up idle fetchers
        cleanupIdleFetchers(actions.activeFetchers);

        // 5. Log reconciliation summary
        if (LOG.isDebugEnabled() && (failedCount > 0 || !actions.goalsToCreate.isEmpty())) {
            LOG.debug(
                    "Reconciliation completed: processed {} failures, {} fetchers to create/update, {} active fetchers",
                    failedCount,
                    actions.goalsToCreate.size(),
                    actions.activeFetchers.size());
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

                Map<TableBucket, IndexFetcherGoal> goalsForIndexBucket =
                        indexBucketGoals.get(indexBucket);
                if (goalsForIndexBucket != null) {
                    IndexFetcherGoal goal = goalsForIndexBucket.get(dataBucket);
                    if (goal != null && goal.getState() == IndexFetcherGoal.State.RUNNING) {
                        LOG.info(
                                "Fetch failure for data-index bucket {}, marking for retry. Reason: {}",
                                dataIndexBucket,
                                reason);
                        goal.markFailed(reason);
                        goal.setLastAttemptTimestamp(currentTimeMs);

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
     * Classify all goals and prepare actions for reconciliation. Returns a ReconciliationActions
     * object containing goals to create and active fetchers.
     */
    @GuardedBy("lock")
    private ReconciliationActions classifyAndPrepareActions(long currentTimeMs) {
        Map<ServerIdAndFetcherId, List<IndexFetcherGoal>> goalsToCreate = new HashMap<>();
        Set<ServerIdAndFetcherId> activeFetchers = new HashSet<>();
        Map<TableBucket, Integer> leaderCache = new HashMap<>();

        // Iterate through all goals and classify them
        for (Map.Entry<TableBucket, Map<TableBucket, IndexFetcherGoal>> entry :
                indexBucketGoals.entrySet()) {
            for (IndexFetcherGoal goal : entry.getValue().values()) {

                switch (goal.getState()) {
                    case PENDING:
                    case FAILED:
                        handlePendingOrFailedGoal(goal, goalsToCreate, leaderCache, currentTimeMs);
                        break;

                    case RUNNING:
                        handleRunningGoal(goal, activeFetchers);
                        break;

                    case CREATING:
                        handleCreatingGoal(goal, currentTimeMs);
                        break;
                }
            }
        }

        return new ReconciliationActions(goalsToCreate, activeFetchers);
    }

    /**
     * Handle a goal in PENDING or FAILED state. May add it to goalsToCreate if ready for
     * (re)creation.
     */
    @GuardedBy("lock")
    private void handlePendingOrFailedGoal(
            IndexFetcherGoal goal,
            Map<ServerIdAndFetcherId, List<IndexFetcherGoal>> goalsToCreate,
            Map<TableBucket, Integer> leaderCache,
            long currentTimeMs) {

        // Check if ready for retry
        if (!goal.isReadyForRetry(currentTimeMs, fetchBackoffMs)) {
            return;
        }

        int currentLeaderServerId;
        boolean queriedFromZK = false;

        // For FAILED goals, check if metadata cache might be stale
        if (goal.getState() == IndexFetcherGoal.State.FAILED
                && goal.getLastKnownLeaderServerId() != INVALID_LEADER_ID) {
            // First, try to get leader from cache
            int cachedLeader =
                    leaderCache.computeIfAbsent(
                            goal.getDataBucket(), bucket -> queryDataBucketLeader(bucket, false));

            // If cached leader is the same as the last known leader we failed with,
            // the cache might be stale. Query directly from ZooKeeper to get fresh data.
            if (cachedLeader == goal.getLastKnownLeaderServerId()) {
                LOG.debug(
                        "Cached leader {} for data bucket {} matches last failed leader, querying ZooKeeper for fresh leader info",
                        cachedLeader,
                        goal.getDataBucket());
                int zkLeader = queryDataBucketLeader(goal.getDataBucket(), true);
                if (zkLeader != INVALID_LEADER_ID && zkLeader != cachedLeader) {
                    LOG.info(
                            "Found different leader in ZooKeeper for data bucket {}: ZK has {}, cache had {}. Cache was stale.",
                            goal.getDataBucket(),
                            zkLeader,
                            cachedLeader);
                    currentLeaderServerId = zkLeader;
                    // Update the leader cache with fresh data
                    leaderCache.put(goal.getDataBucket(), zkLeader);
                    queriedFromZK = true;
                } else {
                    currentLeaderServerId = cachedLeader;
                }
            } else {
                currentLeaderServerId = cachedLeader;
            }
        } else {
            // For PENDING goals, just use the cache
            currentLeaderServerId =
                    leaderCache.computeIfAbsent(
                            goal.getDataBucket(), bucket -> queryDataBucketLeader(bucket, false));
        }

        if (currentLeaderServerId < 0) {
            // Leader not available yet, mark as failed and retry later
            String reason =
                    "Data bucket "
                            + goal.getDataBucket()
                            + " leader not available"
                            + (queriedFromZK ? " (verified from ZooKeeper)" : "");
            if (goal.getState() == IndexFetcherGoal.State.PENDING) {
                LOG.debug("{}, will retry", reason);
                goal.markFailed(reason);
                goal.setLastAttemptTimestamp(currentTimeMs);
            } else {
                // Already failed, just update timestamp
                goal.setLastAttemptTimestamp(currentTimeMs);
            }
            return;
        }

        // Update last known leader
        goal.setLastKnownLeaderServerId(currentLeaderServerId);

        // Log appropriate message based on state
        if (goal.getState() == IndexFetcherGoal.State.PENDING) {
            LOG.info(
                    "Starting to fetch for data-index bucket {}, leader: {}",
                    goal.getDataIndexTableBucket(),
                    currentLeaderServerId);
        } else {
            LOG.info(
                    "Retrying failed goal (attempt {}) for data-index bucket {}, current leader: {}{}. Last failure: {}",
                    goal.getFailureCount() + 1,
                    goal.getDataIndexTableBucket(),
                    currentLeaderServerId,
                    queriedFromZK ? " (from ZooKeeper)" : "",
                    goal.getLastFailureReason());
        }

        ServerIdAndFetcherId fetcherId =
                new ServerIdAndFetcherId(currentLeaderServerId, getFetcherId(goal.getDataBucket()));
        goalsToCreate.computeIfAbsent(fetcherId, k -> new ArrayList<>()).add(goal);
    }

    /** Handle a goal in RUNNING state. Tracks it as an active fetcher. */
    @GuardedBy("lock")
    private void handleRunningGoal(
            IndexFetcherGoal goal, Set<ServerIdAndFetcherId> activeFetchers) {
        if (hasValidLeader(goal)) {
            // Check if the leader has changed
            int currentLeader = queryDataBucketLeader(goal.getDataBucket(), false);
            int lastKnownLeader = goal.getLastKnownLeaderServerId();

            // If we can't query the current leader (returns INVALID_LEADER_ID),
            // it might mean the leader has changed but metadata cache hasn't been updated yet.
            // In this case, we should also mark the goal as failed to trigger reconnection.
            if (currentLeader == INVALID_LEADER_ID) {
                LOG.info(
                        "Data bucket {} leader information not available in metadata cache (last known leader: {}), marking goal as failed to trigger reconnection",
                        goal.getDataBucket(),
                        lastKnownLeader);
                goal.markFailed(
                        String.format(
                                "Data bucket leader information not available (last known: %d)",
                                lastKnownLeader));
                goal.setLastAttemptTimestamp(System.currentTimeMillis());

                // Remove from current fetcher
                removeFromCurrentFetcher(goal.getDataIndexTableBucket());
                return;
            }

            if (currentLeader != lastKnownLeader) {
                // Leader has changed, mark goal as failed to trigger reconnection
                LOG.info(
                        "Data bucket {} leader changed from {} to {}, marking goal as failed to trigger reconnection",
                        goal.getDataBucket(),
                        lastKnownLeader,
                        currentLeader);
                goal.markFailed(
                        String.format(
                                "Data bucket leader changed from %d to %d",
                                lastKnownLeader, currentLeader));
                goal.setLastAttemptTimestamp(System.currentTimeMillis());

                // Remove from current fetcher
                removeFromCurrentFetcher(goal.getDataIndexTableBucket());
                return;
            }

            ServerIdAndFetcherId fetcherId =
                    new ServerIdAndFetcherId(
                            goal.getLastKnownLeaderServerId(), getFetcherId(goal.getDataBucket()));
            activeFetchers.add(fetcherId);
        }
    }

    /** Handle a goal in CREATING state. Checks for timeout and resets if stuck. */
    @GuardedBy("lock")
    private void handleCreatingGoal(IndexFetcherGoal goal, long currentTimeMs) {
        if (goal.isCreatingTimeout(currentTimeMs, CREATING_STATE_TIMEOUT_MS)) {
            LOG.warn(
                    "Goal stuck in CREATING state for data-index bucket {}, resetting to FAILED",
                    goal.getDataIndexTableBucket());
            goal.markFailed("Creation timeout after " + CREATING_STATE_TIMEOUT_MS + "ms");
            goal.setLastAttemptTimestamp(currentTimeMs);
        }
    }

    /** Execute goal creations/updates for all prepared goals. */
    @GuardedBy("lock")
    private void executeGoalCreations(
            Map<ServerIdAndFetcherId, List<IndexFetcherGoal>> goalsToCreate, long currentTimeMs) {
        for (Map.Entry<ServerIdAndFetcherId, List<IndexFetcherGoal>> entry :
                goalsToCreate.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            List<IndexFetcherGoal> goals = entry.getValue();

            try {
                createOrUpdateFetcher(fetcherId, goals, currentTimeMs);
            } catch (Exception e) {
                LOG.error(
                        "Failed to create/update fetcher for server {} fetcher {}",
                        fetcherId.serverId,
                        fetcherId.fetcherId,
                        e);
                // Mark all goals as failed
                for (IndexFetcherGoal goal : goals) {
                    goal.markFailed("Failed to create fetcher: " + e.getMessage());
                    goal.setLastAttemptTimestamp(currentTimeMs);
                }
            }
        }
    }

    /** Check if a goal has a valid leader server ID. */
    private boolean hasValidLeader(IndexFetcherGoal goal) {
        return goal.getLastKnownLeaderServerId() > INVALID_LEADER_ID;
    }

    /** Helper class to hold reconciliation actions. */
    private static class ReconciliationActions {
        final Map<ServerIdAndFetcherId, List<IndexFetcherGoal>> goalsToCreate;
        final Set<ServerIdAndFetcherId> activeFetchers;

        ReconciliationActions(
                Map<ServerIdAndFetcherId, List<IndexFetcherGoal>> goalsToCreate,
                Set<ServerIdAndFetcherId> activeFetchers) {
            this.goalsToCreate = goalsToCreate;
            this.activeFetchers = activeFetchers;
        }
    }

    /**
     * Query the current leader server ID for a data bucket. This method supports querying from
     * either local metadata cache or directly from ZooKeeper.
     *
     * @param dataBucket the data bucket to query
     * @param fromZK if true, query from ZooKeeper and update cache; if false, query from local
     *     cache
     * @return the leader server ID, or INVALID_LEADER_ID if leader is not available
     */
    private int queryDataBucketLeader(TableBucket dataBucket, boolean fromZK) {
        try {
            BucketMetadata bucketMetadata = getBucketMetadata(dataBucket, fromZK);

            if (bucketMetadata != null) {
                return bucketMetadata.getLeaderId().orElse(INVALID_LEADER_ID);
            }

            // Leader not available
            if (fromZK) {
                LOG.debug(
                        "Data bucket {} leader not available in ZooKeeper, will retry", dataBucket);
            } else {
                LOG.debug(
                        "Data bucket {} leader not available in metadata cache, will retry",
                        dataBucket);
            }
            return INVALID_LEADER_ID;
        } catch (Exception e) {
            if (fromZK) {
                LOG.warn(
                        "Failed to query data bucket {} leader from ZooKeeper: {}",
                        dataBucket,
                        e.getMessage());
            } else {
                LOG.debug(
                        "Failed to query data bucket {} leader from metadata cache: {}",
                        dataBucket,
                        e.getMessage());
            }
            return INVALID_LEADER_ID;
        }
    }

    /**
     * Get bucket metadata through TabletServerMetadataProvider. Handles both partitioned and
     * non-partitioned tables.
     *
     * @param tableBucket the table bucket to query
     * @param fromZK if true, query from ZooKeeper and update cache; if false, query from local
     *     cache
     * @return the bucket metadata, or null if not found
     */
    private BucketMetadata getBucketMetadata(TableBucket tableBucket, boolean fromZK) {
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId == null) {
            // Non-partitioned table
            Optional<TablePath> tablePathOpt =
                    metadataProvider.getTablePathFromCache(tableBucket.getTableId());
            if (!tablePathOpt.isPresent()) {
                return null;
            }

            Optional<TableMetadata> tableMetadataOpt;
            if (fromZK) {
                // Query from ZooKeeper and update cache
                java.util.List<TableMetadata> tableMetadataList =
                        metadataProvider.getTablesMetadataFromZK(
                                java.util.Collections.singleton(tablePathOpt.get()));
                tableMetadataOpt =
                        tableMetadataList.isEmpty()
                                ? Optional.empty()
                                : Optional.of(tableMetadataList.get(0));
            } else {
                // Query from cache
                tableMetadataOpt = metadataProvider.getTableMetadataFromCache(tablePathOpt.get());
            }

            if (!tableMetadataOpt.isPresent()) {
                return null;
            }

            // Find the specific bucket
            for (BucketMetadata bucketMetadata : tableMetadataOpt.get().getBucketMetadataList()) {
                if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                    return bucketMetadata;
                }
            }
            return null;
        } else {
            // Partitioned table
            Optional<org.apache.fluss.metadata.PhysicalTablePath> physicalPathOpt =
                    metadataProvider.getPhysicalTablePathFromCache(partitionId);
            if (!physicalPathOpt.isPresent()) {
                return null;
            }

            Optional<PartitionMetadata> partitionMetadataOpt;
            if (fromZK) {
                // Query from ZooKeeper and update cache
                java.util.List<PartitionMetadata> partitionMetadataList =
                        metadataProvider.getPartitionsMetadataFromZK(
                                java.util.Collections.singleton(physicalPathOpt.get()));
                partitionMetadataOpt =
                        partitionMetadataList.isEmpty()
                                ? Optional.empty()
                                : Optional.of(partitionMetadataList.get(0));
            } else {
                // Query from cache
                partitionMetadataOpt =
                        metadataProvider.getPartitionMetadataFromCache(physicalPathOpt.get());
            }

            if (!partitionMetadataOpt.isPresent()) {
                return null;
            }

            // Find the specific bucket
            for (BucketMetadata bucketMetadata :
                    partitionMetadataOpt.get().getBucketMetadataList()) {
                if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                    return bucketMetadata;
                }
            }
            return null;
        }
    }

    /**
     * Create or update a fetcher thread for the given goals. The fetcherId contains the dynamically
     * queried leader server ID. This method should be called while holding the lock.
     *
     * @param fetcherId the target server and fetcher ID (server ID was dynamically queried)
     * @param goals the list of goals to be achieved by this fetcher
     * @param currentTimeMs current timestamp
     */
    @GuardedBy("lock")
    private void createOrUpdateFetcher(
            ServerIdAndFetcherId fetcherId, List<IndexFetcherGoal> goals, long currentTimeMs)
            throws Exception {

        // Mark goals as creating
        for (IndexFetcherGoal goal : goals) {
            goal.setState(IndexFetcherGoal.State.CREATING);
            goal.setLastAttemptTimestamp(currentTimeMs);
        }

        // Get or create fetcher thread
        IndexFetcherThread fetcherThread = indexFetcherThreadMap.get(fetcherId);

        if (fetcherThread == null) {
            // Create new fetcher thread
            fetcherThread = createIndexFetcherThread(fetcherId.fetcherId, fetcherId.serverId);
            indexFetcherThreadMap.put(fetcherId, fetcherThread);
            fetcherThread.start();
            LOG.info(
                    "Created new IndexFetcherThread for leader {} fetcher {} with {} goals",
                    fetcherId.serverId,
                    fetcherId.fetcherId,
                    goals.size());
        } else {
            // Verify fetcher is still targeting the same leader
            if (fetcherThread.getLeader().leaderServerId() != fetcherId.serverId) {
                LOG.warn(
                        "Fetcher thread leader mismatch: expected {}, actual {}. Recreating fetcher.",
                        fetcherId.serverId,
                        fetcherThread.getLeader().leaderServerId());
                try {
                    fetcherThread.shutdown();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while shutting down fetcher thread", e);
                }
                indexFetcherThreadMap.remove(fetcherId);

                // Create new fetcher thread
                fetcherThread = createIndexFetcherThread(fetcherId.fetcherId, fetcherId.serverId);
                indexFetcherThreadMap.put(fetcherId, fetcherThread);
                fetcherThread.start();
            }
        }

        // Prepare initial fetch status for all goals
        // Use the dynamically queried leader server ID from fetcherId
        Map<DataIndexTableBucket, IndexInitialFetchStatus> initialFetchStatusMap = new HashMap<>();
        for (IndexFetcherGoal goal : goals) {
            // Query current state from IndexApplier's BucketStateManager
            IndexApplier.IndexApplyStatus applyStatus =
                    goal.getIndexApplier().getOrInitIndexApplyStatus(goal.getDataBucket());

            IndexInitialFetchStatus fetchStatus =
                    new IndexInitialFetchStatus(
                            goal.getIndexBucket().getTableId(),
                            goal.getIndexTablePath(),
                            // Use dynamically queried leader ID
                            fetcherId.serverId,
                            // Fetch from persisted
                            applyStatus.getLastApplyRecordsDataEndOffset(),
                            // state
                            // Index commit offset
                            applyStatus.getIndexCommitDataOffset(),
                            goal.getIndexApplier());
            initialFetchStatusMap.put(goal.getDataIndexTableBucket(), fetchStatus);
        }

        // Add buckets to fetcher thread
        try {
            fetcherThread.addIndexBuckets(initialFetchStatusMap);

            // Mark goals as running
            for (IndexFetcherGoal goal : goals) {
                goal.markRunning();
                dataIndexBucketToFetcher.put(goal.getDataIndexTableBucket(), fetcherId);
                LOG.debug(
                        "Successfully started fetching for data-index bucket {}",
                        goal.getDataIndexTableBucket());
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while adding buckets to fetcher thread", e);
            // Mark goals as failed
            for (IndexFetcherGoal goal : goals) {
                goal.markFailed("Interrupted while adding to fetcher: " + e.getMessage());
            }
            throw e;
        }
    }

    /** Clean up fetcher threads that have no running goals. */
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
                            fetcherId.serverId,
                            fetcherId.fetcherId);
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

    /** Helper class to represent server id and fetcher id. */
    public static class ServerIdAndFetcherId {
        private final int serverId;
        private final int fetcherId;

        public ServerIdAndFetcherId(int serverId, int fetcherId) {
            this.serverId = serverId;
            this.fetcherId = fetcherId;
        }

        public int getServerId() {
            return serverId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ServerIdAndFetcherId that = (ServerIdAndFetcherId) o;
            return serverId == that.serverId && fetcherId == that.fetcherId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverId, fetcherId);
        }

        @Override
        public String toString() {
            return "ServerIdAndFetcherId{serverId=" + serverId + ", fetcherId=" + fetcherId + '}';
        }
    }

    /** Helper class to pass goal information when adding goals. */
    public static class GoalInfo {
        private final TablePath indexTablePath;
        private final IndexApplier indexApplier;

        public GoalInfo(TablePath indexTablePath, IndexApplier indexApplier) {
            this.indexTablePath = indexTablePath;
            this.indexApplier = indexApplier;
        }

        public TablePath getIndexTablePath() {
            return indexTablePath;
        }
    }
}
