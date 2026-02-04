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
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Manages index fetcher threads using a target-state driven approach with auto reconciliation.
 *
 * <h2>Architecture Overview</h2>
 *
 * <p>The IndexFetcherManager uses a declarative target-state model:
 *
 * <ul>
 *   <li><b>Target State</b>: What we want (IndexBucket should fetch from DataBuckets)
 *   <li><b>Actual State</b>: Running fetcher threads
 *   <li><b>Reconciliation</b>: Background thread that continuously aligns actual with target
 * </ul>
 *
 * <h2>Key Components</h2>
 *
 * <ul>
 *   <li>{@link IndexFetcherTarget}: Represents a single fetch target (DataBucket -> IndexBucket)
 *   <li>{@link IndexFetcherThread}: Worker thread that fetches index data from a leader server
 *   <li>{@link FetcherReconciliationThread}: Background thread for state reconciliation
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>All public methods are thread-safe. Internal state is protected by a single lock object.
 */
@ThreadSafe
public class IndexFetcherManager {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFetcherManager.class);

    // ==================== Constants ====================

    /** Reconciliation interval in milliseconds. */
    private static final long RECONCILIATION_INTERVAL_MS = 5000L;

    /** Timeout for targets stuck in CREATING state. */
    private static final long CREATING_TIMEOUT_MS = 30000L;

    /** Invalid leader server ID marker. */
    private static final int INVALID_LEADER_ID = -1;

    // ==================== Dependencies ====================

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final int serverId;
    private final ReplicaManager replicaManager;
    private final TabletServerMetadataCache metadataCache;
    private final ZooKeeperClient zkClient;
    private final int numFetchersPerServer;
    private final Function<Integer, Optional<ServerNode>> serverNodeCache;
    private final long fetchBackoffMs;

    // ==================== State ====================

    /**
     * Target state: IndexBucket -> (DataBucket -> Target). Represents all fetch targets we need to
     * achieve.
     */
    @GuardedBy("lock")
    private final Map<TableBucket, Map<TableBucket, IndexFetcherTarget>> targets = new HashMap<>();

    /** Actual state: Running fetcher threads keyed by (serverId, fetcherId). */
    @GuardedBy("lock")
    private final Map<ServerIdAndFetcherId, IndexFetcherThread> fetcherThreads = new HashMap<>();

    /** Mapping from DataIndexTableBucket to its current fetcher thread. */
    @GuardedBy("lock")
    private final Map<DataIndexTableBucket, ServerIdAndFetcherId> bucketToFetcher = new HashMap<>();

    private final Object lock = new Object();
    private volatile boolean running = false;
    private FetcherReconciliationThread reconciliationThread;

    // ==================== Constructor ====================

    public IndexFetcherManager(
            Configuration conf,
            RpcClient rpcClient,
            int serverId,
            ReplicaManager replicaManager,
            TabletServerMetadataCache metadataCache,
            ZooKeeperClient zkClient,
            Function<Integer, Optional<ServerNode>> serverNodeCache) {
        this.conf = conf;
        this.rpcClient = rpcClient;
        this.serverId = serverId;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.zkClient = zkClient;
        this.numFetchersPerServer = conf.getInt(ConfigOptions.LOG_REPLICA_FETCHER_NUMBER);
        this.serverNodeCache = serverNodeCache;
        this.fetchBackoffMs = conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL).toMillis();
    }

    // ==================== Lifecycle ====================

    /** Starts the reconciliation background thread. */
    public void startup() {
        synchronized (lock) {
            if (running) {
                LOG.warn("IndexFetcherManager already running");
                return;
            }
            running = true;
            reconciliationThread = new FetcherReconciliationThread();
            reconciliationThread.start();
            LOG.info(
                    "IndexFetcherManager started (reconciliation interval: {}ms)",
                    RECONCILIATION_INTERVAL_MS);
        }
    }

    /** Shuts down the manager and all fetcher threads. */
    public void shutdown() throws InterruptedException {
        LOG.info("Shutting down IndexFetcherManager");

        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }

        if (reconciliationThread != null) {
            reconciliationThread.shutdown();
            reconciliationThread = null;
        }

        shutdownAllFetchers();

        synchronized (lock) {
            targets.clear();
            bucketToFetcher.clear();
        }

        LOG.info("IndexFetcherManager shutdown complete");
    }

    // ==================== Public API ====================

    /**
     * Adds or updates fetch targets for an index bucket.
     *
     * @param indexBucket the index bucket that became leader
     * @param targetInfos map of DataIndexTableBucket to its target info
     */
    public void addOrUpdateTargetsForIndexBucket(
            TableBucket indexBucket, Map<DataIndexTableBucket, FetcherTargetInfo> targetInfos) {
        if (targetInfos.isEmpty()) {
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> indexTargets =
                    targets.computeIfAbsent(indexBucket, k -> new HashMap<>());

            for (Map.Entry<DataIndexTableBucket, FetcherTargetInfo> entry :
                    targetInfos.entrySet()) {
                DataIndexTableBucket dataIndexBucket = entry.getKey();
                FetcherTargetInfo info = entry.getValue();
                TableBucket dataBucket = dataIndexBucket.getDataBucket();

                IndexFetcherTarget existing = indexTargets.get(dataBucket);
                if (existing != null) {
                    // Reset failed targets to pending for retry
                    if (existing.getState() == IndexFetcherTarget.State.FAILED) {
                        existing.resetToPending();
                        LOG.debug("Reset failed target to pending: {}", dataIndexBucket);
                    }
                } else {
                    IndexFetcherTarget newTarget =
                            new IndexFetcherTarget(
                                    dataIndexBucket, info.indexTablePath, info.indexApplier);
                    indexTargets.put(dataBucket, newTarget);
                    LOG.info("Added new target: {}", dataIndexBucket);
                }
            }

            lock.notifyAll(); // Trigger immediate reconciliation
        }

        LOG.info("Added/updated {} targets for index bucket {}", targetInfos.size(), indexBucket);
    }

    /**
     * Removes all targets for an index bucket.
     *
     * @param indexBucket the index bucket to remove
     */
    public void removeIndexBucket(TableBucket indexBucket) {
        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> removed = targets.remove(indexBucket);
            if (removed != null && !removed.isEmpty()) {
                removeTargetsFromFetchers(removed.values());
                LOG.info("Removed {} targets for index bucket {}", removed.size(), indexBucket);
                lock.notifyAll();
            }
        }
    }

    /**
     * Removes targets for specific index buckets.
     *
     * @param indexBuckets set of index buckets to remove
     */
    public void removeIndexBuckets(Set<TableBucket> indexBuckets) {
        if (indexBuckets.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (TableBucket indexBucket : indexBuckets) {
                Map<TableBucket, IndexFetcherTarget> removed = targets.remove(indexBucket);
                if (removed != null && !removed.isEmpty()) {
                    removeTargetsFromFetchers(removed.values());
                }
            }
            lock.notifyAll();
        }
    }

    /**
     * Removes specific data bucket targets from an index bucket.
     *
     * @param indexBucket the index bucket
     * @param dataBucketsToRemove data buckets to remove
     */
    public void removeDataBucketsFromIndexBucket(
            TableBucket indexBucket, Set<TableBucket> dataBucketsToRemove) {
        if (dataBucketsToRemove.isEmpty()) {
            return;
        }

        synchronized (lock) {
            Map<TableBucket, IndexFetcherTarget> indexTargets = targets.get(indexBucket);
            if (indexTargets == null) {
                return;
            }

            List<IndexFetcherTarget> removedTargets = new ArrayList<>();
            for (TableBucket dataBucket : dataBucketsToRemove) {
                IndexFetcherTarget removed = indexTargets.remove(dataBucket);
                if (removed != null) {
                    removedTargets.add(removed);
                }
            }

            if (!removedTargets.isEmpty()) {
                removeTargetsFromFetchers(removedTargets);
                LOG.info(
                        "Removed {} data bucket targets from index bucket {}",
                        removedTargets.size(),
                        indexBucket);
            }

            if (indexTargets.isEmpty()) {
                targets.remove(indexBucket);
            }

            lock.notifyAll();
        }
    }

    // ==================== Reconciliation ====================

    /** Background thread for continuous state reconciliation. */
    private class FetcherReconciliationThread extends ShutdownableThread {
        FetcherReconciliationThread() {
            super("IndexFetcherReconciliation", true);
        }

        @Override
        public void doWork() {
            List<IndexFetcherThread> threadsToShutdown = null;
            try {
                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                    threadsToShutdown = reconcile();
                }

                // Shutdown old threads outside of lock to avoid blocking other operations
                // This is done before wait() to clean up resources promptly
                if (threadsToShutdown != null && !threadsToShutdown.isEmpty()) {
                    for (IndexFetcherThread thread : threadsToShutdown) {
                        try {
                            thread.shutdown();
                            LOG.debug("Successfully shut down old fetcher thread");
                        } catch (InterruptedException e) {
                            LOG.warn("Interrupted while shutting down fetcher thread", e);
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }

                // Wait for next reconciliation cycle
                synchronized (lock) {
                    if (running) {
                        lock.wait(RECONCILIATION_INTERVAL_MS);
                    }
                }
            } catch (InterruptedException e) {
                LOG.debug("Reconciliation thread interrupted");
            } catch (Exception e) {
                LOG.error("Error during reconciliation", e);
            }
        }
    }

    /**
     * Main reconciliation logic. Must be called while holding the lock.
     *
     * @return list of old threads that need to be shut down outside of lock
     */
    @GuardedBy("lock")
    private List<IndexFetcherThread> reconcile() {
        long now = System.currentTimeMillis();

        // Step 1: Process failures reported by fetcher threads
        int failureCount = processFailedBuckets(now);

        // Step 2: Classify targets and determine actions
        Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> toCreate = new HashMap<>();
        classifyTargets(now, toCreate);

        // Step 3: Execute creations (may collect threads to shutdown)
        List<IndexFetcherThread> threadsToShutdown = executeCreations(toCreate, now);

        if (failureCount > 0 || !toCreate.isEmpty()) {
            LOG.info(
                    "Reconciliation: {} failures processed, {} targets to create, {} fetcher threads",
                    failureCount,
                    toCreate.values().stream().mapToInt(List::size).sum(),
                    fetcherThreads.size());
        }

        return threadsToShutdown;
    }

    /** Processes failures reported by fetcher threads. */
    @GuardedBy("lock")
    private int processFailedBuckets(long now) {
        int count = 0;
        for (IndexFetcherThread thread : fetcherThreads.values()) {
            Map<DataIndexTableBucket, String> failures = thread.pollInvalidBuckets();
            for (Map.Entry<DataIndexTableBucket, String> entry : failures.entrySet()) {
                DataIndexTableBucket bucket = entry.getKey();
                String reason = entry.getValue();

                IndexFetcherTarget target = findTarget(bucket);
                if (target != null) {
                    target.markFailed(reason, now);
                    LOG.info("Target {} failed: {}", bucket, reason);
                    count++;
                }
            }
        }
        return count;
    }

    /** Classifies all targets and prepares creation actions. */
    @GuardedBy("lock")
    private void classifyTargets(
            long now, Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> toCreate) {

        for (Map<TableBucket, IndexFetcherTarget> indexTargets : targets.values()) {
            for (IndexFetcherTarget target : indexTargets.values()) {
                switch (target.getState()) {
                    case PENDING:
                    case FAILED:
                        handlePendingOrFailed(target, toCreate, now);
                        break;
                    case RUNNING:
                        handleRunning(target, now);
                        break;
                    case CREATING:
                        handleCreating(target, now);
                        break;
                }
            }
        }
    }

    /** Handles a target in PENDING or FAILED state. */
    @GuardedBy("lock")
    private void handlePendingOrFailed(
            IndexFetcherTarget target,
            Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> toCreate,
            long now) {

        if (!target.isReadyForRetry(now)) {
            return;
        }

        // Query leader
        LeaderInfo leaderInfo = queryLeader(target);
        if (!leaderInfo.isValid()) {
            target.markFailed(leaderInfo.reason, now);
            return;
        }

        int leaderId = leaderInfo.leaderId;
        target.setLastKnownLeaderServerId(leaderId);

        // Log state transition
        if (target.getState() == IndexFetcherTarget.State.PENDING) {
            LOG.info(
                    "Starting fetch for {}, leader: {}",
                    target.getDataIndexTableBucket(),
                    leaderId);
        } else {
            LOG.info(
                    "Retrying {} (attempt {}), leader: {}, last failure: {}",
                    target.getDataIndexTableBucket(),
                    target.getFailureCount() + 1,
                    leaderId,
                    target.getLastFailureReason());
        }

        // Schedule for creation
        ServerIdAndFetcherId fetcherId =
                new ServerIdAndFetcherId(leaderId, computeFetcherId(target.getDataBucket()));
        toCreate.computeIfAbsent(fetcherId, k -> new ArrayList<>()).add(target);
    }

    /** Handles a target in RUNNING state - checks for leader changes. */
    @GuardedBy("lock")
    private void handleRunning(IndexFetcherTarget target, long now) {
        int lastLeader = target.getLastKnownLeaderServerId();
        if (lastLeader == INVALID_LEADER_ID) {
            return;
        }

        Optional<Integer> leaderIdOpt = metadataCache.getBucketLeaderId(target.getDataBucket());

        if (!leaderIdOpt.isPresent() || leaderIdOpt.get() == INVALID_LEADER_ID) {
            LOG.info("Leader unavailable for {}, marking failed", target.getDataBucket());
            target.markFailed("Leader unavailable in cache", now);
            removeFromFetcher(target.getDataIndexTableBucket());
        } else if (leaderIdOpt.get() != lastLeader) {
            LOG.info(
                    "Leader changed for {} ({} -> {}), marking failed",
                    target.getDataBucket(),
                    lastLeader,
                    leaderIdOpt.get());
            target.markFailed(
                    String.format("Leader changed from %d to %d", lastLeader, leaderIdOpt.get()),
                    now);
            removeFromFetcher(target.getDataIndexTableBucket());
        }
    }

    /** Handles a target in CREATING state - checks for timeout. */
    @GuardedBy("lock")
    private void handleCreating(IndexFetcherTarget target, long now) {
        if (target.isCreatingTimeout(now, CREATING_TIMEOUT_MS)) {
            LOG.warn(
                    "Target {} stuck in CREATING, marking failed",
                    target.getDataIndexTableBucket());
            target.markFailed("Creation timeout", now);
        }
    }

    /**
     * Executes fetcher creations for prepared targets. Returns threads that need to be shut down.
     */
    @GuardedBy("lock")
    private List<IndexFetcherThread> executeCreations(
            Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> toCreate, long now) {

        List<IndexFetcherThread> threadsToShutdown = new ArrayList<>();

        for (Map.Entry<ServerIdAndFetcherId, List<IndexFetcherTarget>> entry :
                toCreate.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            List<IndexFetcherTarget> targetList = entry.getValue();

            try {
                IndexFetcherThread oldThread = createOrUpdateFetcher(fetcherId, targetList, now);
                if (oldThread != null) {
                    threadsToShutdown.add(oldThread);
                }
            } catch (Exception e) {
                LOG.error("Failed to create fetcher for {}", fetcherId, e);
                for (IndexFetcherTarget target : targetList) {
                    target.markFailed("Fetcher creation failed: " + e.getMessage(), now);
                }
            }
        }

        return threadsToShutdown;
    }

    // ==================== Leader Query ====================

    /** Result of a leader query. */
    private static class LeaderInfo {
        final int leaderId;
        final String reason;

        LeaderInfo(int leaderId, String reason) {
            this.leaderId = leaderId;
            this.reason = reason;
        }

        boolean isValid() {
            return leaderId > INVALID_LEADER_ID;
        }

        static LeaderInfo valid(int leaderId) {
            return new LeaderInfo(leaderId, null);
        }

        static LeaderInfo invalid(String reason) {
            return new LeaderInfo(INVALID_LEADER_ID, reason);
        }
    }

    /** Queries the leader for a target's data bucket. */
    @GuardedBy("lock")
    private LeaderInfo queryLeader(IndexFetcherTarget target) {
        Optional<Integer> leaderIdOpt;
        boolean fromZK = false;

        if (target.getState() == IndexFetcherTarget.State.FAILED) {
            // For failed targets, try cache first, then ZK if same leader or not found
            leaderIdOpt = metadataCache.getBucketLeaderId(target.getDataBucket());
            if (!leaderIdOpt.isPresent()
                    || leaderIdOpt.get() == target.getLastKnownLeaderServerId()) {
                // Query ZK directly for fresh leader info
                try {
                    leaderIdOpt =
                            zkClient.getLeaderAndIsr(target.getDataBucket())
                                    .map(leaderAndIsr -> leaderAndIsr.leader());
                    fromZK = true;
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to query leader from ZK for {}: {}",
                            target.getDataBucket(),
                            e.getMessage());
                    return LeaderInfo.invalid("ZK query failed: " + e.getMessage());
                }
            }
        } else {
            leaderIdOpt = metadataCache.getBucketLeaderId(target.getDataBucket());
        }

        if (!leaderIdOpt.isPresent()) {
            String reason =
                    String.format(
                            "Leader not found for %s%s",
                            target.getDataBucket(), fromZK ? " (from ZK)" : "");
            return LeaderInfo.invalid(reason);
        }

        int leaderId = leaderIdOpt.get();
        if (leaderId == INVALID_LEADER_ID) {
            return LeaderInfo.invalid("No leader elected for " + target.getDataBucket());
        }

        return LeaderInfo.valid(leaderId);
    }

    // ==================== Fetcher Management ====================

    /**
     * Creates or updates a fetcher thread for the given targets.
     *
     * @return the old thread that needs to be shut down (outside of lock), or null if no shutdown
     *     needed
     */
    @GuardedBy("lock")
    private IndexFetcherThread createOrUpdateFetcher(
            ServerIdAndFetcherId fetcherId, List<IndexFetcherTarget> targetList, long now)
            throws Exception {

        IndexFetcherThread threadToShutdown = null;

        // Mark targets as creating
        for (IndexFetcherTarget target : targetList) {
            target.setState(IndexFetcherTarget.State.CREATING, now);
        }

        // Get or create fetcher thread
        IndexFetcherThread thread = fetcherThreads.get(fetcherId);
        if (thread == null) {
            thread = createFetcherThread(fetcherId);
            fetcherThreads.put(fetcherId, thread);
            thread.start();
            LOG.info("Created fetcher thread for {}", fetcherId);
        } else if (thread.getLeader().leaderServerId() != fetcherId.getServerId()) {
            // Leader mismatch - need to recreate thread
            // Collect old thread for shutdown outside of lock to avoid blocking
            LOG.warn("Fetcher leader mismatch, recreating: {}", fetcherId);
            threadToShutdown = thread;
            fetcherThreads.remove(fetcherId);
            thread = createFetcherThread(fetcherId);
            fetcherThreads.put(fetcherId, thread);
            thread.start();
        }

        // Add buckets to thread
        Map<DataIndexTableBucket, IndexInitialFetchStatus> statusMap = new HashMap<>();
        for (IndexFetcherTarget target : targetList) {
            statusMap.put(
                    target.getDataIndexTableBucket(),
                    new IndexInitialFetchStatus(
                            target.getIndexTablePath(), target.getIndexApplier()));
        }

        thread.addIndexBuckets(statusMap);

        // Mark targets as running
        for (IndexFetcherTarget target : targetList) {
            target.markRunning();
            bucketToFetcher.put(target.getDataIndexTableBucket(), fetcherId);
            LOG.info("Started fetching for {}", target.getDataIndexTableBucket());
        }

        return threadToShutdown;
    }

    /** Removes a bucket from its current fetcher. */
    @GuardedBy("lock")
    private void removeFromFetcher(DataIndexTableBucket bucket) {
        ServerIdAndFetcherId fetcherId = bucketToFetcher.remove(bucket);
        if (fetcherId != null) {
            IndexFetcherThread thread = fetcherThreads.get(fetcherId);
            if (thread != null) {
                thread.removeIf(b -> b.equals(bucket));
            }
        }
    }

    /** Removes multiple targets from their fetchers. */
    @GuardedBy("lock")
    private void removeTargetsFromFetchers(Iterable<IndexFetcherTarget> targetsToRemove) {
        Map<ServerIdAndFetcherId, Set<DataIndexTableBucket>> byFetcher = new HashMap<>();

        for (IndexFetcherTarget target : targetsToRemove) {
            DataIndexTableBucket bucket = target.getDataIndexTableBucket();
            ServerIdAndFetcherId fetcherId = bucketToFetcher.remove(bucket);
            if (fetcherId != null) {
                byFetcher.computeIfAbsent(fetcherId, k -> new HashSet<>()).add(bucket);
            }
        }

        for (Map.Entry<ServerIdAndFetcherId, Set<DataIndexTableBucket>> entry :
                byFetcher.entrySet()) {
            IndexFetcherThread thread = fetcherThreads.get(entry.getKey());
            if (thread != null) {
                Set<DataIndexTableBucket> buckets = entry.getValue();
                thread.removeIf(buckets::contains);
            }
        }
    }

    /** Shuts down all fetcher threads. */
    private void shutdownAllFetchers() throws InterruptedException {
        List<IndexFetcherThread> threads = new ArrayList<>(fetcherThreads.values());

        // Remove all buckets first
        for (IndexFetcherThread thread : threads) {
            thread.removeIf(b -> true);
        }

        // Initiate shutdown
        for (IndexFetcherThread thread : threads) {
            thread.initiateShutdown();
        }

        // Wait for completion
        for (IndexFetcherThread thread : threads) {
            thread.shutdown();
        }

        fetcherThreads.clear();
    }

    // ==================== Helper Methods ====================

    /** Finds a target by its DataIndexTableBucket. */
    @GuardedBy("lock")
    private IndexFetcherTarget findTarget(DataIndexTableBucket bucket) {
        Map<TableBucket, IndexFetcherTarget> indexTargets = targets.get(bucket.getIndexBucket());
        return indexTargets != null ? indexTargets.get(bucket.getDataBucket()) : null;
    }

    /** Computes the fetcher ID for a bucket. */
    private int computeFetcherId(TableBucket bucket) {
        return Math.abs(bucket.hashCode()) % numFetchersPerServer;
    }

    @VisibleForTesting
    IndexFetcherThread createFetcherThread(ServerIdAndFetcherId fetcherId) {
        String name = "IndexFetcher-" + fetcherId.getFetcherId() + "-" + fetcherId.getServerId();
        LeaderEndpoint endpoint = createLeaderEndpoint(fetcherId.getServerId());
        return new IndexFetcherThread(
                name,
                replicaManager,
                endpoint,
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL).toMillis(),
                conf);
    }

    @VisibleForTesting
    RemoteLeaderEndpoint createLeaderEndpoint(int leaderId) {
        return new RemoteLeaderEndpoint(
                conf,
                serverId,
                leaderId,
                GatewayClientProxy.createGatewayProxy(
                        () ->
                                serverNodeCache
                                        .apply(leaderId)
                                        .orElseThrow(
                                                () ->
                                                        new RuntimeException(
                                                                "Server "
                                                                        + leaderId
                                                                        + " not in cache")),
                        rpcClient,
                        TabletServerGateway.class));
    }

    // ==================== Inner Classes ====================

    /** Information needed to create a fetch target. */
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
}
