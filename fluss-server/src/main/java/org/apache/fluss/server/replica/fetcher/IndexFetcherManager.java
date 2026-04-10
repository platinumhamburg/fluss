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
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
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

    // ==================== Dependencies ====================

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final int serverId;
    private final ReplicaManager replicaManager;
    private final TabletServerMetadataCache metadataCache;
    private final ZooKeeperClient zkClient;
    private final int numFetchersPerServer;
    private final Function<Integer, Optional<ServerNode>> serverNodeCache;

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

        // Snapshot the thread list under lock so we can safely shut them down outside the lock.
        List<IndexFetcherThread> threadsToShutdown;
        synchronized (lock) {
            running = false;
            lock.notifyAll();
            threadsToShutdown = new ArrayList<>(fetcherThreads.values());
        }

        if (reconciliationThread != null) {
            reconciliationThread.shutdown();
            reconciliationThread = null;
        }

        shutdownFetchers(threadsToShutdown);

        synchronized (lock) {
            fetcherThreads.clear();
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
            // Collected inside the lock (Phase 3), executed outside to avoid
            // lock-ordering hazard: manager lock -> fetcher thread lock.
            Map<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                    pendingAdditions = new HashMap<>();
            try {
                // Capture timestamp once for the entire cycle. Phase 2 may take time,
                // so Phase 3's markFailed() timestamps will be slightly stale — this is
                // an accepted trade-off (targets may retry ~seconds sooner than the 5s backoff).
                long now = System.currentTimeMillis();

                // Phase 1: Plan (locked, pure memory)
                ReconciliationPlan plan;
                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                    plan = planReconciliation(now);
                }

                // Phase 2: Resolve (unlocked, batch ZK IO)
                // null = ZK call failed entirely; empty map = no queries needed or success
                Map<TableBucket, LeaderAndIsr> zkResults = Collections.emptyMap();
                if (plan.hasZkQueries()) {
                    zkResults = resolveLeaders(plan);
                }

                // Phase 3: Apply (locked, pure memory)
                // pendingAdditions is populated here but executed outside the lock.
                synchronized (lock) {
                    if (!running) {
                        return;
                    }
                    threadsToShutdown = applyPlanAndExecute(plan, zkResults, now, pendingAdditions);
                }

                // Phase 3.5: Add buckets to fetcher threads (unlocked)
                // This must run before Phase 4 (thread shutdown) because the threads
                // referenced here are still alive — Phase 4 only shuts down *old* threads.
                for (Map.Entry<
                                IndexFetcherThread,
                                Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                        entry : pendingAdditions.entrySet()) {
                    entry.getKey().addIndexBuckets(entry.getValue());
                }

                // Phase 4: Cleanup (unlocked)
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
     * Checks health of all fetcher threads by verifying their bound servers are still alive. If a
     * fetcher thread's server is no longer in the server node cache, all RUNNING targets on that
     * thread are marked as FAILED and removed from the fetcher.
     */
    @GuardedBy("lock")
    private void checkFetcherThreadHealth(long now) {
        for (Map.Entry<ServerIdAndFetcherId, IndexFetcherThread> entry :
                fetcherThreads.entrySet()) {
            int boundServerId = entry.getKey().getServerId();
            if (!serverNodeCache.apply(boundServerId).isPresent()) {
                LOG.warn(
                        "Fetcher thread {} is bound to dead server {}, "
                                + "marking all its RUNNING targets as FAILED",
                        entry.getKey(),
                        boundServerId);

                // Find all RUNNING targets that are assigned to this fetcher thread
                // and mark them as FAILED
                for (Map.Entry<DataIndexTableBucket, ServerIdAndFetcherId> bucketEntry :
                        new ArrayList<>(bucketToFetcher.entrySet())) {
                    if (bucketEntry.getValue().equals(entry.getKey())) {
                        DataIndexTableBucket bucket = bucketEntry.getKey();
                        IndexFetcherTarget target = findTarget(bucket);
                        if (target != null
                                && target.getState() == IndexFetcherTarget.State.RUNNING) {
                            target.markFailed(
                                    String.format(
                                            "Fetcher thread bound to dead server %d",
                                            boundServerId),
                                    now);
                            removeFromFetcher(bucket);
                            LOG.info(
                                    "Marked target {} as FAILED due to dead server {}",
                                    bucket,
                                    boundServerId);
                        }
                    }
                }
            }
        }
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

                    // Invalidate stale cached leader on connection failures so the next
                    // retry falls back to ZooKeeper instead of reusing the bad entry.
                    if (isConnectionFailure(reason)) {
                        invalidateCachedLeader(target.getDataBucket());
                        LOG.info(
                                "Invalidated cached leader for {} due to fetch failure: {}",
                                target.getDataBucket(),
                                reason);
                    }

                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Determines whether a failure reason indicates a connection-level problem (e.g. the target
     * server is unreachable or no longer alive), as opposed to a logical error. Connection failures
     * mean the cached leader is likely stale and should be invalidated.
     */
    private static boolean isConnectionFailure(String reason) {
        if (reason == null) {
            return false;
        }
        String lower = reason.toLowerCase();
        return lower.contains("not in cache")
                || lower.contains("connection")
                || lower.contains("unreachable")
                || lower.contains("dead server")
                || lower.contains("refused")
                || lower.contains("timeout")
                || lower.contains("disconnected");
    }

    /**
     * Phase 1: Plan reconciliation actions. Pure memory operations only. Classifies targets and
     * collects ZK query needs for Phase 2.
     */
    @GuardedBy("lock")
    private ReconciliationPlan planReconciliation(long now) {
        // Step 1: Process failures reported by fetcher threads (unchanged)
        processFailedBuckets(now);

        // Step 2: Health check (unchanged)
        checkFetcherThreadHealth(now);

        // Step 3: Classify targets
        ReconciliationPlan plan = new ReconciliationPlan();

        for (Map<TableBucket, IndexFetcherTarget> indexTargets : targets.values()) {
            for (IndexFetcherTarget target : indexTargets.values()) {
                switch (target.getState()) {
                    case PENDING:
                    case FAILED:
                        planPendingOrFailed(target, plan, now);
                        break;
                    case RUNNING:
                        planRunning(target, plan, now);
                        break;
                    case CREATING:
                        handleCreating(target, now);
                        break;
                }
            }
        }

        return plan;
    }

    /**
     * Plans action for a RUNNING target. No ZK calls. Cache miss targets are deferred to Phase 2
     * batch ZK query.
     */
    @GuardedBy("lock")
    private void planRunning(IndexFetcherTarget target, ReconciliationPlan plan, long now) {
        int lastDataBucketLeader = target.getLastKnownDataBucketLeaderId();
        if (lastDataBucketLeader == IndexFetcherTarget.INVALID_DATA_BUCKET_LEADER_ID) {
            return;
        }

        // Sync successful fetch timestamp from the fetcher thread into the target
        ServerIdAndFetcherId fetcherId = bucketToFetcher.get(target.getDataIndexTableBucket());
        if (fetcherId != null) {
            IndexFetcherThread thread = fetcherThreads.get(fetcherId);
            if (thread != null) {
                long ts = thread.getLastSuccessfulFetchTimestamp(target.getDataIndexTableBucket());
                if (ts > target.getLastSuccessfulFetchTimestamp()) {
                    target.recordSuccessfulFetch(ts);
                }
            }
        }

        // Check if the server bound to this target is still alive
        if (!serverNodeCache.apply(lastDataBucketLeader).isPresent()) {
            LOG.warn(
                    "Server {} is no longer in server node cache for {}, marking failed",
                    lastDataBucketLeader,
                    target.getDataIndexTableBucket());
            target.markFailed(
                    String.format("Server %d no longer in server node cache", lastDataBucketLeader),
                    now);
            removeFromFetcher(target.getDataIndexTableBucket());
            return;
        }

        Optional<Integer> leaderIdOpt = getCachedLeaderId(target.getDataBucket());

        if (!leaderIdOpt.isPresent()
                || leaderIdOpt.get() == IndexFetcherTarget.INVALID_DATA_BUCKET_LEADER_ID) {
            // Cache miss — defer to Phase 2 batch ZK query instead of blocking here
            plan.runningNeedsZk.put(
                    target.getDataIndexTableBucket(),
                    new PendingLeaderCheck(
                            target.getDataIndexTableBucket(),
                            target.getDataBucket(),
                            lastDataBucketLeader,
                            target.getState()));
        } else if (leaderIdOpt.get() != lastDataBucketLeader) {
            int newLeader = leaderIdOpt.get();

            // Validation 1: new leader not in alive server list -> ignore dirty cache
            if (!serverNodeCache.apply(newLeader).isPresent()) {
                LOG.warn(
                        "Cache reports leader change for {} to dead server {}, ignoring",
                        target.getDataBucket(),
                        newLeader);
                return;
            }

            // Validation 2: current fetcher recently successful -> don't kill
            long timeSinceLastSuccess = now - target.getLastSuccessfulFetchTimestamp();
            if (timeSinceLastSuccess < RECONCILIATION_INTERVAL_MS * 2) {
                LOG.debug(
                        "Ignoring cache leader change for {} — fetcher recently successful ({}ms ago)",
                        target.getDataBucket(),
                        timeSinceLastSuccess);
                return;
            }

            LOG.info(
                    "Leader changed for {} ({} -> {}), marking failed",
                    target.getDataBucket(),
                    lastDataBucketLeader,
                    leaderIdOpt.get());
            target.markFailed(
                    String.format(
                            "Leader changed from %d to %d",
                            lastDataBucketLeader, leaderIdOpt.get()),
                    now);
            removeFromFetcher(target.getDataIndexTableBucket());
        }
        // else: cache hit, leader unchanged -> no action needed
    }

    /**
     * Plans action for a PENDING or FAILED target. No ZK calls. Cache miss targets are deferred to
     * Phase 2 batch ZK query.
     */
    @GuardedBy("lock")
    private void planPendingOrFailed(IndexFetcherTarget target, ReconciliationPlan plan, long now) {

        if (!target.isReadyForRetry(now)) {
            return;
        }

        // Try cache first
        Optional<Integer> leaderIdOpt = getCachedLeaderId(target.getDataBucket());

        boolean needsZk = false;
        if (!leaderIdOpt.isPresent()) {
            needsZk = true;
        } else {
            int leaderId = leaderIdOpt.get();
            if (leaderId == IndexFetcherTarget.INVALID_DATA_BUCKET_LEADER_ID) {
                needsZk = true;
            } else if (target.getState() == IndexFetcherTarget.State.FAILED) {
                // For FAILED targets: if cache returns same leader that failed before,
                // or leader's server is dead, or failed >= 2 times, need ZK verification
                if (leaderId == target.getLastKnownDataBucketLeaderId()
                        || !serverNodeCache.apply(leaderId).isPresent()
                        || target.getFailureCount() >= 2) {
                    needsZk = true;
                }
            }
        }

        if (needsZk) {
            // Defer to Phase 2 batch ZK query
            plan.failedNeedsZk.put(
                    target.getDataIndexTableBucket(),
                    new PendingLeaderQuery(
                            target.getDataIndexTableBucket(),
                            target.getDataBucket(),
                            target.getFailureCount(),
                            target.getLastKnownDataBucketLeaderId(),
                            target.getLastAttemptTimestamp(),
                            target.getState()));
            return;
        }

        // Cache hit with valid leader — verify server is alive before scheduling
        int leaderId = leaderIdOpt.get();
        if (!serverNodeCache.apply(leaderId).isPresent()) {
            // Leader server is dead — need ZK to find a new leader
            plan.failedNeedsZk.put(
                    target.getDataIndexTableBucket(),
                    new PendingLeaderQuery(
                            target.getDataIndexTableBucket(),
                            target.getDataBucket(),
                            target.getFailureCount(),
                            target.getLastKnownDataBucketLeaderId(),
                            target.getLastAttemptTimestamp(),
                            target.getState()));
            return;
        }
        target.setLastKnownDataBucketLeaderId(leaderId);

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

        ServerIdAndFetcherId fetcherId =
                new ServerIdAndFetcherId(leaderId, computeFetcherId(target.getDataBucket()));
        plan.readyToCreate.computeIfAbsent(fetcherId, k -> new ArrayList<>()).add(target);
    }

    /**
     * Phase 2: Resolve leaders via batch ZK query. Runs OUTSIDE the lock. Uses
     * zkClient.getLeaderAndIsrs() which internally uses async background requests.
     *
     * @return map of data bucket to its LeaderAndIsr from ZK; null on failure
     */
    private Map<TableBucket, LeaderAndIsr> resolveLeaders(ReconciliationPlan plan) {
        Set<TableBucket> bucketsToQuery = plan.collectZkQueryBuckets();
        if (bucketsToQuery.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            Map<TableBucket, LeaderAndIsr> results = queryZkLeaders(bucketsToQuery);
            LOG.debug(
                    "Batch ZK query resolved {} of {} buckets",
                    results.size(),
                    bucketsToQuery.size());
            return results;
        } catch (Exception e) {
            LOG.warn(
                    "Batch ZK leader query failed for {} buckets, will retry next cycle",
                    bucketsToQuery.size(),
                    e);
            // Return null (not empty map) to distinguish "ZK call failed entirely"
            // from "ZK returned no data for these buckets". Phase 3 uses this to
            // avoid marking RUNNING targets FAILED on transient ZK failures.
            return null;
        }
    }

    /**
     * Phase 3: Apply ZK results and execute fetcher creations. Pure memory operations only. Detects
     * stale entries where target state changed between Phase 1 and Phase 3.
     *
     * <p>The {@code pendingAdditions} map is populated with (thread, statusMap) pairs that must be
     * executed <b>outside</b> the lock to avoid a lock-ordering liveness hazard: addIndexBuckets()
     * acquires the fetcher thread's internal lock, which is also held during blocking RPCs.
     *
     * @param pendingAdditions output map collecting addIndexBuckets work to run outside the lock
     * @return list of old threads that need to be shut down outside of lock
     */
    @GuardedBy("lock")
    private List<IndexFetcherThread> applyPlanAndExecute(
            ReconciliationPlan plan,
            Map<TableBucket, LeaderAndIsr> zkResults,
            long now,
            Map<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                    pendingAdditions) {

        // Apply ZK results for RUNNING targets with cache miss
        applyRunningZkResults(plan, zkResults, now);

        // Apply ZK results for FAILED/PENDING targets
        applyFailedZkResults(plan, zkResults, now);

        // Execute all fetcher creations (cache-hit from Phase 1 + ZK-resolved from above)
        List<IndexFetcherThread> threadsToShutdown =
                executeCreations(plan.readyToCreate, now, pendingAdditions);

        int totalResolved = plan.runningNeedsZk.size() + plan.failedNeedsZk.size();
        if (totalResolved > 0 || !plan.readyToCreate.isEmpty()) {
            LOG.info(
                    "Reconciliation: {} ZK queries resolved, {} targets to create, {} fetcher threads",
                    totalResolved,
                    plan.readyToCreate.values().stream().mapToInt(List::size).sum(),
                    fetcherThreads.size());
        }

        return threadsToShutdown;
    }

    /** Applies ZK results for RUNNING targets that had cache miss in Phase 1. */
    @GuardedBy("lock")
    private void applyRunningZkResults(
            ReconciliationPlan plan, Map<TableBucket, LeaderAndIsr> zkResults, long now) {

        if (zkResults == null) {
            // Batch ZK call failed entirely — leave RUNNING targets as-is,
            // they will be retried next reconciliation cycle. This avoids
            // mass RUNNING→FAILED transitions on transient ZK failures.
            LOG.debug(
                    "Skipping {} RUNNING ZK results due to batch ZK failure",
                    plan.runningNeedsZk.size());
            return;
        }

        for (PendingLeaderCheck check : plan.runningNeedsZk.values()) {
            IndexFetcherTarget target = findTarget(check.dataIndexBucket);

            // Stale detection: target removed or state changed since Phase 1
            if (target == null) {
                continue;
            }
            if (target.getState() != check.expectedState
                    || target.getLastKnownDataBucketLeaderId()
                            != check.lastKnownDataBucketLeaderId) {
                LOG.debug("Skipping stale RUNNING ZK result for {}", check.dataIndexBucket);
                continue;
            }

            LeaderAndIsr leaderAndIsr = zkResults.get(check.dataBucket);
            if (leaderAndIsr == null) {
                // ZK has no data or query failed — mark FAILED, retry next cycle
                LOG.info("Leader unavailable in ZK for {}, marking failed", check.dataBucket);
                target.markFailed("Leader unavailable in ZK", now);
                removeFromFetcher(check.dataIndexBucket);
            } else if (leaderAndIsr.leader() == check.lastKnownDataBucketLeaderId) {
                // ZK confirms current leader is still valid — keep RUNNING
                LOG.debug(
                        "ZK confirms leader {} is still valid for {}, keeping fetcher running",
                        check.lastKnownDataBucketLeaderId,
                        check.dataBucket);
            } else if (leaderAndIsr.leader() != IndexFetcherTarget.INVALID_DATA_BUCKET_LEADER_ID
                    && serverNodeCache.apply(leaderAndIsr.leader()).isPresent()) {
                // ZK reports a different valid leader — mark FAILED to trigger re-creation
                LOG.info(
                        "ZK reports new leader {} for {} (was {}), marking failed",
                        leaderAndIsr.leader(),
                        check.dataBucket,
                        check.lastKnownDataBucketLeaderId);
                target.markFailed(
                        String.format(
                                "Leader changed from %d to %d (from ZK)",
                                check.lastKnownDataBucketLeaderId, leaderAndIsr.leader()),
                        now);
                removeFromFetcher(check.dataIndexBucket);
            } else {
                // ZK reports invalid leader or dead server
                LOG.info("Leader unavailable for {}, marking failed", check.dataBucket);
                target.markFailed("Leader unavailable in ZK", now);
                removeFromFetcher(check.dataIndexBucket);
            }
        }
    }

    /** Applies ZK results for FAILED/PENDING targets that needed leader resolution. */
    @GuardedBy("lock")
    private void applyFailedZkResults(
            ReconciliationPlan plan, Map<TableBucket, LeaderAndIsr> zkResults, long now) {

        if (zkResults == null) {
            // Batch ZK call failed — FAILED/PENDING targets stay as-is,
            // will retry next cycle
            return;
        }

        for (PendingLeaderQuery query : plan.failedNeedsZk.values()) {
            IndexFetcherTarget target = findTarget(query.dataIndexBucket);

            // Stale detection: target removed or state changed since Phase 1
            if (target == null) {
                continue;
            }
            if (target.getState() != query.expectedState
                    || target.getFailureCount() != query.failureCount
                    || target.getLastAttemptTimestamp() != query.lastAttemptTimestamp) {
                LOG.debug("Skipping stale FAILED ZK result for {}", query.dataIndexBucket);
                continue;
            }

            LeaderAndIsr leaderAndIsr = zkResults.get(query.dataBucket);
            if (leaderAndIsr == null
                    || leaderAndIsr.leader() == IndexFetcherTarget.INVALID_DATA_BUCKET_LEADER_ID) {
                // No leader found in ZK
                target.markFailed("Leader not found in ZK for " + query.dataBucket, now);
                continue;
            }

            int leaderId = leaderAndIsr.leader();
            if (!serverNodeCache.apply(leaderId).isPresent()) {
                // Leader server is dead
                target.markFailed(String.format("Leader %d from ZK is not alive", leaderId), now);
                continue;
            }

            // Valid leader found — schedule for creation
            target.setLastKnownDataBucketLeaderId(leaderId);

            if (target.getState() == IndexFetcherTarget.State.PENDING) {
                LOG.info(
                        "Starting fetch for {}, leader: {} (from ZK)",
                        target.getDataIndexTableBucket(),
                        leaderId);
            } else {
                LOG.info(
                        "Retrying {} (attempt {}), leader: {} (from ZK), last failure: {}",
                        target.getDataIndexTableBucket(),
                        target.getFailureCount() + 1,
                        leaderId,
                        target.getLastFailureReason());
            }

            ServerIdAndFetcherId fetcherId =
                    new ServerIdAndFetcherId(leaderId, computeFetcherId(target.getDataBucket()));
            plan.readyToCreate.computeIfAbsent(fetcherId, k -> new ArrayList<>()).add(target);
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
     *
     * @param pendingAdditions output map collecting addIndexBuckets work to run outside the lock
     */
    @GuardedBy("lock")
    private List<IndexFetcherThread> executeCreations(
            Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> toCreate,
            long now,
            Map<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                    pendingAdditions) {

        List<IndexFetcherThread> threadsToShutdown = new ArrayList<>();

        for (Map.Entry<ServerIdAndFetcherId, List<IndexFetcherTarget>> entry :
                toCreate.entrySet()) {
            ServerIdAndFetcherId fetcherId = entry.getKey();
            List<IndexFetcherTarget> targetList = entry.getValue();

            try {
                IndexFetcherThread oldThread =
                        createOrUpdateFetcher(fetcherId, targetList, now, pendingAdditions);
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

    /** Plan produced by Phase 1, consumed by Phase 2 and 3. */
    static class ReconciliationPlan {
        /** RUNNING targets with cache miss, need ZK to confirm leader validity. */
        final Map<DataIndexTableBucket, PendingLeaderCheck> runningNeedsZk = new HashMap<>();

        /** FAILED/PENDING targets that need ZK to resolve leader. */
        final Map<DataIndexTableBucket, PendingLeaderQuery> failedNeedsZk = new HashMap<>();

        /** Cache-hit targets ready to create fetchers. */
        final Map<ServerIdAndFetcherId, List<IndexFetcherTarget>> readyToCreate = new HashMap<>();

        /** Collect deduplicated data buckets for batch ZK query. */
        Set<TableBucket> collectZkQueryBuckets() {
            Set<TableBucket> buckets = new HashSet<>();
            for (PendingLeaderCheck c : runningNeedsZk.values()) {
                buckets.add(c.dataBucket);
            }
            for (PendingLeaderQuery q : failedNeedsZk.values()) {
                buckets.add(q.dataBucket);
            }
            return buckets;
        }

        boolean hasZkQueries() {
            return !runningNeedsZk.isEmpty() || !failedNeedsZk.isEmpty();
        }
    }

    /** Snapshot of a RUNNING target that needs ZK confirmation of data bucket leader. */
    static class PendingLeaderCheck {
        final DataIndexTableBucket dataIndexBucket;
        final TableBucket dataBucket;
        /** The last known leader server ID of the data bucket (not the index bucket). */
        final int lastKnownDataBucketLeaderId;

        final IndexFetcherTarget.State expectedState;

        PendingLeaderCheck(
                DataIndexTableBucket dataIndexBucket,
                TableBucket dataBucket,
                int lastKnownDataBucketLeaderId,
                IndexFetcherTarget.State expectedState) {
            this.dataIndexBucket = dataIndexBucket;
            this.dataBucket = dataBucket;
            this.lastKnownDataBucketLeaderId = lastKnownDataBucketLeaderId;
            this.expectedState = expectedState;
        }
    }

    /** Snapshot of a FAILED/PENDING target that needs ZK resolution of data bucket leader. */
    static class PendingLeaderQuery {
        final DataIndexTableBucket dataIndexBucket;
        final TableBucket dataBucket;
        final int failureCount;
        /** The last known leader server ID of the data bucket (not the index bucket). */
        final int lastKnownDataBucketLeaderId;

        final long lastAttemptTimestamp;
        final IndexFetcherTarget.State expectedState;

        PendingLeaderQuery(
                DataIndexTableBucket dataIndexBucket,
                TableBucket dataBucket,
                int failureCount,
                int lastKnownDataBucketLeaderId,
                long lastAttemptTimestamp,
                IndexFetcherTarget.State expectedState) {
            this.dataIndexBucket = dataIndexBucket;
            this.dataBucket = dataBucket;
            this.failureCount = failureCount;
            this.lastKnownDataBucketLeaderId = lastKnownDataBucketLeaderId;
            this.lastAttemptTimestamp = lastAttemptTimestamp;
            this.expectedState = expectedState;
        }
    }

    // ==================== Fetcher Management ====================

    /**
     * Creates or updates a fetcher thread for the given targets.
     *
     * <p>Instead of calling {@code thread.addIndexBuckets()} directly (which would acquire the
     * fetcher thread's internal lock while the manager lock is held), the work is collected into
     * {@code pendingAdditions} for deferred execution outside the lock.
     *
     * @param pendingAdditions output map collecting addIndexBuckets work to run outside the lock
     * @return the old thread that needs to be shut down (outside of lock), or null if no shutdown
     *     needed
     */
    @GuardedBy("lock")
    private IndexFetcherThread createOrUpdateFetcher(
            ServerIdAndFetcherId fetcherId,
            List<IndexFetcherTarget> targetList,
            long now,
            Map<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                    pendingAdditions)
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

        // Collect buckets to add — deferred to outside the lock to avoid
        // lock-ordering hazard (manager lock -> fetcher thread lock)
        Map<DataIndexTableBucket, IndexInitialFetchStatus> statusMap =
                pendingAdditions.computeIfAbsent(thread, k -> new HashMap<>());
        for (IndexFetcherTarget target : targetList) {
            statusMap.put(
                    target.getDataIndexTableBucket(),
                    new IndexInitialFetchStatus(
                            target.getIndexTablePath(), target.getIndexApplier()));
        }

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

    /** Shuts down the given fetcher threads. */
    private void shutdownFetchers(List<IndexFetcherThread> threads) throws InterruptedException {
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
        return (bucket.hashCode() & Integer.MAX_VALUE) % numFetchersPerServer;
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

    // ==================== Dependency Access (overridable for testing) ====================

    /** Invalidates a cached leader entry. Override in tests to avoid MetadataCache. */
    @VisibleForTesting
    protected void invalidateCachedLeader(TableBucket dataBucket) {
        metadataCache.invalidateBucketLeader(dataBucket);
    }

    /** Returns the cached leader ID for a data bucket. Override in tests to avoid MetadataCache. */
    @VisibleForTesting
    protected Optional<Integer> getCachedLeaderId(TableBucket dataBucket) {
        return metadataCache.getBucketLeaderId(dataBucket);
    }

    /**
     * Batch-queries ZK for leader info. Override in tests to avoid ZooKeeperClient.
     *
     * @return leader info map, or null if the ZK call failed entirely
     */
    @VisibleForTesting
    protected Map<TableBucket, LeaderAndIsr> queryZkLeaders(Set<TableBucket> buckets)
            throws Exception {
        return zkClient.getLeaderAndIsrs(buckets);
    }

    /** Returns the server metric group. Override in tests to avoid ReplicaManager. */
    @VisibleForTesting
    protected TabletServerMetricGroup getServerMetricGroup() {
        return replicaManager.getServerMetricGroup();
    }

    // ==================== Test Accessors ====================

    @VisibleForTesting
    IndexFetcherTarget findTargetForTest(DataIndexTableBucket bucket) {
        synchronized (lock) {
            for (Map<TableBucket, IndexFetcherTarget> indexTargets : targets.values()) {
                for (IndexFetcherTarget target : indexTargets.values()) {
                    if (target.getDataBucket().equals(bucket.getDataBucket())
                            && target.getIndexBucket().equals(bucket.getIndexBucket())) {
                        return target;
                    }
                }
            }
            return null;
        }
    }

    @VisibleForTesting
    Map<TableBucket, Map<TableBucket, IndexFetcherTarget>> getTargetsForTest() {
        synchronized (lock) {
            return new HashMap<>(targets);
        }
    }

    @VisibleForTesting
    void triggerReconciliationForTest() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /** Runs one synchronous reconciliation cycle (Plan-Resolve-Apply) for testing. */
    @VisibleForTesting
    void reconcileSyncForTest() {
        long now = System.currentTimeMillis();

        // Phase 1: Plan (locked)
        ReconciliationPlan plan;
        synchronized (lock) {
            plan = planReconciliation(now);
        }

        // Phase 2: Resolve (unlocked)
        Map<TableBucket, LeaderAndIsr> zkResults = Collections.emptyMap();
        if (plan.hasZkQueries()) {
            zkResults = resolveLeaders(plan);
        }

        // Phase 3: Apply (locked)
        Map<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                pendingAdditions = new HashMap<>();
        List<IndexFetcherThread> threadsToShutdown;
        synchronized (lock) {
            threadsToShutdown = applyPlanAndExecute(plan, zkResults, now, pendingAdditions);
        }

        // Phase 3.5: Add buckets outside lock
        for (Map.Entry<IndexFetcherThread, Map<DataIndexTableBucket, IndexInitialFetchStatus>>
                entry : pendingAdditions.entrySet()) {
            try {
                entry.getKey().addIndexBuckets(entry.getValue());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Phase 4: Shutdown old threads outside lock
        if (threadsToShutdown != null) {
            for (IndexFetcherThread thread : threadsToShutdown) {
                try {
                    thread.shutdown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
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
