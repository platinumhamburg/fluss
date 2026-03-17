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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the queryLeader() ZK fallback alternating bug WITHOUT using Mockito.
 *
 * <p>Bug: When metadataCache has stale leader info and the target is in FAILED state, queryLeader()
 * alternates between the correct leader (from ZK) and the stale leader (from cache) on every other
 * reconciliation cycle. Root cause: markFailed() does not update lastKnownLeaderServerId, so the ZK
 * fallback condition {@code leaderIdOpt.get() == target.getLastKnownLeaderServerId()} only matches
 * every other round.
 *
 * <p>Alternating cycle:
 *
 * <ol>
 *   <li>FAILED, lastKnownLeader=18, cache=18 → cache==lastKnown → ZK fallback → gets 17 (correct)
 *   <li>RUNNING with leader=17, handleRunning sees cache=18≠17 → marks FAILED
 *   <li>FAILED, lastKnownLeader=17 (markFailed doesn't update it), cache=18 → cache≠lastKnown → NO
 *       ZK fallback → uses cache leader 18 (wrong)
 *   <li>RUNNING with leader=18, handleRunning sees cache=18==18 → does nothing, but server 18 is
 *       dead → checkFetcherThreadHealth marks FAILED
 *   <li>FAILED, lastKnownLeader=18 → back to step 1
 * </ol>
 *
 * <p>This test uses:
 *
 * <ul>
 *   <li>Real ZooKeeper (via ZooKeeperExtension) with correct leader=17 written
 *   <li>A stub TabletServerMetadataCache subclass that always returns stale leader=18
 *   <li>Unsafe.allocateInstance to create a ReplicaManager shell (no constructor call)
 *   <li>No Mockito — all test doubles are hand-written
 * </ul>
 */
class IndexFetcherManagerQueryLeaderAlternatingTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZK_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final int LOCAL_SERVER_ID = 5;
    private static final int DEAD_SERVER_ID = 18;
    private static final int CORRECT_LEADER_ID = 17;

    private static final long DATA_TABLE_ID = 122L;
    private static final long INDEX_TABLE_ID = 123L;

    private static final TablePath INDEX_TABLE_PATH = TablePath.of("test_db", "test_index_table");

    private final Map<Integer, ServerNode> liveServers = MapUtils.newConcurrentHashMap();
    private Function<Integer, Optional<ServerNode>> serverNodeCache;

    /** Records the leader server ID each time a fetcher thread is created. */
    private final List<Integer> fetcherCreationLeaderIds = new CopyOnWriteArrayList<>();

    private ZooKeeperClient zkClient;
    private TestableManager fetcherManager;

    @BeforeEach
    void setUp() throws Exception {
        fetcherCreationLeaderIds.clear();
        liveServers.clear();

        zkClient = ZK_EXTENSION.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);

        serverNodeCache = serverId -> Optional.ofNullable(liveServers.get(serverId));

        // Server 17 is alive (correct leader), server 18 is dead
        liveServers.put(
                CORRECT_LEADER_ID,
                new ServerNode(CORRECT_LEADER_ID, "10.0.0.17", 9123, ServerType.TABLET_SERVER));
        // Server 18 is NOT in liveServers — it's dead

        // Write correct leader=17 to ZK for the data bucket
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        zkClient.registerLeaderAndIsr(
                dataBucket,
                new LeaderAndIsr(
                        CORRECT_LEADER_ID,
                        LeaderAndIsr.INITIAL_LEADER_EPOCH + 1,
                        Collections.singletonList(CORRECT_LEADER_ID),
                        0,
                        LeaderAndIsr.INITIAL_BUCKET_EPOCH));

        // Create the manager with a stub metadataCache that always returns stale leader=18
        StaleMetadataCache staleCache = new StaleMetadataCache(DEAD_SERVER_ID);

        Configuration conf = new Configuration();
        fetcherManager =
                new TestableManager(conf, LOCAL_SERVER_ID, staleCache, zkClient, serverNodeCache);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fetcherManager != null) {
            fetcherManager.shutdown();
        }
    }

    /**
     * Reproduces the alternating leader bug.
     *
     * <p>After server 18 crashes and ZK has leader=17, the reconciliation should consistently
     * converge to leader=17. Instead, with the bug, fetcher creation alternates between 17 and 18.
     *
     * <p>Expected (after fix): all fetcher creations from FAILED state use leader=17.
     *
     * <p>Actual (with bug): fetcher creations alternate [18, 17, 18, 17, ...].
     */
    @Test
    void testQueryLeaderShouldNotAlternateBetweenCacheAndZk() throws Exception {
        // Add a target
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Do NOT start the background reconciliation thread — drive reconciliation synchronously
        // to avoid timing issues with async thread shutdown and lock.wait().

        // Drive the reconciliation loop. Each reconcileSync() does one full cycle:
        //   processFailedBuckets → checkFetcherThreadHealth → classifyTargets → executeCreations
        // Between cycles, expire the retry backoff so FAILED targets retry immediately.
        //
        // After each cycle where the target transitions from FAILED to RUNNING, record which
        // leader it was assigned to. This captures the alternating pattern even when fetcher
        // threads are reused (createFetcherThread not called again for same server).
        List<Integer> assignedLeaders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fetcherManager.expireRetryBackoff(dataIndexBucket);
            IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
            IndexFetcherTarget.State stateBefore = target != null ? target.getState() : null;
            fetcherManager.reconcileSync();
            IndexFetcherTarget.State stateAfter = target != null ? target.getState() : null;
            // Record the leader when target transitions to RUNNING (i.e., a new fetch assignment)
            if (stateAfter == IndexFetcherTarget.State.RUNNING
                    && stateBefore != IndexFetcherTarget.State.RUNNING) {
                assignedLeaders.add(target.getLastKnownLeaderServerId());
            }
        }

        assertThat(assignedLeaders)
                .as(
                        "Should have at least 2 FAILED→RUNNING transitions across reconciliation cycles")
                .hasSizeGreaterThanOrEqualTo(2);

        // The first assignment is from PENDING state (uses cache → gets 18). That's expected.
        // All subsequent assignments from FAILED state should consistently use the correct
        // leader from ZK (17), not alternate between 17 and 18.
        List<Integer> afterFirstAssignment = assignedLeaders.subList(1, assignedLeaders.size());

        // With the bug: afterFirstAssignment contains both 17 and 18 (alternating)
        // After fix: afterFirstAssignment should contain only 17
        boolean hasStaleLeader = afterFirstAssignment.contains(DEAD_SERVER_ID);

        assertThat(hasStaleLeader)
                .as(
                        "Bug: queryLeader() alternates between ZK (leader=%d) and stale cache "
                                + "(leader=%d). Assigned leader sequence: %s. "
                                + "Root cause: markFailed() does not update lastKnownLeaderServerId, "
                                + "so the ZK fallback condition 'cache_leader == lastKnownLeader' "
                                + "only matches every other round.",
                        CORRECT_LEADER_ID, DEAD_SERVER_ID, assignedLeaders)
                .isFalse();
    }

    // ==================== Test Doubles (No Mockito) ====================

    /**
     * A minimal TabletServerMetadataCache subclass that returns a fixed stale leader ID for all
     * bucket queries. Passes null for MetadataManager since we never call methods that need it.
     */
    private static class StaleMetadataCache extends TabletServerMetadataCache {

        private final int staleLeaderId;

        StaleMetadataCache(int staleLeaderId) {
            super(null); // MetadataManager not needed — only getBucketLeaderId is called
            this.staleLeaderId = staleLeaderId;
        }

        @Override
        public Optional<Integer> getBucketLeaderId(TableBucket tableBucket) {
            return Optional.of(staleLeaderId);
        }
    }

    /** A LeaderEndpoint that always fails. Simulates connecting to a dead server. */
    private static class FailingEndpoint implements LeaderEndpoint {
        private final int serverId;
        private final AtomicInteger attempts = new AtomicInteger(0);

        FailingEndpoint(int serverId) {
            this.serverId = serverId;
        }

        @Override
        public int leaderServerId() {
            return serverId;
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tb) {
            return FutureUtils.completedExceptionally(fail());
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tb) {
            return FutureUtils.completedExceptionally(fail());
        }

        @Override
        public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tb) {
            return FutureUtils.completedExceptionally(fail());
        }

        @Override
        public CompletableFuture<FetchData> fetchLog(FetchLogContext ctx) {
            attempts.incrementAndGet();
            return FutureUtils.completedExceptionally(fail());
        }

        @Override
        public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext ctx) {
            attempts.incrementAndGet();
            return FutureUtils.completedExceptionally(fail());
        }

        @Override
        public Optional<FetchLogContext> buildFetchLogContext(
                Map<TableBucket, BucketFetchStatus> replicas) {
            return Optional.empty();
        }

        @Override
        public void close() {}

        private RuntimeException fail() {
            return new RuntimeException("Server " + serverId + " not in cache");
        }
    }

    /**
     * A LeaderEndpoint for the correct leader that succeeds.
     *
     * <p>Fetch returns empty data since we don't need actual index data for this test.
     */
    private static class SuccessEndpoint implements LeaderEndpoint {
        private final int serverId;

        SuccessEndpoint(int serverId) {
            this.serverId = serverId;
        }

        @Override
        public int leaderServerId() {
            return serverId;
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tb) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tb) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tb) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletableFuture<FetchData> fetchLog(FetchLogContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Optional<FetchLogContext> buildFetchLogContext(
                Map<TableBucket, BucketFetchStatus> replicas) {
            return Optional.empty();
        }

        @Override
        public void close() {}
    }

    // ==================== ReplicaManager Shell ====================

    /**
     * Creates a ReplicaManager instance without calling its constructor, using
     * sun.misc.Unsafe.allocateInstance(). Only the serverMetricGroup field is set via reflection,
     * which is the only field accessed by IndexFetcherThread's constructor.
     */
    private static ReplicaManager createReplicaManagerShell() {
        try {
            // Use Unsafe to allocate without calling constructor
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) unsafeField.get(null);

            ReplicaManager shell = (ReplicaManager) unsafe.allocateInstance(ReplicaManager.class);

            // Set the serverMetricGroup field — the only field IndexFetcherThread reads
            Field metricField = ReplicaManager.class.getDeclaredField("serverMetricGroup");
            metricField.setAccessible(true);
            metricField.set(shell, TestingMetricGroups.TABLET_SERVER_METRICS);

            return shell;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ReplicaManager shell", e);
        }
    }

    // ==================== Testable Manager ====================

    /**
     * Testable subclass of IndexFetcherManager that overrides createFetcherThread.
     *
     * <ol>
     *   <li>Record which leader ID was used for each creation
     *   <li>Return a fetcher thread with a test endpoint (no real RPC)
     *   <li>Use a ReplicaManager shell (no real dependencies)
     * </ol>
     */
    private class TestableManager extends IndexFetcherManager {

        private final ReplicaManager replicaManagerShell = createReplicaManagerShell();

        TestableManager(
                Configuration conf,
                int serverId,
                TabletServerMetadataCache metadataCache,
                ZooKeeperClient zkClient,
                Function<Integer, Optional<ServerNode>> serverNodeCache) {
            super(conf, null, serverId, null, metadataCache, zkClient, serverNodeCache);
        }

        @Override
        IndexFetcherThread createFetcherThread(ServerIdAndFetcherId fetcherId) {
            int leaderServerId = fetcherId.getServerId();
            fetcherCreationLeaderIds.add(leaderServerId);

            LeaderEndpoint endpoint =
                    liveServers.containsKey(leaderServerId)
                            ? new SuccessEndpoint(leaderServerId)
                            : new FailingEndpoint(leaderServerId);

            return new IndexFetcherThread(
                    "TestIndexFetcher-" + fetcherId.getFetcherId() + "-" + leaderServerId,
                    replicaManagerShell,
                    endpoint,
                    100,
                    new Configuration());
        }

        IndexFetcherTarget findTargetForTest(DataIndexTableBucket bucket) {
            try {
                Field targetsField = IndexFetcherManager.class.getDeclaredField("targets");
                targetsField.setAccessible(true);
                Field lockField = IndexFetcherManager.class.getDeclaredField("lock");
                lockField.setAccessible(true);
                Object lock = lockField.get(this);
                synchronized (lock) {
                    @SuppressWarnings("unchecked")
                    Map<TableBucket, Map<TableBucket, IndexFetcherTarget>> targets =
                            (Map<TableBucket, Map<TableBucket, IndexFetcherTarget>>)
                                    targetsField.get(this);
                    Map<TableBucket, IndexFetcherTarget> indexTargets =
                            targets.get(bucket.getIndexBucket());
                    return indexTargets != null ? indexTargets.get(bucket.getDataBucket()) : null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Runs one synchronous reconciliation cycle, including shutting down old threads. This
         * avoids the timing issues of the async FetcherReconciliationThread.
         */
        void reconcileSync() {
            try {
                Field lockField = IndexFetcherManager.class.getDeclaredField("lock");
                lockField.setAccessible(true);
                Object lock = lockField.get(this);

                java.lang.reflect.Method reconcileMethod =
                        IndexFetcherManager.class.getDeclaredMethod("reconcile");
                reconcileMethod.setAccessible(true);

                List<?> threadsToShutdown;
                synchronized (lock) {
                    @SuppressWarnings("unchecked")
                    List<IndexFetcherThread> result =
                            (List<IndexFetcherThread>) reconcileMethod.invoke(this);
                    threadsToShutdown = result;
                }

                // Shutdown old threads outside of lock (same as the real reconciliation thread)
                if (threadsToShutdown != null) {
                    for (Object thread : threadsToShutdown) {
                        ((IndexFetcherThread) thread).shutdown();
                    }
                }
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw new RuntimeException("reconcile() failed", e.getCause());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void triggerReconciliation() {
            try {
                Field lockField = IndexFetcherManager.class.getDeclaredField("lock");
                lockField.setAccessible(true);
                Object lock = lockField.get(this);
                synchronized (lock) {
                    lock.notifyAll();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Expire the retry backoff for a target so it can be retried immediately. This avoids
         * waiting 5 seconds (MIN_RETRY_BACKOFF_MS) in tests.
         */
        void expireRetryBackoff(DataIndexTableBucket bucket) {
            IndexFetcherTarget target = findTargetForTest(bucket);
            if (target != null && target.getState() == IndexFetcherTarget.State.FAILED) {
                try {
                    Field timestampField =
                            IndexFetcherTarget.class.getDeclaredField("lastAttemptTimestamp");
                    timestampField.setAccessible(true);
                    timestampField.set(target, 0L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
