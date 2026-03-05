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
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.MapUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for IndexFetcherManager reconciliation bugs discovered in the fluss-pushdown-test-zjk-1
 * incident (2026-03-04).
 *
 * <p>These tests reproduce scenarios where IndexFetcherManager fails to recover after a
 * TabletServer node crash. Each test is expected to FAIL with the current buggy code and PASS after
 * the corresponding fix is applied.
 *
 * <p>Bug summary:
 *
 * <ul>
 *   <li>Bug 1 (P1): Network exceptions in IndexFetcherThread only go to indexBucketsWithError,
 *       never to invalidBuckets, so reconciliation cannot detect persistent failures.
 *   <li>Bug 2 (P0): handleRunning() does not check if the server bound to an IndexFetcherThread is
 *       still alive in the server node cache.
 *   <li>Bug 4 (P0): No health check mechanism to detect IndexFetcherThreads stuck in persistent
 *       failure loops.
 * </ul>
 */
class IndexFetcherManagerReconciliationTest {

    private static final int LOCAL_SERVER_ID = 5;
    private static final int REMOTE_SERVER_ID = 18;
    private static final int NEW_LEADER_SERVER_ID = 17;

    private static final long DATA_TABLE_ID = 122L;
    private static final long INDEX_TABLE_ID = 123L;

    private static final TablePath INDEX_TABLE_PATH = TablePath.of("test_db", "test_index_table");

    private Configuration conf;
    private TabletServerMetadataCache metadataCache;
    private ZooKeeperClient zkClient;
    private ReplicaManager replicaManager;

    /** Controllable server node cache: serverId to Optional ServerNode. */
    private final Map<Integer, ServerNode> liveServers = MapUtils.newConcurrentHashMap();

    private Function<Integer, Optional<ServerNode>> serverNodeCache;

    /** Tracks fetcher threads created by the manager. */
    private final List<IndexFetcherThread> createdFetcherThreads = new CopyOnWriteArrayList<>();

    private TestableIndexFetcherManager fetcherManager;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        metadataCache = mock(TabletServerMetadataCache.class);
        zkClient = mock(ZooKeeperClient.class);
        replicaManager = mock(ReplicaManager.class);

        // Set up metric group for ReplicaManager mock
        TabletServerMetricGroup metricGroup = TestingMetricGroups.TABLET_SERVER_METRICS;
        when(replicaManager.getServerMetricGroup()).thenReturn(metricGroup);

        // Set up controllable server node cache
        serverNodeCache = serverId -> Optional.ofNullable(liveServers.get(serverId));

        // Initially, remote server 18 is alive
        liveServers.put(
                REMOTE_SERVER_ID,
                new ServerNode(REMOTE_SERVER_ID, "33.111.49.213", 46287, ServerType.TABLET_SERVER));
        liveServers.put(
                NEW_LEADER_SERVER_ID,
                new ServerNode(
                        NEW_LEADER_SERVER_ID, "33.111.49.214", 46288, ServerType.TABLET_SERVER));

        // Default: metadataCache returns REMOTE_SERVER_ID as leader for data buckets
        when(metadataCache.getBucketLeaderId(any(TableBucket.class)))
                .thenReturn(Optional.of(REMOTE_SERVER_ID));

        // Default: ZK returns REMOTE_SERVER_ID as leader
        when(zkClient.getLeaderAndIsr(any(TableBucket.class)))
                .thenReturn(
                        Optional.of(
                                new LeaderAndIsr(
                                        REMOTE_SERVER_ID,
                                        LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                        Collections.singletonList(REMOTE_SERVER_ID),
                                        0,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH)));

        fetcherManager =
                new TestableIndexFetcherManager(
                        conf,
                        mock(RpcClient.class),
                        LOCAL_SERVER_ID,
                        replicaManager,
                        metadataCache,
                        zkClient,
                        serverNodeCache);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fetcherManager != null) {
            fetcherManager.shutdown();
        }
    }

    // ==================== Bug 2 (P0): handleRunning doesn't check server liveness ===========

    /**
     * Reproduces Bug 2: When a server is removed from the node cache (simulating node crash),
     * handleRunning() should detect that the server is no longer available and mark the target as
     * FAILED. Currently, handleRunning() only checks if the data bucket leader has changed in
     * metadataCache, but does NOT check if the IndexFetcherThread's bound server is still alive.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Add target: dataBucket(tableId=122, bucket=0) -> indexBucket(tableId=123, bucket=9),
     *       leader = server 18
     *   <li>Reconciliation creates fetcher thread bound to server 18, target becomes RUNNING
     *   <li>Server 18 crashes: removed from serverNodeCache
     *   <li>But metadataCache still shows server 18 as leader (race window before metadata update)
     *   <li>Expected: handleRunning() detects server 18 is dead, marks target FAILED
     *   <li>Actual (Bug): handleRunning() sees leader unchanged (still 18), does nothing
     * </ol>
     */
    @Test
    void testHandleRunningShouldDetectDeadServer() throws Exception {
        // Step 1: Add target
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket = new DataIndexTableBucket(dataBucket, indexBucket);

        // IndexApplier is final and cannot be mocked, but it's never invoked in these
        // tests because fetches fail before reaching the apply stage. Using null is safe here.
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Step 2: Start reconciliation - this will create fetcher thread and mark target RUNNING
        fetcherManager.startup();

        // Wait for reconciliation to process the target
        waitForCondition(
                () -> !createdFetcherThreads.isEmpty(), 5000, "Fetcher thread should be created");

        // Verify target is RUNNING
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
        assertThat(target).isNotNull();

        waitForCondition(
                () ->
                        fetcherManager.findTargetForTest(dataIndexBucket).getState()
                                == IndexFetcherTarget.State.RUNNING,
                5000,
                "Target should be in RUNNING state");

        assertThat(target.getLastKnownLeaderServerId()).isEqualTo(REMOTE_SERVER_ID);

        // Step 3: Simulate server 18 crash - remove from server node cache
        // But metadataCache still returns server 18 as leader (race window)
        liveServers.remove(REMOTE_SERVER_ID);
        // metadataCache still returns REMOTE_SERVER_ID - this is the race condition

        // Step 4: Trigger reconciliation by waking up the reconciliation thread
        fetcherManager.triggerReconciliation();

        // Step 5: Wait and verify - target should be marked FAILED because server 18 is dead
        // This assertion will FAIL with the current buggy code because handleRunning()
        // only checks metadataCache leader, not server liveness
        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket);
                    return t != null && t.getState() == IndexFetcherTarget.State.FAILED;
                },
                10000,
                "Target should be marked FAILED when bound server is removed from cache. "
                        + "Bug 2: handleRunning() does not check server liveness in serverNodeCache.");
    }

    // ==================== Bug 1 (P1): Network exception not reported to reconciliation ======

    /**
     * Reproduces Bug 1: When IndexFetcherThread.processFetchIndexRequest() catches a
     * RuntimeException (like "Server 18 not in cache"), the bucket is only added to
     * indexBucketsWithError (delay retry), NOT to invalidBuckets. This means reconciliation's
     * processFailedBuckets() never sees these failures.
     *
     * <p>This test verifies the end-to-end impact at the manager level: after a server crash, the
     * IndexFetcherThread bound to the dead server keeps failing with "Server X not in cache", but
     * these failures are never reported to reconciliation via pollInvalidBuckets(). As a result,
     * processFailedBuckets() never marks the target as FAILED, and the target stays RUNNING
     * indefinitely.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Add target, reconciliation creates fetcher thread bound to server 18, target RUNNING
     *   <li>Server 18 crashes: removed from serverNodeCache
     *   <li>Metadata cache still shows server 18 as leader (no leader change detected)
     *   <li>IndexFetcherThread loops "Server 18 not in cache" but only adds to
     *       indexBucketsWithError, never to invalidBuckets
     *   <li>Expected: after multiple reconciliation cycles, processFailedBuckets() should detect
     *       the persistent failure and mark target FAILED
     *   <li>Actual (Bug): processFailedBuckets() never sees the failure because
     *       pollInvalidBuckets() is always empty
     * </ol>
     *
     * <p>Note: This test overlaps with Bug 2 in observable behavior (target stays RUNNING), but the
     * root cause is different. Bug 2 is about handleRunning() not checking server liveness. Bug 1
     * is about IndexFetcherThread not reporting network exceptions to reconciliation. Even if Bug 2
     * is fixed, Bug 1 would still prevent recovery in cases where the metadata cache leader hasn't
     * changed.
     */
    @Test
    void testNetworkExceptionShouldReportToReconciliation() throws Exception {
        // Step 1: Add target
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket = new DataIndexTableBucket(dataBucket, indexBucket);

        // IndexApplier is final and cannot be mocked, but it's never invoked in these
        // tests because fetches fail before reaching the apply stage. Using null is safe here.
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Step 2: Start reconciliation - creates fetcher thread, target becomes RUNNING
        fetcherManager.startup();

        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                5000,
                "Target should be in RUNNING state");

        IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
        assertThat(target.getLastKnownLeaderServerId()).isEqualTo(REMOTE_SERVER_ID);

        // Step 3: Simulate server 18 crash - remove from serverNodeCache
        // Metadata cache still returns server 18 as leader (no change detected by handleRunning)
        liveServers.remove(REMOTE_SERVER_ID);

        // Step 4: Run multiple reconciliation cycles
        // The fetcher thread is now bound to dead server 18. Each fetch attempt throws
        // "Server 18 not in cache". With Bug 1, these exceptions only go to
        // indexBucketsWithError, never to invalidBuckets. So processFailedBuckets()
        // in reconciliation never sees them.
        for (int i = 0; i < 5; i++) {
            fetcherManager.triggerReconciliation();
            Thread.sleep(300);
        }

        // Step 5: Verify - after multiple reconciliation cycles, the target should be FAILED
        // because the fetcher thread's persistent failures should have been reported.
        // With Bug 1, pollInvalidBuckets() is always empty, so processFailedBuckets()
        // never marks the target as FAILED.
        target = fetcherManager.findTargetForTest(dataIndexBucket);
        assertThat(target).isNotNull();
        assertThat(target.getState())
                .as(
                        "Bug 1: Network exceptions (RuntimeException 'Server X not in cache') "
                                + "in IndexFetcherThread.processFetchIndexRequest() should be "
                                + "reported to reconciliation via invalidBuckets, not just "
                                + "delayed via indexBucketsWithError. Currently, persistent "
                                + "network failures are invisible to reconciliation, causing "
                                + "the target to stay RUNNING indefinitely.")
                .isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    // ==================== Bug 4 (P0): No fetcher thread health check =======================

    /**
     * Reproduces Bug 4: There is no mechanism to detect that an IndexFetcherThread is stuck in a
     * persistent failure loop. Even after multiple consecutive failures, the target remains in
     * RUNNING state and the fetcher thread keeps retrying against a dead server.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Add target, let reconciliation create fetcher thread, target becomes RUNNING
     *   <li>Server 18 crashes: removed from serverNodeCache
     *   <li>Metadata cache is updated: leader changes from 18 to 17
     *   <li>handleRunning() detects leader change, marks target FAILED, removes from fetcher
     *   <li>But the IndexFetcherThread itself (bound to server 18) still has OTHER buckets that
     *       were not removed (their data bucket leader didn't change)
     *   <li>These remaining buckets loop "Server 18 not in cache" forever
     *   <li>Expected: health check detects the dead fetcher thread and marks all its targets FAILED
     *   <li>Actual (Bug): no health check exists, remaining buckets loop forever
     * </ol>
     */
    @Test
    void testFetcherThreadHealthCheckShouldDetectDeadServerThread() throws Exception {
        // Add two targets on different data buckets, both fetching to the same index bucket
        // Both data buckets have leader = server 18
        TableBucket dataBucket0 = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket dataBucket1 = new TableBucket(DATA_TABLE_ID, 1);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket0 = new DataIndexTableBucket(dataBucket0, indexBucket);
        DataIndexTableBucket dataIndexBucket1 = new DataIndexTableBucket(dataBucket1, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket0,
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        targetInfos.put(
                dataIndexBucket1,
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Start reconciliation
        fetcherManager.startup();

        // Wait for targets to become RUNNING
        waitForCondition(
                () -> {
                    IndexFetcherTarget t0 = fetcherManager.findTargetForTest(dataIndexBucket0);
                    IndexFetcherTarget t1 = fetcherManager.findTargetForTest(dataIndexBucket1);
                    return t0 != null
                            && t0.getState() == IndexFetcherTarget.State.RUNNING
                            && t1 != null
                            && t1.getState() == IndexFetcherTarget.State.RUNNING;
                },
                5000,
                "Both targets should be RUNNING");

        // Simulate server 18 crash
        liveServers.remove(REMOTE_SERVER_ID);

        // Metadata cache update: dataBucket0's leader changes to 17, but dataBucket1's leader
        // is still reported as 18 (race condition or the bucket was already on another server)
        when(metadataCache.getBucketLeaderId(dataBucket0))
                .thenReturn(Optional.of(NEW_LEADER_SERVER_ID));
        // dataBucket1 still shows leader = 18 in metadata cache
        when(metadataCache.getBucketLeaderId(dataBucket1))
                .thenReturn(Optional.of(REMOTE_SERVER_ID));

        // Trigger reconciliation
        fetcherManager.triggerReconciliation();

        // Wait for dataBucket0's target to be marked FAILED (handleRunning detects leader change)
        waitForCondition(
                () -> {
                    IndexFetcherTarget t0 = fetcherManager.findTargetForTest(dataIndexBucket0);
                    return t0 != null && t0.getState() == IndexFetcherTarget.State.FAILED;
                },
                10000,
                "Target for dataBucket0 should be FAILED due to leader change");

        // Now check dataBucket1's target - it should ALSO be marked FAILED because
        // the fetcher thread is bound to dead server 18.
        // With the current bug, dataBucket1's target stays RUNNING because:
        // - handleRunning() sees leader unchanged (still 18 in metadataCache)
        // - No health check detects that the fetcher thread's server is dead
        // - IndexFetcherThread loops "Server 18 not in cache" but never reports to invalidBuckets

        // Give a few more reconciliation cycles
        for (int i = 0; i < 3; i++) {
            fetcherManager.triggerReconciliation();
            Thread.sleep(200);
        }

        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dataIndexBucket1);
        assertThat(target1).isNotNull();

        // This assertion will FAIL with the current buggy code.
        // Bug 4: No health check mechanism exists to detect that the IndexFetcherThread
        // bound to server 18 is in a persistent failure state.
        assertThat(target1.getState())
                .as(
                        "Bug 4: Target for dataBucket1 should be FAILED because the fetcher "
                                + "thread is bound to dead server %d which is no longer in "
                                + "serverNodeCache. There should be a health check that detects "
                                + "fetcher threads bound to dead servers and marks all their "
                                + "targets as FAILED.",
                        REMOTE_SERVER_ID)
                .isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    // ==================== Helper Classes ====================

    /**
     * A testable subclass of IndexFetcherManager that overrides createFetcherThread to return
     * controllable test instances.
     */
    private class TestableIndexFetcherManager extends IndexFetcherManager {

        TestableIndexFetcherManager(
                Configuration conf,
                RpcClient rpcClient,
                int serverId,
                ReplicaManager replicaManager,
                TabletServerMetadataCache metadataCache,
                ZooKeeperClient zkClient,
                Function<Integer, Optional<ServerNode>> serverNodeCache) {
            super(
                    conf,
                    rpcClient,
                    serverId,
                    replicaManager,
                    metadataCache,
                    zkClient,
                    serverNodeCache);
        }

        @Override
        IndexFetcherThread createFetcherThread(ServerIdAndFetcherId fetcherId) {
            // Create a controllable fetcher thread that doesn't actually fetch
            FailingLeaderEndpoint endpoint =
                    new FailingLeaderEndpoint(
                            fetcherId.getServerId(),
                            new RuntimeException(
                                    "Server " + fetcherId.getServerId() + " not in cache"));
            IndexFetcherThread thread =
                    new IndexFetcherThread(
                            "TestIndexFetcher-"
                                    + fetcherId.getFetcherId()
                                    + "-"
                                    + fetcherId.getServerId(),
                            replicaManager,
                            endpoint,
                            100,
                            conf);
            createdFetcherThreads.add(thread);
            return thread;
        }

        /** Expose findTarget for test assertions. */
        IndexFetcherTarget findTargetForTest(DataIndexTableBucket bucket) {
            synchronized (this) {
                // Use reflection-free approach: iterate through the public API
                // We need to access the private targets map via the parent class
            }
            // Use the package-private findTarget by calling it through reconciliation
            // Instead, we use a different approach: track state via the target object
            return findTargetViaReflection(bucket);
        }

        private IndexFetcherTarget findTargetViaReflection(DataIndexTableBucket bucket) {
            try {
                java.lang.reflect.Field targetsField =
                        IndexFetcherManager.class.getDeclaredField("targets");
                targetsField.setAccessible(true);
                java.lang.reflect.Field lockField =
                        IndexFetcherManager.class.getDeclaredField("lock");
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
                throw new RuntimeException("Failed to access targets via reflection", e);
            }
        }

        /** Trigger reconciliation by notifying the lock. */
        void triggerReconciliation() {
            try {
                java.lang.reflect.Field lockField =
                        IndexFetcherManager.class.getDeclaredField("lock");
                lockField.setAccessible(true);
                Object lock = lockField.get(this);
                synchronized (lock) {
                    lock.notifyAll();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to trigger reconciliation", e);
            }
        }
    }

    /**
     * A LeaderEndpoint that always fails with a configurable exception. Simulates "Server X not in
     * cache" RuntimeException.
     */
    private static class FailingLeaderEndpoint implements LeaderEndpoint {
        private final int serverId;
        private final RuntimeException exception;
        private final AtomicInteger attemptCount = new AtomicInteger(0);

        FailingLeaderEndpoint(int serverId, RuntimeException exception) {
            this.serverId = serverId;
            this.exception = exception;
        }

        int getAttemptCount() {
            return attemptCount.get();
        }

        @Override
        public int leaderServerId() {
            return serverId;
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
            return CompletableFuture.failedFuture(exception);
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
            return CompletableFuture.failedFuture(exception);
        }

        @Override
        public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
            return CompletableFuture.failedFuture(exception);
        }

        @Override
        public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
            attemptCount.incrementAndGet();
            CompletableFuture<FetchData> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }

        @Override
        public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext) {
            attemptCount.incrementAndGet();
            CompletableFuture<FetchIndexData> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }

        @Override
        public Optional<FetchLogContext> buildFetchLogContext(
                Map<TableBucket, BucketFetchStatus> replicas) {
            return Optional.empty();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // ==================== Utility Methods ====================

    private static void waitForCondition(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timed out waiting for condition: " + message);
            }
            Thread.sleep(100);
        }
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
