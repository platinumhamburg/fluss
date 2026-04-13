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
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Plan-Resolve-Apply three-phase reconciliation in IndexFetcherManager.
 *
 * <p>Validates that:
 *
 * <ul>
 *   <li>Phase 1 (Plan): cache hits skip ZK, cache misses are collected for batch ZK query
 *   <li>Phase 2 (Resolve): batch ZK query with deduplication, lock released during IO
 *   <li>Phase 3 (Apply): stale detection, graceful handling of removed targets
 * </ul>
 *
 * <p>Also includes reconciliation bug tests discovered in the fluss-pushdown-test-zjk-1 incident
 * (2026-03-04):
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
class IndexFetcherManagerPlanResolveApplyTest {

    private static final int LOCAL_SERVER_ID = 5;
    private static final int LEADER_SERVER_ID = 18;
    private static final int NEW_LEADER_SERVER_ID = 17;

    private static final long DATA_TABLE_ID = 122L;
    private static final long INDEX_TABLE_ID = 123L;

    private static final TablePath INDEX_TABLE_PATH = TablePath.of("test_db", "test_index_table");

    private Configuration conf;

    private final Map<Integer, ServerNode> liveServers = new ConcurrentHashMap<>();
    private Function<Integer, Optional<ServerNode>> serverNodeCache;
    private final List<IndexFetcherThread> createdFetcherThreads = new CopyOnWriteArrayList<>();

    private TestableIndexFetcherManager fetcherManager;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();

        serverNodeCache = serverId -> Optional.ofNullable(liveServers.get(serverId));

        liveServers.put(
                LEADER_SERVER_ID,
                new ServerNode(LEADER_SERVER_ID, "host1", 9001, ServerType.TABLET_SERVER));
        liveServers.put(
                NEW_LEADER_SERVER_ID,
                new ServerNode(NEW_LEADER_SERVER_ID, "host2", 9002, ServerType.TABLET_SERVER));

        fetcherManager = new TestableIndexFetcherManager(conf, serverNodeCache);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fetcherManager != null) {
            fetcherManager.shutdown();
        }
    }

    // --- Helper to add a target and wait for RUNNING ---
    private DataIndexTableBucket addTargetAndWaitRunning(int dataBucketId, int indexBucketId)
            throws Exception {
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, dataBucketId);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, indexBucketId);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);

        fetcherManager.startup();

        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(5000),
                "Target should become RUNNING");
        return dib;
    }

    @Test
    void testReconciliationWithCacheHit() throws Exception {
        // Cache returns valid leader — no ZK call should happen
        DataIndexTableBucket dib = addTargetAndWaitRunning(0, 9);

        // Verify: ZK was never called
        assertThat(fetcherManager.zkCallCount.get()).isZero();

        // Target should be RUNNING with correct leader
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target.getState()).isEqualTo(IndexFetcherTarget.State.RUNNING);
        assertThat(target.getLastKnownDataBucketLeaderId()).isEqualTo(LEADER_SERVER_ID);
    }

    @Test
    void testReconciliationWithCacheMiss() throws Exception {
        // Cache returns empty — should fall through to batch ZK
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // Set up batch ZK to return leader
        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        fetcherManager.zkLeaderProvider = buckets -> zkResult;

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Should eventually become RUNNING via ZK resolution
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(10000),
                "Target should become RUNNING via ZK resolution");

        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target.getLastKnownDataBucketLeaderId()).isEqualTo(LEADER_SERVER_ID);
    }

    @Test
    void testRunningTargetCacheMissZkConfirmsSameLeaderKeepsRunning() throws Exception {
        // Start with cache hit, target becomes RUNNING
        DataIndexTableBucket dib = addTargetAndWaitRunning(0, 9);

        // Now cache returns empty — simulates UpdateMetadata RPC failure
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        // ZK returns same leader (confirms it's still valid)
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        fetcherManager.zkLeaderProvider = buckets -> zkResult;

        // Trigger reconciliation
        fetcherManager.triggerReconciliation();

        // Phase 1 should add to runningNeedsZk, Phase 2 resolves via batch ZK,
        // Phase 3 confirms same leader -> keeps RUNNING
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(10_000),
                "Target should remain RUNNING after ZK confirms same leader");
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNotNull();
        // ZK confirmed same leader, so target should remain RUNNING (not FAILED)
        assertThat(target.getState()).isEqualTo(IndexFetcherTarget.State.RUNNING);
    }

    @Test
    void testBatchZkQueryDeduplication() throws Exception {
        // Two index buckets pointing to the same data bucket — ZK query should deduplicate
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket1 = new TableBucket(INDEX_TABLE_ID, 9);
        TableBucket indexBucket2 = new TableBucket(INDEX_TABLE_ID, 10);

        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    // Should only contain 1 unique data bucket, not 2
                    assertThat(buckets).hasSize(1);
                    assertThat(buckets).contains(dataBucket);
                    return zkResult;
                };

        // Add target via indexBucket1
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos1 = new HashMap<>();
        infos1.put(
                new DataIndexTableBucket(dataBucket, indexBucket1),
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket1, infos1);

        // Add target via indexBucket2 (same data bucket)
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos2 = new HashMap<>();
        infos2.put(
                new DataIndexTableBucket(dataBucket, indexBucket2),
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket2, infos2);

        fetcherManager.startup();

        waitUntil(
                () -> createdFetcherThreads.size() >= 1,
                Duration.ofMillis(10000),
                "Fetcher threads should be created");
    }

    @Test
    void testBatchZkQueryPartialFailure() throws Exception {
        // Batch ZK returns results for some buckets but not others
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket0 = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket dataBucket1 = new TableBucket(DATA_TABLE_ID, 1);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib0 = new DataIndexTableBucket(dataBucket0, indexBucket);
        DataIndexTableBucket dib1 = new DataIndexTableBucket(dataBucket1, indexBucket);

        // ZK only returns result for dataBucket0, not dataBucket1
        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket0,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        // dataBucket1 intentionally missing from zkResult
        fetcherManager.zkLeaderProvider = buckets -> zkResult;

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib0, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(dib1, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // dataBucket0 should eventually become RUNNING
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib0);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(10000),
                "Target for dataBucket0 should become RUNNING");

        // dataBucket1 should be FAILED (no ZK result)
        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dib1);
        assertThat(target1).isNotNull();
        assertThat(target1.getState()).isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    @Test
    void testStaleDetectionDuringResolve() throws Exception {
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // Make ZK call reset the target during Phase 2 (simulates concurrent modification)
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> newInfos =
                            new HashMap<>();
                    newInfos.put(
                            dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
                    fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, newInfos);

                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    result.put(
                            dataBucket,
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    0,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    0));
                    return result;
                };

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for reconciliation to process the stale ZK result
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null
                            && (t.getState() == IndexFetcherTarget.State.PENDING
                                    || t.getState() == IndexFetcherTarget.State.RUNNING);
                },
                Duration.ofMillis(10_000),
                "Target should be PENDING or RUNNING after stale detection");

        // Target should not be corrupted — stale detection should have skipped the
        // ZK result, and the target should have been re-processed cleanly.
        // State should be PENDING (from resetToPending) or RUNNING (if re-processed
        // in a subsequent cycle). FAILED would indicate stale detection is broken.
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNotNull();
        assertThat(target.getState())
                .as(
                        "Stale detection should prevent applying ZK result to a reset target. "
                                + "Expected PENDING or RUNNING, not FAILED.")
                .isIn(IndexFetcherTarget.State.PENDING, IndexFetcherTarget.State.RUNNING);
    }

    @Test
    void testTargetRemovedDuringResolve() throws Exception {
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // During ZK call, remove the target
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    fetcherManager.removeIndexBucket(indexBucket);
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    result.put(
                            dataBucket,
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    0,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    0));
                    return result;
                };

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for reconciliation to process — target should be removed gracefully
        waitUntil(
                () -> fetcherManager.findTargetForTest(dib) == null,
                Duration.ofMillis(10_000),
                "Target should be removed after reconciliation (no NPE)");

        // Target should be gone
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNull();
    }

    @Test
    void testAddTargetsDuringResolveNotBlocked() throws Exception {
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        AtomicBoolean addCompleted = new AtomicBoolean(false);
        CountDownLatch zkStarted = new CountDownLatch(1);

        // Make ZK call slow (500ms) and signal when it starts
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    zkStarted.countDown();
                    Thread.sleep(500);
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    result.put(
                            dataBucket,
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    0,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    0));
                    return result;
                };

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for ZK call to start (Phase 2 is running, lock is released)
        zkStarted.await(5, TimeUnit.SECONDS);

        // Now try to add a new target — should NOT block
        Thread adder =
                new Thread(
                        () -> {
                            TableBucket dataBucket2 = new TableBucket(DATA_TABLE_ID, 1);
                            TableBucket indexBucket2 = new TableBucket(INDEX_TABLE_ID, 10);
                            DataIndexTableBucket dib2 =
                                    new DataIndexTableBucket(dataBucket2, indexBucket2);
                            Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo>
                                    infos2 = new HashMap<>();
                            infos2.put(
                                    dib2,
                                    new IndexFetcherManager.FetcherTargetInfo(
                                            INDEX_TABLE_PATH, null));
                            fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket2, infos2);
                            addCompleted.set(true);
                        });
        adder.start();
        adder.join(200); // Should complete well within 200ms (ZK takes 500ms)

        assertThat(addCompleted.get())
                .as("addOrUpdateTargets should not be blocked during Phase 2 ZK call")
                .isTrue();
    }

    @Test
    void testEmptyZkResult() throws Exception {
        // ZK returns empty map — all targets should be marked FAILED
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();
        fetcherManager.zkLeaderProvider = buckets -> Collections.emptyMap();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for reconciliation to mark target as FAILED
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.FAILED;
                },
                Duration.ofMillis(10_000),
                "Target should become FAILED after ZK exception");

        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNotNull();
        assertThat(target.getState()).isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    @Test
    void testZkQueryException() throws Exception {
        // ZK throws exception — cache-hit targets should still be processed
        TableBucket dataBucket0 = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket dataBucket1 = new TableBucket(DATA_TABLE_ID, 1);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib0 = new DataIndexTableBucket(dataBucket0, indexBucket);
        DataIndexTableBucket dib1 = new DataIndexTableBucket(dataBucket1, indexBucket);

        // dataBucket0: cache hit (leader available)
        // dataBucket1: cache miss (needs ZK)
        fetcherManager.cachedLeaderProvider =
                bucket -> {
                    if (bucket.equals(dataBucket0)) {
                        return Optional.of(LEADER_SERVER_ID);
                    }
                    return Optional.empty();
                };

        // ZK throws exception
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    throw new RuntimeException("ZK session expired");
                };

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib0, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(dib1, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // dataBucket0 should become RUNNING (cache hit, no ZK needed)
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib0);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(5000),
                "Cache-hit target should become RUNNING despite ZK failure");

        // dataBucket1 should remain FAILED/PENDING (ZK failed)
        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dib1);
        assertThat(target1).isNotNull();
        assertThat(target1.getState())
                .isIn(IndexFetcherTarget.State.PENDING, IndexFetcherTarget.State.FAILED);
    }

    @Test
    void testRemoveTargetsDuringResolveNotBlocked() throws Exception {
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        AtomicBoolean removeCompleted = new AtomicBoolean(false);
        CountDownLatch zkStarted = new CountDownLatch(1);

        fetcherManager.zkLeaderProvider =
                buckets -> {
                    zkStarted.countDown();
                    Thread.sleep(500);
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    result.put(
                            dataBucket,
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    0,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    0));
                    return result;
                };

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        zkStarted.await(5, TimeUnit.SECONDS);

        Thread remover =
                new Thread(
                        () -> {
                            fetcherManager.removeIndexBucket(indexBucket);
                            removeCompleted.set(true);
                        });
        remover.start();
        remover.join(200);

        assertThat(removeCompleted.get())
                .as("removeIndexBucket should not be blocked during Phase 2 ZK call")
                .isTrue();
    }

    @Test
    void testLargeScaleTargetReconciliation() throws Exception {
        // 1000 targets all with cache miss — verify reconciliation completes
        // within reasonable time and does not block the lock
        fetcherManager.cachedLeaderProvider = bucket -> Optional.empty();

        // Batch ZK returns leaders for all buckets
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    LeaderAndIsr lai =
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    0,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    0);
                    for (TableBucket b : buckets) {
                        result.put(b, lai);
                    }
                    return result;
                };

        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, i);
            DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);
            infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        }
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);

        // Measure reconciliation time
        long start = System.currentTimeMillis();
        fetcherManager.startup();

        // Verify at least some targets become RUNNING within 15 seconds
        waitUntil(
                () -> createdFetcherThreads.size() >= 1,
                Duration.ofMillis(15000),
                "At least one fetcher thread should be created for 1000 targets");

        long elapsed = System.currentTimeMillis() - start;
        // Should complete well under 15 seconds — serial ZK would take minutes
        assertThat(elapsed)
                .as("Reconciliation should complete quickly with batch ZK")
                .isLessThan(15000);

        // Verify addOrUpdateTargets is not blocked during this
        AtomicBoolean addCompleted = new AtomicBoolean(false);
        Thread adder =
                new Thread(
                        () -> {
                            TableBucket newIndex = new TableBucket(INDEX_TABLE_ID, 99);
                            TableBucket newData = new TableBucket(DATA_TABLE_ID, 9999);
                            Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo>
                                    newInfos = new HashMap<>();
                            newInfos.put(
                                    new DataIndexTableBucket(newData, newIndex),
                                    new IndexFetcherManager.FetcherTargetInfo(
                                            INDEX_TABLE_PATH, null));
                            fetcherManager.addOrUpdateTargetsForIndexBucket(newIndex, newInfos);
                            addCompleted.set(true);
                        });
        adder.start();
        adder.join(1000);
        assertThat(addCompleted.get())
                .as("addOrUpdateTargets should not be blocked during large-scale reconciliation")
                .isTrue();
    }

    // --- Reconciliation bug tests (fluss-pushdown-test-zjk-1 incident, 2026-03-04) ---

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
        // Use FailingLeaderEndpoint to simulate dead server behavior
        fetcherManager.endpointFactory =
                fetcherId ->
                        new FailingLeaderEndpoint(
                                fetcherId.getServerId(),
                                new RuntimeException(
                                        "Server " + fetcherId.getServerId() + " not in cache"));

        // Default cachedLeaderProvider returns LEADER_SERVER_ID (18)
        fetcherManager.cachedLeaderProvider = bucket -> Optional.of(LEADER_SERVER_ID);

        // ZK returns LEADER_SERVER_ID as leader
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    LeaderAndIsr lai =
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    LeaderAndIsr.INITIAL_BUCKET_EPOCH);
                    for (TableBucket b : buckets) {
                        result.put(b, lai);
                    }
                    return result;
                };

        // Step 1: Add target
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Step 2: Start reconciliation - this will create fetcher thread and mark target RUNNING
        fetcherManager.startup();

        // Wait for reconciliation to process the target
        waitUntil(
                () -> !createdFetcherThreads.isEmpty(),
                Duration.ofMillis(5000),
                "Fetcher thread should be created");

        // Verify target is RUNNING
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
        assertThat(target).isNotNull();

        waitUntil(
                () ->
                        fetcherManager.findTargetForTest(dataIndexBucket).getState()
                                == IndexFetcherTarget.State.RUNNING,
                Duration.ofMillis(5000),
                "Target should be in RUNNING state");

        assertThat(target.getLastKnownDataBucketLeaderId()).isEqualTo(LEADER_SERVER_ID);

        // Step 3: Simulate server 18 crash - remove from server node cache
        // But metadataCache still returns server 18 as leader (race window)
        liveServers.remove(LEADER_SERVER_ID);
        // metadataCache still returns LEADER_SERVER_ID - this is the race condition

        // Step 4: Trigger reconciliation by waking up the reconciliation thread
        fetcherManager.triggerReconciliation();

        // Step 5: Wait and verify - target should be marked FAILED because server 18 is dead
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket);
                    return t != null && t.getState() == IndexFetcherTarget.State.FAILED;
                },
                Duration.ofMillis(10000),
                "Target should be marked FAILED when bound server is removed from cache. "
                        + "Bug 2: handleRunning() does not check server liveness in serverNodeCache.");
    }

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
     */
    @Test
    void testNetworkExceptionShouldReportToReconciliation() throws Exception {
        // Use FailingLeaderEndpoint to simulate dead server behavior
        fetcherManager.endpointFactory =
                fetcherId ->
                        new FailingLeaderEndpoint(
                                fetcherId.getServerId(),
                                new RuntimeException(
                                        "Server " + fetcherId.getServerId() + " not in cache"));

        // Default cachedLeaderProvider returns LEADER_SERVER_ID (18)
        fetcherManager.cachedLeaderProvider = bucket -> Optional.of(LEADER_SERVER_ID);

        // ZK returns LEADER_SERVER_ID as leader
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    LeaderAndIsr lai =
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    LeaderAndIsr.INITIAL_BUCKET_EPOCH);
                    for (TableBucket b : buckets) {
                        result.put(b, lai);
                    }
                    return result;
                };

        // Step 1: Add target
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dataIndexBucket = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> targetInfos =
                new HashMap<>();
        targetInfos.put(
                dataIndexBucket, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));

        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, targetInfos);

        // Step 2: Start reconciliation - creates fetcher thread, target becomes RUNNING
        fetcherManager.startup();

        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(5000),
                "Target should be in RUNNING state");

        IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
        assertThat(target.getLastKnownDataBucketLeaderId()).isEqualTo(LEADER_SERVER_ID);

        // Step 3: Simulate server 18 crash - remove from serverNodeCache
        liveServers.remove(LEADER_SERVER_ID);

        // Step 4: Run multiple reconciliation cycles
        for (int i = 0; i < 5; i++) {
            fetcherManager.triggerReconciliation();
        }
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket);
                    return t != null && t.getState() == IndexFetcherTarget.State.FAILED;
                },
                Duration.ofMillis(10_000),
                "Target should become FAILED after reconciliation cycles");

        // Step 5: Verify
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

    /**
     * Reproduces Bug 4: There is no mechanism to detect that an IndexFetcherThread is stuck in a
     * persistent failure loop. Even after multiple consecutive failures, the target remains in
     * RUNNING state and the fetcher thread keeps retrying against a dead server.
     */
    @Test
    void testFetcherThreadHealthCheckShouldDetectDeadServerThread() throws Exception {
        // Use FailingLeaderEndpoint to simulate dead server behavior
        fetcherManager.endpointFactory =
                fetcherId ->
                        new FailingLeaderEndpoint(
                                fetcherId.getServerId(),
                                new RuntimeException(
                                        "Server " + fetcherId.getServerId() + " not in cache"));

        // Default cachedLeaderProvider returns LEADER_SERVER_ID (18)
        fetcherManager.cachedLeaderProvider = bucket -> Optional.of(LEADER_SERVER_ID);

        // ZK returns LEADER_SERVER_ID as leader
        fetcherManager.zkLeaderProvider =
                buckets -> {
                    Map<TableBucket, LeaderAndIsr> result = new HashMap<>();
                    LeaderAndIsr lai =
                            new LeaderAndIsr(
                                    LEADER_SERVER_ID,
                                    LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                    Collections.singletonList(LEADER_SERVER_ID),
                                    0,
                                    LeaderAndIsr.INITIAL_BUCKET_EPOCH);
                    for (TableBucket b : buckets) {
                        result.put(b, lai);
                    }
                    return result;
                };

        // Add two targets on different data buckets, both fetching to the same index bucket
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
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t0 = fetcherManager.findTargetForTest(dataIndexBucket0);
                    IndexFetcherTarget t1 = fetcherManager.findTargetForTest(dataIndexBucket1);
                    return t0 != null
                            && t0.getState() == IndexFetcherTarget.State.RUNNING
                            && t1 != null
                            && t1.getState() == IndexFetcherTarget.State.RUNNING;
                },
                Duration.ofMillis(5000),
                "Both targets should be RUNNING");

        // Simulate server 18 crash
        liveServers.remove(LEADER_SERVER_ID);

        // Metadata cache update: dataBucket0's leader changes to 17, but dataBucket1's leader
        // is still reported as 18 (race condition)
        fetcherManager.cachedLeaderProvider =
                bucket -> {
                    if (bucket.equals(dataBucket0)) {
                        return Optional.of(NEW_LEADER_SERVER_ID);
                    }
                    return Optional.of(LEADER_SERVER_ID);
                };

        // Trigger reconciliation
        fetcherManager.triggerReconciliation();

        // Wait for dataBucket0's target to be marked FAILED (handleRunning detects leader change)
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t0 = fetcherManager.findTargetForTest(dataIndexBucket0);
                    return t0 != null && t0.getState() == IndexFetcherTarget.State.FAILED;
                },
                Duration.ofMillis(10000),
                "Target for dataBucket0 should be FAILED due to leader change");

        // Now check dataBucket1's target - it should ALSO be marked FAILED because
        // the fetcher thread is bound to dead server 18.
        for (int i = 0; i < 3; i++) {
            fetcherManager.triggerReconciliation();
        }
        waitUntil(
                () -> {
                    fetcherManager.triggerReconciliation();
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dataIndexBucket1);
                    return t != null && t.getState() == IndexFetcherTarget.State.FAILED;
                },
                Duration.ofMillis(10_000),
                "Target for dataBucket1 should become FAILED");

        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dataIndexBucket1);
        assertThat(target1).isNotNull();

        assertThat(target1.getState())
                .as(
                        "Bug 4: Target for dataBucket1 should be FAILED because the fetcher "
                                + "thread is bound to dead server %d which is no longer in "
                                + "serverNodeCache. There should be a health check that detects "
                                + "fetcher threads bound to dead servers and marks all their "
                                + "targets as FAILED.",
                        LEADER_SERVER_ID)
                .isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    /**
     * A testable subclass of IndexFetcherManager that overrides protected dependency methods to
     * avoid Mockito. All behavior is configurable via public fields.
     */
    private class TestableIndexFetcherManager extends IndexFetcherManager {

        /**
         * Configurable: returns leader ID for a given data bucket. Default: returns
         * LEADER_SERVER_ID.
         */
        volatile Function<TableBucket, Optional<Integer>> cachedLeaderProvider =
                bucket -> Optional.of(LEADER_SERVER_ID);

        /** Configurable: returns ZK leader results. Supports throwing exceptions. */
        volatile ZkLeaderProvider zkLeaderProvider = buckets -> Collections.emptyMap();

        /** Counter for ZK query calls (replaces Mockito verify). */
        final AtomicInteger zkCallCount = new AtomicInteger(0);

        /** Counter for invalidateCachedLeader calls. */
        final AtomicInteger invalidateCount = new AtomicInteger(0);

        /**
         * Configurable: creates a LeaderEndpoint for a fetcher thread. Default: NoOpLeaderEndpoint.
         */
        volatile Function<ServerIdAndFetcherId, LeaderEndpoint> endpointFactory =
                fetcherId -> new NoOpLeaderEndpoint(fetcherId.getServerId());

        TestableIndexFetcherManager(
                Configuration conf, Function<Integer, Optional<ServerNode>> serverNodeCache) {
            super(conf, null, LOCAL_SERVER_ID, null, null, null, serverNodeCache);
        }

        /** Alias for triggerReconciliationForTest(). */
        void triggerReconciliation() {
            triggerReconciliationForTest();
        }

        @Override
        protected Optional<Integer> getCachedLeaderId(TableBucket dataBucket) {
            return cachedLeaderProvider.apply(dataBucket);
        }

        @Override
        protected Map<TableBucket, LeaderAndIsr> queryZkLeaders(java.util.Set<TableBucket> buckets)
                throws Exception {
            zkCallCount.incrementAndGet();
            return zkLeaderProvider.query(buckets);
        }

        @Override
        protected TabletServerMetricGroup getServerMetricGroup() {
            return TestingMetricGroups.TABLET_SERVER_METRICS;
        }

        @Override
        protected void invalidateCachedLeader(TableBucket dataBucket) {
            invalidateCount.incrementAndGet();
        }

        @Override
        IndexFetcherThread createFetcherThread(ServerIdAndFetcherId fetcherId) {
            LeaderEndpoint endpoint = endpointFactory.apply(fetcherId);
            IndexFetcherThread thread =
                    new IndexFetcherThread(
                            "TestIndexFetcher-"
                                    + fetcherId.getFetcherId()
                                    + "-"
                                    + fetcherId.getServerId(),
                            null,
                            endpoint,
                            100,
                            conf,
                            TestingMetricGroups.TABLET_SERVER_METRICS);
            createdFetcherThreads.add(thread);
            return thread;
        }
    }

    /** Functional interface for configurable ZK leader queries. */
    @FunctionalInterface
    private interface ZkLeaderProvider {
        Map<TableBucket, LeaderAndIsr> query(java.util.Set<TableBucket> buckets) throws Exception;
    }
}
