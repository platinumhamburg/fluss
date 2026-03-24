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
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
 */
class IndexFetcherManagerPlanResolveApplyTest {

    private static final int LOCAL_SERVER_ID = 5;
    private static final int LEADER_SERVER_ID = 18;
    private static final int NEW_LEADER_SERVER_ID = 17;

    private static final long DATA_TABLE_ID = 122L;
    private static final long INDEX_TABLE_ID = 123L;

    private static final TablePath INDEX_TABLE_PATH = TablePath.of("test_db", "test_index_table");

    private Configuration conf;
    private TabletServerMetadataCache metadataCache;
    private ZooKeeperClient zkClient;
    private ReplicaManager replicaManager;

    private final Map<Integer, ServerNode> liveServers = MapUtils.newConcurrentHashMap();
    private Function<Integer, Optional<ServerNode>> serverNodeCache;
    private final List<IndexFetcherThread> createdFetcherThreads = new CopyOnWriteArrayList<>();

    private TestableIndexFetcherManager fetcherManager;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        metadataCache = mock(TabletServerMetadataCache.class);
        zkClient = mock(ZooKeeperClient.class);
        replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getServerMetricGroup())
                .thenReturn(TestingMetricGroups.TABLET_SERVER_METRICS);

        serverNodeCache = serverId -> Optional.ofNullable(liveServers.get(serverId));

        liveServers.put(
                LEADER_SERVER_ID,
                new ServerNode(LEADER_SERVER_ID, "host1", 9001, ServerType.TABLET_SERVER));
        liveServers.put(
                NEW_LEADER_SERVER_ID,
                new ServerNode(NEW_LEADER_SERVER_ID, "host2", 9002, ServerType.TABLET_SERVER));

        // Default: cache returns leader
        when(metadataCache.getBucketLeaderId(any(TableBucket.class)))
                .thenReturn(Optional.of(LEADER_SERVER_ID));

        // Default: batch ZK returns empty
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(Collections.emptyMap());

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

        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                5000,
                "Target should become RUNNING");
        return dib;
    }

    // ==================== Task 7: Basic Functionality ====================

    @Test
    void testReconciliationWithCacheHit() throws Exception {
        // Cache returns valid leader — no ZK call should happen
        DataIndexTableBucket dib = addTargetAndWaitRunning(0, 9);

        // Verify: getLeaderAndIsrs (batch) was never called
        verify(zkClient, never()).getLeaderAndIsrs(anyCollection());

        // Target should be RUNNING with correct leader
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target.getState()).isEqualTo(IndexFetcherTarget.State.RUNNING);
        assertThat(target.getLastKnownLeaderServerId()).isEqualTo(LEADER_SERVER_ID);
    }

    @Test
    void testReconciliationWithCacheMiss() throws Exception {
        // Cache returns empty — should fall through to batch ZK
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // Set up batch ZK to return leader
        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(zkResult);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Should eventually become RUNNING via ZK resolution
        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                10000,
                "Target should become RUNNING via ZK resolution");

        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target.getLastKnownLeaderServerId()).isEqualTo(LEADER_SERVER_ID);
    }

    @Test
    void testRunningTargetCacheMissZkConfirmsSameLeaderKeepsRunning() throws Exception {
        // Start with cache hit, target becomes RUNNING
        DataIndexTableBucket dib = addTargetAndWaitRunning(0, 9);

        // Now cache returns empty — simulates UpdateMetadata RPC failure
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        // ZK returns same leader (confirms it's still valid)
        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(zkResult);

        // Trigger reconciliation
        fetcherManager.triggerReconciliation();

        // Phase 1 should add to runningNeedsZk, Phase 2 resolves via batch ZK,
        // Phase 3 confirms same leader -> keeps RUNNING
        Thread.sleep(2000);
        fetcherManager.triggerReconciliation();
        Thread.sleep(2000);
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNotNull();
        // ZK confirmed same leader, so target should remain RUNNING (not FAILED)
        assertThat(target.getState()).isEqualTo(IndexFetcherTarget.State.RUNNING);
    }

    // ==================== Task 8: Batch ZK Query and Deduplication ====================

    @Test
    void testBatchZkQueryForMultipleTargets() throws Exception {
        // All cache misses — should trigger ONE batch ZK call, not N serial calls
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket0 = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket dataBucket1 = new TableBucket(DATA_TABLE_ID, 1);
        TableBucket dataBucket2 = new TableBucket(DATA_TABLE_ID, 2);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);

        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0);
        zkResult.put(dataBucket0, leaderAndIsr);
        zkResult.put(dataBucket1, leaderAndIsr);
        zkResult.put(dataBucket2, leaderAndIsr);
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(zkResult);

        // Add 3 targets
        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(
                new DataIndexTableBucket(dataBucket0, indexBucket),
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(
                new DataIndexTableBucket(dataBucket1, indexBucket),
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(
                new DataIndexTableBucket(dataBucket2, indexBucket),
                new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for at least one fetcher thread to be created
        waitForCondition(
                () -> createdFetcherThreads.size() >= 1,
                10000,
                "Fetcher threads should be created");

        // Verify: getLeaderAndIsrs called (batch), NOT getLeaderAndIsr (single)
        verify(zkClient, never()).getLeaderAndIsr(any(TableBucket.class));
    }

    @Test
    void testBatchZkQueryDeduplication() throws Exception {
        // Two index buckets pointing to the same data bucket — ZK query should deduplicate
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket1 = new TableBucket(INDEX_TABLE_ID, 9);
        TableBucket indexBucket2 = new TableBucket(INDEX_TABLE_ID, 10);

        Map<TableBucket, LeaderAndIsr> zkResult = new HashMap<>();
        zkResult.put(
                dataBucket,
                new LeaderAndIsr(
                        LEADER_SERVER_ID, 0, Collections.singletonList(LEADER_SERVER_ID), 0, 0));
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            Collection<TableBucket> buckets = invocation.getArgument(0);
                            // Should only contain 1 unique data bucket, not 2
                            assertThat(buckets).hasSize(1);
                            assertThat(buckets).contains(dataBucket);
                            return zkResult;
                        });

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

        waitForCondition(
                () -> createdFetcherThreads.size() >= 1,
                10000,
                "Fetcher threads should be created");
    }

    @Test
    void testBatchZkQueryPartialFailure() throws Exception {
        // Batch ZK returns results for some buckets but not others
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

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
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(zkResult);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib0, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(dib1, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // dataBucket0 should eventually become RUNNING
        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib0);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                10000,
                "Target for dataBucket0 should become RUNNING");

        // dataBucket1 should be FAILED (no ZK result)
        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dib1);
        assertThat(target1).isNotNull();
        assertThat(target1.getState()).isEqualTo(IndexFetcherTarget.State.FAILED);
    }

    // ==================== Task 9: Stale Detection and Lock Contention ====================

    @Test
    void testStaleDetectionDuringResolve() throws Exception {
        // Target is FAILED, cache miss triggers ZK query.
        // During Phase 2, addOrUpdateTargets resets target to PENDING.
        // Phase 3 should detect stale and skip.
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // Make ZK call slow so we can modify state during Phase 2
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
                            // Simulate slow ZK — during this time, reset the target externally
                            Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo>
                                    newInfos = new HashMap<>();
                            newInfos.put(
                                    dib,
                                    new IndexFetcherManager.FetcherTargetInfo(
                                            INDEX_TABLE_PATH, null));
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
                        });

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Let a few reconciliation cycles run
        Thread.sleep(3000);

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
        // Target removed during Phase 2 — Phase 3 should skip gracefully (no NPE)
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        // During ZK call, remove the target
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
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
                        });

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Let reconciliation run — should not throw NPE
        Thread.sleep(3000);

        // Target should be gone
        IndexFetcherTarget target = fetcherManager.findTargetForTest(dib);
        assertThat(target).isNull();
    }

    @Test
    void testAddTargetsDuringResolveNotBlocked() throws Exception {
        // Verify that addOrUpdateTargets is not blocked during Phase 2 (lock released)
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        AtomicBoolean addCompleted = new AtomicBoolean(false);
        CountDownLatch zkStarted = new CountDownLatch(1);

        // Make ZK call slow (500ms) and signal when it starts
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
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
                        });

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

    // ==================== Task 10: Boundary Conditions ====================

    @Test
    void testNoZkQueryNeeded() throws Exception {
        // All targets have cache hits — Phase 2 should be skipped entirely
        DataIndexTableBucket dib = addTargetAndWaitRunning(0, 9);

        // Verify no batch ZK call was made
        verify(zkClient, never()).getLeaderAndIsrs(anyCollection());
        verify(zkClient, never()).getLeaderAndIsr(any(TableBucket.class));
    }

    @Test
    void testEmptyZkResult() throws Exception {
        // ZK returns empty map — all targets should be marked FAILED
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());
        when(zkClient.getLeaderAndIsrs(anyCollection())).thenReturn(Collections.emptyMap());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // Wait for a few cycles
        Thread.sleep(3000);

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
        when(metadataCache.getBucketLeaderId(dataBucket0))
                .thenReturn(Optional.of(LEADER_SERVER_ID));
        when(metadataCache.getBucketLeaderId(dataBucket1)).thenReturn(Optional.empty());

        // ZK throws exception
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenThrow(new RuntimeException("ZK session expired"));

        Map<DataIndexTableBucket, IndexFetcherManager.FetcherTargetInfo> infos = new HashMap<>();
        infos.put(dib0, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        infos.put(dib1, new IndexFetcherManager.FetcherTargetInfo(INDEX_TABLE_PATH, null));
        fetcherManager.addOrUpdateTargetsForIndexBucket(indexBucket, infos);
        fetcherManager.startup();

        // dataBucket0 should become RUNNING (cache hit, no ZK needed)
        waitForCondition(
                () -> {
                    IndexFetcherTarget t = fetcherManager.findTargetForTest(dib0);
                    return t != null && t.getState() == IndexFetcherTarget.State.RUNNING;
                },
                5000,
                "Cache-hit target should become RUNNING despite ZK failure");

        // dataBucket1 should remain FAILED/PENDING (ZK failed)
        IndexFetcherTarget target1 = fetcherManager.findTargetForTest(dib1);
        assertThat(target1).isNotNull();
        assertThat(target1.getState())
                .isIn(IndexFetcherTarget.State.PENDING, IndexFetcherTarget.State.FAILED);
    }

    @Test
    void testRemoveTargetsDuringResolveNotBlocked() throws Exception {
        // Verify that removeIndexBucket is not blocked during Phase 2
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        TableBucket dataBucket = new TableBucket(DATA_TABLE_ID, 0);
        TableBucket indexBucket = new TableBucket(INDEX_TABLE_ID, 9);
        DataIndexTableBucket dib = new DataIndexTableBucket(dataBucket, indexBucket);

        AtomicBoolean removeCompleted = new AtomicBoolean(false);
        CountDownLatch zkStarted = new CountDownLatch(1);

        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
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
                        });

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
        when(metadataCache.getBucketLeaderId(any(TableBucket.class))).thenReturn(Optional.empty());

        // Batch ZK returns leaders for all buckets
        when(zkClient.getLeaderAndIsrs(anyCollection()))
                .thenAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            Collection<TableBucket> buckets = invocation.getArgument(0);
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
                        });

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
        waitForCondition(
                () -> createdFetcherThreads.size() >= 1,
                15000,
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

    // ==================== Helper Methods ====================

    private static void waitForCondition(
            java.util.function.BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timeout: " + message);
            }
            Thread.sleep(50);
        }
    }

    // ==================== Inner Classes ====================

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

        @Override
        public int leaderServerId() {
            return serverId;
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
            return FutureUtils.completedExceptionally(exception);
        }

        @Override
        public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
            return FutureUtils.completedExceptionally(exception);
        }

        @Override
        public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
            return FutureUtils.completedExceptionally(exception);
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
}
