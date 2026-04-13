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
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the queryLeader() ZK fallback alternating bug WITHOUT using Mockito.
 *
 * <p>Bug: When metadataCache has stale leader info and the target is in FAILED state, queryLeader()
 * alternates between the correct leader (from ZK) and the stale leader (from cache) on every other
 * reconciliation cycle. Root cause: markFailed() does not update lastKnownDataBucketLeaderId, so
 * the ZK fallback condition {@code leaderIdOpt.get() == target.getLastKnownDataBucketLeaderId()}
 * only matches every other round.
 *
 * <p>Alternating cycle:
 *
 * <ol>
 *   <li>FAILED, lastKnownDataBucketLeader=18, cache=18 → cache==lastKnown → ZK fallback → gets 17
 *       (correct)
 *   <li>RUNNING with leader=17, handleRunning sees cache=18≠17 → marks FAILED
 *   <li>FAILED, lastKnownDataBucketLeader=17 (markFailed doesn't update it), cache=18 →
 *       cache≠lastKnown → NO ZK fallback → uses cache leader 18 (wrong)
 *   <li>RUNNING with leader=18, handleRunning sees cache=18==18 → does nothing, but server 18 is
 *       dead → checkFetcherThreadHealth marks FAILED
 *   <li>FAILED, lastKnownDataBucketLeader=18 → back to step 1
 * </ol>
 *
 * <p>This test uses:
 *
 * <ul>
 *   <li>Real ZooKeeper (via ZooKeeperExtension) with correct leader=17 written
 *   <li>A stub TabletServerMetadataCache subclass that always returns stale leader=18
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

    private final Map<Integer, ServerNode> liveServers = new ConcurrentHashMap<>();
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
                        LeaderAndIsr.INITIAL_BUCKET_EPOCH),
                ZkVersion.MATCH_ANY_VERSION.getVersion());

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
     * Verifies that the Plan-Resolve-Apply reconciliation consistently uses the correct leader from
     * ZK, never the stale leader from cache.
     *
     * <p>With the old single-phase reconciliation, queryLeader() would alternate between cache
     * (stale leader=18) and ZK (correct leader=17). The new three-phase model eliminates this by
     * always using batch ZK for cache-miss or stale-leader scenarios.
     *
     * <p>Expected: all fetcher creations use the correct leader=17 (from ZK), never the dead
     * server=18 (from stale cache).
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

        // Drive reconciliation synchronously, recording all leader assignments
        List<Integer> assignedLeaders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            fetcherManager.expireRetryBackoff(dataIndexBucket);
            IndexFetcherTarget target = fetcherManager.findTargetForTest(dataIndexBucket);
            IndexFetcherTarget.State stateBefore = target != null ? target.getState() : null;
            fetcherManager.reconcileSyncForTest();
            IndexFetcherTarget.State stateAfter = target != null ? target.getState() : null;
            // Record the leader on any transition to RUNNING
            if (stateAfter == IndexFetcherTarget.State.RUNNING
                    && stateBefore != IndexFetcherTarget.State.RUNNING) {
                assignedLeaders.add(target.getLastKnownDataBucketLeaderId());
            }
            // Allow fetcher thread time to execute a fetch cycle and report failure
            Thread.yield();
        }

        // Should have at least 1 assignment (PENDING→RUNNING on first cycle)
        assertThat(assignedLeaders)
                .as("Should have at least 1 RUNNING transition across reconciliation cycles")
                .isNotEmpty();

        // Core invariant: NO assignment should ever use the dead server (stale cache leader).
        // In the new Plan-Resolve-Apply model, the stale cache leader=18 is detected as dead
        // in planPendingOrFailed() (serverNodeCache check), so it always falls through to
        // batch ZK which returns the correct leader=17.
        assertThat(assignedLeaders)
                .as(
                        "No assignment should use the dead server %d (stale cache). "
                                + "All assignments should use the correct leader %d (from ZK). "
                                + "Assigned leader sequence: %s",
                        DEAD_SERVER_ID, CORRECT_LEADER_ID, assignedLeaders)
                .doesNotContain(DEAD_SERVER_ID);
    }

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
                            ? new NoOpLeaderEndpoint(leaderServerId, true)
                            : new FailingLeaderEndpoint(leaderServerId);

            // Use @VisibleForTesting constructor: pass null replicaManager (test endpoints
            // never reach replicaManager.getReplicaOrException) and metric group directly.
            return new IndexFetcherThread(
                    "TestIndexFetcher-" + fetcherId.getFetcherId() + "-" + leaderServerId,
                    null,
                    endpoint,
                    100,
                    new Configuration(),
                    TestingMetricGroups.TABLET_SERVER_METRICS);
        }

        /**
         * Expire the retry backoff for a target so it can be retried immediately. This avoids
         * waiting 5 seconds (MIN_RETRY_BACKOFF_MS) in tests.
         */
        void expireRetryBackoff(DataIndexTableBucket bucket) {
            IndexFetcherTarget target = findTargetForTest(bucket);
            if (target != null && target.getState() == IndexFetcherTarget.State.FAILED) {
                target.expireRetryBackoff();
            }
        }
    }
}
