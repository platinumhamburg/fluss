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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level coverage for the read-advance-persist helper used by the Coordinator's
 * {@code processDropPartition} (Plan 3 Task 7). The Coordinator wiring (calling this helper for
 * indexed main tables and shipping the new tombstone via {@code UpdateMetadataRequest}) is
 * verified by inspection; this test exercises the helper against a real ZooKeeper to guarantee
 * persistence, version-bumping and floor compaction across successive drops.
 */
class CoordinatorTombstoneOnDropPartitionTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    @BeforeAll
    static void beforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zkClient.close();
    }

    @Test
    void testFirstDropCreatesTombstoneInZk() throws Exception {
        TablePath tp = TablePath.of("db", "main");
        // Sanity check: znode does not exist yet, so a read returns EMPTY.
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(PartitionTombstone.EMPTY);

        PartitionTombstone updated =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 5L);

        assertThat(updated.getFloor()).isEqualTo(-1L);
        assertThat(updated.getExplicitSet()).containsExactly(5L);
        assertThat(updated.getVersion()).isEqualTo(1L);
        // ZK has the same value the helper returned.
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(updated);
    }

    @Test
    void testSuccessiveDropsAccumulateInExplicitSet() throws Exception {
        TablePath tp = TablePath.of("db", "main");
        PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 5L);
        PartitionTombstone afterTwo =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 7L);

        assertThat(afterTwo.getFloor()).isEqualTo(-1L);
        assertThat(afterTwo.getExplicitSet()).containsExactlyInAnyOrder(5L, 7L);
        assertThat(afterTwo.getVersion()).isEqualTo(2L);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(afterTwo);
    }

    @Test
    void testContiguousDropsCompactIntoFloor() throws Exception {
        TablePath tp = TablePath.of("db", "main");
        // Drop 0 first: floor should advance to 0 immediately because (-1)+1==0.
        PartitionTombstone afterZero =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 0L);
        assertThat(afterZero.getFloor()).isEqualTo(0L);
        assertThat(afterZero.getExplicitSet()).isEmpty();
        assertThat(afterZero.getVersion()).isEqualTo(1L);

        // Then drop 2 (non-contiguous): stays in explicit set.
        PartitionTombstone afterTwo =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 2L);
        assertThat(afterTwo.getFloor()).isEqualTo(0L);
        assertThat(afterTwo.getExplicitSet()).containsExactly(2L);
        assertThat(afterTwo.getVersion()).isEqualTo(2L);

        // Drop 1 closes the gap: floor compacts through 1 and 2.
        PartitionTombstone afterOne =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 1L);
        assertThat(afterOne.getFloor()).isEqualTo(2L);
        assertThat(afterOne.getExplicitSet()).isEmpty();
        assertThat(afterOne.getVersion()).isEqualTo(3L);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(afterOne);
    }

    @Test
    void testIdempotentDropStillBumpsVersion() throws Exception {
        // Re-dropping a partition already covered by floor must not lose history: the version
        // still advances so observers can detect a change. The shape of (floor, explicit) stays
        // identical to the previous snapshot.
        TablePath tp = TablePath.of("db", "main");
        PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 0L);
        PartitionTombstone afterRedrop =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 0L);
        assertThat(afterRedrop.getFloor()).isEqualTo(0L);
        assertThat(afterRedrop.getExplicitSet()).isEmpty();
        assertThat(afterRedrop.getVersion()).isEqualTo(2L);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(afterRedrop);
    }

    @Test
    void testTombstonesAreScopedPerTablePath() throws Exception {
        TablePath tpA = TablePath.of("db", "a");
        TablePath tpB = TablePath.of("db", "b");

        PartitionTombstone afterA =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tpA, 5L);
        PartitionTombstone afterB =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tpB, 9L);

        assertThat(afterA.getExplicitSet()).containsExactly(5L);
        assertThat(afterB.getExplicitSet()).containsExactly(9L);
        // Persistence is per-table: each znode keeps its own state.
        assertThat(zkClient.getPartitionTombstone(tpA)).isEqualTo(afterA);
        assertThat(zkClient.getPartitionTombstone(tpB)).isEqualTo(afterB);
    }
}
