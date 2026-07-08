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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level coverage for the read-advance-persist helper used by the Coordinator's {@code
 * processDropPartition}. The Coordinator wiring (calling this helper for indexed main tables and
 * shipping the new tombstone via {@code UpdateMetadataRequest}) is verified by inspection; this
 * test exercises the helper against a real ZooKeeper to guarantee persistence, version-bumping,
 * conservative fallback, and safe-floor compaction when an alive-partition snapshot is available.
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

        PartitionTombstone updated = PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 5L);

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
    void testLegacyDropsDoNotAdvanceFloorWithoutAliveSnapshot() throws Exception {
        TablePath tp = TablePath.of("db", "main");
        PartitionTombstone afterZero =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 0L);
        assertThat(afterZero.getFloor()).isEqualTo(-1L);
        assertThat(afterZero.getExplicitSet()).containsExactly(0L);
        assertThat(afterZero.getVersion()).isEqualTo(1L);

        PartitionTombstone afterTwo =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 2L);
        assertThat(afterTwo.getFloor()).isEqualTo(-1L);
        assertThat(afterTwo.getExplicitSet()).containsExactlyInAnyOrder(0L, 2L);
        assertThat(afterTwo.getVersion()).isEqualTo(2L);

        PartitionTombstone afterOne =
                PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tp, 1L);
        assertThat(afterOne.getFloor()).isEqualTo(-1L);
        assertThat(afterOne.getExplicitSet()).containsExactlyInAnyOrder(0L, 1L, 2L);
        assertThat(afterOne.getVersion()).isEqualTo(3L);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(afterOne);
    }

    @Test
    void testAdvanceAndPersistUsesAliveSnapshotToCompressSparseDroppedIds() throws Exception {
        TablePath tablePath = TablePath.of("default", "tombstone_safe_floor_sparse");
        zkClient.setOrCreatePartitionTombstone(
                tablePath, new PartitionTombstone(0L, asSet(2L, 10L), 3L));

        PartitionTombstone updated =
                PartitionTombstoneAdvancer.advanceAndPersist(
                        zkClient, tablePath, 100L, asSet(101L, 300L));

        assertThat(updated.getFloor()).isEqualTo(100L);
        assertThat(updated.getExplicitSet()).isEmpty();
        assertThat(updated.getVersion()).isEqualTo(4L);
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(updated);
    }

    @Test
    void testAdvanceAndPersistKeepsHighSparseDropExplicit() throws Exception {
        TablePath tablePath = TablePath.of("default", "tombstone_safe_floor_high_sparse");
        zkClient.setOrCreatePartitionTombstone(tablePath, PartitionTombstone.EMPTY);

        PartitionTombstone updated =
                PartitionTombstoneAdvancer.advanceAndPersist(
                        zkClient, tablePath, 100L, asSet(10L, 200L));

        assertThat(updated.getFloor()).isEqualTo(9L);
        assertThat(updated.getExplicitSet()).containsExactly(100L);
        assertThat(updated.getVersion()).isEqualTo(1L);
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(updated);
    }

    @Test
    void testAdvanceAndPersistFoldsEverythingWhenNoAlivePartitionRemains() throws Exception {
        TablePath tablePath = TablePath.of("default", "tombstone_safe_floor_empty_alive");
        zkClient.setOrCreatePartitionTombstone(
                tablePath, new PartitionTombstone(0L, asSet(2L, 50L), 3L));

        PartitionTombstone updated =
                PartitionTombstoneAdvancer.advanceAndPersist(
                        zkClient, tablePath, 100L, Collections.emptySet());

        assertThat(updated.getFloor()).isEqualTo(100L);
        assertThat(updated.getExplicitSet()).isEmpty();
        assertThat(updated.getVersion()).isEqualTo(4L);
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(updated);
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
        assertThat(afterRedrop.getFloor()).isEqualTo(-1L);
        assertThat(afterRedrop.getExplicitSet()).containsExactly(0L);
        assertThat(afterRedrop.getVersion()).isEqualTo(2L);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(afterRedrop);
    }

    @Test
    void testTombstonesAreScopedPerTablePath() throws Exception {
        TablePath tpA = TablePath.of("db", "a");
        TablePath tpB = TablePath.of("db", "b");

        PartitionTombstone afterA = PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tpA, 5L);
        PartitionTombstone afterB = PartitionTombstoneAdvancer.advanceAndPersist(zkClient, tpB, 9L);

        assertThat(afterA.getExplicitSet()).containsExactly(5L);
        assertThat(afterB.getExplicitSet()).containsExactly(9L);
        // Persistence is per-table: each znode keeps its own state.
        assertThat(zkClient.getPartitionTombstone(tpA)).isEqualTo(afterA);
        assertThat(zkClient.getPartitionTombstone(tpB)).isEqualTo(afterB);
    }

    @Test
    void testMetadataDropPartitionForIndexedTablePersistsTombstoneAtomically() throws Exception {
        MetadataManager metadataManager = newMetadataManager();
        TablePath tablePath = TablePath.of("db", "indexed_partitioned");
        metadataManager.createDatabase(
                tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("region", DataTypes.STRING())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .index("idx_region", "region")
                                        .build())
                        .distributedBy(1, "id")
                        .partitionedBy("dt")
                        .build();
        long tableId = metadataManager.createTable(tablePath, descriptor, null, false);
        PartitionAssignment assignment =
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0)));
        ResolvedPartitionSpec partition = ResolvedPartitionSpec.fromPartitionValue("dt", "2026");
        metadataManager.createPartition(tablePath, tableId, assignment, partition, false);
        PartitionRegistration registration =
                zkClient.getPartition(tablePath, partition.getPartitionName()).get();

        metadataManager.dropPartition(tablePath, partition, false);

        assertThat(zkClient.getPartition(tablePath, partition.getPartitionName())).isEmpty();
        assertThat(zkClient.getPartitionAssignment(registration.getPartitionId()))
                .contains(assignment);
        PartitionTombstone tombstone = zkClient.getPartitionTombstone(tablePath);
        assertThat(tombstone.isTombstoned(registration.getPartitionId())).isTrue();
        assertThat(tombstone.getVersion()).isEqualTo(1L);
    }

    private static Set<Long> asSet(long... values) {
        return LongStream.of(values).boxed().collect(Collectors.toSet());
    }

    private static MetadataManager newMetadataManager() {
        Configuration conf = new Configuration();
        return new MetadataManager(zkClient, conf, new LakeCatalogDynamicLoader(conf, null, true));
    }
}
