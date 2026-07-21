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

package org.apache.fluss.server.zk;

import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ZooKeeperClient}'s {@link PartitionTombstone} CRUD methods. */
class ZooKeeperClientPartitionTombstoneTest {

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
    void testGetReturnsEmptyForUnknownTable() throws Exception {
        PartitionTombstone result = zkClient.getPartitionTombstone(TablePath.of("db", "t"));
        assertThat(result).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testRoundTripPersistsAndReadsBack() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        registerTable(tp, 1L);
        Set<Long> explicit = new HashSet<>();
        explicit.add(7L);
        PartitionTombstone original = new PartitionTombstone(5L, explicit, 3L);
        zkClient.setOrCreatePartitionTombstone(tp, original);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(original);
    }

    @Test
    void testOverwriteAdvancesPersistedValue() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        registerTable(tp, 1L);
        PartitionTombstone v1 = new PartitionTombstone(0L, Collections.emptySet(), 1L);
        PartitionTombstone v2 = new PartitionTombstone(5L, Collections.emptySet(), 2L);
        zkClient.setOrCreatePartitionTombstone(tp, v1);
        zkClient.setOrCreatePartitionTombstone(tp, v2);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(v2);
    }

    @Test
    void testDeleteRemovesTombstoneNode() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        registerTable(tp, 1L);
        zkClient.setOrCreatePartitionTombstone(
                tp, new PartitionTombstone(5L, Collections.emptySet(), 1L));
        zkClient.deletePartitionTombstone(tp);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testDeletePartitionAndSetTombstoneAreAtomicMetadataUpdate() throws Exception {
        TablePath tp = TablePath.of("db", "indexed_main");
        String partitionName = "p=2026";
        long tableId = 11L;
        long partitionId = 22L;
        registerTable(tp, tableId);
        PartitionAssignment assignment =
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0)));
        zkClient.registerPartitionAssignmentAndMetadata(
                partitionId,
                partitionName,
                assignment,
                zkClient.getDefaultRemoteDataDir(),
                tp,
                tableId,
                zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());

        PartitionTombstone updated =
                new PartitionTombstone(partitionId, Collections.emptySet(), 1L);
        ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("coordinator");
        zkClient.deletePartitionAndSetTombstone(
                tp,
                partitionName,
                tableId,
                partitionId,
                updated,
                Optional.empty(),
                epoch.getCoordinatorEpochZkVersion());

        assertThat(zkClient.getPartition(tp, partitionName)).isEmpty();
        assertThat(zkClient.getPartitionAssignment(partitionId)).contains(assignment);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(updated);
    }

    @Test
    void testStalePartitionDropDoesNotMutateRecreatedTable() throws Exception {
        TablePath tablePath = TablePath.of("db", "recreated_indexed_main");
        String partitionName = "p=2026";
        long oldTableId = 21L;
        long oldPartitionId = 22L;
        registerTable(tablePath, oldTableId);
        zkClient.registerPartitionAssignmentAndMetadata(
                oldPartitionId,
                partitionName,
                new PartitionAssignment(
                        oldTableId, Collections.singletonMap(0, BucketAssignment.of(0))),
                zkClient.getDefaultRemoteDataDir(),
                tablePath,
                oldTableId,
                zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());

        zkClient.deleteTable(tablePath);

        long newTableId = 31L;
        long newPartitionId = 32L;
        registerTable(tablePath, newTableId);
        zkClient.registerPartitionAssignmentAndMetadata(
                newPartitionId,
                partitionName,
                new PartitionAssignment(
                        newTableId, Collections.singletonMap(0, BucketAssignment.of(0))),
                zkClient.getDefaultRemoteDataDir(),
                tablePath,
                newTableId,
                zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());

        ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader("coordinator");
        assertThatThrownBy(
                        () ->
                                zkClient.deletePartitionAndSetTombstone(
                                        tablePath,
                                        partitionName,
                                        oldTableId,
                                        oldPartitionId,
                                        new PartitionTombstone(
                                                oldPartitionId, Collections.emptySet(), 1L),
                                        Optional.empty(),
                                        epoch.getCoordinatorEpochZkVersion()))
                .isInstanceOf(IllegalStateException.class);

        assertThat(zkClient.getPartition(tablePath, partitionName))
                .hasValueSatisfying(
                        partition ->
                                assertThat(partition.getPartitionId()).isEqualTo(newPartitionId));
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testStaleCoordinatorCannotDropIndexedPartition() throws Exception {
        TablePath tablePath = TablePath.of("db", "epoch_fenced_indexed_main");
        String partitionName = "p=2026";
        long tableId = 41L;
        long partitionId = 42L;
        registerTable(tablePath, tableId);
        zkClient.registerPartitionAssignmentAndMetadata(
                partitionId,
                partitionName,
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0))),
                zkClient.getDefaultRemoteDataDir(),
                tablePath,
                tableId,
                zkClient.getCurrentEpoch().getCoordinatorEpochZkVersion());
        ZkEpoch staleEpoch = zkClient.fenceBecomeCoordinatorLeader("old-coordinator");
        zkClient.fenceBecomeCoordinatorLeader("new-coordinator");

        assertThatThrownBy(
                        () ->
                                zkClient.deletePartitionAndSetTombstone(
                                        tablePath,
                                        partitionName,
                                        tableId,
                                        partitionId,
                                        new PartitionTombstone(
                                                partitionId, Collections.emptySet(), 1L),
                                        Optional.empty(),
                                        staleEpoch.getCoordinatorEpochZkVersion()))
                .isInstanceOf(
                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException
                                .BadVersionException.class);

        assertThat(zkClient.getPartition(tablePath, partitionName)).isPresent();
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testTombstoneWriteDoesNotRecreateDeletedTable() throws Exception {
        TablePath tablePath = TablePath.of("db", "deleted");
        registerTable(tablePath, 31L);
        zkClient.deleteTable(tablePath);

        assertThatThrownBy(
                        () ->
                                zkClient.setOrCreatePartitionTombstone(
                                        tablePath,
                                        new PartitionTombstone(1L, Collections.emptySet(), 1L)))
                .isInstanceOf(
                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException
                                .NoNodeException.class);
        assertThat(zkClient.getTable(tablePath)).isEmpty();
    }

    @Test
    void testStaleTombstoneSnapshotCannotOverwriteConcurrentUpdate() throws Exception {
        TablePath tablePath = TablePath.of("db", "tombstone_cas");
        registerTable(tablePath, 41L);
        Tuple2<PartitionTombstone, Optional<Integer>> stale =
                zkClient.getPartitionTombstoneWithVersion(tablePath);
        PartitionTombstone concurrent = new PartitionTombstone(-1L, Collections.singleton(7L), 1L);
        zkClient.setOrCreatePartitionTombstone(tablePath, concurrent);

        assertThatThrownBy(
                        () ->
                                zkClient.compareAndSetPartitionTombstone(
                                        tablePath,
                                        new PartitionTombstone(-1L, Collections.singleton(9L), 1L),
                                        stale.f1))
                .isInstanceOf(
                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException
                                .NodeExistsException.class);
        assertThat(zkClient.getPartitionTombstone(tablePath)).isEqualTo(concurrent);
    }

    private static void registerTable(TablePath tablePath, long tableId) throws Exception {
        zkClient.fenceBecomeCoordinatorLeader("test-coordinator");
        if (!zkClient.getDatabase(tablePath.getDatabaseName()).isPresent()) {
            zkClient.registerDatabase(
                    tablePath.getDatabaseName(),
                    DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
        }
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        zkClient.registerTable(
                tablePath,
                TableRegistration.newTable(
                        tableId,
                        zkClient.getDefaultRemoteDataDir(),
                        TableDescriptor.builder().schema(schema).distributedBy(1, "id").build()));
    }
}
