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

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.rebalance.RebalanceManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Canonical BulkLoad metadata setup and exact ZooKeeper snapshots for reassignment tests. */
final class BulkLoadReassignmentTestSupport {

    private BulkLoadReassignmentTestSupport() {}

    static void assertReassignmentRejected(
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            RebalanceManager rebalanceManager,
            String database,
            String remoteDataDir,
            TableDescriptor nonPartitionedDescriptor,
            int replacementServer,
            boolean partitioned,
            BulkLoadState state)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        database,
                        "bulk_load_reassignment_" + partitioned + "_" + state.name().toLowerCase());
        TableDescriptor descriptor =
                partitioned
                        ? TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("part", DataTypes.STRING())
                                                .primaryKey("id", "part")
                                                .build())
                                .distributedBy(1, "id")
                                .partitionedBy("part")
                                .build()
                                .withReplicationFactor(3)
                        : nonPartitionedDescriptor;
        TableAssignment tableAssignment =
                TableAssignment.builder().add(0, BucketAssignment.of(0, 1, 2)).build();
        long tableId =
                metadataManager.createTable(
                        tablePath,
                        remoteDataDir,
                        descriptor,
                        partitioned ? null : tableAssignment,
                        false);
        Long partitionId = null;
        String registrationPath = ZkData.TableZNode.path(tablePath);
        String assignmentPath = ZkData.TableIdZNode.path(tableId);
        if (partitioned) {
            String partitionName = "p0";
            metadataManager.createPartition(
                    tablePath,
                    tableId,
                    remoteDataDir,
                    new PartitionAssignment(tableId, tableAssignment.getBucketAssignments()),
                    ResolvedPartitionSpec.fromPartitionValue("part", partitionName),
                    false);
            PartitionRegistration partitionRegistration =
                    zkClient.getPartition(tablePath, partitionName)
                            .orElseThrow(AssertionError::new);
            partitionId = partitionRegistration.getPartitionId();
            registrationPath = ZkData.PartitionZNode.path(tablePath, partitionName);
            assignmentPath = ZkData.PartitionIdZNode.path(partitionId);
        }
        TableBucket bucket = new TableBucket(tableId, partitionId, 0);
        String bulkLoadId =
                String.format(
                        "550e8400-e29b-41d4-a716-%012d", (partitioned ? 10 : 0) + state.ordinal());
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        partitioned
                                ? PhysicalTablePath.of(tablePath, "p0")
                                : PhysicalTablePath.of(tablePath),
                        tableId,
                        partitionId,
                        bulkLoadId);
        markLoading(zkClient, registrationPath, tablePath, partitioned, bulkLoadId);
        String transactionPath =
                partitioned
                        ? ZkData.BulkLoadPartitionTransactionZNode.path(partitionId, bulkLoadId)
                        : ZkData.BulkLoadTableTransactionZNode.path(tableId, bulkLoadId);
        createZNode(
                zkClient,
                transactionPath,
                partitioned
                        ? ZkData.BulkLoadPartitionTransactionZNode.encode(
                                reassignmentTransaction(handle, registrationPath, state))
                        : ZkData.BulkLoadTableTransactionZNode.encode(
                                reassignmentTransaction(handle, registrationPath, state)));
        ZNodeSnapshot[] before =
                snapshots(
                        zkClient,
                        assignmentPath,
                        ZkData.LeaderAndIsrZNode.path(bucket),
                        registrationPath,
                        transactionPath);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(
                        bucket,
                        0,
                        0,
                        Arrays.asList(0, 1, 2),
                        Arrays.asList(0, 1, replacementServer));

        rebalanceManager.registerRebalance(
                "bulk-load-reassignment-" + bulkLoadId,
                Collections.singletonMap(bucket, plan),
                RebalanceStatus.NOT_STARTED);

        assertSnapshots(zkClient, before);
        clearBulkLoad(zkClient, registrationPath, tablePath, partitioned, transactionPath);
    }

    static void markLoading(
            ZooKeeperClient zkClient,
            String registrationPath,
            TablePath tablePath,
            boolean partitioned,
            String bulkLoadId)
            throws Exception {
        byte[] encoded;
        if (partitioned) {
            PartitionRegistration registration =
                    zkClient.getPartition(tablePath, "p0").orElseThrow(AssertionError::new);
            encoded =
                    ZkData.PartitionZNode.encode(
                            registration.withDataState(BulkLoadDataState.LOADING, bulkLoadId));
        } else {
            TableRegistration registration =
                    zkClient.getTable(tablePath).orElseThrow(AssertionError::new);
            encoded =
                    ZkData.TableZNode.encode(
                            registration.withDataState(BulkLoadDataState.LOADING, bulkLoadId));
        }
        zkClient.getCuratorClient().setData().forPath(registrationPath, encoded);
    }

    static void createZNode(ZooKeeperClient zkClient, String path, byte[] data) throws Exception {
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    static void clearBulkLoad(
            ZooKeeperClient zkClient,
            String registrationPath,
            TablePath tablePath,
            boolean partitioned,
            String transactionPath)
            throws Exception {
        byte[] encoded;
        if (partitioned) {
            PartitionRegistration registration =
                    zkClient.getPartition(tablePath, "p0").orElseThrow(AssertionError::new);
            encoded =
                    ZkData.PartitionZNode.encode(
                            registration.withDataState(BulkLoadDataState.ACTIVE, null));
        } else {
            TableRegistration registration =
                    zkClient.getTable(tablePath).orElseThrow(AssertionError::new);
            encoded =
                    ZkData.TableZNode.encode(
                            registration.withDataState(BulkLoadDataState.ACTIVE, null));
        }
        zkClient.getCuratorClient().setData().forPath(registrationPath, encoded);
        zkClient.getCuratorClient().delete().forPath(transactionPath);
    }

    static BulkLoadTransaction reassignmentTransaction(
            BulkLoadHandle handle, String registrationPath, BulkLoadState state) {
        return reassignmentTransaction(handle, registrationPath, state, 0);
    }

    static BulkLoadTransaction reassignmentTransaction(
            BulkLoadHandle handle,
            String registrationPath,
            BulkLoadState state,
            int fenceMetadataVersion) {
        boolean commitDecided = state == BulkLoadState.COMMITTING;
        return new BulkLoadTransaction(
                handle,
                state,
                "alice",
                "USER",
                "file:///tmp",
                1,
                registrationPath,
                fenceMetadataVersion,
                new long[] {1L},
                1L,
                1L,
                Long.MAX_VALUE,
                commitDecided ? Long.MAX_VALUE : null,
                null,
                commitDecided ? "file:///tmp/manifest.json" : null,
                commitDecided ? 1L : null,
                commitDecided ? repeat('a', 64) : null,
                null,
                null);
    }

    static ZNodeSnapshot[] snapshots(ZooKeeperClient zkClient, String... paths) throws Exception {
        ZNodeSnapshot[] result = new ZNodeSnapshot[paths.length];
        for (int i = 0; i < paths.length; i++) {
            result[i] = snapshot(zkClient, paths[i]);
        }
        return result;
    }

    static void assertSnapshots(ZooKeeperClient zkClient, ZNodeSnapshot... expected)
            throws Exception {
        for (ZNodeSnapshot snapshot : expected) {
            ZNodeSnapshot current = snapshot(zkClient, snapshot.path);
            assertThat(current.data).as(snapshot.path + " bytes").isEqualTo(snapshot.data);
            assertThat(current.version).as(snapshot.path + " version").isEqualTo(snapshot.version);
        }
    }

    private static ZNodeSnapshot snapshot(ZooKeeperClient zkClient, String path) throws Exception {
        Stat stat = new Stat();
        byte[] data = zkClient.getCuratorClient().getData().storingStatIn(stat).forPath(path);
        return new ZNodeSnapshot(path, data, stat.getVersion());
    }

    private static String repeat(char value, int count) {
        char[] characters = new char[count];
        Arrays.fill(characters, value);
        return new String(characters);
    }

    static final class ZNodeSnapshot {
        private final String path;
        private final byte[] data;
        private final int version;

        private ZNodeSnapshot(String path, byte[] data, int version) {
            this.path = path;
            this.data = data;
            this.version = version;
        }
    }
}
