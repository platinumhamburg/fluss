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

import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadBucketZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadOwnerZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTableZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTargetZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTransactionZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.TableZNode;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Acquires fixed target ownership atomically and resolves running owners. */
final class BulkLoadOwnership {
    private final ZooKeeperClient client;
    private final CuratorFramework curator;
    private final ZooKeeperOp zkOp;

    BulkLoadOwnership(ZooKeeperClient client, CuratorFramework curator, ZooKeeperOp zkOp) {
        this.client = client;
        this.curator = curator;
        this.zkOp = zkOp;
    }

    void begin(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> targets,
            int tableZkVersion,
            Map<Long, Integer> partitionZkVersions,
            int epochVersion)
            throws Exception {
        long tableId = transaction.tableId;
        String id = transaction.bulkLoadId;
        ensureBulkLoadDirectory(BulkLoadTableZNode.transactions(tableId));
        ensureBulkLoadDirectory(BulkLoadOwnerZNode.partitions(tableId));
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(zkOp.checkOp(TableZNode.path(transaction.tablePath), tableZkVersion));
        ops.add(
                zkOp.createOp(
                        BulkLoadTransactionZNode.path(tableId, id),
                        BulkLoadTransactionZNode.encode(transaction),
                        CreateMode.PERSISTENT));
        byte[] empty = new byte[0];
        ops.add(zkOp.createOp(BulkLoadTargetZNode.root(tableId, id), empty, CreateMode.PERSISTENT));
        if (!partitionZkVersions.isEmpty()) {
            ops.add(
                    zkOp.createOp(
                            BulkLoadTargetZNode.partitions(tableId, id),
                            empty,
                            CreateMode.PERSISTENT));
        }
        byte[] ownerBytes = BulkLoadOwnerZNode.encode(id);
        for (BulkLoadTarget target : targets) {
            Long partitionId = target.partitionId;
            if (partitionId != null) {
                ops.add(
                        zkOp.checkOp(
                                PartitionZNode.path(transaction.tablePath, target.partitionName),
                                partitionZkVersions.get(partitionId)));
            }
            String ownerPath = BulkLoadOwnerZNode.path(tableId, partitionId);
            Optional<VersionedData<String>> owner = getOwner(ownerPath);
            if (owner.isPresent()) {
                String oldId = owner.get().data();
                String oldPath = BulkLoadTransactionZNode.path(tableId, oldId);
                VersionedData<BulkLoadTransaction> oldTransaction =
                        client.getBulkLoadTransaction(tableId, oldId)
                                .orElseThrow(
                                        () -> new KeeperException.BadVersionException(oldPath));
                if (oldTransaction.data().state == BulkLoadTransaction.RUNNING) {
                    throw new BulkLoadConflictException(
                            "BulkLoad target is owned by " + oldId + ".");
                }
                ops.add(zkOp.checkOp(oldPath, oldTransaction.zkVersion()));
                ops.add(zkOp.updateOp(ownerPath, ownerBytes, owner.get().zkVersion()));
            } else {
                ops.add(zkOp.createOp(ownerPath, ownerBytes, CreateMode.PERSISTENT));
            }
            ops.add(
                    zkOp.createOp(
                            BulkLoadTargetZNode.path(tableId, id, partitionId),
                            BulkLoadTargetZNode.encode(target),
                            CreateMode.PERSISTENT));
            ops.add(
                    zkOp.createOp(
                            BulkLoadBucketZNode.root(transaction, partitionId),
                            empty,
                            CreateMode.PERSISTENT));
        }
        List<CuratorOp> fencedOperations = client.wrapRequestsWithEpochCheck(ops, epochVersion);
        try {
            client.writeBulkLoadMetadata(fencedOperations, epochVersion);
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
            String ownerRoot = BulkLoadOwnerZNode.root(tableId) + "/";
            for (int i = 0; i < fencedOperations.size(); i++) {
                if (ZooKeeperClient.failedMultiOperation(e, i)
                        && fencedOperations
                                .get(i)
                                .getTypeAndPath()
                                .getForPath()
                                .startsWith(ownerRoot)) {
                    throw new BulkLoadConflictException(
                            "BulkLoad target ownership changed concurrently. "
                                    + "Use ListInProgressBulkLoads to find the current owner.");
                }
            }
            throw e;
        }
    }

    private void ensureBulkLoadDirectory(String path) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // Concurrent Begin calls share the organizing directories.
        }
    }

    Optional<VersionedData<String>> getOwner(String path) throws Exception {
        return client.getVersionedData(path, BulkLoadOwnerZNode::decode);
    }

    List<BulkLoadTransaction> list(long tableId, List<Long> partitionIds) throws Exception {
        Map<String, BulkLoadTransaction> result = new LinkedHashMap<>();
        for (Long partitionId : partitionIds) {
            Optional<VersionedData<String>> owner =
                    getOwner(BulkLoadOwnerZNode.path(tableId, partitionId));
            if (owner.isPresent() && !result.containsKey(owner.get().data())) {
                client.getBulkLoadTransaction(tableId, owner.get().data())
                        .map(VersionedData::data)
                        .filter(transaction -> transaction.state == BulkLoadTransaction.RUNNING)
                        .ifPresent(transaction -> result.put(transaction.bulkLoadId, transaction));
            }
        }
        return new ArrayList<>(result.values());
    }
}
