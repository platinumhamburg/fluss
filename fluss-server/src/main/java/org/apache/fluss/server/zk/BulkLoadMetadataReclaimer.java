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

import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadBucketZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadOwnerZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTableZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTargetZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTransactionZNode;
import org.apache.fluss.server.zk.data.ZkData.TableIdZNode;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Reclaims transaction metadata without touching physical bucket recovery references. */
final class BulkLoadMetadataReclaimer {
    private static final int BUCKET_DELETE_BATCH_SIZE = 128;

    private final ZooKeeperClient client;
    private final CuratorFramework curator;
    private final ZooKeeperOp zkOp;
    private final BulkLoadOwnership ownership;

    BulkLoadMetadataReclaimer(
            ZooKeeperClient client,
            CuratorFramework curator,
            ZooKeeperOp zkOp,
            BulkLoadOwnership ownership) {
        this.client = client;
        this.curator = curator;
        this.zkOp = zkOp;
        this.ownership = ownership;
    }

    void maintain(long now, long retentionMillis, int epochVersion) throws Exception {
        Set<Long> liveTableIds = null;
        for (String table : client.getChildren(BulkLoadTableZNode.root())) {
            long tableId = Long.parseLong(table);
            List<String> transactions =
                    client.getChildren(BulkLoadTableZNode.transactions(tableId));
            if (transactions.isEmpty()
                    && curator.checkExists().forPath(TableIdZNode.path(tableId)) == null) {
                if (liveTableIds == null) {
                    liveTableIds = readLiveTableIds();
                }
                if (!liveTableIds.contains(tableId)) {
                    pruneTableDirectories(tableId, epochVersion);
                }
            }
            for (String id : transactions) {
                Optional<VersionedData<BulkLoadTransaction>> transaction =
                        client.getBulkLoadTransaction(tableId, id);
                if (!transaction.isPresent()) {
                    continue;
                }
                BulkLoadTransaction value = transaction.get().data();
                Optional<TableRegistration> registration = client.getTable(value.tablePath);
                if (!registration.isPresent() || registration.get().tableId != tableId) {
                    dropTable(tableId, now, epochVersion);
                    break;
                }
                if (value.state == BulkLoadTransaction.RUNNING) {
                    try {
                        client.withdrawDroppedBulkLoadTargets(transaction.get(), epochVersion);
                    } catch (KeeperException.BadVersionException
                            | KeeperException.NoNodeException
                            | BulkLoadNotFoundException ignored) {
                        // A concurrent pass or RPC changed the transaction.
                    }
                } else if (value.finishedTime != null
                        && value.finishedTime <= now - retentionMillis) {
                    reclaim(transaction.get(), epochVersion);
                }
            }
        }
    }

    void dropTable(long tableId, int epochVersion) throws Exception {
        dropTable(tableId, System.currentTimeMillis(), epochVersion);
    }

    private void dropTable(long tableId, long now, int epochVersion) throws Exception {
        for (String id : client.getChildren(BulkLoadTableZNode.transactions(tableId))) {
            Optional<VersionedData<BulkLoadTransaction>> transaction =
                    client.getBulkLoadTransaction(tableId, id);
            if (transaction.isPresent()) {
                VersionedData<BulkLoadTransaction> current = transaction.get();
                if (current.data().state == BulkLoadTransaction.RUNNING) {
                    try {
                        // Stop bucket mutations before the first deletion batch.
                        BulkLoadTransaction terminal =
                                client.finishBulkLoad(
                                        current,
                                        BulkLoadTransaction.ABORTED,
                                        now,
                                        null,
                                        epochVersion);
                        current = new VersionedData<>(terminal, current.zkVersion() + 1);
                    } catch (KeeperException.BadVersionException
                            | KeeperException.NoNodeException ignored) {
                        // Another writer changed this root. The next pass can reclaim it.
                        continue;
                    }
                }
                reclaim(current, epochVersion);
            }
        }
        pruneTableDirectories(tableId, epochVersion);
    }

    private void reclaim(VersionedData<BulkLoadTransaction> transaction, int epochVersion)
            throws Exception {
        BulkLoadTransaction value = transaction.data();
        long tableId = value.tableId;
        String id = value.bulkLoadId;
        String txnPath = BulkLoadTransactionZNode.path(tableId, id);
        CuratorOp txnCheck = zkOp.checkOp(txnPath, transaction.zkVersion());
        List<VersionedData<BulkLoadTarget>> targets = client.getBulkLoadTargets(value);
        List<CuratorOp> removals = new ArrayList<>();
        try {
            for (VersionedData<BulkLoadTarget> target : targets) {
                Long partitionId = target.data().partitionId;
                String buckets = BulkLoadBucketZNode.root(value, partitionId);
                deleteBuckets(buckets, txnCheck, epochVersion);
                removals.add(zkOp.deleteOp(buckets));
                removals.add(zkOp.deleteOp(BulkLoadTargetZNode.path(tableId, id, partitionId)));
            }
            // Keep owners and their terminal root together until this multi. A Begin that read
            // an old owner version must fail the root check if that owner path was recreated.
            for (VersionedData<BulkLoadTarget> target : targets) {
                String path = BulkLoadOwnerZNode.path(tableId, target.data().partitionId);
                Optional<VersionedData<String>> owner = ownership.getOwner(path);
                if (owner.isPresent() && owner.get().data().equals(id)) {
                    removals.add(
                            curator.transactionOp()
                                    .delete()
                                    .withVersion(owner.get().zkVersion())
                                    .forPath(path));
                }
            }
            if (!targets.isEmpty() && targets.get(0).data().partitionId != null) {
                removals.add(zkOp.deleteOp(BulkLoadTargetZNode.partitions(tableId, id)));
            }
            removals.add(zkOp.deleteOp(BulkLoadTargetZNode.root(tableId, id)));
            removals.add(
                    curator.transactionOp()
                            .delete()
                            .withVersion(transaction.zkVersion())
                            .forPath(txnPath));
            submit(removals, epochVersion);
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException ignored) {
            // A Begin replaced an owner, or another pass already removed these records.
        }
    }

    private void deleteBuckets(String path, CuratorOp txnCheck, int epochVersion) throws Exception {
        List<String> children = client.getChildren(path);
        for (int start = 0; start < children.size(); start += BUCKET_DELETE_BATCH_SIZE) {
            int end = Math.min(start + BUCKET_DELETE_BATCH_SIZE, children.size());
            List<CuratorOp> operations = new ArrayList<>(end - start + 1);
            operations.add(txnCheck);
            for (int i = start; i < end; i++) {
                operations.add(zkOp.deleteOp(path + "/" + children.get(i)));
            }
            submit(operations, epochVersion);
        }
    }

    private Set<Long> readLiveTableIds() throws Exception {
        List<TablePath> paths = new ArrayList<>();
        for (String database : client.listDatabases()) {
            for (String table : client.listTables(database)) {
                paths.add(TablePath.of(database, table));
            }
        }
        return client.getRawTableRegistrations(paths).values().stream()
                .map(registration -> registration.tableId)
                .collect(Collectors.toSet());
    }

    private void pruneTableDirectories(long tableId, int epochVersion) throws Exception {
        for (String path :
                new String[] {
                    BulkLoadOwnerZNode.partitions(tableId),
                    BulkLoadOwnerZNode.root(tableId),
                    BulkLoadTableZNode.transactions(tableId),
                    BulkLoadTableZNode.path(tableId)
                }) {
            try {
                submit(Collections.singletonList(zkOp.deleteOp(path)), epochVersion);
            } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignored) {
                // A directory may be absent or still contain records pending the next pass.
            }
        }
    }

    private void submit(List<CuratorOp> operations, int epochVersion) throws Exception {
        client.writeBulkLoadMetadata(
                client.wrapRequestsWithEpochCheck(operations, epochVersion), epochVersion);
    }
}
