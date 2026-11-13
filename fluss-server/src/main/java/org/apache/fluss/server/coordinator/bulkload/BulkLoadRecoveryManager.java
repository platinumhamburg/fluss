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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.CoordinatorEventManager;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RecoverySnapshot;
import org.apache.fluss.server.zk.data.ZkData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Retries independent cache adoption and leader recovery from physical bucket records. */
public final class BulkLoadRecoveryManager {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadRecoveryManager.class);
    private final ZooKeeperClient client;
    private final CoordinatorEventManager events;
    private final CoordinatorChannelManager channels;
    private final CompletedSnapshotStoreManager snapshots;
    private final Set<TableBucket> active = ConcurrentHashMap.newKeySet();
    private long lastScanTime;

    public BulkLoadRecoveryManager(
            ZooKeeperClient client,
            CoordinatorEventManager events,
            CoordinatorChannelManager channels,
            CompletedSnapshotStoreManager snapshots) {
        this.client = client;
        this.events = events;
        this.channels = channels;
        this.snapshots = snapshots;
    }

    public void request(TableBucket bucket) {
        active.add(bucket);
    }

    /** Scans IDs, processing one bucket at a time rather than retaining all snapshot file lists. */
    public boolean recoverImports(ZkEpoch epoch) throws Exception {
        long now = System.currentTimeMillis();
        if (lastScanTime == 0 || now - lastScanTime >= 60_000L) {
            scanImports(epoch);
            lastScanTime = now;
        }
        for (TableBucket bucket : new ArrayList<>(active)) {
            try {
                Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>> imported =
                        client.getRecoverySnapshot(bucket);
                if (!imported.isPresent() || !recover(bucket, imported.get(), epoch)) {
                    active.remove(bucket);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Cannot reconcile BulkLoad snapshot for {}; the next pass will retry.",
                        bucket,
                        e);
            }
        }
        return !active.isEmpty();
    }

    private void scanImports(ZkEpoch epoch) throws Exception {
        for (String table : client.getChildren(ZkData.TableIdsZNode.path())) {
            long tableId = Long.parseLong(table);
            recoverTarget(tableId, null, ZkData.TableIdZNode.path(tableId), epoch);
        }
        for (String partition : client.getChildren(ZkData.PartitionIdsZNode.path())) {
            long partitionId = Long.parseLong(partition);
            Optional<PartitionAssignment> assignment = client.getPartitionAssignment(partitionId);
            if (assignment.isPresent()) {
                recoverTarget(
                        assignment.get().getTableId(),
                        partitionId,
                        ZkData.PartitionIdZNode.path(partitionId),
                        epoch);
            }
        }
    }

    private void recoverTarget(long tableId, Long partitionId, String path, ZkEpoch epoch)
            throws Exception {
        for (String bucketId : client.getChildren(path + "/buckets")) {
            TableBucket bucket = new TableBucket(tableId, partitionId, Integer.parseInt(bucketId));
            try {
                Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>> imported =
                        client.getRecoverySnapshot(bucket);
                if (imported.isPresent()) {
                    active.add(bucket);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Cannot reconcile BulkLoad snapshot for {}; the next pass will retry.",
                        bucket,
                        e);
            }
        }
    }

    private boolean recover(
            TableBucket bucket,
            ZooKeeperClient.VersionedData<RecoverySnapshot> imported,
            ZkEpoch epoch)
            throws Exception {
        AccessContextEvent<NotifyLeaderAndIsrData> context =
                new AccessContextEvent<>(
                        ctx -> {
                            if (ctx.getCoordinatorZkVersion()
                                    != epoch.getCoordinatorEpochZkVersion()) {
                                return null;
                            }
                            TableInfo table = ctx.getTableInfoById(bucket.getTableId());
                            Optional<LeaderAndIsr> role = ctx.getBucketLeaderAndIsr(bucket);
                            if (table == null || !role.isPresent()) {
                                return null;
                            }
                            PhysicalTablePath path;
                            Integer count = table.getNumBuckets();
                            if (bucket.getPartitionId() == null) {
                                path = PhysicalTablePath.of(table.getTablePath());
                            } else {
                                Optional<PhysicalTablePath> partition =
                                        ctx.getPhysicalTablePath(bucket.getPartitionId());
                                if (!partition.isPresent()
                                        || !partition
                                                .get()
                                                .getTablePath()
                                                .equals(table.getTablePath())) {
                                    return null;
                                }
                                path = partition.get();
                                Integer partitionCount =
                                        ctx.getPartitionBucketCount(
                                                new TablePartition(
                                                        bucket.getTableId(),
                                                        bucket.getPartitionId()));
                                if (partitionCount != null) {
                                    count = partitionCount;
                                }
                            }
                            List<Integer> assignment = ctx.getAssignment(bucket);
                            if (assignment == null || assignment.isEmpty()) {
                                return null;
                            }
                            return new NotifyLeaderAndIsrData(
                                    path,
                                    bucket,
                                    assignment,
                                    role.get(),
                                    count,
                                    table.getBucketCountEpoch(),
                                    imported.data().snapshotId);
                        });
        events.put(context);
        NotifyLeaderAndIsrData data = context.getResultFuture().get();
        if (data == null) {
            return true;
        }
        boolean cacheFailed = false;
        try {
            BucketSnapshot source =
                    client.getTableBucketLatestSnapshot(bucket)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Import has no persistent recovery source."));
            CompletedSnapshotStore snapshotStore =
                    snapshots.getOrCreateCompletedSnapshotStore(
                            data.getPhysicalTablePath().getTablePath(), bucket);
            if (!snapshotStore.getActiveSnapshotIds().contains(source.getSnapshotId())) {
                CompletedSnapshot snapshot =
                        source.toCompletedSnapshotHandle().retrieveCompleteSnapshot();
                snapshotStore.addRegistered(snapshot);
            }
        } catch (Exception e) {
            cacheFailed = true;
            // Cache registration does not gate recovery from the independently durable reference.
            LOG.warn(
                    "Cannot register selected recovery snapshot for {}; registration will retry.",
                    bucket,
                    e);
        }
        NotifyLeaderAndIsrRequest request =
                ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest(
                        epoch.getCoordinatorEpoch(),
                        Collections.singletonList(
                                ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr(data)));
        PbNotifyLeaderAndIsrRespForBucket response =
                channels.bulkLoadRestore(data.getLeader(), request).getNotifyBucketsLeaderRespAt(0);
        if (response.hasErrorCode() && response.getErrorCode() != 0) {
            throw org.apache.fluss.rpc.protocol.Errors.forCode(response.getErrorCode())
                    .exception(response.hasErrorMessage() ? response.getErrorMessage() : null);
        }
        if (response.hasRecoveredSnapshotId()
                && response.getRecoveredSnapshotId() == imported.data().snapshotId
                && ServerRpcMessageUtils.toTableBucket(response.getTableBucket()).equals(bucket)) {
            Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>> current =
                    client.getRecoverySnapshot(bucket);
            return cacheFailed
                    || !current.isPresent()
                    || current.get().data().snapshotId != imported.data().snapshotId;
        }
        return true;
    }
}
