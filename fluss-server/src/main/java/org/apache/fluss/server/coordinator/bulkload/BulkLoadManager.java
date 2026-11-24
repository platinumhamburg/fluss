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

import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoResponse;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetProgress;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.event.CommitBulkLoadBucketEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.concurrent.FutureUtils.runAsync;

/** Manages BulkLoad transactions and the snapshots imported into their buckets. */
public final class BulkLoadManager {
    private final ZooKeeperClient zooKeeperClient;
    private final int epochVersion;
    private final Executor ioExecutor;

    public BulkLoadManager(ZooKeeperClient zooKeeperClient, int epochVersion, Executor ioExecutor) {
        this.zooKeeperClient = zooKeeperClient;
        this.epochVersion = epochVersion;
        this.ioExecutor = ioExecutor;
    }

    /** Establishes ownership of the selected physical targets and fixes their build context. */
    public BeginBulkLoadResponse begin(TablePath path, BeginBulkLoadRequest request, int maxTargets)
            throws Exception {
        List<String> names = request.getPartitionNamesList();
        requireDistinctPartitions(names);
        String id =
                request.hasBulkLoadId() ? request.getBulkLoadId() : UUID.randomUUID().toString();
        VersionedData<TableRegistration> registration = getTableByPath(path);
        long tableId = registration.data().tableId;
        Optional<VersionedData<BulkLoadTransaction>> previous =
                zooKeeperClient.getBulkLoadTransaction(tableId, id);
        if (previous.isPresent()) {
            return replayBegin(previous.get().data(), names);
        }

        TableInfo table =
                registration
                        .data()
                        .toTableInfo(
                                path, getSchema(path, zooKeeperClient.getCurrentSchemaId(path)));
        if (!table.hasPrimaryKey()) {
            throw new InvalidRequestException("BulkLoad requires a primary key table.");
        }
        if (!table.getSchema().getAutoIncrementColumnNames().isEmpty()) {
            throw new InvalidRequestException("BulkLoad does not support auto-increment columns.");
        }
        if (table.getTableConfig().getMergeEngineType().isPresent()) {
            throw new InvalidRequestException(
                    "BulkLoad does not support tables with a merge engine.");
        }
        if (table.isPartitioned() == names.isEmpty()) {
            throw new InvalidRequestException("Invalid partition selection for this table.");
        }
        if (Math.max(1, names.size()) > maxTargets) {
            throw new InvalidRequestException("BulkLoad exceeds max-targets.");
        }

        Map<Long, Integer> partitionVersions = new HashMap<>();
        List<BulkLoadTarget> targets = resolveBeginTargets(table, names, partitionVersions);
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        tableId,
                        id,
                        path,
                        table.getSchemaId(),
                        table.getRemoteDataDir(),
                        BulkLoadTransaction.RUNNING,
                        System.currentTimeMillis(),
                        null,
                        null);
        try {
            zooKeeperClient.beginBulkLoad(
                    transaction,
                    targets,
                    registration.zkVersion(),
                    partitionVersions,
                    epochVersion);
        } catch (KeeperException.NodeExistsException
                | KeeperException.BadVersionException
                | BulkLoadConflictException e) {
            Optional<VersionedData<BulkLoadTransaction>> current =
                    zooKeeperClient.getBulkLoadTransaction(tableId, id);
            if (current.isPresent()) {
                return replayBegin(current.get().data(), names);
            }
            throw e;
        }
        return beginResponse(transaction, targets, table);
    }

    /** Returns progress for the original targets, including withdrawn partitions. */
    public GetBulkLoadInfoResponse info(long tableId, String bulkLoadId) throws Exception {
        VersionedData<BulkLoadTransaction> resolved = getTransaction(tableId, bulkLoadId);
        getOriginalTable(resolved.data());
        BulkLoadTransaction transaction =
                zooKeeperClient.withdrawDroppedBulkLoadTargets(resolved, epochVersion).data();
        GetBulkLoadInfoResponse response =
                new GetBulkLoadInfoResponse().setStatus(status(transaction));
        for (BulkLoadTarget target : getTargets(transaction)) {
            long allocated = 0;
            long committed = 0;
            for (int id : zooKeeperClient.getBulkLoadBucketIds(transaction, target.partitionId)) {
                TableBucket bucket = new TableBucket(tableId, target.partitionId, id);
                Optional<BulkLoadBucket> record =
                        zooKeeperClient.getBulkLoadBucket(transaction, bucket);
                if (record.isPresent()) {
                    allocated++;
                    if (record.get().state == BulkLoadBucket.COMMITTED) {
                        committed++;
                    }
                }
            }
            PbBulkLoadTargetProgress progress =
                    response.addTarget()
                            .setBucketCount(target.bucketCount)
                            .setAllocatedBucketCount(allocated)
                            .setCommittedBucketCount(committed)
                            .setWithdrawn(target.withdrawn);
            if (target.partitionId != null) {
                progress.setPartitionId(target.partitionId);
            }
        }
        return response;
    }

    /** Lists the running transactions owning any of the selected current physical targets. */
    public ListInProgressBulkLoadsResponse list(TablePath path, List<String> names)
            throws Exception {
        requireDistinctPartitions(names);
        TableRegistration table = getTableByPath(path).data();
        if (!table.isPartitioned() && !names.isEmpty()) {
            throw new InvalidRequestException("Non-partitioned target has partition names.");
        }

        List<Long> ids = new ArrayList<>();
        if (table.isPartitioned()) {
            Collection<String> selected =
                    names.isEmpty() ? zooKeeperClient.getPartitions(path) : names;
            for (String name : selected) {
                Optional<PartitionRegistration> partition =
                        zooKeeperClient
                                .getPartition(path, name)
                                .filter(registration -> registration.getTableId() == table.tableId);
                if (partition.isPresent()) {
                    ids.add(partition.get().getPartitionId());
                } else if (!names.isEmpty()) {
                    throw partitionNotFound(name);
                }
            }
        } else {
            ids.add(null);
        }

        ListInProgressBulkLoadsResponse response = new ListInProgressBulkLoadsResponse();
        for (BulkLoadTransaction transaction : zooKeeperClient.listBulkLoads(table.tableId, ids)) {
            response.addTransaction().copyFrom(status(transaction));
        }
        return response;
    }

    /** Commits a completed transaction, or replays its original commit result. */
    public PbBulkLoadStatus commit(long tableId, String bulkLoadId) throws Exception {
        return finish(tableId, bulkLoadId, BulkLoadTransaction.COMMITTED, null);
    }

    /** Aborts a running transaction, preserving the first successful abort explanation. */
    public PbBulkLoadStatus abort(long tableId, String bulkLoadId, @Nullable String message)
            throws Exception {
        return finish(tableId, bulkLoadId, BulkLoadTransaction.ABORTED, message);
    }

    /**
     * Allocates a snapshot at the bucket's current recovery offset, or returns its existing
     * allocation.
     */
    public BulkLoadBucket allocateBucketSnapshot(String bulkLoadId, TableBucket bucket)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction original = transaction.data();
        VersionedData<TableRegistration> table = getOriginalTable(original);
        BulkLoadBucket existing = zooKeeperClient.getBulkLoadBucket(original, bucket).orElse(null);
        if (existing != null && existing.state == BulkLoadBucket.COMMITTED) {
            return existing;
        }

        VersionedData<BulkLoadTarget> target = resolveTarget(original, bucket);
        requireRunning(original);
        Integer partitionVersion = validateBuildContext(original, target.data(), table.data());
        if (existing != null) {
            return existing;
        }
        long snapshotId = zooKeeperClient.allocateBucketSnapshotId(bucket);
        long logOffset =
                zooKeeperClient
                        .getTableBucketLatestSnapshot(bucket)
                        .map(BucketSnapshot::getLogOffset)
                        .orElse(0L);
        BulkLoadBucket allocation =
                new BulkLoadBucket(snapshotId, logOffset, BulkLoadBucket.ALLOCATED);
        try {
            zooKeeperClient.allocateBulkLoadBucket(
                    transaction,
                    target,
                    bucket,
                    allocation,
                    table.zkVersion(),
                    partitionVersion,
                    epochVersion);
            return allocation;
        } catch (KeeperException.NodeExistsException e) {
            // The preceding checks succeeded; only creating the bucket record can report
            // NodeExists.
            Optional<BulkLoadBucket> winner = zooKeeperClient.getBulkLoadBucket(original, bucket);
            if (winner.isPresent()) {
                return winner.get();
            }
            checkMutationTarget(original, target.data());
            throw e;
        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
            checkMutationTarget(original, target.data());
            throw e;
        }
    }

    /** Publishes an allocated snapshot, or acknowledges an already committed bucket. */
    public CompletableFuture<Void> commitBucketSnapshot(
            String bulkLoadId,
            TableBucket bucket,
            CompletedSnapshotStoreManager snapshotStores,
            EventManager eventManager)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction bulkLoadTransaction = transaction.data();
        VersionedData<TableRegistration> currentTable = getOriginalTable(bulkLoadTransaction);
        Optional<BulkLoadBucket> record =
                zooKeeperClient.getBulkLoadBucket(bulkLoadTransaction, bucket);
        if (record.isPresent() && record.get().state == BulkLoadBucket.COMMITTED) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedData<BulkLoadTarget> target = resolveTarget(bulkLoadTransaction, bucket);
        BulkLoadBucket allocation =
                record.orElseThrow(
                        () -> new InvalidRequestException("Bucket has not been allocated."));
        TablePath tablePath = bulkLoadTransaction.tablePath;
        requireRunning(bulkLoadTransaction);
        Integer partitionVersion =
                validateBuildContext(bulkLoadTransaction, target.data(), currentTable.data());
        CompletedSnapshot snapshot =
                BulkLoadSnapshotValidator.validate(tablePath, target.data(), bucket, allocation);
        CommitBulkLoadBucketEvent event =
                new CommitBulkLoadBucketEvent(
                        snapshot,
                        transaction,
                        target,
                        currentTable.zkVersion(),
                        partitionVersion,
                        epochVersion,
                        snapshotStores.getOrCreateCompletedSnapshotStore(tablePath, bucket));
        eventManager.put(event);
        return event.result
                .handle(
                        (ignored, failure) -> {
                            if (failure instanceof KeeperException.NoNodeException
                                    || failure instanceof KeeperException.BadVersionException) {
                                return runAsync(
                                        () -> {
                                            checkMutationTarget(bulkLoadTransaction, target.data());
                                            throw (KeeperException) failure;
                                        },
                                        ioExecutor);
                            }
                            return event.result;
                        })
                .thenCompose(result -> result);
    }

    private VersionedData<TableRegistration> getTableByPath(TablePath path) throws Exception {
        return zooKeeperClient
                .getTableWithVersion(path)
                .orElseThrow(
                        () -> new TableNotExistException("Table " + path + " does not exist."));
    }

    private SchemaInfo getSchema(TablePath path, int schemaId) throws Exception {
        return zooKeeperClient
                .getSchemaById(path, schemaId)
                .orElseThrow(
                        () ->
                                new SchemaNotExistException(
                                        "Schema "
                                                + schemaId
                                                + " of table "
                                                + path
                                                + " does not exist."));
    }

    private BeginBulkLoadResponse replayBegin(BulkLoadTransaction transaction, List<String> names)
            throws Exception {
        TableRegistration table = getOriginalTable(transaction).data();
        List<BulkLoadTarget> targets = getTargets(transaction);
        Set<String> original = new HashSet<>();
        for (BulkLoadTarget target : targets) {
            if (target.partitionName != null) {
                original.add(target.partitionName);
            }
        }
        if (!original.equals(new HashSet<>(names))) {
            throw new InvalidRequestException("bulk_load_id has different targets.");
        }
        SchemaInfo schema = getSchema(transaction.tablePath, transaction.schemaId);
        return beginResponse(
                transaction, targets, table.toTableInfo(transaction.tablePath, schema));
    }

    private List<BulkLoadTarget> resolveBeginTargets(
            TableInfo table, List<String> names, Map<Long, Integer> partitionVersions)
            throws Exception {
        if (!table.isPartitioned()) {
            return Collections.singletonList(
                    new BulkLoadTarget(
                            null, null, table.getNumBuckets(), table.getRemoteDataDir(), false));
        }
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (String name : names) {
            if (HISTORICAL_PARTITION_VALUE.equals(name)) {
                throw new InvalidRequestException("Historical partitions are unsupported.");
            }
            VersionedData<PartitionRegistration> partition =
                    zooKeeperClient
                            .getPartitionWithVersion(table.getTablePath(), name)
                            .filter(data -> data.data().getTableId() == table.getTableId())
                            .orElseThrow(() -> partitionNotFound(name));
            PartitionRegistration registration = partition.data();
            long partitionId = registration.getPartitionId();
            partitionVersions.put(partitionId, partition.zkVersion());
            targets.add(
                    new BulkLoadTarget(
                            partitionId,
                            name,
                            registration.getBucketCountOrDefault(
                                    table.getNumBuckets(), table.getBucketCountEpoch()),
                            registration.getRemoteDataDir(),
                            false));
        }
        targets.sort(Comparator.comparing(target -> target.partitionId));
        return targets;
    }

    private List<BulkLoadTarget> getTargets(BulkLoadTransaction transaction) throws Exception {
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (VersionedData<BulkLoadTarget> target :
                zooKeeperClient.getBulkLoadTargets(transaction)) {
            targets.add(target.data());
        }
        return targets;
    }

    private PbBulkLoadStatus finish(
            long tableId, String bulkLoadId, int state, @Nullable String message) throws Exception {
        VersionedData<BulkLoadTransaction> resolved = getTransaction(tableId, bulkLoadId);
        getOriginalTable(resolved.data());
        VersionedData<BulkLoadTransaction> transaction =
                zooKeeperClient.withdrawDroppedBulkLoadTargets(resolved, epochVersion);
        BulkLoadTransaction original = transaction.data();
        if (original.state == state) {
            return status(original);
        }
        requireRunning(original);
        try {
            return status(
                    zooKeeperClient.finishBulkLoad(
                            transaction, state, System.currentTimeMillis(), message, epochVersion));
        } catch (KeeperException.BadVersionException e) {
            BulkLoadTransaction current = getTransaction(tableId, bulkLoadId).data();
            getOriginalTable(current);
            if (current.state == state) {
                return status(current);
            }
            requireRunning(current);
            throw e;
        }
    }

    private static BeginBulkLoadResponse beginResponse(
            BulkLoadTransaction transaction, List<BulkLoadTarget> targets, TableInfo table) {
        BeginBulkLoadResponse response = new BeginBulkLoadResponse();
        response.setStatus(status(transaction));
        response.setTableInfo()
                .setTableId(transaction.tableId)
                .setSchemaId(transaction.schemaId)
                .setTableJson(table.toTableDescriptor().toJsonBytes())
                .setCreatedTime(table.getCreatedTime())
                .setModifiedTime(table.getModifiedTime())
                .setBucketCountEpoch(table.getBucketCountEpoch())
                .setRemoteDataDir(transaction.remoteDataDir);
        for (BulkLoadTarget target : targets) {
            PbBulkLoadTargetInfo info =
                    response.addTarget()
                            .setBucketCount(target.bucketCount)
                            .setRemoteDataDir(target.remoteDataDir);
            if (target.partitionId != null) {
                info.setPartitionId(target.partitionId).setPartitionName(target.partitionName);
            }
        }
        return response;
    }

    private static PbBulkLoadStatus status(BulkLoadTransaction transaction) {
        PbBulkLoadStatus status =
                new PbBulkLoadStatus()
                        .setState(transaction.state)
                        .setCreatedTime(transaction.createdTime);
        status.setHandle().setTableId(transaction.tableId).setBulkLoadId(transaction.bulkLoadId);
        if (transaction.finishedTime != null) {
            status.setFinishedTime(transaction.finishedTime);
        }
        if (transaction.abortMessage != null) {
            status.setAbortMessage(transaction.abortMessage);
        }
        return status;
    }

    private static void requireDistinctPartitions(List<String> names) {
        if (new HashSet<>(names).size() != names.size()) {
            throw new InvalidRequestException("Duplicate partition names.");
        }
    }

    private static PartitionNotExistException partitionNotFound(String name) {
        return new PartitionNotExistException("Partition " + name + " does not exist.");
    }

    private VersionedData<BulkLoadTransaction> getTransaction(long tableId, String bulkLoadId)
            throws Exception {
        return zooKeeperClient
                .getBulkLoadTransaction(tableId, bulkLoadId)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "BulkLoad transaction does not exist."));
    }

    private VersionedData<TableRegistration> getOriginalTable(BulkLoadTransaction transaction)
            throws Exception {
        return zooKeeperClient
                .getTableWithVersion(transaction.tablePath)
                .filter(table -> table.data().tableId == transaction.tableId)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "The original BulkLoad table no longer exists."));
    }

    private VersionedData<BulkLoadTarget> resolveTarget(
            BulkLoadTransaction transaction, TableBucket bucket) throws Exception {
        VersionedData<BulkLoadTarget> target =
                zooKeeperClient
                        .getBulkLoadTarget(transaction, bucket.getPartitionId())
                        .orElseThrow(
                                () ->
                                        new InvalidRequestException(
                                                "Target is not part of this transaction."));
        if (bucket.getBucket() < 0 || bucket.getBucket() >= target.data().bucketCount) {
            throw new InvalidRequestException("Invalid bucket ID.");
        }
        return target;
    }

    // Schema is compared at read time. The partition registration version fences publication.
    private @Nullable Integer validateBuildContext(
            BulkLoadTransaction transaction, BulkLoadTarget target, TableRegistration table)
            throws Exception {
        if (target.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("The original physical target was deleted.");
        }
        // KV encoding, row TTL, and changelog image are immutable through ALTER TABLE.
        if (zooKeeperClient.getCurrentSchemaId(transaction.tablePath) != transaction.schemaId
                || !Objects.equals(table.remoteDataDir, transaction.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad build context changed.");
        }
        if (target.partitionId == null) {
            if (table.bucketCount != target.bucketCount) {
                throw new InvalidRequestException("BulkLoad target bucket count changed.");
            }
            return null;
        }

        VersionedData<PartitionRegistration> partition =
                getOriginalPartition(transaction.tablePath, target);
        PartitionRegistration registration = partition.data();
        int bucketCount =
                registration.getBucketCountOrDefault(table.bucketCount, table.bucketCountEpoch);
        if (bucketCount != target.bucketCount
                || !Objects.equals(registration.getRemoteDataDir(), target.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad target build context changed.");
        }
        return partition.zkVersion();
    }

    private VersionedData<PartitionRegistration> getOriginalPartition(
            TablePath tablePath, BulkLoadTarget target) throws Exception {
        return zooKeeperClient
                .getPartitionWithVersion(tablePath, target.partitionName)
                .filter(partition -> partition.data().getPartitionId() == target.partitionId)
                .orElseThrow(
                        () ->
                                new BulkLoadTargetWithdrawnException(
                                        "The original partition was deleted."));
    }

    private void checkMutationTarget(BulkLoadTransaction original, BulkLoadTarget target)
            throws Exception {
        getOriginalTable(original);
        requireRunning(getTransaction(original.tableId, original.bulkLoadId).data());
        if (target.partitionId != null) {
            getOriginalPartition(original.tablePath, target);
        }
    }

    private static void requireRunning(BulkLoadTransaction transaction) {
        if (transaction.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException("BulkLoad transaction has ended.");
        }
    }
}
