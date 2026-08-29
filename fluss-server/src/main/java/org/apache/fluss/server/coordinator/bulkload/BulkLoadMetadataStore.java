/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.CheckedMultiResult;
import org.apache.fluss.server.zk.ZooKeeperClient.CheckedOperation;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Lifecycle-specific checked ZooKeeper operations for BulkLoad metadata. */
@Internal
public final class BulkLoadMetadataStore {

    private final ZooKeeperClient zkClient;
    private final long maxTransactionBytes;

    public BulkLoadMetadataStore(ZooKeeperClient zkClient, long maxTransactionBytes) {
        this.zkClient = checkNotNull(zkClient);
        checkArgument(maxTransactionBytes > 0, "ZooKeeper transaction limit must be positive.");
        this.maxTransactionBytes = maxTransactionBytes;
    }

    public <T> ReadResult<T> read(String path, Function<byte[], T> decoder) {
        try {
            Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(path);
            if (!data.isPresent()) {
                return ReadResult.notFound();
            }
            DataWithStat value = data.get();
            return ReadResult.found(
                    new Versioned<>(
                            decoder.apply(value.getData()),
                            path,
                            value.getStat().getVersion(),
                            value.getStat().getEphemeralOwner()));
        } catch (Exception e) {
            return ReadResult.unknown(e);
        }
    }

    public <R> Versioned<BulkLoadTransaction> createTransactionAndFence(
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            BulkLoadTransaction transaction,
            int coordinatorEpochVersion)
            throws Exception {
        BulkLoadHandle handle = transaction.getHandle();
        requireRegistration(handle, registration);
        requireAssignment(handle, assignment);
        requireRegistrationState(
                registration.getValue(), BulkLoadDataState.ACTIVE, null, registration.getVersion());
        R loadingRegistration =
                withDataState(
                        registration.getValue(), BulkLoadDataState.LOADING, handle.getBulkLoadId());
        requireRegistrationValue(handle, loadingRegistration);
        requireRegistrationState(
                loadingRegistration,
                BulkLoadDataState.LOADING,
                handle.getBulkLoadId(),
                transaction.getMetadataVersion());
        checkArgument(
                transaction.getState() == BulkLoadState.BEGUN && !transaction.isFenceReady(),
                "Begin must create BEGUN with fenceReady=false.");
        checkArgument(
                transaction.getMetadataPath().equals(registration.getPath())
                        && transaction.getMetadataVersion() == registration.getVersion() + 1,
                "Begin transaction must carry the LOADING metadata identity.");

        List<CheckedOperation> operations = beginOperations(coordinatorEpochVersion);
        operations.add(CheckedOperation.check(registration.getPath(), registration.getVersion()));
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        if (!zkClient.pathExists(transactionParentPath(handle))) {
            operations.add(CheckedOperation.create(transactionParentPath(handle), new byte[0]));
        }
        operations.add(
                CheckedOperation.create(transactionPath(handle), encodeTransaction(transaction)));
        operations.add(
                CheckedOperation.set(
                        registration.getPath(),
                        encodeRegistration(handle, loadingRegistration),
                        registration.getVersion()));
        CheckedMultiResult result = submitCheckedMulti(operations);
        return transactionResult(transaction, result);
    }

    public <R> Versioned<BulkLoadTransaction> markFenceReady(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            long[] snapshotIds,
            long nowMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.BEGUN, true);
        requireAssignment(transaction.getValue().getHandle(), assignment);
        checkArgument(!transaction.getValue().isFenceReady(), "Fence transition must mark ready.");
        checkArgument(snapshotIds != null && snapshotIds.length > 0, "Snapshot IDs are required.");
        BulkLoadTransaction ready =
                copyTransaction(
                        transaction.getValue(),
                        BulkLoadState.BEGUN,
                        transaction.getValue().getMetadataVersion(),
                        snapshotIds,
                        nowMs,
                        transaction.getValue().getCommitDecisionDeadlineMs(),
                        transaction.getValue().getResultExpireTimeMs(),
                        transaction.getValue().getManifestPath(),
                        transaction.getValue().getManifestLength(),
                        transaction.getValue().getManifestSha256(),
                        transaction.getValue().getAbortReason(),
                        transaction.getValue().getAbortMessage());
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        addRegistrationChecks(operations, assignment, holders);
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(), encodeTransaction(ready), transaction.getVersion()));
        return transactionResult(ready, submitCheckedMulti(operations));
    }

    public <R> Versioned<BulkLoadTransaction> freezeManifest(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            String manifestPath,
            long manifestLength,
            String manifestSha256,
            long nowMs,
            long commitDecisionTimeoutMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.BEGUN, true);
        if (transaction.getValue().getManifestPath() != null) {
            checkArgument(
                    transaction.getValue().getManifestPath().equals(manifestPath)
                            && transaction.getValue().getManifestLength() == manifestLength
                            && transaction.getValue().getManifestSha256().equals(manifestSha256),
                    "A different manifest cannot replace the frozen manifest.");
            confirm(coordinatorEpochVersion, registration, transaction);
            return transaction;
        }
        checkArgument(transaction.getValue().isFenceReady(), "Freeze requires a ready fence.");
        BulkLoadTransaction frozen =
                copyTransaction(
                        transaction.getValue(),
                        BulkLoadState.BEGUN,
                        transaction.getValue().getMetadataVersion(),
                        transaction.getValue().getSnapshotIds(),
                        nowMs,
                        checkedAdd(
                                nowMs,
                                commitDecisionTimeoutMs,
                                "Commit decision deadline overflow."),
                        transaction.getValue().getResultExpireTimeMs(),
                        manifestPath,
                        manifestLength,
                        manifestSha256,
                        transaction.getValue().getAbortReason(),
                        transaction.getValue().getAbortMessage());
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(),
                        encodeTransaction(frozen),
                        transaction.getVersion()));
        return transactionResult(frozen, submitCheckedMulti(operations));
    }

    public <R> Versioned<BulkLoadTransaction> decideCommit(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            long nowMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.BEGUN, true);
        requireAssignment(transaction.getValue().getHandle(), assignment);
        checkArgument(transaction.getValue().isFenceReady(), "Commit requires a ready fence.");
        checkArgument(
                transaction.getValue().getManifestPath() != null,
                "Commit requires a frozen manifest.");
        BulkLoadTransaction committing =
                copyTransaction(
                        transaction.getValue(),
                        BulkLoadState.COMMITTING,
                        transaction.getValue().getMetadataVersion(),
                        transaction.getValue().getSnapshotIds(),
                        nowMs,
                        transaction.getValue().getCommitDecisionDeadlineMs(),
                        transaction.getValue().getResultExpireTimeMs(),
                        transaction.getValue().getManifestPath(),
                        transaction.getValue().getManifestLength(),
                        transaction.getValue().getManifestSha256(),
                        transaction.getValue().getAbortReason(),
                        transaction.getValue().getAbortMessage());
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        addRegistrationChecks(operations, assignment, holders);
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(),
                        encodeTransaction(committing),
                        transaction.getVersion()));
        return transactionResult(committing, submitCheckedMulti(operations));
    }

    public <R> void adoptBucketMetadata(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            int bucketId,
            BucketSnapshot snapshot,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.COMMITTING, true);
        long[] snapshotIds = transaction.getValue().getSnapshotIds();
        checkArgument(
                snapshotIds != null && bucketId >= 0 && bucketId < snapshotIds.length,
                "Bucket must belong to the transaction Snapshot ID array.");
        checkArgument(
                snapshot.getSnapshotId() == snapshotIds[bucketId] && snapshot.getLogOffset() >= 0,
                "Snapshot record must match the transaction Snapshot ID.");

        TableBucket tableBucket =
                new TableBucket(
                        transaction.getValue().getTableId(),
                        transaction.getValue().getPartitionId(),
                        bucketId);
        FsPath snapshotDirectory =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(
                                        transaction.getValue().getRemoteDataDir(),
                                        FlussPaths.REMOTE_KV_DIR_NAME),
                                transaction.getValue().getHandle().getTarget(),
                                tableBucket),
                        snapshot.getSnapshotId());
        checkArgument(
                snapshot.getMetadataPath()
                        .equals(new FsPath(snapshotDirectory, "_METADATA").toString()),
                "Snapshot metadata path must be canonical.");
        String snapshotPath =
                ZkData.BucketSnapshotIdZNode.path(tableBucket, snapshot.getSnapshotId());
        String snapshotParent = ZkData.BucketSnapshotsZNode.path(tableBucket);
        boolean createSnapshotParent = !zkClient.pathExists(snapshotParent);
        DataWithStat bucketRoot = zkClient.getDataWithStat(ZkData.BucketIdZNode.path(tableBucket));
        Optional<Versioned<BucketSnapshot>> existingSnapshot =
                readOptional(snapshotPath, ZkData.BucketSnapshotIdZNode::decode);
        if (existingSnapshot.isPresent()) {
            checkArgument(
                    existingSnapshot.get().getValue().equals(snapshot),
                    "BulkLoad adoption found conflicting ordinary Snapshot metadata.");
            List<CheckedOperation> confirmation =
                    controlChecks(coordinatorEpochVersion, transaction, registration);
            confirmation.add(
                    CheckedOperation.check(
                            ZkData.BucketIdZNode.path(tableBucket),
                            bucketRoot.getStat().getVersion()));
            confirmation.add(
                    CheckedOperation.check(snapshotPath, existingSnapshot.get().getVersion()));
            submitCheckedMulti(confirmation);
            return;
        }

        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(
                CheckedOperation.check(
                        ZkData.BucketIdZNode.path(tableBucket), bucketRoot.getStat().getVersion()));
        if (createSnapshotParent) {
            operations.add(CheckedOperation.create(snapshotParent, new byte[0]));
        }
        operations.add(
                CheckedOperation.create(
                        snapshotPath, ZkData.BucketSnapshotIdZNode.encode(snapshot)));
        submitCheckedMulti(operations);
    }

    public <R> Versioned<BulkLoadTransaction> activateTarget(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            long nowMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.COMMITTING, true);
        return activate(
                transaction,
                registration,
                assignment,
                holders,
                transaction.getValue().getAbortReason(),
                transaction.getValue().getAbortMessage(),
                true,
                nowMs,
                coordinatorEpochVersion);
    }

    public <R> Versioned<BulkLoadTransaction> finishCommit(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            long nowMs,
            long retentionMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.COMMITTING, false);
        requireAssignment(transaction.getValue().getHandle(), assignment);
        R releasedRegistration =
                withDataState(registration.getValue(), BulkLoadDataState.ACTIVE, null);
        BulkLoadTransaction committed =
                terminalTransaction(
                        transaction.getValue(),
                        BulkLoadState.COMMITTED,
                        registration.getVersion() + 1,
                        null,
                        null,
                        nowMs,
                        retentionMs);
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        addRegistrationChecks(operations, assignment, holders);
        operations.add(
                CheckedOperation.set(
                        registration.getPath(),
                        encodeRegistration(
                                transaction.getValue().getHandle(), releasedRegistration),
                        registration.getVersion()));
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(),
                        encodeTransaction(committed),
                        transaction.getVersion()));
        return transactionResult(committed, submitCheckedMulti(operations));
    }

    public <R> Versioned<BulkLoadTransaction> beginAbort(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            BulkLoadAbortReason reason,
            @Nullable String abortMessage,
            long nowMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.BEGUN, true);
        checkArgument(reason != null, "Abort requires an abort reason.");
        return activate(
                transaction,
                registration,
                assignment,
                holders,
                reason,
                abortMessage,
                reason != BulkLoadAbortReason.TARGET_NOT_EMPTY,
                nowMs,
                coordinatorEpochVersion);
    }

    public <R> Versioned<BulkLoadTransaction> finishAbort(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            long nowMs,
            long retentionMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireControl(transaction, registration, BulkLoadState.BEGUN, false);
        requireAssignment(transaction.getValue().getHandle(), assignment);
        checkArgument(
                transaction.getValue().getAbortReason() != null,
                "Aborting BulkLoad requires a persisted abort reason.");
        R releasedRegistration =
                withDataState(registration.getValue(), BulkLoadDataState.ACTIVE, null);
        BulkLoadTransaction aborted =
                terminalTransaction(
                        transaction.getValue(),
                        BulkLoadState.ABORTED,
                        registration.getVersion() + 1,
                        transaction.getValue().getAbortReason(),
                        transaction.getValue().getAbortMessage(),
                        nowMs,
                        retentionMs);
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        addRegistrationChecks(operations, assignment, holders);
        operations.add(
                CheckedOperation.set(
                        registration.getPath(),
                        encodeRegistration(
                                transaction.getValue().getHandle(), releasedRegistration),
                        registration.getVersion()));
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(),
                        encodeTransaction(aborted),
                        transaction.getVersion()));
        return transactionResult(aborted, submitCheckedMulti(operations));
    }

    private <R> Versioned<BulkLoadTransaction> activate(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<R> registration,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage,
            boolean checkHolderRegistrations,
            long nowMs,
            int coordinatorEpochVersion)
            throws Exception {
        requireAssignment(transaction.getValue().getHandle(), assignment);
        R activeRegistration =
                withDataState(
                        registration.getValue(),
                        BulkLoadDataState.ACTIVE,
                        transaction.getValue().getBulkLoadId());
        requireRegistrationValue(transaction.getValue().getHandle(), activeRegistration);
        requireRegistrationState(
                activeRegistration,
                BulkLoadDataState.ACTIVE,
                transaction.getValue().getBulkLoadId(),
                registration.getVersion() + 1);
        BulkLoadTransaction current = transaction.getValue();
        BulkLoadTransaction activated =
                copyTransaction(
                        current,
                        current.getState(),
                        registration.getVersion() + 1,
                        current.getSnapshotIds(),
                        nowMs,
                        current.getCommitDecisionDeadlineMs(),
                        current.getResultExpireTimeMs(),
                        current.getManifestPath(),
                        current.getManifestLength(),
                        current.getManifestSha256(),
                        abortReason,
                        abortMessage);
        List<CheckedOperation> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(CheckedOperation.check(assignment.getPath(), assignment.getVersion()));
        if (checkHolderRegistrations) {
            addRegistrationChecks(operations, assignment, holders);
        }
        operations.add(
                CheckedOperation.set(
                        registration.getPath(),
                        encodeRegistration(transaction.getValue().getHandle(), activeRegistration),
                        registration.getVersion()));
        operations.add(
                CheckedOperation.set(
                        transaction.getPath(),
                        encodeTransaction(activated),
                        transaction.getVersion()));
        return transactionResult(activated, submitCheckedMulti(operations));
    }

    private void confirm(int epochVersion, Versioned<?>... values) throws Exception {
        List<CheckedOperation> operations = beginOperations(epochVersion);
        addChecks(operations, Arrays.asList(values));
        submitCheckedMulti(operations);
    }

    private CheckedMultiResult submitCheckedMulti(List<CheckedOperation> operations)
            throws Exception {
        return zkClient.submitCheckedMulti(operations, maxTransactionBytes);
    }

    private static List<CheckedOperation> beginOperations(int coordinatorEpochVersion) {
        checkArgument(
                coordinatorEpochVersion >= 0, "Coordinator epoch version must be non-negative.");
        return new ArrayList<>(
                Collections.singletonList(
                        CheckedOperation.check(
                                ZkData.CoordinatorEpochZNode.path(), coordinatorEpochVersion)));
    }

    private static List<CheckedOperation> controlChecks(
            int coordinatorEpochVersion,
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration) {
        List<CheckedOperation> operations = beginOperations(coordinatorEpochVersion);
        addChecks(operations, Collections.singletonList(transaction));
        operations.add(CheckedOperation.check(registration.getPath(), registration.getVersion()));
        return operations;
    }

    private static void addChecks(
            List<CheckedOperation> operations, Iterable<? extends Versioned<?>> values) {
        for (Versioned<?> value : values) {
            operations.add(CheckedOperation.check(value.getPath(), value.getVersion()));
        }
    }

    private static void addRegistrationChecks(
            List<CheckedOperation> operations,
            Versioned<? extends TableAssignment> assignment,
            List<RegisteredServer> holders) {
        Set<Integer> assigned = new HashSet<>();
        for (Integer bucketId : assignment.getValue().getBuckets()) {
            assigned.addAll(assignment.getValue().getBucketAssignment(bucketId).getReplicas());
        }
        Map<Integer, RegisteredServer> present = new HashMap<>();
        for (RegisteredServer holder : holders) {
            checkArgument(assigned.contains(holder.serverId), "Unexpected assignment holder.");
            checkArgument(present.put(holder.serverId, holder) == null, "Duplicate holder.");
        }
        List<Integer> holderIds = new ArrayList<>(assigned);
        Collections.sort(holderIds);
        for (Integer holderId : holderIds) {
            RegisteredServer holder = present.get(holderId);
            if (holder == null) {
                operations.add(CheckedOperation.assertAbsent(ZkData.ServerIdZNode.path(holderId)));
            } else {
                Versioned<TabletServerRegistration> registration = holder.registration;
                checkArgument(
                        registration.getEphemeralOwner() != 0,
                        "TabletServer registration must be ephemeral.");
                operations.add(
                        CheckedOperation.check(registration.getPath(), registration.getVersion()));
                operations.add(
                        CheckedOperation.check(
                                ZkData.TabletServerSessionFenceZNode.path(
                                        holder.serverId, registration.getEphemeralOwner()),
                                0));
            }
        }
    }

    private Versioned<BulkLoadTransaction> transactionResult(
            BulkLoadTransaction value, CheckedMultiResult result) throws Exception {
        String path = transactionPath(value.getHandle());
        Stat stat = result.getStat(path);
        if (stat != null) {
            return new Versioned<>(value, path, stat.getVersion(), stat.getEphemeralOwner());
        }
        DataWithStat current = zkClient.getDataWithStat(path);
        return new Versioned<>(
                decodeTransaction(value.getHandle(), current.getData()),
                path,
                current.getStat().getVersion(),
                current.getStat().getEphemeralOwner());
    }

    private <T> Optional<Versioned<T>> readOptional(String path, Function<byte[], T> decoder)
            throws Exception {
        Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(path);
        if (!data.isPresent()) {
            return Optional.empty();
        }
        DataWithStat current = data.get();
        return Optional.of(
                new Versioned<>(
                        decoder.apply(current.getData()),
                        path,
                        current.getStat().getVersion(),
                        current.getStat().getEphemeralOwner()));
    }

    private static void requireControl(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration,
            BulkLoadState state,
            boolean loading) {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        requireTransaction(handle, transaction);
        requireRegistration(handle, registration);
        checkArgument(
                transaction.getValue().getState() == state,
                "Unexpected BulkLoad transaction state.");
        requireRegistrationState(
                registration.getValue(),
                loading ? BulkLoadDataState.LOADING : BulkLoadDataState.ACTIVE,
                handle.getBulkLoadId(),
                transaction.getValue().getMetadataVersion());
        checkArgument(
                registration.getPath().equals(transaction.getValue().getMetadataPath())
                        && registration.getVersion() == transaction.getValue().getMetadataVersion(),
                "Registration must match the transaction metadata identity.");
    }

    private static BulkLoadTransaction terminalTransaction(
            BulkLoadTransaction current,
            BulkLoadState state,
            int metadataVersion,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage,
            long nowMs,
            long retentionMs) {
        return copyTransaction(
                current,
                state,
                metadataVersion,
                current.getSnapshotIds(),
                nowMs,
                current.getCommitDecisionDeadlineMs(),
                checkedAdd(nowMs, retentionMs, "Result expiry overflow."),
                current.getManifestPath(),
                current.getManifestLength(),
                current.getManifestSha256(),
                abortReason,
                abortMessage);
    }

    private static BulkLoadTransaction copyTransaction(
            BulkLoadTransaction current,
            BulkLoadState state,
            int metadataVersion,
            @Nullable long[] snapshotIds,
            long updatedTimeMs,
            @Nullable Long commitDecisionDeadlineMs,
            @Nullable Long resultExpireTimeMs,
            @Nullable String manifestPath,
            @Nullable Long manifestLength,
            @Nullable String manifestSha256,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage) {
        return new BulkLoadTransaction(
                current.getHandle(),
                state,
                current.getCreatorName(),
                current.getCreatorType(),
                current.getRemoteDataDir(),
                current.getSchemaId(),
                current.getMetadataPath(),
                metadataVersion,
                snapshotIds,
                current.getCreatedTimeMs(),
                updatedTimeMs,
                current.getBuildDeadlineMs(),
                commitDecisionDeadlineMs,
                resultExpireTimeMs,
                manifestPath,
                manifestLength,
                manifestSha256,
                abortReason,
                abortMessage);
    }

    private static long checkedAdd(long left, long right, String message) {
        checkArgument(left >= 0 && right >= 0, "BulkLoad time and duration must be non-negative.");
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void requireRegistrationState(
            Object value,
            BulkLoadDataState state,
            @Nullable String bulkLoadId,
            int expectedVersion) {
        BulkLoadDataState actualState;
        String actualId;
        if (value instanceof TableRegistration) {
            actualState = ((TableRegistration) value).dataState;
            actualId = ((TableRegistration) value).bulkLoadId;
        } else {
            actualState = ((PartitionRegistration) value).getDataState();
            actualId = ((PartitionRegistration) value).getBulkLoadId();
        }
        checkArgument(
                actualState == state && Objects.equals(actualId, bulkLoadId),
                "Registration has the wrong BulkLoad identity at metadata version %s.",
                expectedVersion);
    }

    private static void requireTransaction(
            BulkLoadHandle handle, Versioned<BulkLoadTransaction> value) {
        checkArgument(
                value.getPath().equals(transactionPath(handle))
                        && value.getValue().getHandle().equals(handle),
                "Invalid BulkLoad transaction observation.");
    }

    private static void requireRegistration(BulkLoadHandle handle, Versioned<?> value) {
        checkArgument(
                value.getPath().equals(registrationPath(handle)),
                "Invalid target registration observation.");
        requireRegistrationValue(handle, value.getValue());
    }

    private static void requireRegistrationValue(BulkLoadHandle handle, Object value) {
        if (handle.getPartitionId() == null) {
            checkArgument(
                    value instanceof TableRegistration
                            && ((TableRegistration) value).tableId == handle.getTableId(),
                    "Table registration must match the physical target.");
        } else {
            checkArgument(
                    value instanceof PartitionRegistration
                            && ((PartitionRegistration) value).getTableId() == handle.getTableId()
                            && ((PartitionRegistration) value).getPartitionId()
                                    == handle.getPartitionId(),
                    "Partition registration must match the physical target.");
        }
    }

    @SuppressWarnings("unchecked")
    private static <R> R withDataState(
            R registration, BulkLoadDataState state, @Nullable String bulkLoadId) {
        if (registration instanceof TableRegistration) {
            return (R) ((TableRegistration) registration).withDataState(state, bulkLoadId);
        }
        if (registration instanceof PartitionRegistration) {
            return (R) ((PartitionRegistration) registration).withDataState(state, bulkLoadId);
        }
        throw new IllegalArgumentException("Unsupported target registration type.");
    }

    private static void requireAssignment(
            BulkLoadHandle handle, Versioned<? extends TableAssignment> value) {
        checkArgument(
                value.getPath().equals(assignmentPath(handle)),
                "Invalid target assignment observation.");
        checkArgument(
                handle.getPartitionId() == null
                        ? !(value.getValue() instanceof PartitionAssignment)
                        : value.getValue() instanceof PartitionAssignment,
                "Assignment type must match the target.");
    }

    static String registrationPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.TableZNode.path(handle.getTarget().getTablePath())
                : ZkData.PartitionZNode.path(
                        handle.getTarget().getTablePath(), handle.getTarget().getPartitionName());
    }

    static String assignmentPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.TableIdZNode.path(handle.getTableId())
                : ZkData.PartitionIdZNode.path(handle.getPartitionId());
    }

    static String transactionParentPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionsZNode.path(handle.getTableId())
                : ZkData.BulkLoadPartitionTransactionsZNode.path(handle.getPartitionId());
    }

    static String transactionPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionZNode.path(
                        handle.getTableId(), handle.getBulkLoadId())
                : ZkData.BulkLoadPartitionTransactionZNode.path(
                        handle.getPartitionId(), handle.getBulkLoadId());
    }

    private static byte[] encodeTransaction(BulkLoadTransaction value) {
        return value.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionZNode.encode(value)
                : ZkData.BulkLoadPartitionTransactionZNode.encode(value);
    }

    private static BulkLoadTransaction decodeTransaction(BulkLoadHandle handle, byte[] data) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionZNode.decode(data)
                : ZkData.BulkLoadPartitionTransactionZNode.decode(data);
    }

    private static byte[] encodeRegistration(BulkLoadHandle handle, Object value) {
        return handle.getPartitionId() == null
                ? ZkData.TableZNode.encode((TableRegistration) value)
                : ZkData.PartitionZNode.encode((PartitionRegistration) value);
    }

    /** Immutable value plus the ZooKeeper observation that authorized it. */
    public static final class Versioned<T> {
        private final T value;
        private final String path;
        private final int version;
        private final long ephemeralOwner;

        public Versioned(T value, String path, int version, long ephemeralOwner) {
            this.value = checkNotNull(value);
            this.path = checkNotNull(path);
            this.version = version;
            this.ephemeralOwner = ephemeralOwner;
        }

        public T getValue() {
            return value;
        }

        public String getPath() {
            return path;
        }

        public int getVersion() {
            return version;
        }

        public long getEphemeralOwner() {
            return ephemeralOwner;
        }
    }

    /** Result of a fail-closed metadata read. */
    public static final class ReadResult<T> {
        /** Stable classification of one read attempt. */
        public enum Status {
            FOUND,
            NOT_FOUND,
            UNKNOWN
        }

        private final Status status;
        private final @Nullable Versioned<T> value;
        private final @Nullable Exception failure;

        private ReadResult(
                Status status, @Nullable Versioned<T> value, @Nullable Exception failure) {
            this.status = status;
            this.value = value;
            this.failure = failure;
        }

        static <T> ReadResult<T> found(Versioned<T> value) {
            return new ReadResult<>(Status.FOUND, value, null);
        }

        static <T> ReadResult<T> notFound() {
            return new ReadResult<>(Status.NOT_FOUND, null, null);
        }

        static <T> ReadResult<T> unknown(Exception failure) {
            return new ReadResult<>(Status.UNKNOWN, null, failure);
        }

        public Status getStatus() {
            return status;
        }

        @Nullable
        public Versioned<T> getVersioned() {
            return value;
        }

        @Nullable
        public Exception getFailure() {
            return failure;
        }
    }

    /** Current registration process used for one process-local convergence round. */
    static final class RegisteredServer {
        final int serverId;
        final Versioned<TabletServerRegistration> registration;

        RegisteredServer(int serverId, Versioned<TabletServerRegistration> registration) {
            this.serverId = serverId;
            this.registration = registration;
        }
    }
}
