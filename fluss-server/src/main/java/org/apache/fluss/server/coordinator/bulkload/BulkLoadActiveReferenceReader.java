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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Reads ordinary and nonterminal BulkLoad file references with a fail-closed recheck. */
@Internal
public final class BulkLoadActiveReferenceReader {

    private static final Runnable NO_OP = () -> {};

    private final ZooKeeperClient zkClient;
    private final Runnable beforeBulkLoadRecheckHook;

    /** Creates a production active-reference reader. */
    public BulkLoadActiveReferenceReader(ZooKeeperClient zkClient) {
        this(zkClient, NO_OP);
    }

    /** Creates a reader with deterministic race hooks for the stable safety-contract test. */
    @VisibleForTesting
    public BulkLoadActiveReferenceReader(
            ZooKeeperClient zkClient, Runnable beforeBulkLoadRecheckHook) {
        this.zkClient = checkNotNull(zkClient);
        this.beforeBulkLoadRecheckHook = checkNotNull(beforeBulkLoadRecheckHook);
    }

    /**
     * Returns ordinary Snapshot IDs plus every Snapshot ID held by the target's nonterminal
     * BulkLoad transaction.
     *
     * @throws TimeoutException if the BulkLoad observation changes around the ordinary read
     * @throws Exception if any underlying read fails or the persisted lifecycle is incomplete
     */
    public Map<Integer, Set<Long>> readSnapshotIds(
            long tableId,
            @Nullable Long partitionId,
            String registrationPath,
            OrdinarySnapshotReferenceReader ordinaryReader)
            throws Exception {
        Observation before = capture(tableId, partitionId, registrationPath);
        Map<Integer, Set<Long>> references = new HashMap<>();
        mergeSnapshotIds(references, ordinaryReader.read());
        mergeSnapshotIds(references, before.activeSnapshotIds);
        beforeBulkLoadRecheckHook.run();
        requireUnchanged(
                before, capture(tableId, partitionId, registrationPath), tableId, partitionId);
        return references;
    }

    private Observation capture(long tableId, @Nullable Long partitionId, String registrationPath)
            throws Exception {
        NodeObservation epoch =
                NodeObservation.present(
                        zkClient.getDataWithStat(ZkData.CoordinatorEpochZNode.path()));
        DataWithStat registrationData = zkClient.getDataWithStat(registrationPath);
        NodeObservation registrationNode = NodeObservation.present(registrationData);
        String bulkLoadId =
                decodeRegistration(
                        registrationData.getData(), tableId, partitionId, registrationPath);
        if (bulkLoadId == null) {
            return Observation.withoutTransaction(epoch, registrationNode);
        }

        String transactionPath = transactionPath(tableId, partitionId, bulkLoadId);
        DataWithStat transactionData = zkClient.getDataWithStat(transactionPath);
        NodeObservation transactionNode = NodeObservation.present(transactionData);
        BulkLoadTransaction transaction = decodeTransaction(transactionData.getData(), partitionId);
        requireMatchingIdentity(
                transaction,
                bulkLoadId,
                tableId,
                partitionId,
                registrationPath,
                registrationData.getStat().getVersion());
        if (isTerminal(transaction.getState())) {
            throw new IllegalStateException(
                    "Terminal BulkLoad transaction retains target ownership.");
        }

        Map<Integer, Set<Long>> snapshotIds = new HashMap<>();
        long[] transactionSnapshotIds = transaction.getSnapshotIds();
        if (transactionSnapshotIds != null) {
            for (int bucketId = 0; bucketId < transactionSnapshotIds.length; bucketId++) {
                long snapshotId = transactionSnapshotIds[bucketId];
                snapshotIds.computeIfAbsent(bucketId, ignored -> new HashSet<>()).add(snapshotId);
            }
        }
        return Observation.withTransaction(epoch, registrationNode, transactionNode, snapshotIds);
    }

    private static void mergeSnapshotIds(
            Map<Integer, Set<Long>> destination, Map<Integer, Set<Long>> source) {
        for (Map.Entry<Integer, Set<Long>> entry : source.entrySet()) {
            destination
                    .computeIfAbsent(entry.getKey(), ignored -> new HashSet<>())
                    .addAll(entry.getValue());
        }
    }

    private static void requireMatchingIdentity(
            BulkLoadTransaction transaction,
            String bulkLoadId,
            long tableId,
            @Nullable Long partitionId,
            String registrationPath,
            int registrationVersion) {
        BulkLoadHandle handle = transaction.getHandle();
        if (!bulkLoadId.equals(handle.getBulkLoadId())
                || handle.getTableId() != tableId
                || !Objects.equals(handle.getPartitionId(), partitionId)
                || !registrationPath.equals(transaction.getMetadataPath())
                || registrationVersion != transaction.getMetadataVersion()) {
            throw new IllegalStateException(
                    "BulkLoad registration and transaction identities differ.");
        }
    }

    private static @Nullable String decodeRegistration(
            byte[] data, long tableId, @Nullable Long partitionId, String registrationPath) {
        BulkLoadDataState state;
        String bulkLoadId;
        if (partitionId == null) {
            TableRegistration registration = ZkData.TableZNode.decode(data);
            if (registration.tableId != tableId) {
                throw new IllegalStateException("BulkLoad table registration identity differs.");
            }
            state = registration.dataState;
            bulkLoadId = registration.bulkLoadId;
        } else {
            PartitionRegistration registration = ZkData.PartitionZNode.decode(data);
            if (registration.getTableId() != tableId
                    || registration.getPartitionId() != partitionId.longValue()) {
                throw new IllegalStateException(
                        "BulkLoad partition registration identity differs.");
            }
            state = registration.getDataState();
            bulkLoadId = registration.getBulkLoadId();
        }
        if (bulkLoadId != null
                && state != BulkLoadDataState.LOADING
                && state != BulkLoadDataState.ACTIVE) {
            throw new IllegalStateException(
                    "BulkLoad registration state is invalid at " + registrationPath + ".");
        }
        return bulkLoadId;
    }

    private static void requireUnchanged(
            Observation before, Observation after, long tableId, @Nullable Long partitionId) {
        if (!before.sameAs(after)) {
            throw new TimeoutException(
                    String.format(
                            "BulkLoad active references changed while reading tableId=%d"
                                    + " partitionId=%s.",
                            tableId, partitionId));
        }
    }

    private static boolean isTerminal(BulkLoadState state) {
        return state == BulkLoadState.COMMITTED || state == BulkLoadState.ABORTED;
    }

    private static String transactionPath(
            long tableId, @Nullable Long partitionId, String bulkLoadId) {
        return partitionId == null
                ? ZkData.BulkLoadTableTransactionZNode.path(tableId, bulkLoadId)
                : ZkData.BulkLoadPartitionTransactionZNode.path(partitionId, bulkLoadId);
    }

    private static BulkLoadTransaction decodeTransaction(byte[] data, @Nullable Long partitionId) {
        return partitionId == null
                ? ZkData.BulkLoadTableTransactionZNode.decode(data)
                : ZkData.BulkLoadPartitionTransactionZNode.decode(data);
    }

    /** Supplies retained and lease-pinned ordinary Snapshot references. */
    @FunctionalInterface
    public interface OrdinarySnapshotReferenceReader {
        /** Returns ordinary Snapshot IDs grouped by bucket. */
        Map<Integer, Set<Long>> read() throws Exception;
    }

    private static final class Observation {
        private final NodeObservation epoch;
        private final NodeObservation registration;
        private final @Nullable NodeObservation transaction;
        private final Map<Integer, Set<Long>> activeSnapshotIds;

        private Observation(
                NodeObservation epoch,
                NodeObservation registration,
                @Nullable NodeObservation transaction,
                Map<Integer, Set<Long>> activeSnapshotIds) {
            this.epoch = epoch;
            this.registration = registration;
            this.transaction = transaction;
            this.activeSnapshotIds = activeSnapshotIds;
        }

        private static Observation withoutTransaction(
                NodeObservation epoch, NodeObservation registration) {
            return new Observation(epoch, registration, null, Collections.emptyMap());
        }

        private static Observation withTransaction(
                NodeObservation epoch,
                NodeObservation registration,
                NodeObservation transaction,
                Map<Integer, Set<Long>> activeSnapshotIds) {
            return new Observation(epoch, registration, transaction, activeSnapshotIds);
        }

        private boolean sameAs(Observation other) {
            return epoch.sameAs(other.epoch)
                    && registration.sameAs(other.registration)
                    && sameNode(transaction, other.transaction);
        }

        private static boolean sameNode(
                @Nullable NodeObservation left, @Nullable NodeObservation right) {
            return left == null ? right == null : left.sameAs(right);
        }
    }

    private static final class NodeObservation {
        private final byte[] data;
        private final int version;

        private NodeObservation(byte[] data, int version) {
            this.data = data;
            this.version = version;
        }

        private static NodeObservation present(DataWithStat dataWithStat) {
            return new NodeObservation(dataWithStat.getData(), dataWithStat.getStat().getVersion());
        }

        private boolean sameAs(NodeObservation other) {
            return version == other.version && Arrays.equals(data, other.data);
        }
    }
}
