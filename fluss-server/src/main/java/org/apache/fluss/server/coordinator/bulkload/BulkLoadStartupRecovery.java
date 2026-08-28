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
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.zk.ZooKeeperClient.PartitionRegistrationSnapshot;
import org.apache.fluss.server.zk.ZooKeeperClient.TableRegistrationSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Discovers and resumes BulkLoad solely from target, transaction, and ordinary metadata facts. */
@Internal
public final class BulkLoadStartupRecovery {

    private final BulkLoadManager manager;
    private final CoordinatorContext coordinatorContext;

    BulkLoadStartupRecovery(BulkLoadManager manager, CoordinatorContext coordinatorContext) {
        this.manager = manager;
        this.coordinatorContext = coordinatorContext;
    }

    /** Reads exact transactions owned by the already loaded startup registration snapshots. */
    public Plan discover(
            Map<Long, TableRegistrationSnapshot> tables,
            Map<Long, PartitionRegistrationSnapshot> partitions)
            throws Exception {
        return new Plan(discoverTables(tables), discoverPartitions(partitions));
    }

    /** Seeds the event-thread inventory before the Coordinator event thread starts. */
    public void prepare(Plan plan) {
        manager.seedActiveTransactions(plan.transactions());
    }

    /** Resumes every owned transaction from its persisted lifecycle and registration facts. */
    public void resume(Plan plan) throws Exception {
        for (BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction :
                plan.transactions()) {
            resume(transaction.getValue().getHandle(), false);
        }
    }

    /** Rereads one exact owned transaction and registration before replaying its current round. */
    void resume(BulkLoadHandle handle) throws Exception {
        resume(handle, true);
    }

    private void resume(BulkLoadHandle handle, boolean allowReleasedMissingTransaction)
            throws Exception {
        BulkLoadMetadataStore.ReadResult<BulkLoadTransaction> read =
                manager.readTransaction(handle);
        if (read.getStatus() == BulkLoadMetadataStore.ReadResult.Status.NOT_FOUND) {
            if (!allowReleasedMissingTransaction) {
                throw new IllegalStateException("Missing BulkLoad startup transaction.");
            }
            validateUnownedRegistration(handle, manager.readRegistration(handle));
            manager.removeActiveTransaction(handle);
            return;
        }
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                BulkLoadManager.requireFoundRead(read, "transaction");
        manager.recordActiveTransaction(transaction);
        BulkLoadMetadataStore.Versioned<?> registration = manager.readRegistration(handle);
        BulkLoadTransaction value = transaction.getValue();
        if (value.getState() == BulkLoadState.COMMITTED
                || value.getState() == BulkLoadState.ABORTED) {
            validateReleasedRegistration(transaction, registration);
            manager.removeActiveTransaction(handle);
            return;
        }
        resume(transaction, registration);
    }

    private void resume(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadMetadataStore.Versioned<?> registration)
            throws Exception {
        BulkLoadTransaction value = transaction.getValue();
        BulkLoadDataState state = registrationState(registration);
        String bulkLoadId = registrationBulkLoadId(registration);
        if (value.getState() == BulkLoadState.COMMITTING) {
            manager.resumeCommit(transaction);
            return;
        }
        if (state == BulkLoadDataState.ACTIVE && value.getBulkLoadId().equals(bulkLoadId)) {
            if (value.getAbortReason() == null) {
                throw new IllegalStateException(
                        "Aborting BulkLoad has no persisted abort decision.");
            }
            manager.resumeAbort(transaction, value.getAbortReason(), value.getAbortMessage());
            return;
        }
        if (state != BulkLoadDataState.LOADING
                || !value.getBulkLoadId().equals(bulkLoadId)
                || registration.getVersion() != value.getMetadataVersion()
                || !registration.getPath().equals(value.getMetadataPath())) {
            throw new IllegalStateException("BulkLoad recovery metadata identity is inconsistent.");
        }
        BulkLoadAbortReason deadline = deadlineReason(value, System.currentTimeMillis());
        if (deadline != null) {
            manager.resumeAbort(transaction, deadline, null);
        } else if (value.getState() == BulkLoadState.BEGUN) {
            manager.startLoading(transaction);
        }
    }

    private Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> discoverTables(
            Map<Long, TableRegistrationSnapshot> tables) throws Exception {
        Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> result = new HashMap<>();
        for (Map.Entry<Long, TableRegistrationSnapshot> entry : tables.entrySet()) {
            TableRegistrationSnapshot snapshot = entry.getValue();
            if (snapshot.getRegistration().tableId != entry.getKey()) {
                throw new IllegalStateException("BulkLoad startup table identity is inconsistent.");
            }
            TablePath tablePath = coordinatorContext.allTables().get(entry.getKey());
            if (tablePath == null) {
                throw new IllegalStateException(
                        "Missing startup table path for BulkLoad recovery.");
            }
            String registrationPath = ZkData.TableZNode.path(tablePath);
            BulkLoadMetadataStore.Versioned<TableRegistration> registration =
                    new BulkLoadMetadataStore.Versioned<>(
                            snapshot.getRegistration(),
                            registrationPath,
                            snapshot.getVersion(),
                            0L);
            if (registration.getValue().bulkLoadId != null) {
                result.put(
                        entry.getKey(),
                        discoverTransaction(
                                entry.getKey(),
                                null,
                                registration.getValue().bulkLoadId,
                                registration));
            }
        }
        return result;
    }

    private Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> discoverPartitions(
            Map<Long, PartitionRegistrationSnapshot> partitions) throws Exception {
        Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> result = new HashMap<>();
        for (Map.Entry<Long, PartitionRegistrationSnapshot> entry : partitions.entrySet()) {
            PhysicalTablePath path = coordinatorContext.allPartitions().get(entry.getKey());
            if (path == null) {
                throw new IllegalStateException(
                        "Missing startup partition path for BulkLoad recovery.");
            }
            String registrationPath =
                    ZkData.PartitionZNode.path(path.getTablePath(), path.getPartitionName());
            PartitionRegistrationSnapshot snapshot = entry.getValue();
            BulkLoadMetadataStore.Versioned<PartitionRegistration> registration =
                    new BulkLoadMetadataStore.Versioned<>(
                            snapshot.getRegistration(),
                            registrationPath,
                            snapshot.getVersion(),
                            0L);
            if (registration.getValue().getBulkLoadId() != null) {
                result.put(
                        entry.getKey(),
                        discoverTransaction(
                                registration.getValue().getTableId(),
                                entry.getKey(),
                                registration.getValue().getBulkLoadId(),
                                registration));
            }
        }
        return result;
    }

    private BulkLoadMetadataStore.Versioned<BulkLoadTransaction> discoverTransaction(
            long tableId,
            @Nullable Long partitionId,
            String bulkLoadId,
            BulkLoadMetadataStore.Versioned<?> registration)
            throws Exception {
        String transactionPath =
                partitionId == null
                        ? ZkData.BulkLoadTableTransactionZNode.path(tableId, bulkLoadId)
                        : ZkData.BulkLoadPartitionTransactionZNode.path(partitionId, bulkLoadId);
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                manager.requireFound(
                        transactionPath,
                        partitionId == null
                                ? ZkData.BulkLoadTableTransactionZNode::decode
                                : ZkData.BulkLoadPartitionTransactionZNode::decode,
                        "startup transaction");
        BulkLoadHandle handle = transaction.getValue().getHandle();
        if (handle.getTableId() != tableId
                || !Objects.equals(handle.getPartitionId(), partitionId)
                || !handle.getBulkLoadId().equals(bulkLoadId)) {
            throw new IllegalStateException("BulkLoad startup identity is inconsistent.");
        }
        validateOwnedRegistration(transaction, registration);
        return transaction;
    }

    static @Nullable BulkLoadAbortReason deadlineReason(BulkLoadTransaction transaction, long now) {
        if (transaction.getState() == BulkLoadState.BEGUN
                && transaction.getManifestPath() == null
                && now >= transaction.getBuildDeadlineMs()) {
            return BulkLoadAbortReason.BUILD_DEADLINE_EXCEEDED;
        }
        if (transaction.getState() == BulkLoadState.BEGUN
                && transaction.getManifestPath() != null
                && transaction.getCommitDecisionDeadlineMs() != null
                && now >= transaction.getCommitDecisionDeadlineMs()) {
            return BulkLoadAbortReason.COMMIT_DECISION_DEADLINE_EXCEEDED;
        }
        return null;
    }

    private static BulkLoadDataState registrationState(
            BulkLoadMetadataStore.Versioned<?> registration) {
        return registration.getValue() instanceof TableRegistration
                ? ((TableRegistration) registration.getValue()).dataState
                : ((PartitionRegistration) registration.getValue()).getDataState();
    }

    private static @Nullable String registrationBulkLoadId(
            BulkLoadMetadataStore.Versioned<?> registration) {
        return registration.getValue() instanceof TableRegistration
                ? ((TableRegistration) registration.getValue()).bulkLoadId
                : ((PartitionRegistration) registration.getValue()).getBulkLoadId();
    }

    private static void validateOwnedRegistration(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadMetadataStore.Versioned<?> registration) {
        BulkLoadTransaction value = transaction.getValue();
        boolean physicalIdentity = matchesPhysicalIdentity(value, registration);
        boolean exactMetadata =
                registration.getPath().equals(value.getMetadataPath())
                        && registration.getVersion() == value.getMetadataVersion();
        BulkLoadDataState state = registrationState(registration);
        String id = registrationBulkLoadId(registration);
        boolean legalState =
                (state == BulkLoadDataState.LOADING || state == BulkLoadDataState.ACTIVE)
                        && value.getBulkLoadId().equals(id);
        if (!physicalIdentity || !exactMetadata || !legalState) {
            throw new IllegalStateException(
                    "BulkLoad startup registration identity is inconsistent.");
        }
    }

    private static void validateReleasedRegistration(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadMetadataStore.Versioned<?> registration) {
        BulkLoadTransaction value = transaction.getValue();
        boolean exactMetadata =
                registration.getPath().equals(value.getMetadataPath())
                        && registration.getVersion() == value.getMetadataVersion();
        if (!matchesPhysicalIdentity(value, registration)
                || !exactMetadata
                || registrationState(registration) != BulkLoadDataState.ACTIVE
                || registrationBulkLoadId(registration) != null) {
            throw new IllegalStateException(
                    "Terminal BulkLoad transaction retains target ownership.");
        }
    }

    private static void validateUnownedRegistration(
            BulkLoadHandle handle, BulkLoadMetadataStore.Versioned<?> registration) {
        if (!matchesPhysicalIdentity(handle, registration)
                || registrationState(registration) != BulkLoadDataState.ACTIVE
                || registrationBulkLoadId(registration) != null) {
            throw new IllegalStateException(
                    "Missing BulkLoad transaction retains target ownership.");
        }
    }

    private static boolean matchesPhysicalIdentity(
            BulkLoadTransaction transaction, BulkLoadMetadataStore.Versioned<?> registration) {
        return matchesPhysicalIdentity(transaction.getHandle(), registration);
    }

    private static boolean matchesPhysicalIdentity(
            BulkLoadHandle handle, BulkLoadMetadataStore.Versioned<?> registration) {
        if (registration.getValue() instanceof TableRegistration) {
            TableRegistration table = (TableRegistration) registration.getValue();
            return handle.getPartitionId() == null && table.tableId == handle.getTableId();
        } else {
            PartitionRegistration partition = (PartitionRegistration) registration.getValue();
            return handle.getPartitionId() != null
                    && partition.getTableId() == handle.getTableId()
                    && partition.getPartitionId() == handle.getPartitionId();
        }
    }

    /** Immutable owned-transaction inventory captured before ordinary assignment startup. */
    public static final class Plan {
        private final Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> tables;
        private final Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> partitions;

        private Plan(
                Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> tables,
                Map<Long, BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> partitions) {
            this.tables = Collections.unmodifiableMap(new HashMap<>(tables));
            this.partitions = Collections.unmodifiableMap(new HashMap<>(partitions));
        }

        public boolean isTableDeleted(
                long tableId,
                @Nullable BulkLoadDataState registrationState,
                @Nullable String registrationBulkLoadId) {
            validateRegistrationIdentity(
                    tables.get(tableId), registrationState, registrationBulkLoadId);
            return registrationState == null;
        }

        public boolean isPartitionDeleted(
                long tableId,
                long partitionId,
                @Nullable BulkLoadDataState registrationState,
                @Nullable String registrationBulkLoadId) {
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                    partitions.get(partitionId);
            if (transaction != null && transaction.getValue().getTableId() != tableId) {
                throw new IllegalStateException("BulkLoad partition table identity differs.");
            }
            validateRegistrationIdentity(transaction, registrationState, registrationBulkLoadId);
            return registrationState == null;
        }

        public void validateAssignmentCoverage(Set<Long> tableIds, Set<Long> partitionIds) {
            if (!tableIds.containsAll(tables.keySet())
                    || !partitionIds.containsAll(partitions.keySet())) {
                throw new IllegalStateException("BulkLoad target assignment is missing.");
            }
        }

        List<BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> transactions() {
            List<BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> result =
                    new ArrayList<>(tables.values());
            result.addAll(partitions.values());
            return result;
        }

        private static void validateRegistrationIdentity(
                @Nullable BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
                @Nullable BulkLoadDataState state,
                @Nullable String bulkLoadId) {
            if (transaction == null) {
                if (state == null || (state == BulkLoadDataState.ACTIVE && bulkLoadId == null)) {
                    return;
                }
                throw new IllegalStateException(
                        "Unowned startup registration is not ordinary ACTIVE metadata.");
            }
            BulkLoadTransaction value = transaction.getValue();
            boolean loading =
                    state == BulkLoadDataState.LOADING && value.getBulkLoadId().equals(bulkLoadId);
            boolean abortOrActivatedCommit =
                    state == BulkLoadDataState.ACTIVE && value.getBulkLoadId().equals(bulkLoadId);
            if (!loading && !abortOrActivatedCommit) {
                throw new IllegalStateException(
                        "BulkLoad startup registration identity is inconsistent.");
            }
        }
    }
}
