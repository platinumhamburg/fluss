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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.server.ServerApiVersionSupport;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorRequestBatch;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.event.AbortBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.BeginBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadAsyncResultEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent;
import org.apache.fluss.server.coordinator.event.CommitBulkLoadEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.ChildrenWithStat;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromPhysicalTablePath;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinator event-thread owner of the metadata-only BulkLoad lifecycle. */
@Internal
public final class BulkLoadManager {

    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadManager.class);

    private final ZooKeeperClient zkClient;
    private final Configuration configuration;
    private final CoordinatorContext coordinatorContext;
    private final MetadataManager metadataManager;
    private final ExecutorService ioExecutor;
    private final EventManager eventManager;
    private final Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations =
            new HashMap<>();
    private final BulkLoadReplicaConvergence convergence;
    private final BulkLoadResultGc resultGc;
    private final BulkLoadStartupRecovery startupRecovery;
    private final CompletedSnapshotStoreManager completedSnapshotStoreManager;
    private final Map<String, List<BeginWaiter>> beginWaiters = new HashMap<>();
    private final Map<String, List<CommitBulkLoadEvent>> commitWaiters = new HashMap<>();
    private final Map<String, List<AbortBulkLoadEvent>> abortWaiters = new HashMap<>();
    private final Map<String, InFlight> inFlight = new HashMap<>();
    // Empty values reserve admission after an uncertain Begin until exact reconciliation.
    private final Map<
                    BulkLoadHandle, Optional<BulkLoadMetadataStore.Versioned<BulkLoadTransaction>>>
            activeTransactions = new HashMap<>();
    private long nextToken;

    /** Creates the production metadata-only manager. */
    public BulkLoadManager(
            ZooKeeperClient zkClient,
            Configuration configuration,
            CoordinatorContext coordinatorContext,
            MetadataManager metadataManager,
            CoordinatorRequestBatch requestBatch,
            ExecutorService ioExecutor,
            EventManager eventManager,
            CompletedSnapshotStoreManager completedSnapshotStoreManager) {
        this.zkClient = checkNotNull(zkClient);
        this.configuration = checkNotNull(configuration);
        this.coordinatorContext = checkNotNull(coordinatorContext);
        this.metadataManager = checkNotNull(metadataManager);
        checkNotNull(requestBatch);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.eventManager = checkNotNull(eventManager);
        this.completedSnapshotStoreManager = checkNotNull(completedSnapshotStoreManager);
        this.convergence =
                new BulkLoadReplicaConvergence(
                        coordinatorContext, requestBatch, readyRegistrations);
        this.resultGc =
                new BulkLoadResultGc(
                        zkClient, ioExecutor, eventManager, zookeeperTransactionBytes());
        this.startupRecovery = new BulkLoadStartupRecovery(this, coordinatorContext);
    }

    public BulkLoadStartupRecovery startupRecovery() {
        return startupRecovery;
    }

    void seedActiveTransactions(
            List<BulkLoadMetadataStore.Versioned<BulkLoadTransaction>> transactions) {
        activeTransactions.clear();
        for (BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction : transactions) {
            BulkLoadHandle handle = transaction.getValue().getHandle();
            if (activeTransactions.put(handle, Optional.of(transaction)) != null) {
                throw new IllegalStateException("Duplicate active BulkLoad handle during startup.");
            }
        }
    }

    void recordActiveTransaction(BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction) {
        activeTransactions.put(transaction.getValue().getHandle(), Optional.of(transaction));
    }

    void removeActiveTransaction(BulkLoadHandle handle) {
        activeTransactions.remove(handle);
    }

    /** Replays current persisted state independently for every active target. */
    public void resumeActiveTransactions() {
        for (BulkLoadHandle handle : new ArrayList<>(activeTransactions.keySet())) {
            try {
                startupRecovery.resume(handle);
            } catch (Exception failure) {
                LOG.warn("Failed to resume active BulkLoad {}.", handle, failure);
            }
        }
    }

    /** Binds the exact registrations whose gateways were installed during Coordinator startup. */
    public void bindStartupTabletServerGateways(List<ServerInfo> servers) {
        readyRegistrations.clear();
        for (ServerInfo server : servers) {
            bindTabletServerGateway(server, true);
        }
    }

    /**
     * Binds the exact current registration after its gateway is ready in the Coordinator event
     * thread.
     */
    public void bindTabletServerGateway(ServerInfo server, boolean newGatewayInstalled) {
        int serverId = server.id();
        BulkLoadMetadataStore.RegisteredServer previous = readyRegistrations.remove(serverId);
        Optional<DataWithStat> observed;
        try {
            observed = zkClient.getDataWithStatIfExists(ZkData.ServerIdZNode.path(serverId));
        } catch (Exception failure) {
            throw new FlussRuntimeException(
                    "Failed to bind the ready TabletServer registration.", failure);
        }
        if (!observed.isPresent()) {
            return;
        }
        DataWithStat data = observed.get();
        BulkLoadMetadataStore.RegisteredServer current =
                new BulkLoadMetadataStore.RegisteredServer(
                        serverId,
                        new BulkLoadMetadataStore.Versioned<>(
                                ZkData.ServerIdZNode.decode(data.getData()),
                                ZkData.ServerIdZNode.path(serverId),
                                data.getStat().getVersion(),
                                data.getStat().getEphemeralOwner()));
        ServerInfo currentLive = coordinatorContext.getLiveTabletServers().get(serverId);
        if (currentLive == null
                || !currentLive.equals(server)
                || !BulkLoadReplicaConvergence.matchesLive(current, currentLive)) {
            return;
        }
        if (!newGatewayInstalled
                && (previous == null
                        || previous.registration.getEphemeralOwner()
                                != current.registration.getEphemeralOwner())) {
            return;
        }
        readyRegistrations.put(serverId, current);
    }

    /** Invalidates a registration before its live identity and gateway are removed. */
    public void unbindTabletServerGateway(int serverId) {
        readyRegistrations.remove(serverId);
    }

    /** Handles Begin on the Coordinator event thread. */
    public void process(BeginBulkLoadEvent event) {
        try {
            requireClusterCapabilities();
            ExistingTarget existing = findExistingTarget(event.getTarget());
            if (existing != null) {
                resumeExistingBegin(event, existing);
                return;
            }
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> retained =
                    findRetainedSubmission(event);
            if (retained != null) {
                recordActiveTransaction(retained);
                beginWaiters
                        .computeIfAbsent(
                                retained.getValue().getBulkLoadId(), ignored -> new ArrayList<>())
                        .add(new BeginWaiter(event, false, true));
                startFinalActive(retained);
                return;
            }

            BeginAdmission admission = readBeginAdmission(event);
            BulkLoadHandle attemptedHandle = admission.transaction.getHandle();
            activeTransactions.put(attemptedHandle, Optional.empty());
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> created;
            try {
                created =
                        metadataStore()
                                .createTransactionAndFence(
                                        admission.registration,
                                        admission.assignment,
                                        admission.transaction,
                                        coordinatorEpochVersion());
            } catch (Exception uncertain) {
                ExistingTarget winner;
                try {
                    winner = findExistingTarget(event.getTarget());
                } catch (Exception reconciliationFailure) {
                    reconciliationFailure.addSuppressed(uncertain);
                    throw reconciliationFailure;
                }
                if (winner == null) {
                    activeTransactions.remove(attemptedHandle);
                    throw uncertain;
                }
                if (!winner.handle.equals(attemptedHandle)) {
                    activeTransactions.remove(attemptedHandle);
                    activeTransactions.put(winner.handle, Optional.empty());
                }
                resumeExistingBegin(event, winner);
                return;
            }
            recordActiveTransaction(created);
            beginWaiters
                    .computeIfAbsent(
                            admission.transaction.getBulkLoadId(), ignored -> new ArrayList<>())
                    .add(new BeginWaiter(event, true, false));
            try {
                startLoading(created);
            } catch (Throwable failure) {
                failWaiters(admission.transaction.getBulkLoadId(), failure);
                throw failure;
            }
        } catch (Throwable failure) {
            event.getResultFuture().completeExceptionally(failure);
        }
    }

    private void resumeExistingBegin(BeginBulkLoadEvent event, ExistingTarget existing)
            throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(existing.handle);
        recordActiveTransaction(transaction);
        if (!event.getCallerToken().equals(transaction.getValue().getCallerToken())
                || !sameCreator(event, transaction.getValue())) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad target is occupied by another submission.");
        }
        RegistrationState registration = registrationState(readRegistration(existing.handle));
        if (transaction.getValue().getState() == BulkLoadState.BEGUN
                && registration.state == BulkLoadDataState.ACTIVE) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad target is converging back to ACTIVE.");
        }
        if (transaction.getValue().getState() == BulkLoadState.BEGUN) {
            requireLoadingControl(
                    transaction,
                    transaction.getValue().getState(),
                    transaction.getValue().isFenceReady());
        }
        if (transaction.getValue().getState() == BulkLoadState.BEGUN
                && !transaction.getValue().isFenceReady()) {
            beginWaiters
                    .computeIfAbsent(existing.handle.getBulkLoadId(), ignored -> new ArrayList<>())
                    .add(new BeginWaiter(event, false, false));
            try {
                startLoading(transaction);
            } catch (Throwable failure) {
                failWaiters(existing.handle.getBulkLoadId(), failure);
                throw failure;
            }
        } else {
            completeBegin(event, false, transaction.getValue());
        }
    }

    /** Handles Commit on the Coordinator event thread. */
    public void process(CommitBulkLoadEvent event) {
        try {
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                    readTransactionVersioned(event.getHandle());
            if (transaction.getValue().getState() == BulkLoadState.COMMITTED) {
                if (hasManifest(event)) {
                    requireSameManifest(transaction.getValue(), event);
                }
                recordActiveTransaction(transaction);
                commitWaiters
                        .computeIfAbsent(
                                event.getHandle().getBulkLoadId(), ignored -> new ArrayList<>())
                        .add(event);
                startFinalActive(transaction);
                return;
            }
            if (transaction.getValue().getState() == BulkLoadState.BEGUN) {
                requireLoadingControl(transaction, BulkLoadState.BEGUN, true);
                long now = System.currentTimeMillis();
                if (transaction.getValue().getManifestPath() == null
                        && now >= transaction.getValue().getBuildDeadlineMs()) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad build deadline expired before Commit.");
                }
                if (transaction.getValue().getManifestPath() != null
                        && transaction.getValue().getCommitDecisionDeadlineMs() != null
                        && now >= transaction.getValue().getCommitDecisionDeadlineMs()) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad Commit decision deadline expired.");
                }
                if (transaction.getValue().getManifestPath() == null) {
                    requireManifest(event);
                    transaction =
                            metadataStore()
                                    .freezeManifest(
                                            transaction,
                                            readRegistration(event.getHandle()),
                                            event.getManifestPath(),
                                            event.getManifestLength(),
                                            event.getManifestSha256(),
                                            now,
                                            configuration
                                                    .get(
                                                            ConfigOptions
                                                                    .BULKLOAD_COMMIT_DECISION_TIMEOUT)
                                                    .toMillis(),
                                            coordinatorEpochVersion());
                }
            } else if (transaction.getValue().getState() != BulkLoadState.COMMITTING) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad Commit requires BEGUN or COMMITTING.");
            }
            if (hasManifest(event)) {
                requireSameManifest(transaction.getValue(), event);
            }
            commitWaiters
                    .computeIfAbsent(
                            event.getHandle().getBulkLoadId(), ignored -> new ArrayList<>())
                    .add(event);
            try {
                if (transaction.getValue().getState() == BulkLoadState.BEGUN) {
                    startLoading(transaction);
                } else {
                    resumeCommit(transaction);
                }
            } catch (Throwable failure) {
                failWaiters(event.getHandle().getBulkLoadId(), failure);
                throw failure;
            }
        } catch (Throwable failure) {
            event.getResultFuture().completeExceptionally(failure);
        }
    }

    /** Handles Abort on the Coordinator event thread. */
    public void process(AbortBulkLoadEvent event) {
        try {
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                    readTransactionVersioned(event.getHandle());
            if (transaction.getValue().getState() == BulkLoadState.ABORTED) {
                recordActiveTransaction(transaction);
                abortWaiters
                        .computeIfAbsent(
                                event.getHandle().getBulkLoadId(), ignored -> new ArrayList<>())
                        .add(event);
                startFinalActive(transaction);
                return;
            }
            if (transaction.getValue().getState() != BulkLoadState.BEGUN) {
                throw new InvalidBulkLoadRequestException("BulkLoad Abort requires BEGUN.");
            }
            abortWaiters
                    .computeIfAbsent(
                            event.getHandle().getBulkLoadId(), ignored -> new ArrayList<>())
                    .add(event);
            try {
                resumeAbort(transaction, BulkLoadAbortReason.ABORTED_BY_CALLER, null);
            } catch (Throwable failure) {
                failWaiters(event.getHandle().getBulkLoadId(), failure);
                throw failure;
            }
        } catch (Throwable failure) {
            event.getResultFuture().completeExceptionally(failure);
        }
    }

    /** Applies asynchronous I/O and RPC completions after revalidating persisted facts. */
    public void process(BulkLoadAsyncResultEvent event) {
        if (resultGc.processAsyncResult(event.getResult())) {
            return;
        }
        if (!(event.getResult() instanceof AsyncResult)) {
            return;
        }
        AsyncResult result = (AsyncResult) event.getResult();
        InFlight current = inFlight.get(result.handle.getBulkLoadId());
        if (current == null || current.token != result.token || current.phase != result.phase) {
            return;
        }
        inFlight.remove(result.handle.getBulkLoadId());
        try {
            if (result.failure != null) {
                handleAsyncFailure(result, result.failure);
                return;
            }
            switch (result.phase) {
                case LOADING:
                    finishLoading(result);
                    break;
                case VALIDATE_MANIFEST:
                    finishValidateManifest(result);
                    break;
                case RESTORE_REPLICAS:
                    finishRestoreReplicas(result);
                    break;
                case PUBLISH_ACTIVE:
                    finishPublishActive(result);
                    break;
                case ABORT_ACTIVE:
                    finishAbortActive(result);
                    break;
                case FINAL_ACTIVE:
                    finishFinalActive(result);
                    break;
                default:
                    throw new IllegalStateException("Unsupported BulkLoad async phase.");
            }
        } catch (Throwable failure) {
            handleAsyncFailure(result, failure);
        }
    }

    /** Processes fact-driven deadline recovery and retained-result cleanup. */
    public void process(BulkLoadMaintenanceEvent event) {
        resumeActiveTransactions();
        resultGc.runMaintenance(
                System.currentTimeMillis(), coordinatorContext.getCoordinatorZkVersion());
    }

    void startLoading(BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction)
            throws Exception {
        String id = transaction.getValue().getBulkLoadId();
        if (hasAnyInFlight(id)) {
            return;
        }
        requireLoadingControl(transaction, BulkLoadState.BEGUN, false);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(transaction.getValue().getHandle());
        List<BulkLoadMetadataStore.RegisteredServer> registrations =
                registrationsForRound(assignment, !transaction.getValue().isFenceReady());
        if (registrations == null) {
            return;
        }
        long token = beginInFlight(id, Phase.LOADING);
        BulkLoadReplicaConvergence.Attempt attempt =
                convergence.fenceTarget(transaction.getValue(), assignment, registrations);
        attempt.completion()
                .whenComplete(
                        (ignored, failure) ->
                                post(
                                        new AsyncResult(
                                                transaction.getValue().getHandle(),
                                                token,
                                                Phase.LOADING,
                                                new LoadingResult(
                                                        attempt.confirmation(), registrations),
                                                unwrap(failure))));
    }

    private void finishLoading(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        requireLoadingControl(transaction, BulkLoadState.BEGUN, false);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        LoadingResult loadingResult = (LoadingResult) result.value;
        BulkLoadReplicaConvergence.Confirmation confirmation = loadingResult.confirmation;
        if (!confirmation.matches(
                transaction.getValue(),
                assignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startLoading(transaction);
            return;
        }
        proveRemoteEmpty(result.handle, transaction.getValue(), assignment.getValue());
        long[] snapshotIds =
                transaction.getValue().isFenceReady()
                        ? null
                        : allocateSnapshotIds(transaction.getValue(), assignment.getValue());

        BulkLoadMetadataStore.Versioned<? extends TableAssignment> currentAssignment =
                readAssignment(result.handle);
        if (!confirmation.matches(
                transaction.getValue(),
                currentAssignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startLoading(transaction);
            return;
        }
        BulkLoadAbortReason deadline =
                BulkLoadStartupRecovery.deadlineReason(
                        transaction.getValue(), System.currentTimeMillis());
        if (abortWaiters.containsKey(result.handle.getBulkLoadId())) {
            resumeAbort(
                    transaction,
                    BulkLoadAbortReason.ABORTED_BY_CALLER,
                    null,
                    confirmation.holders());
            return;
        }
        if (deadline != null) {
            resumeAbort(transaction, deadline, null, confirmation.holders());
            return;
        }
        if (!transaction.getValue().isFenceReady()) {
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> completed =
                    metadataStore()
                            .markFenceReady(
                                    transaction,
                                    readRegistration(result.handle),
                                    currentAssignment,
                                    confirmation.holders(),
                                    snapshotIds,
                                    System.currentTimeMillis(),
                                    coordinatorEpochVersion());
            completeBeginWaiters(completed.getValue());
            return;
        }
        if (transaction.getValue().getManifestPath() != null) {
            startValidateManifest(transaction, confirmation);
        }
    }

    private void startValidateManifest(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            @Nullable BulkLoadReplicaConvergence.Confirmation confirmation)
            throws Exception {
        String id = transaction.getValue().getBulkLoadId();
        if (hasAnyInFlight(id)) {
            return;
        }
        BulkLoadTransaction value = transaction.getValue();
        if (value.getState() == BulkLoadState.BEGUN && confirmation == null) {
            throw new IllegalArgumentException(
                    "BEGUN manifest validation requires a LOADING confirmation.");
        }
        if (value.getState() != BulkLoadState.BEGUN
                && (value.getState() != BulkLoadState.COMMITTING || confirmation != null)) {
            throw new IllegalArgumentException("Invalid manifest validation state.");
        }
        final long[] snapshotIds = checkNotNull(value.getSnapshotIds());
        final TableInfo tableInfo = tableInfo(transaction.getValue().getHandle());
        final long maxManifestBytes =
                configuration.get(ConfigOptions.BULKLOAD_MANIFEST_MAX_SIZE).getBytes();
        final long maxInputBytes =
                configuration.get(ConfigOptions.BULKLOAD_INPUT_MAX_SIZE).getBytes();
        final long token = beginInFlight(id, Phase.VALIDATE_MANIFEST);
        try {
            ioExecutor.execute(
                    () -> {
                        ManifestValidationResult validationResult = null;
                        Throwable failure = null;
                        try {
                            List<BulkLoadManifestParser.ValidatedBucket> validatedBuckets =
                                    new BulkLoadManifestParser()
                                            .parse(
                                                    value.getHandle(),
                                                    value.getRemoteDataDir(),
                                                    value.getManifestPath(),
                                                    value.getManifestLength(),
                                                    value.getManifestSha256(),
                                                    snapshotIds,
                                                    tableInfo.getTableConfig().getChangelogImage(),
                                                    maxManifestBytes,
                                                    maxInputBytes);
                            validationResult =
                                    new ManifestValidationResult(validatedBuckets, confirmation);
                        } catch (Throwable error) {
                            failure = error;
                        }
                        post(
                                new AsyncResult(
                                        transaction.getValue().getHandle(),
                                        token,
                                        Phase.VALIDATE_MANIFEST,
                                        validationResult,
                                        failure));
                    });
        } catch (RuntimeException | Error failure) {
            removeInFlight(id, token);
            throw failure;
        }
    }

    private void finishValidateManifest(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        BulkLoadMetadataStore.Versioned<?> registration = readRegistration(result.handle);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        List<BulkLoadMetadataStore.RegisteredServer> registrations = readRegisteredServers();
        ManifestValidationResult validation = (ManifestValidationResult) result.value;
        requireLoadingControl(transaction, transaction.getValue().getState(), true, registration);
        if (transaction.getValue().getState() == BulkLoadState.BEGUN) {
            BulkLoadReplicaConvergence.Confirmation confirmation =
                    checkNotNull(validation.confirmation);
            if (!confirmation.matches(
                    transaction.getValue(),
                    assignment,
                    coordinatorContext,
                    registrations,
                    readyRegistrations)) {
                startLoading(transaction);
                return;
            }
            BulkLoadAbortReason deadline =
                    BulkLoadStartupRecovery.deadlineReason(
                            transaction.getValue(), System.currentTimeMillis());
            if (abortWaiters.containsKey(result.handle.getBulkLoadId())) {
                resumeAbort(
                        transaction,
                        BulkLoadAbortReason.ABORTED_BY_CALLER,
                        null,
                        confirmation.holders());
                return;
            }
            if (deadline != null) {
                resumeAbort(transaction, deadline, null, confirmation.holders());
                return;
            }
            transaction =
                    metadataStore()
                            .decideCommit(
                                    transaction,
                                    registration,
                                    assignment,
                                    confirmation.holders(),
                                    System.currentTimeMillis(),
                                    coordinatorEpochVersion());
        } else if (transaction.getValue().getState() != BulkLoadState.COMMITTING) {
            throw new InvalidBulkLoadRequestException(
                    "Manifest validation requires BEGUN or COMMITTING.");
        }
        adoptValidatedBuckets(transaction, registration, validation.validatedBuckets);
        startRestoreReplicas(transaction);
    }

    void resumeCommit(BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction)
            throws Exception {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        RegistrationState registration = registrationState(readRegistration(handle));
        if (transaction.getValue().getState() != BulkLoadState.COMMITTING) {
            throw new InvalidBulkLoadRequestException("Commit requires COMMITTING state.");
        }
        if (registration.state == BulkLoadDataState.LOADING) {
            startValidateManifest(transaction, null);
        } else if (registration.state == BulkLoadDataState.ACTIVE
                && transaction.getValue().getBulkLoadId().equals(registration.bulkLoadId)
                && registration.version == transaction.getValue().getMetadataVersion()) {
            startPublishActive(transaction);
        } else {
            throw new InvalidBulkLoadRequestException(
                    "Commit target metadata identity is inconsistent.");
        }
    }

    private void adoptValidatedBuckets(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadMetadataStore.Versioned<?> registration,
            List<BulkLoadManifestParser.ValidatedBucket> validatedBuckets)
            throws Exception {
        for (int bucketId = 0; bucketId < validatedBuckets.size(); bucketId++) {
            BulkLoadManifestParser.ValidatedBucket validatedBucket = validatedBuckets.get(bucketId);
            CompletedSnapshot snapshot = validatedBucket.getCompletedSnapshot();
            metadataStore()
                    .adoptBucketMetadata(
                            transaction,
                            registration,
                            bucketId,
                            new BucketSnapshot(
                                    snapshot.getSnapshotID(),
                                    snapshot.getLogOffset(),
                                    snapshot.getMetadataFilePath().toString()),
                            coordinatorEpochVersion());
            completedSnapshotStoreManager
                    .getOrCreateCompletedSnapshotStore(
                            transaction.getValue().getHandle().getTarget().getTablePath(),
                            snapshot.getTableBucket())
                    .adoptAfterNodeConfirmed(snapshot);
        }
    }

    private void startRestoreReplicas(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction) throws Exception {
        String id = transaction.getValue().getBulkLoadId();
        if (hasAnyInFlight(id)) {
            return;
        }
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(transaction.getValue().getHandle());
        List<BulkLoadMetadataStore.RegisteredServer> registrations =
                registrationsForRound(assignment, false);
        if (registrations == null) {
            return;
        }
        long token = beginInFlight(id, Phase.RESTORE_REPLICAS);
        BulkLoadReplicaConvergence.Attempt attempt =
                convergence.convergeLoadingReplicas(
                        transaction.getValue(), assignment, registrations);
        attempt.completion()
                .whenComplete(
                        (ignored, failure) ->
                                post(
                                        new AsyncResult(
                                                transaction.getValue().getHandle(),
                                                token,
                                                Phase.RESTORE_REPLICAS,
                                                attempt.confirmation(),
                                                unwrap(failure))));
    }

    private void finishRestoreReplicas(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        requireLoadingControl(transaction, BulkLoadState.COMMITTING, true);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        BulkLoadReplicaConvergence.Confirmation confirmation =
                (BulkLoadReplicaConvergence.Confirmation) result.value;
        if (!confirmation.matches(
                transaction.getValue(),
                assignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startRestoreReplicas(transaction);
            return;
        }
        BulkLoadMetadataStore.Versioned<?> registration = readRegistration(result.handle);
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> activated =
                metadataStore()
                        .activateTarget(
                                transaction,
                                registration,
                                assignment,
                                confirmation.holders(),
                                System.currentTimeMillis(),
                                coordinatorEpochVersion());
        startPublishActive(activated);
    }

    private void startPublishActive(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction) throws Exception {
        startActive(transaction, Phase.PUBLISH_ACTIVE);
    }

    void startFinalActive(BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction)
            throws Exception {
        requireReleasedControl(transaction);
        startActive(transaction, Phase.FINAL_ACTIVE);
    }

    private void startActive(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction, Phase phase)
            throws Exception {
        String id = transaction.getValue().getBulkLoadId();
        if (hasAnyInFlight(id)) {
            return;
        }
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(transaction.getValue().getHandle());
        List<BulkLoadMetadataStore.RegisteredServer> registrations =
                registrationsForRound(assignment, false);
        if (registrations == null) {
            return;
        }
        long token = beginInFlight(id, phase);
        BulkLoadReplicaConvergence.Attempt attempt =
                phase == Phase.ABORT_ACTIVE
                        ? convergence.convergeActiveReplicas(
                                transaction.getValue(), assignment, registrations)
                        : convergence.publishActiveMetadata(
                                transaction.getValue(), assignment, registrations);
        attempt.completion()
                .whenComplete(
                        (ignored, failure) ->
                                post(
                                        new AsyncResult(
                                                transaction.getValue().getHandle(),
                                                token,
                                                phase,
                                                attempt.confirmation(),
                                                unwrap(failure))));
    }

    private void finishPublishActive(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        BulkLoadReplicaConvergence.Confirmation confirmation =
                (BulkLoadReplicaConvergence.Confirmation) result.value;
        if (!confirmation.matches(
                transaction.getValue(),
                assignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startPublishActive(transaction);
            return;
        }
        requireActiveControl(transaction, BulkLoadState.COMMITTING);
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> finished =
                metadataStore()
                        .finishCommit(
                                transaction,
                                readRegistration(result.handle),
                                assignment,
                                confirmation.holders(),
                                System.currentTimeMillis(),
                                configuration
                                        .get(ConfigOptions.BULKLOAD_RESULT_RETENTION)
                                        .toMillis(),
                                coordinatorEpochVersion());
        recordActiveTransaction(finished);
        startFinalActive(finished);
    }

    void resumeAbort(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadAbortReason reason,
            @Nullable String lastError)
            throws Exception {
        resumeAbort(transaction, reason, lastError, null);
    }

    private void resumeAbort(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadAbortReason reason,
            @Nullable String lastError,
            @Nullable List<BulkLoadMetadataStore.RegisteredServer> confirmedHolders)
            throws Exception {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        RegistrationState registration = registrationState(readRegistration(handle));
        if (registration.state == BulkLoadDataState.LOADING) {
            if (confirmedHolders == null) {
                if (transaction.getValue().isFenceReady()) {
                    startLoading(transaction);
                    return;
                }
                BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                        readAssignment(handle);
                confirmedHolders = registrationsForRound(assignment, false);
                if (confirmedHolders == null) {
                    return;
                }
            }
            BulkLoadMetadataStore.Versioned<?> observation = readRegistration(handle);
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                    readAssignment(handle);
            transaction =
                    metadataStore()
                            .beginAbort(
                                    transaction,
                                    observation,
                                    assignment,
                                    BulkLoadReplicaConvergence.assignedHolders(
                                            assignment, confirmedHolders),
                                    reason,
                                    lastError,
                                    System.currentTimeMillis(),
                                    coordinatorEpochVersion());
        } else if (registration.state != BulkLoadDataState.ACTIVE
                || !transaction.getValue().getBulkLoadId().equals(registration.bulkLoadId)
                || registration.version != transaction.getValue().getMetadataVersion()) {
            throw new InvalidBulkLoadRequestException(
                    "Abort target metadata identity is inconsistent.");
        }
        removeInFlight(handle.getBulkLoadId(), Phase.LOADING);
        startActive(transaction, Phase.ABORT_ACTIVE);
    }

    private void finishAbortActive(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        BulkLoadReplicaConvergence.Confirmation confirmation =
                (BulkLoadReplicaConvergence.Confirmation) result.value;
        if (!confirmation.matches(
                transaction.getValue(),
                assignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startActive(transaction, Phase.ABORT_ACTIVE);
            return;
        }
        requireActiveControl(transaction, transaction.getValue().getState());
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> finished =
                metadataStore()
                        .finishAbort(
                                transaction,
                                readRegistration(result.handle),
                                assignment,
                                confirmation.holders(),
                                System.currentTimeMillis(),
                                configuration
                                        .get(ConfigOptions.BULKLOAD_RESULT_RETENTION)
                                        .toMillis(),
                                coordinatorEpochVersion());
        recordActiveTransaction(finished);
        startFinalActive(finished);
    }

    private void finishFinalActive(AsyncResult result) throws Exception {
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        BulkLoadReplicaConvergence.Confirmation confirmation =
                (BulkLoadReplicaConvergence.Confirmation) result.value;
        if (!confirmation.matches(
                transaction.getValue(),
                assignment,
                coordinatorContext,
                readRegisteredServers(),
                readyRegistrations)) {
            startFinalActive(transaction);
            return;
        }
        requireReleasedControl(transaction);
        activeTransactions.remove(result.handle);
        BulkLoadTransaction finished = transaction.getValue();
        if (finished.getState() == BulkLoadState.COMMITTED) {
            completeBeginWaiters(finished);
            completeCommitWaiters(result.handle.getBulkLoadId(), finished, null);
            return;
        }
        completeAbortWaiters(result.handle.getBulkLoadId(), finished, null);
        Throwable abortedFailure =
                new InvalidBulkLoadRequestException(
                        "BulkLoad aborted while the request was pending: "
                                + finished.getAbortReason());
        completeAbortedBeginWaiters(finished, abortedFailure);
        completeCommitWaiters(result.handle.getBulkLoadId(), null, abortedFailure);
    }

    private void handleAsyncFailure(AsyncResult result, Throwable failure) {
        if (result.phase == Phase.PUBLISH_ACTIVE || result.phase == Phase.ABORT_ACTIVE) {
            try {
                BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                        readTransactionVersioned(result.handle);
                if (transaction.getValue().getState() == BulkLoadState.COMMITTED
                        || transaction.getValue().getState() == BulkLoadState.ABORTED) {
                    recordActiveTransaction(transaction);
                    startFinalActive(transaction);
                    return;
                }
            } catch (Throwable reconciliationFailure) {
                failure.addSuppressed(reconciliationFailure);
            }
        }
        try {
            if (restartChangedConvergenceRound(result)) {
                return;
            }
        } catch (Throwable comparisonFailure) {
            failure.addSuppressed(comparisonFailure);
        }
        if (result.phase == Phase.LOADING && isInvalidBulkLoad(failure)) {
            try {
                BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                        readTransactionVersioned(result.handle);
                LoadingResult loadingResult = (LoadingResult) result.value;
                resumeAbort(
                        transaction,
                        BulkLoadAbortReason.TARGET_NOT_EMPTY,
                        ExceptionUtils.stringifyException(failure),
                        loadingResult.registrations);
                return;
            } catch (Throwable abortFailure) {
                failure.addSuppressed(abortFailure);
            }
        }
        failWaiters(result.handle.getBulkLoadId(), failure);
    }

    private boolean restartChangedConvergenceRound(AsyncResult result) throws Exception {
        BulkLoadReplicaConvergence.Confirmation confirmation;
        if (result.phase == Phase.LOADING) {
            confirmation = ((LoadingResult) result.value).confirmation;
        } else if (result.phase == Phase.RESTORE_REPLICAS
                || result.phase == Phase.PUBLISH_ACTIVE
                || result.phase == Phase.ABORT_ACTIVE
                || result.phase == Phase.FINAL_ACTIVE) {
            confirmation = (BulkLoadReplicaConvergence.Confirmation) result.value;
        } else {
            return false;
        }
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment =
                readAssignment(result.handle);
        if (confirmation.matchesRoundFacts(
                assignment, coordinatorContext, readRegisteredServers(), readyRegistrations)) {
            return false;
        }
        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                readTransactionVersioned(result.handle);
        if (result.phase == Phase.LOADING) {
            startLoading(transaction);
        } else if (result.phase == Phase.RESTORE_REPLICAS) {
            startRestoreReplicas(transaction);
        } else if (result.phase == Phase.PUBLISH_ACTIVE) {
            startPublishActive(transaction);
        } else if (result.phase == Phase.FINAL_ACTIVE) {
            startFinalActive(transaction);
        } else {
            startActive(transaction, Phase.ABORT_ACTIVE);
        }
        return true;
    }

    private long[] allocateSnapshotIds(BulkLoadTransaction transaction, TableAssignment assignment)
            throws Exception {
        int count = targetBucketCount(transaction.getHandle());
        requireCompleteAssignment(transaction.getHandle(), assignment, count);
        long[] snapshotIds = new long[count];
        for (int bucketId = 0; bucketId < count; bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(
                            transaction.getTableId(), transaction.getPartitionId(), bucketId);
            snapshotIds[bucketId] = zkClient.allocateTableBucketSnapshotId(tableBucket);
        }
        return snapshotIds;
    }

    private void proveRemoteEmpty(
            BulkLoadHandle handle, BulkLoadTransaction transaction, TableAssignment assignment)
            throws Exception {
        int count = targetBucketCount(handle);
        requireCompleteAssignment(handle, assignment, count);
        for (int bucketId = 0; bucketId < count; bucketId++) {
            TableBucket bucket =
                    new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
            if (!zkClient.listBucketSnapshotIds(bucket).isEmpty()
                    || zkClient.getRemoteLogManifestHandle(bucket).isPresent()) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad target has ordinary remote data for " + bucket + '.');
            }
        }
    }

    private BeginAdmission readBeginAdmission(BeginBulkLoadEvent event) throws Exception {
        TablePath tablePath = event.getTarget().getTablePath();
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        if (tableInfo.getPrimaryKeys().isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad requires a primary-key table.");
        }
        SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
        if (!schemaInfo.getSchema().getAutoIncrementColumnNames().isEmpty()) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad does not support auto-increment columns.");
        }
        Long partitionId = null;
        BulkLoadMetadataStore.Versioned<?> registration;
        BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment;
        String remoteDataDir;
        if (event.getTarget().getPartitionName() == null) {
            BulkLoadMetadataStore.Versioned<TableRegistration> table =
                    requireFound(
                            ZkData.TableZNode.path(tablePath),
                            ZkData.TableZNode::decode,
                            "table registration");
            if (table.getValue().tableId != tableInfo.getTableId()
                    || table.getValue().dataState != BulkLoadDataState.ACTIVE) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad table identity or state changed.");
            }
            registration = table;
            assignment =
                    requireFound(
                            ZkData.TableIdZNode.path(tableInfo.getTableId()),
                            ZkData.TableIdZNode::decode,
                            "table assignment");
            remoteDataDir = table.getValue().remoteDataDir;
        } else {
            BulkLoadMetadataStore.Versioned<PartitionRegistration> partition =
                    requireFound(
                            ZkData.PartitionZNode.path(
                                    tablePath, event.getTarget().getPartitionName()),
                            ZkData.PartitionZNode::decode,
                            "partition registration");
            partitionId = partition.getValue().getPartitionId();
            if (partition.getValue().getTableId() != tableInfo.getTableId()
                    || partition.getValue().getDataState() != BulkLoadDataState.ACTIVE) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad partition identity or state changed.");
            }
            registration = partition;
            assignment =
                    requireFound(
                            ZkData.PartitionIdZNode.path(partitionId),
                            ZkData.PartitionIdZNode::decode,
                            "partition assignment");
            remoteDataDir = partition.getValue().getRemoteDataDir();
        }
        if (remoteDataDir == null || remoteDataDir.isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad remote data directory is missing.");
        }
        requireCompleteAssignment(
                tableInfo.getTableId(),
                partitionId,
                assignment.getValue(),
                tableInfo.getNumBuckets());
        validateTabletCapabilities(readRegisteredServers());
        int active = activeTransactions.size();
        if (active >= configuration.get(ConfigOptions.BULKLOAD_MAX_ACTIVE_TRANSACTIONS)) {
            throw new InvalidBulkLoadRequestException("BulkLoad active transaction limit reached.");
        }
        String bulkLoadId = UUID.randomUUID().toString();
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        event.getTarget(), tableInfo.getTableId(), partitionId, bulkLoadId);
        String historyParent = BulkLoadMetadataStore.transactionParentPath(handle);
        if (zkClient.pathExists(historyParent)
                && zkClient.getChildren(historyParent).size()
                        >= configuration.get(ConfigOptions.BULKLOAD_MAX_TRANSACTIONS_PER_TARGET)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad transaction history limit reached for the target.");
        }
        requireFound(
                ZkData.SchemaZNode.path(tablePath, schemaInfo.getSchemaId()),
                ZkData.SchemaZNode::decode,
                "schema");
        long timeout =
                event.getBuildTimeoutMs() == null
                        ? configuration.get(ConfigOptions.BULKLOAD_BUILD_TIMEOUT_DEFAULT).toMillis()
                        : event.getBuildTimeoutMs();
        if (timeout <= 0
                || timeout
                        > configuration.get(ConfigOptions.BULKLOAD_BUILD_TIMEOUT_MAX).toMillis()) {
            throw new InvalidBulkLoadRequestException("Invalid BulkLoad build timeout.");
        }
        long now = System.currentTimeMillis();
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        handle,
                        BulkLoadState.BEGUN,
                        event.getCallerToken(),
                        event.getCreator().getName(),
                        event.getCreator().getType(),
                        remoteDataDir,
                        schemaInfo.getSchemaId(),
                        registration.getPath(),
                        registration.getVersion() + 1,
                        null,
                        now,
                        now,
                        Math.addExact(now, timeout),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        return new BeginAdmission(registration, assignment, transaction);
    }

    private void requireClusterCapabilities() throws Exception {
        List<CoordinatorAddress> coordinators = new ArrayList<>();
        for (String id : zkClient.getCoordinatorServerList()) {
            coordinators.add(
                    requireFound(
                                    ZkData.CoordinatorIdZNode.path(id),
                                    ZkData.CoordinatorIdZNode::decode,
                                    "Coordinator registration")
                            .getValue());
        }
        List<BulkLoadMetadataStore.RegisteredServer> tablets = readRegisteredServers();
        if (!ServerApiVersionSupport.coordinatorReady(coordinators)) {
            throw new UnsupportedVersionException(
                    "Every Coordinator must cover the BulkLoad capability set.");
        }
        validateTabletCapabilities(tablets);
    }

    private static void validateTabletCapabilities(
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        List<TabletServerRegistration> values = new ArrayList<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            values.add(registration.registration.getValue());
        }
        if (!ServerApiVersionSupport.tabletServersReady(values)) {
            throw new UnsupportedVersionException(
                    "Every TabletServer must cover the BulkLoad capability set.");
        }
    }

    List<BulkLoadMetadataStore.RegisteredServer> readRegisteredServers() throws Exception {
        List<BulkLoadMetadataStore.RegisteredServer> result = new ArrayList<>();
        for (int id : zkClient.getSortedTabletServerList()) {
            result.add(
                    new BulkLoadMetadataStore.RegisteredServer(
                            id,
                            requireFound(
                                    ZkData.ServerIdZNode.path(id),
                                    ZkData.ServerIdZNode::decode,
                                    "TabletServer registration")));
        }
        return Collections.unmodifiableList(result);
    }

    private @Nullable List<BulkLoadMetadataStore.RegisteredServer> registrationsForRound(
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            boolean requireAllHolders)
            throws Exception {
        List<BulkLoadMetadataStore.RegisteredServer> registrations = readRegisteredServers();
        List<BulkLoadMetadataStore.RegisteredServer> recipients =
                BulkLoadReplicaConvergence.liveRecipients(
                        coordinatorContext, registrations, readyRegistrations);
        if (!recipients.isEmpty()) {
            validateTabletCapabilities(recipients);
        }
        Map<Integer, BulkLoadMetadataStore.RegisteredServer> registeredById = new HashMap<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            registeredById.put(registration.serverId, registration);
        }
        Set<Integer> recipientIds = new HashSet<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : recipients) {
            recipientIds.add(registration.serverId);
        }
        Set<Integer> holderIds = new HashSet<>();
        for (Integer bucketId : assignment.getValue().getBuckets()) {
            holderIds.addAll(assignment.getValue().getBucketAssignment(bucketId).getReplicas());
        }
        for (Integer holderId : holderIds) {
            if (!registeredById.containsKey(holderId)) {
                if (requireAllHolders) {
                    return null;
                }
                continue;
            }
            if (!recipientIds.contains(holderId)) {
                return null;
            }
        }
        return recipients;
    }

    private void requireLoadingControl(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadState state,
            boolean requireFenceReady) {
        BulkLoadTransaction value = transaction.getValue();
        requireLoadingControl(
                transaction, state, requireFenceReady, readRegistration(value.getHandle()));
    }

    private static void requireLoadingControl(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction,
            BulkLoadState state,
            boolean requireFenceReady,
            BulkLoadMetadataStore.Versioned<?> registrationObservation) {
        BulkLoadTransaction value = transaction.getValue();
        if (value.getState() != state || (requireFenceReady && !value.isFenceReady())) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad transaction is not in the required LOADING state.");
        }
        RegistrationState registration = registrationState(registrationObservation);
        if (registration.state != BulkLoadDataState.LOADING
                || !value.getBulkLoadId().equals(registration.bulkLoadId)
                || !value.getMetadataPath().equals(registration.path)
                || value.getMetadataVersion() != registration.version) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad LOADING metadata identity changed.");
        }
    }

    private void requireActiveControl(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction, BulkLoadState state) {
        BulkLoadTransaction value = transaction.getValue();
        RegistrationState registration = registrationState(readRegistration(value.getHandle()));
        if (value.getState() != state
                || registration.state != BulkLoadDataState.ACTIVE
                || !value.getBulkLoadId().equals(registration.bulkLoadId)
                || !value.getMetadataPath().equals(registration.path)
                || value.getMetadataVersion() != registration.version) {
            throw new InvalidBulkLoadRequestException("BulkLoad ACTIVE metadata identity changed.");
        }
    }

    private void requireReleasedControl(
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction) {
        BulkLoadTransaction value = transaction.getValue();
        RegistrationState registration = registrationState(readRegistration(value.getHandle()));
        if ((value.getState() != BulkLoadState.COMMITTED
                        && value.getState() != BulkLoadState.ABORTED)
                || registration.state != BulkLoadDataState.ACTIVE
                || registration.bulkLoadId != null
                || !value.getMetadataPath().equals(registration.path)
                || value.getMetadataVersion() != registration.version) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad released metadata identity changed.");
        }
    }

    BulkLoadMetadataStore.Versioned<BulkLoadTransaction> readTransactionVersioned(
            BulkLoadHandle handle) {
        return requireFoundRead(readTransaction(handle), "transaction");
    }

    BulkLoadMetadataStore.ReadResult<BulkLoadTransaction> readTransaction(BulkLoadHandle handle) {
        BulkLoadMetadataStore.ReadResult<BulkLoadTransaction> read =
                metadataStore()
                        .read(
                                BulkLoadMetadataStore.transactionPath(handle),
                                handle.getPartitionId() == null
                                        ? ZkData.BulkLoadTableTransactionZNode::decode
                                        : ZkData.BulkLoadPartitionTransactionZNode::decode);
        if (read.getStatus() == BulkLoadMetadataStore.ReadResult.Status.FOUND) {
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> result = read.getVersioned();
            if (!result.getValue().getHandle().equals(handle)) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad handle does not match the persisted transaction.");
            }
        }
        return read;
    }

    BulkLoadMetadataStore.Versioned<? extends TableAssignment> readAssignment(
            BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? requireFound(
                        ZkData.TableIdZNode.path(handle.getTableId()),
                        ZkData.TableIdZNode::decode,
                        "table assignment")
                : requireFound(
                        ZkData.PartitionIdZNode.path(handle.getPartitionId()),
                        ZkData.PartitionIdZNode::decode,
                        "partition assignment");
    }

    BulkLoadMetadataStore.Versioned<?> readRegistration(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? requireFound(
                        ZkData.TableZNode.path(handle.getTarget().getTablePath()),
                        ZkData.TableZNode::decode,
                        "table registration")
                : requireFound(
                        ZkData.PartitionZNode.path(
                                handle.getTarget().getTablePath(),
                                handle.getTarget().getPartitionName()),
                        ZkData.PartitionZNode::decode,
                        "partition registration");
    }

    <T> BulkLoadMetadataStore.Versioned<T> requireFound(
            String path, Function<byte[], T> decoder, String description) {
        return requireFoundRead(metadataStore().read(path, decoder), description);
    }

    static <T> BulkLoadMetadataStore.Versioned<T> requireFoundRead(
            BulkLoadMetadataStore.ReadResult<T> read, String description) {
        if (read.getStatus() == BulkLoadMetadataStore.ReadResult.Status.FOUND) {
            return read.getVersioned();
        }
        if (read.getStatus() == BulkLoadMetadataStore.ReadResult.Status.UNKNOWN) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad " + description + " is unreadable.", read.getFailure());
        }
        throw new InvalidBulkLoadRequestException("Missing BulkLoad " + description + '.');
    }

    private BulkLoadMetadataStore metadataStore() {
        return new BulkLoadMetadataStore(zkClient, zookeeperTransactionBytes());
    }

    int coordinatorEpochVersion() {
        return coordinatorContext.getCoordinatorZkVersion();
    }

    Configuration configuration() {
        return configuration;
    }

    private long zookeeperTransactionBytes() {
        return (long) configuration.get(ConfigOptions.ZOOKEEPER_MAX_BUFFER_SIZE) * 4L / 5L;
    }

    private int targetBucketCount(BulkLoadHandle handle) {
        return tableInfo(handle).getNumBuckets();
    }

    private TableInfo tableInfo(BulkLoadHandle handle) {
        TableInfo info = metadataManager.getTable(handle.getTarget().getTablePath());
        if (info.getTableId() != handle.getTableId()) {
            throw new InvalidBulkLoadRequestException("BulkLoad physical table identity changed.");
        }
        return info;
    }

    private ExistingTarget findExistingTarget(PhysicalTablePath path) throws Exception {
        TableInfo table = metadataManager.getTable(path.getTablePath());
        Long partitionId = null;
        RegistrationState registration;
        if (path.getPartitionName() != null) {
            BulkLoadMetadataStore.Versioned<PartitionRegistration> partition =
                    requireFound(
                            ZkData.PartitionZNode.path(
                                    path.getTablePath(), path.getPartitionName()),
                            ZkData.PartitionZNode::decode,
                            "partition registration");
            partitionId = partition.getValue().getPartitionId();
            registration = registrationState(partition);
        } else {
            BulkLoadMetadataStore.Versioned<TableRegistration> tableRegistration =
                    requireFound(
                            ZkData.TableZNode.path(path.getTablePath()),
                            ZkData.TableZNode::decode,
                            "table registration");
            if (tableRegistration.getValue().tableId != table.getTableId()) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad physical target identity changed.");
            }
            registration = registrationState(tableRegistration);
        }
        if (registration.bulkLoadId == null) {
            return null;
        }
        return new ExistingTarget(
                new BulkLoadHandle(path, table.getTableId(), partitionId, registration.bulkLoadId));
    }

    private @Nullable BulkLoadMetadataStore.Versioned<BulkLoadTransaction> findRetainedSubmission(
            BeginBulkLoadEvent event) throws Exception {
        PhysicalTablePath target = event.getTarget();
        TableInfo table = metadataManager.getTable(target.getTablePath());
        Long partitionId = null;
        if (target.getPartitionName() != null) {
            BulkLoadMetadataStore.Versioned<PartitionRegistration> partition =
                    requireFound(
                            ZkData.PartitionZNode.path(
                                    target.getTablePath(), target.getPartitionName()),
                            ZkData.PartitionZNode::decode,
                            "partition registration");
            if (partition.getValue().getTableId() != table.getTableId()) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad physical target identity changed.");
            }
            partitionId = partition.getValue().getPartitionId();
        }
        String parent =
                partitionId == null
                        ? ZkData.BulkLoadTableTransactionsZNode.path(table.getTableId())
                        : ZkData.BulkLoadPartitionTransactionsZNode.path(partitionId);
        Optional<ChildrenWithStat> history = zkClient.getChildrenWithStatIfExists(parent);
        if (!history.isPresent()) {
            return null;
        }
        List<String> children = history.get().getChildren();
        if (children.size()
                > configuration.get(ConfigOptions.BULKLOAD_MAX_TRANSACTIONS_PER_TARGET)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad retained transaction history exceeds its scan bound.");
        }

        BulkLoadMetadataStore.Versioned<BulkLoadTransaction> match = null;
        int matchCount = 0;
        boolean differentCreator = false;
        boolean nonTerminal = false;
        for (String child : children) {
            BulkLoadHandle handle;
            try {
                handle = new BulkLoadHandle(target, table.getTableId(), partitionId, child);
            } catch (IllegalArgumentException malformedChild) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad retained transaction history is malformed.");
            }
            BulkLoadMetadataStore.ReadResult<BulkLoadTransaction> read =
                    metadataStore()
                            .read(
                                    BulkLoadMetadataStore.transactionPath(handle),
                                    partitionId == null
                                            ? ZkData.BulkLoadTableTransactionZNode::decode
                                            : ZkData.BulkLoadPartitionTransactionZNode::decode);
            if (read.getStatus() == BulkLoadMetadataStore.ReadResult.Status.NOT_FOUND) {
                continue;
            }
            BulkLoadMetadataStore.Versioned<BulkLoadTransaction> transaction =
                    requireFoundRead(read, "retained transaction");
            if (!transaction.getValue().getHandle().equals(handle)) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad retained transaction history is inconsistent.");
            }
            boolean sameToken =
                    event.getCallerToken().equals(transaction.getValue().getCallerToken());
            boolean sameOwner = sameToken && sameCreator(event, transaction.getValue());
            if (sameToken && !sameOwner) {
                differentCreator = true;
            }
            if (transaction.getValue().getState() != BulkLoadState.COMMITTED
                    && transaction.getValue().getState() != BulkLoadState.ABORTED) {
                nonTerminal = true;
                continue;
            }
            if (!sameOwner) {
                continue;
            }
            matchCount++;
            match = transaction;
        }
        if (differentCreator) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad target is occupied by another submission.");
        }
        if (nonTerminal) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad retained transaction history contains a non-terminal transaction.");
        }
        if (matchCount > 1) {
            throw new InvalidBulkLoadRequestException(
                    "Multiple retained BulkLoad transactions match the submission.");
        }
        return match;
    }

    private static boolean sameCreator(BeginBulkLoadEvent event, BulkLoadTransaction transaction) {
        return event.getCreator().getName().equals(transaction.getCreatorName())
                && event.getCreator().getType().equals(transaction.getCreatorType());
    }

    private void completeBegin(
            BeginBulkLoadEvent event, boolean created, BulkLoadTransaction transaction) {
        BeginBulkLoadResponse response =
                new BeginBulkLoadResponse().setCreated(created).setStatus(toStatus(transaction));
        if (transaction.getState() == BulkLoadState.BEGUN && transaction.isFenceReady()) {
            response.setTargetInfo(toTargetInfo(transaction, tableInfo(transaction.getHandle())));
        }
        event.getResultFuture().complete(response);
    }

    private void completeBeginWaiters(BulkLoadTransaction transaction) {
        List<BeginWaiter> waiters = beginWaiters.remove(transaction.getBulkLoadId());
        if (waiters == null) {
            return;
        }
        for (BeginWaiter waiter : waiters) {
            completeBegin(waiter.event, waiter.created, transaction);
        }
    }

    private void completeBeginWaitersExceptionally(String id, Throwable failure) {
        List<BeginWaiter> waiters = beginWaiters.remove(id);
        if (waiters != null) {
            for (BeginWaiter waiter : waiters) {
                waiter.event.getResultFuture().completeExceptionally(failure);
            }
        }
    }

    private void completeAbortedBeginWaiters(
            BulkLoadTransaction transaction, Throwable abortedFailure) {
        List<BeginWaiter> waiters = beginWaiters.remove(transaction.getBulkLoadId());
        if (waiters == null) {
            return;
        }
        for (BeginWaiter waiter : waiters) {
            if (waiter.terminalResult
                    || transaction.getAbortReason() == BulkLoadAbortReason.TARGET_NOT_EMPTY) {
                completeBegin(waiter.event, waiter.created, transaction);
            } else {
                waiter.event.getResultFuture().completeExceptionally(abortedFailure);
            }
        }
    }

    private static void completeCommit(CommitBulkLoadEvent event, BulkLoadTransaction transaction) {
        event.getResultFuture()
                .complete(new CommitBulkLoadResponse().setStatus(toStatus(transaction)));
    }

    private static void completeAbort(AbortBulkLoadEvent event, BulkLoadTransaction transaction) {
        event.getResultFuture()
                .complete(new AbortBulkLoadResponse().setStatus(toStatus(transaction)));
    }

    private void completeCommitWaiters(
            String id, @Nullable BulkLoadTransaction transaction, @Nullable Throwable failure) {
        List<CommitBulkLoadEvent> waiters = commitWaiters.remove(id);
        if (waiters != null) {
            for (CommitBulkLoadEvent waiter : waiters) {
                if (failure == null) {
                    completeCommit(waiter, transaction);
                } else {
                    waiter.getResultFuture().completeExceptionally(failure);
                }
            }
        }
    }

    private void completeAbortWaiters(
            String id, @Nullable BulkLoadTransaction transaction, @Nullable Throwable failure) {
        List<AbortBulkLoadEvent> waiters = abortWaiters.remove(id);
        if (waiters != null) {
            for (AbortBulkLoadEvent waiter : waiters) {
                if (failure == null) {
                    completeAbort(waiter, transaction);
                } else {
                    waiter.getResultFuture().completeExceptionally(failure);
                }
            }
        }
    }

    private void failWaiters(String id, Throwable failure) {
        inFlight.remove(id);
        completeBeginWaitersExceptionally(id, failure);
        completeCommitWaiters(id, null, failure);
        completeAbortWaiters(id, null, failure);
    }

    private void post(AsyncResult result) {
        eventManager.put(new BulkLoadAsyncResultEvent(result));
    }

    private long beginInFlight(String id, Phase phase) {
        long token = ++nextToken;
        inFlight.put(id, new InFlight(token, phase));
        return token;
    }

    private boolean hasAnyInFlight(String id) {
        return inFlight.containsKey(id);
    }

    private void removeInFlight(String id, long token) {
        InFlight current = inFlight.get(id);
        if (current != null && current.token == token) {
            inFlight.remove(id);
        }
    }

    private void removeInFlight(String id, Phase phase) {
        InFlight current = inFlight.get(id);
        if (current != null && current.phase == phase) {
            inFlight.remove(id);
        }
    }

    private static RegistrationState registrationState(
            BulkLoadMetadataStore.Versioned<?> registration) {
        if (registration.getValue() instanceof TableRegistration) {
            TableRegistration table = (TableRegistration) registration.getValue();
            return new RegistrationState(
                    registration.getPath(),
                    registration.getVersion(),
                    table.dataState,
                    table.bulkLoadId);
        }
        PartitionRegistration partition = (PartitionRegistration) registration.getValue();
        return new RegistrationState(
                registration.getPath(),
                registration.getVersion(),
                partition.getDataState(),
                partition.getBulkLoadId());
    }

    private static void requireSameManifest(
            BulkLoadTransaction transaction, CommitBulkLoadEvent request) {
        requireManifest(request);
        if (!request.getManifestPath().equals(transaction.getManifestPath())
                || !request.getManifestLength().equals(transaction.getManifestLength())
                || !request.getManifestSha256().equals(transaction.getManifestSha256())) {
            throw new InvalidBulkLoadRequestException(
                    "Commit retry conflicts with the frozen manifest.");
        }
    }

    private static boolean hasManifest(CommitBulkLoadEvent request) {
        return request.getManifestPath() != null
                || request.getManifestLength() != null
                || request.getManifestSha256() != null;
    }

    private static void requireManifest(CommitBulkLoadEvent request) {
        if (request.getManifestPath() == null
                || request.getManifestLength() == null
                || request.getManifestSha256() == null) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad Commit requires a manifest before the commit decision.");
        }
    }

    private static void requireCompleteAssignment(
            BulkLoadHandle handle, TableAssignment assignment, int count) {
        requireCompleteAssignment(handle.getTableId(), handle.getPartitionId(), assignment, count);
    }

    private static void requireCompleteAssignment(
            long tableId, @Nullable Long partitionId, TableAssignment assignment, int count) {
        Set<Integer> expected = new HashSet<>();
        for (int bucket = 0; bucket < count; bucket++) {
            expected.add(bucket);
        }
        if (!assignment.getBuckets().equals(expected)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad assignment does not cover the complete physical target.");
        }
        if (partitionId != null
                && (!(assignment instanceof PartitionAssignment)
                        || ((PartitionAssignment) assignment).getTableId() != tableId)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad partition assignment identity changed.");
        }
    }

    private static boolean isInvalidBulkLoad(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof InvalidBulkLoadRequestException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable unwrap(@Nullable Throwable failure) {
        Throwable result = failure;
        while ((result instanceof CompletionException) && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    public static BulkLoadTransaction readTransaction(
            ZooKeeperClient zkClient, BulkLoadHandle handle) {
        try {
            Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(transactionPath(handle));
            if (!data.isPresent()) {
                throw new BulkLoadNotFoundException("BulkLoad transaction does not exist.");
            }
            BulkLoadTransaction transaction =
                    handle.getPartitionId() == null
                            ? ZkData.BulkLoadTableTransactionZNode.decode(data.get().getData())
                            : ZkData.BulkLoadPartitionTransactionZNode.decode(data.get().getData());
            if (!transaction.getHandle().equals(handle)) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad handle does not match the persisted transaction.");
            }
            return transaction;
        } catch (BulkLoadNotFoundException
                | InvalidBulkLoadRequestException
                | CorruptMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to read BulkLoad transaction.", e);
        }
    }

    private static PbBulkLoadStatus toStatus(BulkLoadTransaction transaction) {
        PbBulkLoadStatus status =
                new PbBulkLoadStatus()
                        .setHandle(toPbHandle(transaction.getHandle()))
                        .setState(transaction.getState().getCode());
        if (transaction.getAbortReason() != null) {
            status.setAbortReason(transaction.getAbortReason().getCode());
        }
        if (transaction.getAbortMessage() != null) {
            status.setAbortMessage(transaction.getAbortMessage());
        }
        return status;
    }

    private static PbBulkLoadHandle toPbHandle(BulkLoadHandle handle) {
        PbBulkLoadHandle result =
                new PbBulkLoadHandle()
                        .setTarget(fromPhysicalTablePath(handle.getTarget()))
                        .setTableId(handle.getTableId())
                        .setBulkLoadId(handle.getBulkLoadId());
        if (handle.getPartitionId() != null) {
            result.setPartitionId(handle.getPartitionId());
        }
        return result;
    }

    private static PbBulkLoadTargetInfo toTargetInfo(
            BulkLoadTransaction transaction, TableInfo tableInfo) {
        long[] snapshotIds = checkNotNull(transaction.getSnapshotIds());
        if (snapshotIds.length != tableInfo.getNumBuckets()) {
            throw new IllegalArgumentException(
                    "BulkLoad Snapshot IDs must cover every physical bucket.");
        }
        return new PbBulkLoadTargetInfo()
                .setHandle(toPbHandle(transaction.getHandle()))
                .setSchemaId(transaction.getSchemaId())
                .setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                .setCreatedTime(tableInfo.getCreatedTime())
                .setModifiedTime(tableInfo.getModifiedTime())
                .setRemoteDataDir(transaction.getRemoteDataDir())
                .setSnapshotIds(snapshotIds);
    }

    private static String transactionPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionZNode.path(
                        handle.getTableId(), handle.getBulkLoadId())
                : ZkData.BulkLoadPartitionTransactionZNode.path(
                        handle.getPartitionId(), handle.getBulkLoadId());
    }

    private enum Phase {
        LOADING,
        VALIDATE_MANIFEST,
        RESTORE_REPLICAS,
        PUBLISH_ACTIVE,
        ABORT_ACTIVE,
        FINAL_ACTIVE
    }

    private static final class BeginWaiter {
        private final BeginBulkLoadEvent event;
        private final boolean created;
        private final boolean terminalResult;

        private BeginWaiter(BeginBulkLoadEvent event, boolean created, boolean terminalResult) {
            this.event = event;
            this.created = created;
            this.terminalResult = terminalResult;
        }
    }

    /** Exact ZooKeeper observations needed by the Begin checked multi. */
    private static final class BeginAdmission {
        private final BulkLoadMetadataStore.Versioned<?> registration;
        private final BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment;
        private final BulkLoadTransaction transaction;

        private BeginAdmission(
                BulkLoadMetadataStore.Versioned<?> registration,
                BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
                BulkLoadTransaction transaction) {
            this.registration = registration;
            this.assignment = assignment;
            this.transaction = transaction;
        }
    }

    private static final class ExistingTarget {
        private final BulkLoadHandle handle;

        private ExistingTarget(BulkLoadHandle handle) {
            this.handle = handle;
        }
    }

    private static final class InFlight {
        private final long token;
        private final Phase phase;

        private InFlight(long token, Phase phase) {
            this.token = token;
            this.phase = phase;
        }
    }

    private static final class AsyncResult {
        private final BulkLoadHandle handle;
        private final long token;
        private final Phase phase;
        private final @Nullable Object value;
        private final @Nullable Throwable failure;

        private AsyncResult(
                BulkLoadHandle handle,
                long token,
                Phase phase,
                @Nullable Object value,
                @Nullable Throwable failure) {
            this.handle = handle;
            this.token = token;
            this.phase = phase;
            this.value = value;
            this.failure = failure;
        }
    }

    private static final class LoadingResult {
        private final BulkLoadReplicaConvergence.Confirmation confirmation;
        private final List<BulkLoadMetadataStore.RegisteredServer> registrations;

        private LoadingResult(
                BulkLoadReplicaConvergence.Confirmation confirmation,
                List<BulkLoadMetadataStore.RegisteredServer> registrations) {
            this.confirmation = confirmation;
            this.registrations = registrations;
        }
    }

    private static final class ManifestValidationResult {
        private final List<BulkLoadManifestParser.ValidatedBucket> validatedBuckets;
        private final @Nullable BulkLoadReplicaConvergence.Confirmation confirmation;

        private ManifestValidationResult(
                List<BulkLoadManifestParser.ValidatedBucket> validatedBuckets,
                @Nullable BulkLoadReplicaConvergence.Confirmation confirmation) {
            this.validatedBuckets = Collections.unmodifiableList(new ArrayList<>(validatedBuckets));
            this.confirmation = confirmation;
        }
    }

    private static final class RegistrationState {
        private final String path;
        private final int version;
        private final BulkLoadDataState state;
        private final @Nullable String bulkLoadId;

        private RegistrationState(
                String path, int version, BulkLoadDataState state, @Nullable String bulkLoadId) {
            this.path = path;
            this.version = version;
            this.state = state;
            this.bulkLoadId = bulkLoadId;
        }
    }
}
