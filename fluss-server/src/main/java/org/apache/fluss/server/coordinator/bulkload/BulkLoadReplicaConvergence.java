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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorRequestBatch;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;

/** Converges one BulkLoad target using only ordinary TabletServer metadata and replica RPCs. */
final class BulkLoadReplicaConvergence {

    private final CoordinatorContext context;
    private final CoordinatorRequestBatch requestBatch;
    private final Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations;

    BulkLoadReplicaConvergence(
            CoordinatorContext context,
            CoordinatorRequestBatch requestBatch,
            Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations) {
        this.context = context;
        this.requestBatch = requestBatch;
        this.readyRegistrations = readyRegistrations;
    }

    Attempt fenceTarget(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        return applyMetadata(transaction, assignment, registrations, BulkLoadDataState.LOADING);
    }

    Attempt convergeLoadingReplicas(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        return convergeAssignment(
                transaction, assignment, registrations, BulkLoadDataState.LOADING, false);
    }

    Attempt convergeActiveReplicas(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        return convergeAssignment(
                transaction, assignment, registrations, BulkLoadDataState.ACTIVE, true);
    }

    private Attempt convergeAssignment(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations,
            BulkLoadDataState state,
            boolean applyMetadataAfterRoles) {
        Round round = round(transaction, assignment, registrations);
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            List<PbNotifyLeaderAndIsrReqForBucket> notifications = new ArrayList<>();
            List<TableBucket> localOnlyDeletes = new ArrayList<>();
            Map<TableBucket, Integer> leaderEpochs = new HashMap<>();
            for (Map.Entry<TableBucket, LeaderAndIsr> entry : round.roles.entrySet()) {
                TableBucket bucket = entry.getKey();
                List<Integer> replicas =
                        assignment.getValue().getBucketAssignment(bucket.getBucket()).getReplicas();
                if (replicas.contains(registration.serverId)) {
                    notifications.add(
                            makeNotifyBucketLeaderAndIsr(
                                    new NotifyLeaderAndIsrData(
                                            transaction.getHandle().getTarget(),
                                            bucket,
                                            replicas,
                                            entry.getValue(),
                                            target(transaction, bucket, state))));
                } else {
                    localOnlyDeletes.add(bucket);
                    leaderEpochs.put(bucket, entry.getValue().leaderEpoch());
                }
            }
            if (!notifications.isEmpty()) {
                acknowledgements.add(sendNotify(registration.serverId, notifications));
            }
            if (!localOnlyDeletes.isEmpty()) {
                acknowledgements.add(
                        sendStop(registration.serverId, localOnlyDeletes, leaderEpochs));
            }
        }
        CompletableFuture<Void> completion = complete(acknowledgements);
        if (applyMetadataAfterRoles) {
            completion =
                    completion.thenCompose(
                            ignored ->
                                    sendMetadata(
                                            transaction,
                                            assignment,
                                            registrations,
                                            BulkLoadDataState.ACTIVE,
                                            round));
        }
        return new Attempt(round.confirmation(), completion);
    }

    Attempt publishActiveMetadata(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        return applyMetadata(transaction, assignment, registrations, BulkLoadDataState.ACTIVE);
    }

    private Attempt applyMetadata(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations,
            BulkLoadDataState state) {
        Round round = round(transaction, assignment, registrations);
        return new Attempt(
                round.confirmation(),
                sendMetadata(transaction, assignment, registrations, state, round));
    }

    private CompletableFuture<Void> sendMetadata(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations,
            BulkLoadDataState state,
            Round round) {
        UpdateMetadataRequest request = updateRequest(transaction, assignment, state, round.roles);
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
            try {
                requestBatch.sendUpdateMetadataRequest(
                        registration.serverId,
                        request,
                        (response, failure) -> {
                            if (failure == null && response != null) {
                                acknowledgement.complete(null);
                            } else {
                                acknowledgement.completeExceptionally(
                                        failure == null
                                                ? new FlussRuntimeException(
                                                        "Missing UpdateMetadata acknowledgement.")
                                                : failure);
                            }
                        });
            } catch (Throwable failure) {
                acknowledgement.completeExceptionally(failure);
            }
            acknowledgements.add(acknowledgement);
        }
        return complete(acknowledgements);
    }

    private CompletableFuture<Void> sendNotify(
            int serverId, List<PbNotifyLeaderAndIsrReqForBucket> notifications) {
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        NotifyLeaderAndIsrRequest request =
                makeNotifyLeaderAndIsrRequest(context.getCoordinatorEpoch(), notifications);
        try {
            requestBatch.sendNotifyLeaderAndIsrRequest(
                    serverId,
                    request,
                    (response, failure) -> {
                        if (failure != null) {
                            acknowledgement.completeExceptionally(failure);
                            return;
                        }
                        if (response == null
                                || response.getNotifyBucketsLeaderRespsCount()
                                        != notifications.size()) {
                            acknowledgement.completeExceptionally(
                                    new FlussRuntimeException(
                                            "Incomplete NotifyLeaderAndIsr acknowledgement."));
                            return;
                        }
                        Set<TableBucket> expected = new HashSet<>();
                        for (PbNotifyLeaderAndIsrReqForBucket item : notifications) {
                            expected.add(
                                    org.apache.fluss.server.utils.ServerRpcMessageUtils
                                            .toTableBucket(item.getTableBucket()));
                        }
                        for (PbNotifyLeaderAndIsrRespForBucket item :
                                response.getNotifyBucketsLeaderRespsList()) {
                            TableBucket bucket =
                                    org.apache.fluss.server.utils.ServerRpcMessageUtils
                                            .toTableBucket(item.getTableBucket());
                            if (item.hasErrorCode() || !expected.remove(bucket)) {
                                acknowledgement.completeExceptionally(
                                        new FlussRuntimeException(
                                                "NotifyLeaderAndIsr failed for "
                                                        + bucket
                                                        + (item.hasErrorMessage()
                                                                ? ": " + item.getErrorMessage()
                                                                : ".")));
                                return;
                            }
                        }
                        if (expected.isEmpty()) {
                            acknowledgement.complete(null);
                        } else {
                            acknowledgement.completeExceptionally(
                                    new FlussRuntimeException(
                                            "Incomplete NotifyLeaderAndIsr acknowledgement."));
                        }
                    });
        } catch (Throwable failure) {
            acknowledgement.completeExceptionally(failure);
        }
        return acknowledgement;
    }

    private CompletableFuture<Void> sendStop(
            int serverId, List<TableBucket> buckets, Map<TableBucket, Integer> leaderEpochs) {
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        try {
            requestBatch.sendStopReplicaRequest(
                    serverId,
                    context.getCoordinatorEpoch(),
                    buckets,
                    leaderEpochs,
                    (response, failure) ->
                            validateStop(response, failure, buckets, acknowledgement));
        } catch (Throwable failure) {
            acknowledgement.completeExceptionally(failure);
        }
        return acknowledgement;
    }

    private static void validateStop(
            StopReplicaResponse response,
            Throwable failure,
            List<TableBucket> buckets,
            CompletableFuture<Void> acknowledgement) {
        if (failure != null) {
            acknowledgement.completeExceptionally(failure);
            return;
        }
        if (response == null || response.getStopReplicasRespsCount() != buckets.size()) {
            acknowledgement.completeExceptionally(
                    new FlussRuntimeException("Incomplete StopReplica acknowledgement."));
            return;
        }
        Set<TableBucket> expected = new HashSet<>(buckets);
        for (PbStopReplicaRespForBucket item : response.getStopReplicasRespsList()) {
            TableBucket bucket =
                    org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket(
                            item.getTableBucket());
            if (item.hasErrorCode() || !expected.remove(bucket)) {
                acknowledgement.completeExceptionally(
                        new FlussRuntimeException("StopReplica failed for " + bucket + '.'));
                return;
            }
        }
        if (expected.isEmpty()) {
            acknowledgement.complete(null);
        } else {
            acknowledgement.completeExceptionally(
                    new FlussRuntimeException("Incomplete StopReplica acknowledgement."));
        }
    }

    private CompletableFuture<Void> complete(List<CompletableFuture<Void>> acknowledgements) {
        return CompletableFuture.allOf(acknowledgements.toArray(new CompletableFuture<?>[0]));
    }

    private Round round(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        Map<TableBucket, LeaderAndIsr> roles = new HashMap<>();
        for (Integer bucketId : assignment.getValue().getBuckets()) {
            TableBucket bucket =
                    new TableBucket(
                            transaction.getTableId(), transaction.getPartitionId(), bucketId);
            LeaderAndIsr role =
                    context.getBucketLeaderAndIsr(bucket)
                            .orElseThrow(
                                    () ->
                                            new TimeoutException(
                                                    "Missing current role for " + bucket + '.'));
            roles.put(bucket, role);
        }
        return new Round(
                transaction,
                assignment.getVersion(),
                roles,
                assignedHolders(assignment, registrations),
                registrations);
    }

    static List<BulkLoadMetadataStore.RegisteredServer> liveRecipients(
            CoordinatorContext context,
            List<BulkLoadMetadataStore.RegisteredServer> registrations,
            Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations) {
        List<BulkLoadMetadataStore.RegisteredServer> recipients = new ArrayList<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            ServerInfo live = context.getLiveTabletServers().get(registration.serverId);
            BulkLoadMetadataStore.RegisteredServer ready =
                    readyRegistrations.get(registration.serverId);
            if (live != null
                    && ready != null
                    && matchesLive(registration, live)
                    && sameRegistration(registration.registration, ready.registration)) {
                recipients.add(registration);
            }
        }
        return Collections.unmodifiableList(recipients);
    }

    static boolean matchesLive(
            BulkLoadMetadataStore.RegisteredServer registration, ServerInfo live) {
        TabletServerRegistration value = registration.registration.getValue();
        return Objects.equals(value.getRack(), live.rack())
                && value.getResource().equals(live.resource())
                && new HashSet<>(value.getEndpoints()).equals(new HashSet<>(live.endpoints()));
    }

    static List<BulkLoadMetadataStore.RegisteredServer> assignedHolders(
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            List<BulkLoadMetadataStore.RegisteredServer> registrations) {
        Set<Integer> holderIds = new HashSet<>();
        for (Integer bucketId : assignment.getValue().getBuckets()) {
            holderIds.addAll(assignment.getValue().getBucketAssignment(bucketId).getReplicas());
        }
        Map<Integer, BulkLoadMetadataStore.RegisteredServer> byId = new HashMap<>();
        for (BulkLoadMetadataStore.RegisteredServer registration : registrations) {
            byId.put(registration.serverId, registration);
        }
        List<Integer> sortedHolderIds = new ArrayList<>(holderIds);
        Collections.sort(sortedHolderIds);
        List<BulkLoadMetadataStore.RegisteredServer> holders = new ArrayList<>();
        for (Integer holderId : sortedHolderIds) {
            BulkLoadMetadataStore.RegisteredServer holder = byId.get(holderId);
            if (holder != null) {
                holders.add(holder);
            }
        }
        return Collections.unmodifiableList(holders);
    }

    private UpdateMetadataRequest updateRequest(
            BulkLoadTransaction transaction,
            BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
            BulkLoadDataState state,
            Map<TableBucket, LeaderAndIsr> roles) {
        TableInfo tableInfo = context.getTableInfoById(transaction.getTableId());
        if (tableInfo == null) {
            throw new IllegalStateException("Missing current TableInfo for BulkLoad target.");
        }
        List<BucketMetadata> buckets = new ArrayList<>();
        List<Integer> ids = new ArrayList<>(assignment.getValue().getBuckets());
        Collections.sort(ids);
        for (Integer bucketId : ids) {
            TableBucket bucket =
                    new TableBucket(
                            transaction.getTableId(), transaction.getPartitionId(), bucketId);
            LeaderAndIsr role = roles.get(bucket);
            buckets.add(
                    new BucketMetadata(
                            bucketId,
                            role.leader(),
                            role.leaderEpoch(),
                            assignment.getValue().getBucketAssignment(bucketId).getReplicas()));
        }

        List<TableMetadata> tables = new ArrayList<>();
        List<PartitionMetadata> partitions = new ArrayList<>();
        if (transaction.getPartitionId() == null) {
            tables.add(new TableMetadata(tableInfo, buckets));
        } else {
            tables.add(new TableMetadata(tableInfo, Collections.emptyList()));
            partitions.add(
                    new PartitionMetadata(
                            transaction.getTableId(),
                            transaction.getPartitionName(),
                            transaction.getPartitionId(),
                            buckets));
        }
        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        context.getCoordinatorServerInfo(),
                        context.getCoordinatorEpoch(),
                        new HashSet<>(context.getLiveTabletServers().values()),
                        tables,
                        partitions);
        if (transaction.getPartitionId() == null) {
            decorate(request.getTableMetadataAt(0), transaction, state);
        } else {
            decorate(request.getPartitionMetadataAt(0), transaction, state);
        }
        return request;
    }

    private static void decorate(
            PbTableMetadata metadata, BulkLoadTransaction transaction, BulkLoadDataState state) {
        metadata.setMetadataPath(transaction.getMetadataPath())
                .setMetadataVersion(transaction.getMetadataVersion())
                .setDataState(state.getCode());
        if (state == BulkLoadDataState.LOADING) {
            metadata.setBulkLoadId(transaction.getBulkLoadId());
        }
    }

    private static void decorate(
            PbPartitionMetadata metadata,
            BulkLoadTransaction transaction,
            BulkLoadDataState state) {
        metadata.setMetadataPath(transaction.getMetadataPath())
                .setMetadataVersion(transaction.getMetadataVersion())
                .setDataState(state.getCode());
        if (state == BulkLoadDataState.LOADING) {
            metadata.setBulkLoadId(transaction.getBulkLoadId());
        }
    }

    private static BulkLoadTargetMetadata target(
            BulkLoadTransaction transaction, TableBucket bucket, BulkLoadDataState state) {
        return new BulkLoadTargetMetadata(
                transaction.getMetadataPath(),
                bucket,
                transaction.getMetadataVersion(),
                state,
                state == BulkLoadDataState.LOADING ? transaction.getBulkLoadId() : null);
    }

    /** One convergence attempt and the exact facts captured before its asynchronous RPCs. */
    static final class Attempt {
        private final Confirmation confirmation;
        private final CompletableFuture<Void> completion;

        private Attempt(Confirmation confirmation, CompletableFuture<Void> completion) {
            this.confirmation = confirmation;
            this.completion = completion;
        }

        Confirmation confirmation() {
            return confirmation;
        }

        CompletableFuture<Void> completion() {
            return completion;
        }
    }

    /** Process-local snapshot of one exact metadata and assignment round. */
    static final class Confirmation {
        private final String metadataPath;
        private final int metadataVersion;
        private final int assignmentVersion;
        private final Map<TableBucket, LeaderAndIsr> roles;
        private final List<BulkLoadMetadataStore.RegisteredServer> holders;
        private final List<BulkLoadMetadataStore.RegisteredServer> recipients;

        private Confirmation(
                String metadataPath,
                int metadataVersion,
                int assignmentVersion,
                Map<TableBucket, LeaderAndIsr> roles,
                List<BulkLoadMetadataStore.RegisteredServer> holders,
                List<BulkLoadMetadataStore.RegisteredServer> recipients) {
            this.metadataPath = metadataPath;
            this.metadataVersion = metadataVersion;
            this.assignmentVersion = assignmentVersion;
            this.roles = Collections.unmodifiableMap(new HashMap<>(roles));
            this.holders = Collections.unmodifiableList(new ArrayList<>(holders));
            this.recipients = Collections.unmodifiableList(new ArrayList<>(recipients));
        }

        boolean matches(
                BulkLoadTransaction transaction,
                BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
                CoordinatorContext context,
                List<BulkLoadMetadataStore.RegisteredServer> registrations,
                Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations) {
            if (!metadataPath.equals(transaction.getMetadataPath())
                    || metadataVersion != transaction.getMetadataVersion()) {
                return false;
            }
            return matchesRoundFacts(assignment, context, registrations, readyRegistrations);
        }

        boolean matchesRoundFacts(
                BulkLoadMetadataStore.Versioned<? extends TableAssignment> assignment,
                CoordinatorContext context,
                List<BulkLoadMetadataStore.RegisteredServer> registrations,
                Map<Integer, BulkLoadMetadataStore.RegisteredServer> readyRegistrations) {
            if (assignmentVersion != assignment.getVersion()) {
                return false;
            }
            for (Map.Entry<TableBucket, LeaderAndIsr> role : roles.entrySet()) {
                if (!context.getBucketLeaderAndIsr(role.getKey()).isPresent()
                        || !context.getBucketLeaderAndIsr(role.getKey())
                                .get()
                                .equals(role.getValue())) {
                    return false;
                }
            }
            List<BulkLoadMetadataStore.RegisteredServer> currentRecipients =
                    liveRecipients(context, registrations, readyRegistrations);
            List<BulkLoadMetadataStore.RegisteredServer> currentHolders =
                    assignedHolders(assignment, registrations);
            if (currentHolders.size() != holders.size()) {
                return false;
            }
            for (int i = 0; i < holders.size(); i++) {
                BulkLoadMetadataStore.RegisteredServer expected = holders.get(i);
                BulkLoadMetadataStore.RegisteredServer current = currentHolders.get(i);
                if (expected.serverId != current.serverId
                        || !sameRegistration(expected.registration, current.registration)) {
                    return false;
                }
            }
            Map<Integer, BulkLoadMetadataStore.RegisteredServer> currentById = new HashMap<>();
            for (BulkLoadMetadataStore.RegisteredServer recipient : currentRecipients) {
                currentById.put(recipient.serverId, recipient);
            }
            for (BulkLoadMetadataStore.RegisteredServer expected : recipients) {
                BulkLoadMetadataStore.RegisteredServer current = currentById.get(expected.serverId);
                if (current == null
                        || !sameRegistration(expected.registration, current.registration)) {
                    return false;
                }
            }
            return true;
        }

        List<BulkLoadMetadataStore.RegisteredServer> holders() {
            return holders;
        }
    }

    private static boolean sameRegistration(
            BulkLoadMetadataStore.Versioned<TabletServerRegistration> expected,
            BulkLoadMetadataStore.Versioned<TabletServerRegistration> current) {
        return expected.getPath().equals(current.getPath())
                && expected.getVersion() == current.getVersion()
                && expected.getEphemeralOwner() == current.getEphemeralOwner()
                && expected.getValue().equals(current.getValue());
    }

    private static final class Round {
        private final BulkLoadTransaction transaction;
        private final int assignmentVersion;
        private final Map<TableBucket, LeaderAndIsr> roles;
        private final List<BulkLoadMetadataStore.RegisteredServer> holders;
        private final List<BulkLoadMetadataStore.RegisteredServer> recipients;

        private Round(
                BulkLoadTransaction transaction,
                int assignmentVersion,
                Map<TableBucket, LeaderAndIsr> roles,
                List<BulkLoadMetadataStore.RegisteredServer> holders,
                List<BulkLoadMetadataStore.RegisteredServer> recipients) {
            this.transaction = transaction;
            this.assignmentVersion = assignmentVersion;
            this.roles = roles;
            this.holders = holders;
            this.recipients = recipients;
        }

        private Confirmation confirmation() {
            return new Confirmation(
                    transaction.getMetadataPath(),
                    transaction.getMetadataVersion(),
                    assignmentVersion,
                    roles,
                    holders,
                    recipients);
        }
    }
}
