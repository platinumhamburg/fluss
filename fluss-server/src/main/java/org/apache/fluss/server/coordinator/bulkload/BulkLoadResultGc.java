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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.server.coordinator.event.BulkLoadAsyncResultEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.CheckedOperation;
import org.apache.fluss.server.zk.ZooKeeperClient.ChildrenWithStat;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Bounded garbage collection for expired terminal BulkLoad result metadata. */
@Internal
public final class BulkLoadResultGc {

    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadResultGc.class);
    private static final long PRODUCTION_REFRESH_INTERVAL_MS = 1_000L;
    private final ZooKeeperClient zkClient;
    private final Executor ioExecutor;
    private final EventManager eventManager;
    private final long refreshIntervalMs;
    private final long maxZookeeperTransactionBytes;
    private final Object ownerToken = new Object();

    private List<Candidate> candidates = Collections.emptyList();
    private int candidateIndex;
    private boolean snapshotLoaded;
    private long nextGeneration;
    private long inFlightGeneration = -1L;
    private long refreshNotBeforeMs = Long.MIN_VALUE;

    /** Creates a production result GC worker with the server's ZooKeeper request budget. */
    public BulkLoadResultGc(
            ZooKeeperClient zkClient,
            Executor ioExecutor,
            EventManager eventManager,
            long maxZookeeperTransactionBytes) {
        this(
                zkClient,
                ioExecutor,
                eventManager,
                PRODUCTION_REFRESH_INTERVAL_MS,
                maxZookeeperTransactionBytes);
    }

    /** Creates a worker with directly executed enumeration for the stable safety-contract test. */
    @VisibleForTesting
    BulkLoadResultGc(ZooKeeperClient zkClient, EventManager eventManager) {
        this(
                zkClient,
                Runnable::run,
                eventManager,
                0L,
                zookeeperTransactionBytes(new Configuration()));
    }

    private BulkLoadResultGc(
            ZooKeeperClient zkClient,
            Executor ioExecutor,
            EventManager eventManager,
            long refreshIntervalMs,
            long maxZookeeperTransactionBytes) {
        this.zkClient = checkNotNull(zkClient);
        if (maxZookeeperTransactionBytes <= 0) {
            throw new IllegalArgumentException(
                    "ZooKeeper transaction byte limit must be positive.");
        }
        this.ioExecutor = checkNotNull(ioExecutor);
        this.eventManager = checkNotNull(eventManager);
        this.refreshIntervalMs = refreshIntervalMs;
        this.maxZookeeperTransactionBytes = maxZookeeperTransactionBytes;
    }

    /**
     * Processes at most one expired result or empty physical parent. Enumeration runs on the I/O
     * executor; uncertain reads and stale checked writes are retried by later maintenance events.
     */
    public int runMaintenance(long nowMs, int coordinatorEpochVersion) {
        try {
            Candidate candidate = nextCandidate(nowMs);
            if (candidate == null) {
                return 0;
            }
            if (candidate.emptyParent) {
                return deleteEmptyParent(candidate, coordinatorEpochVersion);
            }
            Eligibility eligibility = eligible(candidate, nowMs);
            if (eligibility == null || !deleteOuterManifest(eligibility.transaction)) {
                return 0;
            }
            return deleteTransactionMetadata(candidate, eligibility, coordinatorEpochVersion);
        } catch (Exception staleOrUnknown) {
            LOG.debug("BulkLoad result GC candidate is not currently deletable.", staleOrUnknown);
            return 0;
        }
    }

    /** Adopts an enumeration result on the Coordinator event thread. */
    public boolean processAsyncResult(Object result) {
        if (!(result instanceof EnumerationResult)) {
            return false;
        }
        EnumerationResult enumeration = (EnumerationResult) result;
        if (enumeration.ownerToken != ownerToken) {
            return false;
        }
        if (enumeration.generation != inFlightGeneration) {
            return true;
        }
        inFlightGeneration = -1L;
        refreshNotBeforeMs = safeAdd(enumeration.requestedAtMs, refreshIntervalMs);
        if (enumeration.failure != null) {
            candidates = Collections.emptyList();
            candidateIndex = 0;
            snapshotLoaded = false;
            LOG.warn("Failed to enumerate BulkLoad result GC candidates.", enumeration.failure);
            return true;
        }
        candidates = enumeration.candidates;
        candidateIndex = 0;
        snapshotLoaded = true;
        return true;
    }

    @Nullable
    private Candidate nextCandidate(long nowMs) {
        if (candidateIndex < candidates.size()) {
            return candidates.get(candidateIndex++);
        }
        if (snapshotLoaded) {
            candidates = Collections.emptyList();
            candidateIndex = 0;
            snapshotLoaded = false;
        }
        scheduleEnumeration(nowMs);
        return null;
    }

    private void scheduleEnumeration(long nowMs) {
        if (inFlightGeneration >= 0L || nowMs < refreshNotBeforeMs) {
            return;
        }
        long generation = nextGeneration++;
        inFlightGeneration = generation;
        try {
            ioExecutor.execute(
                    () -> {
                        EnumerationResult result;
                        try {
                            result =
                                    EnumerationResult.success(
                                            ownerToken, generation, nowMs, enumerateCandidates());
                        } catch (Exception failure) {
                            result =
                                    EnumerationResult.failure(
                                            ownerToken, generation, nowMs, failure);
                        }
                        eventManager.put(new BulkLoadAsyncResultEvent(result));
                    });
        } catch (RuntimeException rejected) {
            inFlightGeneration = -1L;
            refreshNotBeforeMs = safeAdd(nowMs, refreshIntervalMs);
            LOG.warn("Failed to schedule BulkLoad result GC enumeration.", rejected);
        }
    }

    private List<Candidate> enumerateCandidates() throws Exception {
        List<Candidate> result = new ArrayList<>();
        enumerateScope(false, ZkData.BulkLoadTableTransactionsZNode.path(), result);
        enumerateScope(true, ZkData.BulkLoadPartitionTransactionsZNode.path(), result);
        Collections.sort(result, Comparator.comparing(Candidate::sortKey));
        return Collections.unmodifiableList(result);
    }

    private void enumerateScope(boolean partition, String scopePath, List<Candidate> result)
            throws Exception {
        Optional<ChildrenWithStat> physicalChildren =
                zkClient.getChildrenWithStatIfExists(scopePath);
        if (!physicalChildren.isPresent()) {
            return;
        }
        for (String physicalIdText : physicalChildren.get().getChildren()) {
            long physicalId;
            try {
                physicalId = Long.parseLong(physicalIdText);
            } catch (NumberFormatException malformedId) {
                LOG.warn("Ignoring malformed BulkLoad transaction parent {}.", physicalIdText);
                continue;
            }
            String physicalParentPath = scopePath + "/" + physicalIdText;
            Optional<ChildrenWithStat> transactions =
                    zkClient.getChildrenWithStatIfExists(physicalParentPath);
            if (!transactions.isPresent()) {
                continue;
            }
            if (transactions.get().getChildren().isEmpty()) {
                result.add(Candidate.emptyParent(partition, physicalId, physicalParentPath));
                continue;
            }
            for (String bulkLoadId : transactions.get().getChildren()) {
                result.add(
                        Candidate.transaction(
                                partition, physicalId, bulkLoadId, physicalParentPath));
            }
        }
    }

    @Nullable
    private Eligibility eligible(Candidate candidate, long nowMs) throws Exception {
        Optional<DataWithStat> rootData =
                zkClient.getDataWithStatIfExists(checkNotNull(candidate.transactionPath));
        if (!rootData.isPresent()) {
            return null;
        }
        BulkLoadTransaction transaction = candidate.decodeTransaction(rootData.get().getData());
        Long expiry = transaction.getResultExpireTimeMs();
        if (!isTerminal(transaction.getState()) || expiry == null || nowMs < expiry) {
            return null;
        }
        String registrationPath = transaction.getMetadataPath();
        Optional<DataWithStat> registrationData =
                zkClient.getDataWithStatIfExists(registrationPath);
        if (registrationData.isPresent()
                && candidate.ownsRegistration(registrationData.get().getData())) {
            return null;
        }
        return new Eligibility(
                transaction,
                rootData.get().getStat().getVersion(),
                registrationPath,
                registrationData.isPresent()
                        ? registrationData.get().getStat().getVersion()
                        : null);
    }

    private boolean deleteOuterManifest(BulkLoadTransaction transaction) throws IOException {
        if (transaction.getManifestPath() == null) {
            return true;
        }
        FsPath manifestPath = new FsPath(transaction.getManifestPath());
        FileSystem fileSystem = manifestPath.getFileSystem();
        IOException deleteFailure = null;
        boolean deleted = false;
        try {
            deleted = fileSystem.delete(manifestPath, false);
        } catch (IOException failure) {
            deleteFailure = failure;
        }
        if (deleted || !fileSystem.exists(manifestPath)) {
            return true;
        }
        if (deleteFailure != null) {
            throw deleteFailure;
        }
        return false;
    }

    private int deleteTransactionMetadata(
            Candidate candidate, Eligibility eligibility, int coordinatorEpochVersion)
            throws Exception {
        List<CheckedOperation> operations =
                baseChecks(candidate, eligibility, coordinatorEpochVersion);
        long bytes = baseChecksSize(candidate, eligibility);
        String transactionPath = checkNotNull(candidate.transactionPath);
        if (!fits(bytes, ZooKeeperClient.estimateDeleteSerializedSize(transactionPath))) {
            return 0;
        }
        operations.add(CheckedOperation.delete(transactionPath, eligibility.rootVersion));
        zkClient.submitCheckedMulti(operations, maxZookeeperTransactionBytes);
        return 1;
    }

    private int deleteEmptyParent(Candidate candidate, int coordinatorEpochVersion)
            throws Exception {
        Optional<ChildrenWithStat> current =
                zkClient.getChildrenWithStatIfExists(candidate.physicalParentPath);
        if (!current.isPresent() || !current.get().getChildren().isEmpty()) {
            return 0;
        }
        List<CheckedOperation> operations = new ArrayList<>();
        operations.add(
                CheckedOperation.check(
                        ZkData.CoordinatorEpochZNode.path(), coordinatorEpochVersion));
        operations.add(
                CheckedOperation.delete(
                        candidate.physicalParentPath, current.get().getStat().getVersion()));
        zkClient.submitCheckedMulti(operations, maxZookeeperTransactionBytes);
        return 1;
    }

    private static List<CheckedOperation> baseChecks(
            Candidate candidate, Eligibility eligibility, int coordinatorEpochVersion) {
        List<CheckedOperation> operations = new ArrayList<>();
        operations.add(
                CheckedOperation.check(
                        ZkData.CoordinatorEpochZNode.path(), coordinatorEpochVersion));
        operations.add(
                CheckedOperation.check(
                        checkNotNull(candidate.transactionPath), eligibility.rootVersion));
        operations.add(
                eligibility.registrationVersion == null
                        ? CheckedOperation.assertAbsent(eligibility.registrationPath)
                        : CheckedOperation.check(
                                eligibility.registrationPath, eligibility.registrationVersion));
        return operations;
    }

    private static long baseChecksSize(Candidate candidate, Eligibility eligibility) {
        long bytes = 17L;
        bytes =
                ZooKeeperClient.checkedAdd(
                        bytes,
                        ZooKeeperClient.estimateCheckSerializedSize(
                                ZkData.CoordinatorEpochZNode.path()));
        bytes =
                ZooKeeperClient.checkedAdd(
                        bytes,
                        ZooKeeperClient.estimateCheckSerializedSize(
                                checkNotNull(candidate.transactionPath)));
        if (eligibility.registrationVersion == null) {
            bytes =
                    ZooKeeperClient.checkedAdd(
                            bytes,
                            ZooKeeperClient.estimateCreateSerializedSize(
                                    eligibility.registrationPath, new byte[0]));
            return ZooKeeperClient.checkedAdd(
                    bytes,
                    ZooKeeperClient.estimateDeleteSerializedSize(eligibility.registrationPath));
        }
        return ZooKeeperClient.checkedAdd(
                bytes, ZooKeeperClient.estimateCheckSerializedSize(eligibility.registrationPath));
    }

    private boolean fits(long currentBytes, long operationBytes) {
        try {
            return ZooKeeperClient.checkedAdd(currentBytes, operationBytes)
                    <= maxZookeeperTransactionBytes;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private static long zookeeperTransactionBytes(Configuration configuration) {
        return (long) configuration.get(ConfigOptions.ZOOKEEPER_MAX_BUFFER_SIZE) * 4L / 5L;
    }

    private static long safeAdd(long left, long right) {
        if (right > 0L && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static boolean isTerminal(BulkLoadState state) {
        return state == BulkLoadState.COMMITTED || state == BulkLoadState.ABORTED;
    }

    private static final class Eligibility {
        private final BulkLoadTransaction transaction;
        private final int rootVersion;
        private final String registrationPath;
        private final @Nullable Integer registrationVersion;

        private Eligibility(
                BulkLoadTransaction transaction,
                int rootVersion,
                String registrationPath,
                @Nullable Integer registrationVersion) {
            this.transaction = transaction;
            this.rootVersion = rootVersion;
            this.registrationPath = registrationPath;
            this.registrationVersion = registrationVersion;
        }
    }

    private static final class EnumerationResult {
        private final Object ownerToken;
        private final long generation;
        private final long requestedAtMs;
        private final List<Candidate> candidates;
        private final @Nullable Exception failure;

        private EnumerationResult(
                Object ownerToken,
                long generation,
                long requestedAtMs,
                List<Candidate> candidates,
                @Nullable Exception failure) {
            this.ownerToken = ownerToken;
            this.generation = generation;
            this.requestedAtMs = requestedAtMs;
            this.candidates = candidates;
            this.failure = failure;
        }

        private static EnumerationResult success(
                Object ownerToken,
                long generation,
                long requestedAtMs,
                List<Candidate> candidates) {
            return new EnumerationResult(ownerToken, generation, requestedAtMs, candidates, null);
        }

        private static EnumerationResult failure(
                Object ownerToken, long generation, long requestedAtMs, Exception failure) {
            return new EnumerationResult(
                    ownerToken, generation, requestedAtMs, Collections.emptyList(), failure);
        }
    }

    private static final class Candidate {
        private final boolean partition;
        private final long physicalId;
        private final @Nullable String bulkLoadId;
        private final String physicalParentPath;
        private final @Nullable String transactionPath;
        private final boolean emptyParent;

        private Candidate(
                boolean partition,
                long physicalId,
                @Nullable String bulkLoadId,
                String physicalParentPath,
                @Nullable String transactionPath,
                boolean emptyParent) {
            this.partition = partition;
            this.physicalId = physicalId;
            this.bulkLoadId = bulkLoadId;
            this.physicalParentPath = physicalParentPath;
            this.transactionPath = transactionPath;
            this.emptyParent = emptyParent;
        }

        private static Candidate transaction(
                boolean partition, long physicalId, String bulkLoadId, String physicalParentPath) {
            return new Candidate(
                    partition,
                    physicalId,
                    bulkLoadId,
                    physicalParentPath,
                    physicalParentPath + "/" + bulkLoadId,
                    false);
        }

        private static Candidate emptyParent(
                boolean partition, long physicalId, String physicalParentPath) {
            return new Candidate(partition, physicalId, null, physicalParentPath, null, true);
        }

        private String sortKey() {
            return (partition ? "1" : "0")
                    + ":"
                    + String.format("%020d", physicalId)
                    + ":"
                    + (emptyParent ? "1" : "0")
                    + ":"
                    + (bulkLoadId == null ? "" : bulkLoadId);
        }

        private boolean ownsRegistration(byte[] data) {
            if (partition) {
                PartitionRegistration registration = ZkData.PartitionZNode.decode(data);
                return registration.getPartitionId() == physicalId
                        && bulkLoadId.equals(registration.getBulkLoadId());
            }
            TableRegistration registration = ZkData.TableZNode.decode(data);
            return registration.tableId == physicalId && bulkLoadId.equals(registration.bulkLoadId);
        }

        private BulkLoadTransaction decodeTransaction(byte[] data) {
            return partition
                    ? ZkData.BulkLoadPartitionTransactionZNode.decode(data)
                    : ZkData.BulkLoadTableTransactionZNode.decode(data);
        }
    }
}
