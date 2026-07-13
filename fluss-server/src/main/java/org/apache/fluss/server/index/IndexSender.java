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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.apache.fluss.rpc.protocol.MessageCodec.REQUEST_HEADER_LENGTH;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * TabletServer-global send layer for index replication. A fixed pool of worker threads drains
 * pre-encoded {@link IndexBatch}es from the shared {@link IndexAccumulator}, groups them by target
 * Index Table leader server, and dispatches consolidated multi-bucket {@code PutKvRequest}s.
 *
 * <p>Each target index bucket is owned by exactly one worker ({@code bucket.hashCode() % M}),
 * giving per-bucket serialization for free. In addition, a bucket with an in-flight request is
 * "muted" so the worker does not send a second batch for it concurrently; per-bucket in-order
 * delivery is thus guaranteed.
 *
 * <p>Reliability follows at-least-once semantics: on RPC failure (or unresolved leader) the batch
 * is re-enqueued to the front of its bucket queue via {@link
 * IndexAccumulator#reEnqueueIfActive(IndexBatch, long)} for unlimited retry, and the owning
 * window's offset is never advanced. On success the batch notifies its window via {@link
 * IndexBatch#window()}, which advances the replicator's pushed offset once the whole window is
 * acknowledged.
 */
@Internal
@ThreadSafe
public final class IndexSender implements AutoCloseable {

    @VisibleForTesting
    enum LifecyclePhase {
        OPEN,
        CLOSING,
        CLOSED
    }

    /** Resolves the leader server ID for a given Index Table bucket. */
    @FunctionalInterface
    public interface LeaderResolver {
        OptionalInt resolveLeader(long indexTableId, int indexBucket);
    }

    @VisibleForTesting
    interface LifecycleHooks {
        LifecycleHooks NO_OP = new LifecycleHooks() {};

        default void beforePutKvInvocation() {}

        default void beforeFinalPutKvRegistration() {}

        default void afterFinalPutKvRegistrationDecision() {}

        default void beforePutKvCompletion() {}

        default void beforeProgressCallback() {}

        default void beforeBatchRequeue() {}

        default void beforeRetryPublication() {}
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexSender.class);

    private static final int DEFAULT_ACKS = -1;
    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;
    private static final long DEFAULT_RETRY_MAX_BACKOFF_MS = 10_000L;
    private static final long DEFAULT_MAX_REQUEST_BYTES = 1024L * 1024L;
    private static final long MAX_CODEC_FRAMED_BYTES = Integer.MAX_VALUE;
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;

    private final IndexAccumulator accumulator;
    private final LeaderResolver leaderResolver;
    private final Function<Integer, TabletServerGateway> gatewayFactory;
    private final TabletServerMetricGroup metrics;
    private final SenderWorker[] workers;

    /** Base backoff before retrying a failed batch; grows exponentially with attempts. */
    private final long retryBackoffMs;

    /** Upper bound on the exponential retry backoff. */
    private final long retryMaxBackoffMs;

    /** Maximum total encoded bytes packed into a single consolidated PutKv request. */
    private final long maxRequestBytes;

    /** Exact framed request limit enforced by the target Netty server. */
    private final long maxTransportRequestBytes;

    /** Timeout applied to each index {@code PutKv} request. */
    private final long requestTimeoutMs;

    /** Buckets with an in-flight request; value is the request send timestamp in millis. */
    private final ConcurrentMap<TableBucket, Long> inFlightSinceMs = MapUtils.newConcurrentMap();

    private final ConcurrentMap<TableBucket, IndexBatch> inFlightBatches =
            MapUtils.newConcurrentMap();
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Condition lifecycleDrained = lifecycleLock.newCondition();
    private final Map<Integer, TargetContext> targetsByServer = new HashMap<>();
    private final Set<Integer> creatingTargets = new HashSet<>();
    private final Set<IndexBatch> ownedBatches = Collections.newSetFromMap(new IdentityHashMap<>());
    private final ThreadLocal<Integer> activeExternalCallbackDepth =
            ThreadLocal.withInitial(() -> 0);
    private final LifecycleHooks lifecycleHooks;
    private volatile LifecyclePhase lifecyclePhase = LifecyclePhase.OPEN;
    @Nullable private Thread lifecycleFinisher;
    private int activeAccountingOperations;
    private int outstandingAsyncOperations;
    private long nextTargetGeneration;

    public IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs) {
        this(
                accumulator,
                leaderResolver,
                gatewayFactory,
                metrics,
                numWorkers,
                backoffMs,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_MAX_BACKOFF_MS,
                DEFAULT_MAX_REQUEST_BYTES,
                DEFAULT_TIMEOUT_MS);
    }

    public IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes) {
        this(
                accumulator,
                leaderResolver,
                gatewayFactory,
                metrics,
                numWorkers,
                backoffMs,
                retryBackoffMs,
                retryMaxBackoffMs,
                maxRequestBytes,
                Long.MAX_VALUE,
                DEFAULT_TIMEOUT_MS,
                LifecycleHooks.NO_OP);
    }

    public IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes,
            long maxTransportRequestBytes,
            long requestTimeoutMs) {
        this(
                accumulator,
                leaderResolver,
                gatewayFactory,
                metrics,
                numWorkers,
                backoffMs,
                retryBackoffMs,
                retryMaxBackoffMs,
                maxRequestBytes,
                maxTransportRequestBytes,
                requestTimeoutMs,
                LifecycleHooks.NO_OP);
    }

    @VisibleForTesting
    IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes,
            long requestTimeoutMs) {
        this(
                accumulator,
                leaderResolver,
                gatewayFactory,
                metrics,
                numWorkers,
                backoffMs,
                retryBackoffMs,
                retryMaxBackoffMs,
                maxRequestBytes,
                Long.MAX_VALUE,
                requestTimeoutMs,
                LifecycleHooks.NO_OP);
    }

    @VisibleForTesting
    IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes,
            long requestTimeoutMs,
            LifecycleHooks lifecycleHooks) {
        this(
                accumulator,
                leaderResolver,
                gatewayFactory,
                metrics,
                numWorkers,
                backoffMs,
                retryBackoffMs,
                retryMaxBackoffMs,
                maxRequestBytes,
                Long.MAX_VALUE,
                requestTimeoutMs,
                lifecycleHooks);
    }

    @VisibleForTesting
    IndexSender(
            IndexAccumulator accumulator,
            LeaderResolver leaderResolver,
            Function<Integer, TabletServerGateway> gatewayFactory,
            TabletServerMetricGroup metrics,
            int numWorkers,
            long backoffMs,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            long maxRequestBytes,
            long maxTransportRequestBytes,
            long requestTimeoutMs,
            LifecycleHooks lifecycleHooks) {
        this.accumulator = accumulator;
        this.leaderResolver = leaderResolver;
        this.gatewayFactory = gatewayFactory;
        this.metrics = metrics;
        this.retryBackoffMs = retryBackoffMs;
        this.retryMaxBackoffMs = retryMaxBackoffMs;
        this.maxRequestBytes = maxRequestBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.requestTimeoutMs = requestTimeoutMs;
        this.lifecycleHooks = lifecycleHooks;
        int workerCount = Math.max(1, numWorkers);
        this.workers = new SenderWorker[workerCount];
        for (int i = 0; i < workerCount; i++) {
            this.workers[i] = new SenderWorker("index-sender-" + i, i, backoffMs);
        }
        accumulator.setAppendListener(this::enqueueReadyBucket);
        accumulator.setDropListener(this::relinquishDroppedBatch);
        for (TableBucket bucket : accumulator.buckets()) {
            enqueueReadyBucket(bucket);
        }
        for (SenderWorker worker : workers) {
            worker.start();
        }
        LOG.info("IndexSender started with {} workers", workerCount);
    }

    /**
     * Exponential backoff for the given attempt count (1 = first retry), capped at {@link
     * #retryMaxBackoffMs}.
     */
    private long retryDelayMs(int attempts) {
        if (attempts <= 1) {
            return retryBackoffMs;
        }
        long delay = retryBackoffMs;
        for (int i = 1; i < attempts && delay < retryMaxBackoffMs; i++) {
            delay <<= 1;
        }
        return Math.min(delay, retryMaxBackoffMs);
    }

    private void enqueueReadyBucket(TableBucket bucket) {
        if (lifecyclePhase != LifecyclePhase.OPEN) {
            return;
        }
        workers[ownerOf(bucket)].enqueueReadyBucket(bucket);
    }

    private int ownerOf(TableBucket bucket) {
        return Math.floorMod(bucket.hashCode(), workers.length);
    }

    @Override
    public void close() {
        LOG.info("IndexSender closing");
        boolean cannotWaitForSelf =
                activeExternalCallbackDepth.get() > 0 || isSenderWorker(Thread.currentThread());
        boolean finish = false;
        lifecycleLock.lock();
        try {
            if (lifecyclePhase == LifecyclePhase.CLOSED) {
                return;
            }
            if (lifecyclePhase == LifecyclePhase.OPEN) {
                lifecyclePhase = LifecyclePhase.CLOSING;
                nextTargetGeneration++;
                targetsByServer.clear();
                creatingTargets.clear();
            }
            if (!cannotWaitForSelf && lifecycleFinisher == null) {
                lifecycleFinisher = Thread.currentThread();
                finish = true;
            }
        } finally {
            lifecycleLock.unlock();
        }
        requestWorkerShutdown();
        if (cannotWaitForSelf) {
            return;
        }
        if (finish) {
            finishClose();
        }
        awaitClosed();
    }

    private void requestWorkerShutdown() {
        for (SenderWorker worker : workers) {
            worker.initiateShutdown();
            worker.wakeup();
        }
    }

    private void finishClose() {
        boolean interrupted = false;
        for (SenderWorker worker : workers) {
            boolean shutdownComplete = false;
            while (!shutdownComplete) {
                try {
                    worker.awaitShutdown();
                    shutdownComplete = true;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }

        List<IndexBatch> batchesToRelease;
        lifecycleLock.lock();
        try {
            while (outstandingAsyncOperations > 0 || activeAccountingOperations > 0) {
                try {
                    lifecycleDrained.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            batchesToRelease = new ArrayList<>(ownedBatches);
            ownedBatches.clear();
            for (IndexBatch batch : batchesToRelease) {
                TableBucket bucket = batch.targetBucket();
                inFlightBatches.remove(bucket, batch);
                inFlightSinceMs.remove(bucket);
            }
        } finally {
            lifecycleLock.unlock();
        }
        for (IndexBatch batch : batchesToRelease) {
            accumulator.remove(batch);
            accumulator.release(batch);
        }

        lifecycleLock.lock();
        try {
            lifecyclePhase = LifecyclePhase.CLOSED;
            lifecycleFinisher = null;
            lifecycleDrained.signalAll();
        } finally {
            lifecycleLock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void awaitClosed() {
        boolean interrupted = false;
        lifecycleLock.lock();
        try {
            while (lifecyclePhase != LifecyclePhase.CLOSED) {
                try {
                    lifecycleDrained.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lifecycleLock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean isSenderWorker(Thread thread) {
        for (SenderWorker worker : workers) {
            if (worker == thread) {
                return true;
            }
        }
        return false;
    }

    private void tryFinishClose(boolean workerExited) {
        if (!workerExited && isSenderWorker(Thread.currentThread())) {
            return;
        }
        boolean finish = false;
        lifecycleLock.lock();
        try {
            if (lifecyclePhase == LifecyclePhase.CLOSING
                    && lifecycleFinisher == null
                    && outstandingAsyncOperations == 0
                    && activeAccountingOperations == 0) {
                lifecycleFinisher = Thread.currentThread();
                finish = true;
            }
        } finally {
            lifecycleLock.unlock();
        }
        if (finish) {
            requestWorkerShutdown();
            finishClose();
        }
    }

    /** A single sender worker that owns the buckets hashing to its index. */
    private final class SenderWorker extends ShutdownableThread {

        private final int workerId;
        private final long backoffMs;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private final Deque<TableBucket> readyBuckets = new ArrayDeque<>();
        private final Set<TableBucket> queuedBuckets = new HashSet<>();

        SenderWorker(String name, int workerId, long backoffMs) {
            super(name, false);
            this.workerId = workerId;
            this.backoffMs = backoffMs;
        }

        void wakeup() {
            inLock(lock, condition::signalAll);
        }

        void enqueueReadyBucket(TableBucket bucket) {
            inLock(
                    lock,
                    () -> {
                        if (!queuedBuckets.add(bucket)) {
                            return;
                        }
                        readyBuckets.addLast(bucket);
                        condition.signalAll();
                    });
        }

        @Nullable
        private TableBucket pollReadyBucket() {
            return inLock(
                    lock,
                    () -> {
                        TableBucket bucket = readyBuckets.pollFirst();
                        if (bucket != null) {
                            queuedBuckets.remove(bucket);
                        }
                        return bucket;
                    });
        }

        int queuedBucketCountForTesting() {
            return inLock(lock, readyBuckets::size);
        }

        @Override
        public void run() {
            super.run();
            tryFinishClose(true);
        }

        @Override
        public void doWork() {
            boolean didWork = drainAndSend();
            if (!didWork) {
                dispatchMissedAppendNotifications();
                didWork = drainAndSend();
            }
            if (!didWork) {
                inLock(
                        lock,
                        () -> {
                            try {
                                condition.await(backoffMs, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
        }

        /** Route only append notifications whose normal callback failed. */
        private void dispatchMissedAppendNotifications() {
            if (lifecyclePhase != LifecyclePhase.OPEN) {
                return;
            }
            TableBucket bucket;
            while ((bucket = accumulator.pollMissedAppendNotification()) != null) {
                IndexSender.this.enqueueReadyBucket(bucket);
            }
        }

        /** Claims at most one batch per owned, non-muted bucket and dispatches them. */
        private boolean drainAndSend() {
            if (lifecyclePhase != LifecyclePhase.OPEN) {
                return false;
            }
            long now = System.currentTimeMillis();
            List<IndexBatch> claimed = new ArrayList<>();
            List<TableBucket> deferredBuckets = new ArrayList<>();
            TableBucket bucket;
            while ((bucket = pollReadyBucket()) != null) {
                lifecycleLock.lock();
                try {
                    if (lifecyclePhase != LifecyclePhase.OPEN) {
                        break;
                    }
                    if (ownerOf(bucket) != workerId || inFlightSinceMs.containsKey(bucket)) {
                        continue;
                    }
                    IndexBatch batch = accumulator.pollFirstReady(bucket, now);
                    if (batch == null) {
                        if (accumulator.hasPending(bucket)) {
                            deferredBuckets.add(bucket);
                        }
                        continue;
                    }
                    inFlightSinceMs.put(bucket, now);
                    inFlightBatches.put(bucket, batch);
                    ownedBatches.add(batch);
                    claimed.add(batch);
                } finally {
                    lifecycleLock.unlock();
                }
            }
            for (TableBucket deferredBucket : deferredBuckets) {
                enqueueReadyBucket(deferredBucket);
            }
            if (claimed.isEmpty()) {
                return false;
            }

            List<IndexBatch> activeClaimed = new ArrayList<>(claimed.size());
            for (IndexBatch batch : claimed) {
                if (!batch.ownerActive()) {
                    releaseOwnedBatch(batch);
                    continue;
                }
                RequestSizeAccumulator singleton =
                        newRequestSizeAccumulator(
                                batch.targetBucket().getTableId(), requestTimeoutMs);
                singleton.addBucket(
                        batch.targetBucket().getBucket(), batch.encoded().getBytesLength());
                if (isOversized(singleton)) {
                    failOversizedBatch(batch, singleton);
                } else if (batch.ownerActive()) {
                    activeClaimed.add(batch);
                }
            }

            // Group claimed batches by target leader server.
            Map<Integer, List<IndexBatch>> byServer = new HashMap<>();
            for (IndexBatch batch : activeClaimed) {
                if (!batch.ownerActive()) {
                    releaseOwnedBatch(batch);
                    continue;
                }
                TableBucket tb = batch.targetBucket();
                OptionalInt leaderOpt =
                        leaderResolver.resolveLeader(tb.getTableId(), tb.getBucket());
                if (leaderOpt.isPresent()) {
                    int leader = leaderOpt.getAsInt();
                    byServer.computeIfAbsent(leader, k -> new ArrayList<>()).add(batch);
                } else {
                    reEnqueueOwnedBatch(batch);
                }
            }

            for (Map.Entry<Integer, List<IndexBatch>> serverEntry : byServer.entrySet()) {
                sendToServer(serverEntry.getKey(), serverEntry.getValue());
            }
            return true;
        }

        private void sendToServer(int serverId, List<IndexBatch> batches) {
            batches = activeBatches(batches);
            if (batches.isEmpty()) {
                return;
            }
            long now = System.currentTimeMillis();
            TargetContext target = null;
            boolean createTarget = false;
            boolean dispatch = false;
            boolean probe = false;
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                if (lifecyclePhase != LifecyclePhase.OPEN) {
                    addOwnedBatchActionsLocked(actions, batches, BatchDisposition.RELEASE);
                } else {
                    target = targetsByServer.get(serverId);
                    if (target == null) {
                        if (creatingTargets.add(serverId)) {
                            createTarget = true;
                        } else {
                            addOwnedBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                        }
                    } else if (target.compatible) {
                        dispatch = true;
                    } else if (target.queryInFlight || now < target.retryAtMs) {
                        addOwnedBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                    } else {
                        target.queryInFlight = true;
                        registerAsyncOperationLocked();
                        probe = true;
                    }
                }
                beginAccountingLocked(actions);
            } finally {
                lifecycleLock.unlock();
            }
            runAccounting(actions);

            if (createTarget) {
                createAndProbeTarget(serverId, batches);
                return;
            }
            if (dispatch) {
                dispatchIfCurrent(target, batches);
            } else if (probe) {
                invokeCapabilityProbe(target, batches);
            }
        }

        private void createAndProbeTarget(int serverId, List<IndexBatch> batches) {
            TabletServerGateway gateway = null;
            Throwable failure = null;
            try {
                gateway = gatewayFactory.apply(serverId);
            } catch (Throwable t) {
                failure = t;
            }
            boolean leadersCurrent = failure == null && leadersMatch(serverId, batches);
            TargetContext target = null;
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                creatingTargets.remove(serverId);
                if (lifecyclePhase != LifecyclePhase.OPEN) {
                    addOwnedBatchActionsLocked(actions, batches, BatchDisposition.RELEASE);
                } else if (failure != null || gateway == null || !leadersCurrent) {
                    addOwnedBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                } else {
                    target =
                            new TargetContext(
                                    serverId, gateway, ++nextTargetGeneration, false, true, 0L);
                    targetsByServer.put(serverId, target);
                    registerAsyncOperationLocked();
                }
                beginAccountingLocked(actions);
            } finally {
                lifecycleLock.unlock();
            }
            if (failure != null) {
                metrics.indexPushErrors().inc();
                LOG.warn("Failed to get gateway for server {}, re-enqueuing", serverId, failure);
            }
            runAccounting(actions);
            if (target != null) {
                invokeCapabilityProbe(target, batches);
            }
        }

        private void invokeCapabilityProbe(TargetContext target, List<IndexBatch> batches) {
            CompletableFuture<ApiVersionsResponse> capabilityFuture;
            try {
                capabilityFuture =
                        target.gateway.apiVersions(
                                new ApiVersionsRequest()
                                        .setClientSoftwareName("fluss-index-replicator")
                                        .setClientSoftwareVersion("2"));
                FutureUtils.orTimeout(
                                capabilityFuture,
                                requestTimeoutMs,
                                TimeUnit.MILLISECONDS,
                                "Index push ApiVersions timed out for serverId=" + target.serverId)
                        .whenComplete(
                                (response, error) ->
                                        completeCapabilityQuery(target, batches, response, error));
            } catch (Throwable t) {
                completeCapabilityQuery(target, batches, null, t);
            }
        }

        private void completeCapabilityQuery(
                TargetContext target,
                List<IndexBatch> batches,
                @Nullable ApiVersionsResponse response,
                @Nullable Throwable error) {
            boolean dispatch = false;
            boolean leadersCurrent = leadersMatch(target.serverId, batches);
            List<BatchAction> actions = new ArrayList<>();
            try {
                lifecycleLock.lock();
                try {
                    if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                        invalidateTargetLocked(target);
                        addRetryOrReleaseActionsLocked(actions, batches);
                    } else if (error != null) {
                        invalidateTargetLocked(target);
                        metrics.indexPushErrors().inc();
                        LOG.warn(
                                "Failed to query PutKv capability for server {}",
                                target.serverId,
                                error);
                        addRetryOrReleaseActionsLocked(actions, batches);
                    } else if (supportsPutKvV2(response)) {
                        target.compatible = true;
                        target.queryInFlight = false;
                        target.retryAtMs = Long.MAX_VALUE;
                        dispatch = true;
                    } else {
                        target.compatible = false;
                        target.queryInFlight = false;
                        target.retryAtMs =
                                System.currentTimeMillis()
                                        + retryDelayMs(batches.get(0).attempts() + 1);
                        metrics.indexPushErrors().inc();
                        LOG.warn(
                                "Target server {} does not currently advertise PutKv API v2",
                                target.serverId);
                        addOwnedBatchActionsLocked(actions, batches, BatchDisposition.REQUEUE);
                    }
                    beginAccountingLocked(actions);
                } finally {
                    lifecycleLock.unlock();
                }
                runAccounting(actions);
                if (dispatch) {
                    dispatchIfCurrent(target, batches);
                }
            } finally {
                completeAsyncOperation();
            }
        }

        private void dispatchIfCurrent(TargetContext target, List<IndexBatch> batches) {
            batches = activeBatches(batches);
            if (batches.isEmpty()) {
                return;
            }
            // Group by tableId for consolidated multi-bucket requests.
            Map<Long, List<IndexBatch>> byTable = new HashMap<>();
            for (IndexBatch batch : batches) {
                byTable.computeIfAbsent(batch.targetBucket().getTableId(), k -> new ArrayList<>())
                        .add(batch);
            }

            for (Map.Entry<Long, List<IndexBatch>> tableEntry : byTable.entrySet()) {
                long tableId = tableEntry.getKey();
                for (RequestChunk chunk : splitByRequestBytes(tableId, tableEntry.getValue())) {
                    sendOneRequest(target, tableId, chunk);
                }
            }
        }

        private void sendOneRequest(TargetContext target, long tableId, RequestChunk chunk) {
            RequestChunk requestChunk = activeChunk(tableId, chunk);
            if (requestChunk == null) {
                return;
            }
            if (isOversized(requestChunk.size)) {
                for (IndexBatch batch : requestChunk.batches) {
                    failOversizedBatch(batch, requestChunk.size);
                }
                return;
            }
            lifecycleHooks.beforePutKvInvocation();
            requestChunk = activeChunk(tableId, requestChunk);
            if (requestChunk == null) {
                return;
            }
            List<IndexBatch> requestBatches = requestChunk.batches;
            PutKvRequest request = buildRequest(tableId, requestBatches);
            long startNs = System.nanoTime();
            boolean leadersCurrent = leadersMatch(target.serverId, requestBatches);
            List<BatchAction> actions = new ArrayList<>();
            boolean registered = false;
            lifecycleHooks.beforeFinalPutKvRegistration();
            lifecycleLock.lock();
            try {
                if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                    invalidateTargetLocked(target);
                    addRetryOrReleaseActionsLocked(actions, requestBatches);
                } else if (!batchesActiveAndOwnedLocked(requestBatches)) {
                    addRetryOrReleaseActionsLocked(actions, requestBatches);
                } else {
                    registerAsyncOperationLocked();
                    registered = true;
                }
                beginAccountingLocked(actions);
            } finally {
                lifecycleLock.unlock();
            }
            lifecycleHooks.afterFinalPutKvRegistrationDecision();
            runAccounting(actions);
            if (!registered) {
                return;
            }
            CompletableFuture<PutKvResponse> future;
            try {
                future = target.gateway.putKv(request);
                FutureUtils.orTimeout(
                                future,
                                requestTimeoutMs,
                                TimeUnit.MILLISECONDS,
                                "Index push PutKv timed out for tableId=" + tableId)
                        .whenComplete(
                                (resp, err) -> {
                                    lifecycleHooks.beforePutKvCompletion();
                                    completePutKvRequest(
                                            target, tableId, requestBatches, startNs, resp, err);
                                });
            } catch (Throwable t) {
                completePutKvRequest(target, tableId, requestBatches, startNs, null, t);
            }
        }

        @Nullable
        private RequestChunk activeChunk(long tableId, RequestChunk chunk) {
            List<IndexBatch> active = activeBatches(chunk.batches);
            if (active.isEmpty()) {
                return null;
            }
            if (active.size() == chunk.batches.size()) {
                return chunk;
            }
            RequestSizeAccumulator activeSize =
                    newRequestSizeAccumulator(tableId, requestTimeoutMs);
            for (IndexBatch batch : active) {
                activeSize.addBucket(
                        batch.targetBucket().getBucket(), batch.encoded().getBytesLength());
            }
            return new RequestChunk(active, activeSize);
        }

        private List<IndexBatch> activeBatches(List<IndexBatch> batches) {
            List<IndexBatch> active = new ArrayList<>(batches.size());
            for (IndexBatch batch : batches) {
                if (batch.ownerActive()) {
                    active.add(batch);
                } else {
                    releaseOwnedBatch(batch);
                }
            }
            return active;
        }

        private void completePutKvRequest(
                TargetContext target,
                long tableId,
                List<IndexBatch> chunk,
                long startNs,
                @Nullable PutKvResponse response,
                @Nullable Throwable error) {
            boolean leadersCurrent = leadersMatch(target.serverId, chunk);
            List<BatchAction> actions = new ArrayList<>();
            lifecycleLock.lock();
            try {
                if (!targetIsCurrentLocked(target) || !leadersCurrent) {
                    invalidateTargetLocked(target);
                    addRetryOrReleaseActionsLocked(actions, chunk);
                } else if (error != null) {
                    invalidateTargetLocked(target);
                    metrics.indexPushErrors().inc();
                    LOG.warn("PutKv failed for tableId={}", tableId, error);
                    addRetryOrReleaseActionsLocked(actions, chunk);
                } else {
                    metrics.indexPushRequests().inc();
                    addResponseActionsLocked(actions, chunk, response);
                }
                beginAccountingLocked(actions);
            } finally {
                lifecycleLock.unlock();
            }
            metrics.indexPushLatencyHistogram().update((System.nanoTime() - startNs) / 1_000_000L);
            try {
                runAccountingAndCallbacks(actions);
            } finally {
                completeAsyncOperation();
            }
        }
    }

    private boolean leadersMatch(int serverId, List<IndexBatch> batches) {
        try {
            for (IndexBatch batch : batches) {
                TableBucket bucket = batch.targetBucket();
                OptionalInt leader =
                        leaderResolver.resolveLeader(bucket.getTableId(), bucket.getBucket());
                if (!leader.isPresent() || leader.getAsInt() != serverId) {
                    return false;
                }
            }
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    private boolean targetIsCurrentLocked(TargetContext target) {
        return lifecyclePhase == LifecyclePhase.OPEN
                && targetsByServer.get(target.serverId) == target;
    }

    private boolean batchesActiveAndOwnedLocked(List<IndexBatch> batches) {
        for (IndexBatch batch : batches) {
            if (!batch.ownerActive() || inFlightBatches.get(batch.targetBucket()) != batch) {
                return false;
            }
        }
        return true;
    }

    private void invalidateTargetLocked(TargetContext target) {
        if (targetsByServer.remove(target.serverId, target)) {
            nextTargetGeneration++;
        }
    }

    private void registerAsyncOperationLocked() {
        if (lifecyclePhase != LifecyclePhase.OPEN) {
            throw new IllegalStateException("Cannot register an async operation after close");
        }
        outstandingAsyncOperations++;
    }

    private void completeAsyncOperation() {
        lifecycleLock.lock();
        try {
            completeAsyncOperationLocked();
        } finally {
            lifecycleLock.unlock();
        }
        tryFinishClose(false);
    }

    private void completeAsyncOperationLocked() {
        if (--outstandingAsyncOperations == 0) {
            lifecycleDrained.signalAll();
        }
    }

    private static boolean supportsPutKvV2(@Nullable ApiVersionsResponse response) {
        if (response == null) {
            return false;
        }
        for (PbApiVersion version : response.getApiVersionsList()) {
            if (version.getApiKey() == ApiKeys.PUT_KV.id && version.getMaxVersion() >= 2) {
                return true;
            }
        }
        return false;
    }

    private static final class TargetContext {
        private final int serverId;
        private final TabletServerGateway gateway;
        /**
         * Sender-local identity; a new value is allocated only after exact-context invalidation.
         */
        private final long generation;

        private boolean compatible;
        private boolean queryInFlight;
        private long retryAtMs;

        private TargetContext(
                int serverId,
                TabletServerGateway gateway,
                long generation,
                boolean compatible,
                boolean queryInFlight,
                long retryAtMs) {
            this.serverId = serverId;
            this.gateway = gateway;
            this.generation = generation;
            this.compatible = compatible;
            this.queryInFlight = queryInFlight;
            this.retryAtMs = retryAtMs;
        }
    }

    private static final class RequestChunk {
        private final List<IndexBatch> batches;
        private final RequestSizeAccumulator size;

        private RequestChunk(List<IndexBatch> batches, RequestSizeAccumulator size) {
            this.batches = batches;
            this.size = size;
        }
    }

    /** Incremental exact size of one index PutKv request, including the transport frame. */
    @VisibleForTesting
    static final class RequestSizeAccumulator {
        private long bodyBytes;
        private boolean overflow;
        private boolean codecRepresentable;
        private int bucketCount;
        private int sizingOperations;

        private RequestSizeAccumulator(long tableId, long requestTimeoutMs) {
            this.bodyBytes = 0L;
            this.codecRepresentable = true;
            try {
                bodyBytes = checkedAdd(bodyBytes, 1L + signedIntVarintSize(DEFAULT_ACKS));
                bodyBytes = checkedAdd(bodyBytes, 1L + signedLongVarintSize(tableId));
                bodyBytes = checkedAdd(bodyBytes, 1L + signedIntVarintSize((int) requestTimeoutMs));
                bodyBytes = checkedAdd(bodyBytes, 2L); // empty packed target_columns
                bodyBytes =
                        checkedAdd(
                                bodyBytes,
                                1L + signedIntVarintSize(MergeMode.OVERWRITE.getProtoValue()));
                refreshCodecRepresentability();
            } catch (ArithmeticException e) {
                overflow = true;
                codecRepresentable = false;
            }
        }

        private RequestSizeAccumulator(RequestSizeAccumulator source) {
            this.bodyBytes = source.bodyBytes;
            this.overflow = source.overflow;
            this.codecRepresentable = source.codecRepresentable;
            this.bucketCount = source.bucketCount;
            this.sizingOperations = source.sizingOperations;
        }

        void addBucket(int bucketId, long recordsBytes) {
            bucketCount++;
            sizingOperations++;
            if (overflow || recordsBytes < 0L) {
                overflow = true;
                codecRepresentable = false;
                return;
            }
            try {
                long bucketBody = 0L;
                bucketBody = checkedAdd(bucketBody, 1L + signedIntVarintSize(bucketId));
                bucketBody = checkedAdd(bucketBody, 1L + unsignedVarintSize(recordsBytes));
                bucketBody = checkedAdd(bucketBody, recordsBytes);
                if (recordsBytes > Integer.MAX_VALUE || bucketBody > Integer.MAX_VALUE) {
                    codecRepresentable = false;
                }
                long outerContribution =
                        checkedAdd(1L + unsignedVarintSize(bucketBody), bucketBody);
                bodyBytes = checkedAdd(bodyBytes, outerContribution);
                refreshCodecRepresentability();
            } catch (ArithmeticException e) {
                overflow = true;
                codecRepresentable = false;
            }
        }

        long framedBytes() {
            if (arithmeticOverflow()) {
                return Long.MAX_VALUE;
            }
            return bodyBytes + REQUEST_HEADER_LENGTH + LENGTH_PREFIX_BYTES;
        }

        boolean arithmeticOverflow() {
            return overflow
                    || bodyBytes > Long.MAX_VALUE - REQUEST_HEADER_LENGTH - LENGTH_PREFIX_BYTES;
        }

        boolean codecRepresentable() {
            return codecRepresentable && !arithmeticOverflow();
        }

        int bucketCount() {
            return bucketCount;
        }

        int sizingOperations() {
            return sizingOperations;
        }

        private RequestSizeAccumulator copy() {
            return new RequestSizeAccumulator(this);
        }

        private void refreshCodecRepresentability() {
            if (!arithmeticOverflow() && framedBytes() > MAX_CODEC_FRAMED_BYTES) {
                codecRepresentable = false;
            }
        }
    }

    @VisibleForTesting
    static RequestSizeAccumulator newRequestSizeAccumulator(long tableId, long requestTimeoutMs) {
        return new RequestSizeAccumulator(tableId, requestTimeoutMs);
    }

    private static long checkedAdd(long left, long right) {
        return Math.addExact(left, right);
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static int signedIntVarintSize(int value) {
        return value < 0 ? 10 : unsignedVarintSize(value);
    }

    private static int signedLongVarintSize(long value) {
        return value < 0 ? 10 : unsignedVarintSize(value);
    }

    private static int unsignedVarintSize(long value) {
        int bytes = 1;
        while ((value >>>= 7) != 0L) {
            bytes++;
        }
        return bytes;
    }

    /**
     * Consolidates whole target batches up to the preferred payload bound and exact hard transport
     * bound. A singleton above the hard bound remains intact so it can enter terminal failure.
     */
    private List<RequestChunk> splitByRequestBytes(long tableId, List<IndexBatch> batches) {
        List<RequestChunk> chunks = new ArrayList<>();
        List<IndexBatch> current = new ArrayList<>();
        long currentPayloadBytes = 0L;
        RequestSizeAccumulator currentSize = newRequestSizeAccumulator(tableId, requestTimeoutMs);
        for (IndexBatch batch : batches) {
            long batchBytes = batch.encoded().getBytesLength();
            RequestSizeAccumulator candidate = currentSize.copy();
            candidate.addBucket(batch.targetBucket().getBucket(), batchBytes);
            long candidatePayloadBytes = saturatedAdd(currentPayloadBytes, batchBytes);
            boolean exceedsPreferred = candidatePayloadBytes > maxRequestBytes;
            if (!current.isEmpty() && (exceedsPreferred || isOversized(candidate))) {
                chunks.add(new RequestChunk(current, currentSize));
                current = new ArrayList<>();
                currentSize = newRequestSizeAccumulator(tableId, requestTimeoutMs);
                currentPayloadBytes = 0L;
                currentSize.addBucket(batch.targetBucket().getBucket(), batchBytes);
            } else {
                currentSize = candidate;
            }
            current.add(batch);
            currentPayloadBytes = saturatedAdd(currentPayloadBytes, batchBytes);
        }
        if (!current.isEmpty()) {
            chunks.add(new RequestChunk(current, currentSize));
        }
        return chunks;
    }

    /**
     * Completes a chunk against the per-bucket outcomes carried in the PutKv response. A bucket is
     * acked only when the response reports no error for it; a bucket that reports an error (or is
     * absent from the response) is re-enqueued for retry. This keeps a failed index mutation from
     * advancing its window's pushed offset, so SYNC visibility never releases a main-table write
     * whose index push has not actually landed.
     */
    private void addResponseActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches, PutKvResponse response) {
        Map<Integer, PbPutKvRespForBucket> respByBucket = new HashMap<>();
        for (PbPutKvRespForBucket bucketResp : response.getBucketsRespsList()) {
            respByBucket.put(bucketResp.getBucketId(), bucketResp);
        }
        for (IndexBatch batch : batches) {
            int bucketId = batch.targetBucket().getBucket();
            PbPutKvRespForBucket bucketResp = respByBucket.get(bucketId);
            boolean failed = bucketResp == null || bucketResp.hasErrorCode();
            if (failed) {
                metrics.indexPushErrors().inc();
                if (bucketResp == null) {
                    LOG.warn("PutKv response missing bucket {}, re-enqueuing", bucketId);
                } else {
                    LOG.warn(
                            "PutKv failed for bucket {} with error code {}, re-enqueuing",
                            bucketId,
                            bucketResp.getErrorCode());
                }
                addOwnedBatchActionLocked(actions, batch, BatchDisposition.REQUEUE);
            } else {
                addOwnedBatchActionLocked(actions, batch, BatchDisposition.ACK);
            }
        }
    }

    private boolean isOversized(RequestSizeAccumulator size) {
        return !size.codecRepresentable() || size.framedBytes() > maxTransportRequestBytes;
    }

    private void failOversizedBatch(IndexBatch batch, RequestSizeAccumulator size) {
        RecordTooLargeException failure = oversizedFailure(batch, size);
        IndexWindow window = batch.window();
        List<IndexBatch> siblings = window.tryFailAndDrain(failure);
        if (siblings == null) {
            return;
        }
        metrics.indexPushErrors().inc();
        metrics.indexPushRecordTooLargeFailures().inc();
        releaseWindowBatches(siblings);
    }

    private RecordTooLargeException oversizedFailure(
            IndexBatch batch, RequestSizeAccumulator size) {
        long tableId = batch.targetBucket().getTableId();
        if (size.arithmeticOverflow()) {
            return new RecordTooLargeException(
                    "Index PutKv request for table "
                            + tableId
                            + " overflows exact size arithmetic and cannot be represented by "
                            + "the request codec");
        }
        if (!size.codecRepresentable()) {
            return new RecordTooLargeException(
                    "Index PutKv request for table "
                            + tableId
                            + " is "
                            + size.framedBytes()
                            + " bytes, exceeding codec maximum framed size="
                            + MAX_CODEC_FRAMED_BYTES);
        }
        return new RecordTooLargeException(
                "Index PutKv request for table "
                        + tableId
                        + " is "
                        + size.framedBytes()
                        + " bytes, exceeding netty.server.max-request-size="
                        + maxTransportRequestBytes);
    }

    private void releaseWindowBatches(List<IndexBatch> siblings) {
        List<BatchAction> actions = new ArrayList<>();
        lifecycleLock.lock();
        try {
            for (IndexBatch sibling : siblings) {
                addOwnedBatchActionLocked(actions, sibling, BatchDisposition.RELEASE);
                // A sibling may already be between lifecycle admission and accounting. Its local
                // action retains enough ownership to finish, so the sender registry can be cleared.
                ownedBatches.remove(sibling);
            }
            beginAccountingLocked(actions);
        } finally {
            lifecycleLock.unlock();
        }
        runAccounting(actions);
        for (IndexBatch sibling : siblings) {
            accumulator.release(sibling);
            accumulator.remove(sibling);
            if (accumulator.hasPending(sibling.targetBucket())) {
                enqueueReadyBucket(sibling.targetBucket());
            }
        }
    }

    private void releaseOwnedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>(1);
        lifecycleLock.lock();
        try {
            addOwnedBatchActionLocked(actions, batch, BatchDisposition.RELEASE);
            beginAccountingLocked(actions);
        } finally {
            lifecycleLock.unlock();
        }
        runAccounting(actions);
    }

    private void releaseRejectedRetry(IndexBatch batch) {
        lifecycleLock.lock();
        try {
            ownedBatches.remove(batch);
        } finally {
            lifecycleLock.unlock();
        }
        accumulator.release(batch);
    }

    private void relinquishDroppedBatch(IndexBatch batch) {
        TableBucket bucket = batch.targetBucket();
        boolean unmuted = false;
        lifecycleLock.lock();
        try {
            ownedBatches.remove(batch);
            if (inFlightBatches.remove(bucket, batch)) {
                inFlightSinceMs.remove(bucket);
                unmuted = true;
            }
        } finally {
            lifecycleLock.unlock();
        }
        if (unmuted && accumulator.hasPending(bucket)) {
            enqueueReadyBucket(bucket);
        }
    }

    private void reEnqueueOwnedBatch(IndexBatch batch) {
        List<BatchAction> actions = new ArrayList<>();
        lifecycleLock.lock();
        try {
            addRetryOrReleaseActionLocked(actions, batch);
            beginAccountingLocked(actions);
        } finally {
            lifecycleLock.unlock();
        }
        runAccounting(actions);
    }

    private void addRetryOrReleaseActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches) {
        for (IndexBatch batch : batches) {
            addRetryOrReleaseActionLocked(actions, batch);
        }
    }

    private void addRetryOrReleaseActionLocked(List<BatchAction> actions, IndexBatch batch) {
        addOwnedBatchActionLocked(
                actions,
                batch,
                lifecyclePhase == LifecyclePhase.OPEN
                        ? BatchDisposition.REQUEUE
                        : BatchDisposition.RELEASE);
    }

    private void addOwnedBatchActionsLocked(
            List<BatchAction> actions, List<IndexBatch> batches, BatchDisposition disposition) {
        for (IndexBatch batch : batches) {
            addOwnedBatchActionLocked(actions, batch, disposition);
        }
    }

    private void addOwnedBatchActionLocked(
            List<BatchAction> actions, IndexBatch batch, BatchDisposition requestedDisposition) {
        TableBucket bucket = batch.targetBucket();
        if (!inFlightBatches.remove(bucket, batch)) {
            return;
        }
        inFlightSinceMs.remove(bucket);
        BatchDisposition disposition = requestedDisposition;
        if (lifecyclePhase != LifecyclePhase.OPEN || !batch.ownerActive()) {
            disposition = BatchDisposition.RELEASE;
        } else if (disposition == BatchDisposition.ACK && !batch.markAcked()) {
            disposition = BatchDisposition.RELEASE;
        }
        if (disposition != BatchDisposition.REQUEUE) {
            ownedBatches.remove(batch);
        }
        actions.add(new BatchAction(batch, disposition));
    }

    private void beginAccountingLocked(List<BatchAction> actions) {
        if (actions.isEmpty()) {
            return;
        }
        activeAccountingOperations++;
    }

    private void runAccounting(List<BatchAction> actions) {
        if (actions.isEmpty()) {
            return;
        }
        try {
            for (BatchAction action : actions) {
                if (action.disposition == BatchDisposition.REQUEUE) {
                    IndexBatch batch = action.batch;
                    lifecycleHooks.beforeBatchRequeue();
                    long readyAtMs =
                            System.currentTimeMillis() + retryDelayMs(batch.attempts() + 1);
                    lifecycleHooks.beforeRetryPublication();
                    if (!accumulator.reEnqueueIfActive(batch, readyAtMs)) {
                        releaseRejectedRetry(batch);
                    }
                } else {
                    accumulator.release(action.batch);
                }
            }
            for (BatchAction action : actions) {
                TableBucket bucket = action.batch.targetBucket();
                if (accumulator.hasPending(bucket)) {
                    enqueueReadyBucket(bucket);
                }
            }
        } finally {
            lifecycleLock.lock();
            try {
                activeAccountingOperations--;
                lifecycleDrained.signalAll();
            } finally {
                lifecycleLock.unlock();
            }
            tryFinishClose(false);
        }
    }

    private void runAccountingAndCallbacks(List<BatchAction> actions) {
        runAccounting(actions);
        for (BatchAction action : actions) {
            if (action.disposition != BatchDisposition.ACK) {
                continue;
            }
            lifecycleLock.lock();
            try {
                if (lifecyclePhase != LifecyclePhase.OPEN || !action.batch.ownerActive()) {
                    continue;
                }
            } finally {
                lifecycleLock.unlock();
            }
            int previousDepth = activeExternalCallbackDepth.get();
            activeExternalCallbackDepth.set(previousDepth + 1);
            try {
                lifecycleHooks.beforeProgressCallback();
                action.batch.window().onBatchAcked(action.batch);
            } finally {
                activeExternalCallbackDepth.set(previousDepth);
            }
        }
    }

    private enum BatchDisposition {
        ACK,
        REQUEUE,
        RELEASE
    }

    private static final class BatchAction {
        private final IndexBatch batch;
        private final BatchDisposition disposition;

        private BatchAction(IndexBatch batch, BatchDisposition disposition) {
            this.batch = batch;
            this.disposition = disposition;
        }
    }

    public int inFlightRequestCount() {
        return inFlightSinceMs.size();
    }

    @VisibleForTesting
    int queuedBucketCountForTesting() {
        int count = 0;
        for (SenderWorker worker : workers) {
            count += worker.queuedBucketCountForTesting();
        }
        return count;
    }

    @VisibleForTesting
    int outstandingAsyncOperationCount() {
        lifecycleLock.lock();
        try {
            return outstandingAsyncOperations;
        } finally {
            lifecycleLock.unlock();
        }
    }

    @VisibleForTesting
    int ownedBatchCountForTesting() {
        lifecycleLock.lock();
        try {
            return ownedBatches.size();
        } finally {
            lifecycleLock.unlock();
        }
    }

    @VisibleForTesting
    long ownedBatchPayloadBytesForTesting() {
        lifecycleLock.lock();
        try {
            long bytes = 0L;
            for (IndexBatch batch : ownedBatches) {
                bytes += batch.encoded().getBytesLength();
            }
            return bytes;
        } finally {
            lifecycleLock.unlock();
        }
    }

    @VisibleForTesting
    boolean isClosedForTesting() {
        return lifecyclePhase == LifecyclePhase.CLOSED;
    }

    @VisibleForTesting
    boolean isClosingForTesting() {
        return lifecyclePhase == LifecyclePhase.CLOSING;
    }

    @VisibleForTesting
    boolean lifecycleLockHeldByCurrentThreadForTesting() {
        return lifecycleLock.isHeldByCurrentThread();
    }

    public long oldestInFlightAgeMs() {
        long oldest = Long.MAX_VALUE;
        for (Long startMs : inFlightSinceMs.values()) {
            if (startMs < oldest) {
                oldest = startMs;
            }
        }
        return oldest == Long.MAX_VALUE ? 0L : System.currentTimeMillis() - oldest;
    }

    private PutKvRequest buildRequest(long tableId, List<IndexBatch> batches) {
        PutKvRequest req =
                new PutKvRequest()
                        .setTableId(tableId)
                        .setAcks(DEFAULT_ACKS)
                        .setTimeoutMs((int) requestTimeoutMs);
        req.setAggMode(MergeMode.OVERWRITE.getProtoValue());
        for (IndexBatch batch : batches) {
            BytesView encoded = batch.encoded();
            PbPutKvReqForBucket pb =
                    req.addBucketsReq().setBucketId(batch.targetBucket().getBucket());
            pb.setRecordsBytesView(encoded);
        }
        return req;
    }
}
