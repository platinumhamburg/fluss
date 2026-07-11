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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
 * IndexAccumulator#reEnqueue(IndexBatch)} for unlimited retry, and the owning window's offset is
 * never advanced. On success the batch notifies its window via {@link IndexBatch#window()}, which
 * advances the replicator's pushed offset once the whole window is acknowledged.
 */
@Internal
@ThreadSafe
public final class IndexSender implements AutoCloseable {

    /** Resolves the leader server ID for a given Index Table bucket. */
    @FunctionalInterface
    public interface LeaderResolver {
        OptionalInt resolveLeader(long indexTableId, int indexBucket);
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexSender.class);

    private static final int DEFAULT_ACKS = -1;
    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;
    private static final long DEFAULT_RETRY_MAX_BACKOFF_MS = 10_000L;
    private static final long DEFAULT_MAX_REQUEST_BYTES = 1024L * 1024L;

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

    /** Timeout applied to each index {@code PutKv} request. */
    private final long requestTimeoutMs;

    /** Buckets with an in-flight request; value is the request send timestamp in millis. */
    private final ConcurrentMap<TableBucket, Long> inFlightSinceMs = MapUtils.newConcurrentMap();
    private final ConcurrentMap<TableBucket, Integer> resolvedLeaders = MapUtils.newConcurrentMap();
    private final Object capabilityLock = new Object();
    private final Map<Integer, CapabilityEntry> capabilitiesByServer = new HashMap<>();

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
                DEFAULT_TIMEOUT_MS);
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
        this.accumulator = accumulator;
        this.leaderResolver = leaderResolver;
        this.gatewayFactory = gatewayFactory;
        this.metrics = metrics;
        this.retryBackoffMs = retryBackoffMs;
        this.retryMaxBackoffMs = retryMaxBackoffMs;
        this.maxRequestBytes = maxRequestBytes;
        this.requestTimeoutMs = requestTimeoutMs;
        int workerCount = Math.max(1, numWorkers);
        this.workers = new SenderWorker[workerCount];
        for (int i = 0; i < workerCount; i++) {
            this.workers[i] = new SenderWorker("index-sender-" + i, i, backoffMs);
        }
        accumulator.setAppendListener(this::enqueueReadyBucket);
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
        workers[ownerOf(bucket)].enqueueReadyBucket(bucket);
    }

    private int ownerOf(TableBucket bucket) {
        return Math.floorMod(bucket.hashCode(), workers.length);
    }

    @Override
    public void close() {
        LOG.info("IndexSender closing");
        for (SenderWorker worker : workers) {
            worker.initiateShutdown();
        }
        for (SenderWorker worker : workers) {
            worker.wakeup();
            try {
                worker.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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

        @Override
        public void doWork() {
            boolean didWork = drainAndSend();
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

        /** Claims at most one batch per owned, non-muted bucket and dispatches them. */
        private boolean drainAndSend() {
            long now = System.currentTimeMillis();
            List<IndexBatch> claimed = new ArrayList<>();
            List<TableBucket> deferredBuckets = new ArrayList<>();
            TableBucket bucket;
            while ((bucket = pollReadyBucket()) != null) {
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
                claimed.add(batch);
            }
            for (TableBucket deferredBucket : deferredBuckets) {
                enqueueReadyBucket(deferredBucket);
            }
            if (claimed.isEmpty()) {
                return false;
            }

            // Group claimed batches by target leader server.
            Map<Integer, List<IndexBatch>> byServer = new HashMap<>();
            for (IndexBatch batch : claimed) {
                TableBucket tb = batch.targetBucket();
                OptionalInt leaderOpt =
                        leaderResolver.resolveLeader(tb.getTableId(), tb.getBucket());
                if (leaderOpt.isPresent()) {
                    int leader = leaderOpt.getAsInt();
                    Integer previous = resolvedLeaders.put(tb, leader);
                    if (previous != null && previous != leader) {
                        synchronized (capabilityLock) {
                            capabilitiesByServer.remove(previous);
                        }
                    }
                    byServer.computeIfAbsent(leader, k -> new ArrayList<>())
                            .add(batch);
                } else {
                    // Leader not resolvable yet: re-enqueue with backoff and unmute for later
                    // retry.
                    unmute(tb);
                    reEnqueueIfOwnerActive(batch);
                }
            }

            for (Map.Entry<Integer, List<IndexBatch>> serverEntry : byServer.entrySet()) {
                sendToServer(serverEntry.getKey(), serverEntry.getValue());
            }
            return true;
        }

        private void sendToServer(int serverId, List<IndexBatch> batches) {
            TabletServerGateway gateway;
            try {
                gateway = gatewayFactory.apply(serverId);
            } catch (Throwable t) {
                LOG.warn("Failed to get gateway for server {}, re-enqueuing", serverId, t);
                metrics.indexPushErrors().inc();
                reEnqueueAll(batches);
                return;
            }
            if (gateway == null) {
                reEnqueueAll(batches);
                return;
            }

            long now = System.currentTimeMillis();
            boolean useCachedCapability = false;
            boolean waitForRetry = false;
            synchronized (capabilityLock) {
                CapabilityEntry cached = capabilitiesByServer.get(serverId);
                if (cached != null && cached.gateway == gateway) {
                    if (cached.compatible) {
                        useCachedCapability = true;
                    } else if (cached.queryInFlight || now < cached.retryAtMs) {
                        waitForRetry = true;
                    }
                }
                if (!useCachedCapability && !waitForRetry) {
                    capabilitiesByServer.put(
                            serverId, new CapabilityEntry(gateway, false, true, Long.MAX_VALUE));
                }
            }
            if (useCachedCapability) {
                sendCapableBatches(gateway, batches);
                return;
            }
            if (waitForRetry) {
                reEnqueueAll(batches);
                return;
            }

            CompletableFuture<ApiVersionsResponse> capabilityFuture;
            try {
                capabilityFuture =
                        gateway.apiVersions(
                                new ApiVersionsRequest()
                                        .setClientSoftwareName("fluss-index-replicator")
                                        .setClientSoftwareVersion("2"));
            } catch (Throwable t) {
                completeCapabilityQuery(serverId, gateway, batches, null, t);
                return;
            }
            FutureUtils.orTimeout(
                            capabilityFuture,
                            requestTimeoutMs,
                            TimeUnit.MILLISECONDS,
                            "Index push ApiVersions timed out for serverId=" + serverId)
                    .whenComplete(
                            (response, error) ->
                                    completeCapabilityQuery(
                                            serverId, gateway, batches, response, error));
        }

        private void completeCapabilityQuery(
                int serverId,
                TabletServerGateway gateway,
                List<IndexBatch> batches,
                @Nullable ApiVersionsResponse response,
                @Nullable Throwable error) {
            boolean compatible = error == null && supportsPutKvV2(response);
            long retryAtMs =
                    compatible
                            ? Long.MAX_VALUE
                            : System.currentTimeMillis()
                                    + retryDelayMs(batches.get(0).attempts() + 1);
            boolean invalidated;
            synchronized (capabilityLock) {
                CapabilityEntry current = capabilitiesByServer.get(serverId);
                invalidated = current == null || current.gateway != gateway;
                if (!invalidated) {
                    capabilitiesByServer.put(
                            serverId,
                            new CapabilityEntry(gateway, compatible, false, retryAtMs));
                }
            }
            if (invalidated) {
                reEnqueueAll(batches);
                return;
            }
            if (compatible) {
                sendCapableBatches(gateway, batches);
            } else {
                metrics.indexPushErrors().inc();
                LOG.warn(
                        "Target server {} does not currently advertise PutKv API v2",
                        serverId,
                        error);
                reEnqueueAll(batches);
            }
        }

        private void sendCapableBatches(
                TabletServerGateway gateway, List<IndexBatch> batches) {

            // Group by tableId for consolidated multi-bucket requests.
            Map<Long, List<IndexBatch>> byTable = new HashMap<>();
            for (IndexBatch batch : batches) {
                byTable.computeIfAbsent(batch.targetBucket().getTableId(), k -> new ArrayList<>())
                        .add(batch);
            }

            for (Map.Entry<Long, List<IndexBatch>> tableEntry : byTable.entrySet()) {
                long tableId = tableEntry.getKey();
                // Split each table's batches so no single request exceeds maxRequestBytes.
                for (List<IndexBatch> chunk : splitByRequestBytes(tableEntry.getValue())) {
                    sendOneRequest(gateway, tableId, chunk);
                }
            }
        }

        private void sendOneRequest(
                TabletServerGateway gateway, long tableId, List<IndexBatch> chunk) {
            PutKvRequest request = buildRequest(tableId, chunk);
            long startNs = System.nanoTime();
            try {
                CompletableFuture<PutKvResponse> future = gateway.putKv(request);
                FutureUtils.orTimeout(
                                future,
                                requestTimeoutMs,
                                TimeUnit.MILLISECONDS,
                                "Index push PutKv timed out for tableId=" + tableId)
                        .whenComplete(
                                (resp, err) -> {
                                    metrics.indexPushLatencyHistogram()
                                            .update((System.nanoTime() - startNs) / 1_000_000L);
                                    if (err != null) {
                                        metrics.indexPushErrors().inc();
                                        LOG.warn("PutKv failed for tableId={}", tableId, err);
                                        reEnqueueAll(chunk);
                                    } else {
                                        metrics.indexPushRequests().inc();
                                        completeByResponse(chunk, resp);
                                    }
                                });
            } catch (Throwable t) {
                metrics.indexPushErrors().inc();
                reEnqueueAll(chunk);
            }
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

    private static final class CapabilityEntry {
        private final TabletServerGateway gateway;
        private final boolean compatible;
        private final boolean queryInFlight;
        private final long retryAtMs;

        private CapabilityEntry(
                TabletServerGateway gateway,
                boolean compatible,
                boolean queryInFlight,
                long retryAtMs) {
            this.gateway = gateway;
            this.compatible = compatible;
            this.queryInFlight = queryInFlight;
            this.retryAtMs = retryAtMs;
        }
    }

    /**
     * Splits batches into chunks whose total encoded size stays within {@link #maxRequestBytes}. A
     * single oversized batch still forms its own chunk so it is never dropped.
     */
    private List<List<IndexBatch>> splitByRequestBytes(List<IndexBatch> batches) {
        List<List<IndexBatch>> chunks = new ArrayList<>();
        List<IndexBatch> current = new ArrayList<>();
        long currentBytes = 0L;
        for (IndexBatch batch : batches) {
            long size = batch.encoded().getBytesLength();
            if (!current.isEmpty() && currentBytes + size > maxRequestBytes) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = 0L;
            }
            current.add(batch);
            currentBytes += size;
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    /**
     * Completes a chunk against the per-bucket outcomes carried in the PutKv response. A bucket is
     * acked only when the response reports no error for it; a bucket that reports an error (or is
     * absent from the response) is re-enqueued for retry. This keeps a failed index mutation from
     * advancing its window's pushed offset, so SYNC visibility never releases a main-table
     * write whose index push has not actually landed.
     */
    private void completeByResponse(List<IndexBatch> batches, PutKvResponse response) {
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
                reEnqueueIfOwnerActive(batch);
                unmute(batch.targetBucket());
            } else {
                try {
                    if (batch.ownerClosed()) {
                        accumulator.release(batch);
                    } else if (batch.markAcked()) {
                        accumulator.release(batch);
                        batch.window().onBatchAcked();
                    }
                } finally {
                    unmute(batch.targetBucket());
                }
            }
        }
    }

    private void reEnqueueAll(List<IndexBatch> batches) {
        for (IndexBatch batch : batches) {
            reEnqueueIfOwnerActive(batch);
            unmute(batch.targetBucket());
        }
    }

    /**
     * Re-enqueue a failed batch with an exponential retry backoff applied before it is eligible.
     */
    private boolean reEnqueueIfOwnerActive(IndexBatch batch) {
        if (batch.ownerClosed()) {
            accumulator.release(batch);
            return false;
        }
        // attempts() is the pre-retry count; the upcoming reEnqueue increments it, so use +1 here.
        // readyAtMs must be set before the batch is published back to the queue to avoid a racing
        // worker re-sending it ahead of its backoff.
        batch.setReadyAtMs(System.currentTimeMillis() + retryDelayMs(batch.attempts() + 1));
        accumulator.reEnqueue(batch);
        enqueueReadyBucket(batch.targetBucket());
        return true;
    }

    private void unmute(TableBucket bucket) {
        inFlightSinceMs.remove(bucket);
        if (accumulator.hasPending(bucket)) {
            enqueueReadyBucket(bucket);
        }
    }

    public int inFlightRequestCount() {
        return inFlightSinceMs.size();
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
