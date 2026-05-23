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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexMutation;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.MergeMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-data-bucket-leader push client that batches {@link IndexMutation}s by target Index Bucket
 * leader and dispatches them via {@link TabletServerGateway#putKv(PutKvRequest)}. Retries
 * indefinitely with exponential backoff until each batch is acked; on success, invokes the
 * supplied ack callback so the surrounding scheduler can advance the {@code indexPushedOffset}
 * watermark.
 *
 * <p>This class is intentionally minimal — it does NOT embed a {@code FlussConnection} /
 * {@code UpsertWriter} / {@code IdempotenceManager}. Idempotence is provided by the Index Table's
 * primary key: replays of the same (key, value) UPSERT or (key) DELETE are safe.
 *
 * <p>Thread-safety: {@link #send(List)} may be invoked from multiple threads concurrently; ack
 * callbacks may fire on the gateway's callback thread or on the retry executor.
 */
@Internal
public final class IndexPushSender implements AutoCloseable {

    /** Resolves the TabletServer id hosting the leader for a given Index Bucket. */
    @FunctionalInterface
    public interface LeaderResolver {
        /** Returns the TabletServer id hosting the leader for {@code (indexTableId, indexBucket)}. */
        int resolveLeader(long indexTableId, int indexBucket);
    }

    private static final int DEFAULT_ACKS = 1;
    private static final int DEFAULT_TIMEOUT_MS = 30_000;
    private static final long INITIAL_BACKOFF_MS = 200L;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final int PAGE_SIZE = 4096;

    // TODO: derive per-(indexTableId) from metadata once IndexPushScheduler (P2T9) wires it in;
    // for now Index Tables are single-schema (schemaId=1) by construction.
    private static final int DEFAULT_SCHEMA_ID = 1;

    private final Function<Integer, TabletServerGateway> gatewayFactory;
    private final LeaderResolver leaderResolver;
    private final ScheduledExecutorService retryExecutor;
    private final BiConsumer<Long, IndexPushTracker.Target> ackCallback;
    private final int maxBatchSize;

    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final Object drainLock = new Object();
    private volatile boolean closed = false;

    public IndexPushSender(
            Function<Integer, TabletServerGateway> gatewayFactory,
            LeaderResolver leaderResolver,
            ScheduledExecutorService retryExecutor,
            BiConsumer<Long, IndexPushTracker.Target> ackCallback,
            int maxBatchSize) {
        this.gatewayFactory = checkNotNull(gatewayFactory, "gatewayFactory");
        this.leaderResolver = checkNotNull(leaderResolver, "leaderResolver");
        this.retryExecutor = checkNotNull(retryExecutor, "retryExecutor");
        this.ackCallback = checkNotNull(ackCallback, "ackCallback");
        checkArgument(maxBatchSize > 0, "maxBatchSize must be positive, got %s", maxBatchSize);
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Groups {@code mutations} by {@code (indexTableId, indexBucket)}, splits each group into
     * batches of at most {@link #maxBatchSize}, and dispatches every batch. Returns immediately
     * after enqueueing; ack callbacks fire asynchronously.
     */
    public void send(List<IndexMutation> mutations) {
        checkNotNull(mutations, "mutations");
        if (closed) {
            throw new IllegalStateException("IndexPushSender is closed");
        }
        if (mutations.isEmpty()) {
            return;
        }
        Map<IndexPushTracker.Target, List<IndexMutation>> grouped = new LinkedHashMap<>();
        for (IndexMutation m : mutations) {
            IndexPushTracker.Target target =
                    new IndexPushTracker.Target(m.getIndexTableId(), m.getIndexBucket());
            grouped.computeIfAbsent(target, k -> new ArrayList<>()).add(m);
        }
        for (Map.Entry<IndexPushTracker.Target, List<IndexMutation>> e : grouped.entrySet()) {
            List<IndexMutation> all = e.getValue();
            for (int i = 0; i < all.size(); i += maxBatchSize) {
                int end = Math.min(i + maxBatchSize, all.size());
                List<IndexMutation> batch = new ArrayList<>(all.subList(i, end));
                inFlight.incrementAndGet();
                attempt(e.getKey(), batch, INITIAL_BACKOFF_MS);
            }
        }
    }

    /**
     * Stops accepting new sends and blocks until every in-flight batch has either been acked or
     * given up (when the retry executor refuses further scheduling).
     */
    @Override
    public void close() {
        closed = true;
        synchronized (drainLock) {
            while (inFlight.get() > 0) {
                try {
                    drainLock.wait(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // Internal: dispatch + retry
    // ------------------------------------------------------------------------

    private void attempt(
            IndexPushTracker.Target target, List<IndexMutation> batch, long nextBackoffMs) {
        TabletServerGateway gateway;
        PutKvRequest request;
        try {
            int leaderId =
                    leaderResolver.resolveLeader(
                            target.getIndexTableId(), target.getIndexBucket());
            gateway = gatewayFactory.apply(leaderId);
            if (gateway == null) {
                scheduleRetry(target, batch, nextBackoffMs);
                return;
            }
            request = buildRequest(target, batch);
        } catch (Throwable t) {
            scheduleRetry(target, batch, nextBackoffMs);
            return;
        }

        CompletableFuture<PutKvResponse> future;
        try {
            future = gateway.putKv(request);
        } catch (Throwable t) {
            scheduleRetry(target, batch, nextBackoffMs);
            return;
        }
        if (future == null) {
            scheduleRetry(target, batch, nextBackoffMs);
            return;
        }
        future.whenComplete(
                (resp, err) -> {
                    if (err != null || hasError(resp)) {
                        scheduleRetry(target, batch, nextBackoffMs);
                    } else {
                        try {
                            for (IndexMutation m : batch) {
                                ackCallback.accept(m.getSourceOffset(), target);
                            }
                        } finally {
                            completeOne();
                        }
                    }
                });
    }

    private void scheduleRetry(
            IndexPushTracker.Target target, List<IndexMutation> batch, long delayMs) {
        if (closed) {
            // Give up: stop holding back the drain.
            completeOne();
            return;
        }
        long nextBackoff = Math.min((long) (delayMs * BACKOFF_MULTIPLIER), MAX_BACKOFF_MS);
        try {
            retryExecutor.schedule(
                    () -> attempt(target, batch, nextBackoff), delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rex) {
            // Executor shut down: give up so close() can drain.
            completeOne();
        }
    }

    private void completeOne() {
        if (inFlight.decrementAndGet() == 0) {
            synchronized (drainLock) {
                drainLock.notifyAll();
            }
        }
    }

    private static boolean hasError(PutKvResponse resp) {
        if (resp == null) {
            return true;
        }
        for (int i = 0; i < resp.getBucketsRespsCount(); i++) {
            if (resp.getBucketsRespAt(i).hasErrorCode()) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    // Internal: PutKvRequest encoding
    // ------------------------------------------------------------------------

    private static PutKvRequest buildRequest(
            IndexPushTracker.Target target, List<IndexMutation> batch) {
        PutKvRequest req =
                new PutKvRequest()
                        .setTableId(target.getIndexTableId())
                        .setAcks(DEFAULT_ACKS)
                        .setTimeoutMs(DEFAULT_TIMEOUT_MS);
        // Index Tables write the entire value row; mergeMode DEFAULT (no aggregation engine
        // configured on the Index Table) is equivalent to full overwrite.
        req.setAggMode(MergeMode.DEFAULT.getProtoValue());
        PbPutKvReqForBucket pb = req.addBucketsReq().setBucketId(target.getIndexBucket());
        pb.setRecordsBytesView(encodeRecords(batch));
        return req;
    }

    private static BytesView encodeRecords(List<IndexMutation> batch) {
        UnmanagedPagedOutputView output = new UnmanagedPagedOutputView(PAGE_SIZE);
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        DEFAULT_SCHEMA_ID, Integer.MAX_VALUE, output, KvFormat.COMPACTED);
        try {
            for (IndexMutation m : batch) {
                if (m.getValue() == null) {
                    builder.append(m.getKey(), null);
                } else {
                    builder.append(m.getKey(), wrapBytesAsCompactedRow(m.getValue()));
                }
            }
            return builder.build();
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to encode IndexMutation batch", ioe);
        }
    }

    /**
     * Wraps a pre-encoded value byte array as a {@link CompactedRow} that the {@link
     * KvRecordBatchBuilder} can serialize byte-for-byte. The arity/deserializer fields are unused
     * because {@code CompactedRowWriter.serializeCompactedRow} only inspects segment/offset/size.
     */
    private static CompactedRow wrapBytesAsCompactedRow(byte[] bytes) {
        CompactedRow row = new CompactedRow(0, null);
        row.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return row;
    }
}
