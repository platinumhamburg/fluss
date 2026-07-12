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

import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.MapUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexSender} per-bucket in-flight muting and at-least-once retry. */
public class IndexSenderTest {

    private enum LifecyclePoint {
        PUT_KV_INVOCATION,
        FINAL_PUT_KV_REGISTRATION,
        PUT_KV_COMPLETION,
        PROGRESS_CALLBACK,
        BATCH_REQUEUE,
        RETRY_PUBLICATION
    }

    private static final class BlockingLifecycleHooks implements IndexSender.LifecycleHooks {
        private final LifecyclePoint point;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();

        private BlockingLifecycleHooks(LifecyclePoint point) {
            this.point = point;
        }

        @Override
        public void beforePutKvInvocation() {
            blockAt(LifecyclePoint.PUT_KV_INVOCATION);
        }

        @Override
        public void beforeFinalPutKvRegistration() {
            blockAt(LifecyclePoint.FINAL_PUT_KV_REGISTRATION);
        }

        @Override
        public void beforePutKvCompletion() {
            blockAt(LifecyclePoint.PUT_KV_COMPLETION);
        }

        @Override
        public void beforeProgressCallback() {
            blockAt(LifecyclePoint.PROGRESS_CALLBACK);
        }

        @Override
        public void beforeBatchRequeue() {
            blockAt(LifecyclePoint.BATCH_REQUEUE);
        }

        @Override
        public void beforeRetryPublication() {
            blockAt(LifecyclePoint.RETRY_PUBLICATION);
        }

        private void blockAt(LifecyclePoint candidate) {
            if (candidate != point || !blocked.compareAndSet(false, true)) {
                return;
            }
            reached.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void awaitReached() throws InterruptedException {
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void release() {
            release.countDown();
        }
    }

    private static final class FinalPutKvRegistrationHooks implements IndexSender.LifecycleHooks {
        private final CountDownLatch beforeDecisionReached = new CountDownLatch(1);
        private final CountDownLatch beforeDecisionRelease = new CountDownLatch(1);
        private final CountDownLatch afterDecisionReached = new CountDownLatch(1);
        private final CountDownLatch afterDecisionRelease = new CountDownLatch(1);

        @Override
        public void beforeFinalPutKvRegistration() {
            await(beforeDecisionReached, beforeDecisionRelease);
        }

        @Override
        public void afterFinalPutKvRegistrationDecision() {
            await(afterDecisionReached, afterDecisionRelease);
        }

        private void awaitBeforeDecision() throws InterruptedException {
            assertThat(beforeDecisionReached.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void releaseBeforeDecision() {
            beforeDecisionRelease.countDown();
        }

        private void awaitAfterDecision() throws InterruptedException {
            assertThat(afterDecisionReached.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void releaseAfterDecision() {
            afterDecisionRelease.countDown();
        }

        private static void await(CountDownLatch reached, CountDownLatch release) {
            reached.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** Gateway that records every {@code putKv} call and lets the test control completion. */
    private static final class RecordingGateway extends TestTabletServerGateway {
        private final List<CompletableFuture<PutKvResponse>> pending = new CopyOnWriteArrayList<>();
        private final List<PutKvRequest> requests = new CopyOnWriteArrayList<>();
        private final Set<Integer> failBuckets =
                Collections.newSetFromMap(MapUtils.newConcurrentMap());
        private volatile boolean failNext;
        private volatile boolean autoCompleteSuccess;
        private volatile short putKvMaxVersion = 2;
        private volatile int apiVersionsCalls;
        private volatile CompletableFuture<ApiVersionsResponse> pendingApiVersions;

        RecordingGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
            requests.add(request);
            CompletableFuture<PutKvResponse> future = new CompletableFuture<>();
            pending.add(future);
            if (failNext) {
                future.completeExceptionally(new RuntimeException("injected failure"));
            } else if (autoCompleteSuccess) {
                future.complete(responseFor(request));
            }
            return future;
        }

        @Override
        public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
            apiVersionsCalls++;
            if (pendingApiVersions != null) {
                return pendingApiVersions;
            }
            return CompletableFuture.completedFuture(apiVersionsResponse());
        }

        private ApiVersionsResponse apiVersionsResponse() {
            ApiVersionsResponse response = new ApiVersionsResponse();
            response.addAllApiVersions(
                    Collections.singletonList(
                            new PbApiVersion()
                                    .setApiKey(ApiKeys.PUT_KV.id)
                                    .setMinVersion(0)
                                    .setMaxVersion(putKvMaxVersion)));
            return response;
        }

        private void completeApiVersions(boolean success) {
            CompletableFuture<ApiVersionsResponse> future = pendingApiVersions;
            if (success) {
                future.complete(apiVersionsResponse());
            } else {
                future.completeExceptionally(new RuntimeException("injected capability failure"));
            }
        }

        /**
         * Builds a per-bucket response mirroring the request: buckets listed in {@link
         * #failBuckets} carry an error code, the rest are acked. This matches the real server
         * contract where PutKv reports success/failure per bucket rather than per request.
         */
        private PutKvResponse responseFor(PutKvRequest request) {
            PutKvResponse response = new PutKvResponse();
            for (PbPutKvReqForBucket bucketReq : request.getBucketsReqsList()) {
                PbPutKvRespForBucket bucketResp =
                        response.addBucketsResp().setBucketId(bucketReq.getBucketId());
                if (failBuckets.contains(bucketReq.getBucketId())) {
                    bucketResp.setError(
                            Errors.UNKNOWN_SERVER_ERROR.code(), "injected per-bucket failure");
                }
            }
            return response;
        }
    }

    @Test
    void freshProxyFromFactoryIsNotTreatedAsGatewayReplacement() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway delegate = new RecordingGateway();
        delegate.autoCompleteSuccess = true;
        AtomicInteger factoryCalls = new AtomicInteger();
        IndexSender sender =
                sender(
                        accumulator,
                        ignored -> {
                            factoryCalls.incrementAndGet();
                            return freshProxy(delegate);
                        },
                        ignored -> 1,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(39L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));

            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(delegate.apiVersionsCalls).isEqualTo(1);
            assertThat(delegate.requests).hasSize(1);
            assertThat(factoryCalls.get()).isEqualTo(1);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void transportFailureInvalidatesOnlyItsTargetGenerationAndRetriesWithNewProxy()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        first.failNext = true;
        first.autoCompleteSuccess = true;
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger factoryCalls = new AtomicInteger();
        IndexSender sender =
                sender(
                        accumulator,
                        ignored ->
                                freshProxy(
                                        factoryCalls.getAndIncrement() == 0 ? first : second),
                        ignored -> 1,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(38L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));

            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(first.requests).hasSize(1);
            assertThat(second.requests).hasSize(1);
            assertThat(factoryCalls.get()).isEqualTo(2);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateOldGenerationCompletionCannotFenceNewSameServerTarget(boolean lateSuccess)
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger factoryCalls = new AtomicInteger();
        IndexSender sender =
                sender(
                        accumulator,
                        ignored ->
                                freshProxy(
                                        factoryCalls.getAndIncrement() == 0 ? first : second),
                        ignored -> 1,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            IndexReplicator firstOwner = owner(accumulator);
            IndexReplicator secondOwner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(381L, 0),
                            new IndexWindow("first", 10L, 1, firstOwner)));
            accumulator.append(
                    batch(
                            new TableBucket(382L, 0),
                            new IndexWindow("second", 20L, 1, secondOwner)));
            await(() -> first.pending.size() == 2);

            completePutKv(first, 0, false);
            await(() -> second.requests.size() == 1);
            completePutKv(first, 1, lateSuccess);

            await(() -> second.requests.size() == 2);
            await(() -> firstOwner.getSyncIndexPushedOffset() == 10L);
            await(() -> secondOwner.getSyncIndexPushedOffset() == 20L);
            assertThat(first.requests).hasSize(2);
            assertThat(factoryCalls.get()).isEqualTo(2);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void synchronousAckProgressRunsUnlockedAndCanCloseSenderReentrantly() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.autoCompleteSuccess = true;
        AtomicReference<IndexSender> senderRef = new AtomicReference<>();
        AtomicBoolean closeReturned = new AtomicBoolean();
        CountDownLatch progress = new CountDownLatch(1);
        IndexReplicator owner =
                owner(
                        accumulator,
                        (sync, all) -> {
                            IndexSender sender = senderRef.get();
                            assertThat(sender.lifecycleLockHeldByCurrentThreadForTesting())
                                    .isFalse();
                            assertThat(sender.inFlightRequestCount()).isZero();
                            assertThat(accumulator.pendingBytes()).isZero();
                            assertThat(accumulator.hasUnsent()).isFalse();
                            sender.close();
                            closeReturned.set(true);
                            progress.countDown();
                        });
        IndexSender sender =
                sender(
                        accumulator,
                        ignored -> gateway,
                        ignored -> 1,
                        IndexSender.LifecycleHooks.NO_OP);
        senderRef.set(sender);
        try {
            accumulator.append(
                    batch(
                            new TableBucket(37L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));

            assertThat(progress.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeReturned).isTrue();
            await(sender::isClosedForTesting);
            assertThat(gateway.requests).hasSize(1);
            assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(10L);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void admittedProgressCallbackCanReentrantlyCloseWhileNormalCloseWaits() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PROGRESS_CALLBACK);
        AtomicReference<IndexSender> senderRef = new AtomicReference<>();
        AtomicBoolean callbackCloseReturned = new AtomicBoolean();
        IndexReplicator owner =
                owner(
                        accumulator,
                        (sync, all) -> {
                            senderRef.get().close();
                            callbackCloseReturned.set(true);
                        });
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        senderRef.set(sender);
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            accumulator.append(
                    batch(
                            new TableBucket(36L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.pending.size() == 1);

            Future<?> completion = executor.submit(() -> completePutKv(gateway, 0, true));
            hooks.awaitReached();
            Future<?> close = executor.submit(sender::close);
            await(sender::isClosingForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            completion.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThat(callbackCloseReturned).isTrue();
            assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(10L);
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            sender.close();
            executor.shutdownNow();
        }
    }

    @Test
    void normalCloseWaitsForTrueClosedAfterReentrantCloseReturns() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        AtomicReference<IndexSender> senderRef = new AtomicReference<>();
        CountDownLatch reentrantCloseReturned = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        IndexReplicator owner =
                owner(
                        accumulator,
                        (sync, all) -> {
                            senderRef.get().close();
                            reentrantCloseReturned.countDown();
                            awaitLatch(releaseCallback);
                        });
        IndexSender sender =
                sender(
                        accumulator,
                        ignored -> gateway,
                        ignored -> 1,
                        IndexSender.LifecycleHooks.NO_OP);
        senderRef.set(sender);
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            accumulator.append(
                    batch(
                            new TableBucket(35L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.pending.size() == 1);
            Future<?> completion = executor.submit(() -> completePutKv(gateway, 0, true));
            assertThat(reentrantCloseReturned.await(5, TimeUnit.SECONDS)).isTrue();

            CountDownLatch normalCloseStarted = new CountDownLatch(1);
            CountDownLatch normalCloseReturned = new CountDownLatch(1);
            Future<?> normalClose =
                    executor.submit(
                            () -> {
                                normalCloseStarted.countDown();
                                sender.close();
                                normalCloseReturned.countDown();
                            });
            assertThat(normalCloseStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(normalCloseReturned.await(100, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(sender.isClosingForTesting()).isTrue();
            assertThat(sender.isClosedForTesting()).isFalse();

            releaseCallback.countDown();
            completion.get(5, TimeUnit.SECONDS);
            normalClose.get(5, TimeUnit.SECONDS);
            assertThat(normalCloseReturned.getCount()).isZero();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            releaseCallback.countDown();
            sender.close();
            executor.shutdownNow();
        }
    }

    @Test
    void closeDrainsBatchWhoseAdmittedFailureActionRequeuesAfterClosingStarts()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.BATCH_REQUEUE);
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        ExecutorService executor = Executors.newCachedThreadPool();
        IndexBatch claimed =
                batch(
                        new TableBucket(34L, 0),
                        new IndexWindow("idx", 10L, 1, owner(accumulator)));
        try {
            accumulator.append(claimed);
            await(() -> gateway.pending.size() == 1);
            Future<?> failure = executor.submit(() -> completePutKv(gateway, 0, false));
            hooks.awaitReached();

            Future<?> close = executor.submit(sender::close);
            await(sender::isClosingForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            failure.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThat(gateway.requests).hasSize(1);
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(claimed.markReleased()).isFalse();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            sender.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeFencesLateCapabilityCallback(boolean success) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.pendingApiVersions = new CompletableFuture<>();
        TableBucket bucket = new TableBucket(40L, 0);
        IndexReplicator owner = owner(accumulator);
        accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);

        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            await(() -> gateway.apiVersionsCalls == 1);
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosingForTesting);
            assertThat(close.isDone()).isFalse();

            gateway.completeApiVersions(success);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).isEmpty();
            assertThat(sender.inFlightRequestCount()).isZero();
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            gateway.completeApiVersions(success);
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @Test
    void closeLinearizesBeforePutKvInvocationAndWaitsForProbeCleanup() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.pendingApiVersions = new CompletableFuture<>();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_INVOCATION);
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(44L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.apiVersionsCalls == 1);

            Future<?> probeCompletion =
                    lifecycleExecutor.submit(() -> gateway.completeApiVersions(true));
            hooks.awaitReached();
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosingForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            probeCompletion.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).isEmpty();
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            gateway.completeApiVersions(true);
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeWaitsForLatePutKvCallbackAndFencesItsSideEffects(boolean success) throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_COMPLETION);
        IndexSender sender = sender(accumulator, ignored -> gateway, ignored -> 1, hooks);
        ExecutorService lifecycleExecutor = Executors.newCachedThreadPool();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(45L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> gateway.pending.size() == 1);

            Future<?> requestCompletion =
                    lifecycleExecutor.submit(
                            () -> completePutKv(gateway, 0, success));
            hooks.awaitReached();
            Future<?> close = lifecycleExecutor.submit(sender::close);
            await(sender::isClosingForTesting);
            assertThat(close.isDone()).isFalse();

            hooks.release();
            requestCompletion.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);

            assertThat(gateway.requests).hasSize(1);
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            if (!gateway.pending.isEmpty()) {
                completePutKv(gateway, 0, success);
            }
            sender.close();
            lifecycleExecutor.shutdownNow();
        }
    }

    @Test
    void leaderReplacementBeforePutKvInvocationRetriesWithoutSendingToOldTarget()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_INVOCATION);
        IndexSender sender =
                sender(
                        accumulator,
                        serverId -> serverId == 1 ? first : second,
                        ignored -> leader.get(),
                        hooks);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(47L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            hooks.awaitReached();
            leader.set(2);
            hooks.release();

            await(() -> second.requests.size() == 1);
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(first.requests).isEmpty();
            assertThat(second.requests).hasSize(1);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            hooks.release();
            sender.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leaderReplacementFencesLatePutKvCompletionAndRetriesExactlyOnce(boolean success)
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_COMPLETION);
        IndexSender sender =
                sender(
                        accumulator,
                        serverId -> serverId == 1 ? first : second,
                        ignored -> leader.get(),
                        hooks);
        ExecutorService completionExecutor = Executors.newSingleThreadExecutor();
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(46L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.pending.size() == 1);

            Future<?> completion =
                    completionExecutor.submit(() -> completePutKv(first, 0, success));
            hooks.awaitReached();
            leader.set(2);
            hooks.release();
            completion.get(5, TimeUnit.SECONDS);

            await(() -> second.requests.size() == 1);
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(first.requests).hasSize(1);
            assertThat(second.requests).hasSize(1);
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            hooks.release();
            sender.close();
            completionExecutor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leaderChangeFencesLateCapabilityCallbackAndRetriesNewTarget(boolean success)
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway first = new RecordingGateway();
        first.pendingApiVersions = new CompletableFuture<>();
        RecordingGateway second = new RecordingGateway();
        second.autoCompleteSuccess = true;
        AtomicInteger leader = new AtomicInteger(1);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(leader.get()),
                        serverId -> serverId == 1 ? first : second,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            IndexReplicator owner = owner(accumulator);
            accumulator.append(
                    batch(
                            new TableBucket(41L, 0),
                            new IndexWindow("idx", 10L, 1, owner)));
            await(() -> first.apiVersionsCalls == 1);

            leader.set(2);
            first.completeApiVersions(success);

            await(() -> second.requests.size() == 1);
            assertThat(first.requests).isEmpty();
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void gatesExactV1BytesOnConcreteGatewayCapabilityWithoutFallback() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.putKvMaxVersion = 1;
        gateway.autoCompleteSuccess = true;
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        10L,
                        10L,
                        1024L * 1024L,
                        5_000L);
        try {
            TableBucket bucket = new TableBucket(42L, 0);
            IndexReplicator owner = owner(accumulator);
            IndexBatch encodedBatch =
                    v1Batch(bucket, new IndexWindow("idx", 10L, 1, owner));
            byte[] expected = bytes(encodedBatch.encoded());
            KvRecordBatch parsed =
                    KvRecordBatchReader.pointToByteBuffer(
                            encodedBatch.encoded().getByteBuf().nioBuffer());
            assertThat(parsed.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V1);
            assertThat(parsed.fencedWriterKey()).isEqualTo(new WriterKey(9L, 3L));
            assertThat(parsed.fencedSequence()).isEqualTo(10L);
            accumulator.append(encodedBatch);

            await(() -> gateway.apiVersionsCalls == 1);
            assertThat(gateway.requests).isEmpty();

            gateway.putKvMaxVersion = 2;
            await(() -> gateway.apiVersionsCalls >= 2);
            await(() -> gateway.requests.size() == 1);

            PutKvRequest request = gateway.requests.get(0);
            assertThat(request.getAcks()).isEqualTo(-1);
            assertThat(request.getAggMode()).isEqualTo(MergeMode.OVERWRITE.getProtoValue());
            org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf records =
                    request.getBucketsReqsList().get(0).getRecordsSlice();
            byte[] sent = new byte[records.readableBytes()];
            records.getBytes(records.readerIndex(), sent);
            assertThat(sent).containsExactly(expected);
        } finally {
            sender.close();
        }
    }

    @Test
    void exactFramedRequestAtHardLimitIsSent() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.autoCompleteSuccess = true;
        TableBucket bucket = new TableBucket(43L, 0);
        IndexReplicator owner = owner(accumulator);
        IndexBatch encodedBatch = v1Batch(bucket, new IndexWindow("idx", 10L, 1, owner));
        long exactRequestBytes = exactRequestBytes(bucket, encodedBatch);
        accumulator.append(encodedBatch);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, targetBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes,
                        5_000L,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            assertThat(gateway.requests).hasSize(1);
            assertThat(
                            Integer.BYTES
                                    + MessageCodec.REQUEST_HEADER_LENGTH
                                    + gateway.requests.get(0).totalSize())
                    .isEqualTo(exactRequestBytes);
            assertThat(encodedBatch.window().registeredBatchCount()).isZero();
        } finally {
            sender.close();
        }
    }

    @Test
    void singletonAboveHardLimitFailsOnceWithoutRetryOrOffsetAdvance() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.autoCompleteSuccess = true;
        TableBucket bucket = new TableBucket(44L, 0);
        IndexReplicator owner = owner(accumulator);
        IndexBatch encodedBatch = v1Batch(bucket, new IndexWindow("idx", 10L, 1, owner));
        long exactRequestBytes = exactRequestBytes(bucket, encodedBatch);
        long errorsBefore = TestingMetricGroups.TABLET_SERVER_METRICS.indexPushErrors().getCount();
        accumulator.append(encodedBatch);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, targetBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes - 1,
                        5_000L,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            await(() -> owner.terminalFailure() != null);
            await(() -> sender.inFlightRequestCount() == 0);

            assertThat(owner.terminalFailure()).isInstanceOf(RecordTooLargeException.class);
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertThat(gateway.requests).isEmpty();
            assertThat(encodedBatch.attempts()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(TestingMetricGroups.TABLET_SERVER_METRICS.indexPushErrors().getCount())
                    .isEqualTo(errorsBefore + 1);
        } finally {
            sender.close();
        }
    }

    @Test
    void oversizedSingletonFailsBeforeUnresolvedLeaderAndReleasesQueuedSibling()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        TableBucket largeBucket = new TableBucket(45L, 0);
        TableBucket siblingBucket = new TableBucket(45L, 1);
        IndexReplicator owner = owner(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        IndexBatch large = batchOfSize(largeBucket, window, 128);
        IndexBatch sibling = batchOfSize(siblingBucket, window, 1);
        long hardLimit = exactRequestBytes(siblingBucket, sibling);
        AtomicInteger leaderResolutions = new AtomicInteger();
        AtomicInteger gatewayCreations = new AtomicInteger();
        accumulator.append(large);
        accumulator.append(sibling);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, targetBucket) -> {
                            leaderResolutions.incrementAndGet();
                            return OptionalInt.empty();
                        },
                        serverId -> {
                            gatewayCreations.incrementAndGet();
                            return new RecordingGateway();
                        },
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        hardLimit,
                        5_000L,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            await(() -> owner.terminalFailure() != null);
            await(() -> accumulator.pendingBytes() == 0L);

            assertThat(owner.terminalFailure()).isInstanceOf(RecordTooLargeException.class);
            assertThat(leaderResolutions).hasValue(0);
            assertThat(gatewayCreations).hasValue(0);
            assertThat(large.attempts()).isZero();
            assertThat(sibling.attempts()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(window.registeredBatchCount()).isZero();
        } finally {
            sender.close();
        }
    }

    @Test
    void terminalizationAtFinalRetryPublicationBoundaryPreventsRetry() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        long tableId = 49L;
        int retryBucketId = 0;
        int oversizedBucketId = 1;
        while (Math.floorMod(new TableBucket(tableId, retryBucketId).hashCode(), 2)
                == Math.floorMod(new TableBucket(tableId, oversizedBucketId).hashCode(), 2)) {
            oversizedBucketId++;
        }
        TableBucket retryBucket = new TableBucket(tableId, retryBucketId);
        TableBucket oversizedBucket = new TableBucket(tableId, oversizedBucketId);
        IndexReplicator owner = ownerWithIndex(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        owner.registerInFlightWindow("idx", window);
        IndexBatch retry = batchOfSize(retryBucket, window, 1);
        IndexBatch oversized = batchOfSize(oversizedBucket, window, 1024 * 1024);
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.RETRY_PUBLICATION);
        accumulator.append(retry);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (ignoredTable, ignoredBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        2,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes(retryBucket, retry),
                        5_000L,
                        hooks);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThat(owner.inFlightWindow("idx")).isSameAs(window);
            await(() -> gateway.pending.size() == 1);
            Future<?> failedRequest =
                    executor.submit(() -> completePutKv(gateway, 0, false));
            hooks.awaitReached();

            accumulator.append(oversized);
            await(() -> owner.terminalFailure() != null);
            await(() -> accumulator.pendingBytes() == 0L);

            hooks.release();
            failedRequest.get(5, TimeUnit.SECONDS);

            assertThat(retry.attempts()).isZero();
            assertThat(oversized.attempts()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(gateway.requests).hasSize(1);
            assertThat(window.registeredBatchCount()).isZero();
            assertThat(owner.inFlightWindow("idx")).isNull();
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
        } finally {
            hooks.release();
            sender.close();
            executor.shutdownNow();
        }
    }

    @Test
    void ownerCloseAtFinalRetryPublicationReleasesSenderOwnershipAndAllowsClose()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket bucket = new TableBucket(491L, 0);
        IndexReplicator owner = owner(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 1, owner);
        IndexBatch batch = batchOfSize(bucket, window, 17);
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.RETRY_PUBLICATION);
        accumulator.append(batch);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (ignoredTable, ignoredBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L,
                        hooks);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            await(() -> gateway.pending.size() == 1);
            Future<?> failedRequest =
                    executor.submit(() -> completePutKv(gateway, 0, false));
            hooks.awaitReached();

            owner.close();
            assertThat(accumulator.dropForReplicator(owner)).isZero();
            hooks.release();
            failedRequest.get(5, TimeUnit.SECONDS);

            assertThat(batch.attempts()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(sender.ownedBatchCountForTesting()).isZero();

            Future<?> close = executor.submit(sender::close);
            close.get(5, TimeUnit.SECONDS);
            assertClosedAndDrained(sender, accumulator);
        } finally {
            hooks.release();
            sender.close();
            executor.shutdownNow();
        }
    }

    @Test
    void droppingPublishedRetryRelinquishesSenderOwnership() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket bucket = new TableBucket(492L, 0);
        IndexReplicator owner = owner(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 1, owner);
        IndexBatch batch = batchOfSize(bucket, window, 64 * 1024);
        accumulator.append(batch);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (ignoredTable, ignoredBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        TimeUnit.MINUTES.toMillis(1),
                        TimeUnit.MINUTES.toMillis(1),
                        1024L * 1024L,
                        5_000L);
        try {
            await(() -> gateway.pending.size() == 1);
            completePutKv(gateway, 0, false);
            await(() -> batch.attempts() == 1);

            assertThat(accumulator.hasUnsent()).isTrue();
            assertThat(sender.ownedBatchCountForTesting()).isEqualTo(1);
            assertThat(sender.ownedBatchPayloadBytesForTesting()).isEqualTo(64L * 1024L);

            owner.close();
            assertThat(accumulator.dropForReplicator(owner)).isEqualTo(1);

            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(sender.ownedBatchCountForTesting())
                    .as("no sender root may retain the dropped batch payload")
                    .isZero();
            assertThat(sender.ownedBatchPayloadBytesForTesting()).isZero();
        } finally {
            sender.close();
        }
    }

    @Test
    void oversizedSingletonFailsBeforeIncompatibleCapabilityProbe() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.putKvMaxVersion = 1;
        TableBucket bucket = new TableBucket(46L, 0);
        IndexReplicator owner = owner(accumulator);
        IndexBatch batch = batchOfSize(bucket, new IndexWindow("idx", 10L, 1, owner), 64);
        accumulator.append(batch);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, targetBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes(bucket, batch) - 1,
                        5_000L,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            await(() -> owner.terminalFailure() != null);

            assertThat(gateway.apiVersionsCalls).isZero();
            assertThat(gateway.requests).isEmpty();
            assertThat(batch.attempts()).isZero();
        } finally {
            sender.close();
        }
    }

    @Test
    void oversizedSiblingCancelsInFlightBatchAndIgnoresLateCompletion() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket smallBucket = new TableBucket(47L, 0);
        TableBucket largeBucket = new TableBucket(47L, 1);
        IndexReplicator owner = owner(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        IndexBatch small = batchOfSize(smallBucket, window, 1);
        IndexBatch large = batchOfSize(largeBucket, window, 128);
        long hardLimit = exactRequestBytes(smallBucket, small);
        accumulator.append(small);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, targetBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        hardLimit,
                        5_000L,
                        IndexSender.LifecycleHooks.NO_OP);
        try {
            await(() -> gateway.requests.size() == 1);
            accumulator.append(large);
            await(() -> owner.terminalFailure() != null);
            await(() -> sender.inFlightRequestCount() == 0);

            completePutKv(gateway, 0, true);
            await(() -> accumulator.pendingBytes() == 0L);

            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertThat(gateway.requests).hasSize(1);
            assertThat(small.attempts()).isZero();
            assertThat(large.attempts()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void oversizedSiblingStopsRequestPausedBeforeRpcRegistration() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        long tableId = 48L;
        int smallBucketId = 0;
        int largeBucketId = 1;
        while (Math.floorMod(new TableBucket(tableId, smallBucketId).hashCode(), 2)
                == Math.floorMod(new TableBucket(tableId, largeBucketId).hashCode(), 2)) {
            largeBucketId++;
        }
        TableBucket smallBucket = new TableBucket(tableId, smallBucketId);
        TableBucket largeBucket = new TableBucket(tableId, largeBucketId);
        IndexReplicator owner = owner(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        IndexBatch small = batchOfSize(smallBucket, window, 1);
        IndexBatch large = batchOfSize(largeBucket, window, 128);
        BlockingLifecycleHooks hooks =
                new BlockingLifecycleHooks(LifecyclePoint.PUT_KV_INVOCATION);
        accumulator.append(small);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (ignoredTable, ignoredBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        2,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes(smallBucket, small),
                        5_000L,
                        hooks);
        try {
            hooks.awaitReached();
            accumulator.append(large);
            await(() -> owner.terminalFailure() != null);
            hooks.release();
            await(() -> sender.inFlightRequestCount() == 0);

            assertThat(gateway.requests).isEmpty();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
        } finally {
            hooks.release();
            sender.close();
        }
    }

    @Test
    void terminalizationAtFinalPutKvRegistrationPreventsAsyncOperationAndSend()
            throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        long tableId = 481L;
        int smallBucketId = 0;
        int largeBucketId = 1;
        while (Math.floorMod(new TableBucket(tableId, smallBucketId).hashCode(), 2)
                == Math.floorMod(new TableBucket(tableId, largeBucketId).hashCode(), 2)) {
            largeBucketId++;
        }
        TableBucket smallBucket = new TableBucket(tableId, smallBucketId);
        TableBucket largeBucket = new TableBucket(tableId, largeBucketId);
        IndexReplicator owner = ownerWithIndex(accumulator);
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        owner.registerInFlightWindow("idx", window);
        IndexBatch small = batchOfSize(smallBucket, window, 1);
        IndexBatch large = batchOfSize(largeBucket, window, 128);
        FinalPutKvRegistrationHooks hooks = new FinalPutKvRegistrationHooks();
        accumulator.append(small);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (ignoredTable, ignoredBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        2,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        exactRequestBytes(smallBucket, small),
                        5_000L,
                        hooks);
        try {
            hooks.awaitBeforeDecision();
            accumulator.append(large);
            await(() -> owner.terminalFailure() != null);
            assertThat(owner.inFlightWindow("idx")).isNull();

            hooks.releaseBeforeDecision();
            hooks.awaitAfterDecision();
            hooks.releaseAfterDecision();
            await(() -> sender.outstandingAsyncOperationCount() == 0);

            assertThat(gateway.requests).isEmpty();
            assertThat(sender.inFlightRequestCount()).isZero();
            assertThat(sender.outstandingAsyncOperationCount()).isZero();
            assertThat(sender.ownedBatchCountForTesting()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(small.attempts()).isZero();
            assertThat(large.attempts()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
        } finally {
            hooks.releaseBeforeDecision();
            hooks.releaseAfterDecision();
            sender.close();
        }
    }

    @Test
    void exactSizerMatchesGeneratedCodecAcrossVarintAndRecordBoundaries() {
        long[] tableIds =
                new long[] {
                    Long.MIN_VALUE, -1L, 0L, 127L, 128L, 16_383L, 16_384L, Long.MAX_VALUE
                };
        int[] bucketIds =
                new int[] {
                    Integer.MIN_VALUE,
                    -1,
                    0,
                    127,
                    128,
                    16_383,
                    16_384,
                    Integer.MAX_VALUE
                };
        int[] recordLengths = new int[] {0, 1, 127, 128, 16_383, 16_384};

        for (long tableId : tableIds) {
            for (int bucketId : bucketIds) {
                for (int recordLength : recordLengths) {
                    IndexSender.RequestSizeAccumulator size =
                            IndexSender.newRequestSizeAccumulator(tableId, 5_000L);
                    size.addBucket(bucketId, recordLength);
                    PutKvRequest request =
                            new PutKvRequest()
                                    .setTableId(tableId)
                                    .setAcks(-1)
                                    .setTimeoutMs(5_000)
                                    .setAggMode(MergeMode.OVERWRITE.getProtoValue());
                    PbPutKvReqForBucket bucket =
                            request.addBucketsReq()
                                    .setBucketId(bucketId)
                                    .setRecordsBytesView(
                                            new MemorySegmentBytesView(
                                                    MemorySegment.wrap(new byte[recordLength]),
                                                    0,
                                                    recordLength));

                    assertThat(request.hasAggMode()).isTrue();
                    assertThat(bucket.hasPartitionId()).isFalse();
                    assertThat(size.codecRepresentable()).isTrue();
                    assertThat(size.framedBytes())
                            .as(
                                    "tableId=%s, bucketId=%s, records=%s",
                                    tableId,
                                    bucketId,
                                    recordLength)
                            .isEqualTo(
                                    Integer.BYTES
                                            + MessageCodec.REQUEST_HEADER_LENGTH
                                            + request.totalSize());
                }
            }
        }
    }

    @Test
    void exactSizerRejectsCodecCeilingAndLongArithmeticOverflow() {
        long low = 0L;
        long high = Integer.MAX_VALUE;
        while (low < high) {
            long candidateLength = low + (high - low + 1L) / 2L;
            IndexSender.RequestSizeAccumulator candidate =
                    IndexSender.newRequestSizeAccumulator(1L, 5_000L);
            candidate.addBucket(0, candidateLength);
            if (candidate.codecRepresentable()) {
                low = candidateLength;
            } else {
                high = candidateLength - 1L;
            }
        }
        IndexSender.RequestSizeAccumulator atCodecCeiling =
                IndexSender.newRequestSizeAccumulator(1L, 5_000L);
        atCodecCeiling.addBucket(0, low);
        IndexSender.RequestSizeAccumulator aboveCodecCeiling =
                IndexSender.newRequestSizeAccumulator(1L, 5_000L);
        aboveCodecCeiling.addBucket(0, low + 1L);
        assertThat(atCodecCeiling.codecRepresentable()).isTrue();
        assertThat(atCodecCeiling.framedBytes()).isLessThanOrEqualTo(Integer.MAX_VALUE);
        assertThat(aboveCodecCeiling.codecRepresentable()).isFalse();

        IndexSender.RequestSizeAccumulator codecCeiling =
                IndexSender.newRequestSizeAccumulator(1L, 5_000L);
        codecCeiling.addBucket(0, Integer.MAX_VALUE);
        assertThat(codecCeiling.arithmeticOverflow()).isFalse();
        assertThat(codecCeiling.codecRepresentable()).isFalse();

        IndexSender.RequestSizeAccumulator overflow =
                IndexSender.newRequestSizeAccumulator(1L, 5_000L);
        overflow.addBucket(0, Long.MAX_VALUE);
        assertThat(overflow.arithmeticOverflow()).isTrue();
        assertThat(overflow.codecRepresentable()).isFalse();
    }

    @Test
    void manySmallBucketsUseOneIncrementalSizingContributionEach() {
        int bucketCount = 10_000;
        IndexSender.RequestSizeAccumulator size =
                IndexSender.newRequestSizeAccumulator(1L, 5_000L);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            size.addBucket(bucket, 1L);
        }

        assertThat(size.bucketCount()).isEqualTo(bucketCount);
        assertThat(size.sizingOperations()).isEqualTo(bucketCount);
        assertThat(size.codecRepresentable()).isTrue();
    }

    private static IndexBatch batch(TableBucket targetBucket, IndexWindow window) {
        byte[] bytes = new byte[] {1, 2, 3};
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length);
        return new IndexBatch(targetBucket, encoded, window);
    }

    private static IndexBatch batchOfSize(
            TableBucket targetBucket, IndexWindow window, int size) {
        byte[] bytes = new byte[size];
        BytesView encoded = new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, size);
        return new IndexBatch(targetBucket, encoded, window);
    }

    private static IndexBatch v1Batch(TableBucket targetBucket, IndexWindow window)
            throws Exception {
        FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        1,
                        1024,
                        new UnmanagedPagedOutputView(128),
                        KvFormat.COMPACTED);
        builder.append(new byte[] {7, 8, 9}, null);
        builder.setWriterState(new WriterKey(9L, 3L), 10L);
        return new IndexBatch(targetBucket, builder.build(), window);
    }

    private static byte[] bytes(BytesView view) {
        byte[] bytes = new byte[view.getBytesLength()];
        view.getByteBuf().getBytes(view.getByteBuf().readerIndex(), bytes);
        return bytes;
    }

    private static long exactRequestBytes(TableBucket bucket, IndexBatch batch) {
        PutKvRequest request =
                new PutKvRequest()
                        .setTableId(bucket.getTableId())
                        .setAcks(-1)
                        .setTimeoutMs(5_000)
                        .setAggMode(MergeMode.OVERWRITE.getProtoValue());
        request.addBucketsReq()
                .setBucketId(bucket.getBucket())
                .setRecordsBytesView(batch.encoded());
        return Integer.BYTES + MessageCodec.REQUEST_HEADER_LENGTH + request.totalSize();
    }

    /** A per-bucket success response acking exactly the given bucket ids (no error set). */
    private static PutKvResponse ackResponse(int... bucketIds) {
        PutKvResponse response = new PutKvResponse();
        for (int bucketId : bucketIds) {
            response.addBucketsResp().setBucketId(bucketId);
        }
        return response;
    }

    private static IndexReplicator owner(IndexAccumulator accumulator) {
        return owner(accumulator, (sync, all) -> {});
    }

    private static IndexReplicator ownerWithIndex(IndexAccumulator accumulator) {
        IndexSpec spec =
                new IndexSpec(
                        "idx",
                        IndexVisibility.SYNC,
                        1L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> null);
        return new IndexReplicator(
                null,
                Collections.singletonList(spec),
                accumulator,
                null,
                0L,
                1024,
                (sync, all) -> {});
    }

    private static IndexReplicator owner(
            IndexAccumulator accumulator, IndexReplicator.IndexProgressListener listener) {
        return new IndexReplicator(
                null, Collections.emptyList(), accumulator, null, 0L, 1024, listener);
    }

    private static IndexSender sender(
            IndexAccumulator accumulator,
            Function<Integer, ? extends TabletServerGateway> gatewayFactory,
            java.util.function.IntUnaryOperator leader,
            IndexSender.LifecycleHooks hooks) {
        return new IndexSender(
                accumulator,
                (tableId, bucket) -> OptionalInt.of(leader.applyAsInt(bucket)),
                gatewayFactory::apply,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                1,
                5L,
                1L,
                1L,
                1024L * 1024L,
                5_000L,
                hooks);
    }

    private static TabletServerGateway freshProxy(TabletServerGateway delegate) {
        return (TabletServerGateway)
                Proxy.newProxyInstance(
                        TabletServerGateway.class.getClassLoader(),
                        new Class<?>[] {TabletServerGateway.class},
                        (proxy, method, args) -> {
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }

    private static void completePutKv(RecordingGateway gateway, int requestIndex, boolean success) {
        CompletableFuture<PutKvResponse> future = gateway.pending.get(requestIndex);
        if (success) {
            future.complete(gateway.responseFor(gateway.requests.get(requestIndex)));
        } else {
            future.completeExceptionally(new RuntimeException("injected request failure"));
        }
    }

    private static void assertClosedAndDrained(
            IndexSender sender, IndexAccumulator accumulator) {
        assertThat(sender.isClosedForTesting()).isTrue();
        assertThat(sender.inFlightRequestCount()).isZero();
        assertThat(sender.outstandingAsyncOperationCount()).isZero();
        assertThat(accumulator.pendingBytes()).isZero();
        assertThat(accumulator.hasUnsent()).isFalse();
    }

    private static void await(BooleanSupplier condition) {
        waitUntil(
                condition::getAsBoolean,
                Duration.ofSeconds(5),
                "Condition was not met within timeout");
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    void senderDrainsBatchesThatExistedBeforeStartup() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket bucket = new TableBucket(90L, 0);
        IndexReplicator owner = owner(accumulator);
        accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, indexBucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            await(() -> gateway.pending.size() == 1);
            gateway.pending.get(0).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void inFlightMutingSerializesSendsForSameBucket() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(100L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));
            accumulator.append(batch(bucket, new IndexWindow("idx", 20L, 1, owner)));

            // Only the first batch is dispatched; the bucket is muted until its ack.
            await(() -> gateway.pending.size() == 1);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);
            assertThat(accumulator.hasPending(bucket))
                    .as("second batch must stay queued while the first is in flight")
                    .isTrue();
            assertThat(gateway.pending.size())
                    .as("second batch must not be sent while the first is in flight")
                    .isEqualTo(1);

            // Ack the first send -> bucket unmuted -> second batch dispatched.
            gateway.pending.get(0).complete(ackResponse(0));
            await(() -> gateway.pending.size() == 2);
            assertThat(owner.getSyncIndexPushedOffset()).isEqualTo(10L);

            // Ack the second send -> the second window advances the pushed offset.
            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 20L);
        } finally {
            sender.close();
        }
    }

    @Test
    void failedSendIsRetriedWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.failNext = true;
        gateway.autoCompleteSuccess = true;
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(200L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            // Failure re-enqueues for retry: more than one send attempt is observed.
            await(() -> gateway.pending.size() >= 2);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("offset must not advance while sends keep failing")
                    .isEqualTo(0L);

            // Recover: the next attempt auto-succeeds and the offset finally advances.
            gateway.failNext = false;
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void failedHeadBatchRetriesBeforeLaterBatchInSameBucket() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            TableBucket bucket = new TableBucket(250L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));
            accumulator.append(batch(bucket, new IndexWindow("idx", 20L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            gateway.pending.get(0).completeExceptionally(new RuntimeException("first failed"));

            await(() -> gateway.pending.size() == 2);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);
            assertThat(accumulator.hasPending(bucket))
                    .as("later batch must stay queued while the failed head is being retried")
                    .isTrue();
            assertThat(gateway.pending.size())
                    .as("later batch must not be sent while the failed head is being retried")
                    .isEqualTo(2);

            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            await(() -> gateway.pending.size() == 3);

            gateway.pending.get(2).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 20L);
        } finally {
            sender.close();
        }
    }

    @Test
    void partialBucketFailureRetriesOnlyFailedBucketAndHoldsOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        gateway.autoCompleteSuccess = true;
        gateway.failBuckets.add(1); // bucket 1's index push keeps failing
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            IndexReplicator owner = owner(accumulator);
            // One window spanning two index buckets of the same table; it completes only once both
            // buckets are acked.
            IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
            accumulator.append(batch(new TableBucket(300L, 0), window));
            accumulator.append(batch(new TableBucket(300L, 1), window));

            // Bucket 1 keeps failing, so the window never completes and the offset never advances
            // even though bucket 0 was acked. A buggy "RPC-ok == whole-batch-acked" sender would
            // wrongly advance the offset here.
            await(() -> gateway.pending.size() >= 3);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("a per-bucket failure must not advance the pushed offset")
                    .isEqualTo(0L);

            // Recover bucket 1: the window now completes and the offset advances to the window end.
            gateway.failBuckets.clear();
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
        } finally {
            sender.close();
        }
    }

    @Test
    void acknowledgedSiblingIsRemovedFromWindowRegistryWhileRetryStalls() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        TableBucket ackBucket = new TableBucket(301L, 0);
        TableBucket retryBucket = new TableBucket(301L, 1);
        AtomicInteger progressCallbacks = new AtomicInteger();
        IndexReplicator owner =
                owner(accumulator, (sync, all) -> progressCallbacks.incrementAndGet());
        IndexWindow window = new IndexWindow("idx", 10L, 2, owner);
        IndexBatch acked = batchOfSize(ackBucket, window, 11);
        IndexBatch retry = batchOfSize(retryBucket, window, 23);
        accumulator.append(acked);
        accumulator.append(retry);
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        5_000L);
        try {
            await(() -> gateway.pending.size() == 1);
            assertThat(window.registeredBatchCount()).isEqualTo(2);
            assertThat(window.registeredPayloadBytes()).isEqualTo(34L);

            gateway.failBuckets.add(retryBucket.getBucket());
            completePutKv(gateway, 0, true);
            await(() -> gateway.pending.size() == 2);
            await(() -> window.registeredBatchCount() == 1);

            assertThat(acked.isReleased()).isTrue();
            assertThat(retry.isReleased()).isFalse();
            assertThat(window.registeredPayloadBytes()).isEqualTo(23L);
            assertThat(accumulator.pendingBytes()).isEqualTo(23L);
            assertThat(owner.getSyncIndexPushedOffset()).isZero();
            assertThat(progressCallbacks).hasValue(0);

            window.onBatchAcked(acked);
            assertThat(window.registeredBatchCount()).isEqualTo(1);
            assertThat(progressCallbacks).hasValue(0);

            gateway.failBuckets.clear();
            completePutKv(gateway, 1, true);
            await(() -> owner.getSyncIndexPushedOffset() == 10L);

            assertThat(window.registeredBatchCount()).isZero();
            assertThat(window.registeredPayloadBytes()).isZero();
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(progressCallbacks).hasValue(1);

            window.onBatchAcked(acked);
            window.onBatchAcked(retry);
            assertThat(window.registeredBatchCount()).isZero();
            assertThat(progressCallbacks).hasValue(1);
        } finally {
            sender.close();
        }
    }

    @Test
    void stuckPutKvTimesOutAndRetriesWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L,
                        1L,
                        1L,
                        1024L * 1024L,
                        50L);
        try {
            TableBucket bucket = new TableBucket(400L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(sender.inFlightRequestCount()).isEqualTo(1);

            // The first future is never completed by the gateway. The sender-side timeout must
            // unmute and retry the bucket instead of leaving the window stuck forever.
            await(() -> gateway.pending.size() >= 2);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("timeout must not ack or advance the window")
                    .isEqualTo(0L);

            gateway.pending.get(1).complete(ackResponse(0));
            await(() -> owner.getSyncIndexPushedOffset() == 10L);
            await(() -> sender.inFlightRequestCount() == 0);
        } finally {
            sender.close();
        }
    }

    @Test
    void ackAfterOwnerClosedReleasesBatchWithoutAdvancingOffset() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(500L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(accumulator.pendingBytes()).isEqualTo(3L);

            owner.close();
            gateway.pending.get(0).complete(ackResponse(0));

            await(() -> sender.inFlightRequestCount() == 0);
            assertThat(owner.getSyncIndexPushedOffset())
                    .as("a closed owner must not be advanced by a late ack")
                    .isEqualTo(0L);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }

    @Test
    void failureAfterOwnerClosedReleasesBatchWithoutRetryLoop() throws Exception {
        IndexAccumulator accumulator = new IndexAccumulator();
        RecordingGateway gateway = new RecordingGateway();
        IndexSender sender =
                new IndexSender(
                        accumulator,
                        (tableId, bucket) -> OptionalInt.of(1),
                        serverId -> gateway,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        1,
                        5L);
        try {
            TableBucket bucket = new TableBucket(600L, 0);
            IndexReplicator owner = owner(accumulator);
            accumulator.append(batch(bucket, new IndexWindow("idx", 10L, 1, owner)));

            await(() -> gateway.pending.size() == 1);
            assertThat(accumulator.pendingBytes()).isEqualTo(3L);

            owner.close();
            gateway.pending.get(0).completeExceptionally(new RuntimeException("late failure"));

            await(() -> sender.inFlightRequestCount() == 0);
            assertThat(gateway.pending)
                    .as("closed-owner batch must not be re-enqueued for retry")
                    .hasSize(1);
            assertThat(accumulator.pendingBytes()).isZero();
            assertThat(accumulator.hasUnsent()).isFalse();
        } finally {
            sender.close();
        }
    }
}
