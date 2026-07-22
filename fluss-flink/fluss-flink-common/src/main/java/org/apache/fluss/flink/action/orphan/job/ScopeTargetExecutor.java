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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Bounded concurrent execution of live Scope targets with caller-thread result replay. */
final class ScopeTargetExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeTargetExecutor.class);
    private static final long CLOSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10L);

    private final int concurrency;
    private final ExecutorService executor;
    private final ExecutorCompletionService<ScopeTargetEnumeration.Result> completion;
    private final Set<Future<ScopeTargetEnumeration.Result>> inFlight =
            new HashSet<Future<ScopeTargetEnumeration.Result>>();
    private final Queue<Throwable> workerCloseFailures = new ConcurrentLinkedQueue<Throwable>();
    private final ThreadLocal<WorkerContext> workerContext = new ThreadLocal<WorkerContext>();
    private final Supplier<WorkerContext> contextFactory;
    @Nullable private final Connection ownedConnection;
    private final long closeTimeoutMillis;
    private final Object workerExitMonitor = new Object();
    private int liveWorkerThreads;

    private boolean processingFailed;
    private boolean closed;

    private ScopeTargetExecutor(
            int concurrency,
            Supplier<WorkerContext> contextFactory,
            @Nullable Connection ownedConnection,
            long closeTimeoutMillis) {
        checkArgument(concurrency > 0, "concurrency must be greater than 0");
        checkArgument(closeTimeoutMillis > 0, "close timeout must be greater than 0");
        this.concurrency = concurrency;
        this.contextFactory = contextFactory;
        this.ownedConnection = ownedConnection;
        this.closeTimeoutMillis = closeTimeoutMillis;
        this.executor = Executors.newFixedThreadPool(concurrency, new ScopeTargetThreadFactory());
        this.completion = new ExecutorCompletionService<ScopeTargetEnumeration.Result>(executor);
    }

    static ScopeTargetExecutor create(
            Connection connection, int concurrency, RateLimiter remoteFsOpRateLimiter) {
        return create(
                connection,
                concurrency,
                admin -> ScopeTargetEnumeration.worker(admin, remoteFsOpRateLimiter));
    }

    /** Creates an executor that assumes ownership of {@code connection}. */
    static ScopeTargetExecutor create(
            Connection connection,
            int concurrency,
            Function<Admin, ScopeTargetEnumeration.Worker> workerFactory) {
        return create(connection, concurrency, workerFactory, CLOSE_TIMEOUT_MILLIS);
    }

    static ScopeTargetExecutor create(
            Connection connection,
            int concurrency,
            Function<Admin, ScopeTargetEnumeration.Worker> workerFactory,
            long closeTimeoutMillis) {
        return new ScopeTargetExecutor(
                concurrency,
                () -> {
                    Admin admin = connection.createAdmin();
                    try {
                        return new WorkerContext(admin, workerFactory.apply(admin));
                    } catch (RuntimeException | Error failure) {
                        try {
                            admin.close();
                        } catch (Exception | Error closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                        throw failure;
                    }
                },
                connection,
                closeTimeoutMillis);
    }

    static ScopeTargetExecutor testing(
            int concurrency, Supplier<ScopeTargetEnumeration.Worker> workerFactory) {
        return new ScopeTargetExecutor(
                concurrency,
                () -> new WorkerContext(null, workerFactory.get()),
                null,
                CLOSE_TIMEOUT_MILLIS);
    }

    static ScopeTargetExecutor testingContexts(
            int concurrency, Supplier<WorkerContext> contextFactory) {
        return new ScopeTargetExecutor(concurrency, contextFactory, null, CLOSE_TIMEOUT_MILLIS);
    }

    void forEachCompleted(
            List<ScopeTargetEnumeration.Input> inputs,
            Consumer<ScopeTargetEnumeration.Result> consumer)
            throws Exception {
        checkState(!closed, "Scope target executor is already closed");
        int next = 0;
        try {
            while (next < inputs.size() && inFlight.size() < concurrency) {
                submit(inputs.get(next++));
            }
            while (!inFlight.isEmpty()) {
                Future<ScopeTargetEnumeration.Result> completed = completion.take();
                inFlight.remove(completed);
                try {
                    consumer.accept(completed.get());
                } catch (ExecutionException failure) {
                    rethrowWorkerFailure(failure, consumer);
                }
                if (next < inputs.size()) {
                    submit(inputs.get(next++));
                }
            }
        } catch (Exception | Error failure) {
            processingFailed = true;
            throw failure;
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        if (processingFailed) {
            cancelOutstanding();
        }
        executor.shutdownNow();

        Throwable closeFailure = null;
        boolean resourcesSafeToClose = false;
        try {
            if (!executor.awaitTermination(closeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                closeFailure = new IOException("Timed out closing Scope target executor");
            } else {
                awaitWorkerThreadExit();
                resourcesSafeToClose = true;
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            closeFailure = failure;
        }

        closeFailure = drainWorkerCloseFailures(closeFailure);
        if (resourcesSafeToClose) {
            closeFailure = closeOwnedConnection(closeFailure);
        } else {
            startDeferredConnectionCleanup();
        }
        if (closeFailure != null) {
            rethrow(closeFailure);
        }
    }

    private void submit(ScopeTargetEnumeration.Input input) {
        inFlight.add(completion.submit(() -> getOrCreateWorkerContext().worker().enumerate(input)));
    }

    private WorkerContext getOrCreateWorkerContext() {
        WorkerContext context = workerContext.get();
        if (context == null) {
            context = contextFactory.get();
            workerContext.set(context);
        }
        return context;
    }

    private void closeCurrentWorkerContext() {
        WorkerContext context = workerContext.get();
        workerContext.remove();
        if (context != null) {
            try {
                context.close();
            } catch (Exception | Error failure) {
                workerCloseFailures.add(failure);
            }
        }
    }

    private void workerThreadStarted() {
        synchronized (workerExitMonitor) {
            liveWorkerThreads++;
        }
    }

    private void workerThreadExited() {
        synchronized (workerExitMonitor) {
            liveWorkerThreads--;
            workerExitMonitor.notifyAll();
        }
    }

    private void awaitWorkerThreadExit() throws InterruptedException {
        synchronized (workerExitMonitor) {
            while (liveWorkerThreads > 0) {
                workerExitMonitor.wait();
            }
        }
    }

    private void awaitWorkerThreadExitUninterruptibly() {
        boolean interrupted = false;
        synchronized (workerExitMonitor) {
            while (liveWorkerThreads > 0) {
                try {
                    workerExitMonitor.wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Throwable drainWorkerCloseFailures(@Nullable Throwable primary) {
        Throwable failure = primary;
        Throwable workerCloseFailure;
        while ((workerCloseFailure = workerCloseFailures.poll()) != null) {
            failure = addSuppressed(failure, workerCloseFailure);
        }
        return failure;
    }

    private Throwable closeOwnedConnection(@Nullable Throwable primary) {
        Throwable failure = primary;
        if (ownedConnection != null) {
            try {
                ownedConnection.close();
            } catch (Exception | Error connectionCloseFailure) {
                failure = addSuppressed(failure, connectionCloseFailure);
            }
        }
        return failure;
    }

    private void startDeferredConnectionCleanup() {
        Thread cleanupThread =
                new Thread(
                        () -> {
                            boolean interrupted = false;
                            while (!executor.isTerminated()) {
                                try {
                                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                                } catch (InterruptedException ignored) {
                                    interrupted = true;
                                }
                            }
                            awaitWorkerThreadExitUninterruptibly();
                            Throwable deferredFailure = drainWorkerCloseFailures(null);
                            deferredFailure = closeOwnedConnection(deferredFailure);
                            if (deferredFailure != null) {
                                LOG.warn(
                                        "Failed to release deferred Scope target executor resources",
                                        deferredFailure);
                            }
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "fluss-orphan-scope-target-cleanup");
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    private void rethrowWorkerFailure(
            ExecutionException failure, Consumer<ScopeTargetEnumeration.Result> consumer)
            throws Exception {
        Throwable cause = failure.getCause();
        if (cause instanceof ScopeTargetEnumeration.EnumerationException) {
            ScopeTargetEnumeration.EnumerationException enumerationFailure =
                    (ScopeTargetEnumeration.EnumerationException) cause;
            try {
                consumer.accept(enumerationFailure.partialResult());
            } catch (Throwable replayFailure) {
                enumerationFailure.originalFailure().addSuppressed(replayFailure);
            }
            enumerationFailure.rethrowOriginal();
            return;
        }
        rethrow(cause);
    }

    private void cancelOutstanding() {
        for (Future<ScopeTargetEnumeration.Result> future : inFlight) {
            future.cancel(true);
        }
        inFlight.clear();
    }

    private static Throwable addSuppressed(@Nullable Throwable primary, Throwable suppressed) {
        if (primary == null) {
            return suppressed;
        }
        primary.addSuppressed(suppressed);
        return primary;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        throw new RuntimeException(failure);
    }

    static class WorkerContext implements AutoCloseable {
        @Nullable private final Admin admin;
        private final ScopeTargetEnumeration.Worker worker;

        private WorkerContext(@Nullable Admin admin, ScopeTargetEnumeration.Worker worker) {
            this.admin = admin;
            this.worker = worker;
        }

        ScopeTargetEnumeration.Worker worker() {
            return worker;
        }

        @Override
        public void close() throws Exception {
            if (admin != null) {
                admin.close();
            }
        }
    }

    private final class ScopeTargetThreadFactory implements ThreadFactory {
        private final AtomicInteger threadIndex = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread =
                    new Thread(
                            () -> {
                                workerThreadStarted();
                                try {
                                    runnable.run();
                                } finally {
                                    try {
                                        closeCurrentWorkerContext();
                                    } finally {
                                        workerThreadExited();
                                    }
                                }
                            },
                            "fluss-orphan-scope-target-" + threadIndex.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
