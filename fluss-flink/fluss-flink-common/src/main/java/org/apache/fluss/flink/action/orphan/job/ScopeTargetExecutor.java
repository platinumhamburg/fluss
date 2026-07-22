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

    private static final long CLOSE_TIMEOUT_SECONDS = 10L;

    private final int concurrency;
    private final ExecutorService executor;
    private final ExecutorCompletionService<ScopeTargetEnumeration.Result> completion;
    private final Set<Future<ScopeTargetEnumeration.Result>> inFlight =
            new HashSet<Future<ScopeTargetEnumeration.Result>>();
    private final Queue<WorkerContext> contexts = new ConcurrentLinkedQueue<WorkerContext>();
    private final ThreadLocal<WorkerContext> workerContext;

    private boolean processingFailed;
    private boolean closed;

    private ScopeTargetExecutor(int concurrency, Supplier<WorkerContext> contextFactory) {
        checkArgument(concurrency > 0, "concurrency must be greater than 0");
        this.concurrency = concurrency;
        this.executor = Executors.newFixedThreadPool(concurrency, new ScopeTargetThreadFactory());
        this.completion = new ExecutorCompletionService<ScopeTargetEnumeration.Result>(executor);
        this.workerContext =
                ThreadLocal.withInitial(
                        () -> {
                            WorkerContext context = contextFactory.get();
                            contexts.add(context);
                            return context;
                        });
    }

    static ScopeTargetExecutor create(
            Connection connection, int concurrency, RateLimiter remoteFsOpRateLimiter) {
        return create(
                connection,
                concurrency,
                admin -> ScopeTargetEnumeration.worker(admin, remoteFsOpRateLimiter));
    }

    static ScopeTargetExecutor create(
            Connection connection,
            int concurrency,
            Function<Admin, ScopeTargetEnumeration.Worker> workerFactory) {
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
                });
    }

    static ScopeTargetExecutor testing(
            int concurrency, Supplier<ScopeTargetEnumeration.Worker> workerFactory) {
        return new ScopeTargetExecutor(
                concurrency, () -> new WorkerContext(null, workerFactory.get()));
    }

    static ScopeTargetExecutor testingContexts(
            int concurrency, Supplier<WorkerContext> contextFactory) {
        return new ScopeTargetExecutor(concurrency, contextFactory);
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
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                closeFailure = new IOException("Timed out closing Scope target executor");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            closeFailure = failure;
        }

        for (WorkerContext context : contexts) {
            try {
                context.close();
            } catch (Exception | Error failure) {
                closeFailure = addSuppressed(closeFailure, failure);
            }
        }
        if (closeFailure != null) {
            rethrow(closeFailure);
        }
    }

    private void submit(ScopeTargetEnumeration.Input input) {
        inFlight.add(completion.submit(() -> workerContext.get().worker().enumerate(input)));
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

    private static final class ScopeTargetThreadFactory implements ThreadFactory {
        private final AtomicInteger threadIndex = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread =
                    new Thread(
                            runnable, "fluss-orphan-scope-target-" + threadIndex.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
