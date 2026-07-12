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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Emits per-subtask scan progress independently from a potentially blocking cleanup task. */
@Internal
final class ScanHeartbeat implements AutoCloseable {

    private final int subtask;
    private final int parallelism;
    private final int attempt;
    private final LongSupplier clock;
    private final Consumer<Snapshot> logger;
    @Nullable private final ScheduledExecutorService scheduler;
    private volatile CompletedState completed = CompletedState.empty();
    @Nullable private volatile Target currentTarget;

    ScanHeartbeat(
            Duration interval,
            int subtask,
            int parallelism,
            int attempt,
            Consumer<Snapshot> logger) {
        this(
                subtask,
                parallelism,
                attempt,
                System::currentTimeMillis,
                logger,
                createScheduler(interval));
        if (scheduler != null) {
            scheduler.scheduleAtFixedRate(
                    this::emitSafelyNow,
                    interval.toMillis(),
                    interval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    ScanHeartbeat(
            int subtask,
            int parallelism,
            int attempt,
            LongSupplier clock,
            Consumer<Snapshot> logger) {
        this(subtask, parallelism, attempt, clock, logger, null);
    }

    private ScanHeartbeat(
            int subtask,
            int parallelism,
            int attempt,
            LongSupplier clock,
            Consumer<Snapshot> logger,
            @Nullable ScheduledExecutorService scheduler) {
        this.subtask = subtask;
        this.parallelism = parallelism;
        this.attempt = attempt;
        this.clock = clock;
        this.logger = logger;
        this.scheduler = scheduler;
    }

    @Nullable
    private static ScheduledExecutorService createScheduler(Duration interval) {
        if (interval.isZero()) {
            return null;
        }
        ThreadFactory factory =
                runnable -> {
                    Thread thread = new Thread(runnable, "fluss-orphan-scan-heartbeat");
                    thread.setDaemon(true);
                    return thread;
                };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }

    void taskStart(ScopeIdentity scope) {
        currentTarget = new Target(scope, clock.getAsLong());
    }

    void taskComplete(CleanStats stats) {
        CompletedState current = completed;
        completed =
                new CompletedState(
                        current.tasksCompleted + 1L, current.counters.add(stats.counters()));
        currentTarget = null;
    }

    void taskFailed() {
        currentTarget = null;
    }

    void emitNow() {
        Target target = currentTarget;
        CompletedState completedSnapshot = completed;
        long elapsedMillis =
                target == null ? 0L : Math.max(0L, clock.getAsLong() - target.startedMillis);
        logger.accept(
                new Snapshot(
                        subtask,
                        parallelism,
                        attempt,
                        completedSnapshot.tasksCompleted,
                        completedSnapshot.counters,
                        target == null ? null : target.scope,
                        elapsedMillis));
    }

    void emitSafelyNow() {
        try {
            emitNow();
        } catch (Throwable ignored) {
            // Observability must never fail or interrupt the cleanup job.
        }
    }

    boolean isScheduled() {
        return scheduler != null;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private static final class Target {
        private final ScopeIdentity scope;
        private final long startedMillis;

        private Target(ScopeIdentity scope, long startedMillis) {
            this.scope = scope;
            this.startedMillis = startedMillis;
        }
    }

    private static final class CompletedState {
        private final long tasksCompleted;
        private final CleanupCounters counters;

        private CompletedState(long tasksCompleted, CleanupCounters counters) {
            this.tasksCompleted = tasksCompleted;
            this.counters = counters;
        }

        private static CompletedState empty() {
            return new CompletedState(0L, CleanupCounters.empty());
        }
    }

    static final class Snapshot {
        private final int subtask;
        private final int parallelism;
        private final int attempt;
        private final long tasksCompleted;
        private final CleanupCounters counters;
        @Nullable private final ScopeIdentity currentScope;
        private final long currentTaskElapsedMillis;

        private Snapshot(
                int subtask,
                int parallelism,
                int attempt,
                long tasksCompleted,
                CleanupCounters counters,
                @Nullable ScopeIdentity currentScope,
                long currentTaskElapsedMillis) {
            this.subtask = subtask;
            this.parallelism = parallelism;
            this.attempt = attempt;
            this.tasksCompleted = tasksCompleted;
            this.counters = counters;
            this.currentScope = currentScope;
            this.currentTaskElapsedMillis = currentTaskElapsedMillis;
        }

        int subtask() {
            return subtask;
        }

        int parallelism() {
            return parallelism;
        }

        int attempt() {
            return attempt;
        }

        long tasksCompleted() {
            return tasksCompleted;
        }

        CleanupCounters counters() {
            return counters;
        }

        @Nullable
        ScopeIdentity currentScope() {
            return currentScope;
        }

        long currentTaskElapsedMillis() {
            return currentTaskElapsedMillis;
        }
    }
}
