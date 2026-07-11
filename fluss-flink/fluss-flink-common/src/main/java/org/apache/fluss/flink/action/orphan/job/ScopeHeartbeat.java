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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Emits scope progress independently from potentially blocking remote-filesystem calls. */
@Internal
final class ScopeHeartbeat implements AutoCloseable {

    private final ScopePlanStats stats;
    private final LongSupplier clock;
    private final Consumer<Snapshot> logger;
    @Nullable private final ScheduledExecutorService scheduler;
    private final AtomicLong completedTargets = new AtomicLong();

    private volatile String phase = "initializing";
    private volatile long totalTargets;
    @Nullable private volatile Target currentTarget;

    ScopeHeartbeat(Duration interval, ScopePlanStats stats, Consumer<Snapshot> logger) {
        this(stats, System::currentTimeMillis, logger, createScheduler(interval));
        if (scheduler != null) {
            scheduler.scheduleAtFixedRate(
                    this::emitSafely,
                    interval.toMillis(),
                    interval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    ScopeHeartbeat(ScopePlanStats stats, LongSupplier clock, Consumer<Snapshot> logger) {
        this(stats, clock, logger, null);
    }

    private ScopeHeartbeat(
            ScopePlanStats stats,
            LongSupplier clock,
            Consumer<Snapshot> logger,
            @Nullable ScheduledExecutorService scheduler) {
        this.stats = stats;
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
                    Thread thread = new Thread(runnable, "fluss-orphan-scope-heartbeat");
                    thread.setDaemon(true);
                    return thread;
                };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }

    void phase(String phase) {
        this.phase = phase;
    }

    void totalTargets(long totalTargets) {
        this.totalTargets = totalTargets;
    }

    void targetStart(String database, String table, long tableId, @Nullable Long partitionId) {
        currentTarget = new Target(database, table, tableId, partitionId, clock.getAsLong());
    }

    void targetComplete() {
        completedTargets.incrementAndGet();
        currentTarget = null;
    }

    void emitNow() {
        Target target = currentTarget;
        long elapsedMillis =
                target == null ? 0L : Math.max(0L, clock.getAsLong() - target.startedMillis);
        logger.accept(
                new Snapshot(
                        phase,
                        completedTargets.get(),
                        totalTargets,
                        target == null ? null : target.database,
                        target == null ? null : target.table,
                        target == null ? null : target.tableId,
                        target == null ? null : target.partitionId,
                        elapsedMillis,
                        stats));
    }

    private void emitSafely() {
        try {
            emitNow();
        } catch (Throwable ignored) {
            // Observability must never fail or interrupt the cleanup job.
        }
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private static final class Target {
        private final String database;
        private final String table;
        private final long tableId;
        @Nullable private final Long partitionId;
        private final long startedMillis;

        private Target(
                String database,
                String table,
                long tableId,
                @Nullable Long partitionId,
                long startedMillis) {
            this.database = database;
            this.table = table;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.startedMillis = startedMillis;
        }
    }

    static final class Snapshot {
        private final String phase;
        private final long completedTargets;
        private final long totalTargets;
        @Nullable private final String database;
        @Nullable private final String table;
        @Nullable private final Long tableId;
        @Nullable private final Long partitionId;
        private final long targetElapsedMillis;
        private final ScopePlanStats stats;

        private Snapshot(
                String phase,
                long completedTargets,
                long totalTargets,
                @Nullable String database,
                @Nullable String table,
                @Nullable Long tableId,
                @Nullable Long partitionId,
                long targetElapsedMillis,
                ScopePlanStats stats) {
            this.phase = phase;
            this.completedTargets = completedTargets;
            this.totalTargets = totalTargets;
            this.database = database;
            this.table = table;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.targetElapsedMillis = targetElapsedMillis;
            this.stats = stats;
        }

        String phase() {
            return phase;
        }

        long completedTargets() {
            return completedTargets;
        }

        long totalTargets() {
            return totalTargets;
        }

        @Nullable
        String database() {
            return database;
        }

        @Nullable
        String table() {
            return table;
        }

        @Nullable
        Long tableId() {
            return tableId;
        }

        @Nullable
        Long partitionId() {
            return partitionId;
        }

        long targetElapsedMillis() {
            return targetElapsedMillis;
        }

        ScopePlanStats stats() {
            return stats;
        }
    }
}
