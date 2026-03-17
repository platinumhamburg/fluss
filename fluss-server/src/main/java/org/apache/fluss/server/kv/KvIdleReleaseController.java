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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Periodically checks OPEN KvTablets and releases the coldest ones back to LAZY state. Uses LRU
 * (least recently accessed) ordering. Operates directly on {@link KvTablet} — no dependency on
 * Replica layer.
 */
public class KvIdleReleaseController implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KvIdleReleaseController.class);

    private final ScheduledExecutorService scheduler;
    private final Supplier<Collection<KvTablet>> tabletSupplier;
    private final Clock clock;

    private final long checkIntervalMs;
    private final long idleIntervalMs;
    private final int maxOpenCount;
    private final int maxClosePerRound;

    private final @Nullable Counter idleReleaseTotalCounter;

    private volatile ScheduledFuture<?> scheduledTask;

    public KvIdleReleaseController(
            ScheduledExecutorService scheduler,
            Supplier<Collection<KvTablet>> tabletSupplier,
            Clock clock,
            long checkIntervalMs,
            long idleIntervalMs,
            int maxOpenCount,
            int maxClosePerRound,
            @Nullable Counter idleReleaseTotalCounter) {
        this.scheduler = scheduler;
        this.tabletSupplier = tabletSupplier;
        this.clock = clock;
        this.checkIntervalMs = checkIntervalMs;
        this.idleIntervalMs = idleIntervalMs;
        this.maxOpenCount = maxOpenCount;
        this.maxClosePerRound = maxClosePerRound;
        this.idleReleaseTotalCounter = idleReleaseTotalCounter;
    }

    public void start() {
        scheduledTask =
                scheduler.scheduleWithFixedDelay(
                        this::checkAndRelease,
                        checkIntervalMs,
                        checkIntervalMs,
                        TimeUnit.MILLISECONDS);
        LOG.info(
                "KvIdleReleaseController started: checkInterval={}ms, idleInterval={}ms, "
                        + "maxOpen={}, maxClosePerRound={}",
                checkIntervalMs,
                idleIntervalMs,
                maxOpenCount,
                maxClosePerRound);
    }

    @VisibleForTesting
    void checkAndRelease() {
        try {
            Collection<KvTablet> tablets = tabletSupplier.get();

            int openCount = tablets.size();
            int overLimit = Math.max(0, openCount - maxOpenCount);

            // Quick check: if under limit, scan for any idle tablet before sorting
            long now = clock.milliseconds();
            if (overLimit == 0) {
                boolean hasIdle = false;
                for (KvTablet t : tablets) {
                    if (now - t.getLastAccessTimestamp() > idleIntervalMs) {
                        hasIdle = true;
                        break;
                    }
                }
                if (!hasIdle) {
                    return;
                }
            }

            // Sort by coldest first (LRU)
            List<ReleaseCandidate> candidates =
                    tablets.stream()
                            .map(t -> new ReleaseCandidate(t, t.getLastAccessTimestamp()))
                            .sorted(Comparator.comparingLong(c -> c.lastAccessTimestamp))
                            .collect(Collectors.toList());

            int idleCount = 0;
            for (ReleaseCandidate c : candidates) {
                if (now - c.lastAccessTimestamp > idleIntervalMs) {
                    idleCount++;
                } else {
                    break;
                }
            }

            int releaseTarget = Math.min(Math.max(overLimit, idleCount), maxClosePerRound);
            if (releaseTarget == 0) {
                return;
            }

            LOG.info(
                    "Idle release round: open={}, overLimit={}, idle={}, target={}",
                    openCount,
                    overLimit,
                    idleCount,
                    releaseTarget);

            int released = 0;
            for (int i = 0; i < candidates.size() && released < releaseTarget; i++) {
                ReleaseCandidate candidate = candidates.get(i);
                KvTablet tablet = candidate.tablet;

                long idleThreshold = i < idleCount ? idleIntervalMs : 0;
                if (tablet.canRelease(idleThreshold, now)) {
                    try {
                        boolean success = tablet.releaseKv();
                        if (success) {
                            released++;
                            if (idleReleaseTotalCounter != null) {
                                idleReleaseTotalCounter.inc();
                            }
                            LOG.debug(
                                    "Released KvTablet for {} (idle {}ms)",
                                    tablet.getTableBucket(),
                                    now - candidate.lastAccessTimestamp);
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to release KvTablet for {}", tablet.getTableBucket(), e);
                    }
                }
            }

            LOG.info("Idle release round complete: released={}/{}", released, releaseTarget);
        } catch (Exception e) {
            LOG.error("Error during idle release check", e);
        }
    }

    @Override
    public void close() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
    }

    private static class ReleaseCandidate {
        final KvTablet tablet;
        final long lastAccessTimestamp;

        ReleaseCandidate(KvTablet tablet, long lastAccessTimestamp) {
            this.tablet = tablet;
            this.lastAccessTimestamp = lastAccessTimestamp;
        }
    }
}
