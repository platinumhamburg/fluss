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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Server-scoped bitmap access registry shared by listener groups. All state uses {@link #lock}. */
final class RequestDispatchCoordinator {
    final ReentrantLock lock = new ReentrantLock();
    final RequestSchedulingStrategy policy;
    private final long protectionNanos;
    private final int bypassLimit;
    private final long initialEstimateNanos;
    private final LongSupplier clock;
    private final Map<Integer, Long> estimates = new HashMap<>();
    private int nextGroup;
    private final Map<Integer, Double> virtualFloors = new HashMap<>();
    private final int plannerMaxHeads;
    private final Map<TableBucket, Slot> slots = new HashMap<>();
    private final BitSet allocated = new BitSet();
    private final Map<Access, Boolean> requests = new LinkedHashMap<>();
    private final List<Runnable> wakeups = new ArrayList<>();
    private long sequence;

    RequestDispatchCoordinator(RequestSchedulingStrategy policy, long protectionNanos) {
        this(policy, protectionNanos, 1, 1_000_000L, System::nanoTime);
    }

    RequestDispatchCoordinator(
            RequestSchedulingStrategy policy,
            long protectionNanos,
            int bypassLimit,
            long initialEstimateNanos,
            LongSupplier clock) {
        this(policy, protectionNanos, bypassLimit, initialEstimateNanos, clock, 16);
    }

    RequestDispatchCoordinator(
            RequestSchedulingStrategy policy,
            long protectionNanos,
            int bypassLimit,
            long initialEstimateNanos,
            LongSupplier clock,
            int plannerMaxHeads) {
        checkArgument(plannerMaxHeads > 0, "Planning head limit must be positive");
        this.plannerMaxHeads = plannerMaxHeads;
        checkArgument(protectionNanos >= 0, "Negative scheduling protection interval");
        checkArgument(bypassLimit >= 0, "Negative bypass limit");
        checkArgument(initialEstimateNanos > 0, "Service estimate must be positive");
        this.policy = policy;
        this.protectionNanos = protectionNanos;
        this.bypassLimit = bypassLimit;
        this.initialEstimateNanos = initialEstimateNanos;
        this.clock = clock;
    }

    static RequestDispatchCoordinator fromConfiguration(Configuration configuration) {
        RequestSchedulingPolicy preset =
                RequestSchedulingPolicy.parse(
                        configuration.get(ConfigOptions.NETTY_SERVER_REQUEST_SCHEDULING_POLICY));
        String waiting = configuration.get(ConfigOptions.NETTY_SERVER_REQUEST_WAITING_POLICY);
        if (preset == RequestSchedulingPolicy.CONNECTION && waiting.isEmpty()) {
            return null;
        }
        return new RequestDispatchCoordinator(
                preset.withWaiting(waiting),
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                        configuration.get(
                                ConfigOptions.NETTY_SERVER_REQUEST_SCHEDULING_PROTECTION_MS)),
                configuration.get(ConfigOptions.NETTY_SERVER_REQUEST_BYPASS_LIMIT),
                java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(
                        configuration.get(ConfigOptions.NETTY_SERVER_REQUEST_INITIAL_ESTIMATE_US)),
                System::nanoTime,
                configuration.get(ConfigOptions.NETTY_SERVER_REQUEST_PLANNER_MAX_HEADS));
    }

    int newGroup() {
        lock.lock();
        try {
            return nextGroup++;
        } finally {
            lock.unlock();
        }
    }

    void addWakeup(Runnable wakeup) {
        lock.lock();
        try {
            wakeups.add(wakeup);
        } finally {
            lock.unlock();
        }
    }

    Access register(RpcRequest request, Source source, int group, int workers) {
        long now = clock.getAsLong();
        RequestSchedulingInfo info = request.schedulingInfo();
        int type = request.getRequestType().id;
        int estimateKey =
                request instanceof FlussRequest
                        ? 65536 + Short.toUnsignedInt(((FlussRequest) request).getApiKey())
                        : type;
        long estimate =
                info.estimatedNanos() > 0
                        ? info.estimatedNanos()
                        : estimates.getOrDefault(estimateKey, initialEstimateNanos);
        source.virtualTime = Math.max(source.virtualTime, virtualFloors.getOrDefault(group, 0.0));
        Access access =
                new Access(sequence++, now, estimate, info, source, group, workers, estimateKey);
        access.earlyRelease =
                RequestAccessProvider.describe(
                        request, (bucket, write) -> add(access, bucket, write));
        requests.put(access, false);
        return access;
    }

    private void add(Access access, TableBucket bucket, boolean write) {
        if (access.resources.containsKey(bucket)) {
            return;
        }
        Slot slot = slots.get(bucket);
        if (slot == null) {
            slot = new Slot(allocated.nextClearBit(0));
            allocated.set(slot.index);
            slots.put(bucket, slot);
        }
        slot.references++;
        access.resources.put(bucket, slot);
        (write ? access.writes : access.reads).set(slot.index);
    }

    Access select(List<Access> candidates) {
        long now = clock.getAsLong();
        List<RequestSchedulingContext.Request> waiting = new ArrayList<>();
        List<RequestSchedulingContext.Request> running = new ArrayList<>();
        List<RequestSchedulingContext.Request> local = new ArrayList<>();
        Map<Long, Access> bySequence = new HashMap<>();
        Set<Access> candidateSet = new HashSet<>(candidates);
        for (Map.Entry<Access, Boolean> entry : requests.entrySet()) {
            Access access = entry.getKey();
            boolean active = entry.getValue();
            long elapsed = active ? Math.max(0, now - access.startedNanos) : 0;
            RequestSchedulingContext.Request view =
                    new RequestSchedulingContext.Request(
                            access.sequence,
                            access.createdNanos,
                            access.estimateNanos,
                            access.info.deadlineNanos(),
                            Math.max(1, access.estimateNanos - elapsed),
                            access.bypasses,
                            access.group,
                            access.workers,
                            access.source.virtualTime,
                            access.reads,
                            access.writes);
            bySequence.put(access.sequence, access);
            (active ? running : waiting).add(view);
            if (candidateSet.contains(access)) {
                local.add(view);
            }
        }
        RequestSchedulingContext context =
                new RequestSchedulingContext(
                        local,
                        waiting,
                        running,
                        now,
                        protectionNanos,
                        bypassLimit,
                        plannerMaxHeads);
        RequestSchedulingContext.Request selected = policy.select(context);
        if (selected == null) {
            return null;
        }
        checkArgument(
                local.contains(selected) && context.canRun(selected),
                "Strategy selected an unavailable connection head");
        return bySequence.get(selected.sequence());
    }

    void claim(Access access) {
        for (Map.Entry<Access, Boolean> entry : requests.entrySet()) {
            Access waiting = entry.getKey();
            if (!entry.getValue()
                    && waiting.sequence < access.sequence
                    && conflicts(access, waiting)
                    && waiting.bypasses < Integer.MAX_VALUE) {
                waiting.bypasses++;
            }
        }
        access.startedNanos = clock.getAsLong();
        requests.put(access, true);
    }

    void releaseRead(Access access, TableBucket bucket) {
        lock.lock();
        try {
            Slot slot = access.resources.get(bucket);
            if (access.earlyRelease && slot != null && access.reads.get(slot.index)) {
                access.reads.clear(slot.index);
                wakeups.forEach(Runnable::run);
            }
        } finally {
            lock.unlock();
        }
    }

    void remove(Access access) {
        boolean ran = Boolean.TRUE.equals(requests.remove(access));
        if (ran) {
            long elapsed = Math.max(1, clock.getAsLong() - access.startedNanos);
            long old = estimates.getOrDefault(access.estimateKey, elapsed);
            estimates.put(access.estimateKey, old - old / 8 + elapsed / 8);
            access.source.virtualTime += elapsed / access.info.weight();
            double minimum = access.source.virtualTime;
            for (Access outstanding : requests.keySet()) {
                if (outstanding.group == access.group) {
                    minimum = Math.min(minimum, outstanding.source.virtualTime);
                }
            }
            virtualFloors.merge(access.group, minimum, Math::max);
        }
        access.resources.forEach(
                (bucket, slot) -> {
                    if (--slot.references == 0) {
                        slots.remove(bucket);
                        allocated.clear(slot.index);
                    }
                });
        wakeups.forEach(Runnable::run);
    }

    private static boolean conflicts(Access first, Access second) {
        return first.writes.intersects(second.reads)
                || first.writes.intersects(second.writes)
                || first.reads.intersects(second.writes);
    }

    static final class Access {
        final long sequence;
        final long createdNanos;
        boolean earlyRelease;
        final BitSet reads = new BitSet();
        final BitSet writes = new BitSet();
        final Map<TableBucket, Slot> resources = new HashMap<>();

        final long estimateNanos;
        final RequestSchedulingInfo info;
        final Source source;
        final int group;
        final int workers;
        final int estimateKey;
        long startedNanos;
        int bypasses;

        Access(
                long sequence,
                long createdNanos,
                long estimateNanos,
                RequestSchedulingInfo info,
                Source source,
                int group,
                int workers,
                int estimateKey) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
            this.estimateNanos = estimateNanos;
            this.info = info;
            this.source = source;
            this.group = group;
            this.workers = workers;
            this.estimateKey = estimateKey;
        }
    }

    static final class Source {
        private double virtualTime;
    }

    private static final class Slot {
        private final int index;
        private int references;

        private Slot(int index) {
            this.index = index;
        }
    }
}
