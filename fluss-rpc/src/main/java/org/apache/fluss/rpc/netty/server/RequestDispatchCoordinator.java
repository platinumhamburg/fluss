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

import org.apache.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Server-scoped bitmap access registry shared by listener groups. All state uses {@link #lock}. */
final class RequestDispatchCoordinator {
    final ReentrantLock lock = new ReentrantLock();
    final RequestSchedulingStrategy policy;
    private final long protectionNanos;
    private final Map<TableBucket, Slot> slots = new HashMap<>();
    private final BitSet allocated = new BitSet();
    private final Map<Access, Boolean> requests = new LinkedHashMap<>();
    private final List<Runnable> wakeups = new ArrayList<>();
    private long sequence;

    RequestDispatchCoordinator(RequestSchedulingStrategy policy, long protectionNanos) {
        checkArgument(protectionNanos >= 0, "Negative scheduling protection interval");
        this.policy = policy;
        this.protectionNanos = protectionNanos;
    }

    void addWakeup(Runnable wakeup) {
        lock.lock();
        try {
            wakeups.add(wakeup);
        } finally {
            lock.unlock();
        }
    }

    Access register(RpcRequest request) {
        Access access = new Access(sequence++, System.nanoTime());
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

    boolean eligible(Access candidate) {
        long now = System.nanoTime();
        for (Map.Entry<Access, Boolean> entry : requests.entrySet()) {
            Access other = entry.getKey();
            if (other == candidate || !conflicts(candidate, other)) {
                continue;
            }
            if (entry.getValue()) {
                return false;
            }
            boolean protect = policy.protects(now - other.createdNanos, protectionNanos);
            if (protect && policy.precedes(other.sequence, candidate.sequence)) {
                return false;
            }
        }
        return true;
    }

    void claim(Access access) {
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
        requests.remove(access);
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

        Access(long sequence, long createdNanos) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
        }
    }

    private static final class Slot {
        private final int index;
        private int references;

        private Slot(int index) {
            this.index = index;
        }
    }
}
