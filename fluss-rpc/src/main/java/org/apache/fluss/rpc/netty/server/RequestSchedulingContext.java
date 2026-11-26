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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/** Immutable decision snapshot for a non-preemptive, whole-RPC scheduler. */
public final class RequestSchedulingContext {
    private final List<Request> candidates;
    private final List<Request> waiting;
    private final List<Request> running;
    private final long nowNanos;
    private final long protectionNanos;
    private final int bypassLimit;
    private final int plannerMaxHeads;

    RequestSchedulingContext(
            List<Request> candidates,
            List<Request> waiting,
            List<Request> running,
            long nowNanos,
            long protectionNanos,
            int bypassLimit) {
        this(candidates, waiting, running, nowNanos, protectionNanos, bypassLimit, 16);
    }

    RequestSchedulingContext(
            List<Request> candidates,
            List<Request> waiting,
            List<Request> running,
            long nowNanos,
            long protectionNanos,
            int bypassLimit,
            int plannerMaxHeads) {
        this.plannerMaxHeads = plannerMaxHeads;
        this.candidates = Collections.unmodifiableList(new ArrayList<>(candidates));
        this.waiting = Collections.unmodifiableList(new ArrayList<>(waiting));
        this.running = Collections.unmodifiableList(new ArrayList<>(running));
        this.nowNanos = nowNanos;
        this.protectionNanos = protectionNanos;
        this.bypassLimit = bypassLimit;
    }

    /** Returns heads eligible for this worker's listener group, before conflict filtering. */
    public List<Request> candidates() {
        return candidates;
    }

    /** Returns visible waiting heads in server registration order, across listener groups. */
    public List<Request> waiting() {
        return waiting;
    }

    /** Returns synchronous requests already occupying workers. */
    public List<Request> running() {
        return running;
    }

    /** Returns the monotonic decision time. */
    public long nowNanos() {
        return nowNanos;
    }

    /** Returns the wait-age threshold. */
    public long protectionNanos() {
        return protectionNanos;
    }

    /** Returns the number of later conflicting claims permitted by bounded bypass. */
    public int bypassLimit() {
        return bypassLimit;
    }

    /** Returns the maximum visible request count permitted for predictive interval planning. */
    public int plannerMaxHeads() {
        return plannerMaxHeads;
    }

    /** Whether the request is compatible with all actual in-flight resource accesses. */
    public boolean canRun(Request candidate) {
        for (Request active : running) {
            if (candidate.conflicts(active)) {
                return false;
            }
        }
        return true;
    }

    /** A read-only candidate, including resource identity, cost and service history. */
    public static final class Request {
        private final long sequence;
        private final long createdNanos;
        private final long estimatedNanos;
        private final long deadlineNanos;
        private final long remainingNanos;
        private final int bypasses;
        private final int group;
        private final int workers;
        private final double virtualTime;
        private final BitSet reads;
        private final BitSet writes;

        Request(
                long sequence,
                long createdNanos,
                long estimatedNanos,
                long deadlineNanos,
                long remainingNanos,
                int bypasses,
                int group,
                int workers,
                double virtualTime,
                BitSet reads,
                BitSet writes) {
            this.sequence = sequence;
            this.createdNanos = createdNanos;
            this.estimatedNanos = estimatedNanos;
            this.deadlineNanos = deadlineNanos;
            this.remainingNanos = remainingNanos;
            this.bypasses = bypasses;
            this.group = group;
            this.workers = workers;
            this.virtualTime = virtualTime;
            this.reads = (BitSet) reads.clone();
            this.writes = (BitSet) writes.clone();
        }

        /** Returns the stable head-registration sequence. */
        public long sequence() {
            return sequence;
        }

        /** Returns when this head became visible. */
        public long createdNanos() {
            return createdNanos;
        }

        /** Returns predicted synchronous service time. */
        public long estimatedNanos() {
            return estimatedNanos;
        }

        /** Returns the trusted monotonic deadline, or MAX_VALUE. */
        public long deadlineNanos() {
            return deadlineNanos;
        }

        /** Returns predicted remaining service time for an in-flight request. */
        public long remainingNanos() {
            return remainingNanos;
        }

        /** Returns the number of later conflicting claims since registration. */
        public int bypasses() {
            return bypasses;
        }

        /** Returns the listener group identity. */
        public int group() {
            return group;
        }

        /** Returns the group's synchronous worker capacity. */
        public int workers() {
            return workers;
        }

        /** Returns normalized synchronous service consumed by this connection. */
        public double virtualTime() {
            return virtualTime;
        }

        /** Whether simultaneous access by these two requests is incompatible. */
        public boolean conflicts(Request other) {
            return writes.intersects(other.reads)
                    || writes.intersects(other.writes)
                    || reads.intersects(other.writes);
        }
    }
}
