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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Predicts non-preemptive resource intervals and listener-group worker occupancy. */
final class RequestSchedulePlanner {
    private RequestSchedulePlanner() {}

    static Map<Long, Long> starts(
            RequestSchedulingContext context,
            List<RequestSchedulingContext.Request> waiting,
            RequestSchedulingContext.Request forced) {
        List<Interval> reservations = new ArrayList<>();
        for (RequestSchedulingContext.Request running : context.running()) {
            reservations.add(new Interval(running, 0, running.remainingNanos()));
        }
        Map<Long, Long> starts = new LinkedHashMap<>();
        if (forced != null) {
            reservations.add(new Interval(forced, 0, forced.estimatedNanos()));
            starts.put(forced.sequence(), 0L);
        }
        for (RequestSchedulingContext.Request request : waiting) {
            if (request == forced) {
                continue;
            }
            long start = earliest(request, reservations);
            reservations.add(new Interval(request, start, add(start, request.estimatedNanos())));
            starts.put(request.sequence(), start);
        }
        return starts;
    }

    static long makespan(
            RequestSchedulingContext context, RequestSchedulingContext.Request forced) {
        Map<Long, Long> starts = starts(context, context.waiting(), forced);
        long finish = 0;
        for (RequestSchedulingContext.Request running : context.running()) {
            finish = Math.max(finish, running.remainingNanos());
        }
        for (RequestSchedulingContext.Request request : context.waiting()) {
            finish =
                    Math.max(finish, add(starts.get(request.sequence()), request.estimatedNanos()));
        }
        return finish;
    }

    private static long earliest(RequestSchedulingContext.Request request, List<Interval> plan) {
        long start = 0;
        while (true) {
            long end = add(start, request.estimatedNanos());
            long next = Long.MAX_VALUE;
            boolean fits = true;
            for (Interval interval : plan) {
                if (!interval.overlaps(start, end)) {
                    continue;
                }
                next = Math.min(next, interval.end);
                if (request.conflicts(interval.request)) {
                    fits = false;
                }
                if (interval.request.group() == request.group()
                        && occupied(request.group(), Math.max(start, interval.start), plan)
                                >= request.workers()) {
                    fits = false;
                }
            }
            if (fits) {
                return start;
            }
            if (next <= start || next == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            start = next;
        }
    }

    private static int occupied(int group, long time, List<Interval> plan) {
        int count = 0;
        for (Interval interval : plan) {
            if (interval.request.group() == group
                    && interval.start <= time
                    && time < interval.end) {
                count++;
            }
        }
        return count;
    }

    static long add(long first, long second) {
        return first > Long.MAX_VALUE - second ? Long.MAX_VALUE : first + second;
    }

    private static final class Interval {
        private final RequestSchedulingContext.Request request;
        private final long start;
        private final long end;

        private Interval(RequestSchedulingContext.Request request, long start, long end) {
            this.request = request;
            this.start = start;
            this.end = end;
        }

        private boolean overlaps(long from, long to) {
            return start < to && from < end;
        }
    }
}
