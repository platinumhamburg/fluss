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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Selection rules evaluated after the immutable framework snapshot is constructed. */
final class RequestSchedulingRules {
    private RequestSchedulingRules() {}

    static RequestSchedulingContext.Request select(
            RequestSchedulingContext context,
            RequestSchedulingPolicy.Waiting protection,
            RequestSchedulingPolicy.Ranking ranking) {
        boolean planning =
                protection == RequestSchedulingPolicy.Waiting.EARLIEST_START
                        || protection == RequestSchedulingPolicy.Waiting.ALL_STARTS
                        || ranking == RequestSchedulingPolicy.Ranking.SHORTEST_MAKESPAN;
        if (planning
                && context.waiting().size() + context.running().size()
                        > context.plannerMaxHeads()) {
            return planningFallback(context);
        }
        RequestSchedulingContext.Request selected = null;
        long best = Long.MAX_VALUE;
        Map<Long, Integer> impact =
                ranking == RequestSchedulingPolicy.Ranking.BLOCKING_IMPACT
                        ? dependencyWeights(context)
                        : Collections.emptyMap();
        for (RequestSchedulingContext.Request candidate : context.candidates()) {
            if (!context.canRun(candidate) || !allowed(context, candidate, protection)) {
                continue;
            }
            long score = score(context, candidate, ranking, impact);
            int order =
                    ranking == RequestSchedulingPolicy.Ranking.VIRTUAL_TIME && selected != null
                            ? Double.compare(candidate.virtualTime(), selected.virtualTime())
                            : Long.compare(score, best);
            if (selected == null
                    || order < 0
                    || (order == 0 && candidate.sequence() < selected.sequence())) {
                selected = candidate;
                best = score;
            }
        }
        return selected;
    }

    private static RequestSchedulingContext.Request planningFallback(
            RequestSchedulingContext context) {
        // With no room to trial resource intervals, only the group's oldest head may start.
        // Cross-group conflicting predecessors retain FIFO protection as well.
        for (RequestSchedulingContext.Request candidate : context.candidates()) {
            boolean oldest = true;
            for (RequestSchedulingContext.Request earlier : context.waiting()) {
                if (earlier.sequence() >= candidate.sequence()) {
                    break;
                }
                if (earlier.group() == candidate.group()) {
                    oldest = false;
                    break;
                }
            }
            if (oldest
                    && context.canRun(candidate)
                    && allowed(context, candidate, RequestSchedulingPolicy.Waiting.FIFO)) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean allowed(
            RequestSchedulingContext context,
            RequestSchedulingContext.Request candidate,
            RequestSchedulingPolicy.Waiting protection) {
        if (protection == RequestSchedulingPolicy.Waiting.EARLIEST_START
                || protection == RequestSchedulingPolicy.Waiting.ALL_STARTS) {
            return preservesStarts(context, candidate, protection);
        }
        for (RequestSchedulingContext.Request earlier : context.waiting()) {
            if (earlier.sequence() >= candidate.sequence()) {
                break;
            }
            if (!earlier.conflicts(candidate)) {
                continue;
            }
            boolean protectedHead =
                    protection == RequestSchedulingPolicy.Waiting.FIFO
                            || (protection == RequestSchedulingPolicy.Waiting.LIMITED_BYPASS
                                    && earlier.bypasses() >= context.bypassLimit())
                            || (protection == RequestSchedulingPolicy.Waiting.AGE
                                    && context.nowNanos() - earlier.createdNanos()
                                            >= context.protectionNanos());
            if (protectedHead) {
                return false;
            }
        }
        return true;
    }

    private static boolean preservesStarts(
            RequestSchedulingContext context,
            RequestSchedulingContext.Request candidate,
            RequestSchedulingPolicy.Waiting protection) {
        List<RequestSchedulingContext.Request> earlier = new ArrayList<>();
        for (RequestSchedulingContext.Request request : context.waiting()) {
            if (request.sequence() >= candidate.sequence()) {
                break;
            }
            earlier.add(request);
            if (protection == RequestSchedulingPolicy.Waiting.EARLIEST_START) {
                break;
            }
        }
        if (earlier.isEmpty()) {
            return true;
        }
        Map<Long, Long> original = RequestSchedulePlanner.starts(context, earlier, null);
        Map<Long, Long> advanced = RequestSchedulePlanner.starts(context, earlier, candidate);
        for (RequestSchedulingContext.Request request : earlier) {
            if (advanced.get(request.sequence()) > original.get(request.sequence())) {
                return false;
            }
        }
        return true;
    }

    private static long score(
            RequestSchedulingContext context,
            RequestSchedulingContext.Request request,
            RequestSchedulingPolicy.Ranking ranking,
            Map<Long, Integer> impact) {
        switch (ranking) {
            case SHORTEST_JOB:
                return request.estimatedNanos();
            case EARLIEST_DEADLINE:
                return request.deadlineNanos();
            case SHORTEST_MAKESPAN:
                return RequestSchedulePlanner.makespan(context, request);
            case BLOCKING_IMPACT:
                return -impact.get(request.sequence());
            default:
                return 0;
        }
    }

    private static Map<Long, Integer> dependencyWeights(RequestSchedulingContext context) {
        List<RequestSchedulingContext.Request> waiting = context.waiting();
        BitSet[] descendants = new BitSet[waiting.size()];
        Map<Long, Integer> weights = new HashMap<>();
        for (int i = waiting.size() - 1; i >= 0; i--) {
            BitSet reachable = new BitSet(waiting.size());
            for (int j = i + 1; j < waiting.size(); j++) {
                if (waiting.get(i).conflicts(waiting.get(j))) {
                    reachable.set(j);
                    reachable.or(descendants[j]);
                }
            }
            descendants[i] = reachable;
            weights.put(waiting.get(i).sequence(), reachable.cardinality());
        }
        return weights;
    }
}
