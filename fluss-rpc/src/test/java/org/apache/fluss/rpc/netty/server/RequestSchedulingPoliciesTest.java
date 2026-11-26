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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.fluss.rpc.netty.server.RequestSchedulingPolicy.Ranking;
import static org.apache.fluss.rpc.netty.server.RequestSchedulingPolicy.Waiting;
import static org.assertj.core.api.Assertions.assertThat;

class RequestSchedulingPoliciesTest {
    @Test
    void testAllRankingRulesChooseByTheirOwnInputs() {
        RequestSchedulingContext.Request older = request(0, 0, 5, Long.MAX_VALUE, 0, 0, 2, 20, 1);
        RequestSchedulingContext.Request shorter = request(1, 0, 2, 100, 0, 0, 2, 10, 2);
        RequestSchedulingContext context =
                context(Arrays.asList(older, shorter), Collections.emptyList());
        assertThat(RequestSchedulingPolicy.READY_FIFO.select(context)).isSameAs(older);
        assertThat(RequestSchedulingPolicy.SHORTEST_JOB.select(context)).isSameAs(shorter);
        assertThat(RequestSchedulingPolicy.EARLIEST_DEADLINE.select(context)).isSameAs(shorter);
        assertThat(RequestSchedulingPolicy.VIRTUAL_TIME.select(context)).isSameAs(shorter);
    }

    @Test
    void testDeadlineOrderingKeepsNanosecondPrecision() {
        long deadline = 1L << 55;
        RequestSchedulingContext.Request older = request(0, 0, 1, deadline + 1, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request sooner = request(1, 0, 1, deadline, 0, 0, 2, 0, 2);
        assertThat(
                        RequestSchedulingPolicy.EARLIEST_DEADLINE.select(
                                context(Arrays.asList(older, sooner), Collections.emptyList())))
                .isSameAs(sooner);
    }

    @Test
    void testLimitedBypassChangesEligibilityAtConfiguredCount() {
        RequestSchedulingContext.Request running = request(0, 0, 10, Long.MAX_VALUE, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request writer = request(1, 0, 3, Long.MAX_VALUE, 0, 0, 2, 0, 3);
        RequestSchedulingContext.Request shortRead =
                request(2, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 2);
        RequestSchedulingContext before =
                context(Arrays.asList(writer, shortRead), Arrays.asList(running));
        assertThat(RequestSchedulingPolicy.LIMITED_BYPASS.select(before)).isSameAs(shortRead);
        writer = request(1, 0, 3, Long.MAX_VALUE, 1, 0, 2, 0, 3);
        RequestSchedulingContext after =
                context(Arrays.asList(writer, shortRead), Arrays.asList(running));
        assertThat(RequestSchedulingPolicy.LIMITED_BYPASS.select(after)).isNull();
        assertThat(RequestSchedulingPolicy.READY_FIFO.select(after)).isSameAs(shortRead);
    }

    @Test
    void testAgeProtectionUsesTheWaitingAge() {
        RequestSchedulingContext.Request running = request(0, 0, 10, Long.MAX_VALUE, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request writer = request(1, 95, 3, Long.MAX_VALUE, 0, 0, 2, 0, 3);
        RequestSchedulingContext.Request shortRead =
                request(2, 99, 1, Long.MAX_VALUE, 0, 0, 2, 0, 2);
        List<RequestSchedulingContext.Request> waiting = Arrays.asList(writer, shortRead);
        RequestSchedulingContext before =
                new RequestSchedulingContext(waiting, waiting, Arrays.asList(running), 100, 10, 1);
        RequestSchedulingContext after =
                new RequestSchedulingContext(waiting, waiting, Arrays.asList(running), 105, 10, 1);
        assertThat(RequestSchedulingPolicy.AGED_READY.select(before)).isSameAs(shortRead);
        assertThat(RequestSchedulingPolicy.AGED_READY.select(after)).isNull();
    }

    @Test
    void testBackfillChecksDurationAndFutureWorkerCapacity() {
        RequestSchedulingContext.Request running = request(0, 0, 10, Long.MAX_VALUE, 0, 0, 1, 0, 1);
        RequestSchedulingContext.Request writer = request(1, 0, 3, Long.MAX_VALUE, 0, 1, 1, 0, 3);
        RequestSchedulingContext.Request shortRead =
                request(2, 0, 2, Long.MAX_VALUE, 0, 1, 1, 0, 2);
        RequestSchedulingContext.Request longUnrelated =
                request(3, 0, 20, Long.MAX_VALUE, 0, 1, 1, 0, 4);
        RequestSchedulingContext first =
                context(Arrays.asList(writer, shortRead), Arrays.asList(running));
        assertThat(RequestSchedulingPolicy.EASY_BACKFILL.select(first)).isSameAs(shortRead);
        RequestSchedulingContext second =
                context(Arrays.asList(writer, longUnrelated), Arrays.asList(running));
        assertThat(second.canRun(longUnrelated)).isTrue();
        assertThat(RequestSchedulingPolicy.EASY_BACKFILL.select(second)).isNull();
    }

    @Test
    void testConservativeBackfillAlsoProtectsLaterReservations() {
        RequestSchedulingContext.Request active = request(0, 0, 10, Long.MAX_VALUE, 0, 0, 3, 0, 1);
        RequestSchedulingContext.Request first = request(1, 0, 1, Long.MAX_VALUE, 0, 0, 3, 0, 1);
        RequestSchedulingContext.Request second = request(2, 0, 1, Long.MAX_VALUE, 0, 0, 3, 0, 2);
        RequestSchedulingContext.Request candidate =
                request(3, 0, 5, Long.MAX_VALUE, 0, 0, 3, 0, 2);
        List<RequestSchedulingContext.Request> waiting = Arrays.asList(first, second, candidate);
        RequestSchedulingContext view =
                new RequestSchedulingContext(
                        Arrays.asList(candidate), waiting, Arrays.asList(active), 100, 10, 1);
        assertThat(RequestSchedulingPolicy.EASY_BACKFILL.select(view)).isSameAs(candidate);
        assertThat(RequestSchedulingPolicy.CONSERVATIVE_BACKFILL.select(view)).isNull();
    }

    @Test
    void testBlockingImpactCountsUniqueTransitiveDependents() {
        RequestSchedulingContext.Request unrelated =
                request(0, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 8);
        RequestSchedulingContext.Request root = request(1, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request middle = request(2, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 3);
        RequestSchedulingContext.Request tail = request(3, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 2);
        assertThat(
                        RequestSchedulingPolicy.BLOCKING_IMPACT.select(
                                context(
                                        Arrays.asList(unrelated, root, middle, tail),
                                        Collections.emptyList())))
                .isSameAs(root);
    }

    @Test
    void testMakespanRankingTrialsTheResourceSchedule() {
        List<RequestSchedulingContext.Request> waiting =
                Arrays.asList(
                        request(0, 0, 5, Long.MAX_VALUE, 0, 0, 2, 0, 5),
                        request(1, 0, 6, Long.MAX_VALUE, 0, 0, 2, 0, 6),
                        request(2, 0, 9, Long.MAX_VALUE, 0, 0, 2, 0, 7),
                        request(3, 0, 8, Long.MAX_VALUE, 0, 0, 2, 0, 1),
                        request(4, 0, 4, Long.MAX_VALUE, 0, 0, 2, 0, 7));
        RequestSchedulingContext context = context(waiting, Collections.emptyList());
        assertThat(RequestSchedulePlanner.makespan(context, waiting.get(0))).isEqualTo(32);
        assertThat(RequestSchedulePlanner.makespan(context, waiting.get(2))).isEqualTo(26);
        assertThat(RequestSchedulingPolicy.SHORTEST_MAKESPAN.select(context))
                .isSameAs(waiting.get(2));
    }

    @Test
    void testAllThirtySixCombinationsRespectInFlightConflictsAndStableTies() {
        RequestSchedulingContext.Request active = request(0, 0, 10, Long.MAX_VALUE, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request blocked = request(1, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 1);
        RequestSchedulingContext.Request runnable = request(2, 0, 1, Long.MAX_VALUE, 0, 0, 2, 0, 2);
        RequestSchedulingContext context =
                context(Arrays.asList(blocked, runnable), Arrays.asList(active));
        for (Waiting waiting : Waiting.values()) {
            for (Ranking ranking : Ranking.values()) {
                assertThat(RequestSchedulingPolicy.compose(waiting, ranking).select(context))
                        .as("%s + %s", waiting, ranking)
                        .isSameAs(runnable);
            }
        }
    }

    @Test
    void testPlannerFitsAllResourceIntervalsAndWorkerLimits() {
        Random random = new Random(5285);
        for (int trial = 0; trial < 100; trial++) {
            List<RequestSchedulingContext.Request> requests = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                requests.add(
                        request(
                                i,
                                0,
                                1 + random.nextInt(8),
                                Long.MAX_VALUE,
                                0,
                                i % 2,
                                2,
                                0,
                                1 + random.nextInt(7)));
            }
            RequestSchedulingContext context = context(requests, Collections.emptyList());
            Map<Long, Long> starts =
                    RequestSchedulePlanner.starts(context, requests, requests.get(trial % 6));
            for (RequestSchedulingContext.Request first : requests) {
                long start = starts.get(first.sequence());
                long finish = start + first.estimatedNanos();
                for (long time = start; time < finish; time++) {
                    int workers = 0;
                    for (RequestSchedulingContext.Request other : requests) {
                        long otherStart = starts.get(other.sequence());
                        if (otherStart <= time && time < otherStart + other.estimatedNanos()) {
                            if (first.group() == other.group()) {
                                workers++;
                            }
                            if (first != other) {
                                assertThat(first.conflicts(other)).isFalse();
                            }
                        }
                    }
                    assertThat(workers).isLessThanOrEqualTo(first.workers());
                }
            }
        }
    }

    @Test
    void testPredictivePolicyUsesDocumentedFallbackAbovePlanningBudget() {
        List<RequestSchedulingContext.Request> requests =
                Arrays.asList(
                        request(0, 0, 5, Long.MAX_VALUE, 0, 0, 2, 0, 5),
                        request(1, 0, 6, Long.MAX_VALUE, 0, 0, 2, 0, 6),
                        request(2, 0, 9, Long.MAX_VALUE, 0, 0, 2, 0, 7),
                        request(3, 0, 8, Long.MAX_VALUE, 0, 0, 2, 0, 1),
                        request(4, 0, 4, Long.MAX_VALUE, 0, 0, 2, 0, 7));
        RequestSchedulingContext limited =
                new RequestSchedulingContext(
                        requests, requests, Collections.emptyList(), 100, 10, 1, 2);
        assertThat(RequestSchedulingPolicy.SHORTEST_MAKESPAN.select(limited))
                .isSameAs(requests.get(0));
        assertThat(
                        RequestSchedulingPolicy.SHORTEST_MAKESPAN.select(
                                context(requests, Collections.emptyList())))
                .isSameAs(requests.get(2));
    }

    private static RequestSchedulingContext context(
            List<RequestSchedulingContext.Request> waiting,
            List<RequestSchedulingContext.Request> running) {
        return new RequestSchedulingContext(waiting, waiting, running, 100, 10, 1);
    }

    private static RequestSchedulingContext.Request request(
            long sequence,
            long created,
            long estimate,
            long deadline,
            int bypasses,
            int group,
            int workers,
            double virtualTime,
            int mask) {
        BitSet writes = BitSet.valueOf(new long[] {mask});
        return new RequestSchedulingContext.Request(
                sequence,
                created,
                estimate,
                deadline,
                estimate,
                bypasses,
                group,
                workers,
                virtualTime,
                new BitSet(),
                writes);
    }
}
