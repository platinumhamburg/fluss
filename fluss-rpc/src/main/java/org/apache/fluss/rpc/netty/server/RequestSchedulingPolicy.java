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

import java.util.Locale;

/** Named presets for independently composable waiting protection and candidate ranking. */
public enum RequestSchedulingPolicy implements RequestSchedulingStrategy {
    CONNECTION(Waiting.NONE, Ranking.ARRIVAL),
    CONFLICT_FIFO(Waiting.FIFO, Ranking.ARRIVAL),
    READY_FIFO(Waiting.NONE, Ranking.ARRIVAL),
    AGED_READY(Waiting.AGE, Ranking.ARRIVAL),
    LIMITED_BYPASS(Waiting.LIMITED_BYPASS, Ranking.ARRIVAL),
    EASY_BACKFILL(Waiting.EARLIEST_START, Ranking.ARRIVAL),
    CONSERVATIVE_BACKFILL(Waiting.ALL_STARTS, Ranking.ARRIVAL),
    SHORTEST_JOB(Waiting.NONE, Ranking.SHORTEST_JOB),
    EARLIEST_DEADLINE(Waiting.NONE, Ranking.EARLIEST_DEADLINE),
    BLOCKING_IMPACT(Waiting.NONE, Ranking.BLOCKING_IMPACT),
    SHORTEST_MAKESPAN(Waiting.NONE, Ranking.SHORTEST_MAKESPAN),
    VIRTUAL_TIME(Waiting.NONE, Ranking.VIRTUAL_TIME);

    /** Rules protecting earlier waiting requests. */
    public enum Waiting {
        NONE,
        FIFO,
        LIMITED_BYPASS,
        AGE,
        EARLIEST_START,
        ALL_STARTS
    }

    /** Rules ranking compatible candidate heads. */
    public enum Ranking {
        ARRIVAL,
        SHORTEST_JOB,
        EARLIEST_DEADLINE,
        BLOCKING_IMPACT,
        SHORTEST_MAKESPAN,
        VIRTUAL_TIME
    }

    private final Waiting waiting;
    private final Ranking ranking;

    RequestSchedulingPolicy(Waiting waiting, Ranking ranking) {
        this.waiting = waiting;
        this.ranking = ranking;
    }

    @Override
    public RequestSchedulingContext.Request select(RequestSchedulingContext context) {
        return RequestSchedulingRules.select(context, waiting, ranking);
    }

    /** Composes a waiting rule with a candidate-ranking rule. */
    public static RequestSchedulingStrategy compose(Waiting waiting, Ranking ranking) {
        return context -> RequestSchedulingRules.select(context, waiting, ranking);
    }

    /** Returns this preset with an explicit waiting-rule override. */
    public RequestSchedulingStrategy withWaiting(String rule) {
        return rule.isEmpty() ? this : compose(parseWaiting(rule), ranking);
    }

    /** Parses a named preset. */
    public static RequestSchedulingPolicy parse(String name) {
        return valueOf(normalize(name));
    }

    /** Parses a waiting rule for composition. */
    public static Waiting parseWaiting(String name) {
        return Waiting.valueOf(normalize(name));
    }

    private static String normalize(String name) {
        return name.replace('-', '_').toUpperCase(Locale.ROOT);
    }
}
