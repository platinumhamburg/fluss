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
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/** Final pre-aggregated cleanup report; consumers need not aggregate raw file events. */
@Internal
public final class CleanupReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CleanupCounters global;
    private final Map<ScopeIdentity, TableCleanupSummary> tables;
    private final Map<String, CleanupCounters> databases;
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<SkipReasonCode, Long> bySkipReason;

    private CleanupReport(
            CleanupCounters global,
            Map<ScopeIdentity, TableCleanupSummary> tables,
            Map<String, CleanupCounters> databases,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> bySkipReason) {
        this.global = global;
        this.tables = new HashMap<>(tables);
        this.databases = new HashMap<>(databases);
        Map<CleanupObjectType, CleanupCounters> objectCopy = new HashMap<>();
        objectCopy.putAll(byObjectType);
        this.byObjectType = objectCopy;
        Map<SkipReasonCode, Long> reasonCopy = new HashMap<>();
        reasonCopy.putAll(bySkipReason);
        this.bySkipReason = reasonCopy;
    }

    public static CleanupReport aggregate(
            Collection<TablePlanStats> plans, Collection<CleanStats> stats, boolean dryRun) {
        Accumulator accumulator = accumulator(dryRun);
        for (TablePlanStats plan : plans) {
            accumulator.addPlan(plan);
        }
        for (CleanStats taskStats : stats) {
            accumulator.addStats(taskStats);
        }
        return accumulator.build();
    }

    public static Accumulator accumulator(boolean dryRun) {
        return new Accumulator(dryRun);
    }

    private static void mergeCounters(
            Map<CleanupObjectType, CleanupCounters> target,
            Map<CleanupObjectType, CleanupCounters> source) {
        for (Map.Entry<CleanupObjectType, CleanupCounters> entry : source.entrySet()) {
            target.put(
                    entry.getKey(),
                    target.getOrDefault(entry.getKey(), CleanupCounters.empty())
                            .add(entry.getValue()));
        }
    }

    private static void mergeReasons(
            Map<SkipReasonCode, Long> target, Map<SkipReasonCode, Long> source) {
        for (Map.Entry<SkipReasonCode, Long> entry : source.entrySet()) {
            target.put(entry.getKey(), target.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
    }

    public CleanupCounters global() {
        return global;
    }

    public TableCleanupSummary tableSummary(ScopeIdentity scope) {
        TableCleanupSummary summary = tables.get(scope.tableKey());
        if (summary == null) {
            throw new IllegalArgumentException("No table summary for scope " + scope.table());
        }
        return summary;
    }

    public Map<ScopeIdentity, TableCleanupSummary> tables() {
        return Collections.unmodifiableMap(tables);
    }

    public Map<String, CleanupCounters> databases() {
        return Collections.unmodifiableMap(databases);
    }

    public Map<CleanupObjectType, CleanupCounters> byObjectType() {
        return Collections.unmodifiableMap(byObjectType);
    }

    public Map<SkipReasonCode, Long> bySkipReason() {
        return Collections.unmodifiableMap(bySkipReason);
    }

    /** Incremental bounded-memory report aggregation for the final Flink operator. */
    public static final class Accumulator {
        private final boolean dryRun;
        private CleanupCounters global = CleanupCounters.empty();
        private final Map<ScopeIdentity, TableAccumulator> tables = new HashMap<>();
        private final Map<String, CleanupCounters> databases = new HashMap<>();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);

        private Accumulator(boolean dryRun) {
            this.dryRun = dryRun;
        }

        public void addPlan(TablePlanStats plan) {
            TableAccumulator table =
                    tables.computeIfAbsent(plan.scope(), ignored -> new TableAccumulator());
            mergeReasons(table.bySkipReason, plan.skipped());
            mergeReasons(bySkipReason, plan.skipped());
            if (!plan.scope().database().isEmpty()) {
                databases.putIfAbsent(plan.scope().database(), CleanupCounters.empty());
            }
        }

        public void addStats(CleanStats taskStats) {
            CleanupCounters counters = taskStats.counters();
            global = global.add(counters);
            ScopeIdentity tableKey = taskStats.scope().tableKey();
            TableAccumulator table =
                    tables.computeIfAbsent(tableKey, ignored -> new TableAccumulator());
            table.counters = table.counters.add(counters);
            mergeCounters(table.byObjectType, taskStats.byObjectType());
            mergeReasons(table.bySkipReason, taskStats.bySkipReason());
            if (!tableKey.database().isEmpty()) {
                databases.put(
                        tableKey.database(),
                        databases
                                .getOrDefault(tableKey.database(), CleanupCounters.empty())
                                .add(counters));
            }
            mergeCounters(byObjectType, taskStats.byObjectType());
            mergeReasons(bySkipReason, taskStats.bySkipReason());
        }

        public CleanupReport build() {
            if (dryRun
                    && (global.deletedFiles() != 0L
                            || global.bytesReclaimed() != 0L
                            || global.deleteFailures() != 0L)) {
                throw new IllegalStateException("dry-run report contains actual deletion counters");
            }
            Map<ScopeIdentity, TableCleanupSummary> summaries = new HashMap<>();
            for (Map.Entry<ScopeIdentity, TableAccumulator> entry : tables.entrySet()) {
                TableAccumulator table = entry.getValue();
                summaries.put(
                        entry.getKey(),
                        new TableCleanupSummary(
                                entry.getKey(),
                                table.counters,
                                table.byObjectType,
                                table.bySkipReason));
            }
            return new CleanupReport(global, summaries, databases, byObjectType, bySkipReason);
        }
    }

    private static final class TableAccumulator {
        private CleanupCounters counters = CleanupCounters.empty();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);
    }
}
