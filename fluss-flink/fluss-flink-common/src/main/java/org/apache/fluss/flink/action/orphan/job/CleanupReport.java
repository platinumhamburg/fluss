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
    private final long tasksPlanned;
    private final long metadataFailures;
    private final Map<ScopeIdentity, TableCleanupSummary> tables;
    private final Map<String, CleanupCounters> databases;
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<SkipReasonCode, Long> bySkipReason;
    private final Map<CleanupObjectType, RuleDecisionCounters> byRuleDecision;

    private CleanupReport(
            CleanupCounters global,
            long tasksPlanned,
            long metadataFailures,
            Map<ScopeIdentity, TableCleanupSummary> tables,
            Map<String, CleanupCounters> databases,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> bySkipReason,
            Map<CleanupObjectType, RuleDecisionCounters> byRuleDecision) {
        this.global = global;
        this.tasksPlanned = tasksPlanned;
        this.metadataFailures = metadataFailures;
        this.tables = new HashMap<>(tables);
        this.databases = new HashMap<>(databases);
        Map<CleanupObjectType, CleanupCounters> objectCopy = new HashMap<>();
        objectCopy.putAll(byObjectType);
        this.byObjectType = objectCopy;
        Map<SkipReasonCode, Long> reasonCopy = new HashMap<>();
        reasonCopy.putAll(bySkipReason);
        this.bySkipReason = reasonCopy;
        Map<CleanupObjectType, RuleDecisionCounters> decisionCopy = new HashMap<>();
        decisionCopy.putAll(byRuleDecision);
        this.byRuleDecision = decisionCopy;
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

    private static void mergeRuleDecisions(
            Map<CleanupObjectType, RuleDecisionCounters> target,
            Map<CleanupObjectType, RuleDecisionCounters> source) {
        for (Map.Entry<CleanupObjectType, RuleDecisionCounters> entry : source.entrySet()) {
            target.put(
                    entry.getKey(),
                    target.getOrDefault(entry.getKey(), RuleDecisionCounters.empty())
                            .add(entry.getValue()));
        }
    }

    public CleanupCounters global() {
        return global;
    }

    public long tasksPlanned() {
        return tasksPlanned;
    }

    public long metadataFailures() {
        return metadataFailures;
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

    public Map<CleanupObjectType, RuleDecisionCounters> byRuleDecision() {
        return Collections.unmodifiableMap(byRuleDecision);
    }

    public long mtimeUnavailableFiles() {
        return sumMtimeUnavailableFiles(byRuleDecision.values());
    }

    public long mtimeUnavailableBytes() {
        long total = 0L;
        for (RuleDecisionCounters counters : byRuleDecision.values()) {
            total += counters.mtimeUnavailableBytes();
        }
        return total;
    }

    public long mtimeUnavailableDirs() {
        return bySkipReason.getOrDefault(SkipReasonCode.MTIME_UNAVAILABLE, 0L)
                - mtimeUnavailableFiles();
    }

    public boolean ruleCountersConsistent() {
        if (bySkipReason.getOrDefault(SkipReasonCode.MTIME_UNAVAILABLE, 0L)
                < mtimeUnavailableFiles()) {
            return false;
        }
        for (TableCleanupSummary table : tables.values()) {
            if (!table.mtimeCountersConsistent()) {
                return false;
            }
        }
        for (Map.Entry<CleanupObjectType, RuleDecisionCounters> entry : byRuleDecision.entrySet()) {
            RuleDecisionCounters decisions = entry.getValue();
            CleanupCounters counters =
                    byObjectType.getOrDefault(entry.getKey(), CleanupCounters.empty());
            if (!decisions.isConsistent()
                    || decisions.candidateFiles() != counters.plannedFiles()
                    || decisions.candidateBytes() != counters.plannedBytes()) {
                return false;
            }
        }
        for (Map.Entry<CleanupObjectType, CleanupCounters> entry : byObjectType.entrySet()) {
            if (entry.getValue().plannedFiles() > 0L
                    && !byRuleDecision.containsKey(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    private static long sumMtimeUnavailableFiles(
            Collection<RuleDecisionCounters> decisionCounters) {
        long total = 0L;
        for (RuleDecisionCounters counters : decisionCounters) {
            total += counters.mtimeUnavailableFiles();
        }
        return total;
    }

    public boolean coverageComplete() {
        if (metadataFailures > 0L) {
            return false;
        }
        for (Map.Entry<SkipReasonCode, Long> entry : bySkipReason.entrySet()) {
            if (entry.getValue() > 0L && entry.getKey().actionRequired()) {
                return false;
            }
        }
        return true;
    }

    /** Incremental bounded-memory report aggregation for the final Flink operator. */
    public static final class Accumulator {
        private final boolean dryRun;
        private CleanupCounters global = CleanupCounters.empty();
        private long tasksPlanned;
        private long metadataFailures;
        private final Map<ScopeIdentity, TableAccumulator> tables = new HashMap<>();
        private final Map<String, CleanupCounters> databases = new HashMap<>();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);
        private final EnumMap<CleanupObjectType, RuleDecisionCounters> byRuleDecision =
                new EnumMap<>(CleanupObjectType.class);

        private Accumulator(boolean dryRun) {
            this.dryRun = dryRun;
        }

        public void addPlan(TablePlanStats plan) {
            tasksPlanned += plan.tasksPlanned();
            metadataFailures += plan.metadataFailures();
            TableAccumulator table =
                    tables.computeIfAbsent(plan.scope(), ignored -> new TableAccumulator());
            table.tasksPlanned += plan.tasksPlanned();
            table.metadataFailures += plan.metadataFailures();
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
            mergeRuleDecisions(table.byRuleDecision, taskStats.byRuleDecision());
            if (!tableKey.database().isEmpty()) {
                databases.put(
                        tableKey.database(),
                        databases
                                .getOrDefault(tableKey.database(), CleanupCounters.empty())
                                .add(counters));
            }
            mergeCounters(byObjectType, taskStats.byObjectType());
            mergeReasons(bySkipReason, taskStats.bySkipReason());
            mergeRuleDecisions(byRuleDecision, taskStats.byRuleDecision());
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
                                table.tasksPlanned,
                                table.metadataFailures,
                                table.counters,
                                table.byObjectType,
                                table.bySkipReason,
                                table.byRuleDecision));
            }
            return new CleanupReport(
                    global,
                    tasksPlanned,
                    metadataFailures,
                    summaries,
                    databases,
                    byObjectType,
                    bySkipReason,
                    byRuleDecision);
        }
    }

    private static final class TableAccumulator {
        private long tasksPlanned;
        private long metadataFailures;
        private CleanupCounters counters = CleanupCounters.empty();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);
        private final EnumMap<CleanupObjectType, RuleDecisionCounters> byRuleDecision =
                new EnumMap<>(CleanupObjectType.class);
    }
}
