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
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<SkipReasonCode, Long> bySkipReason;

    private CleanupReport(
            CleanupCounters global,
            Map<ScopeIdentity, TableCleanupSummary> tables,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> bySkipReason) {
        this.global = global;
        this.tables = Collections.unmodifiableMap(new HashMap<>(tables));
        EnumMap<CleanupObjectType, CleanupCounters> objectCopy =
                new EnumMap<>(CleanupObjectType.class);
        objectCopy.putAll(byObjectType);
        this.byObjectType = Collections.unmodifiableMap(objectCopy);
        EnumMap<SkipReasonCode, Long> reasonCopy = new EnumMap<>(SkipReasonCode.class);
        reasonCopy.putAll(bySkipReason);
        this.bySkipReason = Collections.unmodifiableMap(reasonCopy);
    }

    public static CleanupReport aggregate(
            Collection<TablePlanStats> plans, Collection<CleanStats> stats, boolean dryRun) {
        CleanupCounters global = CleanupCounters.empty();
        Map<ScopeIdentity, CleanupCounters> tableCounters = new HashMap<>();
        EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        EnumMap<SkipReasonCode, Long> bySkipReason = new EnumMap<>(SkipReasonCode.class);

        for (TablePlanStats plan : plans) {
            tableCounters.putIfAbsent(plan.scope(), CleanupCounters.empty());
            mergeReasons(bySkipReason, plan.skipped());
        }
        for (CleanStats taskStats : stats) {
            CleanupCounters counters = taskStats.counters();
            global = global.add(counters);
            ScopeIdentity tableKey = taskStats.scope().tableKey();
            tableCounters.put(
                    tableKey,
                    tableCounters.getOrDefault(tableKey, CleanupCounters.empty()).add(counters));
            for (Map.Entry<CleanupObjectType, CleanupCounters> entry :
                    taskStats.byObjectType().entrySet()) {
                byObjectType.put(
                        entry.getKey(),
                        byObjectType
                                .getOrDefault(entry.getKey(), CleanupCounters.empty())
                                .add(entry.getValue()));
            }
            mergeReasons(bySkipReason, taskStats.bySkipReason());
        }
        if (dryRun
                && (global.deletedFiles() != 0L
                        || global.bytesReclaimed() != 0L
                        || global.deleteFailures() != 0L)) {
            throw new IllegalStateException("dry-run report contains actual deletion counters");
        }

        Map<ScopeIdentity, TableCleanupSummary> tables = new HashMap<>();
        for (Map.Entry<ScopeIdentity, CleanupCounters> entry : tableCounters.entrySet()) {
            tables.put(entry.getKey(), new TableCleanupSummary(entry.getKey(), entry.getValue()));
        }
        return new CleanupReport(global, tables, byObjectType, bySkipReason);
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
        return tables;
    }

    public Map<CleanupObjectType, CleanupCounters> byObjectType() {
        return byObjectType;
    }

    public Map<SkipReasonCode, Long> bySkipReason() {
        return bySkipReason;
    }
}
