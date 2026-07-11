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
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Mutable stage-1 accumulator that retains tables even when no cleanup task is emitted. */
@Internal
final class TablePlanTracker {

    private final Map<ScopeIdentity, MutablePlan> plans = new LinkedHashMap<>();

    void ensure(ScopeIdentity scope) {
        plans.computeIfAbsent(scope.tableKey(), ignored -> new MutablePlan());
    }

    void task(ScopeIdentity scope) {
        mutable(scope).tasksPlanned++;
    }

    void metadataFailure(ScopeIdentity scope) {
        MutablePlan plan = mutable(scope);
        plan.metadataFailures++;
        plan.skip(SkipReasonCode.METADATA_READ_FAILED);
    }

    void skip(ScopeIdentity scope, SkipReasonCode reason) {
        mutable(scope).skip(reason);
    }

    Collection<TablePlanStats> snapshots() {
        Collection<TablePlanStats> result = new ArrayList<>(plans.size());
        for (Map.Entry<ScopeIdentity, MutablePlan> entry : plans.entrySet()) {
            MutablePlan plan = entry.getValue();
            result.add(
                    new TablePlanStats(
                            entry.getKey(),
                            plan.tasksPlanned,
                            plan.metadataFailures,
                            plan.skipped));
        }
        return result;
    }

    private MutablePlan mutable(ScopeIdentity scope) {
        ScopeIdentity tableKey = scope.tableKey();
        return plans.computeIfAbsent(tableKey, ignored -> new MutablePlan());
    }

    private static final class MutablePlan {
        private long tasksPlanned;
        private long metadataFailures;
        private final EnumMap<SkipReasonCode, Long> skipped = new EnumMap<>(SkipReasonCode.class);

        private void skip(SkipReasonCode reason) {
            skipped.put(reason, skipped.getOrDefault(reason, 0L) + 1L);
        }
    }
}
