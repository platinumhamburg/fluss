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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Self-contained per-table cleanup totals suitable for direct operator inspection. */
@Internal
public final class TableCleanupSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ScopeIdentity scope;
    private final long tasksPlanned;
    private final long metadataFailures;
    private final CleanupCounters counters;
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<SkipReasonCode, Long> bySkipReason;

    TableCleanupSummary(
            ScopeIdentity scope,
            long tasksPlanned,
            long metadataFailures,
            CleanupCounters counters,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> bySkipReason) {
        this.scope = scope.tableKey();
        this.tasksPlanned = tasksPlanned;
        this.metadataFailures = metadataFailures;
        this.counters = counters;
        Map<CleanupObjectType, CleanupCounters> objectCopy = new HashMap<>();
        objectCopy.putAll(byObjectType);
        this.byObjectType = objectCopy;
        Map<SkipReasonCode, Long> reasonCopy = new HashMap<>();
        reasonCopy.putAll(bySkipReason);
        this.bySkipReason = reasonCopy;
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public long plannedFiles() {
        return counters.plannedFiles();
    }

    public long tasksPlanned() {
        return tasksPlanned;
    }

    public long metadataFailures() {
        return metadataFailures;
    }

    public long plannedBytes() {
        return counters.plannedBytes();
    }

    public CleanupCounters counters() {
        return counters;
    }

    public Map<CleanupObjectType, CleanupCounters> byObjectType() {
        return Collections.unmodifiableMap(byObjectType);
    }

    public Map<SkipReasonCode, Long> bySkipReason() {
        return Collections.unmodifiableMap(bySkipReason);
    }
}
