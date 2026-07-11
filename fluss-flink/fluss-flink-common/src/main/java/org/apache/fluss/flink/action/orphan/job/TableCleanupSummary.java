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

import java.io.Serializable;

/** Self-contained per-table cleanup totals suitable for direct operator inspection. */
@Internal
public final class TableCleanupSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ScopeIdentity scope;
    private final CleanupCounters counters;

    TableCleanupSummary(ScopeIdentity scope, CleanupCounters counters) {
        this.scope = scope.tableKey();
        this.counters = counters;
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public long plannedFiles() {
        return counters.plannedFiles();
    }

    public long plannedBytes() {
        return counters.plannedBytes();
    }

    public CleanupCounters counters() {
        return counters;
    }
}
