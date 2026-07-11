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

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/** Per-table work plan produced by scope enumeration for final audit reporting. */
@Internal
public final class TablePlanStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ScopeIdentity scope;
    private final long tasksPlanned;
    private final long metadataFailures;
    private final Map<SkipReasonCode, Long> skipped;

    public TablePlanStats(
            ScopeIdentity scope,
            long tasksPlanned,
            long metadataFailures,
            Map<SkipReasonCode, Long> skipped) {
        this.scope = scope.tableKey();
        this.tasksPlanned = tasksPlanned;
        this.metadataFailures = metadataFailures;
        EnumMap<SkipReasonCode, Long> copy = new EnumMap<>(SkipReasonCode.class);
        copy.putAll(skipped);
        this.skipped = Collections.unmodifiableMap(copy);
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public long tasksPlanned() {
        return tasksPlanned;
    }

    public long metadataFailures() {
        return metadataFailures;
    }

    public Map<SkipReasonCode, Long> skipped() {
        return skipped;
    }
}
