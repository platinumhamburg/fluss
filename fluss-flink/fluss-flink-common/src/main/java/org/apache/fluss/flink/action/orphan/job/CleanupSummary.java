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

import java.io.Serializable;
import java.util.Objects;

/** Fixed-size immutable terminal result of one orphan cleanup job. */
@Internal
public final class CleanupSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean dryRun;
    private final CleanupCounters globalCounters;
    private final long tasksPlanned;
    private final long metadataFailures;
    private final long actionRequiredSkips;
    private final long inconsistentObjectTypes;
    private final long inconsistentScopes;
    private final long ruleCandidateFiles;
    private final long ruleCandidateBytes;
    private final long incompleteScopeTargets;
    private final boolean coverageComplete;
    private final boolean scopeCountersConsistent;
    private final boolean ruleCountersConsistent;
    private final boolean dryRunCountersConsistent;

    CleanupSummary(
            boolean dryRun,
            CleanupCounters globalCounters,
            long tasksPlanned,
            long metadataFailures,
            long actionRequiredSkips,
            long inconsistentObjectTypes,
            long inconsistentScopes,
            long ruleCandidateFiles,
            long ruleCandidateBytes,
            long incompleteScopeTargets,
            boolean coverageComplete,
            boolean scopeCountersConsistent,
            boolean ruleCountersConsistent,
            boolean dryRunCountersConsistent) {
        this.dryRun = dryRun;
        this.globalCounters = Objects.requireNonNull(globalCounters);
        this.tasksPlanned = tasksPlanned;
        this.metadataFailures = metadataFailures;
        this.actionRequiredSkips = actionRequiredSkips;
        this.inconsistentObjectTypes = inconsistentObjectTypes;
        this.inconsistentScopes = inconsistentScopes;
        this.ruleCandidateFiles = ruleCandidateFiles;
        this.ruleCandidateBytes = ruleCandidateBytes;
        this.incompleteScopeTargets = incompleteScopeTargets;
        this.coverageComplete = coverageComplete;
        this.scopeCountersConsistent = scopeCountersConsistent;
        this.ruleCountersConsistent = ruleCountersConsistent;
        this.dryRunCountersConsistent = dryRunCountersConsistent;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public CleanupCounters globalCounters() {
        return globalCounters;
    }

    public long tasksPlanned() {
        return tasksPlanned;
    }

    public long metadataFailures() {
        return metadataFailures;
    }

    public long actionRequiredSkips() {
        return actionRequiredSkips;
    }

    public long inconsistentObjectTypes() {
        return inconsistentObjectTypes;
    }

    public long inconsistentScopes() {
        return inconsistentScopes;
    }

    public long ruleCandidateFiles() {
        return ruleCandidateFiles;
    }

    public long ruleCandidateBytes() {
        return ruleCandidateBytes;
    }

    public boolean coverageComplete() {
        return coverageComplete;
    }

    public long incompleteScopeTargets() {
        return incompleteScopeTargets;
    }

    public boolean scopeCountersConsistent() {
        return scopeCountersConsistent;
    }

    public boolean countersConsistent() {
        return scopeCountersConsistent && ruleCountersConsistent;
    }

    public boolean ruleCountersConsistent() {
        return ruleCountersConsistent;
    }

    public boolean dryRunCountersConsistent() {
        return dryRunCountersConsistent;
    }
}
