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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/** Stage 3 statistics owner. Aggregates all details and emits one fixed-size terminal summary. */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanupSummary>
        implements OneInputStreamOperator<CleanupStats, CleanupSummary>, BoundedOneInput {

    private static final long serialVersionUID = 4L;

    private final boolean dryRun;
    private final AuditReporterSpec auditReporterSpec;

    private transient AuditReporterRuntime auditRuntime;
    private transient AuditLogger audit;
    private transient boolean scopeSummarySeen;
    private transient CleanupCounters global;
    private transient long tasksPlanned;
    private transient long metadataFailures;
    private transient Map<ScopeIdentity, ScopeAccumulator> scopes;
    private transient EnumMap<CleanupObjectType, CleanupCounters> byObjectType;
    private transient EnumMap<SkipReasonCode, Long> bySkipReason;
    private transient EnumMap<CleanupObjectType, RuleDecisionCounters> byRuleDecision;
    private transient Throwable processingFailure;
    private transient boolean auditRuntimeFlushAttempted;

    public StatsAggregateOperator(boolean dryRun) {
        this(dryRun, null);
    }

    public StatsAggregateOperator(boolean dryRun, AuditReporterSpec auditReporterSpec) {
        this.dryRun = dryRun;
        this.auditReporterSpec = auditReporterSpec;
    }

    @Override
    public void open() throws Exception {
        super.open();
        processingFailure = null;
        auditRuntimeFlushAttempted = false;
        scopeSummarySeen = false;
        global = CleanupCounters.empty();
        tasksPlanned = 0L;
        metadataFailures = 0L;
        scopes = new HashMap<>();
        byObjectType = new EnumMap<>(CleanupObjectType.class);
        bySkipReason = new EnumMap<>(SkipReasonCode.class);
        byRuleDecision = new EnumMap<>(CleanupObjectType.class);
        if (auditReporterSpec == null) {
            audit = new AuditLogger();
            return;
        }

        AuditReporterContext reporterContext =
                new AuditReporterContext(
                        auditReporterSpec.runId(),
                        dryRun,
                        AuditStage.SUMMARY,
                        "StatsAggregate",
                        RuntimeContextAdapter.getIndexOfThisSubtask(getRuntimeContext()),
                        RuntimeContextAdapter.getAttemptNumber(getRuntimeContext()),
                        getRuntimeContext().getUserCodeClassLoader());
        auditRuntime = AuditReporterRuntime.open(auditReporterSpec, reporterContext);
        audit = new AuditLogger(auditRuntime, reporterContext);
    }

    @Override
    public void processElement(StreamRecord<CleanupStats> element) {
        try {
            CleanupStats stats = element.getValue();
            if (stats.sourceStage() == CleanupStats.SourceStage.SCOPE) {
                if (scopeSummarySeen) {
                    throw new IllegalStateException("Duplicate scope summary");
                }
                scopeSummarySeen = true;
            }

            global = global.add(stats.counters());
            tasksPlanned += stats.tasksPlanned();
            metadataFailures += stats.metadataFailures();
            mergeCounters(byObjectType, stats.byObjectType());
            mergeReasons(bySkipReason, stats.skipped());
            mergeRuleDecisions(byRuleDecision, stats.ruleDecisions());

            if (stats.sourceStage() == CleanupStats.SourceStage.SCAN) {
                ScopeIdentity scope = stats.scope().tableKey();
                ScopeAccumulator accumulator =
                        scopes.computeIfAbsent(scope, ignored -> new ScopeAccumulator());
                mergeCounters(accumulator.byObjectType, stats.byObjectType());
                mergeReasons(accumulator.bySkipReason, stats.skipped());
                mergeRuleDecisions(accumulator.byRuleDecision, stats.ruleDecisions());
            }
        } catch (RuntimeException | Error failure) {
            if (processingFailure == null) {
                processingFailure = failure;
            }
            throw failure;
        }
    }

    @Override
    public void endInput() throws Exception {
        try {
            if (!scopeSummarySeen) {
                throw new IllegalStateException("Missing scope summary");
            }

            CleanupSummary summary = buildSummary();
            emitDetailedAudit(audit, summary);

            IllegalStateException integrityFailure =
                    !summary.ruleCountersConsistent() || !summary.dryRunCountersConsistent()
                            ? new IllegalStateException(
                                    "Orphan cleanup audit integrity check failed")
                            : null;
            RuntimeException flushFailure = null;
            try {
                flushAuditRuntime();
            } catch (RuntimeException failure) {
                flushFailure = failure;
            }
            if (integrityFailure != null) {
                if (flushFailure != null) {
                    integrityFailure.addSuppressed(flushFailure);
                }
                throw integrityFailure;
            }
            if (flushFailure != null) {
                throw flushFailure;
            }
            output.collect(new StreamRecord<>(summary));
        } catch (Exception | Error failure) {
            if (processingFailure == null) {
                processingFailure = failure;
            }
            throw failure;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            closeAuditRuntime();
        } catch (RuntimeException | Error lifecycleFailure) {
            if (processingFailure == null) {
                throw lifecycleFailure;
            }
            processingFailure.addSuppressed(lifecycleFailure);
        } finally {
            super.close();
        }
    }

    private CleanupSummary buildSummary() {
        long actionRequiredSkips = 0L;
        for (Map.Entry<SkipReasonCode, Long> entry : bySkipReason.entrySet()) {
            if (entry.getKey().actionRequired()) {
                actionRequiredSkips += entry.getValue();
            }
        }

        long inconsistentObjectTypes = countObjectInconsistencies(byObjectType, byRuleDecision);
        long inconsistentScopes = 0L;
        for (ScopeAccumulator scope : scopes.values()) {
            if (countObjectInconsistencies(scope.byObjectType, scope.byRuleDecision) > 0L
                    || scope.bySkipReason.getOrDefault(SkipReasonCode.MTIME_UNAVAILABLE, 0L)
                            < sumMtimeUnavailableFiles(scope.byRuleDecision.values())) {
                inconsistentScopes++;
            }
        }

        long candidateFiles = 0L;
        long candidateBytes = 0L;
        for (RuleDecisionCounters counters : byRuleDecision.values()) {
            candidateFiles += counters.candidateFiles();
            candidateBytes += counters.candidateBytes();
        }

        boolean coverageComplete = metadataFailures == 0L && actionRequiredSkips == 0L;
        boolean mtimeCountersConsistent =
                bySkipReason.getOrDefault(SkipReasonCode.MTIME_UNAVAILABLE, 0L)
                        >= sumMtimeUnavailableFiles(byRuleDecision.values());
        boolean ruleCountersConsistent =
                mtimeCountersConsistent
                        && inconsistentObjectTypes == 0L
                        && inconsistentScopes == 0L
                        && candidateFiles == global.plannedFiles()
                        && candidateBytes == global.plannedBytes();
        boolean dryRunCountersConsistent =
                !dryRun
                        || (global.deletedFiles() == 0L
                                && global.bytesReclaimed() == 0L
                                && global.deleteFailures() == 0L);

        return new CleanupSummary(
                dryRun,
                global,
                tasksPlanned,
                metadataFailures,
                actionRequiredSkips,
                inconsistentObjectTypes,
                inconsistentScopes,
                candidateFiles,
                candidateBytes,
                coverageComplete,
                ruleCountersConsistent,
                dryRunCountersConsistent);
    }

    private void emitDetailedAudit(AuditLogger audit, CleanupSummary summary) {
        for (Map.Entry<ScopeIdentity, ScopeAccumulator> scopeEntry : scopes.entrySet()) {
            for (Map.Entry<CleanupObjectType, RuleDecisionCounters> ruleEntry :
                    scopeEntry.getValue().byRuleDecision.entrySet()) {
                audit.logTableRuleSummary(
                        scopeEntry.getKey(), ruleEntry.getKey(), ruleEntry.getValue(), dryRun);
            }
        }
        for (Map.Entry<CleanupObjectType, RuleDecisionCounters> entry : byRuleDecision.entrySet()) {
            audit.logGlobalRuleSummary(entry.getKey(), entry.getValue(), dryRun);
        }

        long mtimeFiles = sumMtimeUnavailableFiles(byRuleDecision.values());
        long mtimeBytes = 0L;
        for (RuleDecisionCounters counters : byRuleDecision.values()) {
            mtimeBytes += counters.mtimeUnavailableBytes();
        }
        long mtimeDirs =
                bySkipReason.getOrDefault(SkipReasonCode.MTIME_UNAVAILABLE, 0L) - mtimeFiles;
        audit.logCoverageSummary(
                bySkipReason,
                metadataFailures,
                mtimeFiles,
                mtimeBytes,
                mtimeDirs,
                summary.coverageComplete(),
                dryRun);
        audit.logAuditIntegrity(summary);
        audit.logSummary(
                global.scannedFiles(),
                global.deletedFiles(),
                global.emptyDirsRemoved(),
                global.deleteFailures(),
                global.bytesReclaimed(),
                dryRun);
    }

    private static long countObjectInconsistencies(
            Map<CleanupObjectType, CleanupCounters> objectCounters,
            Map<CleanupObjectType, RuleDecisionCounters> ruleCounters) {
        EnumSet<CleanupObjectType> types = EnumSet.noneOf(CleanupObjectType.class);
        types.addAll(objectCounters.keySet());
        types.addAll(ruleCounters.keySet());
        long inconsistent = 0L;
        for (CleanupObjectType type : types) {
            CleanupCounters counters = objectCounters.getOrDefault(type, CleanupCounters.empty());
            RuleDecisionCounters decisions = ruleCounters.get(type);
            if (decisions == null) {
                if (counters.plannedFiles() > 0L) {
                    inconsistent++;
                }
            } else if (!decisions.isConsistent()
                    || decisions.candidateFiles() != counters.plannedFiles()
                    || decisions.candidateBytes() != counters.plannedBytes()) {
                inconsistent++;
            }
        }
        return inconsistent;
    }

    private static long sumMtimeUnavailableFiles(Collection<RuleDecisionCounters> counters) {
        long total = 0L;
        for (RuleDecisionCounters counter : counters) {
            total += counter.mtimeUnavailableFiles();
        }
        return total;
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

    private void closeAuditRuntime() {
        AuditReporterRuntime runtime = auditRuntime;
        boolean flushNeeded = !auditRuntimeFlushAttempted;
        auditRuntime = null;
        audit = null;
        auditRuntimeFlushAttempted = false;
        if (runtime == null) {
            return;
        }

        RuntimeException failure = null;
        if (flushNeeded) {
            try {
                runtime.flush();
            } catch (RuntimeException flushFailure) {
                failure = flushFailure;
            }
        }
        try {
            runtime.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void flushAuditRuntime() {
        if (auditRuntime == null || auditRuntimeFlushAttempted) {
            return;
        }
        auditRuntimeFlushAttempted = true;
        auditRuntime.flush();
    }

    private static final class ScopeAccumulator {
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);
        private final EnumMap<CleanupObjectType, RuleDecisionCounters> byRuleDecision =
                new EnumMap<>(CleanupObjectType.class);
    }
}
