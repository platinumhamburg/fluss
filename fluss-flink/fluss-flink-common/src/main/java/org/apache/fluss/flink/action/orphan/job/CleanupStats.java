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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable statistics envelope shared by the scope and scan stages. */
@Internal
public final class CleanupStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Stage that produced this statistics envelope. */
    public enum SourceStage {
        SCOPE,
        SCAN
    }

    private final SourceStage sourceStage;
    private final ScopeIdentity scope;
    private final CleanupCounters counters;
    private final long tasksPlanned;
    private final long metadataFailures;
    private final long scopeDiscoveredBuckets;
    private final long scopeTargetBuckets;
    private final long scopeLogClassifiedBuckets;
    private final long scopeKvTargetBuckets;
    private final long scopeKvClassifiedBuckets;
    private final long incompleteScopeTargets;
    private final Map<SkipReasonCode, Long> skipped;
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<CleanupObjectType, RuleDecisionCounters> ruleDecisions;

    private CleanupStats(
            SourceStage sourceStage,
            ScopeIdentity scope,
            CleanupCounters counters,
            long tasksPlanned,
            long metadataFailures,
            long scopeDiscoveredBuckets,
            long scopeTargetBuckets,
            long scopeLogClassifiedBuckets,
            long scopeKvTargetBuckets,
            long scopeKvClassifiedBuckets,
            long incompleteScopeTargets,
            Map<SkipReasonCode, Long> skipped,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<CleanupObjectType, RuleDecisionCounters> ruleDecisions) {
        this.sourceStage = Objects.requireNonNull(sourceStage);
        this.scope = Objects.requireNonNull(scope);
        this.counters = Objects.requireNonNull(counters);
        this.tasksPlanned = tasksPlanned;
        this.metadataFailures = metadataFailures;
        this.scopeDiscoveredBuckets = scopeDiscoveredBuckets;
        this.scopeTargetBuckets = scopeTargetBuckets;
        this.scopeLogClassifiedBuckets = scopeLogClassifiedBuckets;
        this.scopeKvTargetBuckets = scopeKvTargetBuckets;
        this.scopeKvClassifiedBuckets = scopeKvClassifiedBuckets;
        this.incompleteScopeTargets = incompleteScopeTargets;
        this.skipped = copyMap(skipped);
        this.byObjectType = copyMap(byObjectType);
        this.ruleDecisions = copyMap(ruleDecisions);
    }

    public static CleanupStats scope(
            long tasksPlanned, long metadataFailures, Map<SkipReasonCode, Long> skipped) {
        return new CleanupStats(
                SourceStage.SCOPE,
                ScopeIdentity.global(),
                CleanupCounters.empty(),
                tasksPlanned,
                metadataFailures,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                skipped,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    static CleanupStats scope(ScopePlanStats plan, Map<SkipReasonCode, Long> skipped) {
        return new CleanupStats(
                SourceStage.SCOPE,
                ScopeIdentity.global(),
                CleanupCounters.empty(),
                plan.bucketTasks() + plan.orphanDirTasks(),
                plan.metadataFailures(),
                plan.discoveredBuckets(),
                plan.targetBuckets(),
                plan.logResolvedBuckets()
                        + plan.logNoManifestBuckets()
                        + plan.logReadFailedBuckets()
                        + plan.logUnavailableBuckets()
                        + plan.outOfScopeBuckets(),
                plan.kvTargetBuckets(),
                plan.kvActiveBuckets()
                        + plan.kvEmptyBuckets()
                        + plan.kvUnavailableBuckets()
                        + plan.kvOutOfScopeBuckets(),
                plan.incompleteTargets(),
                skipped,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public static CleanupStats scan(
            ScopeIdentity scope,
            CleanupCounters counters,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> skipped,
            Map<CleanupObjectType, RuleDecisionCounters> ruleDecisions) {
        return new CleanupStats(
                SourceStage.SCAN,
                scope,
                counters,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                skipped,
                byObjectType,
                ruleDecisions);
    }

    public static CleanupStats emptyScan(ScopeIdentity scope) {
        return scan(
                scope,
                CleanupCounters.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public static Builder scanBuilder(ScopeIdentity scope) {
        return new Builder(scope);
    }

    public SourceStage sourceStage() {
        return sourceStage;
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public CleanupCounters counters() {
        return counters;
    }

    public long tasksPlanned() {
        return tasksPlanned;
    }

    public long metadataFailures() {
        return metadataFailures;
    }

    public long scopeTargetBuckets() {
        return scopeTargetBuckets;
    }

    public long scopeDiscoveredBuckets() {
        return scopeDiscoveredBuckets;
    }

    public long scopeLogClassifiedBuckets() {
        return scopeLogClassifiedBuckets;
    }

    public long scopeKvTargetBuckets() {
        return scopeKvTargetBuckets;
    }

    public long scopeKvClassifiedBuckets() {
        return scopeKvClassifiedBuckets;
    }

    public long incompleteScopeTargets() {
        return incompleteScopeTargets;
    }

    public boolean scopeCountersConsistent() {
        return scopeDiscoveredBuckets == scopeTargetBuckets
                && scopeTargetBuckets == scopeLogClassifiedBuckets
                && scopeKvTargetBuckets == scopeKvClassifiedBuckets;
    }

    public Map<SkipReasonCode, Long> skipped() {
        return Collections.unmodifiableMap(skipped);
    }

    public Map<CleanupObjectType, CleanupCounters> byObjectType() {
        return Collections.unmodifiableMap(byObjectType);
    }

    public Map<CleanupObjectType, RuleDecisionCounters> ruleDecisions() {
        return Collections.unmodifiableMap(ruleDecisions);
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        Objects.requireNonNull(source);
        // CleanupStats crosses a Flink network edge. Keep the transport representation on a
        // plain JDK map because Kryo's reflective EnumMap serializer is not reliable on all
        // supported JDKs; enum-aware maps remain an implementation detail of the local builder.
        return new HashMap<>(source);
    }

    /** Builder used by cleaners to retain low-cardinality audit dimensions. */
    public static final class Builder {

        private final ScopeIdentity scope;
        private CleanupCounters counters = CleanupCounters.empty();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> skipped = new EnumMap<>(SkipReasonCode.class);
        private final EnumMap<CleanupObjectType, RuleDecisionCounters> ruleDecisions =
                new EnumMap<>(CleanupObjectType.class);

        private Builder(ScopeIdentity scope) {
            this.scope = Objects.requireNonNull(scope);
        }

        public Builder scanned(CleanupObjectType type, long files) {
            return add(type, new CleanupCounters(files, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
        }

        public Builder planned(CleanupObjectType type, long files, long bytes) {
            return add(type, new CleanupCounters(0L, files, 0L, bytes, 0L, 0L, 0L, 0L));
        }

        public Builder deleted(CleanupObjectType type, long files, long bytes) {
            return add(type, new CleanupCounters(0L, 0L, 0L, 0L, files, 0L, 0L, bytes));
        }

        public Builder deleteFailed(CleanupObjectType type, long files) {
            return add(type, new CleanupCounters(0L, 0L, 0L, 0L, 0L, 0L, files, 0L));
        }

        public Builder plannedDirectory(long dirs) {
            return add(
                    CleanupObjectType.DIRECTORY,
                    new CleanupCounters(0L, 0L, dirs, 0L, 0L, 0L, 0L, 0L));
        }

        public Builder removedDirectory(long dirs) {
            return add(
                    CleanupObjectType.DIRECTORY,
                    new CleanupCounters(0L, 0L, 0L, 0L, 0L, dirs, 0L, 0L));
        }

        public Builder skipped(SkipReasonCode reason, long count) {
            skipped.put(reason, skipped.getOrDefault(reason, 0L) + count);
            return this;
        }

        public Builder ruleDecision(CleanupObjectType type, RuleDecisionCounters counters) {
            ruleDecisions.put(
                    type,
                    ruleDecisions.getOrDefault(type, RuleDecisionCounters.empty()).add(counters));
            return this;
        }

        public CleanupStats build() {
            return CleanupStats.scan(scope, counters, byObjectType, skipped, ruleDecisions);
        }

        private Builder add(CleanupObjectType type, CleanupCounters delta) {
            counters = counters.add(delta);
            byObjectType.put(
                    type, byObjectType.getOrDefault(type, CleanupCounters.empty()).add(delta));
            return this;
        }
    }
}
