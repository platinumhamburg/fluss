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

import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ScopePlanStatsTest {

    @Test
    void aggregatesExpectedNoManifestSkipsWithoutPerBucketWarnings() {
        ScopePlanStats stats = new ScopePlanStats();

        for (int i = 0; i < 12; i++) {
            stats.discoveredBucket();
            stats.skippedNoRemoteManifest();
        }

        assertThat(stats.discoveredBuckets()).isEqualTo(12L);
        assertThat(stats.skippedNoRemoteManifestCount()).isEqualTo(12L);
        assertThat(stats.bucketTasks()).isZero();
    }

    @Test
    void createsOneImmutableScopeSummaryTask() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.bucketTask();
        stats.orphanDirTask();
        stats.metadataFailure();
        stats.skippedNoRemoteManifest();

        ScopeSummaryTask task = ScopeSummaryTask.from(stats);

        assertThat(task.scope()).isEqualTo(ScopeIdentity.global());
        assertThat(task.stats().sourceStage()).isEqualTo(CleanupStats.SourceStage.SCOPE);
        assertThat(task.stats().tasksPlanned()).isEqualTo(2L);
        assertThat(task.stats().metadataFailures()).isEqualTo(1L);
        assertThat(task.stats().skipped()).containsEntry(SkipReasonCode.NO_REMOTE_MANIFEST, 1L);
    }

    @Test
    void aggregatesTargetCoverageCategoriesWithoutLosingBucketEquation() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.discoveredBuckets(4L);
        ScopeTargetStats target =
                new ScopeTargetStats(
                        ScopeIdentity.table("db", "table", 7L).withPartitionAndBucket(9L, null),
                        4L,
                        true);
        target.logResolvedBucket();
        target.logResolvedBucket();
        target.logNoManifestBucket();
        target.logReadFailedBucket();
        target.kvActiveBucket();
        target.kvActiveBucket();
        target.kvEmptyBucket();
        target.kvEmptyBucket();
        target.taskEmitted();
        target.taskEmitted();
        target.taskEmitted();
        target.incomplete(123L);

        stats.target(target);

        assertThat(target.logCoverageConsistent()).isTrue();
        assertThat(target.kvCoverageConsistent()).isTrue();
        assertThat(target.complete()).isFalse();
        assertThat(stats.scopeTargets()).isEqualTo(1L);
        assertThat(stats.logResolvedBuckets()).isEqualTo(2L);
        assertThat(stats.logNoManifestBuckets()).isEqualTo(1L);
        assertThat(stats.logReadFailedBuckets()).isEqualTo(1L);
        assertThat(stats.kvActiveBuckets()).isEqualTo(2L);
        assertThat(stats.kvEmptyBuckets()).isEqualTo(2L);
        assertThat(stats.incompleteTargets()).isEqualTo(1L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }

    @Test
    void marksRpcUnavailableTargetIncompleteWithoutClaimingPhysicalCoverage() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.discoveredBuckets(3L);
        ScopeTargetStats target =
                new ScopeTargetStats(ScopeIdentity.table("db", "table", 7L), 3L, true);
        target.logRpcFailed();
        target.kvRpcFailed();
        target.incomplete(50L);

        stats.target(target);

        assertThat(target.logCoverageConsistent()).isTrue();
        assertThat(target.kvCoverageConsistent()).isTrue();
        assertThat(target.complete()).isFalse();
        assertThat(stats.logUnavailableBuckets()).isEqualTo(3L);
        assertThat(stats.kvUnavailableBuckets()).isEqualTo(3L);
        assertThat(stats.incompleteTargets()).isEqualTo(1L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }

    @Test
    void countsDisappearedPartitionAsCompleteScopeChange() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.discoveredBuckets(3L);
        ScopeTargetStats target =
                new ScopeTargetStats(
                        ScopeIdentity.table("db", "table", 7L).withPartitionAndBucket(9L, null),
                        3L,
                        true);
        target.targetDisappeared(SkipReasonCode.PARTITION_NOT_EXIST);
        target.complete(50L);

        stats.target(target);

        assertThat(target.disappeared()).isTrue();
        assertThat(target.disappearanceReason()).isEqualTo(SkipReasonCode.PARTITION_NOT_EXIST);
        assertThat(target.disappearedBuckets()).isEqualTo(3L);
        assertThat(target.hasCoverageFailure()).isFalse();
        assertThat(target.logCoverageConsistent()).isTrue();
        assertThat(target.kvCoverageConsistent()).isTrue();
        assertThat(stats.disappearedTargets()).isEqualTo(1L);
        assertThat(stats.disappearedBuckets()).isEqualTo(3L);
        assertThat(stats.disappearedPartitionTargets()).isEqualTo(1L);
        assertThat(stats.incompleteTargets()).isZero();
        assertThat(stats.metadataFailures()).isZero();
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isTrue();
        assertThat(ScopeSummaryTask.from(stats).stats().skipped())
                .containsEntry(SkipReasonCode.PARTITION_NOT_EXIST, 1L);
    }

    @Test
    void doesNotInventKvCoverageForDisappearedLogOnlyTarget() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.discoveredBuckets(2L);
        ScopeTargetStats target =
                new ScopeTargetStats(ScopeIdentity.table("db", "log_table", 8L), 2L, false);
        target.targetDisappeared(SkipReasonCode.TABLE_NOT_EXIST);
        target.complete(7L);

        stats.target(target);

        assertThat(target.logCoverageConsistent()).isTrue();
        assertThat(target.kvCoverageConsistent()).isTrue();
        assertThat(stats.kvTargetBuckets()).isZero();
        assertThat(stats.kvDisappearedBuckets()).isZero();
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isTrue();
    }

    @Test
    void mergePreservesDisappearanceCounters() {
        ScopePlanStats delta = new ScopePlanStats();
        delta.discoveredBuckets(2L);
        ScopeTargetStats target =
                new ScopeTargetStats(
                        ScopeIdentity.table("db", "table", 7L).withPartitionAndBucket(9L, null),
                        2L,
                        true);
        target.targetDisappeared(SkipReasonCode.PARTITION_NOT_EXIST);
        target.complete(3L);
        delta.target(target);

        ScopePlanStats total = new ScopePlanStats();
        total.mergeFrom(delta);

        assertThat(total.disappearedTargets()).isEqualTo(1L);
        assertThat(total.disappearedBuckets()).isEqualTo(2L);
        assertThat(total.kvDisappearedBuckets()).isEqualTo(2L);
        assertThat(total.disappearedPartitionTargets()).isEqualTo(1L);
        assertThat(total.countersConsistent()).isTrue();
        assertThat(total.coverageComplete()).isTrue();
    }

    @Test
    void classifiesOutOfScopeBucketsWithoutInventingKvCoverageForLogOnlyTable() {
        ScopePlanStats stats = new ScopePlanStats();
        stats.discoveredBuckets(2L);
        ScopeTargetStats target =
                new ScopeTargetStats(ScopeIdentity.table("db", "log_table", 8L), 2L, false);
        target.outOfScope();
        target.incomplete(7L);

        stats.target(target);

        assertThat(target.logCoverageConsistent()).isTrue();
        assertThat(target.kvCoverageConsistent()).isTrue();
        assertThat(stats.outOfScopeBuckets()).isEqualTo(2L);
        assertThat(stats.kvOutOfScopeBuckets()).isZero();
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }
}
