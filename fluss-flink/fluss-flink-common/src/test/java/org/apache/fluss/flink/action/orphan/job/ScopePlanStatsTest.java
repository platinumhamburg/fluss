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
}
