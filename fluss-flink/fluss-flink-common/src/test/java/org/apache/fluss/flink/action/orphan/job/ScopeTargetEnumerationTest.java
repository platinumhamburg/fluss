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

import org.apache.fluss.flink.action.orphan.audit.AuditFailureDetail;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ScopeTargetEnumerationTest {

    @Test
    void targetDurationIncludesCallerThreadTaskReplay() {
        TestingAuditReporterFactory.reset();
        AuditReporterSpec spec = testingReporterSpec();
        AuditReporterContext context = testingContext(spec);
        AuditReporterRuntime runtime = AuditReporterRuntime.open(spec, context);
        AuditLogger audit = new AuditLogger(runtime, context);
        ScopeTargetStats targetStats =
                new ScopeTargetStats(ScopeIdentity.table("db", "table", 7L), 1L, false);
        targetStats.logResolvedBucket();
        CleanTask task = mock(CleanTask.class);
        ScopeTargetEnumeration.Result result =
                ScopeTargetEnumeration.Result.builder(targetStats)
                        .task(task)
                        .targetTiming(100L, true)
                        .build();
        AtomicLong clock = new AtomicLong(100L);

        try {
            result.replay(audit, new ScopePlanStats(), ignored -> clock.addAndGet(37L), clock::get);

            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("scope_target_summary"))
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMetrics()).containsEntry("duration_ms", 37L);
                                assertThat(event.getFlags()).containsEntry("complete", true);
                            });
        } finally {
            runtime.close();
            TestingAuditReporterFactory.reset();
        }
    }

    @Test
    void resultSnapshotsStatsAtBuildTime() {
        TestingAuditReporterFactory.reset();
        AuditReporterSpec spec = testingReporterSpec();
        AuditReporterContext context = testingContext(spec);
        AuditReporterRuntime runtime = AuditReporterRuntime.open(spec, context);
        AuditLogger audit = new AuditLogger(runtime, context);
        ScopeTargetStats sourceStats =
                new ScopeTargetStats(ScopeIdentity.table("db", "table", 7L), 1L, false);
        sourceStats.logResolvedBucket();
        sourceStats.complete(5L);
        CleanTask firstTask = mock(CleanTask.class);
        CleanTask laterTask = mock(CleanTask.class);
        ScopeTargetEnumeration.Result.Builder builder =
                ScopeTargetEnumeration.Result.builder(sourceStats)
                        .task(firstTask)
                        .metadataFailure();
        ScopeTargetEnumeration.Result result = builder.build();

        builder.task(laterTask).metadataFailure();
        sourceStats.logRpcFailed();
        sourceStats.incomplete(99L);
        ScopeTargetStats exposedCopy = result.targetStats();
        exposedCopy.logRpcFailed();
        exposedCopy.taskEmitted();
        exposedCopy.incomplete(101L);

        ScopePlanStats total = new ScopePlanStats();
        List<CleanTask> emitted = new ArrayList<CleanTask>();
        try {
            result.replay(audit, total, emitted::add);

            assertThat(emitted).containsExactly(firstTask);
            assertThat(total.metadataFailures()).isEqualTo(1L);
            assertThat(total.scopeTargets()).isEqualTo(1L);
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("scope_target_summary"))
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getMetrics())
                                        .containsEntry("log_resolved_buckets", 1L)
                                        .containsEntry("log_unavailable_buckets", 0L)
                                        .containsEntry("tasks_emitted", 1L)
                                        .containsEntry("duration_ms", 5L);
                                assertThat(event.getFlags()).containsEntry("complete", true);
                            });
        } finally {
            runtime.close();
            TestingAuditReporterFactory.reset();
        }
    }

    @Test
    void callerReplaysIncompleteResultBeforeRethrowingOriginalFailure() {
        TestingAuditReporterFactory.reset();
        AuditReporterSpec spec = testingReporterSpec();
        AuditReporterContext context = testingContext(spec);
        AuditReporterRuntime runtime = AuditReporterRuntime.open(spec, context);
        AuditLogger audit = new AuditLogger(runtime, context);
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 7L);
        ScopeTargetStats targetStats = new ScopeTargetStats(scope, 2L, false);
        targetStats.logRpcFailed();
        ScopeTargetEnumeration.Result partialResult =
                ScopeTargetEnumeration.Result.builder(targetStats)
                        .discoveredBuckets(2L)
                        .metadataFailure()
                        .build();
        IllegalStateException originalFailure = new IllegalStateException("fatal-enumeration");
        ScopeTargetEnumeration.Worker worker =
                ignored -> {
                    throw new ScopeTargetEnumeration.EnumerationException(
                            partialResult, originalFailure);
                };
        ScopePlanStats total = new ScopePlanStats();

        try {
            assertThatThrownBy(
                            () ->
                                    ScopeEnumeratorFunction.enumerateTarget(
                                            worker, null, audit, total, ignored -> {}))
                    .isSameAs(originalFailure);

            assertThat(total.discoveredBuckets()).isEqualTo(2L);
            assertThat(total.metadataFailures()).isEqualTo(1L);
            assertThat(total.scopeTargets()).isEqualTo(1L);
            assertThat(total.incompleteTargets()).isEqualTo(1L);
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("scope_target_summary"))
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getTableId()).isEqualTo(7L);
                                assertThat(event.getFlags()).containsEntry("complete", false);
                            });
        } finally {
            runtime.close();
            TestingAuditReporterFactory.reset();
        }
    }

    @Test
    void resultReplaysTasksStatsAndDiagnosticsOnCallerThread() {
        TestingAuditReporterFactory.reset();
        AuditReporterSpec spec = testingReporterSpec();
        AuditReporterContext context = testingContext(spec);
        AuditReporterRuntime runtime = AuditReporterRuntime.open(spec, context);
        AuditLogger audit = new AuditLogger(runtime, context);
        ScopePlanStats total = new ScopePlanStats();
        List<CleanTask> emitted = new ArrayList<CleanTask>();
        CleanTask task = mock(CleanTask.class);
        ScopeIdentity scope = ScopeIdentity.table("db", "table", 7L);
        AuditFailureDetail failure =
                AuditFailureDetail.builder("list_remote_log_manifests", "timeout")
                        .exceptionClass(IOException.class)
                        .retryable(true)
                        .actionRequired(true)
                        .build();

        ScopeTargetEnumeration.Result result =
                ScopeTargetEnumeration.Result.builder(new ScopeTargetStats(scope, 1, false))
                        .task(task)
                        .rpcFailure(scope, CleanupObjectType.LOG_MANIFEST, failure)
                        .metadataFailure()
                        .build();

        try {
            result.replay(audit, total, emitted::add);

            assertThat(emitted).containsExactly(task);
            assertThat(total.metadataFailures()).isEqualTo(1L);
            assertThat(TestingAuditReporterFactory.events("testing"))
                    .extracting(event -> event.getAction())
                    .containsExactly("rpc_failure", "scope_target_summary");
            assertThat(TestingAuditReporterFactory.events("testing").get(0))
                    .satisfies(
                            event -> {
                                assertThat(event.getStage()).isEqualTo(AuditStage.SCOPE);
                                assertThat(event.getTableId()).isEqualTo(7L);
                                assertThat(event.getObjectType()).isEqualTo("log_manifest");
                                assertThat(event.getDimensions())
                                        .containsEntry("operation", "list_remote_log_manifests")
                                        .containsEntry("failure_category", "timeout");
                            });
        } finally {
            runtime.close();
            TestingAuditReporterFactory.reset();
        }
    }

    private static AuditReporterSpec testingReporterSpec() {
        return new AuditReporterSpec(
                "00000000-0000-0000-0000-000000000003",
                Collections.singletonList(
                        new ReporterSpec("testing", true, Collections.<String, String>emptyMap())));
    }

    private static AuditReporterContext testingContext(AuditReporterSpec spec) {
        return new AuditReporterContext(
                spec.runId(),
                true,
                AuditStage.SCOPE,
                "ScopeEnumerator",
                0,
                0,
                ScopeTargetEnumerationTest.class.getClassLoader());
    }
}
