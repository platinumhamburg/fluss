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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReportingException;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class ScanAndCleanFunctionTest {

    @BeforeEach
    void resetReporter() throws Exception {
        OrphanFilesCleanJobTest.invokeTestingFactory("reset", new Class<?>[0]);
    }

    @Test
    void forwardsScopeSummaryWithoutOpeningFilesystemState() throws Exception {
        ScopePlanStats plan = new ScopePlanStats();
        ScopeSummaryTask marker = ScopeSummaryTask.from(plan);
        CleanupStats expected = marker.stats();
        ScanAndCleanFunction function =
                new ScanAndCleanFunction(
                        100L, Collections.emptyMap(), OrphanFilesCleanJobTest.reporterSpec(false));
        List<CleanupStats> output = new ArrayList<>();

        function.processElement(marker, null, new ListCollector(output));

        assertThat(output).containsExactly(expected);
    }

    @Test
    void opensOneReporterRuntimePerAttemptAndNoTaskCloseFlushesThenCloses() throws Exception {
        ScanAndCleanFunction function =
                new ScanAndCleanFunction(
                        100L, Collections.emptyMap(), OrphanFilesCleanJobTest.reporterSpec(true));
        ProcessOperator<CleanTask, CleanupStats> operator = new ProcessOperator<>(function);
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                new OneInputStreamOperatorTestHarness<>(operator, 5, 5, 3);

        harness.open();
        AuditReporterContext context = OrphanFilesCleanJobTest.auditContext(function);
        assertThat(context.getStage()).isEqualTo(AuditStage.SCAN);
        assertThat(context.getOperatorName()).isEqualTo("ScanAndClean");
        assertThat(context.getSubtaskIndex()).isEqualTo(3);
        assertThat(context.getAttemptNumber()).isZero();
        assertThat(context.getUserCodeClassLoader())
                .isSameAs(function.getRuntimeContext().getUserCodeClassLoader());
        assertThat(harness.getRecordOutput()).isEmpty();
        harness.close();
        function.close();

        assertThat(OrphanFilesCleanJobTest.testingCalls())
                .containsExactly(
                        "testing:validate",
                        "testing:create",
                        "testing:open",
                        "testing:flush",
                        "testing:close");
    }

    @Test
    void requiredReporterOpenFailureFailsScanOpen() throws Exception {
        OrphanFilesCleanJobTest.invokeTestingFactory(
                "fail",
                new Class<?>[] {String.class, String.class, String.class},
                "testing",
                "open",
                "not-exposed");
        ScanAndCleanFunction function =
                new ScanAndCleanFunction(
                        100L, Collections.emptyMap(), OrphanFilesCleanJobTest.reporterSpec(true));
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<CleanTask, CleanupStats>(function), 1, 1, 0);

        assertThatThrownBy(harness::open)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("testing")
                .hasMessageContaining("open")
                .hasMessageNotContaining("not-exposed");
        assertThatCode(function::close).doesNotThrowAnyException();
    }

    @Test
    void scanCloseAttachesSanitizedFailureToTheExactProcessFailure() throws Exception {
        OrphanFilesCleanJobTest.invokeTestingFactory(
                "fail",
                new Class<?>[] {String.class, String.class, String.class},
                "testing",
                "close",
                "raw-provider-secret");
        ScanAndCleanFunction function =
                new ScanAndCleanFunction(
                        100L, Collections.emptyMap(), OrphanFilesCleanJobTest.reporterSpec(true));
        OneInputStreamOperatorTestHarness<CleanTask, CleanupStats> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<CleanTask, CleanupStats>(function), 1, 1, 0);
        harness.open();
        ScopeIdentity scope =
                ScopeIdentity.table("db", "table", 1L).withPartitionAndBucket(null, 0);
        BucketCleanTask task =
                new BucketCleanTask(
                        scope,
                        "unknownfs://host/tablet",
                        null,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Long.MAX_VALUE,
                        true,
                        false);

        Throwable primary =
                catchThrowable(() -> harness.processElement(new StreamRecord<CleanTask>(task)));
        assertThat(primary)
                .isNotNull()
                .isNotInstanceOf(AuditReportingException.class)
                .hasNoSuppressedExceptions();

        harness.close();
        assertThatCode(function::close).doesNotThrowAnyException();
        OrphanFilesCleanJobTest.assertSanitizedCleanupSuppressed(primary, "close");
    }

    private static final class ListCollector implements Collector<CleanupStats> {
        private final List<CleanupStats> output;

        private ListCollector(List<CleanupStats> output) {
            this.output = output;
        }

        @Override
        public void collect(CleanupStats record) {
            output.add(record);
        }

        @Override
        public void close() {}
    }
}
