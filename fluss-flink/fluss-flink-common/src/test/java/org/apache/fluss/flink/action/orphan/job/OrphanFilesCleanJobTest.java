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

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReportingException;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

class OrphanFilesCleanJobTest {

    private static final String RUN_ID = "123e4567-e89b-12d3-a456-426614174000";

    @BeforeEach
    void resetReporter() throws Exception {
        invokeTestingFactory("reset", new Class<?>[0]);
    }

    @Test
    void scopeOpensReporterWithRuntimeIdentityAndEmitsNoReporterRecords() throws Exception {
        ScopeEnumeratorFunction function = new ScopeEnumeratorFunction(configWithReporter(true));
        ProcessOperator<Integer, CleanTask> operator = new ProcessOperator<>(function);
        try (OneInputStreamOperatorTestHarness<Integer, CleanTask> harness =
                new OneInputStreamOperatorTestHarness<>(operator, 3, 3, 2)) {
            harness.open();

            AuditReporterContext context = auditContext(function);
            assertThat(context.getRunId()).isEqualTo(RUN_ID);
            assertThat(context.getStage()).isEqualTo(AuditStage.SCOPE);
            assertThat(context.getOperatorName()).isEqualTo("ScopeEnumerator");
            assertThat(context.getSubtaskIndex()).isEqualTo(2);
            assertThat(context.getAttemptNumber()).isZero();
            assertThat(context.getUserCodeClassLoader())
                    .isSameAs(function.getRuntimeContext().getUserCodeClassLoader());
            assertThat(harness.getRecordOutput()).isEmpty();
        }

        assertThat(testingCalls())
                .containsExactly(
                        "testing:validate", "testing:create", "testing:open", "testing:close");
    }

    @Test
    void requiredReporterOpenFailureFailsScopeOpen() throws Exception {
        invokeTestingFactory(
                "fail",
                new Class<?>[] {String.class, String.class, String.class},
                "testing",
                "open",
                "not-exposed");
        ScopeEnumeratorFunction function = new ScopeEnumeratorFunction(configWithReporter(true));
        OneInputStreamOperatorTestHarness<Integer, CleanTask> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<Integer, CleanTask>(function), 1, 1, 0);

        assertThatThrownBy(harness::open)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("testing")
                .hasMessageContaining("open")
                .hasMessageNotContaining("not-exposed");
        assertThatCode(function::close).doesNotThrowAnyException();
        assertThatCode(function::close).doesNotThrowAnyException();
    }

    @Test
    void scopeCloseAttachesSanitizedFailureToTheExactProcessFailure() throws Exception {
        invokeTestingFactory(
                "fail",
                new Class<?>[] {String.class, String.class, String.class},
                "testing",
                "close",
                "raw-provider-secret");
        ScopeEnumeratorFunction function =
                new ScopeEnumeratorFunction(configWithReporter(true, "127.0.0.1:1"));
        OneInputStreamOperatorTestHarness<Integer, CleanTask> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<Integer, CleanTask>(function), 1, 1, 0);
        harness.open();

        Throwable primary =
                catchThrowable(() -> harness.processElement(new StreamRecord<Integer>(1)));
        assertThat(primary)
                .isNotNull()
                .isNotInstanceOf(AuditReportingException.class)
                .hasNoSuppressedExceptions();

        harness.close();
        assertThatCode(function::close).doesNotThrowAnyException();
        assertSanitizedCleanupSuppressed(primary, "close");
    }

    @Test
    void unopenedStagesSerializeOnlyConfigurationAndDoNotInstantiateReporter() throws Exception {
        OrphanCleanConfig config = configWithReporter(true);
        AuditReporterSpec reporterSpec = config.auditReporterSpec();

        serialize(new ScopeEnumeratorFunction(config));
        serialize(
                new ScanAndCleanFunction(
                        config.remoteFsOpRateLimitPerSecond(),
                        config.extraConfigs(),
                        reporterSpec));
        serialize(new StatsAggregateOperator(config.dryRun(), reporterSpec));

        assertThat(testingInstantiations()).isZero();
        assertNonTransientFields(ScopeEnumeratorFunction.class, OrphanCleanConfig.class);
        assertNonTransientFields(
                ScanAndCleanFunction.class, long.class, Map.class, AuditReporterSpec.class);
        assertNonTransientFields(
                StatsAggregateOperator.class, boolean.class, AuditReporterSpec.class);
        assertTransientField(
                ScopeEnumeratorFunction.class, "auditReporterRuntime", AuditReporterRuntime.class);
        assertTransientField(ScopeEnumeratorFunction.class, "taskFailure", Throwable.class);
        assertTransientField(
                ScanAndCleanFunction.class, "auditReporterRuntime", AuditReporterRuntime.class);
        assertTransientField(ScanAndCleanFunction.class, "taskFailure", Throwable.class);
        assertTransientField(
                StatsAggregateOperator.class, "auditReporterRuntime", AuditReporterRuntime.class);
        assertTransientField(StatsAggregateOperator.class, "taskFailure", Throwable.class);
    }

    @Test
    void buildPipelinePreservesTheThreeStageTopology() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OrphanFilesCleanJob.buildPipeline(env, configWithReporter(true), 4);

        StreamGraph graph = env.getStreamGraph();
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());
        assertThat(nodes).hasSize(4);
        assertThat(nodes)
                .extracting(StreamNode::getOperatorName)
                .containsExactlyInAnyOrder(
                        "Source: Collection Source",
                        "ScopeEnumerator",
                        "ScanAndClean",
                        "StatsAggregate");
        assertThat(nodes)
                .extracting(StreamNode::getOperatorName)
                .noneMatch(
                        name ->
                                name.contains("Reporter")
                                        || name.contains("JDBC")
                                        || name.contains("SLS")
                                        || name.contains("Sink"));

        StreamNode source = node(nodes, "Source: Collection Source");
        StreamNode scope = node(nodes, "ScopeEnumerator");
        StreamNode scan = node(nodes, "ScanAndClean");
        StreamNode stats = node(nodes, "StatsAggregate");
        assertThat(scope.getParallelism()).isEqualTo(1);
        assertThat(maxParallelism(scope)).isEqualTo(1);
        assertThat(scan.getParallelism()).isEqualTo(4);
        assertThat(stats.getParallelism()).isEqualTo(1);
        assertThat(maxParallelism(stats)).isEqualTo(1);

        List<StreamEdge> edges =
                nodes.stream()
                        .flatMap(node -> node.getOutEdges().stream())
                        .collect(Collectors.toList());
        assertThat(edges).hasSize(3);
        assertThat(edges)
                .extracting(StreamEdge::getSourceId, StreamEdge::getTargetId)
                .containsExactlyInAnyOrder(
                        tuple(source.getId(), scope.getId()),
                        tuple(scope.getId(), scan.getId()),
                        tuple(scan.getId(), stats.getId()));
        assertThat(scope.getOutEdges())
                .singleElement()
                .extracting(StreamEdge::getPartitioner)
                .isInstanceOf(RebalancePartitioner.class);
    }

    static AuditReporterSpec reporterSpec(boolean required) {
        return new AuditReporterSpec(
                RUN_ID,
                Collections.singletonList(
                        new AuditReporterSpec.ReporterSpec(
                                "testing", required, Collections.emptyMap())));
    }

    static AuditReporterContext auditContext(Object stage) throws Exception {
        Field auditField = stage.getClass().getDeclaredField("audit");
        auditField.setAccessible(true);
        AuditLogger audit = (AuditLogger) auditField.get(stage);
        Field contextField = AuditLogger.class.getDeclaredField("context");
        contextField.setAccessible(true);
        return (AuditReporterContext) contextField.get(audit);
    }

    static List<String> testingCalls() throws Exception {
        return (List<String>) invokeTestingFactory("calls", new Class<?>[0]);
    }

    static void assertSanitizedCleanupSuppressed(Throwable primary, String phase) {
        assertThat(primary.getSuppressed()).hasSize(1);
        assertThat(primary.getSuppressed()[0])
                .isInstanceOf(AuditReportingException.class)
                .hasMessage("Audit reporter 'testing' failed during " + phase)
                .hasMessageNotContaining("raw-provider-secret")
                .hasNoCause();
    }

    static Object invokeTestingFactory(String method, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method target = TestingAuditReporterFactory.class.getDeclaredMethod(method, parameterTypes);
        target.setAccessible(true);
        return target.invoke(null, args);
    }

    private static int testingInstantiations() throws Exception {
        return (Integer) invokeTestingFactory("totalInstantiations", new Class<?>[0]);
    }

    private static void serialize(Object value) throws Exception {
        try (ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream())) {
            output.writeObject(value);
        }
    }

    private static void assertNonTransientFields(Class<?> type, Class<?>... expectedTypes) {
        assertThat(Arrays.stream(type.getDeclaredFields()))
                .filteredOn(
                        field ->
                                !Modifier.isStatic(field.getModifiers())
                                        && !Modifier.isTransient(field.getModifiers()))
                .extracting(Field::getType)
                .containsExactly(expectedTypes);
    }

    private static void assertTransientField(Class<?> type, String name, Class<?> fieldType)
            throws Exception {
        Field field = type.getDeclaredField(name);
        assertThat(field.getType()).isEqualTo(fieldType);
        assertThat(Modifier.isTransient(field.getModifiers())).isTrue();
    }

    private static int maxParallelism(StreamNode node) throws Exception {
        Method method = StreamNode.class.getDeclaredMethod("getMaxParallelism");
        method.setAccessible(true);
        return (Integer) method.invoke(node);
    }

    private static StreamNode node(List<StreamNode> nodes, String name) {
        return nodes.stream()
                .filter(node -> name.equals(node.getOperatorName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing transformation " + name));
    }

    private static OrphanCleanConfig configWithReporter(boolean required) {
        return configWithReporter(required, "h:9123");
    }

    private static OrphanCleanConfig configWithReporter(boolean required, String bootstrapServer) {
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(
                        new String[] {
                            "--bootstrap-server",
                            bootstrapServer,
                            "--all-databases",
                            "--dry-run",
                            "--conf",
                            "audit.run-id=" + RUN_ID,
                            "--conf",
                            "audit.reporters=testing",
                            "--conf",
                            "audit.reporter.testing.required=" + required
                        }));
    }
}
