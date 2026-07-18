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
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory.OpenContextSnapshot;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrphanFilesCleanJobTest {

    private static final String RUN_ID = "00000000-0000-0000-0000-000000000005";

    @BeforeEach
    void resetReporterProbe() {
        TestingAuditReporterFactory.reset();
    }

    @Test
    void scopeOpenUsesActualRuntimeIdentity() throws Exception {
        ScopeEnumeratorFunction function = new ScopeEnumeratorFunction(reportingConfig(true));
        int attemptNumber = 4;
        ClassLoader userCodeClassLoader = getClass().getClassLoader();
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.getTaskInfo())
                .thenReturn(new TaskInfoImpl("ScopeEnumerator", 1, 0, 1, attemptNumber));
        when(runtimeContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        function.setRuntimeContext(runtimeContext);

        try {
            function.open(new OpenContext() {});

            assertThat(TestingAuditReporterFactory.openContexts("testing"))
                    .singleElement()
                    .satisfies(
                            context ->
                                    assertRuntimeIdentity(
                                            context,
                                            AuditStage.SCOPE,
                                            "ScopeEnumerator",
                                            0,
                                            attemptNumber,
                                            userCodeClassLoader));
        } finally {
            function.close();
        }
    }

    @Test
    void requiredReporterOpenFailureFailsScopeHarnessOpen() throws Exception {
        TestingAuditReporterFactory.fail("testing", "open", "injected-open-failure");
        ScopeEnumeratorFunction function = new ScopeEnumeratorFunction(reportingConfig(true));
        try (OneInputStreamOperatorTestHarness<Integer, CleanTask> harness =
                scopeHarness(function)) {
            assertThatThrownBy(harness::open)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("testing")
                    .hasMessageContaining("open")
                    .hasMessageNotContaining("injected-open-failure");
        }
    }

    @Test
    void scopeCloseFallbackFlushesThenClosesReporterExactlyOnce() throws Exception {
        ScopeEnumeratorFunction function = new ScopeEnumeratorFunction(reportingConfig(true));
        OneInputStreamOperatorTestHarness<Integer, CleanTask> harness = scopeHarness(function);
        try {
            harness.open();
        } finally {
            harness.close();
        }

        function.close();

        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "testing:validate",
                        "testing:create",
                        "testing:open",
                        "testing:flush",
                        "testing:close");
    }

    @Test
    void scopeTriggerFailurePreservesProcessingFailureAndSuppressesLifecycleFailures()
            throws Exception {
        TestingAuditReporterFactory.fail("testing", "flush", "injected-flush-failure");
        TestingAuditReporterFactory.fail("testing", "close", "injected-close-failure");
        ScopeEnumeratorFunction function =
                new ScopeEnumeratorFunction(reportingConfigWithUnreachableBootstrap());
        OneInputStreamOperatorTestHarness<Integer, CleanTask> harness = scopeHarness(function);
        try {
            harness.open();

            assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bootstrap servers")
                    .satisfies(
                            processingFailure ->
                                    assertThat(processingFailure.getSuppressed())
                                            .singleElement()
                                            .satisfies(
                                                    lifecycleFailure -> {
                                                        assertThat(lifecycleFailure)
                                                                .hasMessageContaining("testing")
                                                                .hasMessageContaining("flush")
                                                                .hasMessageNotContaining(
                                                                        "injected-flush-failure");
                                                        assertThat(lifecycleFailure.getSuppressed())
                                                                .singleElement()
                                                                .satisfies(
                                                                        closeFailure ->
                                                                                assertThat(
                                                                                                closeFailure)
                                                                                        .hasMessageContaining(
                                                                                                "testing")
                                                                                        .hasMessageContaining(
                                                                                                "close")
                                                                                        .hasMessageNotContaining(
                                                                                                "injected-close-failure"));
                                                    }));

            assertThat(TestingAuditReporterFactory.calls())
                    .containsExactly(
                            "testing:validate",
                            "testing:create",
                            "testing:open",
                            "testing:flush",
                            "testing:close");

            function.close();
            assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
            assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
        } finally {
            harness.close();
        }

        assertThat(TestingAuditReporterFactory.callCount("testing:flush")).isEqualTo(1);
        assertThat(TestingAuditReporterFactory.callCount("testing:close")).isEqualTo(1);
    }

    @Test
    void unopenedStagesSerializeOnlyConfigurationAndDoNotInstantiateReporters() throws Exception {
        OrphanCleanConfig config = reportingConfig(true);
        ScopeEnumeratorFunction scope = new ScopeEnumeratorFunction(config);
        ScanAndCleanFunction scan = newScanFunction(config.auditReporterSpec(), config.dryRun());
        StatsAggregateOperator stats =
                newStatsOperator(config.dryRun(), config.auditReporterSpec());

        assertSerializedConfigurationOnly(
                scope, new HashSet<Class<?>>(Collections.singletonList(OrphanCleanConfig.class)));
        assertSerializedConfigurationOnly(
                scan, new HashSet<Class<?>>(Arrays.asList(Map.class, AuditReporterSpec.class)));
        assertSerializedConfigurationOnly(
                stats, new HashSet<Class<?>>(Collections.singletonList(AuditReporterSpec.class)));
        assertThat(TestingAuditReporterFactory.totalInstantiations()).isZero();
        assertThat(TestingAuditReporterFactory.calls()).isEmpty();
    }

    @Test
    void buildPipelinePreservesTheExistingFourNodeDag() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int scanParallelism = 3;

        DataStream<CleanupSummary> result =
                invokeBuildPipeline(env, reportingConfig(true), scanParallelism);

        assertThat(result).isNotNull();
        assertThat(env.getTransformations())
                .extracting(Transformation::getName)
                .containsExactly("ScopeEnumerator", "ScanAndClean", "StatsAggregate");

        Map<String, Transformation<?>> transformations =
                env.getTransformations().stream()
                        .collect(
                                Collectors.toMap(
                                        Transformation::getName, transformation -> transformation));
        assertParallelism(transformations.get("ScopeEnumerator"), 1, 1);
        assertThat(transformations.get("ScanAndClean").getParallelism()).isEqualTo(scanParallelism);
        assertParallelism(transformations.get("StatsAggregate"), 1, 1);

        StreamGraph graph = env.getStreamGraph(false);
        Map<Integer, String> namesById =
                graph.getStreamNodes().stream()
                        .collect(
                                Collectors.toMap(
                                        StreamNode::getId,
                                        node -> semanticOperatorName(node.getOperatorName())));
        assertThat(namesById.values())
                .containsExactlyInAnyOrder(
                        "Collection Source", "ScopeEnumerator", "ScanAndClean", "StatsAggregate");

        Collection<StreamEdge> edges =
                graph.getStreamNodes().stream()
                        .flatMap(node -> node.getOutEdges().stream())
                        .collect(Collectors.toList());
        assertThat(edges)
                .extracting(
                        edge ->
                                namesById.get(edge.getSourceId())
                                        + " -> "
                                        + namesById.get(edge.getTargetId()))
                .containsExactlyInAnyOrder(
                        "Collection Source -> ScopeEnumerator",
                        "ScopeEnumerator -> ScanAndClean",
                        "ScanAndClean -> StatsAggregate");
        assertThat(edges)
                .filteredOn(
                        edge ->
                                "ScopeEnumerator".equals(namesById.get(edge.getSourceId()))
                                        && "ScanAndClean".equals(namesById.get(edge.getTargetId())))
                .singleElement()
                .extracting(StreamEdge::getPartitioner)
                .isInstanceOf(RebalancePartitioner.class);

        assertThat(transformations.keySet())
                .allSatisfy(
                        name ->
                                assertThat(name.toLowerCase())
                                        .doesNotContain("reporter", "jdbc", "sls", "sink"));
    }

    @Test
    void executablePipelineUsesDiscardingTerminalSinkInsteadOfCollectSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int scanParallelism = 3;

        OrphanFilesCleanJob.buildExecutablePipeline(env, reportingConfig(true), scanParallelism);

        assertThat(env.getTransformations())
                .extracting(Transformation::getName)
                .containsExactly("ScopeEnumerator", "ScanAndClean", "StatsAggregate", "end");

        Map<String, Transformation<?>> transformations =
                env.getTransformations().stream()
                        .collect(
                                Collectors.toMap(
                                        Transformation::getName, transformation -> transformation));
        assertParallelism(transformations.get("ScopeEnumerator"), 1, 1);
        assertThat(transformations.get("ScanAndClean").getParallelism()).isEqualTo(scanParallelism);
        assertParallelism(transformations.get("StatsAggregate"), 1, 1);
        assertThat(transformations.get("end").getParallelism()).isEqualTo(1);

        StreamGraph graph = env.getStreamGraph(false);
        Map<Integer, String> namesById =
                graph.getStreamNodes().stream()
                        .collect(
                                Collectors.toMap(
                                        StreamNode::getId,
                                        node -> semanticOperatorName(node.getOperatorName())));
        assertThat(namesById.values())
                .containsExactlyInAnyOrder(
                        "Collection Source",
                        "ScopeEnumerator",
                        "ScanAndClean",
                        "StatsAggregate",
                        "end: Writer");

        Collection<StreamEdge> edges =
                graph.getStreamNodes().stream()
                        .flatMap(node -> node.getOutEdges().stream())
                        .collect(Collectors.toList());
        assertThat(edges)
                .extracting(
                        edge ->
                                namesById.get(edge.getSourceId())
                                        + " -> "
                                        + namesById.get(edge.getTargetId()))
                .containsExactlyInAnyOrder(
                        "Collection Source -> ScopeEnumerator",
                        "ScopeEnumerator -> ScanAndClean",
                        "ScanAndClean -> StatsAggregate",
                        "StatsAggregate -> end: Writer");
        assertThat(namesById.values())
                .allSatisfy(name -> assertThat(name.toLowerCase()).doesNotContain("collect sink"));
    }

    private static String semanticOperatorName(String operatorName) {
        String sourcePrefix = "Source: ";
        String sinkPrefix = "Sink: ";
        if (operatorName.startsWith(sourcePrefix)) {
            return operatorName.substring(sourcePrefix.length());
        }
        return operatorName.startsWith(sinkPrefix)
                ? operatorName.substring(sinkPrefix.length())
                : operatorName;
    }

    private static OneInputStreamOperatorTestHarness<Integer, CleanTask> scopeHarness(
            ScopeEnumeratorFunction function) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(function), 1, 1, 0);
    }

    private static void assertSerializedConfigurationOnly(
            Object stage, Set<Class<?>> allowedReferenceTypes) throws IOException {
        byte[] serialized = serialize(stage);
        String descriptors = new String(serialized, StandardCharsets.ISO_8859_1);
        assertThat(descriptors).contains(AuditReporterSpec.class.getName());
        assertThat(descriptors)
                .doesNotContain(
                        AuditReporterRuntime.class.getName(),
                        AuditLogger.class.getName(),
                        TestingAuditReporterFactory.class.getName());

        assertThat(Arrays.asList(stage.getClass().getDeclaredFields()))
                .filteredOn(
                        field ->
                                !Modifier.isStatic(field.getModifiers())
                                        && !Modifier.isTransient(field.getModifiers())
                                        && !field.getType().isPrimitive())
                .extracting(Field::getType)
                .allMatch(allowedReferenceTypes::contains);
    }

    private static byte[] serialize(Object value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static void assertParallelism(
            Transformation<?> transformation, int parallelism, int maxParallelism) {
        assertThat(transformation.getParallelism()).isEqualTo(parallelism);
        assertThat(transformation.getMaxParallelism()).isEqualTo(maxParallelism);
    }

    @SuppressWarnings("unchecked")
    private static DataStream<CleanupSummary> invokeBuildPipeline(
            StreamExecutionEnvironment env, OrphanCleanConfig config, Integer parallelism) {
        Method method;
        try {
            method =
                    OrphanFilesCleanJob.class.getDeclaredMethod(
                            "buildPipeline",
                            StreamExecutionEnvironment.class,
                            OrphanCleanConfig.class,
                            Integer.class);
        } catch (NoSuchMethodException missingTaskFiveMethod) {
            throw new AssertionError(
                    "Task 5 requires package-visible static OrphanFilesCleanJob.buildPipeline",
                    missingTaskFiveMethod);
        }
        int modifiers = method.getModifiers();
        assertThat(Modifier.isStatic(modifiers)).isTrue();
        assertThat(Modifier.isPublic(modifiers)).isFalse();
        assertThat(Modifier.isProtected(modifiers)).isFalse();
        assertThat(Modifier.isPrivate(modifiers)).isFalse();
        try {
            return (DataStream<CleanupSummary>) method.invoke(null, env, config, parallelism);
        } catch (IllegalAccessException | InvocationTargetException reflectionFailure) {
            throw new AssertionError(
                    "Unable to invoke OrphanFilesCleanJob.buildPipeline", reflectionFailure);
        }
    }

    private static ScanAndCleanFunction newScanFunction(
            AuditReporterSpec reporterSpec, boolean dryRun) {
        try {
            Constructor<ScanAndCleanFunction> constructor =
                    ScanAndCleanFunction.class.getDeclaredConstructor(
                            long.class, Map.class, AuditReporterSpec.class, boolean.class);
            Map<String, String> fileConfigs = new HashMap<>();
            fileConfigs.put("fs.test.root", "file:///tmp/orphan-cleanup");
            return constructor.newInstance(100L, fileConfigs, reporterSpec, dryRun);
        } catch (NoSuchMethodException missingTaskFiveConstructor) {
            throw new AssertionError(
                    "Task 5 requires ScanAndCleanFunction(long, Map, AuditReporterSpec, boolean)",
                    missingTaskFiveConstructor);
        } catch (ReflectiveOperationException reflectionFailure) {
            throw new AssertionError("Unable to construct ScanAndCleanFunction", reflectionFailure);
        }
    }

    private static StatsAggregateOperator newStatsOperator(
            boolean dryRun, AuditReporterSpec reporterSpec) {
        try {
            Constructor<StatsAggregateOperator> constructor =
                    StatsAggregateOperator.class.getDeclaredConstructor(
                            boolean.class, AuditReporterSpec.class);
            return constructor.newInstance(dryRun, reporterSpec);
        } catch (NoSuchMethodException missingTaskFiveConstructor) {
            throw new AssertionError(
                    "Task 5 requires StatsAggregateOperator(boolean, AuditReporterSpec)",
                    missingTaskFiveConstructor);
        } catch (ReflectiveOperationException reflectionFailure) {
            throw new AssertionError(
                    "Unable to construct StatsAggregateOperator", reflectionFailure);
        }
    }

    private static void assertRuntimeIdentity(
            OpenContextSnapshot context,
            AuditStage stage,
            String operatorName,
            int subtaskIndex,
            int attemptNumber,
            ClassLoader userCodeClassLoader) {
        assertThat(context.getRunId()).isEqualTo(RUN_ID);
        assertThat(context.isDryRun()).isTrue();
        assertThat(context.getStage()).isEqualTo(stage);
        assertThat(context.getOperatorName()).isEqualTo(operatorName);
        assertThat(context.getSubtaskIndex()).isEqualTo(subtaskIndex);
        assertThat(context.getAttemptNumber()).isEqualTo(attemptNumber);
        assertThat(context.getUserCodeClassLoader()).isSameAs(userCodeClassLoader);
    }

    private static OrphanCleanConfig reportingConfig(boolean required) {
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
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

    private static OrphanCleanConfig reportingConfigWithUnreachableBootstrap() {
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(
                        new String[] {
                            "--bootstrap-server",
                            "localhost:9123",
                            "--all-databases",
                            "--dry-run",
                            "--conf",
                            "audit.run-id=" + RUN_ID,
                            "--conf",
                            "audit.reporters=testing",
                            "--conf",
                            "audit.reporter.testing.required=true"
                        }));
    }
}
