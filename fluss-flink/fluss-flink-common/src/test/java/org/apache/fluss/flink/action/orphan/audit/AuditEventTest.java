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

package org.apache.fluss.flink.action.orphan.audit;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class AuditEventTest {

    private static final String EVENT_ID = "43c08df6-8b20-4902-8ff4-205cb8b59fe1";
    private static final String RUN_ID = "3b5939f1-9837-49d8-8a02-945273a0d7e2";

    @Test
    void rejectsMissingRequiredFields() {
        assertThatThrownBy(
                        () ->
                                AuditEvent.builder()
                                        .runId(RUN_ID)
                                        .severity(AuditSeverity.INFO)
                                        .stage(AuditStage.RUN)
                                        .action("run_start")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("eventId");
        assertThatThrownBy(
                        () ->
                                AuditEvent.builder()
                                        .eventId(EVENT_ID)
                                        .severity(AuditSeverity.INFO)
                                        .stage(AuditStage.RUN)
                                        .action("run_start")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId");
        assertThatThrownBy(
                        () ->
                                AuditEvent.builder()
                                        .eventId(EVENT_ID)
                                        .runId(RUN_ID)
                                        .stage(AuditStage.RUN)
                                        .action("run_start")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("severity");
        assertThatThrownBy(
                        () ->
                                AuditEvent.builder()
                                        .eventId(EVENT_ID)
                                        .runId(RUN_ID)
                                        .severity(AuditSeverity.INFO)
                                        .action("run_start")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("stage");
        assertThatThrownBy(
                        () ->
                                AuditEvent.builder()
                                        .eventId(EVENT_ID)
                                        .runId(RUN_ID)
                                        .severity(AuditSeverity.INFO)
                                        .stage(AuditStage.RUN)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("action");
    }

    @Test
    void rejectsInvalidIdentifiersActionAndTimestamp() {
        assertThatThrownBy(() -> validEventBuilder().eventId("").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("eventId");
        assertThatThrownBy(() -> validEventBuilder().eventId("not-a-uuid").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("eventId");
        assertThatThrownBy(() -> validEventBuilder().runId("").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId");
        assertThatThrownBy(() -> validEventBuilder().runId("not-a-uuid").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId");
        assertThatThrownBy(() -> validEventBuilder().action("RunStart").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("action");
        assertThatThrownBy(() -> validEventBuilder().action("run-start").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("action");
        assertThatThrownBy(() -> validEventBuilder().eventTimeMillis(-1L).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("eventTimeMillis");
    }

    @Test
    void validatesMapKeysAndScalarValues() {
        Map<String, String> nullDimensionKey = new LinkedHashMap<>();
        nullDimensionKey.put(null, "value");
        Map<String, String> emptyDimensionKey = new LinkedHashMap<>();
        emptyDimensionKey.put("", "value");
        Map<String, String> nullDimensionValue = new LinkedHashMap<>();
        nullDimensionValue.put("key", null);
        Map<String, String> emptyDimensionValue = new LinkedHashMap<>();
        emptyDimensionValue.put("key", "");
        Map<String, Long> nullMetricValue = new LinkedHashMap<>();
        nullMetricValue.put("key", null);
        Map<String, Boolean> nullFlagValue = new LinkedHashMap<>();
        nullFlagValue.put("key", null);

        assertThatThrownBy(() -> validEventBuilder().dimensions(null).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().metrics(null).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().flags(null).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().dimensions(nullDimensionKey).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().dimensions(emptyDimensionKey).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().dimensions(nullDimensionValue).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().dimensions(emptyDimensionValue).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().metrics(nullMetricValue).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> validEventBuilder().flags(nullFlagValue).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void defensivelyCopiesMapsAndPreservesInsertionOrder() {
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("first", "one");
        dimensions.put("second", "two");
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("files", 2L);
        metrics.put("bytes", 10L);
        Map<String, Boolean> flags = new LinkedHashMap<>();
        flags.put("dry_run", true);
        flags.put("complete", false);

        AuditEvent event =
                validEventBuilder().dimensions(dimensions).metrics(metrics).flags(flags).build();
        dimensions.put("third", "three");
        metrics.put("failures", 1L);
        flags.put("retryable", true);

        assertThat(event.getDimensions())
                .containsExactly(entry("first", "one"), entry("second", "two"));
        assertThat(event.getMetrics()).containsExactly(entry("files", 2L), entry("bytes", 10L));
        assertThat(event.getFlags())
                .containsExactly(entry("dry_run", true), entry("complete", false));
        assertThatThrownBy(() -> event.getDimensions().put("other", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> event.getMetrics().put("other", 3L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> event.getFlags().put("other", true))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void readsAllEnvelopeFieldsWithoutLoss() {
        AuditEvent event =
                validEventBuilder()
                        .eventTimeMillis(1_723_456_789_012L)
                        .severity(AuditSeverity.ERROR)
                        .stage(AuditStage.SCAN)
                        .action("delete_failed")
                        .operatorName("ScanAndClean")
                        .subtaskIndex(3)
                        .attemptNumber(2)
                        .database("inventory")
                        .table("orders")
                        .tableId(101L)
                        .partitionId(202L)
                        .bucketId(7)
                        .scopeKind("table")
                        .objectType("log_segment")
                        .path("oss://bucket/path/file")
                        .sizeBytes(4096L)
                        .mtimeMs(1_723_000_000_000L)
                        .rule("log_segment")
                        .reasonCode("rpc_error")
                        .result("failed")
                        .build();

        assertThat(event.getSchemaVersion()).isEqualTo(AuditEvent.SCHEMA_VERSION).isEqualTo(1);
        assertThat(event.getEventId()).isEqualTo(EVENT_ID);
        assertThat(event.getRunId()).isEqualTo(RUN_ID);
        assertThat(event.getEventTimeMillis()).isEqualTo(1_723_456_789_012L);
        assertThat(event.getSeverity()).isEqualTo(AuditSeverity.ERROR);
        assertThat(event.getStage()).isEqualTo(AuditStage.SCAN);
        assertThat(event.getAction()).isEqualTo("delete_failed");
        assertThat(event.getOperatorName()).isEqualTo("ScanAndClean");
        assertThat(event.getSubtaskIndex()).isEqualTo(3);
        assertThat(event.getAttemptNumber()).isEqualTo(2);
        assertThat(event.getDatabase()).isEqualTo("inventory");
        assertThat(event.getTable()).isEqualTo("orders");
        assertThat(event.getTableId()).isEqualTo(101L);
        assertThat(event.getPartitionId()).isEqualTo(202L);
        assertThat(event.getBucketId()).isEqualTo(7);
        assertThat(event.getScopeKind()).isEqualTo("table");
        assertThat(event.getObjectType()).isEqualTo("log_segment");
        assertThat(event.getPath()).isEqualTo("oss://bucket/path/file");
        assertThat(event.getSizeBytes()).isEqualTo(4096L);
        assertThat(event.getMtimeMs()).isEqualTo(1_723_000_000_000L);
        assertThat(event.getRule()).isEqualTo("log_segment");
        assertThat(event.getReasonCode()).isEqualTo("rpc_error");
        assertThat(event.getResult()).isEqualTo("failed");
        assertThat(event.getDimensions()).isEmpty();
        assertThat(event.getMetrics()).isEmpty();
        assertThat(event.getFlags()).isEmpty();
    }

    @Test
    void reporterContextIsRuntimeOnlyAndReadsAllFields() {
        ClassLoader classLoader = getClass().getClassLoader();
        AuditReporterContext context =
                new AuditReporterContext(
                        RUN_ID, true, AuditStage.SUMMARY, "StatsAggregate", 0, 4, classLoader);

        assertThat(context.getRunId()).isEqualTo(RUN_ID);
        assertThat(context.isDryRun()).isTrue();
        assertThat(context.getStage()).isEqualTo(AuditStage.SUMMARY);
        assertThat(context.getOperatorName()).isEqualTo("StatsAggregate");
        assertThat(context.getSubtaskIndex()).isZero();
        assertThat(context.getAttemptNumber()).isEqualTo(4);
        assertThat(context.getUserCodeClassLoader()).isSameAs(classLoader);
        assertThat(Serializable.class.isAssignableFrom(AuditReporterContext.class)).isFalse();
        assertThat(Serializable.class.isAssignableFrom(AuditEvent.class)).isFalse();
    }

    @Test
    void reporterContextRejectsInvalidRequiredArguments() {
        ClassLoader classLoader = getClass().getClassLoader();

        assertThatThrownBy(
                        () ->
                                new AuditReporterContext(
                                        "not-a-uuid",
                                        false,
                                        AuditStage.RUN,
                                        null,
                                        null,
                                        null,
                                        classLoader))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId");
        assertThatThrownBy(
                        () ->
                                new AuditReporterContext(
                                        RUN_ID, false, null, null, null, null, classLoader))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("stage");
        assertThatThrownBy(
                        () ->
                                new AuditReporterContext(
                                        RUN_ID, false, AuditStage.RUN, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("userCodeClassLoader");
    }

    @Test
    void exposesExactReporterSpi() throws Exception {
        assertThat(AuditReporter.class.getInterfaces()).containsExactly(AutoCloseable.class);
        assertMethod(
                AuditReporter.class.getMethod("open", AuditReporterContext.class),
                void.class,
                Exception.class);
        assertMethod(
                AuditReporter.class.getMethod("report", AuditEvent.class),
                void.class,
                Exception.class);
        assertMethod(AuditReporter.class.getMethod("flush"), void.class, Exception.class);
        assertMethod(AuditReporter.class.getMethod("close"), void.class, Exception.class);

        assertMethod(AuditReporterFactory.class.getMethod("identifier"), String.class);
        assertMethod(AuditReporterFactory.class.getMethod("validate", Map.class), void.class);
        assertMethod(
                AuditReporterFactory.class.getMethod("create", Map.class), AuditReporter.class);
    }

    @Test
    void publicApiDoesNotExposeInternalOrFlinkTypes() {
        for (Class<?> apiType :
                Arrays.asList(
                        AuditEvent.class,
                        AuditEvent.Builder.class,
                        AuditSeverity.class,
                        AuditStage.class,
                        AuditReporter.class,
                        AuditReporterFactory.class,
                        AuditReporterContext.class)) {
            for (Constructor<?> constructor : apiType.getDeclaredConstructors()) {
                if (Modifier.isPublic(constructor.getModifiers())) {
                    for (Type parameter : constructor.getGenericParameterTypes()) {
                        assertProviderNeutral(parameter);
                    }
                }
            }
            for (Method method : apiType.getDeclaredMethods()) {
                if (Modifier.isPublic(method.getModifiers())) {
                    assertProviderNeutral(method.getGenericReturnType());
                    for (Type parameter : method.getGenericParameterTypes()) {
                        assertProviderNeutral(parameter);
                    }
                }
            }
        }
    }

    private static AuditEvent.Builder validEventBuilder() {
        return AuditEvent.builder()
                .eventId(EVENT_ID)
                .runId(RUN_ID)
                .severity(AuditSeverity.INFO)
                .stage(AuditStage.RUN)
                .action("run_start");
    }

    private static void assertMethod(
            Method method, Class<?> returnType, Class<?>... exceptionTypes) {
        assertThat(method.getReturnType()).isEqualTo(returnType);
        assertThat(method.getExceptionTypes()).containsExactly(exceptionTypes);
    }

    private static void assertProviderNeutral(Type type) {
        assertThat(type.getTypeName())
                .doesNotContain(
                        "org.apache.fluss.fs.FsPath",
                        "org.apache.fluss.flink.action.orphan.rule.FileMeta",
                        "org.apache.fluss.flink.action.orphan.audit.ScopeIdentity",
                        "org.apache.fluss.flink.action.orphan.job.CleanupSummary",
                        "org.apache.flink.");
    }
}
