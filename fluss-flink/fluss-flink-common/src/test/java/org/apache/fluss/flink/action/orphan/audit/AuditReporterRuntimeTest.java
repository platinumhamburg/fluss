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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class AuditReporterRuntimeTest {

    private static final String SERVICE_RESOURCE =
            "META-INF/services/org.apache.fluss.flink.action.orphan.audit.AuditReporterFactory";
    private static final String RUN_ID = "123e4567-e89b-12d3-a456-426614174000";
    private static final String EVENT_ID = "123e4567-e89b-12d3-a456-426614174001";
    private static final String OPTION_SECRET = "OptionValueSecret";
    private static final String PATH_SECRET = "s3://bucket/EventPathSecret";
    private static final String REASON_SECRET = "EventReasonSecret";
    private static final String THROWABLE_SECRET = "ThrowableSecret";

    @BeforeEach
    void resetProviderState() {
        TestingAuditReporterFactory.reset();
    }

    @Test
    void emptySpecDoesNotTouchServiceLoaderOrInstantiateProviders() {
        ClassLoader rejectingLoader =
                new ClassLoader(getClass().getClassLoader()) {
                    @Override
                    public Enumeration<URL> getResources(String name) throws IOException {
                        if (SERVICE_RESOURCE.equals(name)) {
                            throw new AssertionError("ServiceLoader was touched");
                        }
                        return super.getResources(name);
                    }
                };

        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(emptySpec(), context(rejectingLoader));

        assertThatCode(() -> runtime.report(secretEvent())).doesNotThrowAnyException();
        assertThatCode(runtime::flush).doesNotThrowAnyException();
        assertThatCode(runtime::close).doesNotThrowAnyException();
        assertThatCode(runtime::close).doesNotThrowAnyException();
        assertThat(TestingAuditReporterFactory.totalInstantiations()).isZero();
        assertThat(TestingAuditReporterFactory.calls()).isEmpty();
    }

    @Test
    void emptySpecStillRejectsNullArgumentsAndReportAfterClose() {
        AuditReportingException nullSpec =
                catchThrowableOfType(
                        () -> AuditReporterRuntime.open(null, context(getClass().getClassLoader())),
                        AuditReportingException.class);
        AuditReportingException nullContext =
                catchThrowableOfType(
                        () -> AuditReporterRuntime.open(emptySpec(), null),
                        AuditReportingException.class);

        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(emptySpec(), context(getClass().getClassLoader()));
        AuditReportingException nullEvent =
                catchThrowableOfType(() -> runtime.report(null), AuditReportingException.class);
        runtime.close();
        AuditReportingException afterClose =
                catchThrowableOfType(
                        () -> runtime.report(secretEvent()), AuditReportingException.class);

        assertFailure(nullSpec, "runtime", "discovery");
        assertFailure(nullContext, "runtime", "discovery");
        assertFailure(nullEvent, "runtime", "report");
        assertFailure(afterClose, "runtime", "report");
        assertThat(TestingAuditReporterFactory.totalInstantiations()).isZero();
        assertThat(TestingAuditReporterFactory.calls()).isEmpty();
    }

    @Test
    void discoveryUsesSuppliedUserCodeClassLoader() throws Exception {
        ClassLoader visibleLoader = getClass().getClassLoader();
        try (URLClassLoader hiddenLoader = new URLClassLoader(new URL[0], null)) {
            AuditReportingException missing =
                    catchThrowableOfType(
                            () ->
                                    AuditReporterRuntime.open(
                                            spec(reporter("testing", true)), context(hiddenLoader)),
                            AuditReportingException.class);
            assertFailure(missing, "testing", "discovery");
        }

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader hiddenGlobalLoader = new URLClassLoader(new URL[0], null)) {
            Thread.currentThread().setContextClassLoader(hiddenGlobalLoader);
            AuditReporterRuntime runtime =
                    AuditReporterRuntime.open(
                            spec(reporter("testing", true)), context(visibleLoader));
            runtime.close();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }

        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "testing:validate", "testing:create", "testing:open", "testing:close");
    }

    @Test
    void missingIdentifierFailsDiscoveryBeforeAnyReporterOpens() {
        AuditReportingException failure =
                catchThrowableOfType(
                        () ->
                                AuditReporterRuntime.open(
                                        spec(reporter("first", true), reporter("missing", true)),
                                        context(getClass().getClassLoader())),
                        AuditReportingException.class);

        assertFailure(failure, "missing", "discovery");
        assertThat(TestingAuditReporterFactory.calls()).isEmpty();
    }

    @Test
    void duplicateVisibleIdentifierFailsDiscoveryBeforeAnyReporterOpens(@TempDir Path tempDir)
            throws Exception {
        try (URLClassLoader loader = duplicateProviderLoader(tempDir)) {
            AuditReportingException failure =
                    catchThrowableOfType(
                            () ->
                                    AuditReporterRuntime.open(
                                            spec(reporter("testing", true)), context(loader)),
                            AuditReportingException.class);

            assertFailure(failure, "testing", "discovery");
            assertThat(TestingAuditReporterFactory.calls()).isEmpty();
        }
    }

    @Test
    void duplicateUnselectedIdentifierDoesNotBlockSelectedProvider(@TempDir Path tempDir)
            throws Exception {
        try (URLClassLoader loader = duplicateProviderLoader(tempDir)) {
            AuditReporterRuntime runtime =
                    AuditReporterRuntime.open(spec(reporter("first", true)), context(loader));
            runtime.close();
        }

        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly("first:validate", "first:create", "first:open", "first:close");
    }

    @Test
    void serviceConfigurationErrorsAreSanitized(@TempDir Path tempDir) throws Exception {
        try (URLClassLoader loader = invalidProviderLoader(tempDir)) {
            AuditReportingException failure =
                    catchThrowableOfType(
                            () ->
                                    AuditReporterRuntime.open(
                                            spec(reporter("testing", true)), context(loader)),
                            AuditReportingException.class);

            assertFailure(failure, "provider", "discovery");
            assertThat(render(failure)).doesNotContain(THROWABLE_SECRET);
            assertThat(TestingAuditReporterFactory.calls()).isEmpty();
        }
    }

    @Test
    void invalidAndNullProviderIdentifiersFailSanitizedDiscovery(@TempDir Path tempDir)
            throws Exception {
        try (URLClassLoader invalidLoader = invalidIdentifierLoader(tempDir.resolve("invalid"))) {
            AuditReportingException invalid =
                    catchThrowableOfType(
                            () ->
                                    AuditReporterRuntime.open(
                                            spec(reporter("testing", true)),
                                            context(invalidLoader)),
                            AuditReportingException.class);
            assertFailure(invalid, "provider", "discovery");
        }

        try (URLClassLoader nullLoader = nullIdentifierLoader(tempDir.resolve("null"))) {
            AuditReportingException nullIdentifier =
                    catchThrowableOfType(
                            () ->
                                    AuditReporterRuntime.open(
                                            spec(reporter("testing", true)), context(nullLoader)),
                            AuditReportingException.class);
            assertFailure(nullIdentifier, "provider", "discovery");
        }

        assertThat(TestingAuditReporterFactory.calls()).isEmpty();
    }

    @Test
    void serviceConfigurationErrorsFromFactoryAndReporterAreSanitized() {
        TestingAuditReporterFactory.failWithServiceConfigurationError(
                "testing", "validate", THROWABLE_SECRET + "Factory");
        AuditReportingException factoryFailure =
                catchThrowableOfType(
                        () ->
                                AuditReporterRuntime.open(
                                        spec(reporter("testing", true, OPTION_SECRET)),
                                        context(getClass().getClassLoader())),
                        AuditReportingException.class);

        assertFailure(factoryFailure, "testing", "validate");
        assertRenderedFailureIsRedacted(factoryFailure);

        TestingAuditReporterFactory.reset();
        TestingAuditReporterFactory.failWithServiceConfigurationError(
                "testing", "report", THROWABLE_SECRET + "Reporter");
        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(reporter("testing", true, OPTION_SECRET)),
                        context(getClass().getClassLoader()));
        AuditReportingException reporterFailure =
                catchThrowableOfType(
                        () -> runtime.report(secretEvent()), AuditReportingException.class);

        assertFailure(reporterFailure, "testing", "report");
        assertRenderedFailureIsRedacted(reporterFailure);
        runtime.close();
    }

    @Test
    void factoryReceivesSeparateImmutableOptionCopies() {
        LinkedHashMap<String, String> configured = new LinkedHashMap<>();
        configured.put("first-key", OPTION_SECRET);
        configured.put("second-key", "second-value");

        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(new ReporterSpec("testing", true, configured)),
                        context(getClass().getClassLoader()));

        Map<String, String> validateOptions =
                TestingAuditReporterFactory.validateOptions("testing");
        Map<String, String> createOptions = TestingAuditReporterFactory.createOptions("testing");
        assertThat(validateOptions).containsExactlyEntriesOf(configured);
        assertThat(createOptions).containsExactlyEntriesOf(configured);
        assertThat(new ArrayList<>(validateOptions.keySet()))
                .containsExactly("first-key", "second-key");
        assertThat(new ArrayList<>(createOptions.keySet()))
                .containsExactly("first-key", "second-key");
        assertThat(validateOptions).isNotSameAs(createOptions).isNotSameAs(configured);
        assertThat(createOptions).isNotSameAs(configured);
        assertThat(TestingAuditReporterFactory.validateOptionsWereImmutable("testing")).isTrue();
        assertThat(TestingAuditReporterFactory.createOptionsWereImmutable("testing")).isTrue();
        runtime.close();
    }

    @Test
    void requiredOpenFailureClosesEarlierReportersInReverseAndAggregatesCleanupFailures() {
        TestingAuditReporterFactory.fail("third", "open", THROWABLE_SECRET + "Open");
        TestingAuditReporterFactory.fail("third", "close", THROWABLE_SECRET + "CloseThird");
        TestingAuditReporterFactory.fail("second", "close", THROWABLE_SECRET + "CloseSecond");
        TestingAuditReporterFactory.fail("first", "close", THROWABLE_SECRET + "CloseFirst");

        AuditReportingException failure =
                catchThrowableOfType(
                        () ->
                                AuditReporterRuntime.open(
                                        spec(
                                                reporter("first", true),
                                                reporter("second", true),
                                                reporter("third", true, OPTION_SECRET)),
                                        context(getClass().getClassLoader())),
                        AuditReportingException.class);

        assertFailure(failure, "third", "open");
        assertSuppressedFailures(failure, "third", "close", "second", "close", "first", "close");
        assertRenderedFailureIsRedacted(failure);
        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "first:validate",
                        "first:create",
                        "first:open",
                        "second:validate",
                        "second:create",
                        "second:open",
                        "third:validate",
                        "third:create",
                        "third:open",
                        "third:close",
                        "second:close",
                        "first:close");
    }

    @Test
    void nullReporterFromCreateFailsSanitizedAndClosesPreviouslyOpenedReporters() {
        TestingAuditReporterFactory.returnNullOnCreate("second");

        AuditReportingException failure =
                catchThrowableOfType(
                        () ->
                                AuditReporterRuntime.open(
                                        spec(reporter("first", true), reporter("second", true)),
                                        context(getClass().getClassLoader())),
                        AuditReportingException.class);

        assertFailure(failure, "second", "create");
        assertThat(TestingAuditReporterFactory.calls())
                .containsExactly(
                        "first:validate",
                        "first:create",
                        "first:open",
                        "second:validate",
                        "second:create",
                        "first:close");
    }

    @Test
    void optionalSetupFailuresWarnOnceEachAndContinue() {
        TestingAuditReporterFactory.fail("testing", "validate", THROWABLE_SECRET + "Validate");
        TestingAuditReporterFactory.fail("first", "create", THROWABLE_SECRET + "Create");
        TestingAuditReporterFactory.fail("second", "open", THROWABLE_SECRET + "Open");
        TestingAuditReporterFactory.fail("second", "close", THROWABLE_SECRET + "Close");
        List<CapturedLog> logs = new CopyOnWriteArrayList<>();

        try (RuntimeLogCapture ignored = new RuntimeLogCapture(logs)) {
            AuditReporterRuntime runtime =
                    AuditReporterRuntime.open(
                            spec(
                                    reporter("testing", false, OPTION_SECRET),
                                    reporter("first", false, OPTION_SECRET),
                                    reporter("second", false, OPTION_SECRET),
                                    reporter("third", true)),
                            context(getClass().getClassLoader()));
            runtime.report(secretEvent());
            runtime.close();
        }

        assertThat(logs).hasSize(3).allMatch(log -> log.level == Level.WARN);
        assertThat(logs)
                .extracting(log -> log.message)
                .containsExactly(
                        "Audit reporter 'testing' failed during validate",
                        "Audit reporter 'first' failed during create",
                        "Audit reporter 'second' failed during open");
        assertLogsAreRedacted(logs);
        assertThat(TestingAuditReporterFactory.calls())
                .containsSubsequence("third:open", "third:report", "third:close")
                .contains("second:close")
                .doesNotContain("testing:create", "first:open", "second:report");
    }

    @Test
    void reportAttemptsAllAndAggregatesRequiredFailuresWithoutSecrets() {
        TestingAuditReporterFactory.fail("first", "report", THROWABLE_SECRET + "FirstReport");
        TestingAuditReporterFactory.fail("third", "report", THROWABLE_SECRET + "ThirdReport");
        AuditReporterRuntime runtime = openThreeRequired();
        AuditEvent event = secretEvent();

        AuditReportingException failure =
                catchThrowableOfType(() -> runtime.report(event), AuditReportingException.class);

        assertFailure(failure, "first", "report");
        assertSuppressedFailures(failure, "third", "report");
        assertRenderedFailureIsRedacted(failure);
        assertThat(TestingAuditReporterFactory.calls())
                .containsSubsequence("first:report", "second:report", "third:report");
        assertThat(TestingAuditReporterFactory.events("first")).containsExactly(event);
        assertThat(TestingAuditReporterFactory.events("second")).containsExactly(event);
        assertThat(TestingAuditReporterFactory.events("third")).containsExactly(event);
        runtime.close();
    }

    @Test
    void flushAttemptsAllAndAggregatesRequiredFailures() {
        TestingAuditReporterFactory.fail("first", "flush", THROWABLE_SECRET + "FirstFlush");
        TestingAuditReporterFactory.fail("third", "flush", THROWABLE_SECRET + "ThirdFlush");
        AuditReporterRuntime runtime = openThreeRequired();

        AuditReportingException failure =
                catchThrowableOfType(runtime::flush, AuditReportingException.class);

        assertFailure(failure, "first", "flush");
        assertSuppressedFailures(failure, "third", "flush");
        assertRenderedFailureIsRedacted(failure);
        assertThat(TestingAuditReporterFactory.calls())
                .containsSubsequence("first:flush", "second:flush", "third:flush");
        runtime.close();
    }

    @Test
    void closeAttemptsAllInReverseAndAggregatesRequiredFailures() {
        TestingAuditReporterFactory.fail("first", "close", THROWABLE_SECRET + "FirstClose");
        TestingAuditReporterFactory.fail("third", "close", THROWABLE_SECRET + "ThirdClose");
        AuditReporterRuntime runtime = openThreeRequired();

        AuditReportingException failure =
                catchThrowableOfType(runtime::close, AuditReportingException.class);

        assertFailure(failure, "third", "close");
        assertSuppressedFailures(failure, "first", "close");
        assertRenderedFailureIsRedacted(failure);
        assertThat(TestingAuditReporterFactory.calls())
                .containsSubsequence("third:close", "second:close", "first:close");
        assertThatCode(runtime::close).doesNotThrowAnyException();
    }

    @Test
    void logsOpenedAndClosedReporterLifecycleWithOnlySafeContext() {
        List<CapturedLog> logs = new CopyOnWriteArrayList<>();

        try (RuntimeLogCapture ignored = new RuntimeLogCapture(logs, Level.INFO)) {
            AuditReporterRuntime runtime =
                    AuditReporterRuntime.open(
                            spec(reporter("testing", true, OPTION_SECRET)),
                            context(getClass().getClassLoader()));
            runtime.close();
        }

        assertThat(logs)
                .extracting(log -> log.level, log -> log.message)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                Level.INFO,
                                "Audit reporter 'testing' opened run_id="
                                        + RUN_ID
                                        + " stage=SCAN operator=runtime-test subtask=1 attempt=0"),
                        org.assertj.core.groups.Tuple.tuple(
                                Level.INFO,
                                "Audit reporter 'testing' closed run_id="
                                        + RUN_ID
                                        + " stage=SCAN operator=runtime-test subtask=1 attempt=0"));
        assertLogsAreRedacted(logs);
    }

    @Test
    void requiredReportFailureLogsErrorWithoutEventOrThrowableSecrets() {
        TestingAuditReporterFactory.fail("testing", "report", THROWABLE_SECRET + "RequiredReport");
        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(reporter("testing", true, OPTION_SECRET)),
                        context(getClass().getClassLoader()));
        List<CapturedLog> logs = new CopyOnWriteArrayList<>();

        try (RuntimeLogCapture ignored = new RuntimeLogCapture(logs, Level.INFO)) {
            AuditReportingException failure =
                    catchThrowableOfType(
                            () -> runtime.report(secretEvent()), AuditReportingException.class);
            assertFailure(failure, "testing", "report");
        }

        assertThat(logs)
                .extracting(log -> log.level, log -> log.message)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                Level.ERROR, "Audit reporter 'testing' failed during report"));
        assertLogsAreRedacted(logs);
        runtime.close();
    }

    @Test
    void optionalReportFailureWarnsExactlyOnceWithoutEventOrThrowableSecrets() {
        TestingAuditReporterFactory.fail("first", "report", THROWABLE_SECRET + "OptionalReport");
        List<CapturedLog> logs = new CopyOnWriteArrayList<>();
        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(reporter("first", false, OPTION_SECRET), reporter("second", true)),
                        context(getClass().getClassLoader()));

        try (RuntimeLogCapture ignored = new RuntimeLogCapture(logs)) {
            assertThatCode(() -> runtime.report(secretEvent())).doesNotThrowAnyException();
        }

        assertThat(logs).hasSize(1);
        assertThat(logs.get(0).level).isEqualTo(Level.WARN);
        assertThat(logs.get(0).message).isEqualTo("Audit reporter 'first' failed during report");
        assertLogsAreRedacted(logs);
        assertThat(TestingAuditReporterFactory.calls())
                .containsSubsequence("first:report", "second:report");
        runtime.close();
    }

    @Test
    void optionalFlushAndCloseFailuresWarnAndDoNotThrow() {
        TestingAuditReporterFactory.fail("first", "flush", THROWABLE_SECRET + "OptionalFlush");
        TestingAuditReporterFactory.fail("second", "close", THROWABLE_SECRET + "OptionalClose");
        List<CapturedLog> logs = new CopyOnWriteArrayList<>();
        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(reporter("first", false), reporter("second", false)),
                        context(getClass().getClassLoader()));

        try (RuntimeLogCapture ignored = new RuntimeLogCapture(logs)) {
            assertThatCode(runtime::flush).doesNotThrowAnyException();
            assertThatCode(runtime::close).doesNotThrowAnyException();
        }

        assertThat(logs).hasSize(2).allMatch(log -> log.level == Level.WARN);
        assertThat(logs)
                .extracting(log -> log.message)
                .containsExactly(
                        "Audit reporter 'first' failed during flush",
                        "Audit reporter 'second' failed during close");
        assertLogsAreRedacted(logs);
    }

    @Test
    void repeatedFlushAndCloseAreSafeAndReportAfterCloseFailsLocally() {
        AuditReporterRuntime runtime =
                AuditReporterRuntime.open(
                        spec(reporter("first", true)), context(getClass().getClassLoader()));

        runtime.flush();
        runtime.flush();
        runtime.close();
        runtime.close();
        runtime.flush();
        AuditReportingException failure =
                catchThrowableOfType(
                        () -> runtime.report(secretEvent()), AuditReportingException.class);

        assertFailure(failure, "runtime", "report");
        assertRenderedFailureIsRedacted(failure);
        assertThat(TestingAuditReporterFactory.callCount("first:flush")).isEqualTo(2);
        assertThat(TestingAuditReporterFactory.callCount("first:close")).isEqualTo(1);
        assertThat(TestingAuditReporterFactory.callCount("first:report")).isZero();
    }

    private AuditReporterRuntime openThreeRequired() {
        return AuditReporterRuntime.open(
                spec(
                        reporter("first", true, OPTION_SECRET),
                        reporter("second", true),
                        reporter("third", true)),
                context(getClass().getClassLoader()));
    }

    private static AuditReporterSpec emptySpec() {
        return new AuditReporterSpec(RUN_ID, Collections.<ReporterSpec>emptyList());
    }

    private static AuditReporterSpec spec(ReporterSpec... reporters) {
        List<ReporterSpec> specs = new ArrayList<>();
        Collections.addAll(specs, reporters);
        return new AuditReporterSpec(RUN_ID, specs);
    }

    private static ReporterSpec reporter(String identifier, boolean required) {
        return new ReporterSpec(identifier, required, Collections.<String, String>emptyMap());
    }

    private static ReporterSpec reporter(String identifier, boolean required, String optionSecret) {
        LinkedHashMap<String, String> options = new LinkedHashMap<>();
        options.put("endpoint", optionSecret);
        return new ReporterSpec(identifier, required, options);
    }

    private static AuditReporterContext context(ClassLoader classLoader) {
        return new AuditReporterContext(
                RUN_ID, true, AuditStage.SCAN, "runtime-test", 1, 0, classLoader);
    }

    private static AuditEvent secretEvent() {
        return AuditEvent.builder()
                .eventId(EVENT_ID)
                .runId(RUN_ID)
                .eventTimeMillis(1L)
                .severity(AuditSeverity.ERROR)
                .stage(AuditStage.SCAN)
                .action("runtime_test")
                .path(PATH_SECRET)
                .reasonCode(REASON_SECRET)
                .build();
    }

    private static void assertFailure(
            AuditReportingException failure, String identifier, String phase) {
        assertThat(failure).isNotNull().hasNoCause();
        assertThat(failure.getMessage())
                .isEqualTo("Audit reporter '" + identifier + "' failed during " + phase);
    }

    private static void assertSuppressedFailures(
            AuditReportingException failure, String... identifierAndPhase) {
        assertThat(identifierAndPhase.length % 2).isZero();
        Throwable[] suppressed = failure.getSuppressed();
        assertThat(suppressed).hasSize(identifierAndPhase.length / 2);
        for (int i = 0; i < suppressed.length; i++) {
            assertThat(suppressed[i])
                    .isInstanceOf(AuditReportingException.class)
                    .hasNoCause()
                    .hasMessage(
                            "Audit reporter '"
                                    + identifierAndPhase[i * 2]
                                    + "' failed during "
                                    + identifierAndPhase[i * 2 + 1]);
        }
    }

    private static void assertRenderedFailureIsRedacted(AuditReportingException failure) {
        assertThat(render(failure))
                .doesNotContain(OPTION_SECRET)
                .doesNotContain(PATH_SECRET)
                .doesNotContain(REASON_SECRET)
                .doesNotContain(THROWABLE_SECRET)
                .doesNotContain("FactoryToStringSecret")
                .doesNotContain("ReporterToStringSecret");
    }

    private static String render(Throwable throwable) {
        StringWriter rendered = new StringWriter();
        throwable.printStackTrace(new PrintWriter(rendered));
        return rendered.toString();
    }

    private static void assertLogsAreRedacted(List<CapturedLog> logs) {
        assertThat(logs)
                .extracting(log -> log.message)
                .allMatch(
                        message ->
                                !message.contains(OPTION_SECRET)
                                        && !message.contains(PATH_SECRET)
                                        && !message.contains(REASON_SECRET)
                                        && !message.contains(THROWABLE_SECRET)
                                        && !message.contains("FactoryToStringSecret")
                                        && !message.contains("ReporterToStringSecret"));
    }

    private URLClassLoader duplicateProviderLoader(Path tempDir) throws IOException {
        return serviceLoader(
                tempDir,
                "org.apache.fluss.flink.action.orphan.audit."
                        + "TestingAuditReporterFactory$DuplicateTestingFactory\n");
    }

    private URLClassLoader invalidProviderLoader(Path tempDir) throws IOException {
        return serviceLoader(tempDir, "missing." + THROWABLE_SECRET + "Provider\n");
    }

    private URLClassLoader invalidIdentifierLoader(Path tempDir) throws IOException {
        return serviceLoader(
                tempDir,
                "org.apache.fluss.flink.action.orphan.audit."
                        + "TestingAuditReporterFactory$InvalidIdentifierFactory\n");
    }

    private URLClassLoader nullIdentifierLoader(Path tempDir) throws IOException {
        return serviceLoader(
                tempDir,
                "org.apache.fluss.flink.action.orphan.audit."
                        + "TestingAuditReporterFactory$NullIdentifierFactory\n");
    }

    private URLClassLoader serviceLoader(Path tempDir, String descriptor) throws IOException {
        Path service = tempDir.resolve(SERVICE_RESOURCE);
        Files.createDirectories(service.getParent());
        Files.write(service, descriptor.getBytes(StandardCharsets.UTF_8));
        return new ChildFirstServiceClassLoader(
                new URL[] {tempDir.toUri().toURL()}, getClass().getClassLoader());
    }

    private static final class ChildFirstServiceClassLoader extends URLClassLoader {
        private ChildFirstServiceClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (!SERVICE_RESOURCE.equals(name)) {
                return super.getResources(name);
            }
            Vector<URL> resources = new Vector<>();
            resources.addAll(Collections.list(findResources(name)));
            resources.addAll(Collections.list(getParent().getResources(name)));
            return resources.elements();
        }
    }

    private static final class RuntimeLogCapture implements AutoCloseable {
        private final LoggerContext context;
        private final org.apache.logging.log4j.core.config.Configuration configuration;
        private final LoggerConfig loggerConfig;
        private final String loggerName;
        private final CapturingAppender appender;

        private RuntimeLogCapture(List<CapturedLog> logs) {
            this(logs, Level.WARN);
        }

        private RuntimeLogCapture(List<CapturedLog> logs, Level level) {
            context = (LoggerContext) LogManager.getContext(false);
            configuration = context.getConfiguration();
            loggerName = AuditReporterRuntime.class.getName();
            appender = new CapturingAppender("audit-reporter-runtime-test", logs);
            appender.start();
            loggerConfig = new LoggerConfig(loggerName, level, false);
            loggerConfig.addAppender(appender, level, null);
            configuration.addLogger(loggerName, loggerConfig);
            context.updateLoggers();
        }

        @Override
        public void close() {
            loggerConfig.removeAppender(appender.getName());
            configuration.removeLogger(loggerName);
            context.updateLoggers();
            appender.stop();
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<CapturedLog> logs;

        private CapturingAppender(String name, List<CapturedLog> logs) {
            super(name, null, null, false, null);
            this.logs = logs;
        }

        @Override
        public void append(LogEvent event) {
            logs.add(new CapturedLog(event.getLevel(), event.getMessage().getFormattedMessage()));
        }
    }

    private static final class CapturedLog {
        private final Level level;
        private final String message;

        private CapturedLog(Level level, String message) {
            this.level = level;
            this.message = message;
        }
    }
}
