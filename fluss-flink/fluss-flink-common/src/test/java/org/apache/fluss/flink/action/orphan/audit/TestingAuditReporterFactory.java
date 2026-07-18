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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.Set;

/** Stateful test providers for {@link AuditReporterRuntimeTest}. */
public final class TestingAuditReporterFactory {

    private static final Object LOCK = new Object();
    private static final List<String> CALLS = new ArrayList<>();
    private static final Map<String, Integer> INSTANTIATIONS = new HashMap<>();
    private static final Map<String, String> FAILURES = new HashMap<>();
    private static final Map<String, String> SERVICE_CONFIGURATION_FAILURES = new HashMap<>();
    private static final Set<String> NULL_REPORTERS = new HashSet<>();
    private static final Map<String, Map<String, String>> VALIDATE_OPTIONS = new HashMap<>();
    private static final Map<String, Map<String, String>> CREATE_OPTIONS = new HashMap<>();
    private static final Map<String, Boolean> VALIDATE_IMMUTABLE = new HashMap<>();
    private static final Map<String, Boolean> CREATE_IMMUTABLE = new HashMap<>();
    private static final Map<String, List<AuditEvent>> EVENTS = new HashMap<>();
    private static final Map<String, List<OpenContextSnapshot>> OPEN_CONTEXTS = new HashMap<>();

    private TestingAuditReporterFactory() {}

    public static void reset() {
        synchronized (LOCK) {
            CALLS.clear();
            INSTANTIATIONS.clear();
            FAILURES.clear();
            SERVICE_CONFIGURATION_FAILURES.clear();
            NULL_REPORTERS.clear();
            VALIDATE_OPTIONS.clear();
            CREATE_OPTIONS.clear();
            VALIDATE_IMMUTABLE.clear();
            CREATE_IMMUTABLE.clear();
            EVENTS.clear();
            OPEN_CONTEXTS.clear();
        }
    }

    public static void fail(String identifier, String phase, String message) {
        synchronized (LOCK) {
            FAILURES.put(key(identifier, phase), message);
        }
    }

    static void failWithServiceConfigurationError(String identifier, String phase, String message) {
        synchronized (LOCK) {
            SERVICE_CONFIGURATION_FAILURES.put(key(identifier, phase), message);
        }
    }

    static void returnNullOnCreate(String identifier) {
        synchronized (LOCK) {
            NULL_REPORTERS.add(identifier);
        }
    }

    public static List<String> calls() {
        synchronized (LOCK) {
            return new ArrayList<>(CALLS);
        }
    }

    public static int callCount(String call) {
        synchronized (LOCK) {
            int count = 0;
            for (String actual : CALLS) {
                if (call.equals(actual)) {
                    count++;
                }
            }
            return count;
        }
    }

    public static int totalInstantiations() {
        synchronized (LOCK) {
            int total = 0;
            for (Integer count : INSTANTIATIONS.values()) {
                total += count;
            }
            return total;
        }
    }

    static Map<String, String> validateOptions(String identifier) {
        synchronized (LOCK) {
            return VALIDATE_OPTIONS.get(identifier);
        }
    }

    static Map<String, String> createOptions(String identifier) {
        synchronized (LOCK) {
            return CREATE_OPTIONS.get(identifier);
        }
    }

    static boolean validateOptionsWereImmutable(String identifier) {
        synchronized (LOCK) {
            return Boolean.TRUE.equals(VALIDATE_IMMUTABLE.get(identifier));
        }
    }

    static boolean createOptionsWereImmutable(String identifier) {
        synchronized (LOCK) {
            return Boolean.TRUE.equals(CREATE_IMMUTABLE.get(identifier));
        }
    }

    public static List<AuditEvent> events(String identifier) {
        synchronized (LOCK) {
            List<AuditEvent> events = EVENTS.get(identifier);
            return events == null ? new ArrayList<AuditEvent>() : new ArrayList<>(events);
        }
    }

    /** Returns immutable snapshots of the contexts supplied to a reporter's {@code open}. */
    public static List<OpenContextSnapshot> openContexts(String identifier) {
        synchronized (LOCK) {
            List<OpenContextSnapshot> contexts = OPEN_CONTEXTS.get(identifier);
            if (contexts == null) {
                return Collections.emptyList();
            }
            return Collections.unmodifiableList(new ArrayList<>(contexts));
        }
    }

    private static void instantiated(String identifier) {
        synchronized (LOCK) {
            Integer count = INSTANTIATIONS.get(identifier);
            INSTANTIATIONS.put(identifier, count == null ? 1 : count + 1);
        }
    }

    private static void called(String identifier, String phase) {
        synchronized (LOCK) {
            CALLS.add(key(identifier, phase));
        }
    }

    private static void recordEvent(String identifier, AuditEvent event) {
        synchronized (LOCK) {
            List<AuditEvent> events = EVENTS.get(identifier);
            if (events == null) {
                events = new ArrayList<>();
                EVENTS.put(identifier, events);
            }
            events.add(event);
        }
    }

    private static void recordOpenContext(String identifier, AuditReporterContext context) {
        synchronized (LOCK) {
            List<OpenContextSnapshot> contexts = OPEN_CONTEXTS.get(identifier);
            if (contexts == null) {
                contexts = new ArrayList<>();
                OPEN_CONTEXTS.put(identifier, contexts);
            }
            contexts.add(new OpenContextSnapshot(context));
        }
    }

    private static void throwIfConfigured(String identifier, String phase) throws Exception {
        String message;
        String serviceConfigurationMessage;
        synchronized (LOCK) {
            message = FAILURES.get(key(identifier, phase));
            serviceConfigurationMessage =
                    SERVICE_CONFIGURATION_FAILURES.get(key(identifier, phase));
        }
        if (serviceConfigurationMessage != null) {
            throw new ServiceConfigurationError(serviceConfigurationMessage);
        }
        if (message != null) {
            throw new Exception(message);
        }
    }

    private static boolean rejectsMutation(Map<String, String> options) {
        try {
            options.put("test-mutation", "test-value");
            options.remove("test-mutation");
            return false;
        } catch (UnsupportedOperationException expected) {
            return true;
        }
    }

    private static String key(String identifier, String phase) {
        return identifier + ":" + phase;
    }

    private static boolean returnsNullReporter(String identifier) {
        synchronized (LOCK) {
            return NULL_REPORTERS.contains(identifier);
        }
    }

    /** Common test factory implementation. */
    public abstract static class BaseFactory implements AuditReporterFactory {
        private final String identifier;

        BaseFactory(String identifier) {
            this.identifier = identifier;
            instantiated(identifier);
        }

        @Override
        public final String identifier() {
            return identifier;
        }

        @Override
        public final void validate(Map<String, String> options) {
            called(identifier, "validate");
            synchronized (LOCK) {
                VALIDATE_OPTIONS.put(identifier, options);
                VALIDATE_IMMUTABLE.put(identifier, rejectsMutation(options));
            }
            try {
                throwIfConfigured(identifier, "validate");
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        @Override
        public final AuditReporter create(Map<String, String> options) {
            called(identifier, "create");
            synchronized (LOCK) {
                CREATE_OPTIONS.put(identifier, options);
                CREATE_IMMUTABLE.put(identifier, rejectsMutation(options));
            }
            try {
                throwIfConfigured(identifier, "create");
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage());
            }
            if (returnsNullReporter(identifier)) {
                return null;
            }
            return new TestingReporter(identifier);
        }

        @Override
        public final String toString() {
            throw new AssertionError("FactoryToStringSecret");
        }
    }

    /** Provider with identifier {@code testing}. */
    public static final class TestingFactory extends BaseFactory {
        public TestingFactory() {
            super("testing");
        }
    }

    /** Provider with identifier {@code first}. */
    public static final class FirstFactory extends BaseFactory {
        public FirstFactory() {
            super("first");
        }
    }

    /** Provider with identifier {@code second}. */
    public static final class SecondFactory extends BaseFactory {
        public SecondFactory() {
            super("second");
        }
    }

    /** Provider with identifier {@code third}. */
    public static final class ThirdFactory extends BaseFactory {
        public ThirdFactory() {
            super("third");
        }
    }

    /** Additional provider used only by a temporary duplicate-provider descriptor. */
    public static final class DuplicateTestingFactory extends BaseFactory {
        public DuplicateTestingFactory() {
            super("testing");
        }
    }

    /** Provider with an invalid identifier used only by a temporary service descriptor. */
    public static final class InvalidIdentifierFactory implements AuditReporterFactory {

        @Override
        public String identifier() {
            return "Invalid";
        }

        @Override
        public void validate(Map<String, String> options) {
            throw new AssertionError("invalid identifier provider must not be validated");
        }

        @Override
        public AuditReporter create(Map<String, String> options) {
            throw new AssertionError("invalid identifier provider must not be created");
        }
    }

    /** Provider with a null identifier used only by a temporary service descriptor. */
    public static final class NullIdentifierFactory implements AuditReporterFactory {

        @Override
        public String identifier() {
            return null;
        }

        @Override
        public void validate(Map<String, String> options) {
            throw new AssertionError("null identifier provider must not be validated");
        }

        @Override
        public AuditReporter create(Map<String, String> options) {
            throw new AssertionError("null identifier provider must not be created");
        }
    }

    private static final class TestingReporter implements AuditReporter {
        private final String identifier;

        private TestingReporter(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public void open(AuditReporterContext context) throws Exception {
            called(identifier, "open");
            recordOpenContext(identifier, context);
            throwIfConfigured(identifier, "open");
        }

        @Override
        public void report(AuditEvent event) throws Exception {
            called(identifier, "report");
            recordEvent(identifier, event);
            throwIfConfigured(identifier, "report");
        }

        @Override
        public void flush() throws Exception {
            called(identifier, "flush");
            throwIfConfigured(identifier, "flush");
        }

        @Override
        public void close() throws Exception {
            called(identifier, "close");
            throwIfConfigured(identifier, "close");
        }

        @Override
        public String toString() {
            throw new AssertionError("ReporterToStringSecret");
        }
    }

    /** Immutable test probe for one reporter-open context. */
    public static final class OpenContextSnapshot {
        private final String runId;
        private final boolean dryRun;
        private final AuditStage stage;
        private final String operatorName;
        private final Integer subtaskIndex;
        private final Integer attemptNumber;
        private final ClassLoader userCodeClassLoader;

        private OpenContextSnapshot(AuditReporterContext context) {
            this.runId = context.getRunId();
            this.dryRun = context.isDryRun();
            this.stage = context.getStage();
            this.operatorName = context.getOperatorName();
            this.subtaskIndex = context.getSubtaskIndex();
            this.attemptNumber = context.getAttemptNumber();
            this.userCodeClassLoader = context.getUserCodeClassLoader();
        }

        public String getRunId() {
            return runId;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        public AuditStage getStage() {
            return stage;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public Integer getSubtaskIndex() {
            return subtaskIndex;
        }

        public Integer getAttemptNumber() {
            return attemptNumber;
        }

        public ClassLoader getUserCodeClassLoader() {
            return userCodeClassLoader;
        }
    }
}
