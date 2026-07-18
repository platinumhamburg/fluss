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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;

/** Discovers configured audit reporters and manages their runtime lifecycle. */
@Internal
public final class AuditReporterRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuditReporterRuntime.class);
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-z][a-z0-9_-]*");

    private final transient List<ActiveReporter> reporters;
    private final boolean disabled;
    private transient boolean closed;

    private AuditReporterRuntime(List<ActiveReporter> reporters, boolean disabled) {
        this.reporters = reporters;
        this.disabled = disabled;
    }

    /** Opens the configured reporters using the user-code class loader in the supplied context. */
    public static AuditReporterRuntime open(AuditReporterSpec spec, AuditReporterContext context) {
        if (spec == null) {
            throw failure("runtime", "discovery");
        }
        if (context == null) {
            throw failure("runtime", "discovery");
        }
        if (spec.reporters().isEmpty()) {
            return new AuditReporterRuntime(Collections.<ActiveReporter>emptyList(), true);
        }

        DiscoveredFactories discovered = discover(context);
        validateSelections(spec.reporters(), discovered);

        List<ActiveReporter> opened = new ArrayList<>();
        for (ReporterSpec reporterSpec : spec.reporters()) {
            AuditReporterFactory factory = discovered.factories.get(reporterSpec.identifier());
            try {
                factory.validate(copyOptions(reporterSpec.options()));
            } catch (RuntimeException | ServiceConfigurationError e) {
                handleOpenFailure(opened, reporterSpec, "validate", null, context);
                continue;
            }

            AuditReporter reporter;
            try {
                reporter = factory.create(copyOptions(reporterSpec.options()));
                if (reporter == null) {
                    throw new IllegalStateException();
                }
            } catch (RuntimeException | ServiceConfigurationError e) {
                handleOpenFailure(opened, reporterSpec, "create", null, context);
                continue;
            }

            try {
                reporter.open(context);
            } catch (Exception | ServiceConfigurationError e) {
                handleOpenFailure(opened, reporterSpec, "open", reporter, context);
                continue;
            }
            opened.add(
                    new ActiveReporter(
                            reporterSpec.identifier(), reporterSpec.required(), reporter, context));
            logLifecycle(reporterSpec.identifier(), "opened", context);
        }
        return new AuditReporterRuntime(opened, false);
    }

    /** Reports one event to every active reporter in configured order. */
    public synchronized void report(AuditEvent event) {
        if (closed || event == null) {
            throw failure("runtime", "report");
        }
        if (disabled) {
            return;
        }

        AuditReportingException aggregate = null;
        for (ActiveReporter active : reporters) {
            try {
                active.reporter.report(event);
            } catch (Exception | ServiceConfigurationError e) {
                aggregate = handleLifecycleFailure(aggregate, active, "report");
            }
        }
        throwIfPresent(aggregate);
    }

    /** Flushes every active reporter in configured order. */
    public synchronized void flush() {
        if (disabled || closed) {
            return;
        }

        AuditReportingException aggregate = null;
        for (ActiveReporter active : reporters) {
            try {
                active.reporter.flush();
            } catch (Exception | ServiceConfigurationError e) {
                aggregate = handleLifecycleFailure(aggregate, active, "flush");
            }
        }
        throwIfPresent(aggregate);
    }

    /** Closes every active reporter in reverse configured order. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (disabled) {
            return;
        }
        throwIfPresent(closeReporters(reporters, null));
    }

    private static DiscoveredFactories discover(AuditReporterContext context) {
        ServiceLoader<AuditReporterFactory> loader =
                ServiceLoader.load(AuditReporterFactory.class, context.getUserCodeClassLoader());
        Map<String, AuditReporterFactory> factories = new LinkedHashMap<>();
        Set<String> duplicates = new LinkedHashSet<>();
        Iterator<AuditReporterFactory> iterator = loader.iterator();
        while (true) {
            boolean hasNext;
            try {
                hasNext = iterator.hasNext();
            } catch (ServiceConfigurationError e) {
                throw failure("provider", "discovery");
            }
            if (!hasNext) {
                break;
            }

            AuditReporterFactory factory;
            try {
                factory = iterator.next();
            } catch (ServiceConfigurationError e) {
                throw failure("provider", "discovery");
            }

            String identifier;
            try {
                identifier = factory.identifier();
            } catch (RuntimeException | ServiceConfigurationError e) {
                throw failure("provider", "discovery");
            }
            if (identifier == null || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
                throw failure("provider", "discovery");
            }
            if (factories.put(identifier, factory) != null) {
                duplicates.add(identifier);
            }
        }
        return new DiscoveredFactories(factories, duplicates);
    }

    private static void validateSelections(
            List<ReporterSpec> reporterSpecs, DiscoveredFactories discovered) {
        for (ReporterSpec reporterSpec : reporterSpecs) {
            String identifier = reporterSpec.identifier();
            if (!discovered.factories.containsKey(identifier)
                    || discovered.duplicates.contains(identifier)) {
                throw failure(reporterSpec.identifier(), "discovery");
            }
        }
    }

    private static Map<String, String> copyOptions(Map<String, String> options) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(options));
    }

    private static void handleOpenFailure(
            List<ActiveReporter> opened,
            ReporterSpec reporterSpec,
            String phase,
            AuditReporter partialReporter,
            AuditReporterContext context) {
        logFailure(reporterSpec.identifier(), phase, reporterSpec.required(), context);
        AuditReportingException aggregate =
                reporterSpec.required() ? failure(reporterSpec.identifier(), phase) : null;
        if (partialReporter != null) {
            try {
                partialReporter.close();
            } catch (Exception | ServiceConfigurationError e) {
                if (aggregate != null) {
                    aggregate.addSuppressed(failure(reporterSpec.identifier(), "close"));
                }
            }
        }

        if (!reporterSpec.required()) {
            return;
        }

        throw closeReporters(opened, aggregate);
    }

    private static AuditReportingException closeReporters(
            List<ActiveReporter> activeReporters, AuditReportingException aggregate) {
        for (int i = activeReporters.size() - 1; i >= 0; i--) {
            ActiveReporter active = activeReporters.get(i);
            try {
                active.reporter.close();
                logLifecycle(active.identifier, "closed", active.context);
            } catch (Exception | ServiceConfigurationError e) {
                aggregate = handleLifecycleFailure(aggregate, active, "close");
            }
        }
        return aggregate;
    }

    private static AuditReportingException handleLifecycleFailure(
            AuditReportingException aggregate, ActiveReporter active, String phase) {
        logFailure(active.identifier, phase, active.required, active.context);
        if (!active.required) {
            return aggregate;
        }

        AuditReportingException sanitized = failure(active.identifier, phase);
        if (aggregate == null) {
            return sanitized;
        }
        aggregate.addSuppressed(sanitized);
        return aggregate;
    }

    private static void logFailure(
            String identifier, String phase, boolean required, AuditReporterContext context) {
        if (required) {
            LOG.error(
                    "Audit reporter '{}' failed during {} run_id={} stage={} operator={} subtask={} attempt={}",
                    identifier,
                    phase,
                    context.getRunId(),
                    context.getStage(),
                    context.getOperatorName(),
                    context.getSubtaskIndex(),
                    context.getAttemptNumber());
        } else {
            LOG.warn(
                    "Audit reporter '{}' failed during {} run_id={} stage={} operator={} subtask={} attempt={}",
                    identifier,
                    phase,
                    context.getRunId(),
                    context.getStage(),
                    context.getOperatorName(),
                    context.getSubtaskIndex(),
                    context.getAttemptNumber());
        }
    }

    private static void logLifecycle(
            String identifier, String action, AuditReporterContext context) {
        LOG.info(
                "Audit reporter '{}' {} run_id={} stage={} operator={} subtask={} attempt={}",
                identifier,
                action,
                context.getRunId(),
                context.getStage(),
                context.getOperatorName(),
                context.getSubtaskIndex(),
                context.getAttemptNumber());
    }

    private static AuditReportingException failure(String identifier, String phase) {
        return new AuditReportingException(identifier, phase);
    }

    private static void throwIfPresent(AuditReportingException aggregate) {
        if (aggregate != null) {
            throw aggregate;
        }
    }

    private static final class ActiveReporter {
        private final String identifier;
        private final boolean required;
        private final AuditReporter reporter;
        private final AuditReporterContext context;

        private ActiveReporter(
                String identifier,
                boolean required,
                AuditReporter reporter,
                AuditReporterContext context) {
            this.identifier = identifier;
            this.required = required;
            this.reporter = reporter;
            this.context = context;
        }
    }

    private static final class DiscoveredFactories {
        private final Map<String, AuditReporterFactory> factories;
        private final Set<String> duplicates;

        private DiscoveredFactories(
                Map<String, AuditReporterFactory> factories, Set<String> duplicates) {
            this.factories = factories;
            this.duplicates = duplicates;
        }
    }
}
