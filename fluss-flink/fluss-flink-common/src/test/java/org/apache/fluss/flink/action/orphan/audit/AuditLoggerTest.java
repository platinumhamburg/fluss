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

import org.apache.fluss.flink.action.orphan.job.CleanStats;
import org.apache.fluss.flink.action.orphan.job.CleanupReport;
import org.apache.fluss.flink.action.orphan.job.RuleDecisionCounters;
import org.apache.fluss.flink.action.orphan.job.TablePlanStats;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLoggerTest {

    @Test
    void emitsBoundedRuleAndCoverageSummaries() {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        CleanStats stats =
                CleanStats.builder(orders)
                        .scanned(CleanupObjectType.LOG_SEGMENT, 2L)
                        .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                        .ruleDecision(
                                CleanupObjectType.LOG_SEGMENT,
                                RuleDecisionCounters.scanned(10L)
                                        .add(RuleDecisionCounters.scanned(5L))
                                        .add(RuleDecisionCounters.candidate(10L))
                                        .add(RuleDecisionCounters.keepActive(5L)))
                        .skipped(SkipReasonCode.KEEP_ACTIVE, 1L)
                        .build();
        TablePlanStats plan =
                new TablePlanStats(orders, 1L, 1L, Map.of(SkipReasonCode.RPC_ERROR, 1L));
        CleanupReport report =
                CleanupReport.aggregate(
                        Collections.singletonList(plan), Collections.singletonList(stats), true);
        List<String> events = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(events)) {
            new AuditLogger().logReport(report, true);
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=table_rule_summary")
                                        && event.contains("database=db")
                                        && event.contains("table=orders")
                                        && event.contains("object_type=log_segment")
                                        && event.contains("keep_active_files=1")
                                        && event.contains("candidate_files=1"));
        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=coverage_summary")
                                        && event.contains("metadata_read_failed_targets=1")
                                        && event.contains("rpc_failed_targets=1")
                                        && event.contains("complete=false"));
        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=audit_integrity")
                                        && event.contains("rule_counters_consistent=true")
                                        && event.contains("coverage_complete=false"));
        assertThat(events)
                .noneMatch(
                        event ->
                                event.contains("action=scan_heartbeat")
                                        || event.contains("action=scan_progress")
                                        || event.contains("action=keep_active")
                                        || event.contains("action=newer_than_cutoff"));
    }

    private static final class AuditCapture implements AutoCloseable {
        private final LoggerContext context;
        private final LoggerConfig loggerConfig;
        private final Level previousLevel;
        private final CapturingAppender appender;

        private AuditCapture(List<String> events) {
            context = (LoggerContext) LogManager.getContext(false);
            loggerConfig = context.getConfiguration().getLoggerConfig("fluss.orphan.audit");
            previousLevel = loggerConfig.getLevel();
            appender = new CapturingAppender("audit-logger-test", events);
            appender.start();
            loggerConfig.setLevel(Level.INFO);
            loggerConfig.addAppender(appender, Level.INFO, null);
            context.updateLoggers();
        }

        @Override
        public void close() {
            loggerConfig.removeAppender(appender.getName());
            loggerConfig.setLevel(previousLevel);
            context.updateLoggers();
            appender.stop();
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> events;

        private CapturingAppender(String name, List<String> events) {
            super(name, null, null, false, null);
            this.events = events;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.getMessage().getFormattedMessage());
        }
    }
}
