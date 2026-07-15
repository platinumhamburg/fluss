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

import org.apache.fluss.flink.action.orphan.job.CleanupStats;
import org.apache.fluss.flink.action.orphan.job.CleanupSummary;
import org.apache.fluss.flink.action.orphan.job.RuleDecisionCounters;
import org.apache.fluss.flink.action.orphan.job.StatsAggregateOperator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLoggerTest {

    @Test
    void emitsBoundedRuleAndCoverageSummaries() throws Exception {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        CleanupStats stats =
                CleanupStats.scanBuilder(orders)
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
        List<String> events = new CopyOnWriteArrayList<>();

        try (AuditCapture ignored = new AuditCapture(events);
                OneInputStreamOperatorTestHarness<CleanupStats, CleanupSummary> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StatsAggregateOperator(true), 1, 1, 0)) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(
                            CleanupStats.scope(
                                    1L,
                                    1L,
                                    Collections.singletonMap(SkipReasonCode.RPC_ERROR, 1L))));
            harness.processElement(new StreamRecord<>(stats));
            harness.endInput();
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
                                        && event.contains("coverage_complete=false")
                                        && event.contains("dry_run_counters_consistent=true")
                                        && event.contains("inconsistent_object_types=0")
                                        && event.contains("inconsistent_scopes=0"));
        assertThat(events)
                .noneMatch(
                        event ->
                                event.contains("action=scan_heartbeat")
                                        || event.contains("action=scan_progress")
                                        || event.contains("action=keep_active")
                                        || event.contains("action=newer_than_cutoff"));
    }

    @Test
    void mtimeUnavailableErrorIsBoundedPerLoggerInstance() {
        List<String> events = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();
        ScopeIdentity scope =
                ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 4);

        try (AuditCapture ignored = new AuditCapture(events)) {
            logger.logMtimeUnavailableOnce(
                    scope, CleanupObjectType.LOG_SEGMENT, "file", "first.log");
            logger.logMtimeUnavailableOnce(
                    scope, CleanupObjectType.DIRECTORY, "directory", "second-dir");
        }

        assertThat(events.stream().filter(e -> e.contains("action=mtime_unavailable")).count())
                .isEqualTo(1L);
        assertThat(events)
                .anyMatch(
                        event ->
                                event.startsWith("ERROR ")
                                        && event.contains("table_id=7")
                                        && event.contains("partition_id=11")
                                        && event.contains("bucket_id=4")
                                        && event.contains("entry_kind=file")
                                        && event.contains("sample_name=first.log")
                                        && event.contains("action_required=true"));
    }

    @Test
    void mtimeUnavailableSampleNameIsSanitizedAndCapped() {
        List<String> events = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();
        String longSuffix = String.join("", Collections.nCopies(140, "a"));

        try (AuditCapture ignored = new AuditCapture(events)) {
            logger.logMtimeUnavailableOnce(
                    ScopeIdentity.global(),
                    CleanupObjectType.DIRECTORY,
                    "directory",
                    "bad path/" + longSuffix);
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=mtime_unavailable")
                                        && event.contains("sample_name=bad_path_")
                                        && !event.contains(longSuffix));
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
            events.add(event.getLevel().name() + " " + event.getMessage().getFormattedMessage());
        }
    }
}
