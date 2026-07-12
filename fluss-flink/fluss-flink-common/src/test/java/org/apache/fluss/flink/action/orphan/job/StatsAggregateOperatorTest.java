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

import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class StatsAggregateOperatorTest {

    @Test
    void logsSummaryAndImmediatelyEmitsTotals() throws Exception {
        List<String> events = new CopyOnWriteArrayList<String>();
        try (AuditCapture capture = new AuditCapture(events);
                OneInputStreamOperatorTestHarness<CleanupReportInput, CleanupReport> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StatsAggregateOperator("run-1", false))) {
            harness.open();
            ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
            harness.processElement(
                    new StreamRecord<>(
                            CleanupReportInput.stats(
                                    CleanStats.builder(orders)
                                            .scanned(CleanupObjectType.LOG_SEGMENT, 1L)
                                            .planned(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                                            .deleted(CleanupObjectType.LOG_SEGMENT, 1L, 10L)
                                            .plannedDirectory(1L)
                                            .removedDirectory(1L)
                                            .build())));
            harness.processElement(
                    new StreamRecord<>(
                            CleanupReportInput.stats(
                                    CleanStats.builder(orders)
                                            .scanned(CleanupObjectType.KV_SHARED_SST, 2L)
                                            .planned(CleanupObjectType.KV_SHARED_SST, 2L, 20L)
                                            .deleteFailed(CleanupObjectType.KV_SHARED_SST, 1L)
                                            .plannedDirectory(1L)
                                            .skipped(SkipReasonCode.KEEP_ACTIVE, 2L)
                                            .build())));
            harness.processElement(
                    new StreamRecord<>(
                            CleanupReportInput.plan(
                                    new TablePlanStats(
                                            orders,
                                            2L,
                                            1L,
                                            java.util.Collections.singletonMap(
                                                    SkipReasonCode.RPC_ERROR, 1L)))));
            harness.endInput();

            CleanupReport result = harness.getRecordOutput().iterator().next().getValue();
            assertThat(result.global().scannedFiles()).isEqualTo(3L);
            assertThat(result.global().plannedFiles()).isEqualTo(3L);
            assertThat(result.global().plannedDirs()).isEqualTo(2L);
            assertThat(result.global().plannedBytes()).isEqualTo(30L);
            assertThat(result.global().deletedFiles()).isEqualTo(1L);
            assertThat(result.global().bytesReclaimed()).isEqualTo(10L);
            assertThat(result.tasksPlanned()).isEqualTo(2L);
            assertThat(result.metadataFailures()).isEqualTo(1L);

            assertThat(indexOf(events, "action=summary")).isGreaterThanOrEqualTo(0);
            assertThat(events).noneMatch(event -> event.contains("action=retention_wait_"));
            assertThat(events)
                    .anyMatch(
                            event ->
                                    event.contains("action=summary")
                                            && event.contains("planned_bytes=30")
                                            && event.contains("planned_size=30 B")
                                            && event.contains("deleted_files=1")
                                            && event.contains("bytes_reclaimed=10")
                                            && event.contains("reclaimed_size=10 B")
                                            && event.contains("run_id=run-1"));
            assertThat(events).anyMatch(event -> event.contains("action=table_summary"));
            assertThat(events).anyMatch(event -> event.contains("action=table_object_summary"));
            assertThat(events).anyMatch(event -> event.contains("action=table_skip_summary"));
            assertThat(events).anyMatch(event -> event.contains("action=database_summary"));
            assertThat(events).anyMatch(event -> event.contains("action=summary_by_type"));
            assertThat(events).anyMatch(event -> event.contains("action=summary_by_reason"));
            assertThat(events).anyMatch(event -> event.contains("action=audit_integrity"));
        }
    }

    private static int indexOf(List<String> events, String marker) {
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).contains(marker)) {
                return i;
            }
        }
        return -1;
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
            appender = new CapturingAppender("stats-aggregate-audit", events);
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
            super(
                    name,
                    null,
                    null,
                    true,
                    org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY);
            this.events = events;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.getMessage().getFormattedMessage());
        }
    }
}
