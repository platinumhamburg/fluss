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
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ScanProgressOperatorTest {

    @Test
    void downstreamOperatorOnlyEmitsBoundedCompletionSummary() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicLong clock = new AtomicLong(0L);
        try (AuditCapture capture = new AuditCapture(events);
                OneInputStreamOperatorTestHarness<CleanStats, CleanStats> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new ScanProgressOperator("run-1", true, clock::get))) {
            harness.open();
            harness.processElement(new StreamRecord<>(stats(2L, 300L)));
            harness.endInput();

            assertThat(events).noneMatch(event -> event.contains("action=scan_progress"));
            assertThat(events).noneMatch(event -> event.contains("action=scan_start"));
            assertThat(events)
                    .anyMatch(
                            event ->
                                    event.contains("action=scan_subtask_summary")
                                            && event.contains("tasks_completed=1")
                                            && event.contains("planned_files=2")
                                            && event.contains("planned_bytes=300"));
            assertThat(harness.getRecordOutput()).hasSize(1);
        }
    }

    @Test
    void downstreamOperatorNeverPretendsToBeRealtimeProgress() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicLong clock = new AtomicLong(0L);
        try (AuditCapture capture = new AuditCapture(events);
                OneInputStreamOperatorTestHarness<CleanStats, CleanStats> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new ScanProgressOperator("run-2", true, clock::get))) {
            harness.open();
            clock.set(30_000L);
            harness.processElement(new StreamRecord<>(stats(1L, 100L)));
            clock.set(59_000L);
            harness.processElement(new StreamRecord<>(stats(1L, 200L)));
            clock.set(60_000L);
            harness.processElement(new StreamRecord<>(stats(1L, 400L)));

            assertThat(matching(events, "action=scan_start")).isEmpty();
            assertThat(matching(events, "action=scan_progress")).isEmpty();
            harness.endInput();
            assertThat(matching(events, "action=scan_subtask_summary"))
                    .singleElement()
                    .satisfies(
                            event ->
                                    assertThat(event)
                                            .contains("tasks_completed=3", "planned_bytes=700"));
        }
    }

    private static CleanStats stats(long plannedFiles, long plannedBytes) {
        return new CleanStats(0L, plannedFiles, 0L, plannedBytes, 0L, 0L, 0L, 0L);
    }

    private static List<String> matching(List<String> events, String marker) {
        List<String> matches = new java.util.ArrayList<>();
        for (String event : events) {
            if (event.contains(marker)) {
                matches.add(event);
            }
        }
        return matches;
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
            appender = new CapturingAppender("scan-progress-audit", events);
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
