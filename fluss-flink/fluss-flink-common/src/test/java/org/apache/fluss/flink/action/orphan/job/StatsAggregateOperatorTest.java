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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class StatsAggregateOperatorTest {

    @Test
    void logsSummaryBeforeRetentionWaitAndThenEmitsTotals() throws Exception {
        List<String> events = new CopyOnWriteArrayList<String>();
        try (AuditCapture capture = new AuditCapture(events);
                OneInputStreamOperatorTestHarness<CleanStats, CleanStats> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StatsAggregateOperator(
                                        false,
                                        Duration.ofMillis(5),
                                        millis -> events.add("sleeper")))) {
            harness.open();
            harness.processElement(new StreamRecord<>(new CleanStats(1, 1, 1, 10, 1, 1, 0, 10)));
            harness.processElement(new StreamRecord<>(new CleanStats(2, 2, 1, 20, 0, 0, 1, 0)));
            harness.endInput();

            CleanStats result = harness.getRecordOutput().iterator().next().getValue();
            assertThat(result.scannedFiles()).isEqualTo(3L);
            assertThat(result.plannedFiles()).isEqualTo(3L);
            assertThat(result.plannedDirs()).isEqualTo(2L);
            assertThat(result.plannedBytes()).isEqualTo(30L);
            assertThat(result.deletedFiles()).isEqualTo(1L);
            assertThat(result.bytesReclaimed()).isEqualTo(10L);

            assertThat(indexOf(events, "action=summary"))
                    .isLessThan(indexOf(events, "action=retention_wait_start"));
            assertThat(indexOf(events, "action=retention_wait_start"))
                    .isLessThan(events.indexOf("sleeper"));
            assertThat(events.indexOf("sleeper"))
                    .isLessThan(indexOf(events, "action=retention_wait_end"));
            assertThat(events)
                    .anyMatch(
                            event ->
                                    event.contains("action=summary")
                                            && event.contains("planned_bytes=30")
                                            && event.contains("planned_size=30 B")
                                            && event.contains("deleted_files=1")
                                            && event.contains("bytes_reclaimed=10")
                                            && event.contains("reclaimed_size=10 B"));
        }
    }

    @Test
    void zeroWaitDoesNotCallSleeper() throws Exception {
        AtomicBoolean slept = new AtomicBoolean();
        try (OneInputStreamOperatorTestHarness<CleanStats, CleanStats> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new StatsAggregateOperator(
                                true, Duration.ZERO, millis -> slept.set(true)))) {
            harness.open();
            harness.endInput();
        }
        assertThat(slept).isFalse();
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
