/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

class AuditLoggerTest {

    @Test
    void scopeTargetEventsIdentifyTableAndCompletionStatus() {
        List<String> events = new CopyOnWriteArrayList<>();
        AuditLogger logger = new AuditLogger();

        try (AuditCapture ignored = new AuditCapture(events)) {
            logger.logScopeTargetStart("run-1", "db", "orders", 7L, 11L);
            logger.logScopeTargetComplete(
                    "run-1", "db", "orders", 7L, 11L, "ok", "not_applicable", 123L);
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=scope_target_start")
                                        && event.contains("database=db")
                                        && event.contains("table=orders")
                                        && event.contains("table_id=7")
                                        && event.contains("partition_id=11"));
        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=scope_target_complete")
                                        && event.contains("log_status=ok")
                                        && event.contains("kv_status=not_applicable")
                                        && event.contains("duration_ms=123"));
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
