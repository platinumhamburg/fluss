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

package org.apache.fluss.flink.action.orphan.fs;

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SafeDeleter} against the local filesystem. */
class SafeDeleterTest {

    @TempDir Path tmp;

    @Test
    void deleteFileRespectsDryRun() throws IOException {
        Path target = Files.createFile(tmp.resolve("orphan.log"));
        SafeDeleter d = newDeleter(localFs(), true);
        d.deleteFile(new FsPath(target.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        assertThat(Files.exists(target)).isTrue();
    }

    @Test
    void deleteFileActuallyDeletesWhenNotDryRun() throws IOException {
        Path target = Files.createFile(tmp.resolve("orphan.log"));
        SafeDeleter d = newDeleter(localFs(), false);
        d.deleteFile(new FsPath(target.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        assertThat(Files.exists(target)).isFalse();
    }

    @Test
    void dryRunAuditContainsObjectSizeRuleAndTableScope() throws IOException {
        Path target = Files.write(tmp.resolve("orphan.log"), new byte[] {1, 2, 3});
        List<String> events = new CopyOnWriteArrayList<>();
        ScopeIdentity scope =
                ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 3);

        try (AuditCapture capture = new AuditCapture(events)) {
            SafeDeleter deleter =
                    new SafeDeleter(
                            localFs(),
                            true,
                            new AuditLogger(),
                            RateLimiter.create(1000.0),
                            "run-1",
                            scope);
            deleter.deleteFile(
                    new FileMeta(new FsPath(target.toString()), 3L, 123L),
                    Decision.DELETE,
                    RuleId.LOG_SEGMENT);
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=would_delete")
                                        && event.contains("run_id=run-1")
                                        && event.contains("object_type=log_segment")
                                        && event.contains("size_bytes=3")
                                        && event.contains("mtime_ms=123")
                                        && event.contains("database=db")
                                        && event.contains("table=orders")
                                        && event.contains("table_id=7")
                                        && event.contains("partition_id=11")
                                        && event.contains("bucket_id=3")
                                        && event.contains("reason_code=older_than_cutoff"));
    }

    @Test
    void deleteFileRejectsNonDeleteDecision() {
        SafeDeleter d = newDeleter(null, false);
        assertThatThrownBy(
                        () ->
                                d.deleteFile(
                                        new FsPath("/tmp/x"), Decision.KEEP_ACTIVE, RuleId.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deleteEmptyDirNoOpsOnNonEmpty() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("d"));
        Files.createFile(dir.resolve("child"));
        SafeDeleter d = newDeleter(localFs(), false);
        d.deleteEmptyDir(new FsPath(dir.toString()));
        assertThat(Files.exists(dir)).isTrue();
    }

    @Test
    void deleteEmptyDirActuallyDeletes() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("d"));
        SafeDeleter d = newDeleter(localFs(), false);
        d.deleteEmptyDir(new FsPath(dir.toString()));
        assertThat(Files.exists(dir)).isFalse();
    }

    @Test
    void dryRunDirectoryAuditContainsScopeAndMetadata() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("d"));
        List<String> events = new CopyOnWriteArrayList<>();
        ScopeIdentity scope = ScopeIdentity.table("db", "orders", 7L);

        try (AuditCapture capture = new AuditCapture(events)) {
            SafeDeleter deleter =
                    new SafeDeleter(
                            localFs(),
                            true,
                            new AuditLogger(),
                            RateLimiter.create(1000.0),
                            "run-1",
                            scope);
            assertThat(deleter.deleteEmptyDir(new FsPath(dir.toString()), 123L)).isTrue();
        }

        assertThat(events)
                .anyMatch(
                        event ->
                                event.contains("action=would_delete")
                                        && event.contains("run_id=run-1")
                                        && event.contains("object_type=directory")
                                        && event.contains("mtime_ms=123")
                                        && event.contains("database=db")
                                        && event.contains("table=orders")
                                        && event.contains(
                                                "reason_code=empty_and_older_than_cutoff"));
    }

    private static SafeDeleter newDeleter(FileSystem fs, boolean dryRun) {
        return new SafeDeleter(fs, dryRun, new AuditLogger(), RateLimiter.create(1000.0));
    }

    private static FileSystem localFs() {
        return LocalFileSystem.getSharedInstance();
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
            appender = new CapturingAppender("safe-deleter-audit", events);
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
