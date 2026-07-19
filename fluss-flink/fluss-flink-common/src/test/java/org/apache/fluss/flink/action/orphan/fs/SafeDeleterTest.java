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
import org.apache.fluss.flink.action.orphan.audit.AuditReporterContext;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterRuntime;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.TestingAuditReporterFactory;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void listFailureReportsExactDirectoryWithoutRawExceptionMessage() throws IOException {
        TestingAuditReporterFactory.reset();
        FileSystem fs = mock(FileSystem.class);
        FsPath dir = new FsPath("file:/sensitive/table-1/partition-2/bucket-3");
        when(fs.listStatus(dir)).thenThrow(new IOException("credential-like-sensitive-message"));
        AuditReporterSpec spec =
                new AuditReporterSpec(
                        "00000000-0000-0000-0000-000000000006",
                        Collections.singletonList(
                                new ReporterSpec(
                                        "testing", true, Collections.<String, String>emptyMap())));
        AuditReporterContext context =
                new AuditReporterContext(
                        spec.runId(),
                        true,
                        AuditStage.SCAN,
                        "ScanAndClean",
                        0,
                        0,
                        SafeDeleterTest.class.getClassLoader());
        AuditReporterRuntime runtime = AuditReporterRuntime.open(spec, context);
        try {
            SafeDeleter deleter =
                    new SafeDeleter(
                            fs,
                            true,
                            new AuditLogger(runtime, context),
                            RateLimiter.create(1000.0),
                            ScopeIdentity.table("db", "table", 1L).withPartitionAndBucket(2L, 3));

            assertThat(deleter.deleteEmptyDirDetailed(dir, 123L))
                    .isEqualTo(SafeDeleter.DirectoryDeleteResult.LIST_FAILED);

            assertThat(TestingAuditReporterFactory.events("testing"))
                    .filteredOn(event -> event.getAction().equals("filesystem_failure"))
                    .singleElement()
                    .satisfies(
                            event -> {
                                assertThat(event.getPath()).isEqualTo(dir.toString());
                                assertThat(event.getDimensions())
                                        .containsEntry("operation", "list_directory")
                                        .containsEntry("failure_category", "directory_list_failed")
                                        .containsEntry(
                                                "exception_class", IOException.class.getName())
                                        .doesNotContainValue("credential-like-sensitive-message");
                                assertThat(event.getFlags())
                                        .containsEntry("retryable", true)
                                        .containsEntry("action_required", true)
                                        .containsEntry("consistency_race_possible", true);
                            });
        } finally {
            runtime.close();
            TestingAuditReporterFactory.reset();
        }
    }

    private static SafeDeleter newDeleter(FileSystem fs, boolean dryRun) {
        return new SafeDeleter(fs, dryRun, new AuditLogger(), RateLimiter.create(1000.0));
    }

    private static FileSystem localFs() {
        return LocalFileSystem.getSharedInstance();
    }
}
