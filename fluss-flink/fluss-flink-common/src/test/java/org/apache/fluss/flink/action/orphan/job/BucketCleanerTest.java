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

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BucketCleanerTest {

    @Test
    void removesOldEmptySegmentDirAfterDeletingExpiredFiles(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path logFile =
                Files.write(
                        segmentDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(0L)
                                        + FlussPaths.LOG_FILE_SUFFIX),
                        new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(logFile, cutoff - 1000L);
        makeOld(segmentDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff, false);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.scannedFiles).isEqualTo(1L);
        assertThat(stats.plannedFiles).isEqualTo(1L);
        assertThat(stats.plannedDirs).isEqualTo(1L);
        assertThat(stats.plannedBytes).isEqualTo(1L);
        assertThat(stats.deletedFiles).isEqualTo(1L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(1L);
        assertThat(stats.bytesReclaimed).isEqualTo(1L);
        assertThat(Files.exists(logFile)).isFalse();
        assertThat(Files.exists(segmentDir)).isFalse();
        assertThat(Files.exists(bucketRoot)).isTrue();
    }

    @Test
    void keepsFreshEmptySegmentDir(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        long cutoff = System.currentTimeMillis() - 1000L;

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff, false);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(
                        new BucketActiveRefs(
                                Collections.<String>emptySet(),
                                Collections.<String>emptySet(),
                                Collections.<String>emptySet()),
                        new FsPath(bucketRoot.toString()));

        assertThat(stats.deletedFiles).isEqualTo(0L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(0L);
        assertThat(Files.exists(segmentDir)).isTrue();
    }

    @Test
    void scansButDoesNotDeleteUnknownDotFiles(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path dotFile = Files.write(segmentDir.resolve(".unknown"), new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(dotFile, cutoff - 1000L);
        makeOld(segmentDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff, false);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.scannedFiles).isEqualTo(1L);
        assertThat(stats.plannedFiles).isEqualTo(0L);
        assertThat(stats.deletedFiles).isEqualTo(0L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(0L);
        assertThat(stats.bySkipReason).containsEntry(SkipReasonCode.UNKNOWN_FILE_TYPE, 1L);
        RuleDecisionCounters decisions = stats.byRuleDecision.get(CleanupObjectType.LOG_SEGMENT);
        assertThat(decisions.scannedFiles()).isEqualTo(1L);
        assertThat(decisions.scannedBytes()).isEqualTo(1L);
        assertThat(decisions.unknownFileTypeFiles()).isEqualTo(1L);
        assertThat(decisions.isConsistent()).isTrue();
        assertThat(Files.exists(dotFile)).isTrue();
        assertThat(Files.exists(segmentDir)).isTrue();
    }

    @Test
    void dryRunReportsPlanWithoutActualReclamation(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path logFile =
                Files.write(
                        segmentDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(0L)
                                        + FlussPaths.LOG_FILE_SUFFIX),
                        new byte[10]);
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(logFile, cutoff - 1000L);
        makeOld(segmentDir, cutoff - 1000L);

        BucketCleaner.BucketCleanStats stats =
                createCleaner(bucketRoot, cutoff, true)
                        .clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.plannedFiles).isEqualTo(1L);
        assertThat(stats.plannedDirs).isEqualTo(1L);
        assertThat(stats.plannedBytes).isEqualTo(10L);
        assertThat(stats.deletedFiles).isEqualTo(0L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(0L);
        assertThat(stats.deleteFailures).isEqualTo(0L);
        assertThat(stats.bytesReclaimed).isEqualTo(0L);
        RuleDecisionCounters decisions = stats.byRuleDecision.get(CleanupObjectType.LOG_SEGMENT);
        assertThat(decisions.scannedFiles()).isEqualTo(1L);
        assertThat(decisions.candidateFiles()).isEqualTo(1L);
        assertThat(decisions.candidateBytes()).isEqualTo(10L);
        assertThat(decisions.isConsistent()).isTrue();
        assertThat(Files.exists(logFile)).isTrue();
    }

    @Test
    void recordsNewerThanCutoffDecision(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path logFile =
                Files.write(
                        segmentDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(0L)
                                        + FlussPaths.LOG_FILE_SUFFIX),
                        new byte[5]);
        long cutoff = System.currentTimeMillis() - 10_000L;

        BucketCleaner.BucketCleanStats stats =
                createCleaner(bucketRoot, cutoff, true)
                        .clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        RuleDecisionCounters decisions = stats.byRuleDecision.get(CleanupObjectType.LOG_SEGMENT);
        assertThat(decisions.scannedFiles()).isEqualTo(1L);
        assertThat(decisions.scannedBytes()).isEqualTo(5L);
        assertThat(decisions.newerThanCutoffFiles()).isEqualTo(1L);
        assertThat(decisions.newerThanCutoffBytes()).isEqualTo(5L);
        assertThat(decisions.isConsistent()).isTrue();
        assertThat(Files.exists(logFile)).isTrue();
    }

    @Test
    void failedDeleteKeepsPlanButDoesNotReportReclamation(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path logFile =
                Files.write(
                        segmentDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(0L)
                                        + FlussPaths.LOG_FILE_SUFFIX),
                        new byte[10]);
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(logFile, cutoff - 1000L);
        FileSystem failingFs = mock(FileSystem.class);
        when(failingFs.delete(any(FsPath.class), eq(false))).thenReturn(false);
        RateLimiter limiter = RateLimiter.create(1000.0);
        BucketCleaner cleaner =
                new BucketCleaner(
                        new RuleDispatcher(),
                        new SafeDeleter(failingFs, false, new AuditLogger(), limiter),
                        new AuditLogger(),
                        cutoff,
                        limiter,
                        false);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.plannedFiles).isEqualTo(1L);
        assertThat(stats.plannedBytes).isEqualTo(10L);
        assertThat(stats.deletedFiles).isEqualTo(0L);
        assertThat(stats.deleteFailures).isEqualTo(1L);
        assertThat(stats.bytesReclaimed).isEqualTo(0L);
    }

    private static void makeOld(Path path, long timestampMillis) throws IOException {
        Files.setLastModifiedTime(path, FileTime.fromMillis(timestampMillis));
    }

    private static BucketCleaner createCleaner(Path bucketRoot, long cutoff, boolean dryRun)
            throws IOException {
        RateLimiter remoteFsOpRateLimiter = RateLimiter.create(1000.0);
        return new BucketCleaner(
                new RuleDispatcher(),
                new SafeDeleter(
                        new FsPath(bucketRoot.toString()).getFileSystem(),
                        dryRun,
                        new AuditLogger(),
                        remoteFsOpRateLimiter),
                new AuditLogger(),
                cutoff,
                remoteFsOpRateLimiter,
                dryRun);
    }
}
