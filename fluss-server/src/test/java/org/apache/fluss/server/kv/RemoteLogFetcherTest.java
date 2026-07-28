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

package org.apache.fluss.server.kv;

import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogTablet.RemoteLogSegmentPage;
import org.apache.fluss.server.log.remote.RemoteLogTestBase;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Test for {@link RemoteLogFetcher}. */
class RemoteLogFetcherTest extends RemoteLogTestBase {

    /** Default knobs for the tests: prefetch depth 2, 2 download threads. */
    private static final int DEFAULT_TEST_PREFETCH_NUM = 2;

    private static final int DEFAULT_TEST_DOWNLOAD_THREADS = 2;

    private RemoteLogFetcher newFetcher(TableBucket tb, File logTabletDir) {
        return new RemoteLogFetcher(
                remoteLogManager,
                tb,
                logTabletDir,
                DEFAULT_TEST_PREFETCH_NUM,
                DEFAULT_TEST_DOWNLOAD_THREADS);
    }

    private RemoteLogFetcher newFetcher(
            TableBucket tb, File logTabletDir, int prefetchNum, int downloadThreads) {
        return new RemoteLogFetcher(
                remoteLogManager, tb, logTabletDir, prefetchNum, downloadThreads);
    }

    @Test
    void testBasicFetch() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        // Make leader and add 5 segments (each with 10 batches).
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        // Trigger remote log tasks to upload segments to remote storage.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();

        // Get the end offset of all remote segments to use as localLogStartOffset.
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            // Fetch all remote log batches from offset 0 to remoteEndOffset.
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            // We should have received multiple batches.
            assertThat(batchList).isNotEmpty();
            // Verify ordering: each batch's baseLogOffset should be monotonically increasing.
            long prevOffset = -1;
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.baseLogOffset()).isGreaterThan(prevOffset);
                prevOffset = batch.baseLogOffset();
            }
        }
    }

    @Test
    void testFetchWithStartOffsetInMiddleOfSegment() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);

        // Start from the middle of the first segment (offset = 5, assuming each segment has 10
        // batches starting from 0).
        long startOffset = 5;
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(startOffset, remoteEndOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            assertThat(batchList).isNotEmpty();
            // All returned batches should have nextLogOffset > startOffset.
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.nextLogOffset()).isGreaterThan(startOffset);
            }
        }
    }

    @Test
    void testFetchStopsAtLocalLogStartOffset() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);

        // Use the start offset of the second segment as localLogStartOffset.
        long localLogStartOffset = segments.get(1).remoteLogStartOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, localLogStartOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            assertThat(batchList).isNotEmpty();
            // All returned batches should have baseLogOffset < localLogStartOffset.
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.baseLogOffset()).isLessThan(localLogStartOffset);
            }
        }
    }

    @Test
    void testFetchNoRemoteSegmentsThrowsException() throws Exception {
        // Use a table bucket that has no remote segments.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        // Don't upload any segments to remote.

        Replica replica = replicaManager.getReplicaOrException(tb);
        File logTabletDir = replica.getLogTablet().getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            assertThatThrownBy(() -> fetcher.fetch(0, 100))
                    .isInstanceOf(RemoteStorageException.class)
                    .hasMessageContaining("No remote log segments found");
        }
    }

    @Test
    void testClosingFetcherDoesNotDeleteAnotherRecoveryDirectory() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        File logTabletDir = replica.getLogTablet().getLogDir();

        Path tmpDir = logTabletDir.toPath().resolve("tmp");
        RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, logTabletDir);
        Path ownDir = Files.createDirectories(fetcher.getTempDir());
        Path concurrentDir =
                Files.createDirectories(tmpDir.resolve("remote-log-recovery-concurrent"));
        Path concurrentFile = Files.createFile(concurrentDir.resolve("segment.log"));

        fetcher.close();

        assertThat(ownDir).doesNotExist();
        assertThat(concurrentDir).exists();
        assertThat(concurrentFile).exists();
    }

    @Test
    void testNewFetcherScavengesAbandonedDirectoryButPreservesActiveRecovery() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        File logTabletDir = replicaManager.getReplicaOrException(tb).getLogTablet().getLogDir();
        Path tmpDir = logTabletDir.toPath().resolve("tmp");

        try (RemoteLogFetcher active = new RemoteLogFetcher(remoteLogManager, tb, logTabletDir)) {
            Path activeDir = Files.createDirectories(active.getTempDir());
            Path activeFile = Files.createFile(activeDir.resolve("active.log"));
            Path abandonedDir =
                    Files.createDirectories(tmpDir.resolve("remote-log-recovery-abandoned"));
            Files.createFile(abandonedDir.resolve("stale.log"));

            try (RemoteLogFetcher ignored =
                    new RemoteLogFetcher(remoteLogManager, tb, logTabletDir)) {
                assertThat(abandonedDir).doesNotExist();
                assertThat(activeDir).exists();
                assertThat(activeFile).exists();
            }
        }
    }

    @Test
    void testTempDirectoryCreatedLazilyOnFetchAndCleanedUp() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir);
        Path tempDir = fetcher.getTempDir();

        assertThat(Files.exists(tempDir)).isFalse();

        for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
            assertThat(Files.exists(tempDir)).isTrue();
        }
        assertThat(Files.exists(tempDir)).isTrue();

        fetcher.close();
        assertThat(Files.exists(tempDir)).isFalse();
    }

    @Test
    void testFetchMultipleSegmentsInOrder() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);

        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);

            long prevNextOffset = 0;
            int batchCount = 0;
            for (LogRecordBatch batch : batches) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            assertThat(batchCount).isGreaterThan(10);
        }
    }

    @Test
    void testFetchWithEmptyRange() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();

        // Empty range [x, x) — guards against advance() >= being changed to >.
        long sameOffset = segments.get(0).remoteLogStartOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(sameOffset, sameOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }
            assertThat(batchList).isEmpty();
        }
    }

    @Test
    void testFetchDownloadsFilesToTempDir() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Path tempDir = fetcher.getTempDir();

            // Consume all batches to trigger downloading.
            int batchCount = 0;
            for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
                batchCount++;
                // Check mid-iteration; files are deleted once the segment is consumed.
                if (batchCount == 1) {
                    File[] downloadedFiles = tempDir.toFile().listFiles();
                    assertThat(downloadedFiles).isNotNull();
                    assertThat(downloadedFiles.length).isGreaterThan(0);
                    for (File file : downloadedFiles) {
                        assertThat(file.getName()).endsWith(".log");
                    }
                }
            }
            assertThat(batchCount).isGreaterThan(0);
            assertThat(countLogFiles(tempDir)).isZero();
        }
    }

    @Test
    void testPrefetchFailurePropagatesAfterAsyncRetriesExhausted() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Exhaust retries for segment #1 so the async worker raises ExecutionException.
        int prefetchAttempts = RemoteLogFetcher.DOWNLOAD_MAX_RETRIES + 1;
        UUID failingSegmentId = segments.get(1).remoteLogSegmentId();
        remoteLogStorage.fetchLogDataFailureBudget.put(
                failingSegmentId, new AtomicInteger(prefetchAttempts));

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();

            assertThatThrownBy(
                            () -> {
                                while (iterator.hasNext()) {
                                    iterator.next();
                                }
                            })
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to fetch remote log segment");

            // Budget fully drained — proves prefetch exhausted retries.
            assertThat(remoteLogStorage.fetchLogDataFailureBudget.get(failingSegmentId).get())
                    .as("prefetch retries should be fully exhausted")
                    .isZero();
        } finally {
            remoteLogStorage.fetchLogDataFailureBudget.clear();
        }
    }

    @Test
    void testCloseCancelsPendingPrefetchFuture() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Block workers in fetchLogData; the only way out is Thread.interrupt().
        int expectedInFlight = 2;
        CountDownLatch entered = new CountDownLatch(expectedInFlight);
        CountDownLatch release = new CountDownLatch(1);
        remoteLogStorage.fetchLogDataBarrier =
                () -> {
                    entered.countDown();
                    release.await();
                };

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2);
        try {
            // fetch() is lazy — kicks off prefetch workers that will block on `release`.
            fetcher.fetch(0, remoteEndOffset);

            assertThat(entered.await(30, TimeUnit.SECONDS))
                    .as("prefetch workers should have entered fetchLogData")
                    .isTrue();

            // Snapshot pending futures BEFORE close so we can assert on them afterwards.
            List<Future<File>> inflight = fetcher.snapshotPrefetchFuturesForTest();
            assertThat(inflight).isNotEmpty();

            fetcher.close();

            // Every captured pending future must end up done and cancelled or exceptional
            // (an interrupted worker surfaces as RemoteStorageException → ExecutionException).
            for (Future<File> f : inflight) {
                assertThat(f.isDone()).isTrue();
                boolean exceptional = false;
                try {
                    f.get();
                } catch (CancellationException | ExecutionException expected) {
                    exceptional = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                assertThat(f.isCancelled() || exceptional).isTrue();
            }
        } finally {
            remoteLogStorage.fetchLogDataBarrier = null;
            release.countDown();
            fetcher.close();
        }
    }

    @Test
    void testSecondFetchClosesPreviousIteratorAndOldIteratorStops() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> firstIterable = fetcher.fetch(0, remoteEndOffset);
            Iterator<LogRecordBatch> firstIterator = firstIterable.iterator();

            assertThat(firstIterator.hasNext()).isTrue();
            firstIterator.next();

            // Trigger a second fetch. This should close the previous active iterator.
            Iterable<LogRecordBatch> secondIterable = fetcher.fetch(0, remoteEndOffset);
            Iterator<LogRecordBatch> secondIterator = secondIterable.iterator();

            assertThat(firstIterator.hasNext()).isFalse();
            assertThat(secondIterator.hasNext()).isTrue();
        }
    }

    @Test
    void testSecondFetchAfterRealAsyncPrefetchDoesNotLetOldIteratorContinue() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Path tempDir = fetcher.getTempDir();

            Iterator<LogRecordBatch> firstIterator = fetcher.fetch(0, remoteEndOffset).iterator();

            assertThat(firstIterator.hasNext()).isTrue();
            firstIterator.next();

            waitUntilLogFileCountAtLeast(tempDir, 2);

            long fileCountBeforeSecondFetch = countLogFiles(tempDir);
            assertThat(fileCountBeforeSecondFetch).isGreaterThanOrEqualTo(2);

            Iterator<LogRecordBatch> secondIterator = fetcher.fetch(0, remoteEndOffset).iterator();

            assertThat(firstIterator.hasNext()).isFalse();
            assertThat(secondIterator.hasNext()).isTrue();
            assertThat(Files.exists(tempDir)).isTrue();
        }
    }

    // ======================================================================================
    // New tests specific to the configurable prefetch window / multi-threaded download
    // ======================================================================================

    @Test
    void testPrefetchDepthGreaterThanOnePrefetchesMultipleSegments() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 3, 3)) {
            Path tempDir = fetcher.getTempDir();
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            // Window refills after first segment is opened.
            waitUntilLogFileCountAtLeast(tempDir, 3);
            assertThat(countLogFiles(tempDir)).isGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void testPrefetchWindowRefillsAfterSegmentConsumed() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        // Small window forces repeated refills.
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : batches) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            // With 4+ segments and 10 batches each we expect clearly more than 2*windowSize.
            assertThat(batchCount).isGreaterThan(10);
        }
    }

    @Test
    void testPrefetchDepthOfOnePreservesLegacyBehavior() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            assertThat(fetcher.getPrefetchNum()).isEqualTo(1);

            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : batches) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            assertThat(batchCount).isGreaterThan(0);
        }
    }

    @Test
    void testCloseCancelsAllPendingPrefetchFutures() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Block real prefetch workers so multiple futures are pending in parallel. Focus
        // here (vs testCloseCancelsPendingPrefetchFuture) is the *count* contract:
        // snapshot() returns all in-flight futures and close() cancels every single one.
        int expectedInFlight = 2;
        CountDownLatch entered = new CountDownLatch(expectedInFlight);
        CountDownLatch release = new CountDownLatch(1);
        remoteLogStorage.fetchLogDataBarrier =
                () -> {
                    entered.countDown();
                    release.await();
                };

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2);
        try {
            // fetch() is lazy — kicks off prefetch workers that will all block on `release`.
            fetcher.fetch(0, remoteEndOffset);

            assertThat(entered.await(30, TimeUnit.SECONDS))
                    .as("prefetch workers should have entered fetchLogData")
                    .isTrue();

            List<Future<File>> futures = fetcher.snapshotPrefetchFuturesForTest();
            assertThat(futures).hasSizeGreaterThanOrEqualTo(2);

            fetcher.close();

            for (Future<File> f : futures) {
                assertThat(f.isDone()).isTrue();
                boolean exceptional = false;
                try {
                    f.get();
                } catch (CancellationException | ExecutionException expected) {
                    exceptional = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                assertThat(f.isCancelled() || exceptional).isTrue();
            }
        } finally {
            remoteLogStorage.fetchLogDataBarrier = null;
            release.countDown();
            fetcher.close();
        }
    }

    // ======================================================================================
    // Integration tests: slow / failing remote storage via TestingRemoteLogStorage hooks.
    // ======================================================================================

    @Test
    void testSlowDownloadOverlappedWithConsumption() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        remoteLogStorage.fetchLogDataDelayMs.set(200L);

        File logTabletDir = logTablet.getLogDir();
        // Three concurrent slow downloads should overlap with consumption.
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 3, 3)) {
            Path tempDir = fetcher.getTempDir();
            long startNanos = System.nanoTime();

            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            long firstBatchElapsedMs =
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - startNanos);

            // Window should hold >= 3 .log files, proving parallel downloads.
            waitUntilLogFileCountAtLeast(tempDir, 3);
            assertThat(countLogFiles(tempDir)).isGreaterThanOrEqualTo(3);

            int batchCount = 1;
            while (iterator.hasNext()) {
                iterator.next();
                batchCount++;
            }
            assertThat(batchCount).isGreaterThan(10);

            // Serial downloads would need 4 * 200ms; parallel keeps first-batch wait ~200ms.
            assertThat(firstBatchElapsedMs).isLessThan(2_000L);
        } finally {
            remoteLogStorage.fetchLogDataDelayMs.set(0L);
        }
    }

    @Test
    void testPersistentDownloadFailureTriggersRetryAndPropagates() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Force every fetchLogData to fail once uploads are done.
        remoteLogStorage.fetchLogDataAlwaysFail.set(true);

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            int countBefore = remoteLogStorage.fetchLogDataInvocationCount();

            // Head segment exhausts retries then fails.
            assertThatThrownBy(() -> fetcher.fetch(0, remoteEndOffset).iterator().hasNext())
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to fetch remote log segment");

            int countAfter = remoteLogStorage.fetchLogDataInvocationCount();
            // Eager prefetch may perform one extra invocation.
            assertThat(countAfter - countBefore)
                    .isGreaterThanOrEqualTo(RemoteLogFetcher.DOWNLOAD_MAX_RETRIES + 1);
        } finally {
            remoteLogStorage.fetchLogDataAlwaysFail.set(false);
        }
    }

    @Test
    void testTransientDownloadFailureRecoversOnRetry() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // First 2 calls fail, 3rd succeeds — retry loop must recover.
        remoteLogStorage.fetchLogDataFailFirstN.set(2);

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }

            assertThat(batchCount).isGreaterThan(0);
            assertThat(remoteLogStorage.fetchLogDataInvocationCount()).isGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void testCloseInterruptsOngoingDownload() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Block workers in fetchLogData; the only way out is Thread.interrupt().
        int expectedInFlight = 2;
        CountDownLatch entered = new CountDownLatch(expectedInFlight);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(expectedInFlight);

        remoteLogStorage.fetchLogDataBarrier =
                () -> {
                    entered.countDown();
                    try {
                        release.await();
                    } finally {
                        finished.countDown();
                    }
                };

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2);
        try {
            fetcher.fetch(0, remoteEndOffset);

            assertThat(entered.await(30, TimeUnit.SECONDS))
                    .as("prefetch workers should have entered fetchLogData")
                    .isTrue();

            List<Future<File>> inflight = fetcher.snapshotPrefetchFuturesForTest();
            assertThat(inflight).isNotEmpty();

            long closeStart = System.nanoTime();
            fetcher.close();
            long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart);

            // release is never counted down — finished reaching zero proves interrupt.
            assertThat(finished.await(10, TimeUnit.SECONDS))
                    .as("close() should have interrupted the blocked prefetch workers")
                    .isTrue();

            assertThat(closeMs).isLessThan(5_000L);

            for (Future<File> f : inflight) {
                assertThat(f.isDone()).isTrue();
                boolean exceptional = false;
                try {
                    f.get();
                } catch (CancellationException | ExecutionException expected) {
                    exceptional = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                assertThat(f.isCancelled() || exceptional).isTrue();
            }
        } finally {
            remoteLogStorage.fetchLogDataBarrier = null;
            release.countDown();
            fetcher.close();
        }
    }

    private static void waitUntilLogFileCountAtLeast(Path dir, int expectedCount) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(dir) && countLogFiles(dir) >= expectedCount) {
                return;
            }
            Thread.sleep(50L);
        }

        assertThat(countLogFiles(dir)).isGreaterThanOrEqualTo(expectedCount);
    }

    private static long countLogFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return 0L;
        }

        try (java.util.stream.Stream<Path> paths = Files.list(dir)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".log")).count();
        }
    }

    @Test
    void testCloseOwnsFileOpenedAfterIteratorClose() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);
        AtomicReference<FileLogRecords> acquired = new AtomicReference<>();
        RemoteLogFetcher fetcher =
                new RemoteLogFetcher(remoteLogManager, tb, logTablet.getLogDir()) {
                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile)
                            throws IOException {
                        FileLogRecords records = super.openDownloadedSegment(localFile);
                        acquired.set(records);
                        opened.countDown();
                        awaitUninterruptibly(releaseOpen);
                        return records;
                    }
                };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> hasNext =
                executor.submit(() -> fetcher.fetch(0, remoteEndOffset).iterator().hasNext());
        try {
            assertThat(opened.await(30, TimeUnit.SECONDS)).isTrue();
            fetcher.close();
            releaseOpen.countDown();

            assertThat(hasNext.get(30, TimeUnit.SECONDS)).isFalse();
            assertThat(acquired.get().channel().isOpen()).isFalse();
            assertThat(Files.exists(fetcher.getTempDir())).isFalse();
        } finally {
            releaseOpen.countDown();
            FileLogRecords records = acquired.get();
            if (records != null && records.channel().isOpen()) {
                records.close();
            }
            fetcher.close();
            executor.shutdownNow();
        }
    }

    @Test
    void testBoundedSegmentDiscoveryIsLinearAcrossTinyCatchUpReads() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 32);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();
        org.apache.fluss.server.log.remote.RemoteLogManager countingManager = spy(remoteLogManager);
        AtomicInteger pageCalls = new AtomicInteger();
        AtomicInteger examinedMetadata = new AtomicInteger();
        doAnswer(
                        invocation -> {
                            RemoteLogSegmentPage page =
                                    (RemoteLogSegmentPage) invocation.callRealMethod();
                            int requestedLimit = invocation.getArgument(3);
                            assertThat(page.examinedSegmentCount())
                                    .isLessThanOrEqualTo(requestedLimit);
                            assertThat(page.segments()).hasSizeLessThanOrEqualTo(requestedLimit);
                            pageCalls.incrementAndGet();
                            examinedMetadata.addAndGet(page.examinedSegmentCount());
                            return page;
                        })
                .when(countingManager)
                .relevantRemoteLogSegmentPage(eq(tb), anyLong(), any(), anyInt());

        List<FileLogRecords> openedRecords = new ArrayList<>();
        AtomicInteger maxOpenHandles = new AtomicInteger();
        try (RemoteLogFetcher fetcher =
                new RemoteLogFetcher(countingManager, tb, logTablet.getLogDir()) {
                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile)
                            throws IOException {
                        FileLogRecords records = super.openDownloadedSegment(localFile);
                        openedRecords.add(records);
                        int openHandles =
                                (int)
                                        openedRecords.stream()
                                                .filter(opened -> opened.channel().isOpen())
                                                .count();
                        maxOpenHandles.accumulateAndGet(openHandles, Math::max);
                        return records;
                    }
                }) {
            long offset = 0L;
            int reads = 0;
            while (offset < remoteEndOffset) {
                Iterator<LogRecordBatch> batches =
                        fetcher.fetch(offset, remoteEndOffset, 1).iterator();
                assertThat(batches.hasNext()).isTrue();
                offset = batches.next().nextLogOffset();
                assertThat(batches.hasNext()).isFalse();
                reads++;
            }

            assertThat(reads).isPositive();
            assertThat(pageCalls).hasValueLessThanOrEqualTo(reads * 2);
            assertThat(examinedMetadata).hasValueLessThanOrEqualTo(reads * 16);
            assertThat(maxOpenHandles).hasValue(1);
            assertThat(openedRecords).hasSizeLessThanOrEqualTo(reads);
        }
        assertThat(openedRecords).allMatch(records -> !records.channel().isOpen());
    }

    @Test
    void testIndexModeResumesWithinSegmentWithoutRedownload() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogSegment segment = remoteLogManager.relevantRemoteLogSegments(tb, 0L).get(0);
        AtomicInteger downloads = new AtomicInteger();
        AtomicInteger opens = new AtomicInteger();
        List<FileLogRecords> openedRecords = new ArrayList<>();
        try (RemoteLogFetcher fetcher =
                new RemoteLogFetcher(
                        remoteLogManager,
                        tb,
                        logTablet.getLogDir(),
                        RemoteLogFetcher.ConsumerMode.INDEX_RETAINED) {
                    @Override
                    protected File downloadSegment(RemoteLogSegment remoteSegment)
                            throws IOException {
                        downloads.incrementAndGet();
                        return super.downloadSegment(remoteSegment);
                    }

                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile)
                            throws IOException {
                        opens.incrementAndGet();
                        FileLogRecords records = super.openDownloadedSegment(localFile);
                        openedRecords.add(records);
                        return records;
                    }
                }) {
            long offset = segment.remoteLogStartOffset();
            int windows = 0;
            while (offset < segment.remoteLogEndOffset()) {
                try (RemoteLogFetcher.FetchResult result =
                        fetcher.fetch(offset, segment.remoteLogEndOffset(), 1)) {
                    Iterator<LogRecordBatch> batches = result.iterator();
                    assertThat(batches.hasNext()).isTrue();
                    LogRecordBatch batch = batches.next();
                    assertThat(batch.nextLogOffset()).isGreaterThan(offset);
                    offset = batch.nextLogOffset();
                    assertThat(batches.hasNext()).isFalse();
                    if (offset < segment.remoteLogEndOffset()) {
                        assertThat(result.stopReason())
                                .isEqualTo(RemoteLogFetcher.StopReason.BYTE_LIMIT);
                    }
                }
                windows++;
            }

            assertThat(windows).isGreaterThan(1);
            assertThat(downloads).hasValue(1);
            assertThat(opens).hasValue(1);
            assertThat(openedRecords)
                    .singleElement()
                    .matches(records -> records.channel().isOpen());
        }
        assertThat(openedRecords).allMatch(records -> !records.channel().isOpen());
    }

    @Test
    void testKvStreamingKeepsOneOpenHandleAcrossManySegments() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 32);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();
        List<FileLogRecords> openedRecords = new ArrayList<>();
        AtomicInteger maxOpenHandles = new AtomicInteger();
        try (RemoteLogFetcher fetcher =
                new RemoteLogFetcher(remoteLogManager, tb, logTablet.getLogDir()) {
                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile)
                            throws IOException {
                        FileLogRecords records = super.openDownloadedSegment(localFile);
                        openedRecords.add(records);
                        int openHandles =
                                (int)
                                        openedRecords.stream()
                                                .filter(opened -> opened.channel().isOpen())
                                                .count();
                        maxOpenHandles.accumulateAndGet(openHandles, Math::max);
                        return records;
                    }
                }) {
            long expectedOffset = 0L;
            int batches = 0;
            for (LogRecordBatch batch : fetcher.fetch(0L, remoteEndOffset)) {
                assertThat(batch.baseLogOffset()).isEqualTo(expectedOffset);
                expectedOffset = batch.nextLogOffset();
                batches++;
            }

            assertThat(batches).isPositive();
            assertThat(expectedOffset).isEqualTo(remoteEndOffset);
            assertThat(maxOpenHandles).hasValue(1);
        }
        assertThat(openedRecords).allMatch(records -> !records.channel().isOpen());
    }

    @Test
    void testKvStreamingConsumesFinalRemoteRecordBeforeSegmentClose() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tb).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 6);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        long startOffset = segments.get(0).remoteLogStartOffset();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();
        List<FileLogRecords> openedRecords = new ArrayList<>();
        AtomicInteger maxOpenHandles = new AtomicInteger();
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createCompactedRowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                RemoteLogFetcher fetcher =
                        new RemoteLogFetcher(remoteLogManager, tb, logTablet.getLogDir()) {
                            @Override
                            protected FileLogRecords openDownloadedSegment(File localFile)
                                    throws IOException {
                                FileLogRecords records = super.openDownloadedSegment(localFile);
                                openedRecords.add(records);
                                int openHandles =
                                        (int)
                                                openedRecords.stream()
                                                        .filter(opened -> opened.channel().isOpen())
                                                        .count();
                                maxOpenHandles.accumulateAndGet(openHandles, Math::max);
                                return records;
                            }
                        }) {
            long expectedBatchOffset = startOffset;
            long expectedRecordOffset = startOffset;
            for (LogRecordBatch batch : fetcher.fetch(startOffset, remoteEndOffset)) {
                assertThat(batch.baseLogOffset()).isEqualTo(expectedBatchOffset);
                try (CloseableIterator<LogRecord> records = batch.records(readContext)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isEqualTo(expectedRecordOffset);
                        expectedRecordOffset++;
                    }
                }
                expectedBatchOffset = batch.nextLogOffset();
            }

            assertThat(expectedBatchOffset).isEqualTo(remoteEndOffset);
            assertThat(expectedRecordOffset).isEqualTo(remoteEndOffset);
            assertThat(maxOpenHandles).hasValue(1);
        }
        assertThat(openedRecords).allMatch(records -> !records.channel().isOpen());
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
