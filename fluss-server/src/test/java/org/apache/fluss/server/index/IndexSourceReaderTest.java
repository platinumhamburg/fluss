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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for continuous local/remote raw-WAL reads used by index replication. */
final class IndexSourceReaderTest {

    private static final TableBucket REMOTE_BUCKET = new TableBucket(1L, 0);
    private static final TableBucket LOCAL_BUCKET = new TableBucket(2L, 0);

    private final LogRecordReadContext readContext = mock(LogRecordReadContext.class);
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    @Test
    void testReadsExactRemoteSegmentsAndLocalHandoff() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 30L, batches(batch(20L, 30L)));
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Arrays.asList(batch(0L, 10L), batch(10L, 20L)));
        ControllableExecutor executor = new ControllableExecutor();
        long bytesBefore =
                TestingMetricGroups.TABLET_SERVER_METRICS.indexSourceRemoteReadBytes().getCount();
        IndexSourceReader reader = readerWithMetrics(sourceWal, () -> remote, executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 30L, 1024);
        assertThat(future).isNotDone();
        executor.runNext();

        try (IndexSourceReader.ReadResult result = future.join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(0L, 30L));
            assertThat(result.nextOffset()).isEqualTo(30L);
            assertThat(remote.closed.get()).isFalse();
        }
        assertThat(
                        TestingMetricGroups.TABLET_SERVER_METRICS
                                .indexSourceRemoteReadBytes()
                                .getCount())
                .isEqualTo(bytesBefore + 20L);
        assertThat(remote.closed.get()).isTrue();
        reader.close();
        assertThat(remote.closed.get()).isTrue();
        assertThat(sourceWal.readOffsets).containsExactly(20L);
    }

    @Test
    void testReturnsRemoteOnlyResultWhenLocalHandoffBatchExceedsRemainingBudget() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(REMOTE_BUCKET, 0L, 10L, 20L, batches(batch(10L, 20L)));
        ControllableExecutor executor = new ControllableExecutor();
        IndexSourceReader reader =
                reader(
                        sourceWal,
                        () -> new TestingRemoteFetcher(Collections.singletonList(batch(0L, 10L))),
                        executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 15);
        executor.runNext();

        try (IndexSourceReader.ReadResult result = future.join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(0L, 10L));
            assertThat(result.nextOffset()).isEqualTo(10L);
        }
        assertThat(sourceWal.readOffsets).containsExactly(10L);
        assertThat(sourceWal.readMaxBytes).containsExactly(5);
        assertThat(sourceWal.readMinOneMessage).containsExactly(true);
    }

    @Test
    void testRemoteByteLimitReturnsHealthyPrefixWithoutPrematureEnd() {
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Arrays.asList(batch(0L, 10L), batch(10L, 20L))) {
                    @Override
                    public IndexSourceReader.RemoteRead fetchBounded(
                            long startOffset, long localLogStartOffset, int maxBytes) {
                        return remoteRead(Collections.singletonList(batch(0L, 10L)), true);
                    }
                };
        IndexSourceReader reader = reader(sourceWal, () -> remote, executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 15);
        executor.runNext();

        try (IndexSourceReader.ReadResult result = future.join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(0L, 10L));
            assertThat(result.nextOffset()).isEqualTo(10L);
        }
        assertThat(remote.closed).isFalse();
    }

    @Test
    void testReusesRemoteSessionAcrossBoundedReadResults() {
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        AtomicInteger opens = new AtomicInteger();
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Arrays.asList(batch(0L, 10L), batch(10L, 20L))) {
                    @Override
                    public IndexSourceReader.RemoteRead fetchBounded(
                            long startOffset, long localLogStartOffset, int maxBytes) {
                        LogRecordBatch selected =
                                startOffset == 0L ? batch(0L, 10L) : batch(10L, 20L);
                        return remoteRead(
                                Collections.singletonList(selected),
                                selected.nextLogOffset() < localLogStartOffset);
                    }
                };
        IndexSourceReader reader =
                reader(
                        sourceWal,
                        () -> {
                            opens.incrementAndGet();
                            return remote;
                        },
                        executor);

        CompletableFuture<IndexSourceReader.ReadResult> first = reader.read(0L, 20L, 5);
        executor.runNext();
        try (IndexSourceReader.ReadResult result = first.join()) {
            assertThat(result.nextOffset()).isEqualTo(10L);
        }
        assertThat(opens).hasValue(1);
        assertThat(remote.closed).isFalse();

        CompletableFuture<IndexSourceReader.ReadResult> second = reader.read(10L, 20L, 5);
        executor.runNext();
        try (IndexSourceReader.ReadResult result = second.join()) {
            assertThat(result.nextOffset()).isEqualTo(20L);
        }

        assertThat(opens).hasValue(1);
        assertThat(remote.closed).isTrue();
        reader.close();
        assertThat(remote.closed).isTrue();
    }

    @Test
    void testLocalFirstBatchMayExceedBudgetWithMinOneMessage() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(LOCAL_BUCKET, 10L, 10L, 20L, batches(batch(10L, 20L)));
        IndexSourceReader reader = reader(sourceWal, null, Executors.directExecutor());

        try (IndexSourceReader.ReadResult result = reader.read(10L, 20L, 5).join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(10L, 20L));
            assertThat(result.nextOffset()).isEqualTo(20L);
        }
        assertThat(sourceWal.readOffsets).containsExactly(10L);
        assertThat(sourceWal.readMaxBytes).containsExactly(5);
        assertThat(sourceWal.readMinOneMessage).containsExactly(true);
    }

    @Test
    void testDeduplicatesOverlappingRemoteRecordsWithoutCopying() {
        LogRecordBatch first = batch(0L, 10L);
        LogRecordBatch overlapping = batch(5L, 20L);
        TestingSourceWal sourceWal =
                new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 25L, batches(batch(20L, 25L)));
        ControllableExecutor executor = new ControllableExecutor();
        IndexSourceReader reader =
                reader(
                        sourceWal,
                        () -> new TestingRemoteFetcher(Arrays.asList(first, overlapping)),
                        executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 25L, 1024);
        executor.runNext();

        try (IndexSourceReader.ReadResult result = future.join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(0L, 25L));
            assertThat(result.batches()).hasSize(3);
        }
    }

    @Test
    void testRejectsRemoteGap() {
        assertRemoteReadFails(
                Arrays.asList(batch(0L, 10L), batch(11L, 20L)), 20L, "expected offset 10");
    }

    @Test
    void testRejectsRemoteEndBeforeLocalStart() {
        assertRemoteReadFails(Collections.singletonList(batch(0L, 10L)), 20L, "expected offset 10");
    }

    @Test
    void testRejectsLocalGap() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(LOCAL_BUCKET, 20L, 20L, 30L, batches(batch(21L, 30L)));
        IndexSourceReader reader = reader(sourceWal, null, Executors.directExecutor());

        assertCorruption(reader.read(20L, 30L, 1024), "expected offset 20");
    }

    @Test
    void testRejectsGapInsideBatchWhileRecordsAreConsumed() {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(3L);
        when(batch.sizeInBytes()).thenReturn(3);
        when(batch.getRecordCount()).thenReturn(3);
        when(batch.records(any()))
                .thenAnswer(
                        ignored ->
                                CloseableIterator.wrap(
                                        Arrays.asList(record(0L), record(2L)).iterator()));
        TestingSourceWal sourceWal = new TestingSourceWal(LOCAL_BUCKET, 0L, 0L, 3L, batches(batch));
        IndexSourceReader reader = reader(sourceWal, null, Executors.directExecutor());

        try (IndexSourceReader.ReadResult result = reader.read(0L, 3L, 1024).join()) {
            assertThatThrownBy(() -> offsets(result))
                    .isInstanceOf(IndexSourceWalCorruptionException.class)
                    .hasMessageContaining("expected record offset 1 but found 2");
        }
    }

    @Test
    void testRejectsCorruptRemoteBytes() throws Exception {
        MemoryLogRecords records =
                genMemoryLogRecordsByObject(Collections.singletonList(DATA1.get(0)));
        byte[] corruptBytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), corruptBytes, 0, corruptBytes.length);
        corruptBytes[corruptBytes.length - 1] ^= 1;
        LogRecordBatch corruptBatch =
                MemoryLogRecords.pointToBytes(corruptBytes).batches().iterator().next();
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 1L, 1L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        IndexSourceReader reader =
                reader(
                        sourceWal,
                        () -> new TestingRemoteFetcher(Collections.singletonList(corruptBatch)),
                        executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 1L, 1024);
        executor.runNext();
        assertCorruption(future, "integrity validation");
    }

    @Test
    void testPartialRemoteFailureCountsConsumedBytesAndOneRemoteFailure() {
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        TabletServerMetricGroup metrics = isolatedMetrics("partial-remote");
        LogRecordBatch consumed = batch(0L, 10L);
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Collections.emptyList()) {
                    @Override
                    public IndexSourceReader.RemoteRead fetchBounded(
                            long startOffset, long localLogStartOffset, int maxBytes) {
                        return failingAfter(consumed, new IllegalStateException("remote decode"));
                    }
                };
        IndexSourceReader reader = readerWithMetrics(sourceWal, () -> remote, executor, metrics);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 1024);
        executor.runNext();

        assertThatThrownBy(future::join).hasRootCauseMessage("remote decode");
        assertThat(metrics.indexSourceRemoteReadBytes().getCount()).isEqualTo(10L);
        assertThat(metrics.indexSourceRemoteReadFailures().getCount()).isEqualTo(1L);
    }

    @Test
    void testLocalHandoffFailureDoesNotCountAsRemoteFailure() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(REMOTE_BUCKET, 0L, 10L, 20L, batches()) {
                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            FetchIsolation isolation,
                            boolean minOneMessage)
                            throws IOException {
                        throw new IOException("local handoff");
                    }
                };
        ControllableExecutor executor = new ControllableExecutor();
        TabletServerMetricGroup metrics = isolatedMetrics("local-handoff");
        IndexSourceReader reader =
                readerWithMetrics(
                        sourceWal,
                        () -> new TestingRemoteFetcher(Collections.singletonList(batch(0L, 10L))),
                        executor,
                        metrics);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 1024);
        executor.runNext();

        assertThatThrownBy(future::join).hasRootCauseMessage("local handoff");
        assertThat(metrics.indexSourceRemoteReadBytes().getCount()).isEqualTo(10L);
        assertThat(metrics.indexSourceRemoteReadFailures().getCount()).isZero();
    }

    @Test
    void testRemoteResourceCloseFailureCountsAsRemoteFailure() {
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 10L, 10L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        TabletServerMetricGroup metrics = isolatedMetrics("remote-resource");
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Collections.emptyList()) {
                    @Override
                    public IndexSourceReader.RemoteRead fetchBounded(
                            long startOffset, long localLogStartOffset, int maxBytes) {
                        return new IndexSourceReader.RemoteRead() {
                            @Override
                            public boolean stoppedByByteLimit() {
                                return false;
                            }

                            @Override
                            public Iterator<LogRecordBatch> iterator() {
                                return Collections.singletonList(batch(0L, 10L)).iterator();
                            }

                            @Override
                            public void close() {
                                throw new IllegalStateException("remote resource close");
                            }
                        };
                    }
                };
        IndexSourceReader reader = readerWithMetrics(sourceWal, () -> remote, executor, metrics);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 10L, 1024);
        executor.runNext();
        IndexSourceReader.ReadResult result = future.join();

        assertThatThrownBy(result::close).hasRootCauseMessage("remote resource close");
        assertThat(metrics.indexSourceRemoteReadBytes().getCount()).isEqualTo(10L);
        assertThat(metrics.indexSourceRemoteReadFailures().getCount()).isEqualTo(1L);
    }

    @Test
    void testClipsRecordsAtHighWatermark() {
        TestingSourceWal sourceWal =
                new TestingSourceWal(LOCAL_BUCKET, 20L, 20L, 35L, batches(batch(20L, 35L)));
        IndexSourceReader reader = reader(sourceWal, null, Executors.directExecutor());

        try (IndexSourceReader.ReadResult result = reader.read(20L, 30L, 1024).join()) {
            assertThat(offsets(result)).containsExactlyElementsOf(offsets(20L, 30L));
            assertThat(result.nextOffset()).isEqualTo(30L);
        }
    }

    @Test
    void testRejectsSecondRemoteReadWhileFirstIsPending() {
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        IndexSourceReader reader =
                reader(
                        sourceWal,
                        () -> new TestingRemoteFetcher(Collections.singletonList(batch(0L, 20L))),
                        executor);

        reader.read(0L, 20L, 1024);
        assertThatIllegalStateException()
                .isThrownBy(() -> reader.read(0L, 20L, 1024))
                .withMessageContaining("read already in progress");
    }

    @Test
    void testCloseCancelsPendingFetchAndClosesDownloadedResources() throws Exception {
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch releaseFetch = new CountDownLatch(1);
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Collections.singletonList(batch(0L, 20L))) {
                    @Override
                    public Iterable<LogRecordBatch> fetch(
                            long startOffset, long localLogStartOffset) throws Exception {
                        fetchStarted.countDown();
                        releaseFetch.await();
                        return super.fetch(startOffset, localLogStartOffset);
                    }
                };
        ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        closeables.add(
                () -> {
                    releaseFetch.countDown();
                    executor.shutdownNow();
                    executor.awaitTermination(10, TimeUnit.SECONDS);
                });
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        IndexSourceReader reader = reader(sourceWal, () -> remote, executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 1024);
        assertThat(fetchStarted.await(10, TimeUnit.SECONDS)).isTrue();
        reader.close();
        releaseFetch.countDown();

        assertThat(future).isCancelled();
        assertThat(remote.closed.get()).isTrue();
    }

    @Test
    void testCloseBeforeQueuedFetchStartsClosesFetcherOpenedByTask() {
        ControllableExecutor executor = new ControllableExecutor();
        TestingRemoteFetcher remote =
                new TestingRemoteFetcher(Collections.singletonList(batch(0L, 20L)));
        TestingSourceWal sourceWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 20L, 20L, batches());
        IndexSourceReader reader = reader(sourceWal, () -> remote, executor);

        CompletableFuture<IndexSourceReader.ReadResult> future = reader.read(0L, 20L, 1024);
        reader.close();
        executor.runNext();

        assertThat(future).isCancelled();
        assertThat(remote.closed.get()).isTrue();
    }

    @Test
    void testPendingRemoteFetchDoesNotBlockAnotherReplicatorOnSharedWorker() throws Exception {
        ControllableExecutor remoteExecutor = new ControllableExecutor();
        TestingSourceWal remoteWal = new TestingSourceWal(REMOTE_BUCKET, 0L, 1L, 1L, batches());
        IndexSourceReader remoteReader =
                reader(
                        remoteWal,
                        () -> new TestingRemoteFetcher(Collections.singletonList(batch(0L, 1L))),
                        remoteExecutor);

        TestingSourceWal localWal =
                new TestingSourceWal(LOCAL_BUCKET, 0L, 0L, 1L, batches(batch(0L, 1L)));
        IndexSourceReader localReader = reader(localWal, null, Executors.directExecutor());
        IndexSpec spec =
                new IndexSpec(
                        "idx",
                        IndexVisibility.ASYNC,
                        3L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            throw new AssertionError("null index columns must not be encoded");
                        });
        CountDownLatch localAdvanced = new CountDownLatch(1);
        IndexReplicator remoteReplicator = replicator(remoteReader, spec, (sync, all) -> {});
        IndexReplicator localReplicator =
                replicator(localReader, spec, (sync, all) -> localAdvanced.countDown());
        IndexReplicatorPool pool = new IndexReplicatorPool(1, 1024, 10_000L);
        closeables.add(pool);
        closeables.add(remoteReplicator);
        closeables.add(localReplicator);

        pool.register(REMOTE_BUCKET, remoteReplicator);
        pool.register(LOCAL_BUCKET, localReplicator);

        assertThat(remoteExecutor.submitted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(localAdvanced.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(remoteExecutor).hasToString("1 pending task");
        assertThat(remoteReplicator.nextReadOffset()).isZero();
        assertThat(localReplicator.nextReadOffset()).isEqualTo(1L);
    }

    private IndexSourceReader reader(
            TestingSourceWal sourceWal,
            IndexSourceReader.RemoteFetcherFactory remoteFetcherFactory,
            Executor executor) {
        IndexSourceReader reader =
                new IndexSourceReader(sourceWal, remoteFetcherFactory, executor, readContext);
        closeables.add(reader);
        return reader;
    }

    private IndexReplicator replicator(
            IndexSourceReader reader,
            IndexSpec spec,
            IndexReplicator.IndexProgressListener progressListener) {
        return IndexReplicator.forTesting(
                reader,
                Collections.singletonList(spec),
                new IndexAccumulator(),
                readContext,
                0L,
                1024,
                1024,
                progressListener);
    }

    private void assertRemoteReadFails(
            List<LogRecordBatch> remoteBatches, long localStart, String message) {
        TestingSourceWal sourceWal =
                new TestingSourceWal(REMOTE_BUCKET, 0L, localStart, localStart + 10L, batches());
        ControllableExecutor executor = new ControllableExecutor();
        long failuresBefore =
                TestingMetricGroups.TABLET_SERVER_METRICS
                        .indexSourceRemoteReadFailures()
                        .getCount();
        IndexSourceReader reader =
                readerWithMetrics(
                        sourceWal, () -> new TestingRemoteFetcher(remoteBatches), executor);

        CompletableFuture<IndexSourceReader.ReadResult> future =
                reader.read(0L, localStart + 10L, 1024);
        executor.runNext();
        assertCorruption(future, message);
        assertThat(
                        TestingMetricGroups.TABLET_SERVER_METRICS
                                .indexSourceRemoteReadFailures()
                                .getCount())
                .isEqualTo(failuresBefore + 1L);
    }

    private IndexSourceReader readerWithMetrics(
            TestingSourceWal sourceWal,
            IndexSourceReader.RemoteFetcherFactory remoteFetcherFactory,
            Executor executor) {
        return readerWithMetrics(
                sourceWal,
                remoteFetcherFactory,
                executor,
                TestingMetricGroups.TABLET_SERVER_METRICS);
    }

    private IndexSourceReader readerWithMetrics(
            TestingSourceWal sourceWal,
            IndexSourceReader.RemoteFetcherFactory remoteFetcherFactory,
            Executor executor,
            TabletServerMetricGroup metrics) {
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceWal, remoteFetcherFactory, executor, readContext, metrics);
        closeables.add(reader);
        return reader;
    }

    private static TabletServerMetricGroup isolatedMetrics(String clusterId) {
        return new TabletServerMetricGroup(
                NOPMetricRegistry.INSTANCE, clusterId, "rack", "host", 1);
    }

    private static void assertCorruption(
            CompletableFuture<IndexSourceReader.ReadResult> future, String message) {
        Throwable failure = catchThrowable(future::join);
        assertThat(failure).hasCauseInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(failure.getCause()).hasMessageContaining(message);
    }

    private List<Long> offsets(IndexSourceReader.ReadResult result) {
        List<Long> offsets = new ArrayList<>();
        for (LogRecordBatch batch : result.batches()) {
            try (CloseableIterator<LogRecord> records = batch.records(readContext)) {
                while (records.hasNext()) {
                    offsets.add(records.next().logOffset());
                }
            }
        }
        return offsets;
    }

    private static List<Long> offsets(long startInclusive, long endExclusive) {
        List<Long> offsets = new ArrayList<>();
        for (long offset = startInclusive; offset < endExclusive; offset++) {
            offsets.add(offset);
        }
        return offsets;
    }

    private static LogRecords batches(LogRecordBatch... batches) {
        List<LogRecordBatch> batchList = Arrays.asList(batches);
        return new LogRecords() {
            @Override
            public int sizeInBytes() {
                return batchList.stream().mapToInt(LogRecordBatch::sizeInBytes).sum();
            }

            @Override
            public Iterable<LogRecordBatch> batches() {
                return batchList;
            }
        };
    }

    private static LogRecordBatch batch(long startInclusive, long endExclusive) {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        List<LogRecord> records = new ArrayList<>();
        for (long offset = startInclusive; offset < endExclusive; offset++) {
            records.add(record(offset));
        }
        when(batch.baseLogOffset()).thenReturn(startInclusive);
        when(batch.lastLogOffset()).thenReturn(endExclusive - 1L);
        when(batch.nextLogOffset()).thenReturn(endExclusive);
        when(batch.sizeInBytes()).thenReturn(Math.max(1, records.size()));
        when(batch.getRecordCount()).thenReturn(records.size());
        when(batch.records(any()))
                .thenAnswer(ignored -> CloseableIterator.wrap(records.iterator()));
        return batch;
    }

    private static LogRecord record(long offset) {
        LogRecord record = mock(LogRecord.class);
        InternalRow row = mock(InternalRow.class);
        when(record.logOffset()).thenReturn(offset);
        when(record.getChangeType()).thenReturn(ChangeType.INSERT);
        when(record.getRow()).thenReturn(row);
        when(row.isNullAt(0)).thenReturn(true);
        return record;
    }

    private static class TestingSourceWal implements IndexReplicator.SourceWal {
        private final TableBucket tableBucket;
        private final long logStartOffset;
        private final long localLogStartOffset;
        private final long highWatermark;
        private final LogRecords localRecords;
        private final List<Long> readOffsets = new ArrayList<>();
        private final List<Integer> readMaxBytes = new ArrayList<>();
        private final List<Boolean> readMinOneMessage = new ArrayList<>();

        private TestingSourceWal(
                TableBucket tableBucket,
                long logStartOffset,
                long localLogStartOffset,
                long highWatermark,
                LogRecords localRecords) {
            this.tableBucket = tableBucket;
            this.logStartOffset = logStartOffset;
            this.localLogStartOffset = localLogStartOffset;
            this.highWatermark = highWatermark;
            this.localRecords = localRecords;
        }

        @Override
        public TableBucket tableBucket() {
            return tableBucket;
        }

        @Override
        public long highWatermark() {
            return highWatermark;
        }

        @Override
        public long logStartOffset() {
            return logStartOffset;
        }

        @Override
        public long localLogStartOffset() {
            return localLogStartOffset;
        }

        @Override
        public FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage)
                throws IOException {
            readOffsets.add(offset);
            readMaxBytes.add(maxBytes);
            readMinOneMessage.add(minOneMessage);
            return new FetchDataInfo(localRecords);
        }
    }

    private static class TestingRemoteFetcher implements IndexSourceReader.RemoteFetcher {
        private final List<LogRecordBatch> batches;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TestingRemoteFetcher(List<LogRecordBatch> batches) {
            this.batches = batches;
        }

        @Override
        public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
                throws Exception {
            return batches;
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static IndexSourceReader.RemoteRead remoteRead(
            Iterable<LogRecordBatch> batches, boolean stoppedByByteLimit) {
        return new IndexSourceReader.RemoteRead() {
            @Override
            public boolean stoppedByByteLimit() {
                return stoppedByByteLimit;
            }

            @Override
            public java.util.Iterator<LogRecordBatch> iterator() {
                return batches.iterator();
            }

            @Override
            public void close() {}
        };
    }

    private static IndexSourceReader.RemoteRead failingAfter(
            LogRecordBatch batch, RuntimeException failure) {
        return remoteRead(
                () ->
                        new Iterator<LogRecordBatch>() {
                            private boolean delivered;

                            @Override
                            public boolean hasNext() {
                                if (delivered) {
                                    throw failure;
                                }
                                return true;
                            }

                            @Override
                            public LogRecordBatch next() {
                                delivered = true;
                                return batch;
                            }
                        },
                false);
    }

    private static final class ControllableExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private final CountDownLatch submitted = new CountDownLatch(1);

        @Override
        public synchronized void execute(Runnable command) {
            tasks.add(command);
            submitted.countDown();
        }

        private void runNext() {
            Runnable task;
            synchronized (this) {
                task = tasks.remove();
            }
            task.run();
        }

        @Override
        public synchronized String toString() {
            return tasks.size() + " pending task" + (tasks.size() == 1 ? "" : "s");
        }
    }
}
