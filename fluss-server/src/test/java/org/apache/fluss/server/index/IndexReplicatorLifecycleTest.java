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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link IndexReplicator} lifecycle ownership. */
public class IndexReplicatorLifecycleTest {

    private static final TableBucket SOURCE_BUCKET = new TableBucket(11L, 0);
    private static final TableBucket TARGET_BUCKET = new TableBucket(12L, 0);

    @Test
    void closeClosesReadContextExactlyOnce() {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        IndexReplicator replicator =
                new IndexReplicator(
                        null,
                        Collections.emptyList(),
                        new IndexSendBuffer(),
                        readContext,
                        0L,
                        1024,
                        (sync, all) -> {});

        replicator.close();
        replicator.close();

        verify(readContext, times(1)).close();
    }

    @Test
    void closeWaitsForRemoteResultConsumptionBeforeClosingResources() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        CountDownLatch consuming = new CountDownLatch(1);
        CountDownLatch releaseConsumption = new CountDownLatch(1);
        AtomicBoolean remoteClosed = new AtomicBoolean();
        LogRecordBatch batch =
                batch(readContext, new BarrierIterator(consuming, releaseConsumption));
        IndexSourceReader.SourceLog sourceLog = sourceLog(0L, 1L, emptyRecords());
        IndexSourceReader.RemoteFetcher remoteFetcher =
                new IndexSourceReader.RemoteFetcher() {
                    @Override
                    public Iterable<LogRecordBatch> fetch(
                            long startOffset, long localLogStartOffset) {
                        return Collections.singletonList(batch);
                    }

                    @Override
                    public void close() {
                        remoteClosed.set(true);
                    }
                };
        IndexSourceReader reader =
                new IndexSourceReader(sourceLog, () -> remoteFetcher, Runnable::run, readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator replicator = replicator(reader, sendBuffer, readContext);
        ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();

        Future<Boolean> poll = executor.submit(replicator::poll);
        assertThat(consuming.await(10, TimeUnit.SECONDS)).isTrue();
        Future<?> close =
                executor.submit(
                        () -> {
                            closeThread.set(Thread.currentThread());
                            closeStarted.countDown();
                            replicator.close();
                        });
        assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
        awaitContendedOrDone(close, closeThread);

        try {
            assertThat(close.isDone()).isFalse();
            assertThat(remoteClosed).isFalse();
        } finally {
            releaseConsumption.countDown();
            executor.shutdown();
        }
        assertThat(poll.get(10, TimeUnit.SECONDS)).isTrue();
        close.get(10, TimeUnit.SECONDS);
        assertThat(remoteClosed).isTrue();
        sendBuffer.dropForReplicator(replicator);
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void closeWaitsForPollAtSendBufferPublicationBeforeDrop() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        CountDownLatch admitted = new CountDownLatch(1);
        CountDownLatch releasePublication = new CountDownLatch(1);
        LogRecordBatch batch =
                batch(
                        readContext,
                        CloseableIterator.wrap(Collections.singletonList(record()).iterator()));
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 0L, records(batch)), null, Runnable::run, readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        sendBuffer.setAfterAppendAdmissionHook(
                () -> {
                    admitted.countDown();
                    awaitUninterruptibly(releasePublication);
                });
        IndexReplicator replicator = replicator(reader, sendBuffer, readContext);
        ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();

        Future<Boolean> poll = executor.submit(replicator::poll);
        assertThat(admitted.await(10, TimeUnit.SECONDS)).isTrue();
        Future<?> close =
                executor.submit(
                        () -> {
                            closeThread.set(Thread.currentThread());
                            closeStarted.countDown();
                            replicator.close();
                        });
        assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
        awaitContendedOrDone(close, closeThread);

        try {
            assertThat(close.isDone()).isFalse();
        } finally {
            releasePublication.countDown();
            executor.shutdown();
        }
        assertThat(poll.get(10, TimeUnit.SECONDS)).isTrue();
        close.get(10, TimeUnit.SECONDS);
        assertThat(sendBuffer.dropForReplicator(replicator)).isZero();
        assertThat(sendBuffer.hasPending(TARGET_BUCKET)).isFalse();
        assertThat(sendBuffer.pendingBytes(replicator)).isZero();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void terminalWindowFailureRetiresAnotherIndexesPendingRemoteRead() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch =
                batch(
                        readContext,
                        CloseableIterator.wrap(Collections.singletonList(record()).iterator()));
        Queue<Runnable> queuedRemoteReads = new ArrayDeque<>();
        AtomicInteger submittedReads = new AtomicInteger();
        AtomicInteger openedFetchers = new AtomicInteger();
        AtomicInteger closedFetchers = new AtomicInteger();
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 1L, emptyRecords()),
                        () -> {
                            openedFetchers.incrementAndGet();
                            return remoteFetcher(batch, closedFetchers);
                        },
                        command -> {
                            if (submittedReads.getAndIncrement() == 0) {
                                command.run();
                            } else {
                                queuedRemoteReads.add(command);
                            }
                        },
                        readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator replicator =
                replicator(
                        reader,
                        sendBuffer,
                        readContext,
                        Arrays.asList(spec("first"), spec("second")));

        assertThat(replicator.poll()).isTrue();
        IndexWindow firstWindow = replicator.inFlightWindow("first");
        assertThat(firstWindow).isNotNull();
        assertThat(queuedRemoteReads).hasSize(1);
        assertThat(replicator.hasPendingRead()).isTrue();

        RuntimeException failure = new RuntimeException("terminal sender failure");
        for (IndexBatch drained : firstWindow.tryFailAndDrain(failure)) {
            sendBuffer.remove(drained);
            sendBuffer.release(drained);
        }

        assertThat(replicator.terminalFailure()).isSameAs(failure);
        assertThat(replicator.hasPendingRead()).isFalse();
        assertThatThrownBy(() -> reader.read(0L, 1L, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        queuedRemoteReads.remove().run();
        assertThat(openedFetchers).hasValue(2);
        assertThat(closedFetchers).hasValue(2);
        assertThat(replicator.hasPendingRead()).isFalse();
        assertThat(replicator.poll()).isFalse();
        replicator.close();
    }

    @Test
    void terminalCleanupContinuesWhenCompletedRemoteReadCloseFails() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch =
                batch(
                        readContext,
                        CloseableIterator.wrap(Collections.singletonList(record()).iterator()));
        Queue<Runnable> queuedRemoteReads = new ArrayDeque<>();
        AtomicInteger openedFetchers = new AtomicInteger();
        AtomicInteger closedFetchers = new AtomicInteger();
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 1L, emptyRecords()),
                        () -> {
                            if (openedFetchers.getAndIncrement() == 0) {
                                return remoteFetcher(batch, closedFetchers);
                            }
                            return new IndexSourceReader.RemoteFetcher() {
                                @Override
                                public Iterable<LogRecordBatch> fetch(
                                        long startOffset, long localLogStartOffset) {
                                    return Collections.singletonList(batch);
                                }

                                @Override
                                public IndexSourceReader.RemoteRead fetchBounded(
                                        long startOffset, long localLogStartOffset, int maxBytes) {
                                    return new IndexSourceReader.RemoteRead() {
                                        @Override
                                        public boolean stoppedByByteLimit() {
                                            return false;
                                        }

                                        @Override
                                        public java.util.Iterator<LogRecordBatch> iterator() {
                                            return Collections.singletonList(batch).iterator();
                                        }

                                        @Override
                                        public void close() {
                                            throw new RuntimeException("remote read close failed");
                                        }
                                    };
                                }

                                @Override
                                public void close() {
                                    closedFetchers.incrementAndGet();
                                }
                            };
                        },
                        command -> {
                            if (openedFetchers.get() == 0) {
                                command.run();
                            } else {
                                queuedRemoteReads.add(command);
                            }
                        },
                        readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator replicator =
                replicator(
                        reader,
                        sendBuffer,
                        readContext,
                        Arrays.asList(spec("first"), spec("second")));

        assertThat(replicator.poll()).isTrue();
        IndexWindow firstWindow = replicator.inFlightWindow("first");
        assertThat(firstWindow).isNotNull();
        assertThat(queuedRemoteReads).hasSize(1);
        queuedRemoteReads.remove().run();
        assertThat(replicator.hasPendingRead()).isTrue();

        RuntimeException failure = new RuntimeException("terminal sender failure");
        firstWindow.tryFailAndDrain(failure);

        assertThat(replicator.terminalFailure()).isSameAs(failure);
        assertThat(failure.getSuppressed())
                .singleElement()
                .satisfies(
                        cleanupFailure -> {
                            assertThat(cleanupFailure)
                                    .hasMessageContaining("Failed to close index source read");
                            assertThat(cleanupFailure.getCause())
                                    .hasMessageContaining("remote read close failed");
                        });
        assertThat(replicator.hasPendingRead()).isFalse();
        assertThat(replicator.inFlightWindow("first")).isNull();
        assertThat(sendBuffer.pendingBytes(replicator)).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        assertThat(openedFetchers).hasValue(2);
        assertThat(closedFetchers).hasValue(2);
        verify(readContext, times(1)).close();
        assertThatThrownBy(() -> reader.read(0L, 1L, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        replicator.close();
        verify(readContext, times(1)).close();
    }

    @Test
    void terminalWindowFailureRetiresAllPublishedWindowsForOwner() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(1L);
        when(batch.sizeInBytes()).thenReturn(1);
        when(batch.getRecordCount()).thenReturn(1);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored ->
                                CloseableIterator.wrap(
                                        Collections.singletonList(record()).iterator()));
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicator replicator =
                replicator(
                        new IndexSourceReader(
                                sourceLog(0L, 0L, records(batch)),
                                null,
                                Runnable::run,
                                readContext),
                        sendBuffer,
                        readContext,
                        Arrays.asList(spec("first"), spec("second")));

        assertThat(replicator.poll()).isTrue();
        IndexWindow first = replicator.inFlightWindow("first");
        IndexWindow second = replicator.inFlightWindow("second");
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(sendBuffer.pendingBytes(replicator)).isPositive();

        RuntimeException failure = new RuntimeException("terminal sender failure");
        for (IndexBatch drained : first.tryFailAndDrain(failure)) {
            sendBuffer.remove(drained);
            sendBuffer.release(drained);
        }

        assertThat(replicator.terminalFailure()).isSameAs(failure);
        assertThat(replicator.inFlightWindow("first")).isNull();
        assertThat(replicator.inFlightWindow("second")).isNull();
        assertThat(second.isActive()).isFalse();
        assertThat(sendBuffer.pendingBytes(replicator)).isZero();
        assertThat(sendBuffer.hasUnsent()).isFalse();
        verify(readContext, times(1)).close();
        replicator.close();
        verify(readContext, times(1)).close();
    }

    @Test
    void lazyRecordGapTransitionsTerminalWithoutRereadingRemoteWal() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecord first = record(0L);
        LogRecord gap = record(2L);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(3L);
        when(batch.sizeInBytes()).thenReturn(3);
        when(batch.getRecordCount()).thenReturn(3);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored -> CloseableIterator.wrap(Arrays.asList(first, gap).iterator()));
        Queue<Runnable> queuedRemoteReads = new ArrayDeque<>();
        AtomicInteger fetchCount = new AtomicInteger();
        AtomicInteger closeCount = new AtomicInteger();
        IndexSourceReader.RemoteFetcher fetcher =
                new IndexSourceReader.RemoteFetcher() {
                    @Override
                    public Iterable<LogRecordBatch> fetch(
                            long startOffset, long localLogStartOffset) {
                        fetchCount.incrementAndGet();
                        return Collections.singletonList(batch);
                    }

                    @Override
                    public void close() {
                        closeCount.incrementAndGet();
                    }
                };
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 3L, 3L, emptyRecords()),
                        () -> fetcher,
                        queuedRemoteReads::add,
                        readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<IndexReplicator> reportedReplicator = new AtomicReference<>();
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        IndexReplicator replicator =
                replicator(
                        reader,
                        sendBuffer,
                        readContext,
                        (reported, failure) -> {
                            verify(readContext, times(1)).close();
                            assertThat(sendBuffer.pendingBytes(reported)).isZero();
                            callbackCount.incrementAndGet();
                            reportedReplicator.set(reported);
                            reportedFailure.set(failure);
                        });

        assertThat(replicator.poll()).isFalse();
        assertThat(queuedRemoteReads).hasSize(1);
        queuedRemoteReads.remove().run();

        assertThatThrownBy(replicator::poll)
                .isInstanceOf(IndexSourceWalCorruptionException.class)
                .hasMessageContaining("expected record offset 1 but found 2");
        Throwable terminal = replicator.terminalFailure();
        assertThat(terminal).isInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(callbackCount).hasValue(1);
        assertThat(reportedReplicator.get()).isSameAs(replicator);
        assertThat(reportedFailure.get()).isSameAs(replicator.terminalFailure());
        verify(readContext, times(1)).close();
        assertThat(sendBuffer.pendingBytes(replicator)).isZero();
        assertThat(fetchCount).hasValue(1);
        assertThat(closeCount).hasValue(1);

        assertThat(replicator.poll()).isFalse();
        assertThat(replicator.terminalFailure()).isSameAs(terminal);
        assertThat(fetchCount).hasValue(1);
        replicator.close();
        replicator.close();
        assertThat(callbackCount).hasValue(1);
    }

    @Test
    void terminalCallbackFailureIsSuppressedAfterCleanup() throws Exception {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        LogRecord first = record(0L);
        LogRecord gap = record(2L);
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(3L);
        when(batch.sizeInBytes()).thenReturn(3);
        when(batch.getRecordCount()).thenReturn(3);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored -> CloseableIterator.wrap(Arrays.asList(first, gap).iterator()));
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 0L, 3L, records(batch)), null, Runnable::run, readContext);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        AtomicInteger callbackCount = new AtomicInteger();
        RuntimeException callbackFailure = new RuntimeException("terminal callback failed");
        IndexReplicator replicator =
                replicator(
                        reader,
                        sendBuffer,
                        readContext,
                        (reported, failure) -> {
                            callbackCount.incrementAndGet();
                            throw callbackFailure;
                        });

        assertThatThrownBy(replicator::poll)
                .isInstanceOf(IndexSourceWalCorruptionException.class)
                .hasMessageContaining("expected record offset 1 but found 2");

        Throwable terminal = replicator.terminalFailure();
        assertThat(terminal).isInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(terminal.getSuppressed()).contains(callbackFailure);
        assertThat(callbackCount).hasValue(1);
        verify(readContext, times(1)).close();
        assertThat(sendBuffer.pendingBytes(replicator)).isZero();

        assertThat(replicator.poll()).isFalse();
        replicator.close();
        replicator.close();
        assertThat(callbackCount).hasValue(1);
    }

    private static IndexReplicator replicator(
            IndexSourceReader reader,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext) {
        return replicator(reader, sendBuffer, readContext, Collections.singletonList(spec("idx")));
    }

    private static IndexReplicator replicator(
            IndexSourceReader reader,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure) {
        return IndexReplicator.forTesting(
                reader,
                Collections.singletonList(spec("idx")),
                sendBuffer,
                readContext,
                0L,
                1024,
                1024,
                (sync, all) -> {},
                onTerminalFailure);
    }

    private static IndexReplicator replicator(
            IndexSourceReader reader,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            java.util.List<IndexSpec> specs) {
        return IndexReplicator.forTesting(
                reader, specs, sendBuffer, readContext, 0L, 1024, 1024, (sync, all) -> {});
    }

    private static IndexSpec spec(String name) {
        RowEncoder encoder =
                RowEncoder.create(
                        KvFormat.COMPACTED,
                        new org.apache.fluss.types.DataType[] {DataTypes.BIGINT()});
        return new IndexSpec(
                name,
                IndexVisibility.ASYNC,
                TARGET_BUCKET.getTableId(),
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> {
                    encoder.startNewRow();
                    encoder.encodeField(0, row.getLong(0));
                    BinaryRow value = encoder.finishRow();
                    return new IndexSpec.IndexEntry(new byte[] {1}, value, 0);
                });
    }

    private static IndexSourceReader.RemoteFetcher remoteFetcher(
            LogRecordBatch batch, AtomicInteger closeCount) {
        return new IndexSourceReader.RemoteFetcher() {
            @Override
            public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset) {
                return Collections.singletonList(batch);
            }

            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };
    }

    private static IndexSourceReader.SourceLog sourceLog(
            long logStartOffset, long localLogStartOffset, LogRecords records) {
        return sourceLog(logStartOffset, localLogStartOffset, 1L, records);
    }

    private static IndexSourceReader.SourceLog sourceLog(
            long logStartOffset, long localLogStartOffset, long highWatermark, LogRecords records) {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return SOURCE_BUCKET;
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
                return new FetchDataInfo(records);
            }
        };
    }

    private static LogRecordBatch batch(
            LogRecordReadContext readContext, CloseableIterator<LogRecord> iterator) {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(1L);
        when(batch.sizeInBytes()).thenReturn(1);
        when(batch.getRecordCount()).thenReturn(1);
        when(batch.records(readContext)).thenReturn(iterator);
        return batch;
    }

    private static LogRecords records(LogRecordBatch batch) {
        return new LogRecords() {
            @Override
            public int sizeInBytes() {
                return batch.sizeInBytes();
            }

            @Override
            public Iterable<LogRecordBatch> batches() {
                return Collections.singletonList(batch);
            }
        };
    }

    private static LogRecords emptyRecords() {
        return new LogRecords() {
            @Override
            public int sizeInBytes() {
                return 0;
            }

            @Override
            public Iterable<LogRecordBatch> batches() {
                return Collections.emptyList();
            }
        };
    }

    private static LogRecord record() {
        return record(0L);
    }

    private static LogRecord record(long offset) {
        return new LogRecord() {
            @Override
            public long logOffset() {
                return offset;
            }

            @Override
            public long timestamp() {
                return 0L;
            }

            @Override
            public ChangeType getChangeType() {
                return ChangeType.INSERT;
            }

            @Override
            public GenericRow getRow() {
                return GenericRow.of(1L);
            }
        };
    }

    private static void awaitContendedOrDone(
            Future<?> future, AtomicReference<Thread> threadReference) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!future.isDone()) {
            Thread thread = threadReference.get();
            if (thread != null
                    && (thread.getState() == Thread.State.WAITING
                            || thread.getState() == Thread.State.BLOCKED)) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("close neither completed nor contended on poll");
            }
            Thread.onSpinWait();
        }
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

    private static final class BarrierIterator implements CloseableIterator<LogRecord> {
        private final CountDownLatch consuming;
        private final CountDownLatch releaseConsumption;
        private boolean consumed;

        private BarrierIterator(CountDownLatch consuming, CountDownLatch releaseConsumption) {
            this.consuming = consuming;
            this.releaseConsumption = releaseConsumption;
        }

        @Override
        public boolean hasNext() {
            if (consumed) {
                return false;
            }
            consuming.countDown();
            awaitUninterruptibly(releaseConsumption);
            return true;
        }

        @Override
        public LogRecord next() {
            if (consumed) {
                throw new NoSuchElementException();
            }
            consumed = true;
            return record();
        }

        @Override
        public void close() {}
    }
}
