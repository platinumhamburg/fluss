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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link IndexReplicationSupervisor}'s index replication lifecycle. */
class IndexReplicationSupervisorTest {

    private static final TableBucket SOURCE_BUCKET = new TableBucket(41L, 0);
    private static final TableBucket TABLE_BUCKET = new TableBucket(42L, 0);
    private static final TableBucket TARGET_BUCKET = new TableBucket(43L, 0);

    private IndexReplicatorPool pool;

    @BeforeEach
    void setUp() {
        pool = new IndexReplicatorPool(1, 1024, 10_000L);
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void terminalFailureOnlyChangesTheCurrentInstalledReplicator() {
        IndexReplicationSupervisor controller = controller();
        IndexReplicator first = idleReplicator((ignored, failure) -> {});
        RuntimeException firstFailure = new RuntimeException("first failure");

        controller.installIndexReplicator(first);
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.RUNNING);
        assertThat(controller.getIndexReplicator()).isSameAs(first);

        controller.onIndexReplicatorFailed(first, firstFailure);
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.FAILED);
        assertThat(controller.isFailed()).isTrue();

        controller.onBecomeFollower();
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.NOT_STARTED);
        assertThat(controller.getIndexReplicator()).isNull();
        assertThat(first.isClosed()).isTrue();

        IndexReplicator replacement = idleReplicator((ignored, failure) -> {});
        controller.installIndexReplicator(replacement);
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.RUNNING);
        assertThat(controller.getIndexReplicator()).isSameAs(replacement);

        controller.onIndexReplicatorFailed(first, new RuntimeException("late old callback"));
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.RUNNING);
        assertThat(controller.getIndexReplicator()).isSameAs(replacement);

        controller.onIndexReplicatorFailed(
                replacement, new RuntimeException("replacement failure"));
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.FAILED);
        assertThat(controller.isFailed()).isTrue();

        controller.close();
        controller.close();
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.NOT_STARTED);
        assertThat(controller.getIndexReplicator()).isNull();
        assertThat(replacement.isClosed()).isTrue();
    }

    @Test
    void terminalCallbackMovesInstalledControllerToFailed() throws Exception {
        IndexReplicationSupervisor controller = controller();
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        AtomicBoolean exposeCorruption = new AtomicBoolean();
        IndexReplicator replicator =
                corruptingReplicator(
                        readContext, controller::onIndexReplicatorFailed, exposeCorruption);

        controller.installIndexReplicator(replicator);
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.RUNNING);

        exposeCorruption.set(true);
        try {
            replicator.poll();
        } catch (IndexSourceWalCorruptionException ignored) {
            // The pool worker can race the explicit poll and observe the corrupt WAL first.
        }
        awaitFailed(controller);

        assertThat(replicator.terminalFailure())
                .isInstanceOf(IndexSourceWalCorruptionException.class);
        assertThat(controller.getIndexReplicator()).isSameAs(replicator);
        assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.FAILED);
        verify(readContext, times(1)).close();
    }

    @Test
    void followerStopUnregistersBeforeClosingAndDroppingCurrentReplicator() throws Exception {
        IndexSendBuffer sendBuffer = new IndexSendBuffer();
        IndexReplicationSupervisor controller = controller(sendBuffer);
        AtomicBoolean observeProbePoll = new AtomicBoolean();
        CountDownLatch probePolled = new CountDownLatch(1);
        IndexReplicator probe =
                idleReplicator(
                        new IndexSendBuffer(),
                        mock(LogRecordReadContext.class),
                        (ignored, failure) -> {},
                        () -> {
                            if (observeProbePoll.get()) {
                                probePolled.countDown();
                            }
                        });
        LogRecordReadContext oldReadContext = mock(LogRecordReadContext.class);
        AtomicReference<IndexReplicator> oldReference = new AtomicReference<>();
        doAnswer(
                        ignored -> {
                            // IndexReplicator.close() retires its own queued batches before it
                            // closes the read context. The positive pre-stop assertion below
                            // proves this was a real owned backlog rather than an empty fixture.
                            assertThat(sendBuffer.pendingBytes(oldReference.get())).isZero();
                            pool.register(TABLE_BUCKET, probe);
                            return null;
                        })
                .when(oldReadContext)
                .close();
        IndexReplicator old =
                idleReplicator(sendBuffer, oldReadContext, (ignored, failure) -> {}, () -> {});
        oldReference.set(old);
        IndexWindow window = new IndexWindow("idx", 1L, 1, old);
        IndexBatch queued =
                new IndexBatch(
                        TARGET_BUCKET,
                        new MemorySegmentBytesView(MemorySegment.wrap(new byte[] {1}), 0, 1),
                        window);
        sendBuffer.append(queued);
        assertThat(sendBuffer.pendingBytes(old)).isPositive();

        try {
            controller.installIndexReplicator(old);
            controller.onBecomeFollower();

            assertThat(old.isClosed()).isTrue();
            assertThat(sendBuffer.pendingBytes(old)).isZero();
            assertThat(controller.getIndexReplicator()).isNull();
            assertThat(controller.getState()).isEqualTo(IndexReplicationSupervisor.State.NOT_STARTED);
            verify(oldReadContext, times(1)).close();

            // The second stop must not unregister the probe that was installed while old was
            // closing.
            controller.onBecomeFollower();
            observeProbePoll.set(true);
            pool.signal(TABLE_BUCKET);
            assertThat(probePolled.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.unregister(TABLE_BUCKET);
            probe.close();
        }
    }

    private IndexReplicationSupervisor controller() {
        return controller(new IndexSendBuffer());
    }

    private IndexReplicationSupervisor controller(IndexSendBuffer sendBuffer) {
        return new IndexReplicationSupervisor(null, TABLE_BUCKET, null, pool, sendBuffer, null, null);
    }

    private static IndexReplicator idleReplicator(
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure) {
        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
        return idleReplicator(new IndexSendBuffer(), readContext, onTerminalFailure, () -> {});
    }

    private static IndexReplicator idleReplicator(
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure,
            Runnable onHighWatermark) {
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 0L, 0L, emptyRecords(), null, onHighWatermark),
                        null,
                        Runnable::run,
                        readContext);
        return IndexReplicator.forTesting(
                reader,
                Collections.singletonList(spec()),
                sendBuffer,
                readContext,
                0L,
                1024,
                1024,
                (sync, all) -> {},
                onTerminalFailure);
    }

    private static IndexReplicator corruptingReplicator(
            LogRecordReadContext readContext,
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure,
            AtomicBoolean exposeCorruption) {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(3L);
        when(batch.sizeInBytes()).thenReturn(3);
        when(batch.getRecordCount()).thenReturn(3);
        when(batch.records(readContext))
                .thenAnswer(
                        ignored ->
                                CloseableIterator.wrap(
                                        Arrays.asList(record(0L), record(2L)).iterator()));
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(0L, 0L, 3L, records(batch), exposeCorruption),
                        null,
                        Runnable::run,
                        readContext);
        return IndexReplicator.forTesting(
                reader,
                Collections.singletonList(spec()),
                new IndexSendBuffer(),
                readContext,
                0L,
                1024,
                1024,
                (sync, all) -> {},
                onTerminalFailure);
    }

    private static void awaitFailed(IndexReplicationSupervisor controller) {
        waitUntil(
                controller::isFailed,
                Duration.ofSeconds(10),
                "controller did not receive the terminal callback");
    }

    private static IndexSpec spec() {
        RowEncoder encoder =
                RowEncoder.create(
                        KvFormat.COMPACTED,
                        new org.apache.fluss.types.DataType[] {DataTypes.BIGINT()});
        return new IndexSpec(
                "idx",
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

    private static IndexSourceReader.SourceLog sourceLog(
            long logStartOffset, long localLogStartOffset, long highWatermark, LogRecords records) {
        return sourceLog(
                logStartOffset, localLogStartOffset, highWatermark, records, null, () -> {});
    }

    private static IndexSourceReader.SourceLog sourceLog(
            long logStartOffset,
            long localLogStartOffset,
            long highWatermark,
            LogRecords records,
            AtomicBoolean exposeCorruption) {
        return sourceLog(
                logStartOffset,
                localLogStartOffset,
                highWatermark,
                records,
                exposeCorruption,
                () -> {});
    }

    private static IndexSourceReader.SourceLog sourceLog(
            long logStartOffset,
            long localLogStartOffset,
            long highWatermark,
            LogRecords records,
            AtomicBoolean exposeCorruption,
            Runnable onHighWatermark) {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return SOURCE_BUCKET;
            }

            @Override
            public long highWatermark() {
                onHighWatermark.run();
                return exposeCorruption == null
                        ? highWatermark
                        : (exposeCorruption.get() ? highWatermark : 0L);
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
}
