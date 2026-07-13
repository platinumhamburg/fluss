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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogTestBase;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Production remote-fetch integration coverage for {@link IndexSourceReader}. */
class IndexSourceReaderRemoteLogFetcherTest extends RemoteLogTestBase {

    @TempDir private Path channelTempDir;

    @Test
    void testRemoteByteLimitIsNotPrematureEndOrLookaheadDownload() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        LogRecordBatch first = batch(0L, 10L, 10);
        LogRecordBatch second = batch(10L, 20L, 10);
        FileLogRecords records = mock(FileLogRecords.class);
        FileChannel channel =
                FileChannel.open(
                        Files.createTempFile(channelTempDir, "index-source-reader", ".log"),
                        StandardOpenOption.READ);
        when(records.channel()).thenReturn(channel);
        when(records.sizeInBytes()).thenReturn(20);
        when(records.batches()).thenReturn(Arrays.asList(first, second));
        doAnswer(
                        ignored -> {
                            channel.close();
                            return null;
                        })
                .when(records)
                .close();

        AtomicInteger downloads = new AtomicInteger();
        AtomicInteger opens = new AtomicInteger();
        RemoteLogFetcher fetcher =
                new RemoteLogFetcher(
                        remoteLogManager,
                        tableBucket,
                        logTablet.getLogDir(),
                        RemoteLogFetcher.ConsumerMode.INDEX_RETAINED) {
                    @Override
                    protected File downloadSegment(RemoteLogSegment segment) throws IOException {
                        downloads.incrementAndGet();
                        return super.downloadSegment(segment);
                    }

                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile) {
                        opens.incrementAndGet();
                        return records;
                    }
                };
        IndexSourceReader reader =
                new IndexSourceReader(
                        sourceLog(tableBucket),
                        () -> remoteFetcher(fetcher),
                        Executors.directExecutor(),
                        mock(LogRecordReadContext.class));

        try (IndexSourceReader.ReadResult result = reader.read(0L, 20L, 15).join()) {
            assertThat(result.batches())
                    .singleElement()
                    .satisfies(
                            batch -> {
                                assertThat(batch.baseLogOffset()).isZero();
                                assertThat(batch.nextLogOffset()).isEqualTo(10L);
                            });
            assertThat(result.nextOffset()).isEqualTo(10L);
        }
        assertThat(downloads).hasValue(1);
        assertThat(opens).hasValue(1);
        assertThat(channel.isOpen()).isTrue();

        reader.close();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void testHandoffResultCloseRetiresRemoteSessionBeforeLocalReads() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tableBucket, false);
        LogTablet logTablet = replicaManager.getReplicaOrException(tableBucket).getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        LogRecordBatch remoteBatch = batch(0L, 10L, 10);
        FileLogRecords remoteRecords = mock(FileLogRecords.class);
        FileChannel channel =
                FileChannel.open(
                        Files.createTempFile(channelTempDir, "index-source-handoff", ".log"),
                        StandardOpenOption.READ);
        when(remoteRecords.channel()).thenReturn(channel);
        when(remoteRecords.sizeInBytes()).thenReturn(10);
        when(remoteRecords.batches()).thenReturn(Collections.singletonList(remoteBatch));
        doAnswer(
                        ignored -> {
                            channel.close();
                            return null;
                        })
                .when(remoteRecords)
                .close();

        AtomicInteger downloads = new AtomicInteger();
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger sessionsOpened = new AtomicInteger();
        AtomicInteger sessionsClosed = new AtomicInteger();
        AtomicInteger localReads = new AtomicInteger();
        RemoteLogFetcher fetcher =
                new RemoteLogFetcher(
                        remoteLogManager,
                        tableBucket,
                        logTablet.getLogDir(),
                        RemoteLogFetcher.ConsumerMode.INDEX_RETAINED) {
                    @Override
                    protected File downloadSegment(RemoteLogSegment segment) throws IOException {
                        downloads.incrementAndGet();
                        return super.downloadSegment(segment);
                    }

                    @Override
                    protected FileLogRecords openDownloadedSegment(File localFile) {
                        opens.incrementAndGet();
                        return remoteRecords;
                    }
                };
        IndexSourceReader reader =
                new IndexSourceReader(
                        handoffSourceLog(tableBucket, localReads),
                        () -> {
                            sessionsOpened.incrementAndGet();
                            return remoteFetcher(fetcher, sessionsClosed::incrementAndGet);
                        },
                        Executors.directExecutor(),
                        mock(LogRecordReadContext.class));

        IndexSourceReader.ReadResult handoff = reader.read(0L, 20L, 15).join();
        assertThat(handoff.batches())
                .singleElement()
                .satisfies(
                        batch -> {
                            assertThat(batch.baseLogOffset()).isZero();
                            assertThat(batch.nextLogOffset()).isEqualTo(10L);
                        });
        assertThat(handoff.nextOffset()).isEqualTo(10L);
        assertThat(channel.isOpen()).isTrue();
        assertThat(Files.exists(logTablet.getLogDir().toPath().resolve("tmp"))).isTrue();

        handoff.close();

        assertThat(channel.isOpen()).isFalse();
        assertThat(Files.exists(logTablet.getLogDir().toPath().resolve("tmp"))).isFalse();
        assertThat(sessionsOpened).hasValue(1);
        assertThat(sessionsClosed).hasValue(1);

        try (IndexSourceReader.ReadResult firstLocal = reader.read(10L, 15L, 15).join();
                IndexSourceReader.ReadResult secondLocal = reader.read(15L, 20L, 15).join()) {
            assertThat(firstLocal.nextOffset()).isEqualTo(15L);
            assertThat(secondLocal.nextOffset()).isEqualTo(20L);
        }
        assertThat(localReads).hasValue(3);
        assertThat(downloads).hasValue(1);
        assertThat(opens).hasValue(1);
        assertThat(sessionsOpened).hasValue(1);
        assertThat(sessionsClosed).hasValue(1);

        reader.close();
    }

    private static IndexSourceReader.SourceLog sourceLog(TableBucket tableBucket) {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return tableBucket;
            }

            @Override
            public long highWatermark() {
                return 20L;
            }

            @Override
            public long logStartOffset() {
                return 0L;
            }

            @Override
            public long localLogStartOffset() {
                return 20L;
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                throw new AssertionError("byte-limited remote prefix must not read local WAL");
            }
        };
    }

    private static IndexSourceReader.SourceLog handoffSourceLog(
            TableBucket tableBucket, AtomicInteger localReads) {
        LogRecordBatch localBatch = batch(10L, 20L, 10);
        LogRecords localRecords =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return 10;
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Collections.singletonList(localBatch);
                    }
                };
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return tableBucket;
            }

            @Override
            public long highWatermark() {
                return 20L;
            }

            @Override
            public long logStartOffset() {
                return 0L;
            }

            @Override
            public long localLogStartOffset() {
                return 10L;
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                localReads.incrementAndGet();
                return new FetchDataInfo(localRecords);
            }
        };
    }

    private static IndexSourceReader.RemoteFetcher remoteFetcher(RemoteLogFetcher fetcher) {
        return remoteFetcher(fetcher, () -> {});
    }

    private static IndexSourceReader.RemoteFetcher remoteFetcher(
            RemoteLogFetcher fetcher, Runnable onClose) {
        return new IndexSourceReader.RemoteFetcher() {
            @Override
            public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
                    throws Exception {
                return fetcher.fetch(startOffset, localLogStartOffset);
            }

            @Override
            public IndexSourceReader.RemoteRead fetchBounded(
                    long startOffset, long localLogStartOffset, int maxBytes) throws Exception {
                RemoteLogFetcher.FetchResult result =
                        fetcher.fetch(startOffset, localLogStartOffset, maxBytes);
                return new IndexSourceReader.RemoteRead() {
                    @Override
                    public boolean stoppedByByteLimit() {
                        return result.stopReason() == RemoteLogFetcher.StopReason.BYTE_LIMIT;
                    }

                    @Override
                    public java.util.Iterator<LogRecordBatch> iterator() {
                        return result.iterator();
                    }

                    @Override
                    public void close() {
                        result.close();
                    }
                };
            }

            @Override
            public void close() {
                onClose.run();
                fetcher.close();
            }
        };
    }

    private static LogRecordBatch batch(long startOffset, long endOffset, int size) {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(startOffset);
        when(batch.nextLogOffset()).thenReturn(endOffset);
        when(batch.sizeInBytes()).thenReturn(size);
        return batch;
    }
}
