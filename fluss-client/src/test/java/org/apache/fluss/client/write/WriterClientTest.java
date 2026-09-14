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

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.entity.ProduceLogDataForBucket;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.COORDINATOR;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toProduceLogDataForBuckets;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Routing and completion contracts of {@link WriterClient}. */
class WriterClientTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "pending");
    private static final PhysicalTablePath PARTITION = PhysicalTablePath.of(TABLE_PATH, "p");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("dt", DataTypes.STRING())
                    .build();
    private static final TableInfo TABLE =
            TableInfo.of(
                    TABLE_PATH,
                    1L,
                    1,
                    TableDescriptor.builder()
                            .schema(SCHEMA)
                            .partitionedBy("dt")
                            .distributedBy(1)
                            .build(),
                    "",
                    1L,
                    1L);
    private final CompletableFuture<Cluster> metadata = new CompletableFuture<>();
    private final List<Integer> received = new CopyOnWriteArrayList<>();
    private final List<Integer> counts = new CopyOnWriteArrayList<>();
    private final AtomicReference<Cluster> cache = new AtomicReference<>(cluster(false));
    private final ExecutorService callers = Executors.newCachedThreadPool();
    private WriterClient writer;
    private final CompletableFuture<Void> creation = new CompletableFuture<>();
    private boolean acknowledge = true;
    private final BlockingQueue<Runnable> acknowledgments = new LinkedBlockingQueue<>();

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close(Duration.ZERO);
        }
        callers.shutdownNow();
    }

    @Test
    void testUnresolvedEpochZeroRecordCompletesWithoutAnotherSendOrFlush() throws Exception {
        start(4096);
        CompletableFuture<Void> result = send(1);
        assertThat(result).isNotDone();
        assertThat(received).isEmpty();
        resolve();
        result.get(10, TimeUnit.SECONDS);
        assertThat(received).containsExactly(1);
        assertThat(counts).containsOnly(2);
    }

    @Test
    void testPendingWriteOwnsReusedRow() throws Exception {
        start(4096);
        IndexedRow row = indexedRow(SCHEMA.getRowType(), new Object[] {1, "p"});
        CompletableFuture<Void> result = new CompletableFuture<>();
        writer.send(WriteRecord.forIndexedAppend(TABLE, PARTITION, row, null), callback(result));
        IndexedRow replacement = indexedRow(SCHEMA.getRowType(), new Object[] {99, "p"});
        row.pointTo(
                replacement.getSegments(), replacement.getOffset(), replacement.getSizeInBytes());
        resolve();
        result.get(10, TimeUnit.SECONDS);
        assertThat(received).containsExactly(1);
    }

    @Test
    void testFullPendingBufferRecoversWithoutFlush() throws Exception {
        // One owned record fits, two do not.
        start(512);
        CompletableFuture<Void> first = send(1);
        CountDownLatch entered = new CountDownLatch(1);
        Future<CompletableFuture<Void>> second =
                callers.submit(
                        () -> {
                            entered.countDown();
                            return send(2);
                        });
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> second.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        resolve();
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
        assertThat(received).containsExactly(1, 2);
    }

    @Test
    void testFlushIncludesUnresolvedRecord() throws Exception {
        start(4096);
        CompletableFuture<Void> result = send(1);
        Future<?> flush = callers.submit(writer::flush);
        assertThatThrownBy(() -> flush.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        resolve();
        flush.get(10, TimeUnit.SECONDS);
        assertThat(result).isCompleted();
        assertThat(received).containsExactly(1);
    }

    @Test
    void testMetadataFailureCompletesPendingOnlyWriteAndUnblocksAdmission() throws Exception {
        start(512);
        CompletableFuture<Void> first = send(1);
        Future<CompletableFuture<Void>> second = callers.submit(() -> send(2));
        metadata.completeExceptionally(new FlussRuntimeException("metadata unavailable"));
        assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("metadata unavailable");
        assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("metadata unavailable");
        assertThat(received).isEmpty();
    }

    @Test
    void testCloseBoundsMetadataWaitAndCompletesPendingWrite() throws Exception {
        start(512);
        CompletableFuture<Void> first = send(1);
        long start = System.nanoTime();
        writer.close(Duration.ofMillis(50));
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        assertThat(first).isCompletedExceptionally();
        resolve();
        assertThat(received).isEmpty();
    }

    @Test
    void testFlushWaitsForLastTransferBlockedOnBatchMemory() throws Exception {
        acknowledge = false;
        start(4096);
        cache.set(cluster(true));
        CompletableFuture<Void> first = send(1);
        Runnable ackFirst = acknowledgments.poll(10, TimeUnit.SECONDS);
        assertThat(ackFirst).isNotNull();
        CompletableFuture<Void> second = send(2);
        Runnable ackSecond = acknowledgments.poll(10, TimeUnit.SECONDS);
        assertThat(ackSecond).isNotNull();
        // Both batch pages are in flight. The next accepted record must remain visible to flush
        // while its transfer waits for one of those pages.
        cache.set(cluster(false));
        CompletableFuture<Void> third = send(3);
        resolve();
        Future<?> flush = callers.submit(writer::flush);
        ackFirst.run();
        Runnable ackThird = acknowledgments.poll(10, TimeUnit.SECONDS);
        assertThat(ackThird).isNotNull();
        ackSecond.run();
        assertThatThrownBy(() -> flush.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        assertThat(third).isNotDone();
        ackThird.run();
        flush.get(10, TimeUnit.SECONDS);
        assertThat(first).isCompleted();
        assertThat(second).isCompleted();
        assertThat(third).isCompleted();
        assertThat(received).containsExactly(1, 2, 3);
    }

    @Test
    void testRecordLargerThanPendingBudgetWaitsForRoutingWithoutBeingRejected() throws Exception {
        start(256);
        Future<CompletableFuture<Void>> call = callers.submit(() -> send(1));
        assertThatThrownBy(() -> call.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        resolve();
        call.get(10, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
        assertThat(received).containsExactly(1);
    }

    @Test
    void testCloseSupportsUnboundedConnectionTimeout() throws Exception {
        start(4096);
        CompletableFuture<Void> result = send(1);
        resolve();
        writer.close(Duration.ofMillis(Long.MAX_VALUE));
        assertThat(result).isCompleted();
    }

    @Test
    void testCreationFailureFailsPendingOnlyWriter() throws Exception {
        start(4096);
        CompletableFuture<Void> result = send(1);
        metadata.completeExceptionally(new PartitionNotExistException("missing"));
        creation.completeExceptionally(new IllegalStateException("creation denied"));
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("creation denied");
        assertThatThrownBy(() -> send(2)).hasRootCauseMessage("creation denied");
        assertThat(received).isEmpty();
    }

    @Test
    void testCloseCompletesTransferWaitingForBatchMemory() throws Exception {
        acknowledge = false;
        start(4096);
        cache.set(cluster(true));
        CompletableFuture<Void> first = send(1);
        assertThat(acknowledgments.poll(10, TimeUnit.SECONDS)).isNotNull();
        CompletableFuture<Void> second = send(2);
        assertThat(acknowledgments.poll(10, TimeUnit.SECONDS)).isNotNull();
        cache.set(cluster(false));
        CompletableFuture<Void> third = send(3);
        resolve();
        writer.close(Duration.ZERO);
        assertThat(first).isCompletedExceptionally();
        assertThat(second).isCompletedExceptionally();
        assertThat(third).isCompletedExceptionally();
        assertThat(received).containsExactly(1, 2);
    }

    private void start(long capacity) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(1024));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(512));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(512));
        conf.set(ConfigOptions.CLIENT_WRITER_PER_REQUEST_MEMORY_SIZE, new MemorySize(512));
        conf.set(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
        conf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, true);
        conf.set(ConfigOptions.CLIENT_WRITER_PENDING_BUFFER_MEMORY_SIZE, new MemorySize(capacity));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_BATCH_SIZE_ENABLED, false);
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_WAIT_TIMEOUT, Duration.ofSeconds(5));
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getCluster()).thenAnswer(ignored -> cache.get());
        when(updater.getCoordinatorServer()).thenReturn(COORDINATOR);
        when(updater.getPartitionId(PARTITION))
                .thenAnswer(ignored -> cache.get().getPartitionId(PARTITION));
        when(updater.ensurePartitionMetadataAsync(PARTITION)).thenReturn(metadata);
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<ProduceLogResponse> produceLog(
                            ProduceLogRequest request) {
                        List<ProduceLogResultForBucket> results = new ArrayList<>();
                        for (ProduceLogDataForBucket data : toProduceLogDataForBuckets(request)) {
                            try (LogRecordReadContext context =
                                    LogRecordReadContext.createIndexedReadContext(
                                            SCHEMA.getRowType(),
                                            1,
                                            new TestingSchemaGetter(new SchemaInfo(SCHEMA, 1)))) {
                                for (LogRecordBatch batch : data.records().batches()) {
                                    try (CloseableIterator<LogRecord> rows =
                                            batch.records(context)) {
                                        while (rows.hasNext()) {
                                            received.add(rows.next().getRow().getInt(0));
                                        }
                                    }
                                }
                            } catch (Exception error) {
                                throw new RuntimeException(error);
                            }
                            results.add(
                                    new ProduceLogResultForBucket(
                                            data.tableBucket(), 0, received.size()));
                        }
                        request.getBucketsReqsList()
                                .forEach(bucket -> counts.add(bucket.getRoutingBucketCount()));
                        ProduceLogResponse response = makeProduceLogResponse(results);
                        if (acknowledge) {
                            return CompletableFuture.completedFuture(response);
                        }
                        CompletableFuture<ProduceLogResponse> result = new CompletableFuture<>();
                        acknowledgments.add(() -> result.complete(response));
                        return result;
                    }
                };
        when(updater.newTabletServerClientForNode(anyInt())).thenReturn(gateway);
        Admin admin = mock(Admin.class);
        when(admin.createPartition(any(), any(), eq(true))).thenReturn(creation);
        writer = new WriterClient(conf, updater, TestingWriterMetricGroup.newInstance(), admin);
    }

    private CompletableFuture<Void> send(int value) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        writer.send(
                WriteRecord.forIndexedAppend(
                        TABLE,
                        PARTITION,
                        indexedRow(SCHEMA.getRowType(), new Object[] {value, "p"}),
                        null),
                callback(result));
        return result;
    }

    private static WriteCallback callback(CompletableFuture<Void> future) {
        return (bucket, offset, error) -> {
            if (error == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(error);
            }
        };
    }

    private void resolve() {
        Cluster resolved = cluster(true);
        cache.set(resolved);
        metadata.complete(resolved);
    }

    private static Cluster cluster(boolean resolved) {
        List<BucketLocation> buckets = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            buckets.add(
                    new BucketLocation(
                            PARTITION,
                            new TableBucket(1L, 100L, i),
                            NODE1.id(),
                            new int[] {NODE1.id()}));
        }
        return new Cluster(
                Collections.singletonMap(NODE1.id(), NODE1),
                COORDINATOR,
                resolved ? Collections.singletonMap(PARTITION, buckets) : Collections.emptyMap(),
                Collections.singletonMap(TABLE_PATH, 1L),
                resolved ? Collections.singletonMap(PARTITION, 100L) : Collections.emptyMap(),
                resolved
                        ? Collections.singletonMap(TableOrPartition.ofPartition(100L), 2)
                        : Collections.emptyMap());
    }
}
