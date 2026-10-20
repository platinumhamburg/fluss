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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Tests prefix lookup isolation and lifetime while KV writes and close run concurrently. */
class KvTabletPrefixLookupTest extends ReplicaTestBase {
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private KvTablet kvTablet;

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS))
                .as("Prefix lookup test executor must terminate before tablet cleanup")
                .isTrue();
    }

    @Test
    void testPrefixLookupAllowsWriteAndFlushBeforeSeek() throws Exception {
        createTablet();
        byte[] key = getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f0;
        kvTablet.putAsLeader(genKvRecordBatch(new Object[] {1, "old"}), null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        RocksDBKv reader = interceptReads();
        CountDownLatch scanReady = new CountDownLatch(1);
        CountDownLatch continueScan = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            RocksDBKv.PrefixLookup lookup =
                                    spy((RocksDBKv.PrefixLookup) invocation.callRealMethod());
                            doAnswer(
                                            read -> {
                                                scanReady.countDown();
                                                assertThat(continueScan.await(10, TimeUnit.SECONDS))
                                                        .isTrue();
                                                return read.callRealMethod();
                                            })
                                    .when(lookup)
                                    .readAll();
                            return lookup;
                        })
                .when(reader)
                .openPrefixLookup(any(byte[].class));
        Future<List<ByteArraySlice>> scan = executor.submit(() -> kvTablet.prefixLookup(key));
        try {
            assertThat(scanReady.await(10, TimeUnit.SECONDS)).isTrue();
            Future<?> write =
                    executor.submit(
                            () -> {
                                kvTablet.putAsLeader(
                                        genKvRecordBatch(new Object[] {1, "new"}), null);
                                flushAndWait(kvTablet, Long.MAX_VALUE);
                                return null;
                            });
            write.get(10, TimeUnit.SECONDS);
            assertThat(scan).isNotDone();
        } finally {
            continueScan.countDown();
        }
        List<ByteArraySlice> values = scan.get(10, TimeUnit.SECONDS);
        assertThat(values).hasSize(1);
        assertThat(values.get(0).toByteArray())
                .containsExactly(getKeyValuePairs(genKvRecords(new Object[] {1, "old"})).get(0).f1);
        assertThat(kvTablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
    }

    @Test
    void testPrefixLookupFailureReleasesReadLease() throws Exception {
        createTablet();
        RocksDBKv reader = interceptReads();
        doAnswer(
                        invocation -> {
                            RocksDBKv.PrefixLookup lookup =
                                    spy((RocksDBKv.PrefixLookup) invocation.callRealMethod());
                            doThrow(new IOException("read failed")).when(lookup).readAll();
                            return lookup;
                        })
                .when(reader)
                .openPrefixLookup(any(byte[].class));
        assertThatThrownBy(() -> kvTablet.prefixLookup(new byte[] {1}))
                .isInstanceOf(IOException.class)
                .hasMessage("read failed");
        assertThat(kvTablet.getRocksDBKv().getResourceGuard().getLeaseCount()).isZero();
        kvTablet.close();
    }

    @Test
    void testFailedCloseIsReportedToLaterCloseAndDrop() throws Exception {
        createTablet();
        RocksDBKv original = kvTablet.getRocksDBKv();
        RocksDBKv rocks = interceptReads();
        IOException failure = new IOException("close failed");
        doThrow(failure).when(rocks).close(KvCloseMode.PRESERVE_LOCAL_STATE);
        try {
            assertThatThrownBy(kvTablet::close).isSameAs(failure);
            assertThatThrownBy(kvTablet::close).isSameAs(failure);
            assertThatThrownBy(kvTablet::drop).isSameAs(failure);
            assertThat(kvTablet.getKvTabletDir()).exists();
        } finally {
            original.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentCloseOrDropWaitsForPrefixRead(boolean drop) throws Exception {
        createTablet();
        RocksDBKv rocks = kvTablet.getRocksDBKv();
        RocksDBKv.PrefixLookup read = rocks.openPrefixLookup(new byte[] {1});
        Future<?> firstClose =
                executor.submit(
                        () -> {
                            kvTablet.close();
                            return null;
                        });
        Future<?> secondClose = null;
        try {
            waitUntil(
                    rocks.getResourceGuard()::isClosed,
                    Duration.ofSeconds(10),
                    "Close must wait for the read lease.");
            AtomicReference<Thread> closingThread = new AtomicReference<>();
            secondClose =
                    executor.submit(
                            () -> {
                                closingThread.set(Thread.currentThread());
                                if (drop) {
                                    kvTablet.drop();
                                } else {
                                    kvTablet.close();
                                }
                                return null;
                            });
            Future<?> closing = secondClose;
            waitUntil(
                    () ->
                            closing.isDone()
                                    || (closingThread.get() != null
                                            && closingThread.get().getState()
                                                    == Thread.State.WAITING),
                    Duration.ofSeconds(10),
                    "The second closer must wait for physical close.");
            assertThat(secondClose).isNotDone();
            assertThat(kvTablet.getKvTabletDir()).exists();
            assertThatThrownBy(() -> kvTablet.prefixLookup(new byte[] {1}))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closing");
            assertThat(read.readAll()).isEmpty();
        } finally {
            read.close();
            firstClose.get(10, TimeUnit.SECONDS);
            if (secondClose != null) {
                secondClose.get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(rocks.getResourceGuard().getLeaseCount()).isZero();
        if (drop) {
            assertThat(kvTablet.getKvTabletDir()).doesNotExist();
        }
    }

    private void createTablet() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        Replica replica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, bucket);
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        DATA1_PHYSICAL_TABLE_PATH_PK,
                        bucket,
                        Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                0,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                0)));
        kvTablet = replica.getKvTablet();
    }

    private RocksDBKv interceptReads() throws Exception {
        RocksDBKv rocks = spy(kvTablet.getRocksDBKv());
        Field field = KvTablet.class.getDeclaredField("rocksDBKv");
        field.setAccessible(true);
        field.set(kvTablet, rocks);
        return rocks;
    }
}
