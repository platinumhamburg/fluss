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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class RequestDispatchTest {
    @Test
    void testReadyFifoUsesUncontendedBucketAndPreservesConnectionOrder() {
        verifyPartialOverlap(RequestSchedulingPolicy.READY_FIFO, true);
    }

    @Test
    void testConflictFifoProtectsEarlierMultiBucketWrite() {
        verifyPartialOverlap(RequestSchedulingPolicy.CONFLICT_FIFO, false);
    }

    @Test
    void testAgedReadyProtectsWriterOnceWaitingThresholdIsReached() {
        verifyPartialOverlap(RequestSchedulingPolicy.AGED_READY, false);
    }

    private void verifyPartialOverlap(RequestSchedulingPolicy policy, boolean bypass) {
        RequestDispatchCoordinator dispatch = new RequestDispatchCoordinator(policy, 0);
        RequestChannel[] workers = RequestChannel.forGroup(2, 20, 5, dispatch);
        FlussRequest read = lookup(1);
        RequestChannel.Connection reader = enqueue(workers[0], read);
        assertThat(workers[0].pollRequest(0)).isSameAs(read);
        PutKvRequest put = new PutKvRequest().setTableId(1);
        put.addBucketsReq().setBucketId(1);
        put.addBucketsReq().setBucketId(2);
        FlussRequest write = request(put);
        enqueue(workers[1], write);
        FlussRequest shortRead = lookup(2);
        RequestChannel.Connection shortConnection = enqueue(workers[1], shortRead);
        FlussRequest next = lookup(3);
        submit(workers[1], shortConnection, next);
        if (bypass) {
            assertThat(workers[1].pollRequest(0)).isSameAs(shortRead);
            workers[1].requestFinished();
            assertThat(workers[1].pollRequest(0)).isSameAs(next);
            workers[1].requestFinished();
        } else {
            assertThat(workers[1].pollRequest(0)).isNull();
        }
        workers[0].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(write);
        workers[1].requestFinished();
        if (!bypass) {
            assertThat(workers[1].pollRequest(0)).isSameAs(shortRead);
            workers[1].requestFinished();
            assertThat(workers[1].pollRequest(0)).isSameAs(next);
            workers[1].requestFinished();
        }
        workers[0].closeConnection(reader);
    }

    @Test
    void testCrossGroupConflictsAndReadViewRelease() {
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(RequestSchedulingPolicy.READY_FIFO, 0);
        RequestChannel first = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
        RequestChannel second = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
        PrefixLookupRequest prefix = new PrefixLookupRequest().setTableId(1);
        prefix.addBucketsReq().setBucketId(1).addKey(new byte[] {1});
        FlussRequest read = request(prefix);
        enqueue(first, read);
        assertThat(first.pollRequest(0)).isSameAs(read);
        PutKvRequest put = new PutKvRequest().setTableId(1);
        put.addBucketsReq().setBucketId(1);
        FlussRequest write = request(put);
        enqueue(second, write);
        assertThat(second.pollRequest(0)).isNull();
        RequestAccessContext.releaseRead(new TableBucket(1, 1));
        assertThat(second.pollRequest(0)).isSameAs(write);
        second.requestFinished();
        first.requestFinished();
    }

    @Test
    void testMultiplePrefixesRetainReservationUntilHandlerReturns() {
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(RequestSchedulingPolicy.READY_FIFO, 0);
        RequestChannel[] workers = RequestChannel.forGroup(2, 20, 5, dispatch);
        PrefixLookupRequest prefix = new PrefixLookupRequest().setTableId(1);
        org.apache.fluss.rpc.messages.PbPrefixLookupReqForBucket bucket =
                prefix.addBucketsReq().setBucketId(1);
        bucket.addKey(new byte[] {1});
        bucket.addKey(new byte[] {2});
        FlussRequest read = request(prefix);
        enqueue(workers[0], read);
        assertThat(workers[0].pollRequest(0)).isSameAs(read);
        PutKvRequest put = new PutKvRequest().setTableId(1);
        put.addBucketsReq().setBucketId(1);
        FlussRequest write = request(put);
        enqueue(workers[1], write);
        RequestAccessContext.releaseRead(new TableBucket(1, 1));
        assertThat(workers[1].pollRequest(0)).isNull();
        workers[0].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(write);
        workers[1].requestFinished();
    }

    @Test
    void testCompatibleReadersProceedAcrossResourceGroups() {
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(RequestSchedulingPolicy.CONFLICT_FIFO, 0);
        RequestChannel first = RequestChannel.forGroup(1, 1, 1, dispatch)[0];
        RequestChannel second = RequestChannel.forGroup(1, 1, 1, dispatch)[0];
        FlussRequest firstRead = lookup(1);
        FlussRequest secondRead = lookup(1);
        enqueue(first, firstRead);
        enqueue(second, secondRead);
        assertThat(first.pollRequest(0)).isSameAs(firstRead);
        assertThat(second.pollRequest(0)).isSameAs(secondRead);
        assertThat(first.groupRequestsCount()).isZero();
        assertThat(second.groupRequestsCount()).isZero();
        second.requestFinished();
        first.requestFinished();
    }

    @Test
    void testDisconnectRetainsRunningWriteUntilSynchronousCompletion() {
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(RequestSchedulingPolicy.READY_FIFO, 0);
        RequestChannel[] workers = RequestChannel.forGroup(2, 4, 2, dispatch);
        PutKvRequest put = new PutKvRequest().setTableId(1);
        put.addBucketsReq().setBucketId(1);
        FlussRequest write = request(put);
        RequestChannel.Connection connection = enqueue(workers[0], write);
        assertThat(workers[0].pollRequest(0)).isSameAs(write);
        workers[0].closeConnection(connection);
        FlussRequest read = lookup(1);
        enqueue(workers[1], read);
        assertThat(workers[1].pollRequest(0)).isNull();
        workers[0].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(read);
        workers[1].requestFinished();
    }

    @Test
    void testLimitedBypassCountsActualClaimsAcrossTheConnectionQueue() {
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(RequestSchedulingPolicy.LIMITED_BYPASS, 0);
        RequestChannel[] workers = RequestChannel.forGroup(2, 20, 5, dispatch);
        FlussRequest active = lookup(1);
        enqueue(workers[0], active);
        assertThat(workers[0].pollRequest(0)).isSameAs(active);
        PutKvRequest message = new PutKvRequest().setTableId(1);
        message.addBucketsReq().setBucketId(1);
        message.addBucketsReq().setBucketId(2);
        FlussRequest writer = request(message);
        enqueue(workers[1], writer);
        FlussRequest firstRead = lookup(2);
        RequestChannel.Connection reader = enqueue(workers[1], firstRead);
        FlussRequest secondRead = lookup(2);
        submit(workers[1], reader, secondRead);
        assertThat(workers[1].pollRequest(0)).isSameAs(firstRead);
        workers[1].requestFinished();
        assertThat(workers[1].pollRequest(0)).isNull();
        workers[0].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(writer);
        workers[1].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(secondRead);
        workers[1].requestFinished();
    }

    @Test
    void testVirtualTimeUsesMeasuredServiceAndKeepsHistoryOnTheConnection() {
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        RequestDispatchCoordinator dispatch =
                new RequestDispatchCoordinator(
                        RequestSchedulingPolicy.VIRTUAL_TIME, 10, 1, 10, clock::get);
        RequestChannel worker = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
        FlussRequest hotFirst = lookup(1);
        RequestChannel.Connection hot = enqueue(worker, hotFirst);
        FlussRequest hotSecond = lookup(1);
        submit(worker, hot, hotSecond);
        FlussRequest coldFirst = lookup(2);
        enqueue(worker, coldFirst);
        assertThat(worker.pollRequest(0)).isSameAs(hotFirst);
        clock.set(100);
        worker.requestFinished();
        assertThat(worker.pollRequest(0)).isSameAs(coldFirst);
        worker.requestFinished();
        assertThat(worker.pollRequest(0)).isSameAs(hotSecond);
        worker.requestFinished();
    }

    @Test
    void testServerConfigurationUsesBootstrapCostAndRankingPreset() {
        for (long bootstrap : new long[] {1, 10}) {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.NETTY_SERVER_REQUEST_SCHEDULING_POLICY, "shortest-job");
            conf.set(ConfigOptions.NETTY_SERVER_REQUEST_INITIAL_ESTIMATE_US, bootstrap);
            RequestChannel worker =
                    RequestChannel.forGroup(
                            1, 20, 5, RequestDispatchCoordinator.fromConfiguration(conf))[0];
            LookupRequest message = new LookupRequest().setTableId(1);
            message.addBucketsReq().setBucketId(1);
            FlussRequest hinted =
                    new FlussRequest(
                            (short) 0,
                            (short) 0,
                            0,
                            null,
                            message,
                            Unpooled.EMPTY_BUFFER,
                            "test",
                            false,
                            null,
                            null,
                            new CompletableFuture<>(),
                            new RequestSchedulingInfo(5000, Long.MAX_VALUE, 1));
            FlussRequest unhinted = lookup(2);
            enqueue(worker, hinted);
            enqueue(worker, unhinted);
            assertThat(worker.pollRequest(0)).isSameAs(bootstrap == 1 ? unhinted : hinted);
            worker.requestFinished();
            assertThat(worker.pollRequest(0)).isSameAs(bootstrap == 1 ? hinted : unhinted);
            worker.requestFinished();
        }
    }

    @Test
    void testServerConfigurationUsesZeroBypassLimit() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_SERVER_REQUEST_SCHEDULING_POLICY, "limited-bypass");
        conf.set(ConfigOptions.NETTY_SERVER_REQUEST_BYPASS_LIMIT, 0);
        RequestChannel[] workers =
                RequestChannel.forGroup(
                        2, 20, 5, RequestDispatchCoordinator.fromConfiguration(conf));
        FlussRequest active = lookup(1);
        enqueue(workers[0], active);
        assertThat(workers[0].pollRequest(0)).isSameAs(active);
        PutKvRequest message = new PutKvRequest().setTableId(1);
        message.addBucketsReq().setBucketId(1);
        message.addBucketsReq().setBucketId(2);
        FlussRequest writer = request(message);
        FlussRequest later = lookup(2);
        enqueue(workers[1], writer);
        enqueue(workers[1], later);
        assertThat(workers[1].pollRequest(0)).isNull();
        workers[0].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(writer);
        workers[1].requestFinished();
        assertThat(workers[1].pollRequest(0)).isSameAs(later);
        workers[1].requestFinished();
    }

    @Test
    void testVirtualTimeFloorIsIndependentForUnrelatedListenerGroups() {
        for (boolean externalWaiter : new boolean[] {false, true}) {
            java.util.concurrent.atomic.AtomicLong clock =
                    new java.util.concurrent.atomic.AtomicLong();
            RequestDispatchCoordinator dispatch =
                    new RequestDispatchCoordinator(
                            RequestSchedulingPolicy.VIRTUAL_TIME, 10, 1, 10, clock::get);
            RequestChannel internal = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
            RequestChannel external = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
            FlussRequest externalRead = lookup(9);
            if (externalWaiter) {
                enqueue(external, externalRead);
            }
            FlussRequest first = lookup(1);
            RequestChannel.Connection established = enqueue(internal, first);
            FlussRequest next = lookup(1);
            submit(internal, established, next);
            assertThat(internal.pollRequest(0)).isSameAs(first);
            clock.set(100);
            internal.requestFinished();
            FlussRequest newcomer = lookup(2);
            enqueue(internal, newcomer);
            assertThat(internal.pollRequest(0)).isSameAs(next);
            internal.requestFinished();
            assertThat(internal.pollRequest(0)).isSameAs(newcomer);
            internal.requestFinished();
            if (externalWaiter) {
                assertThat(external.pollRequest(0)).isSameAs(externalRead);
                external.requestFinished();
            }
        }
    }

    @Test
    void testVirtualTimeChargesSynchronousServiceByTrustedWeight() {
        for (int weight : new int[] {1, 2}) {
            java.util.concurrent.atomic.AtomicLong clock =
                    new java.util.concurrent.atomic.AtomicLong();
            RequestDispatchCoordinator dispatch =
                    new RequestDispatchCoordinator(
                            RequestSchedulingPolicy.VIRTUAL_TIME, 10, 1, 10, clock::get);
            RequestChannel worker = RequestChannel.forGroup(1, 20, 5, dispatch)[0];
            LookupRequest message = new LookupRequest().setTableId(1);
            message.addBucketsReq().setBucketId(1);
            FlussRequest first =
                    request(message, new RequestSchedulingInfo(10, Long.MAX_VALUE, weight));
            RequestChannel.Connection weighted = enqueue(worker, first);
            FlussRequest weightedNext = lookup(1);
            submit(worker, weighted, weightedNext);
            FlussRequest ordinary = lookup(2);
            RequestChannel.Connection unweighted = enqueue(worker, ordinary);
            FlussRequest ordinaryNext = lookup(2);
            submit(worker, unweighted, ordinaryNext);
            assertThat(worker.pollRequest(0)).isSameAs(first);
            clock.set(100);
            worker.requestFinished();
            assertThat(worker.pollRequest(0)).isSameAs(ordinary);
            clock.set(160);
            worker.requestFinished();
            assertThat(worker.pollRequest(0)).isSameAs(weight == 1 ? ordinaryNext : weightedNext);
            worker.requestFinished();
            assertThat(worker.pollRequest(0)).isSameAs(weight == 1 ? weightedNext : ordinaryNext);
            worker.requestFinished();
        }
    }

    private static FlussRequest lookup(int bucket) {
        LookupRequest message = new LookupRequest().setTableId(1);
        message.addBucketsReq().setBucketId(bucket);
        return request(message);
    }

    private static FlussRequest request(ApiMessage message) {
        return request(message, RequestSchedulingInfo.DEFAULT);
    }

    private static FlussRequest request(ApiMessage message, RequestSchedulingInfo info) {
        return new FlussRequest(
                (short) 0,
                (short) 0,
                0,
                null,
                message,
                Unpooled.EMPTY_BUFFER,
                "test",
                false,
                null,
                null,
                new CompletableFuture<>(),
                info);
    }

    private static RequestChannel.Connection enqueue(RequestChannel worker, FlussRequest request) {
        RequestChannel.Connection connection = worker.newConnection(() -> {});
        submit(worker, connection, request);
        return connection;
    }

    private static void submit(
            RequestChannel worker, RequestChannel.Connection connection, FlussRequest request) {
        assertThat(worker.acquireRead(connection)).isTrue();
        worker.putRequest(connection, request);
    }
}
