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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableDeletion;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetadataManagerLifecycleTest {

    @Test
    void testPartitionMutationsFromSameCoordinatorAreSerialized() throws Exception {
        ZooKeeperClient zkClient = mock(ZooKeeperClient.class);
        TablePath tablePath = TablePath.of("db", "partitioned_table");
        CountDownLatch firstReadEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstRead = new CountDownLatch(1);
        AtomicInteger partitionReads = new AtomicInteger();
        when(zkClient.getPartition(eq(tablePath), anyString()))
                .thenAnswer(
                        ignored -> {
                            if (partitionReads.incrementAndGet() == 1) {
                                firstReadEntered.countDown();
                                assertThat(releaseFirstRead.await(10, TimeUnit.SECONDS)).isTrue();
                            }
                            return Optional.empty();
                        });
        when(zkClient.getPartitionNumber(tablePath)).thenReturn(0);
        when(zkClient.getPartitionIdAndIncrement()).thenReturn(1L, 2L);
        when(zkClient.getPartitionTombstone(tablePath)).thenReturn(PartitionTombstone.EMPTY);
        when(zkClient.getDefaultRemoteDataDir()).thenReturn("/tmp");

        MetadataManager metadataManager = newMetadataManager(zkClient);
        PartitionAssignment assignment =
                new PartitionAssignment(11L, Collections.singletonMap(0, BucketAssignment.of(1)));
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        CountDownLatch firstDone = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);

        Thread first =
                new Thread(
                        () -> {
                            try {
                                metadataManager.createPartition(
                                        tablePath,
                                        11L,
                                        assignment,
                                        ResolvedPartitionSpec.fromPartitionValue("p", "1"),
                                        false,
                                        7);
                            } catch (Throwable t) {
                                firstFailure.set(t);
                            } finally {
                                firstDone.countDown();
                            }
                        });
        Thread second =
                new Thread(
                        () -> {
                            secondStarted.countDown();
                            try {
                                metadataManager.createPartition(
                                        tablePath,
                                        11L,
                                        assignment,
                                        ResolvedPartitionSpec.fromPartitionValue("p", "2"),
                                        false,
                                        7);
                            } catch (Throwable t) {
                                secondFailure.set(t);
                            } finally {
                                secondDone.countDown();
                            }
                        });

        first.start();
        assertThat(firstReadEntered.await(10, TimeUnit.SECONDS)).isTrue();
        second.start();
        assertThat(secondStarted.await(10, TimeUnit.SECONDS)).isTrue();
        retry(
                Duration.ofSeconds(10),
                () -> assertThat(second.getState()).isEqualTo(Thread.State.BLOCKED));
        assertThat(partitionReads).hasValue(1);

        releaseFirstRead.countDown();
        assertThat(firstDone.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(secondDone.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(firstFailure).hasValue(null);
        assertThat(secondFailure).hasValue(null);
        assertThat(partitionReads).hasValue(2);
    }

    @Test
    void testFailedMarkedTableDeletionRetriesOnlyRemainingTables() throws Exception {
        ZooKeeperClient zkClient = mock(ZooKeeperClient.class);
        Deque<Runnable> retries = new ArrayDeque<>();
        MetadataManager metadataManager = newMetadataManager(zkClient, retries);
        TableDeletion first = new TableDeletion(TablePath.of("db", "first"), 1L);
        TableDeletion second = new TableDeletion(TablePath.of("db", "second"), 2L);
        doNothing().when(zkClient).completeTableDeletion(first.getTablePath(), 9);
        doThrow(new KeeperException.ConnectionLossException())
                .doNothing()
                .when(zkClient)
                .completeTableDeletion(second.getTablePath(), 9);

        assertThatThrownBy(() -> metadataManager.deleteTables(Arrays.asList(first, second), 9))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("second");
        assertThat(retries).hasSize(1);

        retries.removeFirst().run();

        verify(zkClient).completeTableDeletion(first.getTablePath(), 9);
        verify(zkClient, org.mockito.Mockito.times(2))
                .completeTableDeletion(second.getTablePath(), 9);
        assertThat(retries).isEmpty();
    }

    @Test
    void testDeletionRetryStopsWhenCoordinatorEpochChanges() throws Exception {
        ZooKeeperClient zkClient = mock(ZooKeeperClient.class);
        Deque<Runnable> retries = new ArrayDeque<>();
        MetadataManager metadataManager = newMetadataManager(zkClient, retries);
        TableDeletion deletion = new TableDeletion(TablePath.of("db", "table"), 1L);
        doThrow(new KeeperException.ConnectionLossException())
                .doThrow(new KeeperException.BadVersionException())
                .when(zkClient)
                .completeTableDeletion(deletion.getTablePath(), 12);

        assertThatThrownBy(
                        () -> metadataManager.deleteTables(Collections.singletonList(deletion), 12))
                .isInstanceOf(FlussRuntimeException.class)
                .hasCauseInstanceOf(KeeperException.ConnectionLossException.class);
        assertThat(retries).hasSize(1);

        retries.removeFirst().run();

        assertThat(retries).isEmpty();
        verify(zkClient, org.mockito.Mockito.times(2))
                .completeTableDeletion(deletion.getTablePath(), 12);
    }

    @Test
    void testDeletionRetryStopsAfterInvalidMetadataState() throws Exception {
        ZooKeeperClient zkClient = mock(ZooKeeperClient.class);
        Deque<Runnable> retries = new ArrayDeque<>();
        MetadataManager metadataManager = newMetadataManager(zkClient, retries);
        TableDeletion deletion = new TableDeletion(TablePath.of("db", "table"), 1L);
        doThrow(new KeeperException.ConnectionLossException())
                .doThrow(new IllegalStateException("table identity mismatch"))
                .when(zkClient)
                .completeTableDeletion(deletion.getTablePath(), 12);

        assertThatThrownBy(
                        () -> metadataManager.deleteTables(Collections.singletonList(deletion), 12))
                .isInstanceOf(FlussRuntimeException.class)
                .hasCauseInstanceOf(KeeperException.ConnectionLossException.class);
        assertThat(retries).hasSize(1);

        retries.removeFirst().run();

        assertThat(retries).isEmpty();
        verify(zkClient, org.mockito.Mockito.times(2))
                .completeTableDeletion(deletion.getTablePath(), 12);
    }

    private static MetadataManager newMetadataManager(ZooKeeperClient zkClient) {
        return newMetadataManager(zkClient, new ArrayDeque<>());
    }

    private static MetadataManager newMetadataManager(
            ZooKeeperClient zkClient, Deque<Runnable> retries) {
        Configuration configuration = new Configuration();
        return new MetadataManager(
                zkClient,
                configuration,
                new LakeCatalogDynamicLoader(configuration, null, true),
                Runnable::run,
                retries::addLast);
    }
}
