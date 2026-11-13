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

package org.apache.fluss.server.replica;

import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RecoverySnapshot;
import org.apache.fluss.server.zk.data.ZkData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that bucket recovery does not block unrelated replica role changes. */
class ReplicaRecoveryTest extends ReplicaTestBase {

    @Test
    void testFailedRestoreLeavesClientOperationsRetriable(@TempDir Path directory)
            throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, bucket.getBucket());
        Replica replica = replicaManager.getReplicaOrException(bucket);
        CompletedSnapshot missing =
                new CompletedSnapshot(
                        bucket,
                        7,
                        new FsPath(directory.resolve("snapshot").toUri()),
                        KvSnapshotHandle.create(
                                Collections.singletonList(
                                        KvFileHandleAndLocalPath.of(
                                                new KvFileHandle(
                                                        directory
                                                                .resolve("missing.sst")
                                                                .toUri()
                                                                .toString(),
                                                        1),
                                                "missing.sst")),
                                Collections.emptyList(),
                                1),
                        0,
                        null,
                        null);
        Path metadata = Paths.get(missing.getMetadataFilePath().toUri());
        Files.createDirectories(metadata.getParent());
        Files.write(metadata, CompletedSnapshotJsonSerde.toJson(missing));
        zkClient.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(7, 0, missing.getMetadataFilePath().toString()));
        zkClient.getCuratorClient()
                .create()
                .forPath(
                        ZkData.RecoverySnapshotZNode.path(bucket),
                        new RecoverySnapshot(7).toJson());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            assertThat(
                            replica.requestBulkLoadRestore(
                                            7L,
                                            replica.getLeaderEpoch(),
                                            replica.getCoordinatorEpoch(),
                                            worker)
                                    .get(5, TimeUnit.SECONDS))
                    .isNull();
            waitUntil(
                    () -> !replica.hasKvRecoveryInProgress(),
                    Duration.ofSeconds(10),
                    "failed recovery did not release its task");
            assertThat(replica.isLeader()).isTrue();
            assertThat(replica.getKvTablet()).isNull();
            assertThatThrownBy(() -> replica.lookups(Collections.singletonList(new byte[] {1})))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(
                            () ->
                                    replica.lookupsFromBufferOrKv(
                                            Collections.singletonList(new byte[] {1})))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> replica.prefixLookup(new byte[] {1}))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> replica.limitKvScan(1))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> replica.openScan(scannerManager, "recovering", 1, 0))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(
                            () ->
                                    replica.putRecordsToLeader(
                                            genKvRecordBatch(DATA1.toArray(new Object[0][])),
                                            null,
                                            MergeMode.DEFAULT,
                                            1))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    void testRecoveryValidationReservesBucketBeforeWorkerStarts() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, bucket.getBucket());
        Replica replica = replicaManager.getReplicaOrException(bucket);
        Queue<Runnable> pending = new ArrayDeque<>();
        CompletableFuture<Long> recovery =
                replica.requestBulkLoadRestore(
                        7L, replica.getLeaderEpoch(), replica.getCoordinatorEpoch(), pending::add);
        NotifyLeaderAndIsrData role =
                new NotifyLeaderAndIsrData(
                        replica.getPhysicalTablePath(),
                        bucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                replica.getLeaderEpoch(),
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                replica.getCoordinatorEpoch(),
                                replica.getBucketEpoch()),
                        replica.getRoutingBucketCount(),
                        replica.getBucketCountEpoch(),
                        null);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                replica.getCoordinatorEpoch(), Collections.singletonList(role), response::complete);
        assertThat(response.get(5, TimeUnit.SECONDS).get(0).getError().error())
                .isEqualTo(Errors.REQUEST_TIME_OUT);
        makeLogTableAsLeader(2);
        assertThat(recovery).isNotDone();
        pending.remove().run();
        assertThat(recovery.get(5, TimeUnit.SECONDS)).isNull();
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> retried =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                replica.getCoordinatorEpoch(), Collections.singletonList(role), retried::complete);
        assertThat(retried.get(5, TimeUnit.SECONDS).get(0).failed()).isFalse();
    }

    @Test
    void testRestoreNotificationDoesNotWaitForBusyBucket() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, bucket.getBucket());
        Replica replica = replicaManager.getReplicaOrException(bucket);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            workers.submit(
                    () -> {
                        replica.getLeaderIsrUpdateLock().writeLock().lock();
                        try {
                            locked.countDown();
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            replica.getLeaderIsrUpdateLock().writeLock().unlock();
                        }
                    });
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
            NotifyLeaderAndIsrData data =
                    new NotifyLeaderAndIsrData(
                            replica.getPhysicalTablePath(),
                            bucket,
                            Collections.singletonList(TABLET_SERVER_ID),
                            new LeaderAndIsr(
                                    TABLET_SERVER_ID,
                                    replica.getLeaderEpoch(),
                                    Collections.singletonList(TABLET_SERVER_ID),
                                    Collections.emptyList(),
                                    replica.getCoordinatorEpoch(),
                                    replica.getBucketEpoch()),
                            replica.getRoutingBucketCount(),
                            replica.getBucketCountEpoch(),
                            7L);
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                    new CompletableFuture<>();
            workers.submit(
                    () ->
                            replicaManager.becomeLeaderOrFollower(
                                    replica.getCoordinatorEpoch(),
                                    Collections.singletonList(data),
                                    response::complete));
            assertThat(response.get(5, TimeUnit.SECONDS).get(0).failed()).isFalse();
            assertThat(response.get().get(0).getRecoveredSnapshotId()).isNull();
            CompletableFuture<Void> other =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    makeLogTableAsLeader(2);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            workers);
            other.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void testKvLeaderActivationDoesNotHoldGlobalRoleLock() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, bucket.getBucket());
        Replica replica = replicaManager.getReplicaOrException(bucket);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> response =
                new CompletableFuture<>();
        int nextEpoch = replica.getLeaderEpoch() + 1;
        try {
            workers.submit(
                    () -> {
                        replica.getLeaderIsrUpdateLock().writeLock().lock();
                        try {
                            locked.countDown();
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            replica.getLeaderIsrUpdateLock().writeLock().unlock();
                        }
                    });
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
            NotifyLeaderAndIsrData data =
                    new NotifyLeaderAndIsrData(
                            replica.getPhysicalTablePath(),
                            bucket,
                            Collections.singletonList(TABLET_SERVER_ID),
                            new LeaderAndIsr(
                                    TABLET_SERVER_ID,
                                    nextEpoch,
                                    Collections.singletonList(TABLET_SERVER_ID),
                                    Collections.emptyList(),
                                    replica.getCoordinatorEpoch(),
                                    replica.getBucketEpoch() + 1),
                            replica.getRoutingBucketCount(),
                            replica.getBucketCountEpoch(),
                            null);
            workers.submit(
                            () ->
                                    replicaManager.becomeLeaderOrFollower(
                                            replica.getCoordinatorEpoch(),
                                            Collections.singletonList(data),
                                            response::complete))
                    .get(5, TimeUnit.SECONDS);
            assertThat(response).isNotDone();
            CompletableFuture.runAsync(
                            () -> {
                                try {
                                    makeLogTableAsLeader(2);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            workers)
                    .get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
        assertThat(response.get(10, TimeUnit.SECONDS).get(0).failed()).isFalse();
        assertThat(replica.getLeaderEpoch()).isEqualTo(nextEpoch);
    }
}
