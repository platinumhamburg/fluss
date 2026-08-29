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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor.TableDistribution;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.event.BulkLoadAsyncResultEvent;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable safety contracts shared by BulkLoad active-reference reads and retained-result GC. */
class BulkLoadCleanupComponentTest {

    private static final String SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testNonterminalTransactionProtectsActiveReferencesAndFailsClosedOnChange()
            throws Exception {
        long tableId = 9401L;
        try (ZooKeeperClient zkClient = client()) {
            BulkLoadTransaction transaction = begunTransaction(tableId, new long[] {41L});
            persistActiveTransaction(zkClient, transaction);
            BulkLoadActiveReferenceReader reader = new BulkLoadActiveReferenceReader(zkClient);
            Map<Integer, Set<Long>> snapshots =
                    reader.readSnapshotIds(
                            tableId,
                            null,
                            registrationPath(transaction),
                            () -> references(0, Collections.singleton(88L)));
            assertThat(snapshots.get(0)).containsExactlyInAnyOrder(41L, 88L);

            BulkLoadActiveReferenceReader changingReader =
                    new BulkLoadActiveReferenceReader(
                            zkClient,
                            () ->
                                    replaceTransaction(
                                            zkClient,
                                            transaction,
                                            begunTransaction(tableId, new long[] {42L})));
            assertThatThrownBy(
                            () ->
                                    changingReader.readSnapshotIds(
                                            tableId,
                                            null,
                                            registrationPath(transaction),
                                            Collections::emptyMap))
                    .isInstanceOf(TimeoutException.class);

            BulkLoadTransaction withoutSnapshotIds = begunTransaction(9403L, null);
            persistActiveTransaction(zkClient, withoutSnapshotIds);
            assertThat(
                            reader.readSnapshotIds(
                                    withoutSnapshotIds.getTableId(),
                                    null,
                                    registrationPath(withoutSnapshotIds),
                                    Collections::emptyMap))
                    .isEmpty();
            BulkLoadTransaction transientTransaction = begunTransaction(9404L, new long[] {43L});
            persistOrdinaryRegistration(zkClient, transientTransaction);
            BulkLoadActiveReferenceReader abaReader =
                    new BulkLoadActiveReferenceReader(
                            zkClient,
                            () -> {
                                try {
                                    persistActiveTransaction(zkClient, transientTransaction);
                                    releaseRegistration(zkClient, transientTransaction);
                                } catch (Exception failure) {
                                    throw new AssertionError(failure);
                                }
                            });
            assertThatThrownBy(
                            () ->
                                    abaReader.readSnapshotIds(
                                            transientTransaction.getTableId(),
                                            null,
                                            registrationPath(transientTransaction),
                                            Collections::emptyMap))
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void testResultGcRequiresOwnershipReleaseAndProtectsOuterManifest(@TempDir Path temp)
            throws Exception {
        long tableId = 9405L;
        Path outerManifest = Files.write(temp.resolve("bulkload-manifest.json"), new byte[] {1});
        BulkLoadTransaction transaction =
                abortedTransaction(tableId, outerManifest.toUri().toString());
        Path undeletableManifest = Files.createDirectory(temp.resolve("undeletable-manifest"));
        Files.write(undeletableManifest.resolve("child"), new byte[] {2});
        BulkLoadTransaction undeletableTransaction =
                abortedTransaction(9400L, undeletableManifest.toUri().toString());

        try (ZooKeeperClient zkClient = client()) {
            persistActiveTransaction(zkClient, undeletableTransaction);
            releaseRegistration(zkClient, undeletableTransaction);
            persistActiveTransaction(zkClient, transaction);
            BulkLoadResultGc resultGc = resultGc(zkClient);

            runMaintenance(resultGc, zkClient, 100L, 20);
            assertThat(zkClient.pathExists(transactionPath(undeletableTransaction))).isTrue();
            assertThat(zkClient.pathExists(transactionPath(transaction))).isTrue();
            assertThat(outerManifest).exists();

            releaseRegistration(zkClient, transaction);
            int staleEpochVersion = coordinatorEpochVersion(zkClient);
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(ZkData.CoordinatorEpochZNode.path(), new byte[] {1});
            BulkLoadResultGc staleEpochGc = resultGc(zkClient);
            staleEpochGc.runMaintenance(100L, staleEpochVersion);
            staleEpochGc.runMaintenance(100L, staleEpochVersion);
            assertThat(staleEpochGc.runMaintenance(100L, staleEpochVersion)).isZero();
            assertThat(zkClient.pathExists(transactionPath(transaction))).isTrue();
            assertThat(outerManifest).doesNotExist();

            resultGc = resultGc(zkClient);
            for (int pass = 0;
                    pass < 50 && zkClient.pathExists(transactionPath(transaction));
                    pass++) {
                assertThat(resultGc.runMaintenance(100L, coordinatorEpochVersion(zkClient)))
                        .isLessThanOrEqualTo(1);
            }

            assertThat(zkClient.pathExists(transactionPath(transaction))).isFalse();
            assertThat(outerManifest).doesNotExist();
            for (int pass = 0;
                    pass < 50
                            && zkClient.pathExists(
                                    ZkData.BulkLoadTableTransactionsZNode.path(tableId));
                    pass++) {
                resultGc.runMaintenance(100L, coordinatorEpochVersion(zkClient));
            }
            assertThat(zkClient.pathExists(ZkData.BulkLoadTableTransactionsZNode.path(tableId)))
                    .isFalse();
        }
    }

    @Test
    void testResultGcTreatsRecreatedTableAsReleasedOwnership() throws Exception {
        BulkLoadTransaction transaction = abortedTransaction(9406L, "file:///missing-manifest");
        try (ZooKeeperClient zkClient = client()) {
            persistActiveTransaction(zkClient, transaction);
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(
                            registrationPath(transaction),
                            ZkData.TableZNode.encode(registration(transaction.getTableId() + 1)));

            BulkLoadResultGc resultGc = resultGc(zkClient);
            runMaintenance(resultGc, zkClient, 100L, 20);

            assertThat(zkClient.pathExists(transactionPath(transaction))).isFalse();
        }
    }

    private static ZooKeeperClient client() {
        return ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    private static BulkLoadResultGc resultGc(ZooKeeperClient zkClient) {
        AtomicReference<BulkLoadResultGc> resultGcRef = new AtomicReference<>();
        BulkLoadResultGc resultGc =
                new BulkLoadResultGc(
                        zkClient,
                        event -> {
                            BulkLoadAsyncResultEvent resultEvent = (BulkLoadAsyncResultEvent) event;
                            assertThat(
                                            resultGcRef
                                                    .get()
                                                    .processAsyncResult(resultEvent.getResult()))
                                    .isTrue();
                        });
        resultGcRef.set(resultGc);
        return resultGc;
    }

    private static void persistActiveTransaction(
            ZooKeeperClient zkClient, BulkLoadTransaction transaction) throws Exception {
        if (!zkClient.pathExists(ZkData.CoordinatorEpochZNode.path())) {
            create(zkClient, ZkData.CoordinatorEpochZNode.path(), new byte[] {0});
        }
        create(
                zkClient,
                transactionPath(transaction),
                ZkData.BulkLoadTableTransactionZNode.encode(transaction));
        TableRegistration registration =
                registration(transaction)
                        .withDataState(
                                transaction.getState() == BulkLoadState.BEGUN
                                        ? BulkLoadDataState.LOADING
                                        : BulkLoadDataState.ACTIVE,
                                transaction.getBulkLoadId());
        if (zkClient.pathExists(registrationPath(transaction))) {
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(registrationPath(transaction), ZkData.TableZNode.encode(registration));
        } else {
            create(zkClient, registrationPath(transaction), ZkData.TableZNode.encode(registration));
        }
    }

    private static void persistOrdinaryRegistration(
            ZooKeeperClient zkClient, BulkLoadTransaction transaction) throws Exception {
        create(
                zkClient,
                registrationPath(transaction),
                ZkData.TableZNode.encode(registration(transaction)));
    }

    private static void releaseRegistration(
            ZooKeeperClient zkClient, BulkLoadTransaction transaction) throws Exception {
        zkClient.getCuratorClient()
                .setData()
                .forPath(
                        registrationPath(transaction),
                        ZkData.TableZNode.encode(registration(transaction)));
    }

    private static TableRegistration registration(BulkLoadTransaction transaction) {
        return registration(transaction.getTableId());
    }

    private static TableRegistration registration(long tableId) {
        return new TableRegistration(
                tableId,
                null,
                Collections.emptyList(),
                new TableDistribution(1, Collections.singletonList("id")),
                Collections.emptyMap(),
                Collections.emptyMap(),
                "file:///warehouse",
                1L,
                1L);
    }

    private static void replaceTransaction(
            ZooKeeperClient zkClient,
            BulkLoadTransaction transaction,
            BulkLoadTransaction replacement) {
        try {
            zkClient.getCuratorClient()
                    .setData()
                    .forPath(
                            transactionPath(transaction),
                            ZkData.BulkLoadTableTransactionZNode.encode(replacement));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static void runMaintenance(
            BulkLoadResultGc resultGc, ZooKeeperClient zkClient, long nowMs, int passes)
            throws Exception {
        for (int pass = 0; pass < passes; pass++) {
            resultGc.runMaintenance(nowMs, coordinatorEpochVersion(zkClient));
        }
    }

    private static int coordinatorEpochVersion(ZooKeeperClient zkClient) throws Exception {
        return zkClient.getDataWithStat(ZkData.CoordinatorEpochZNode.path()).getStat().getVersion();
    }

    private static void create(ZooKeeperClient zkClient, String path, byte[] data)
            throws Exception {
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }

    private static Map<Integer, Set<Long>> references(int bucketId, Set<Long> snapshotIds) {
        Map<Integer, Set<Long>> references = new HashMap<>();
        references.put(bucketId, new HashSet<>(snapshotIds));
        return references;
    }

    private static BulkLoadTransaction begunTransaction(long tableId, long[] snapshotIds) {
        return new BulkLoadTransaction(
                handle(tableId, "550e8400-e29b-41d4-a716-446655440041"),
                BulkLoadState.BEGUN,
                "alice",
                "USER",
                "file:///warehouse",
                3,
                ZkData.TableZNode.path(TablePath.of("db", "table_" + tableId)),
                0,
                snapshotIds,
                10L,
                20L,
                300L,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static BulkLoadTransaction abortedTransaction(long tableId, String manifestPath) {
        return new BulkLoadTransaction(
                handle(tableId, "550e8400-e29b-41d4-a716-446655440042"),
                BulkLoadState.ABORTED,
                "alice",
                "USER",
                "file:///warehouse",
                3,
                ZkData.TableZNode.path(TablePath.of("db", "table_" + tableId)),
                0,
                new long[] {7L},
                10L,
                20L,
                300L,
                400L,
                100L,
                manifestPath,
                1L,
                SHA256,
                BulkLoadAbortReason.ABORTED_BY_CALLER,
                null);
    }

    private static BulkLoadHandle handle(long tableId, String bulkLoadId) {
        return new BulkLoadHandle(
                PhysicalTablePath.of(TablePath.of("db", "table_" + tableId)),
                tableId,
                null,
                bulkLoadId);
    }

    private static String transactionPath(BulkLoadTransaction transaction) {
        return ZkData.BulkLoadTableTransactionZNode.path(
                transaction.getTableId(), transaction.getBulkLoadId());
    }

    private static String registrationPath(BulkLoadTransaction transaction) {
        return transaction.getMetadataPath();
    }
}
