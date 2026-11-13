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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadIncompleteException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.SharedKvFileRegistry;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.RecoverySnapshot;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for atomic ownership, commit decisions and independent recovery metadata. */
class BulkLoadMetadataStoreTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient client;
    private BulkLoadMetadataStore store;
    private static final int EPOCH = -1;

    @BeforeEach
    void setUp() {
        client = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZK.getCustomExtension().cleanupRoot();
        store = new BulkLoadMetadataStore(client);
    }

    @Test
    void testOverlappingBeginIsAtomicAndTerminalOwnerCanBeReplaced() throws Exception {
        begin("a", 10L);
        assertThatThrownBy(() -> begin("b", 11L, 10L))
                .isInstanceOf(BulkLoadConflictException.class)
                .hasMessageContaining("owned by a");
        assertThat(store.transaction(1, "b")).isEmpty();
        assertThat(store.list(1, Collections.singletonList(11L))).isEmpty();
        store.abort(transaction("a"), 20, null, EPOCH);
        begin("b", 10L, 11L);
        assertThat(store.list(1, Arrays.asList(10L, 11L))).hasSize(1);
        assertThat(store.list(1, Arrays.asList(10L, 11L)))
                .extracting(t -> t.bulkLoadId)
                .containsExactly("b");
    }

    @Test
    void testAbortFencesAllocatedBucketCommitAndFixesExplanation() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        Versioned<BulkLoadTransaction> stale = transaction("a");
        Versioned<BulkLoadTarget> target = store.targets(stale.value).get(0);
        Versioned<BulkLoadBucket> allocation = store.bucket(stale.value, bucket).get();
        store.abort(stale, 20, "cancelled", EPOCH);
        assertThatThrownBy(
                        () ->
                                store.commitBucket(
                                        stale,
                                        target,
                                        bucket,
                                        allocation,
                                        new BucketSnapshot(7, 100, "file:/snapshot/_METADATA"),
                                        Collections.emptyList(),
                                        EPOCH))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(store.recoverySnapshot(bucket)).isEmpty();
        assertThat(client.getTableBucketSnapshot(bucket, 7)).isEmpty();
        assertThat(store.bucket(transaction("a").value, bucket).get().value.state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(transaction("a").value.abortMessage).isEqualTo("cancelled");
        assertThatThrownBy(() -> store.abort(transaction("a"), 30, "other", EPOCH))
                .isInstanceOf(BulkLoadStateException.class);
        assertThat(transaction("a").value.abortMessage).isEqualTo("cancelled");
    }

    @Test
    void testCommittedSnapshotSurvivesAbortAndTransactionDeletion() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        store.abort(transaction("a"), 20, null, EPOCH);
        client.getCuratorClient()
                .delete()
                .deletingChildrenIfNeeded()
                .forPath(BulkLoadMetadataStore.transactionPath(1, "a"));
        assertThat(client.getTableBucketSnapshot(bucket, 7)).isPresent();
        assertThat(store.recoverySnapshot(bucket).get().value.snapshotId).isEqualTo(7);
        assertThat(client.getTableBucketSnapshot(bucket, 7).get().getLogOffset()).isEqualTo(100);
    }

    @Test
    void testTransactionCompletionDoesNotChangeRecoverySelection() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        store.commit(transaction("a"), EPOCH, 20);
        assertThat(transaction("a").value.state).isEqualTo(BulkLoadTransaction.COMMITTED);
        assertThat(store.recoverySnapshot(bucket).get().version).isZero();
        begin("b", 10L);
        allocate("b", bucket, 8, 110);
        commitBucket("b", bucket);
        assertThat(store.recoverySnapshot(bucket).get().value.snapshotId).isEqualTo(8);
        assertThat(store.recoverySnapshot(bucket).get().version).isEqualTo(1);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.snapshotId)
                .isEqualTo(7);
    }

    @Test
    void testUnallocatedBucketsBlockCommitAndWithdrawalAllowsCompletion() throws Exception {
        begin("a", 10L);
        assertThatThrownBy(() -> store.commit(transaction("a"), EPOCH, 20))
                .isInstanceOf(BulkLoadIncompleteException.class)
                .hasMessageContaining("incomplete");
        Versioned<BulkLoadTransaction> transaction = transaction("a");
        Versioned<BulkLoadTarget> target = store.targets(transaction.value).get(0);
        store.withdraw(transaction, target, EPOCH);
        store.commit(transaction("a"), EPOCH, 20);
        assertThatThrownBy(
                        () ->
                                store.withdraw(
                                        transaction("a"),
                                        store.targets(transaction("a").value).get(0),
                                        EPOCH))
                .isInstanceOf(BulkLoadStateException.class);
    }

    @Test
    void testConcurrentAllocationKeepsFirstRecoveryOffset() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        assertThatThrownBy(
                        () ->
                                store.allocate(
                                        transaction("a"),
                                        store.targets(transaction("a").value).get(0),
                                        bucket,
                                        new BulkLoadBucket(8, 110, BulkLoadBucket.ALLOCATED),
                                        Collections.emptyList(),
                                        EPOCH))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.snapshotId)
                .isEqualTo(7);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.logOffset)
                .isEqualTo(100);
    }

    @Test
    void testRetentionPreservesRunningOwnerAndPhysicalRecoveryRecord() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        store.abort(transaction("a"), 20, null, EPOCH);
        client.registerTable(
                TablePath.of("db", "t"),
                TableRegistration.newTable(
                        1, "file:/remote", transaction("a").value.tableInfo.toTableDescriptor()));
        begin("b", 10L);
        store.maintain(119, 100, EPOCH);
        assertThat(store.transaction(1, "a")).isPresent();
        store.maintain(120, 100, EPOCH);
        assertThat(store.transaction(1, "a")).isEmpty();
        assertThat(store.list(1, Collections.singletonList(10L)))
                .extracting(t -> t.bulkLoadId)
                .containsExactly("b");
        assertThat(store.recoverySnapshot(bucket).get().value.snapshotId).isEqualTo(7);
        client.deleteTable(TablePath.of("db", "t"));
        store.maintain(121, 100, EPOCH);
        assertThat(
                        client.getCuratorClient()
                                .checkExists()
                                .forPath(BulkLoadMetadataStore.tablePath(1)))
                .isNull();
        assertThat(store.recoverySnapshot(bucket).get().value.snapshotId).isEqualTo(7);
    }

    @Test
    void testDeposedCoordinatorCannotWriteTerminalState() throws Exception {
        begin("a", 10L);
        int epoch = client.fenceBecomeCoordinatorLeader("first").getCoordinatorEpochZkVersion();
        client.fenceBecomeCoordinatorLeader("second");
        assertThatThrownBy(() -> store.abort(transaction("a"), 20, null, epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(transaction("a").value.state).isEqualTo(BulkLoadTransaction.RUNNING);
    }

    @Test
    void testSnapshotReferenceConflictCannotPartiallyCommitBucket() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        client.registerTableBucketSnapshot(bucket, new BucketSnapshot(7, 100, "file:/existing"));
        assertThatThrownBy(() -> commitBucket("a", bucket))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(store.recoverySnapshot(bucket)).isEmpty();
    }

    @Test
    void testVersionlessLateSnapshotCannotSelectAndMatchingContextCanAdvanceAtEqualOffset()
            throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        client.registerTableBucketSnapshot(bucket, new BucketSnapshot(8, 100, "file:/old"));
        commitBucket("a", bucket);
        client.registerTableBucketSnapshot(bucket, new BucketSnapshot(9, 101, "file:/late"));
        assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId()).isEqualTo(7);
        assertThat(client.isRecoverySnapshotInUse(bucket, 7)).isTrue();
        assertThatThrownBy(() -> client.deleteTableBucketSnapshot(bucket, 7))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class);
        client.registerTableBucketSnapshot(
                bucket, new BucketSnapshot(10, 100, "file:/replacement"), 7L);
        assertThatThrownBy(
                        () ->
                                client.registerTableBucketSnapshot(
                                        bucket, new BucketSnapshot(11, 102, "file:/stale"), 7L))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(client.getTableBucketSnapshot(bucket, 11)).isEmpty();
        assertThat(client.isRecoverySnapshotInUse(bucket, 7)).isFalse();
        client.deleteTableBucketSnapshot(bucket, 7);
        assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId()).isEqualTo(10);
        assertThat(store.recoverySnapshot(bucket).get().value.snapshotId).isEqualTo(10);
    }

    @Test
    void testFirstImportDoesNotSelectConcurrentVersionlessPublication() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        CountDownLatch validated = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ZooKeeperClient publishingClient =
                new ZooKeeperClient(borrowedCurator(), borrowedConfiguration()) {
                    @Override
                    public Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>>
                            getRecoverySnapshot(TableBucket observed) throws Exception {
                        Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>> result =
                                super.getRecoverySnapshot(observed);
                        validated.countDown();
                        resume.await();
                        return result;
                    }
                };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> publishing =
                    executor.submit(
                            () -> {
                                try {
                                    publishingClient.registerTableBucketSnapshot(
                                            bucket, new BucketSnapshot(8, 99, "file:/late"));
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            assertThat(validated.await(30, TimeUnit.SECONDS)).isTrue();
            commitBucket("a", bucket);
            resume.countDown();
            publishing.get();
            assertThat(client.getTableBucketSnapshot(bucket, 8)).isPresent();
            assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                    .isEqualTo(7);
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testUnknownRegistrationOutcomeRetainsMetadataForIdempotentRetry(@TempDir Path directory)
            throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        Path metadata = directory.resolve("_METADATA");
        byte[] bytes = new byte[] {1, 2, 3};
        Files.write(metadata, bytes);
        CompletedSnapshotHandle handle =
                new CompletedSnapshotHandle(8, new FsPath(metadata.toUri()), 101);
        ZooKeeperClient failingClient =
                new ZooKeeperClient(borrowedCurator(), borrowedConfiguration()) {
                    @Override
                    public void registerTableBucketSnapshot(
                            TableBucket physical, BucketSnapshot snapshot, Long expectedSnapshotId)
                            throws Exception {
                        super.registerTableBucketSnapshot(physical, snapshot, expectedSnapshotId);
                        throw new java.io.IOException("Registration response lost.");
                    }
                };
        assertThatThrownBy(
                        () ->
                                new org.apache.fluss.server.kv.snapshot
                                                .ZooKeeperCompletedSnapshotHandleStore(
                                                failingClient)
                                        .add(bucket, 8, handle))
                .isInstanceOf(java.io.IOException.class);
        assertThat(Files.readAllBytes(metadata)).isEqualTo(bytes);
        new ZooKeeperCompletedSnapshotHandleStore(client).add(bucket, 8, handle);
        assertThat(Files.readAllBytes(metadata)).isEqualTo(bytes);
        assertThat(client.getTableBucketSnapshot(bucket, 8)).isPresent();
    }

    private Configuration borrowedConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, client.getDefaultRemoteDataDir());
        return configuration;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOldCoordinatorRetentionCannotBorrowANewSelection(
            boolean initialized, @TempDir Path directory) throws Exception {
        TableBucket bucket = new TableBucket(1, 10L, 0);
        Path sst = directory.resolve("000001.sst");
        Files.write(sst, new byte[] {1, 2, 3});
        KvFileHandle file = new KvFileHandle(sst.toUri().toString(), 3);
        List<KvFileHandleAndLocalPath> shared =
                Collections.singletonList(KvFileHandleAndLocalPath.of(file, "000001.sst"));
        AtomicBoolean pause = new AtomicBoolean();
        CountDownLatch captured = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ZooKeeperCompletedSnapshotHandleStore handles =
                new ZooKeeperCompletedSnapshotHandleStore(client) {
                    @Override
                    public SnapshotRetention beginRetention(TableBucket physical) throws Exception {
                        SnapshotRetention retention = super.beginRetention(physical);
                        if (pause.compareAndSet(true, false)) {
                            captured.countDown();
                            resume.await();
                        }
                        return retention;
                    }
                };
        CompletedSnapshotStore oldStore =
                new CompletedSnapshotStore(
                        1,
                        new SharedKvFileRegistry(),
                        Collections.emptyList(),
                        handles,
                        org.apache.fluss.utils.concurrent.Executors.directExecutor(),
                        snapshot -> {
                            try {
                                return client.isRecoverySnapshotInUse(
                                        bucket, snapshot.getSnapshotID());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        if (initialized) {
            CompletedSnapshot initial = fileSnapshot(directory, bucket, 7, Collections.emptyList());
            writeSnapshotMetadata(initial);
            begin("initial", 10L);
            allocate("initial", bucket, 7, 100);
            commitBucket("initial", bucket, initial.getMetadataFilePath().toString());
            store.commit(transaction("initial"), EPOCH, 20);
            oldStore.addRegistered(initial);
        }
        begin("next", 10L);
        allocate("next", bucket, 17, 100);
        CompletedSnapshot selected = fileSnapshot(directory, bucket, 17, shared);
        writeSnapshotMetadata(selected);
        oldStore.add(fileSnapshot(directory, bucket, 18, shared));
        CompletedSnapshot ordinary = fileSnapshot(directory, bucket, 20, Collections.emptyList());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            pause.set(true);
            Future<?> retention =
                    worker.submit(
                            () -> {
                                try {
                                    oldStore.add(ordinary);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            assertThat(captured.await(10, TimeUnit.SECONDS)).isTrue();
            // A successor Coordinator has a different cache and cannot take the old store lock.
            commitBucket("next", bucket, selected.getMetadataFilePath().toString());
            resume.countDown();
            assertThatThrownBy(retention::get)
                    .rootCause()
                    .isInstanceOf(KeeperException.BadVersionException.class);
            assertThat(Files.exists(sst)).isTrue();
            assertThat(client.getTableBucketSnapshot(bucket, 18)).isPresent();
            assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                    .isEqualTo(17);
            oldStore.add(ordinary);
            assertThat(Files.exists(sst)).isTrue();
            long selectedSnapshotId = client.getRecoverySnapshot(bucket).get().data().snapshotId;
            oldStore.add(
                    fileSnapshot(directory, bucket, 21, Collections.emptyList()),
                    selectedSnapshotId);
            assertThat(Files.exists(sst)).isFalse();
        } finally {
            resume.countDown();
            worker.shutdownNow();
        }
    }

    private CompletedSnapshot fileSnapshot(
            Path directory, TableBucket bucket, long id, List<KvFileHandleAndLocalPath> shared) {
        return new CompletedSnapshot(
                bucket,
                id,
                new FsPath(directory.resolve("snapshot-" + id).toUri()),
                KvSnapshotHandle.create(shared, Collections.emptyList(), 3),
                100,
                null,
                null);
    }

    private void writeSnapshotMetadata(CompletedSnapshot snapshot) throws Exception {
        Path metadata = Paths.get(snapshot.getMetadataFilePath().toUri());
        Files.createDirectories(metadata.getParent());
        Files.write(metadata, CompletedSnapshotJsonSerde.toJson(snapshot));
    }

    @Test
    void testReclamationChecksTheObservedSelection() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        client.registerTableBucketSnapshot(bucket, new BucketSnapshot(8, 100, "file:/old"));
        CountDownLatch observed = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ZooKeeperClient deleting =
                new ZooKeeperClient(borrowedCurator(), borrowedConfiguration()) {
                    @Override
                    public Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>>
                            getRecoverySnapshot(TableBucket physical) throws Exception {
                        Optional<ZooKeeperClient.VersionedData<RecoverySnapshot>> value =
                                super.getRecoverySnapshot(physical);
                        observed.countDown();
                        resume.await();
                        return value;
                    }
                };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> deletion =
                    worker.submit(
                            () -> {
                                try {
                                    deleting.deleteTableBucketSnapshot(bucket, 8);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            assertThat(observed.await(10, TimeUnit.SECONDS)).isTrue();
            client.registerTableBucketSnapshot(
                    bucket, new BucketSnapshot(9, 100, "file:/current"), 7L);
            resume.countDown();
            assertThatThrownBy(deletion::get)
                    .rootCause()
                    .isInstanceOf(KeeperException.BadVersionException.class);
            assertThat(client.getTableBucketSnapshot(bucket, 8)).isPresent();
            assertThat(client.getTableBucketLatestSnapshot(bucket).get().getSnapshotId())
                    .isEqualTo(9);
            client.deleteTableBucketSnapshot(bucket, 8);
            assertThat(client.getTableBucketSnapshot(bucket, 8)).isEmpty();
        } finally {
            resume.countDown();
            worker.shutdownNow();
        }
    }

    private CuratorFrameworkWithUnhandledErrorListener borrowedCurator() {
        return new CuratorFrameworkWithUnhandledErrorListener(
                client.getCuratorClient(), (message, error) -> {});
    }

    private Versioned<BulkLoadTransaction> transaction(String id) throws Exception {
        return store.transaction(1, id).get();
    }

    private void begin(String id, Long... partitions) throws Exception {
        TableInfo table =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1,
                        1,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("p", DataTypes.STRING())
                                                .column("k", DataTypes.INT())
                                                .primaryKey("p", "k")
                                                .build())
                                .partitionedBy("p")
                                .distributedBy(1, "k")
                                .build(),
                        "file:/remote",
                        1,
                        1);
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(1, id, table, BulkLoadTransaction.RUNNING, 10, null, null);
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (long partition : partitions) {
            targets.add(new BulkLoadTarget(partition, "p" + partition, 1, "file:/remote", false));
        }
        store.begin(transaction, targets, Collections.emptyList(), EPOCH);
    }

    private void allocate(String id, TableBucket bucket, long sid, long offset) throws Exception {
        String path = ZkData.BucketIdZNode.path(bucket);
        if (client.getCuratorClient().checkExists().forPath(path) == null) {
            client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path);
        }
        store.allocate(
                transaction(id),
                store.targets(transaction(id).value).get(0),
                bucket,
                new BulkLoadBucket(sid, offset, BulkLoadBucket.ALLOCATED),
                Collections.emptyList(),
                EPOCH);
    }

    private void commitBucket(String id, TableBucket bucket) throws Exception {
        commitBucket(id, bucket, "file:/snapshot/_METADATA");
    }

    private void commitBucket(String id, TableBucket bucket, String metadataPath) throws Exception {
        Versioned<BulkLoadTransaction> transaction = transaction(id);
        Versioned<BulkLoadBucket> allocation = store.bucket(transaction.value, bucket).get();
        store.commitBucket(
                transaction,
                store.targets(transaction.value).get(0),
                bucket,
                allocation,
                new BucketSnapshot(
                        allocation.value.snapshotId, allocation.value.logOffset, metadataPath),
                Collections.emptyList(),
                EPOCH);
    }
}
