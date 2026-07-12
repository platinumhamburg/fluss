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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.snapshot.PeriodicSnapshotManager;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicaLeaderTransitionTest extends ReplicaTestBase {

    private static final String DISABLED_SNAPSHOT_INTERVAL_TAG = "disabled-snapshot-interval";

    @Override
    protected Configuration getServerConf() {
        Configuration configuration = super.getServerConf();
        configuration.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofHours(1));
        return configuration;
    }

    @Override
    protected Duration getKvSnapshotInterval(TestInfo testInfo) {
        return testInfo.getTags().contains(DISABLED_SNAPSHOT_INTERVAL_TAG)
                ? Duration.ZERO
                : super.getKvSnapshotInterval(testInfo);
    }

    @Tag(DISABLED_SNAPSHOT_INTERVAL_TAG)
    @ParameterizedTest
    @EnumSource(KvIdempotenceProtocol.class)
    void testDisabledSnapshotIntervalPublishesReadyKvLeaderWithoutRetry(
            KvIdempotenceProtocol protocol) throws Exception {
        long tableId = 150106L + protocol.version();
        TablePath tablePath =
                TablePath.of(
                        "test_db_1", "disabled_snapshot_interval_" + protocol.version());
        TableBucket tableBucket = new TableBucket(tableId, 0);
        registerTableInZkClient(
                tablePath,
                DATA1_SCHEMA_PK,
                tableId,
                Collections.singletonList("a"),
                Collections.singletonMap(
                        ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key(),
                        String.valueOf(protocol.version())));

        assertThat(notifyLeader(tablePath, tableBucket, 2, 0).get())
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(tableBucket));
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getKvSnapshotManager()).isNull();

        AtomicInteger initializationAttempts = new AtomicInteger();
        List<PeriodicSnapshotManager> attemptedManagers = new ArrayList<>();
        replica.setKvSnapshotInitializationFaultInjector(
                manager -> {
                    initializationAttempts.incrementAndGet();
                    attemptedManagers.add(manager);
                });

        assertThat(notifyLeader(tablePath, tableBucket, TABLET_SERVER_ID, 1).get())
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(tableBucket));

        assertThat(initializationAttempts).hasValue(1);
        assertThat(attemptedManagers).containsExactly(replica.getKvSnapshotManager());
        assertThat(replica.isLeader()).isTrue();
        assertThat(replica.getLeaderId()).isEqualTo(TABLET_SERVER_ID);
        assertThat(replica.getLeaderEpoch()).isEqualTo(1);
        assertThat(replica.getKvTablet()).isNotNull();
        assertThat(kvManager.getKv(tableBucket)).contains(replica.getKvTablet());
        assertThat(replica.hasReadyKvSnapshotManager()).isTrue();
        assertThat(replica.getKvSnapshotManager().isStarted()).isTrue();
        assertThat(replica.getKvSnapshotManager().hasScheduledSnapshot()).isFalse();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).hasSize(1);
    }

    @Test
    void testSameNodeHigherEpochRecoveryFailureRevokesLeaderAndTiering() throws Exception {
        ReplayRecoveryFixture fixture =
                prepareReplayRecoveryFixture(150102L, "same_node_failed_replay");
        Replica replica = fixture.replica;
        int priorLeaderEpoch = replica.getLeaderEpoch();
        assertThat(replica.isLeader()).isTrue();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).hasSize(1);

        AtomicInteger replayAttempts = new AtomicInteger();
        replica.setKvRecoveryFaultInjector(
                nextOffset -> {
                    replayAttempts.incrementAndGet();
                    throw new IOException("injected replay failure at " + nextOffset);
                });

        List<NotifyLeaderAndIsrResultForBucket> failedReassignment =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                priorLeaderEpoch + 1)
                        .get();

        assertThat(failedReassignment).hasSize(1);
        assertThat(failedReassignment.get(0).getErrorMessage())
                .contains("Fail to init kv tablet");
        assertThat(replayAttempts).hasValue(5);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getLeaderId()).isNull();
        assertThat(replica.getLeaderEpoch()).isEqualTo(priorLeaderEpoch);
        assertThat(replica.getKvTablet()).isNull();
        assertThat(kvManager.getKv(fixture.tableBucket)).isEmpty();
        assertThat(replica.getKvSnapshotManager()).isNull();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).isEmpty();

        List<NotifyLeaderAndIsrResultForBucket> incompleteEqualEpoch =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                priorLeaderEpoch)
                        .get();
        assertThat(incompleteEqualEpoch.get(0).getErrorMessage())
                .contains("local leader role is not published");
        assertThat(replayAttempts).hasValue(5);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getKvTablet()).isNull();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).isEmpty();
    }

    @Test
    void testSameFailedEpochRetryRerunsRecoveryAndPublishesLeader() throws Exception {
        ReplayRecoveryFixture fixture =
                prepareReplayRecoveryFixture(150103L, "same_epoch_recovery_retry");
        Replica replica = fixture.replica;
        int priorLeaderEpoch = replica.getLeaderEpoch();
        int requestedLeaderEpoch = priorLeaderEpoch + 1;
        AtomicBoolean failReplay = new AtomicBoolean(true);
        AtomicInteger replayAttempts = new AtomicInteger();
        replica.setKvRecoveryFaultInjector(
                nextOffset -> {
                    replayAttempts.incrementAndGet();
                    if (failReplay.get()) {
                        throw new IOException("injected replay failure at " + nextOffset);
                    }
                });

        List<NotifyLeaderAndIsrResultForBucket> failedReassignment =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                requestedLeaderEpoch)
                        .get();
        assertThat(failedReassignment.get(0).getErrorMessage())
                .contains("Fail to init kv tablet");
        assertThat(replayAttempts).hasValue(5);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getLeaderEpoch()).isEqualTo(priorLeaderEpoch);

        failReplay.set(false);
        List<NotifyLeaderAndIsrResultForBucket> successfulRetry =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                requestedLeaderEpoch)
                        .get();

        assertThat(successfulRetry)
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(fixture.tableBucket));
        assertThat(replayAttempts).hasValue(7);
        assertThat(replica.isLeader()).isTrue();
        assertThat(replica.getLeaderId()).isEqualTo(TABLET_SERVER_ID);
        assertThat(replica.getLeaderEpoch()).isEqualTo(requestedLeaderEpoch);
        assertThat(replica.getKvTablet()).isNotNull();
        assertThat(kvManager.getKv(fixture.tableBucket)).contains(replica.getKvTablet());
        assertThat(replica.getKvSnapshotManager()).isNotNull();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).hasSize(1);
        CompletableFuture<byte[]> recoveredValue = new CompletableFuture<>();
        replicaManager.lookup(fixture.tableBucket, new byte[] {2}, recoveredValue::complete);
        assertThat(recoveredValue.get()).isNotNull();
    }

    @Test
    void testSnapshotInitializationFailureCleansAttemptAndRetryPublishesOnce()
            throws Exception {
        ReplayRecoveryFixture fixture =
                prepareReplayRecoveryFixture(150104L, "snapshot_initialization_retry");
        Replica replica = fixture.replica;
        int priorLeaderEpoch = replica.getLeaderEpoch();
        int requestedLeaderEpoch = priorLeaderEpoch + 1;
        AtomicBoolean failInitialization = new AtomicBoolean(true);
        AtomicInteger replayedBatches = new AtomicInteger();
        List<PeriodicSnapshotManager> attemptedManagers = new ArrayList<>();
        replica.setKvRecoveryFaultInjector(ignored -> replayedBatches.incrementAndGet());
        replica.setKvSnapshotInitializationFaultInjector(
                manager -> {
                    attemptedManagers.add(manager);
                    if (failInitialization.get()) {
                        throw new IOException("injected snapshot initialization failure");
                    }
                });

        List<NotifyLeaderAndIsrResultForBucket> failedReassignment =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                requestedLeaderEpoch)
                        .get();

        assertThat(failedReassignment.get(0).getErrorMessage())
                .contains("Failed to initialize periodic KV snapshots");
        assertThat(attemptedManagers).hasSize(5).allMatch(manager -> !manager.isStarted());
        assertThat(replayedBatches).hasValue(10);
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getLeaderId()).isNull();
        assertThat(replica.getLeaderEpoch()).isEqualTo(priorLeaderEpoch);
        assertThat(replica.getKvTablet()).isNull();
        assertThat(kvManager.getKv(fixture.tableBucket)).isEmpty();
        assertThat(replica.getKvSnapshotManager()).isNull();
        assertThat(replica.hasReadyKvSnapshotManager()).isFalse();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).isEmpty();

        List<NotifyLeaderAndIsrResultForBucket> incompleteEqualEpoch =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                priorLeaderEpoch)
                        .get();
        assertThat(incompleteEqualEpoch.get(0).getErrorMessage())
                .contains("local leader role is not published");
        assertThat(attemptedManagers).hasSize(5);
        assertThat(replayedBatches).hasValue(10);
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).isEmpty();

        failInitialization.set(false);
        List<NotifyLeaderAndIsrResultForBucket> successfulRetry =
                notifyLeader(
                                fixture.tablePath,
                                fixture.tableBucket,
                                TABLET_SERVER_ID,
                                requestedLeaderEpoch)
                        .get();

        assertThat(successfulRetry)
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(fixture.tableBucket));
        assertThat(attemptedManagers).hasSize(6);
        assertThat(attemptedManagers.subList(0, 5))
                .allMatch(manager -> !manager.isStarted());
        assertThat(attemptedManagers.get(5)).isSameAs(replica.getKvSnapshotManager());
        assertThat(attemptedManagers.get(5).isStarted()).isTrue();
        assertThat(replayedBatches).hasValue(12);
        assertThat(replica.isLeader()).isTrue();
        assertThat(replica.getLeaderEpoch()).isEqualTo(requestedLeaderEpoch);
        assertThat(replica.hasReadyKvSnapshotManager()).isTrue();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).hasSize(1);
        CompletableFuture<byte[]> recoveredValue = new CompletableFuture<>();
        replicaManager.lookup(fixture.tableBucket, new byte[] {2}, recoveredValue::complete);
        assertThat(recoveredValue.get()).isNotNull();
    }

    @Test
    void testSnapshotInitializationErrorCleansAttemptAndPreservesIdentity() throws Exception {
        ReplayRecoveryFixture fixture =
                prepareReplayRecoveryFixture(150105L, "snapshot_initialization_error");
        Replica replica = fixture.replica;
        int priorLeaderEpoch = replica.getLeaderEpoch();
        AssertionError injected = new AssertionError("injected snapshot initialization error");
        List<PeriodicSnapshotManager> attemptedManagers = new ArrayList<>();
        replica.setKvSnapshotInitializationFaultInjector(
                manager -> {
                    attemptedManagers.add(manager);
                    throw injected;
                });

        assertThatThrownBy(
                        () ->
                                notifyLeader(
                                        fixture.tablePath,
                                        fixture.tableBucket,
                                        TABLET_SERVER_ID,
                                        priorLeaderEpoch + 1))
                .isSameAs(injected);

        assertThat(attemptedManagers).hasSize(1).allMatch(manager -> !manager.isStarted());
        assertThat(replica.isLeader()).isFalse();
        assertThat(replica.getLeaderId()).isNull();
        assertThat(replica.getLeaderEpoch()).isEqualTo(priorLeaderEpoch);
        assertThat(replica.getKvTablet()).isNull();
        assertThat(kvManager.getKv(fixture.tableBucket)).isEmpty();
        assertThat(replica.getKvSnapshotManager()).isNull();
        assertThat(replica.hasReadyKvSnapshotManager()).isFalse();
        assertThat(remoteLogTaskScheduler.getActivePeriodicScheduledTask()).isEmpty();
    }

    private CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> notifyLeader(
            TablePath tablePath, TableBucket tableBucket, int leaderId, int leaderEpoch) {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> result =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(tablePath),
                                tableBucket,
                                Arrays.asList(TABLET_SERVER_ID, 2),
                                new LeaderAndIsr(
                                        leaderId,
                                        leaderEpoch,
                                        Collections.singletonList(leaderId),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH + leaderEpoch))),
                result::complete);
        return result;
    }

    private static KvRecordBatch fencedBatch(
            WriterKey writerKey, long sequence, int key, String value) throws Exception {
        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        DEFAULT_SCHEMA_ID,
                        1024,
                        new UnmanagedPagedOutputView(128),
                        KvFormat.COMPACTED)) {
            builder.append(
                    new byte[] {(byte) key},
                    compactedRow(DATA1_SCHEMA_PK.getRowType(), new Object[] {key, value}));
            builder.setWriterState(writerKey, sequence);
            return KvRecordBatchReader.pointToByteBuffer(
                    builder.build().getByteBuf().nioBuffer());
        }
    }

    private ReplayRecoveryFixture prepareReplayRecoveryFixture(long tableId, String tableName)
            throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", tableName);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        registerTableInZkClient(
                tablePath,
                DATA1_SCHEMA_PK,
                tableId,
                Collections.singletonList("a"),
                Collections.singletonMap(
                        ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key(),
                        String.valueOf(KvIdempotenceProtocol.V1_FENCED.version())));
        assertThat(notifyLeader(tablePath, tableBucket, TABLET_SERVER_ID, 0).get())
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(tableBucket));
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        WriterKey writerKey = new WriterKey(41L, 17L);
        replica.putRecordsToLeader(
                fencedBatch(writerKey, 100L, 1, "before-snapshot"),
                null,
                MergeMode.OVERWRITE,
                -1);
        replica.getKvSnapshotManager().triggerSnapshot();
        snapshotReporter.waitUntilSnapshotComplete(tableBucket, 0);
        replica.putRecordsToLeader(
                fencedBatch(writerKey, 500L, 2, "after-snapshot"),
                null,
                MergeMode.OVERWRITE,
                -1);
        return new ReplayRecoveryFixture(tablePath, tableBucket, replica);
    }

    private static class ReplayRecoveryFixture {
        private final TablePath tablePath;
        private final TableBucket tableBucket;
        private final Replica replica;

        private ReplayRecoveryFixture(
                TablePath tablePath, TableBucket tableBucket, Replica replica) {
            this.tablePath = tablePath;
            this.tableBucket = tableBucket;
            this.replica = replica;
        }
    }
}
