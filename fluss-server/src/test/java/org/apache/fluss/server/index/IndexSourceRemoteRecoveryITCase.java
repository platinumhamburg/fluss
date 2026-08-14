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

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.RemoteLogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestHooks;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end recovery of an asynchronous index from raw remote source WAL. */
class IndexSourceRemoteRecoveryITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(IndexSourceRemoteRecoveryITCase.class);

    private static final int TABLET_SERVER_COUNT = 4;
    private static final int REPLICATION_FACTOR = 3;
    private static final int INDEX_BUCKET_COUNT = 8;
    private static final int BASELINE_ROW_COUNT = 12;
    private static final int UPDATED_ROW_COUNT = 6;
    private static final int REPLAY_INSERT_COUNT = 12;
    private static final int NEVER_WRITTEN_KEY = 10_000;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration REMOTE_TIMEOUT = Duration.ofMinutes(2);

    private static final String INDEX_NAME = "idx_b";
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));
    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(configuration())
                    .build();

    @Test
    void recoverAsyncIndexFromRawRemoteSourceWalAndContinueLocally() throws Throwable {
        try (RemoteRecoveryFixture recovery = setUpRemoteRecovery()) {
            Map<Integer, String> expectedRows = new LinkedHashMap<>();
            long baselinePushedOffset = persistBaselineSnapshot(recovery, expectedRows);
            long committedSourceEnd =
                    createRemoteReplayRange(recovery, baselinePushedOffset, expectedRows);
            RecoveredSource recovered =
                    recoverFromRemoteWal(recovery, baselinePushedOffset, committedSourceEnd);
            verifyRecoveredProjection(recovery, recovered, committedSourceEnd, expectedRows);
            long continuationEnd =
                    verifyLocalContinuation(recovery, recovered, committedSourceEnd, expectedRows);

            LOG.info(
                    "Remote index recovery evidence: sourceLeader={}, recoveryFollower={}, "
                            + "offlineFollower={}, targetOnly={}, targetBucket={}; baseline={}, "
                            + "committedEnd={}, followerLocalStart={}, replayProgress={}, "
                            + "remoteBytes={}->{}, continuationEnd={}",
                    recovery.sourceLeader,
                    recovery.recoveryFollower,
                    recovery.offlineFollower,
                    recovery.targetOnlyServer,
                    recovery.gatedTargetBucket,
                    baselinePushedOffset,
                    committedSourceEnd,
                    recovered.followerLocalStart,
                    recovery.replayGate().admittedProgress(),
                    recovered.remoteBytesBefore,
                    recovered.remoteBytesWhileAckHeld,
                    continuationEnd);
        }
    }

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofMillis(5));
        return conf;
    }

    private static RemoteRecoveryFixture setUpRemoteRecovery() throws Exception {
        IndexedSourceTable indexedSourceTable = createIndexedSourceTable();
        CLUSTER.waitUntilAllReplicaReady(indexedSourceTable.sourceTableBucket);
        Replica sourceReplica =
                CLUSTER.waitAndGetLeaderReplica(indexedSourceTable.sourceTableBucket);
        waitUntil(
                () -> sourceReplica.getIsr().size() == REPLICATION_FACTOR,
                TIMEOUT,
                "wait for source RF3 ISR");
        waitUntil(
                () -> sourceReplica.getIndexReplicator() != null,
                TIMEOUT,
                "wait for initial source IndexReplicator");
        int sourceLeader = CLUSTER.waitAndGetLeader(indexedSourceTable.sourceTableBucket);
        RecoveryRoles roles =
                resolveRecoveryRoles(indexedSourceTable.sourceTableBucket, sourceLeader);
        TableBucket gatedTargetBucket =
                findTargetBucketLedBy(
                        indexedSourceTable.indexTableId,
                        roles.targetOnlyServer,
                        roles.sourceReplicas);

        int targetBucket = gatedTargetBucket.getBucket();
        String firstIndexValue = valueForIndexBucket("remote-old", targetBucket);
        String secondIndexValue = valueForIndexBucket("remote-new", targetBucket);
        assertThat(firstIndexValue).isNotEqualTo(secondIndexValue);
        assertThat(indexBucket(firstIndexValue)).isEqualTo(targetBucket);
        assertThat(indexBucket(secondIndexValue)).isEqualTo(targetBucket);
        assertThat(CLUSTER.waitAndGetLeader(gatedTargetBucket)).isEqualTo(roles.targetOnlyServer);

        LOG.info(
                "Remote-recovery topology: source={} assignment={}, sourceLeader={}, recoveryFollower={}, "
                        + "offlineFollower={}, targetOnly={}, targetBucket={}, values=[{}, {}]",
                indexedSourceTable.sourceTableBucket,
                roles.sourceReplicas,
                roles.sourceLeader,
                roles.recoveryFollower,
                roles.offlineFollower,
                roles.targetOnlyServer,
                gatedTargetBucket,
                firstIndexValue,
                secondIndexValue);
        return new RemoteRecoveryFixture(
                indexedSourceTable.mainTableId,
                indexedSourceTable.indexTableId,
                indexedSourceTable.sourceTableBucket,
                roles.sourceLeader,
                roles.recoveryFollower,
                roles.offlineFollower,
                roles.targetOnlyServer,
                gatedTargetBucket,
                firstIndexValue,
                secondIndexValue,
                sourceReplica);
    }

    private static IndexedSourceTable createIndexedSourceTable() throws Exception {
        String tableName = "source_remote_recovery_" + System.nanoTime();
        TablePath mainPath = TablePath.of("task7", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.ASYNC,
                                INDEX_BUCKET_COUNT)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder().schema(schema).distributedBy(1, "a").build());
        TablePath indexPath =
                TablePath.of(
                        "task7",
                        IndexTableUtils.indexTableName(mainPath.getTableName(), INDEX_NAME));
        long indexTableId =
                CLUSTER.getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(() -> new AssertionError("Index Table missing from ZK"))
                        .tableId;
        return new IndexedSourceTable(mainTableId, indexTableId);
    }

    private static RecoveryRoles resolveRecoveryRoles(
            TableBucket sourceTableBucket, int sourceLeader) throws Exception {
        TableAssignment sourceAssignment =
                CLUSTER.getZooKeeperClient()
                        .getTableAssignment(sourceTableBucket.getTableId())
                        .orElseThrow(() -> new AssertionError("Source assignment missing from ZK"));
        List<Integer> sourceReplicas =
                new ArrayList<>(sourceAssignment.getBucketAssignment(0).getReplicas());
        assertThat(sourceReplicas)
                .as("the single source bucket must have an exact RF3 assignment")
                .hasSize(REPLICATION_FACTOR)
                .contains(sourceLeader);
        assertThat(new LinkedHashSet<>(sourceReplicas)).hasSize(REPLICATION_FACTOR);
        List<Integer> sourceFollowers =
                sourceReplicas.stream()
                        .filter(server -> server != sourceLeader)
                        .collect(Collectors.toList());
        assertThat(sourceFollowers).hasSize(2);
        int recoveryFollower = sourceFollowers.get(0);
        int offlineFollower = sourceFollowers.get(1);

        int[] liveServerIds = CLUSTER.getZooKeeperClient().getSortedTabletServerList();
        assertThat(liveServerIds).hasSize(TABLET_SERVER_COUNT);
        List<Integer> targetOnlyServers =
                Arrays.stream(liveServerIds)
                        .boxed()
                        .filter(server -> !sourceReplicas.contains(server))
                        .collect(Collectors.toList());
        assertThat(targetOnlyServers)
                .as("the source RF3 assignment must leave one target-only TabletServer")
                .hasSize(1);
        int targetOnlyServer = targetOnlyServers.get(0);
        Set<Integer> roles =
                new LinkedHashSet<>(
                        Arrays.asList(
                                sourceLeader, recoveryFollower, offlineFollower, targetOnlyServer));
        assertThat(roles).hasSize(TABLET_SERVER_COUNT);
        assertThat(roles)
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.stream(liveServerIds).boxed().collect(Collectors.toList()));
        return new RecoveryRoles(
                sourceReplicas, sourceLeader, recoveryFollower, offlineFollower, targetOnlyServer);
    }

    private static TableBucket findTargetBucketLedBy(
            long indexTableId, int targetOnlyServer, List<Integer> sourceReplicas)
            throws Exception {
        int targetBucket = -1;
        int[] indexLeaders = new int[INDEX_BUCKET_COUNT];
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket indexBucket = new TableBucket(indexTableId, bucket);
            Replica indexReplica = CLUSTER.waitAndGetLeaderReplica(indexBucket);
            waitUntil(
                    () -> indexReplica.getIsr().size() == REPLICATION_FACTOR,
                    TIMEOUT,
                    "wait for Index Table RF3 ISR " + indexBucket);
            int leader = CLUSTER.waitAndGetLeader(indexBucket);
            indexLeaders[bucket] = leader;
            if (targetBucket < 0 && leader == targetOnlyServer) {
                targetBucket = bucket;
            }
        }
        if (targetBucket < 0) {
            throw new AssertionError(
                    "No Index Table bucket is led by target-only server "
                            + targetOnlyServer
                            + "; source replicas="
                            + sourceReplicas
                            + "; index leaders="
                            + Arrays.toString(indexLeaders));
        }
        LOG.info(
                "Index Table leaders for target-only server {}: {}",
                targetOnlyServer,
                Arrays.toString(indexLeaders));
        return new TableBucket(indexTableId, targetBucket);
    }

    private static long persistBaselineSnapshot(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        recovery.stopServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(
                recovery.sourceTableBucket, recovery.recoveryFollower);
        recovery.stopServer(recovery.offlineFollower);
        CLUSTER.waitUntilReplicaShrinkFromIsr(recovery.sourceTableBucket, recovery.offlineFollower);

        for (int key = 0; key < BASELINE_ROW_COUNT; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.firstIndexValue);
            expectedRows.put(key, recovery.firstIndexValue);
        }
        waitForExactPhysicalRows(recovery, expectedRows);
        waitUntil(
                () ->
                        recovery.sourceReplica.getAllIndexPushedOffset()
                                == recovery.sourceReplica.getLocalLogEndOffset(),
                TIMEOUT,
                "wait for the exact baseline index prefix");
        long baselinePushedOffset = recovery.sourceReplica.getLocalLogEndOffset();
        assertThat(baselinePushedOffset).isPositive();
        assertThat(recovery.sourceReplica.getAllIndexPushedOffset())
                .as("the conservative baseline must cover the complete first source prefix")
                .isEqualTo(baselinePushedOffset);

        CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(recovery.sourceTableBucket);
        assertThat(snapshot.getLogOffset()).isEqualTo(baselinePushedOffset);
        assertThat(snapshot.getIndexPushedOffset())
                .as("the source snapshot must persist the exact conservative index prefix")
                .isEqualTo(baselinePushedOffset);
        return baselinePushedOffset;
    }

    private static long createRemoteReplayRange(
            RemoteRecoveryFixture recovery,
            long baselinePushedOffset,
            Map<Integer, String> expectedRows)
            throws Exception {
        IndexReplicator oldReplicator = recovery.sourceReplica.getIndexReplicator();
        assertThat(oldReplicator).isNotNull();
        oldReplicator.close();
        assertThat(oldReplicator.isClosed()).isTrue();
        sourceTabletServer(recovery.sourceLeader)
                .getReplicaManager()
                .getIndexReplicatorPool()
                .unregister(recovery.sourceTableBucket);
        assertThat(recovery.sourceReplica.getIndexReplicator())
                .as("the retired source controller must still identify the exact old run")
                .isSameAs(oldReplicator);

        for (int key = 0; key < UPDATED_ROW_COUNT; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.secondIndexValue);
            expectedRows.put(key, recovery.secondIndexValue);
        }
        int replayInsertEnd = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT;
        for (int key = BASELINE_ROW_COUNT; key < replayInsertEnd; key++) {
            putSourceRow(recovery, recovery.sourceLeader, key, recovery.secondIndexValue);
            expectedRows.put(key, recovery.secondIndexValue);
        }
        waitForSourceCommit(recovery.sourceReplica);
        long committedSourceEnd = recovery.sourceReplica.getLocalLogEndOffset();
        assertThat(committedSourceEnd).isGreaterThan(baselinePushedOffset);
        assertThat(recovery.sourceReplica.getAllIndexPushedOffset())
                .as("the closed old run must not advance over the second source prefix")
                .isEqualTo(baselinePushedOffset);

        LogTablet sourceLog = recovery.sourceReplica.getLogTablet();
        sourceLog.roll(Optional.empty());
        assertThat(sourceLog.activeLogSegment().getBaseOffset())
                .as("the explicit roll must close the complete committed replay range")
                .isEqualTo(committedSourceEnd);
        waitForRawRemoteReplayCoverage(
                recovery, recovery.sourceReplica, baselinePushedOffset, committedSourceEnd);
        return committedSourceEnd;
    }

    private static RecoveredSource recoverFromRemoteWal(
            RemoteRecoveryFixture recovery, long baselinePushedOffset, long committedSourceEnd)
            throws Exception {
        recovery.startServer(recovery.recoveryFollower);
        CLUSTER.waitUntilReplicaExpandToIsr(recovery.sourceTableBucket, recovery.recoveryFollower);
        Replica recoveryFollowerReplica =
                CLUSTER.waitAndGetFollowerReplica(
                        recovery.sourceTableBucket, recovery.recoveryFollower);
        long followerLocalStart = recoveryFollowerReplica.getLogTablet().localLogStartOffset();
        assertThat(followerLocalStart)
                .as("the selected recovery follower must have discarded the baseline range")
                .isGreaterThan(baselinePushedOffset);

        assertThat(CLUSTER.waitAndGetLeader(recovery.gatedTargetBucket))
                .as("the gated target must remain led by the target-only server")
                .isEqualTo(recovery.targetOnlyServer);
        Replica gatedTargetReplica = CLUSTER.waitAndGetLeaderReplica(recovery.gatedTargetBucket);
        WriterKey writerKey = IndexWriterKey.encode(recovery.sourceTableBucket);
        recovery.installReplayGate(gatedTargetReplica, writerKey, baselinePushedOffset);

        TabletServer recoveryTabletServer = sourceTabletServer(recovery.recoveryFollower);
        TabletServerMetricGroup recoveryMetrics =
                recoveryTabletServer.getReplicaManager().getServerMetricGroup();
        long remoteBytesBefore = recoveryMetrics.indexReplicationSourceBytes().getCount();

        recovery.stopServer(recovery.sourceLeader);
        waitUntil(
                () ->
                        CLUSTER.waitAndGetLeader(recovery.sourceTableBucket)
                                == recovery.recoveryFollower,
                TIMEOUT,
                "wait for the exact remote-recovery follower to lead the source bucket");
        Replica newSourceReplica = CLUSTER.waitAndGetLeaderReplica(recovery.sourceTableBucket);
        assertThat(CLUSTER.waitAndGetLeader(recovery.sourceTableBucket))
                .isEqualTo(recovery.recoveryFollower);
        waitUntil(
                () -> newSourceReplica.getIndexReplicator() != null,
                TIMEOUT,
                "wait for the recovered source IndexReplicator");

        recovery.replayGate().awaitAdmission();
        long remoteBytesWhileAckHeld = recoveryMetrics.indexReplicationSourceBytes().getCount();
        assertThat(newSourceReplica.getLogTablet().localLogStartOffset())
                .as("the replay start must be unavailable from local source WAL")
                .isGreaterThan(baselinePushedOffset);
        assertThat(newSourceReplica.getAllIndexPushedOffset())
                .as("the held target ACK must preserve the snapshot-restored baseline")
                .isEqualTo(baselinePushedOffset);
        assertThat(remoteBytesWhileAckHeld)
                .as("raw remote source bytes must be consumed before target progress advances")
                .isGreaterThan(remoteBytesBefore);
        return new RecoveredSource(
                newSourceReplica, followerLocalStart, remoteBytesBefore, remoteBytesWhileAckHeld);
    }

    private static void verifyRecoveredProjection(
            RemoteRecoveryFixture recovery,
            RecoveredSource recovered,
            long committedSourceEnd,
            Map<Integer, String> expectedRows)
            throws Exception {
        recovery.replayGate().release();
        waitUntil(
                () -> recovered.replica.getAllIndexPushedOffset() == committedSourceEnd,
                TIMEOUT,
                "wait for exact recovered source replay progress");
        assertThat(recovered.replica.getAllIndexPushedOffset()).isEqualTo(committedSourceEnd);
        assertExactIndexProjection(recovery, expectedRows);
        assertIndexKeyAbsent(recovery, recovery.firstIndexValue, 0, "known stale pre-update key");
        assertIndexKeyAbsent(
                recovery, recovery.secondIndexValue, NEVER_WRITTEN_KEY, "never-written index key");
    }

    private static long verifyLocalContinuation(
            RemoteRecoveryFixture recovery,
            RecoveredSource recovered,
            long committedSourceEnd,
            Map<Integer, String> expectedRows)
            throws Exception {
        int continuationKey = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT;
        putSourceRow(
                recovery, recovery.recoveryFollower, continuationKey, recovery.firstIndexValue);
        expectedRows.put(continuationKey, recovery.firstIndexValue);
        waitForSourceCommit(recovered.replica);
        long continuationEnd = recovered.replica.getLocalLogEndOffset();
        assertThat(continuationEnd).isGreaterThan(committedSourceEnd);
        waitUntil(
                () -> recovered.replica.getAllIndexPushedOffset() == continuationEnd,
                TIMEOUT,
                "wait for exact local continuation progress");
        assertThat(recovered.replica.getAllIndexPushedOffset()).isEqualTo(continuationEnd);
        assertExactIndexProjection(recovery, expectedRows);
        return continuationEnd;
    }

    private static void putSourceRow(
            RemoteRecoveryFixture recovery, int sourceLeader, int key, String indexedValue)
            throws Exception {
        TabletServerGateway gateway = CLUSTER.newTabletServerClientForNode(sourceLeader);
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                                recovery.mainTableId,
                                                0,
                                                1,
                                                genKvRecordBatch(new Object[] {key, indexedValue}))
                                        .setTimeoutMs((int) TIMEOUT.toMillis()))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response.getBucketsRespsList())
                .allSatisfy(bucket -> assertThat(bucket.hasErrorCode()).isFalse());
    }

    private static void waitForSourceCommit(Replica sourceReplica) {
        waitUntil(
                () ->
                        sourceReplica.getLogTablet().getHighWatermark()
                                == sourceReplica.getLocalLogEndOffset(),
                TIMEOUT,
                "wait for source high watermark to reach local end");
        assertThat(sourceReplica.getLogTablet().getHighWatermark())
                .isEqualTo(sourceReplica.getLocalLogEndOffset());
    }

    private static void waitForRawRemoteReplayCoverage(
            RemoteRecoveryFixture recovery,
            Replica sourceReplica,
            long baselinePushedOffset,
            long committedSourceEnd) {
        LogTablet sourceLog = sourceReplica.getLogTablet();
        RemoteLogManager remoteLogManager =
                sourceTabletServer(recovery.sourceLeader).getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(recovery.sourceTableBucket);
        LOG.info(
                "Initial remote coverage state: {}",
                safeRemoteCoverageState(
                        sourceReplica, remoteLog, baselinePushedOffset, committedSourceEnd));

        try {
            waitUntil(
                    () ->
                            sourceLog.canFetchFromRemoteLog(baselinePushedOffset)
                                    && sourceLog.canFetchFromRemoteLog(committedSourceEnd - 1L),
                    REMOTE_TIMEOUT,
                    "wait for raw remote source WAL to cover the complete replay range");
        } catch (AssertionError | RuntimeException failure) {
            String state =
                    safeRemoteCoverageState(
                            sourceReplica, remoteLog, baselinePushedOffset, committedSourceEnd);
            throw new AssertionError(
                    "raw remote source WAL coverage timed out; final tiering state: " + state,
                    failure);
        }

        String finalState =
                safeRemoteCoverageState(
                        sourceReplica, remoteLog, baselinePushedOffset, committedSourceEnd);
        LOG.info("Raw remote replay range covered: {}", finalState);
        assertThat(sourceLog.canFetchFromRemoteLog(baselinePushedOffset))
                .as("raw remote source WAL must cover the replay start")
                .isTrue();
        assertThat(sourceLog.canFetchFromRemoteLog(committedSourceEnd - 1L))
                .as("raw remote source WAL must cover the replay end minus one")
                .isTrue();
    }

    private static String safeRemoteCoverageState(
            Replica sourceReplica,
            RemoteLogTablet remoteLog,
            long baselinePushedOffset,
            long committedSourceEnd) {
        try {
            return remoteCoverageState(
                    sourceReplica, remoteLog, baselinePushedOffset, committedSourceEnd);
        } catch (RuntimeException failure) {
            return "diagnostic-error="
                    + failure.getClass().getSimpleName()
                    + ':'
                    + failure.getMessage();
        }
    }

    private static String remoteCoverageState(
            Replica sourceReplica,
            RemoteLogTablet remoteLog,
            long baselinePushedOffset,
            long committedSourceEnd) {
        LogTablet sourceLog = sourceReplica.getLogTablet();
        long remoteStart = remoteLog.getRemoteLogStartOffset();
        long remoteEnd = remoteLog.getRemoteLogEndOffset().orElse(-1L);
        return String.format(
                "required=[%d,%d), covered={start=%s,endMinusOne=%s}, "
                        + "remote=[%d,%d), local=[%d,%d), logStart=%d, hw=%d, "
                        + "recoveryPoint=%d, activeBase=%d, retainedLocalSegments=%d, isr=%s, "
                        + "manifest=%s, localSegments=%s, remoteSegments=%s, "
                        + "copy={requests=%d,bytes=%d,errors=%d}",
                baselinePushedOffset,
                committedSourceEnd,
                sourceLog.canFetchFromRemoteLog(baselinePushedOffset),
                sourceLog.canFetchFromRemoteLog(committedSourceEnd - 1L),
                remoteStart,
                remoteEnd,
                sourceLog.localLogStartOffset(),
                sourceLog.localLogEndOffset(),
                sourceLog.logStartOffset(),
                sourceLog.getHighWatermark(),
                sourceLog.getRecoveryPoint(),
                sourceLog.activeLogSegment().getBaseOffset(),
                sourceLog.getTieredLogLocalSegments(),
                sourceReplica.getIsr(),
                remoteManifestState(sourceReplica.getTableBucket()),
                localSegmentState(sourceLog),
                remoteSegmentState(remoteLog),
                sourceReplica.tableMetrics().remoteLogCopyRequests().getCount(),
                sourceReplica.tableMetrics().remoteLogCopyBytes().getCount(),
                sourceReplica.tableMetrics().remoteLogCopyErrors().getCount());
    }

    private static String remoteManifestState(TableBucket tableBucket) {
        try {
            return CLUSTER.getZooKeeperClient().getRemoteLogManifestHandle(tableBucket).toString();
        } catch (Exception failure) {
            return "error=" + failure.getClass().getSimpleName() + ':' + failure.getMessage();
        }
    }

    private static List<String> localSegmentState(LogTablet logTablet) {
        List<LogSegment> segments = logTablet.logSegments();
        List<String> state = new ArrayList<>(segments.size());
        long activeBase = logTablet.activeLogSegment().getBaseOffset();
        for (int index = 0; index < segments.size(); index++) {
            LogSegment segment = segments.get(index);
            long endOffset =
                    index + 1 < segments.size()
                            ? segments.get(index + 1).getBaseOffset()
                            : logTablet.localLogEndOffset();
            state.add(
                    String.format(
                            "[%d,%d):%db%s",
                            segment.getBaseOffset(),
                            endOffset,
                            segment.getSizeInBytes(),
                            segment.getBaseOffset() == activeBase ? ":active" : ""));
        }
        return state;
    }

    private static List<String> remoteSegmentState(RemoteLogTablet remoteLog) {
        return remoteLog.allRemoteLogSegments().stream()
                .map(
                        segment ->
                                String.format(
                                        "[%d,%d):%db",
                                        segment.remoteLogStartOffset(),
                                        segment.remoteLogEndOffset(),
                                        segment.segmentSizeInBytes()))
                .collect(Collectors.toList());
    }

    private static void waitForExactPhysicalRows(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        int schemaId = liveIndexTableInfo(recovery).getSchemaId();
        for (Map.Entry<Integer, String> expectedRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(expectedRow.getKey(), expectedRow.getValue(), schemaId);
            TableBucket target =
                    new TableBucket(recovery.indexTableId, indexBucket(expectedRow.getValue()));
            waitUntil(
                    () -> {
                        byte[] value =
                                CLUSTER.waitAndGetLeaderReplica(target)
                                        .lookups(Collections.singletonList(physical.key))
                                        .get(0);
                        return Arrays.equals(value, physical.value);
                    },
                    TIMEOUT,
                    "wait for exact physical Index Table row " + expectedRow);
        }
    }

    private static void assertExactIndexProjection(
            RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception {
        int schemaId = liveIndexTableInfo(recovery).getSchemaId();
        Map<TableBucket, Map<String, String>> expected = emptyIndexProjection(recovery);
        for (Map.Entry<Integer, String> sourceRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(sourceRow.getKey(), sourceRow.getValue(), schemaId);
            TableBucket bucket =
                    new TableBucket(recovery.indexTableId, indexBucket(sourceRow.getValue()));
            String previous =
                    expected.get(bucket).put(encode(physical.key), encode(physical.value));
            assertThat(previous).as("physical index keys must be unique").isNull();
        }

        Map<TableBucket, Map<String, String>> actual = emptyIndexProjection(recovery);
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(recovery.indexTableId, bucket);
            Replica replica = CLUSTER.waitAndGetLeaderReplica(tableBucket);
            replica.getKvTablet().flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
            try (ReadOptions readOptions = new ReadOptions();
                    RocksIterator iterator =
                            replica.getKvTablet()
                                    .getRocksDBKv()
                                    .getDb()
                                    .newIterator(
                                            replica.getKvTablet()
                                                    .getRocksDBKv()
                                                    .getDefaultColumnFamilyHandle(),
                                            readOptions)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    actual.get(tableBucket).put(encode(iterator.key()), encode(iterator.value()));
                    iterator.next();
                }
                iterator.status();
            }
        }
        assertThat(actual)
                .as("every physical Index Table key/value byte must exactly project source state")
                .isEqualTo(expected);
    }

    private static void assertIndexKeyAbsent(
            RemoteRecoveryFixture recovery, String indexedValue, int primaryKey, String description)
            throws Exception {
        PhysicalIndexRow physical =
                physicalIndexRow(
                        primaryKey, indexedValue, liveIndexTableInfo(recovery).getSchemaId());
        TableBucket target = new TableBucket(recovery.indexTableId, indexBucket(indexedValue));
        assertThat(
                        CLUSTER.waitAndGetLeaderReplica(target)
                                .lookups(Collections.singletonList(physical.key))
                                .get(0))
                .as(description)
                .isNull();
    }

    private static TableInfo liveIndexTableInfo(RemoteRecoveryFixture recovery) {
        return CLUSTER.waitAndGetLeaderReplica(new TableBucket(recovery.indexTableId, 0))
                .getTableInfo();
    }

    private static Map<TableBucket, Map<String, String>> emptyIndexProjection(
            RemoteRecoveryFixture recovery) {
        Map<TableBucket, Map<String, String>> projection = new LinkedHashMap<>();
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            projection.put(new TableBucket(recovery.indexTableId, bucket), new LinkedHashMap<>());
        }
        return projection;
    }

    private static PhysicalIndexRow physicalIndexRow(
            int primaryKey, String indexedValue, int schemaId) throws Exception {
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(INDEX_ROW_TYPE);
        try (RowEncoder valueEncoder = RowEncoder.create(KvFormat.COMPACTED, INDEX_ROW_TYPE)) {
            valueEncoder.startNewRow();
            valueEncoder.encodeField(0, fromString(indexedValue));
            valueEncoder.encodeField(1, primaryKey);
            BinaryRow row = valueEncoder.finishRow();
            return new PhysicalIndexRow(
                    keyEncoder.encodeKey(row), ValueEncoder.encodeValue((short) schemaId, row));
        }
    }

    private static String valueForIndexBucket(String prefix, int targetBucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String candidate = prefix + '-' + suffix;
            if (indexBucket(candidate) == targetBucket) {
                return candidate;
            }
        }
        throw new AssertionError("No value found for Index Table bucket " + targetBucket);
    }

    private static int indexBucket(String indexedValue) {
        GenericRow bucketKey = new GenericRow(1);
        bucketKey.setField(0, fromString(indexedValue));
        return new FlussBucketingFunction()
                .bucketing(
                        new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE).encodeKey(bucketKey),
                        INDEX_BUCKET_COUNT);
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static TabletServer sourceTabletServer(int serverId) {
        TabletServer tabletServer = CLUSTER.getTabletServerById(serverId);
        assertThat(tabletServer).as("TabletServer %s must be live", serverId).isNotNull();
        return tabletServer;
    }

    private static void stopAndTrack(int serverId, Set<Integer> stoppedServers) throws Exception {
        assertThat(stoppedServers).doesNotContain(serverId);
        CLUSTER.stopTabletServer(serverId);
        assertThat(stoppedServers.add(serverId)).isTrue();
    }

    private static void startAndUntrack(int serverId, Set<Integer> stoppedServers)
            throws Exception {
        assertThat(stoppedServers).contains(serverId);
        CLUSTER.startTabletServer(serverId);
        assertThat(stoppedServers.remove(serverId)).isTrue();
    }

    private static void closeGate(@Nullable ReplayGate gate) throws Exception {
        if (gate != null) {
            gate.close();
        }
    }

    private static void cleanup(List<Throwable> failures, CleanupAction action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failures.add(failure);
        }
    }

    private static void reportCleanupFailures(
            @Nullable Throwable primaryFailure, List<Throwable> cleanupFailures) {
        if (cleanupFailures.isEmpty()) {
            return;
        }
        AssertionError cleanupFailure =
                new AssertionError("source remote-recovery IT cleanup failed");
        cleanupFailures.forEach(cleanupFailure::addSuppressed);
        if (primaryFailure != null) {
            primaryFailure.addSuppressed(cleanupFailure);
        } else {
            throw cleanupFailure;
        }
    }

    private static void await(CountDownLatch latch, String description) {
        try {
            assertThat(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as(description)
                    .isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Exception;
    }

    private static final class ReplayGate implements AutoCloseable {
        private final WriterKey writerKey;
        private final long baselineProgress;
        private final AtomicLong admittedProgress = new AtomicLong(-1L);
        private final CountDownLatch replayAdmitted = new CountDownLatch(1);
        private final CountDownLatch releaseReplay = new CountDownLatch(1);
        private final AutoCloseable registration;

        private ReplayGate(Replica targetReplica, WriterKey writerKey, long baselineProgress) {
            this.writerKey = writerKey;
            this.baselineProgress = baselineProgress;
            this.registration =
                    ReplicaTestHooks.installAfterPutAdmissionHook(
                            targetReplica, this::afterAdmission);
        }

        private void afterAdmission(KvRecordBatch batch) {
            if (!writerKey.equals(batch.writerKey())
                    || batch.writerProgress() <= baselineProgress) {
                return;
            }
            admittedProgress.accumulateAndGet(batch.writerProgress(), Math::max);
            replayAdmitted.countDown();
            await(releaseReplay, "wait to release remote source replay ACK");
        }

        private void awaitAdmission() {
            await(replayAdmitted, "wait for canonical remote source replay admission");
            assertThat(admittedProgress.get()).isGreaterThan(baselineProgress);
        }

        private long admittedProgress() {
            return admittedProgress.get();
        }

        private void release() {
            releaseReplay.countDown();
        }

        @Override
        public void close() throws Exception {
            release();
            registration.close();
        }
    }

    private static final class PhysicalIndexRow {
        private final byte[] key;
        private final byte[] value;

        private PhysicalIndexRow(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private static final class IndexedSourceTable {
        private final long mainTableId;
        private final long indexTableId;
        private final TableBucket sourceTableBucket;

        private IndexedSourceTable(long mainTableId, long indexTableId) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.sourceTableBucket = new TableBucket(mainTableId, 0);
        }
    }

    private static final class RecoveryRoles {
        private final List<Integer> sourceReplicas;
        private final int sourceLeader;
        private final int recoveryFollower;
        private final int offlineFollower;
        private final int targetOnlyServer;

        private RecoveryRoles(
                List<Integer> sourceReplicas,
                int sourceLeader,
                int recoveryFollower,
                int offlineFollower,
                int targetOnlyServer) {
            this.sourceReplicas = new ArrayList<>(sourceReplicas);
            this.sourceLeader = sourceLeader;
            this.recoveryFollower = recoveryFollower;
            this.offlineFollower = offlineFollower;
            this.targetOnlyServer = targetOnlyServer;
        }
    }

    private static final class RecoveredSource {
        private final Replica replica;
        private final long followerLocalStart;
        private final long remoteBytesBefore;
        private final long remoteBytesWhileAckHeld;

        private RecoveredSource(
                Replica replica,
                long followerLocalStart,
                long remoteBytesBefore,
                long remoteBytesWhileAckHeld) {
            this.replica = replica;
            this.followerLocalStart = followerLocalStart;
            this.remoteBytesBefore = remoteBytesBefore;
            this.remoteBytesWhileAckHeld = remoteBytesWhileAckHeld;
        }
    }

    private static final class RemoteRecoveryFixture implements AutoCloseable {
        private final long mainTableId;
        private final long indexTableId;
        private final TableBucket sourceTableBucket;
        private final int sourceLeader;
        private final int recoveryFollower;
        private final int offlineFollower;
        private final int targetOnlyServer;
        private final TableBucket gatedTargetBucket;
        private final String firstIndexValue;
        private final String secondIndexValue;
        private final Replica sourceReplica;
        private final Set<Integer> stoppedServers = new LinkedHashSet<>();
        @Nullable private ReplayGate replayGate;

        private RemoteRecoveryFixture(
                long mainTableId,
                long indexTableId,
                TableBucket sourceTableBucket,
                int sourceLeader,
                int recoveryFollower,
                int offlineFollower,
                int targetOnlyServer,
                TableBucket gatedTargetBucket,
                String firstIndexValue,
                String secondIndexValue,
                Replica sourceReplica) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.sourceTableBucket = sourceTableBucket;
            this.sourceLeader = sourceLeader;
            this.recoveryFollower = recoveryFollower;
            this.offlineFollower = offlineFollower;
            this.targetOnlyServer = targetOnlyServer;
            this.gatedTargetBucket = gatedTargetBucket;
            this.firstIndexValue = firstIndexValue;
            this.secondIndexValue = secondIndexValue;
            this.sourceReplica = sourceReplica;
        }

        private void stopServer(int serverId) throws Exception {
            stopAndTrack(serverId, stoppedServers);
        }

        private void startServer(int serverId) throws Exception {
            startAndUntrack(serverId, stoppedServers);
        }

        private ReplayGate installReplayGate(
                Replica targetReplica, WriterKey writerKey, long baselineProgress) {
            assertThat(replayGate).as("a replay gate may be installed only once").isNull();
            replayGate = new ReplayGate(targetReplica, writerKey, baselineProgress);
            return replayGate;
        }

        private ReplayGate replayGate() {
            assertThat(replayGate).as("the replay gate must be installed").isNotNull();
            return replayGate;
        }

        @Override
        public void close() {
            List<Throwable> failures = new ArrayList<>();
            if (replayGate != null) {
                replayGate.release();
            }
            cleanup(failures, () -> closeGate(replayGate));
            for (int stoppedServer : new LinkedHashSet<>(stoppedServers)) {
                cleanup(failures, () -> startAndUntrack(stoppedServer, stoppedServers));
            }
            cleanup(failures, () -> CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT));
            reportCleanupFailures(null, failures);
        }
    }
}
