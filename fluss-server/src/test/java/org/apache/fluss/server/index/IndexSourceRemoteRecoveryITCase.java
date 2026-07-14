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

    private Fixture fixture;

    @Test
    void recoverAsyncIndexFromRawRemoteSourceWalAndContinueLocally() throws Throwable {
        fixture = createFixture();
        Set<Integer> stoppedServers = new LinkedHashSet<>();
        ReplayGate replayGate = null;
        Throwable primaryFailure = null;

        try {
            stopAndTrack(fixture.recoveryFollower, stoppedServers);
            CLUSTER.waitUntilReplicaShrinkFromIsr(
                    fixture.sourceTableBucket, fixture.recoveryFollower);
            stopAndTrack(fixture.offlineFollower, stoppedServers);
            CLUSTER.waitUntilReplicaShrinkFromIsr(
                    fixture.sourceTableBucket, fixture.offlineFollower);

            Map<Integer, String> expectedRows = new LinkedHashMap<>();
            for (int key = 0; key < BASELINE_ROW_COUNT; key++) {
                putSourceRow(fixture.sourceLeader, key, fixture.firstIndexValue);
                expectedRows.put(key, fixture.firstIndexValue);
            }
            waitForExactPhysicalRows(expectedRows);
            waitUntil(
                    () ->
                            fixture.sourceReplica.getAllIndexPushedOffset()
                                    == fixture.sourceReplica.getLocalLogEndOffset(),
                    TIMEOUT,
                    "wait for the exact baseline index prefix");
            long baselinePushedOffset = fixture.sourceReplica.getLocalLogEndOffset();
            assertThat(baselinePushedOffset).isPositive();
            assertThat(fixture.sourceReplica.getAllIndexPushedOffset())
                    .as("the conservative baseline must cover the complete first source prefix")
                    .isEqualTo(baselinePushedOffset);

            CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(fixture.sourceTableBucket);
            assertThat(snapshot.getLogOffset()).isEqualTo(baselinePushedOffset);
            assertThat(snapshot.getIndexPushedOffset())
                    .as("the source snapshot must persist the exact conservative index prefix")
                    .isEqualTo(baselinePushedOffset);

            IndexReplicator oldReplicator = fixture.sourceReplica.getIndexReplicator();
            assertThat(oldReplicator).isNotNull();
            oldReplicator.close();
            assertThat(oldReplicator.isClosed()).isTrue();
            sourceTabletServer(fixture.sourceLeader)
                    .getReplicaManager()
                    .getIndexReplicatorPool()
                    .unregister(fixture.sourceTableBucket);
            assertThat(fixture.sourceReplica.getIndexReplicator())
                    .as("the retired source controller must still identify the exact old run")
                    .isSameAs(oldReplicator);

            for (int key = 0; key < UPDATED_ROW_COUNT; key++) {
                putSourceRow(fixture.sourceLeader, key, fixture.secondIndexValue);
                expectedRows.put(key, fixture.secondIndexValue);
            }
            int replayInsertEnd = BASELINE_ROW_COUNT + REPLAY_INSERT_COUNT;
            for (int key = BASELINE_ROW_COUNT; key < replayInsertEnd; key++) {
                putSourceRow(fixture.sourceLeader, key, fixture.secondIndexValue);
                expectedRows.put(key, fixture.secondIndexValue);
            }
            waitForSourceCommit(fixture.sourceReplica);
            long committedSourceEnd = fixture.sourceReplica.getLocalLogEndOffset();
            assertThat(committedSourceEnd).isGreaterThan(baselinePushedOffset);
            assertThat(fixture.sourceReplica.getAllIndexPushedOffset())
                    .as("the closed old run must not advance over the second source prefix")
                    .isEqualTo(baselinePushedOffset);

            fixture.sourceReplica.getLogTablet().roll(Optional.empty());
            waitUntil(
                    () ->
                            fixture.sourceReplica
                                            .getLogTablet()
                                            .canFetchFromRemoteLog(baselinePushedOffset)
                                    && fixture.sourceReplica
                                            .getLogTablet()
                                            .canFetchFromRemoteLog(committedSourceEnd - 1L),
                    REMOTE_TIMEOUT,
                    "wait for raw remote source WAL to cover the complete replay range");
            assertThat(
                            fixture.sourceReplica
                                    .getLogTablet()
                                    .canFetchFromRemoteLog(baselinePushedOffset))
                    .isTrue();
            assertThat(
                            fixture.sourceReplica
                                    .getLogTablet()
                                    .canFetchFromRemoteLog(committedSourceEnd - 1L))
                    .isTrue();

            startAndUntrack(fixture.recoveryFollower, stoppedServers);
            CLUSTER.waitUntilReplicaExpandToIsr(
                    fixture.sourceTableBucket, fixture.recoveryFollower);
            Replica recoveryFollowerReplica =
                    CLUSTER.waitAndGetFollowerReplica(
                            fixture.sourceTableBucket, fixture.recoveryFollower);
            long followerLocalStart = recoveryFollowerReplica.getLogTablet().localLogStartOffset();
            assertThat(followerLocalStart)
                    .as("the selected recovery follower must have discarded the baseline range")
                    .isGreaterThan(baselinePushedOffset);

            assertThat(CLUSTER.waitAndGetLeader(fixture.gatedTargetBucket))
                    .as("the gated target must remain led by the target-only server")
                    .isEqualTo(fixture.targetOnlyServer);
            Replica gatedTargetReplica = CLUSTER.waitAndGetLeaderReplica(fixture.gatedTargetBucket);
            WriterKey writerKey = IndexWriterKey.encode(fixture.sourceTableBucket);
            replayGate = new ReplayGate(gatedTargetReplica, writerKey, baselinePushedOffset);

            TabletServer recoveryTabletServer = sourceTabletServer(fixture.recoveryFollower);
            TabletServerMetricGroup recoveryMetrics =
                    recoveryTabletServer.getReplicaManager().getServerMetricGroup();
            long remoteBytesBefore = recoveryMetrics.indexSourceRemoteReadBytes().getCount();

            stopAndTrack(fixture.sourceLeader, stoppedServers);
            waitUntil(
                    () ->
                            CLUSTER.waitAndGetLeader(fixture.sourceTableBucket)
                                    == fixture.recoveryFollower,
                    TIMEOUT,
                    "wait for the exact remote-recovery follower to lead the source bucket");
            Replica newSourceReplica = CLUSTER.waitAndGetLeaderReplica(fixture.sourceTableBucket);
            assertThat(CLUSTER.waitAndGetLeader(fixture.sourceTableBucket))
                    .isEqualTo(fixture.recoveryFollower);
            waitUntil(
                    () -> newSourceReplica.getIndexReplicator() != null,
                    TIMEOUT,
                    "wait for the recovered source IndexReplicator");

            replayGate.awaitAdmission();
            long remoteBytesWhileAckHeld = recoveryMetrics.indexSourceRemoteReadBytes().getCount();
            assertThat(newSourceReplica.getLogTablet().localLogStartOffset())
                    .as("the replay start must be unavailable from local source WAL")
                    .isGreaterThan(baselinePushedOffset);
            assertThat(newSourceReplica.getAllIndexPushedOffset())
                    .as("the held target ACK must preserve the snapshot-restored baseline")
                    .isEqualTo(baselinePushedOffset);
            assertThat(remoteBytesWhileAckHeld)
                    .as("raw remote source bytes must be consumed before target progress advances")
                    .isGreaterThan(remoteBytesBefore);

            replayGate.release();
            waitUntil(
                    () -> newSourceReplica.getAllIndexPushedOffset() == committedSourceEnd,
                    TIMEOUT,
                    "wait for exact recovered source replay progress");
            assertThat(newSourceReplica.getAllIndexPushedOffset()).isEqualTo(committedSourceEnd);
            assertExactIndexProjection(expectedRows);
            assertIndexKeyAbsent(fixture.firstIndexValue, 0, "known stale pre-update key");
            assertIndexKeyAbsent(
                    fixture.secondIndexValue, NEVER_WRITTEN_KEY, "never-written index key");

            int continuationKey = replayInsertEnd;
            putSourceRow(fixture.recoveryFollower, continuationKey, fixture.firstIndexValue);
            expectedRows.put(continuationKey, fixture.firstIndexValue);
            waitForSourceCommit(newSourceReplica);
            long continuationEnd = newSourceReplica.getLocalLogEndOffset();
            assertThat(continuationEnd).isGreaterThan(committedSourceEnd);
            waitUntil(
                    () -> newSourceReplica.getAllIndexPushedOffset() == continuationEnd,
                    TIMEOUT,
                    "wait for exact local continuation progress");
            assertThat(newSourceReplica.getAllIndexPushedOffset()).isEqualTo(continuationEnd);
            assertExactIndexProjection(expectedRows);

            LOG.info(
                    "Task 7 evidence: roles sourceLeader={}, recoveryFollower={}, "
                            + "offlineFollower={}, targetOnly={}, targetBucket={}; "
                            + "baseline={}, committedEnd={}, followerLocalStart={}, "
                            + "replaySequence={}, remoteBytes={}->{}, continuationEnd={}",
                    fixture.sourceLeader,
                    fixture.recoveryFollower,
                    fixture.offlineFollower,
                    fixture.targetOnlyServer,
                    fixture.gatedTargetBucket,
                    baselinePushedOffset,
                    committedSourceEnd,
                    followerLocalStart,
                    replayGate.admittedSequence(),
                    remoteBytesBefore,
                    remoteBytesWhileAckHeld,
                    continuationEnd);
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (replayGate != null) {
                replayGate.release();
            }
            ReplayGate finalReplayGate = replayGate;
            cleanup(cleanupFailures, () -> closeGate(finalReplayGate));
            for (int stoppedServer : new LinkedHashSet<>(stoppedServers)) {
                cleanup(cleanupFailures, () -> startAndUntrack(stoppedServer, stoppedServers));
            }
            cleanup(
                    cleanupFailures,
                    () -> CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT));
            reportCleanupFailures(primaryFailure, cleanupFailures);
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
        conf.set(ConfigOptions.INDEX_REPLICATION_BACKOFF_INTERVAL, Duration.ofMillis(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofMillis(5));
        return conf;
    }

    private static Fixture createFixture() throws Exception {
        String tableName = "source_remote_recovery_" + System.nanoTime();
        TablePath mainPath = TablePath.of("task7", tableName);
        TablePath indexPath =
                TablePath.of("task7", IndexTableUtils.indexTableName(tableName, INDEX_NAME));
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
        long indexTableId =
                CLUSTER.getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(() -> new AssertionError("Index Table missing from ZK"))
                        .tableId;
        TableBucket sourceTableBucket = new TableBucket(mainTableId, 0);
        CLUSTER.waitUntilAllReplicaReady(sourceTableBucket);
        Replica sourceReplica = CLUSTER.waitAndGetLeaderReplica(sourceTableBucket);
        waitUntil(
                () -> sourceReplica.getIsr().size() == REPLICATION_FACTOR,
                TIMEOUT,
                "wait for source RF3 ISR");
        waitUntil(
                () -> sourceReplica.getIndexReplicator() != null,
                TIMEOUT,
                "wait for initial source IndexReplicator");
        int sourceLeader = CLUSTER.waitAndGetLeader(sourceTableBucket);

        TableAssignment sourceAssignment =
                CLUSTER.getZooKeeperClient()
                        .getTableAssignment(mainTableId)
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

        String firstIndexValue = valueForIndexBucket("remote-old", targetBucket);
        String secondIndexValue = valueForIndexBucket("remote-new", targetBucket);
        assertThat(firstIndexValue).isNotEqualTo(secondIndexValue);
        assertThat(indexBucket(firstIndexValue)).isEqualTo(targetBucket);
        assertThat(indexBucket(secondIndexValue)).isEqualTo(targetBucket);
        TableBucket gatedTargetBucket = new TableBucket(indexTableId, targetBucket);
        assertThat(CLUSTER.waitAndGetLeader(gatedTargetBucket)).isEqualTo(targetOnlyServer);

        LOG.info(
                "Task 7 topology: source={} assignment={}, sourceLeader={}, recoveryFollower={}, "
                        + "offlineFollower={}, targetOnly={}, indexLeaders={}, targetBucket={}, "
                        + "values=[{}, {}]",
                sourceTableBucket,
                sourceReplicas,
                sourceLeader,
                recoveryFollower,
                offlineFollower,
                targetOnlyServer,
                indexLeaders,
                gatedTargetBucket,
                firstIndexValue,
                secondIndexValue);
        return new Fixture(
                mainTableId,
                indexTableId,
                sourceTableBucket,
                sourceLeader,
                recoveryFollower,
                offlineFollower,
                targetOnlyServer,
                gatedTargetBucket,
                firstIndexValue,
                secondIndexValue,
                sourceReplica);
    }

    private void putSourceRow(int sourceLeader, int key, String indexedValue) throws Exception {
        TabletServerGateway gateway = CLUSTER.newTabletServerClientForNode(sourceLeader);
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                                fixture.mainTableId,
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

    private void waitForExactPhysicalRows(Map<Integer, String> expectedRows) throws Exception {
        int schemaId = liveIndexTableInfo().getSchemaId();
        for (Map.Entry<Integer, String> expectedRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(expectedRow.getKey(), expectedRow.getValue(), schemaId);
            TableBucket target =
                    new TableBucket(fixture.indexTableId, indexBucket(expectedRow.getValue()));
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

    private void assertExactIndexProjection(Map<Integer, String> expectedRows) throws Exception {
        int schemaId = liveIndexTableInfo().getSchemaId();
        Map<TableBucket, Map<String, String>> expected = emptyIndexProjection();
        for (Map.Entry<Integer, String> sourceRow : expectedRows.entrySet()) {
            PhysicalIndexRow physical =
                    physicalIndexRow(sourceRow.getKey(), sourceRow.getValue(), schemaId);
            TableBucket bucket =
                    new TableBucket(fixture.indexTableId, indexBucket(sourceRow.getValue()));
            String previous =
                    expected.get(bucket).put(encode(physical.key), encode(physical.value));
            assertThat(previous).as("physical index keys must be unique").isNull();
        }

        Map<TableBucket, Map<String, String>> actual = emptyIndexProjection();
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(fixture.indexTableId, bucket);
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

    private void assertIndexKeyAbsent(String indexedValue, int primaryKey, String description)
            throws Exception {
        PhysicalIndexRow physical =
                physicalIndexRow(primaryKey, indexedValue, liveIndexTableInfo().getSchemaId());
        TableBucket target = new TableBucket(fixture.indexTableId, indexBucket(indexedValue));
        assertThat(
                        CLUSTER.waitAndGetLeaderReplica(target)
                                .lookups(Collections.singletonList(physical.key))
                                .get(0))
                .as(description)
                .isNull();
    }

    private TableInfo liveIndexTableInfo() {
        return CLUSTER.waitAndGetLeaderReplica(new TableBucket(fixture.indexTableId, 0))
                .getTableInfo();
    }

    private Map<TableBucket, Map<String, String>> emptyIndexProjection() {
        Map<TableBucket, Map<String, String>> projection = new LinkedHashMap<>();
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            projection.put(new TableBucket(fixture.indexTableId, bucket), new LinkedHashMap<>());
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
        private final long baselineSequence;
        private final AtomicLong admittedSequence = new AtomicLong(-1L);
        private final CountDownLatch replayAdmitted = new CountDownLatch(1);
        private final CountDownLatch releaseReplay = new CountDownLatch(1);
        private final AutoCloseable registration;

        private ReplayGate(Replica targetReplica, WriterKey writerKey, long baselineSequence) {
            this.writerKey = writerKey;
            this.baselineSequence = baselineSequence;
            this.registration =
                    ReplicaTestHooks.installAfterPutAdmissionHook(
                            targetReplica, this::afterAdmission);
        }

        private void afterAdmission(KvRecordBatch batch) {
            if (!writerKey.equals(batch.fencedWriterKey())
                    || batch.fencedSequence() <= baselineSequence) {
                return;
            }
            admittedSequence.accumulateAndGet(batch.fencedSequence(), Math::max);
            replayAdmitted.countDown();
            await(releaseReplay, "wait to release remote source replay ACK");
        }

        private void awaitAdmission() {
            await(replayAdmitted, "wait for canonical remote source replay admission");
            assertThat(admittedSequence.get()).isGreaterThan(baselineSequence);
        }

        private long admittedSequence() {
            return admittedSequence.get();
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

    private static final class Fixture {
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

        private Fixture(
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
    }
}
