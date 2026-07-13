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
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestHooks;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Adversarial ordering across source replay, target fencing, and both leader failovers. */
class IndexPushOrderingITCase {

    private static final int TABLET_SERVER_COUNT = 5;
    private static final int REPLICATION_FACTOR = 3;
    private static final int MAIN_BUCKET_COUNT = 5;
    private static final int INDEX_BUCKET_COUNT = 20;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static final RowType MAIN_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("a", DataTypes.INT().copy(false)));
    private static final RowType INDEX_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("b", DataTypes.STRING().copy(false)),
                    new DataField("a", DataTypes.INT().copy(false)));
    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));
    private static final Schema INDEX_SCHEMA =
            Schema.newBuilder().fromRowType(INDEX_ROW_TYPE).build();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(configuration())
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.INDEX_REPLICATION_BACKOFF_INTERVAL, Duration.ofMillis(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofMillis(5));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_MAX_BACKOFF, Duration.ofMillis(20));
        conf.set(ConfigOptions.INDEX_REPLICATION_MAX_WINDOW_BYTES, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.INDEX_REPLICATION_MAX_REQUEST_BYTES, MemorySize.parse("1mb"));
        return conf;
    }

    @Test
    void oldSourceRequestsCannotOutrunNewLeaderReplayAcrossTargetFailover() throws Exception {
        Fixture fixture = createFixture();
        ExecutorService failoverExecutor = Executors.newFixedThreadPool(2);
        List<CompletableFuture<PutKvResponse>> pendingSourceResponses = new ArrayList<>();
        CompletableFuture<Void> sourceStop = null;
        CompletableFuture<Void> targetStop = null;
        SequenceGate oldKeyGate = null;
        SequenceGate newKeyGate = null;
        boolean sourceRestarted = false;
        boolean targetRestarted = false;
        Throwable primaryFailure = null;

        try {
            List<Integer> primaryKeys = primaryKeysForBucket(fixture.sourceBucket, 4);
            int upsertDeletePk = primaryKeys.get(0);
            int deleteUpsertPk = primaryKeys.get(1);
            int threeStepPk = primaryKeys.get(2);
            int movementPk = primaryKeys.get(3);

            String upsertDeleteValue = valueForBucketPrefix("upsert-delete", -1);
            String deleteUpsertValue = valueForBucketPrefix("delete-upsert", -1);
            String threeStepValue = valueForBucketPrefix("three-step", -1);
            String movementOldValue =
                    valueForBucketPrefix("movement-old", fixture.failoverTarget.bucket);
            String movementNewValue =
                    valueForBucketPrefix("movement-new", fixture.otherTarget.bucket);

            putSourceAndWait(
                    fixture, SourceMutation.upsert(deleteUpsertPk, deleteUpsertValue), true);
            putSourceAndWait(fixture, SourceMutation.upsert(movementPk, movementOldValue), true);

            long snapshotOffset = fixture.sourceReplica.getSyncIndexPushedOffset();
            assertThat(snapshotOffset)
                    .as("baseline writes must establish the conservative replay point")
                    .isEqualTo(fixture.sourceReplica.getAllIndexPushedOffset())
                    .isEqualTo(fixture.sourceReplica.getLocalLogEndOffset());
            CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(fixture.sourceTableBucket);
            assertThat(snapshot.getIndexPushedOffset())
                    .as("source failover must start from this exact persisted offset")
                    .isEqualTo(snapshotOffset);

            WriterKey writerKey = IndexWriterKey.encode(fixture.sourceTableBucket);
            oldKeyGate =
                    new SequenceGate(
                            fixture.failoverTarget.replica,
                            writerKey,
                            physicalRow(movementOldValue, movementPk),
                            true);
            newKeyGate =
                    new SequenceGate(
                            fixture.otherTarget.replica,
                            writerKey,
                            physicalRow(movementNewValue, movementPk),
                            false);

            SourceMutation movement = SourceMutation.upsert(movementPk, movementNewValue);
            pendingSourceResponses.add(putSourceAndWait(fixture, movement, false));
            oldKeyGate.awaitOldAdmission();
            newKeyGate.awaitOldAdmission();
            long oldWindowEnd = oldKeyGate.oldSequence();
            assertThat(newKeyGate.oldSequence())
                    .as("all target batches derived from one source window share its end offset")
                    .isEqualTo(oldWindowEnd);
            assertThat(oldWindowEnd).isGreaterThan(snapshotOffset);

            pendingSourceResponses.add(
                    putSourceAndWait(
                            fixture,
                            SourceMutation.upsert(upsertDeletePk, upsertDeleteValue),
                            false));
            pendingSourceResponses.add(
                    putSourceAndWait(fixture, SourceMutation.delete(upsertDeletePk), false));

            pendingSourceResponses.add(
                    putSourceAndWait(fixture, SourceMutation.delete(deleteUpsertPk), false));
            pendingSourceResponses.add(
                    putSourceAndWait(
                            fixture,
                            SourceMutation.upsert(deleteUpsertPk, deleteUpsertValue),
                            false));

            pendingSourceResponses.add(
                    putSourceAndWait(
                            fixture, SourceMutation.upsert(threeStepPk, threeStepValue), false));
            pendingSourceResponses.add(
                    putSourceAndWait(fixture, SourceMutation.delete(threeStepPk), false));
            pendingSourceResponses.add(
                    putSourceAndWait(
                            fixture, SourceMutation.upsert(threeStepPk, threeStepValue), false));

            waitUntil(
                    () ->
                            fixture.sourceReplica.getLogTablet().getHighWatermark()
                                    == fixture.sourceReplica.getLocalLogEndOffset(),
                    TIMEOUT,
                    "wait for every submitted source mutation to become committed WAL");
            long committedSourceEnd = fixture.sourceReplica.getLocalLogEndOffset();
            assertThat(committedSourceEnd)
                    .as("the replayed new-leader window must dominate the held old window")
                    .isGreaterThan(oldWindowEnd);

            int oldSourceLeader = fixture.sourceLeader;
            sourceStop =
                    CompletableFuture.runAsync(
                            () -> stopTabletServer(oldSourceLeader), failoverExecutor);
            waitUntil(
                    () -> currentLeader(fixture.sourceTableBucket) != oldSourceLeader,
                    TIMEOUT,
                    "wait for old source leader to lose leadership");
            oldKeyGate.releaseOld();
            newKeyGate.releaseOld();
            oldKeyGate.awaitOldAbandoned();
            newKeyGate.awaitOldAbandoned();
            sourceStop.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Replica newSourceReplica = CLUSTER.waitAndGetLeaderReplica(fixture.sourceTableBucket);
            assertThat(currentLeader(fixture.sourceTableBucket)).isNotEqualTo(oldSourceLeader);
            waitUntil(
                    () -> newSourceReplica.getIndexReplicator() != null,
                    TIMEOUT,
                    "wait for new source IndexReplicator");

            oldKeyGate.awaitNewAdmission();
            newKeyGate.awaitNewAdmission();
            assertThat(newSourceReplica.getSyncIndexPushedOffset())
                    .as("new source leader must restore the exact conservative snapshot offset")
                    .isEqualTo(snapshotOffset);
            assertThat(newSourceReplica.getAllIndexPushedOffset()).isEqualTo(snapshotOffset);
            assertThat(oldKeyGate.newSequence())
                    .as("new leader must choose a larger replay window than the held old leader")
                    .isGreaterThan(oldWindowEnd)
                    .isEqualTo(newKeyGate.newSequence())
                    .isEqualTo(committedSourceEnd);

            newKeyGate.releaseNew();
            oldKeyGate.releaseNew();
            waitUntil(
                    () -> newSourceReplica.getSyncIndexPushedOffset() == committedSourceEnd,
                    TIMEOUT,
                    "wait for the dominating source replay window");
            assertThat(newSourceReplica.getAllIndexPushedOffset()).isEqualTo(committedSourceEnd);

            FencedWriterStateEntry dominatingEntry =
                    waitForFence(fixture.failoverTarget.tableBucket, writerKey, committedSourceEnd);
            FencedWriterStateEntry otherDominatingEntry =
                    waitForFence(fixture.otherTarget.tableBucket, writerKey, committedSourceEnd);
            Replica targetBeforeFailover =
                    CLUSTER.waitAndGetLeaderReplica(fixture.failoverTarget.tableBucket);
            assertThat(targetBeforeFailover.getLogTablet().getHighWatermark())
                    .as("the dominating target WAL record must be committed before failover")
                    .isGreaterThan(dominatingEntry.dominatingTargetWalOffset());

            byte[] lostResponseRetry = oldKeyGate.dominatingBatch();
            long walBeforeLostResponseRetry = targetBeforeFailover.getLocalLogEndOffset();
            TabletServer targetServerBeforeFailover =
                    CLUSTER.getTabletServerById(
                            CLUSTER.waitAndGetLeader(fixture.failoverTarget.tableBucket));
            long staleBeforeLostResponseRetry = staleCount(targetServerBeforeFailover);
            sendCapturedBatch(fixture, fixture.failoverTarget.bucket, lostResponseRetry);
            assertThat(staleCount(targetServerBeforeFailover))
                    .as("a lost-response retry must be observed as one duplicate fence")
                    .isEqualTo(staleBeforeLostResponseRetry + 1L);
            assertThat(targetBeforeFailover.getLocalLogEndOffset())
                    .as("an identical successful retry must append no target WAL")
                    .isEqualTo(walBeforeLostResponseRetry);
            assertThat(currentFence(fixture.failoverTarget.tableBucket, writerKey))
                    .isEqualTo(dominatingEntry);

            int oldTargetLeader = fixture.failoverTarget.leader;
            targetStop =
                    CompletableFuture.runAsync(
                            () -> stopTabletServer(oldTargetLeader), failoverExecutor);
            waitUntil(
                    () -> currentLeader(fixture.failoverTarget.tableBucket) != oldTargetLeader,
                    TIMEOUT,
                    "wait for target leader failover");
            FencedWriterStateEntry recoveredEntry =
                    waitForFence(fixture.failoverTarget.tableBucket, writerKey, committedSourceEnd);
            assertThat(recoveredEntry)
                    .as("new target leader must recover the exact committed dominating fence")
                    .isEqualTo(dominatingEntry);
            Replica recoveredTarget =
                    CLUSTER.waitAndGetLeaderReplica(fixture.failoverTarget.tableBucket);
            assertThat(recoveredTarget.getLogTablet().getHighWatermark())
                    .isGreaterThan(recoveredEntry.dominatingTargetWalOffset());
            targetStop.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(currentFence(fixture.failoverTarget.tableBucket, writerKey))
                    .isEqualTo(recoveredEntry);

            Map<TableBucket, Long> walBeforeStaleDelivery =
                    currentWalEnds(fixture.failoverTarget, fixture.otherTarget);
            byte[] delayedOldKeyDelete = oldKeyGate.capturedBatch();
            byte[] delayedNewKeyUpsert = newKeyGate.capturedBatch();
            newKeyGate.close();
            newKeyGate = null;
            oldKeyGate.close();
            oldKeyGate = null;
            TabletServer currentOtherTarget =
                    CLUSTER.getTabletServerById(
                            CLUSTER.waitAndGetLeader(fixture.otherTarget.tableBucket));
            TabletServer currentFailoverTarget =
                    CLUSTER.getTabletServerById(
                            CLUSTER.waitAndGetLeader(fixture.failoverTarget.tableBucket));
            Set<TabletServer> staleMetricServers =
                    new LinkedHashSet<>(Arrays.asList(currentOtherTarget, currentFailoverTarget));
            long staleBefore = totalStaleCount(staleMetricServers);

            // Target state has no RPC-identity input. Re-delivering the checksum-equivalent batch
            // models an already-sent old request whose transport cancellation loses the race.
            sendCapturedBatch(fixture, fixture.otherTarget.bucket, delayedNewKeyUpsert);
            assertThat(totalStaleCount(staleMetricServers)).isEqualTo(staleBefore + 1L);
            assertStaleDeliveryHasNoSideEffects(
                    fixture,
                    writerKey,
                    walBeforeStaleDelivery,
                    recoveredEntry,
                    otherDominatingEntry);
            sendCapturedBatch(fixture, fixture.failoverTarget.bucket, delayedOldKeyDelete);
            assertThat(totalStaleCount(staleMetricServers)).isEqualTo(staleBefore + 2L);
            assertStaleDeliveryHasNoSideEffects(
                    fixture,
                    writerKey,
                    walBeforeStaleDelivery,
                    recoveredEntry,
                    otherDominatingEntry);

            assertIndexEqualsCommittedSourceWal(fixture, committedSourceEnd);

            CLUSTER.startTabletServer(oldSourceLeader);
            sourceRestarted = true;
            CLUSTER.startTabletServer(oldTargetLeader);
            targetRestarted = true;
            CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT);
        } catch (Exception | AssertionError failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (newKeyGate != null) {
                newKeyGate.releaseAll();
            }
            if (oldKeyGate != null) {
                oldKeyGate.releaseAll();
            }
            SequenceGate finalNewKeyGate = newKeyGate;
            cleanup(cleanupFailures, () -> closeGate(finalNewKeyGate));
            SequenceGate finalOldKeyGate = oldKeyGate;
            cleanup(cleanupFailures, () -> closeGate(finalOldKeyGate));
            for (CompletableFuture<PutKvResponse> response : pendingSourceResponses) {
                response.cancel(true);
            }
            CompletableFuture<Void> finalSourceStop = sourceStop;
            cleanup(cleanupFailures, () -> awaitStop(finalSourceStop));
            CompletableFuture<Void> finalTargetStop = targetStop;
            cleanup(cleanupFailures, () -> awaitStop(finalTargetStop));
            if (!sourceRestarted && CLUSTER.getTabletServerById(fixture.sourceLeader) == null) {
                cleanup(cleanupFailures, () -> CLUSTER.startTabletServer(fixture.sourceLeader));
            }
            if (!targetRestarted
                    && CLUSTER.getTabletServerById(fixture.failoverTarget.leader) == null) {
                cleanup(
                        cleanupFailures,
                        () -> CLUSTER.startTabletServer(fixture.failoverTarget.leader));
            }
            failoverExecutor.shutdownNow();
            cleanup(
                    cleanupFailures,
                    () -> {
                        if (!failoverExecutor.awaitTermination(
                                TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                            throw new AssertionError("failover executor did not terminate");
                        }
                    });
            reportCleanupFailures(primaryFailure, cleanupFailures);
        }
    }

    private static Fixture createFixture() throws Exception {
        String tableName = "ordering_" + System.nanoTime();
        TablePath mainPath = TablePath.of("task12", tableName);
        TablePath indexPath =
                TablePath.of("task12", IndexTableUtils.indexTableName(tableName, "idx_b"));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                "idx_b",
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                INDEX_BUCKET_COUNT)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(MAIN_BUCKET_COUNT, "a")
                                .build());
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;

        List<Target> targets = new ArrayList<>(INDEX_BUCKET_COUNT);
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket tableBucket = new TableBucket(indexTableId, bucket);
            Replica replica = CLUSTER.waitAndGetLeaderReplica(tableBucket);
            waitUntil(
                    () -> replica.getIsr().size() == REPLICATION_FACTOR,
                    TIMEOUT,
                    "wait for index target ISR " + tableBucket);
            targets.add(
                    new Target(
                            bucket, tableBucket, CLUSTER.waitAndGetLeader(tableBucket), replica));
        }

        for (int sourceBucket = 0; sourceBucket < MAIN_BUCKET_COUNT; sourceBucket++) {
            TableBucket sourceTableBucket = new TableBucket(mainTableId, sourceBucket);
            Replica sourceReplica = CLUSTER.waitAndGetLeaderReplica(sourceTableBucket);
            waitUntil(
                    () -> sourceReplica.getIsr().size() == REPLICATION_FACTOR,
                    TIMEOUT,
                    "wait for source ISR " + sourceTableBucket);
            int sourceLeader = CLUSTER.waitAndGetLeader(sourceTableBucket);
            Set<Integer> sourceIsr = new LinkedHashSet<>(sourceReplica.getIsr());

            for (Target failoverTarget : targets) {
                if (sourceIsr.contains(failoverTarget.leader)
                        || failoverTarget.replica.getIsr().contains(sourceLeader)) {
                    continue;
                }
                Optional<Target> otherTarget =
                        targets.stream()
                                .filter(target -> target.bucket != failoverTarget.bucket)
                                .filter(target -> target.leader != sourceLeader)
                                .filter(target -> target.leader != failoverTarget.leader)
                                .findFirst();
                if (otherTarget.isPresent()) {
                    waitUntil(
                            () -> sourceReplica.getIndexReplicator() != null,
                            TIMEOUT,
                            "wait for source IndexReplicator");
                    return new Fixture(
                            mainTableId,
                            indexTableId,
                            sourceBucket,
                            sourceTableBucket,
                            sourceLeader,
                            sourceReplica,
                            failoverTarget,
                            otherTarget.get());
                }
            }
        }
        throw new AssertionError(
                "unable to select independent source and target failover topology: " + targets);
    }

    private static CompletableFuture<PutKvResponse> putSourceAndWait(
            Fixture fixture, SourceMutation mutation, boolean awaitResponse) throws Exception {
        long previousEnd = fixture.sourceReplica.getLocalLogEndOffset();
        KvRecordBatch batch =
                mutation.value == null
                        ? genKvRecordBatch(
                                Collections.singletonList(
                                        Tuple2.of(new Object[] {mutation.primaryKey}, null)))
                        : genKvRecordBatch(new Object[] {mutation.primaryKey, mutation.value});
        PutKvRequest request =
                newPutKvRequest(fixture.mainTableId, fixture.sourceBucket, 1, batch)
                        .setTimeoutMs((int) TIMEOUT.toMillis());
        CompletableFuture<PutKvResponse> response =
                CLUSTER.newTabletServerClientForNode(fixture.sourceLeader).putKv(request);
        waitUntil(
                () -> fixture.sourceReplica.getLocalLogEndOffset() > previousEnd,
                TIMEOUT,
                "wait for ordered source WAL append " + mutation);
        if (awaitResponse) {
            assertSuccess(response.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        }
        return response;
    }

    private static CapturedBatch copyBatch(
            KvRecordBatch batch, PhysicalRow expectedRow, boolean delete) throws IOException {
        try (FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        batch.schemaId(),
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(1024),
                        KvFormat.COMPACTED)) {
            boolean containsExpectedMutation = false;
            KvRecordReadContext readContext =
                    KvRecordReadContext.createReadContext(
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(batch.schemaId(), INDEX_SCHEMA));
            for (KvRecord record : batch.records(readContext)) {
                ByteBuffer keyBuffer = record.getKey().duplicate();
                byte[] key = new byte[keyBuffer.remaining()];
                keyBuffer.get(key);
                builder.append(key, record.getRow());
                if (Arrays.equals(key, expectedRow.key) && delete == (record.getRow() == null)) {
                    containsExpectedMutation = true;
                }
            }
            builder.setWriterState(batch.fencedWriterKey(), batch.fencedSequence());
            BytesView encoded = builder.build();
            byte[] bytes = new byte[encoded.getBytesLength()];
            encoded.getByteBuf().getBytes(encoded.getByteBuf().readerIndex(), bytes);
            return new CapturedBatch(bytes, containsExpectedMutation);
        }
    }

    private static void sendCapturedBatch(Fixture fixture, int targetBucket, byte[] bytes)
            throws Exception {
        PutKvRequest request =
                new PutKvRequest()
                        .setTableId(fixture.indexTableId)
                        .setAcks(-1)
                        .setTimeoutMs((int) TIMEOUT.toMillis());
        request.setAggMode(MergeMode.OVERWRITE.getProtoValue());
        PbPutKvReqForBucket bucketRequest = request.addBucketsReq().setBucketId(targetBucket);
        bucketRequest.setRecordsBytesView(
                new org.apache.fluss.record.bytesview.MemorySegmentBytesView(
                        org.apache.fluss.memory.MemorySegment.wrap(bytes), 0, bytes.length));
        TabletServerGateway gateway =
                CLUSTER.newTabletServerClientForNode(
                        CLUSTER.waitAndGetLeader(
                                new TableBucket(fixture.indexTableId, targetBucket)));
        assertSuccess(gateway.putKv(request).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    }

    private static FencedWriterStateEntry waitForFence(
            TableBucket target, WriterKey writerKey, long expectedSequence) {
        AtomicLong observedSequence = new AtomicLong(-1L);
        waitUntil(
                () -> {
                    Optional<FencedWriterStateEntry> entry =
                            CLUSTER.waitAndGetLeaderReplica(target)
                                    .getLogTablet()
                                    .writerStateManager()
                                    .lastFencedEntry(writerKey);
                    entry.ifPresent(value -> observedSequence.set(value.lastSequence()));
                    return entry.map(value -> value.lastSequence() == expectedSequence)
                            .orElse(false);
                },
                TIMEOUT,
                "wait for target fence " + expectedSequence + ", observed=" + observedSequence);
        return currentFence(target, writerKey);
    }

    private static FencedWriterStateEntry currentFence(TableBucket target, WriterKey writerKey) {
        return CLUSTER.waitAndGetLeaderReplica(target)
                .getLogTablet()
                .writerStateManager()
                .lastFencedEntry(writerKey)
                .orElseThrow(AssertionError::new);
    }

    private static Map<TableBucket, Long> currentWalEnds(Target... targets) {
        Map<TableBucket, Long> result = new LinkedHashMap<>();
        for (Target target : targets) {
            result.put(
                    target.tableBucket,
                    CLUSTER.waitAndGetLeaderReplica(target.tableBucket).getLocalLogEndOffset());
        }
        return result;
    }

    private static void assertStaleDeliveryHasNoSideEffects(
            Fixture fixture,
            WriterKey writerKey,
            Map<TableBucket, Long> expectedWalEnds,
            FencedWriterStateEntry expectedFailoverTargetEntry,
            FencedWriterStateEntry expectedOtherTargetEntry) {
        assertThat(currentWalEnds(fixture.failoverTarget, fixture.otherTarget))
                .as("released original old-source requests must append no target WAL")
                .isEqualTo(expectedWalEnds);
        assertThat(currentFence(fixture.failoverTarget.tableBucket, writerKey))
                .isEqualTo(expectedFailoverTargetEntry);
        assertThat(currentFence(fixture.otherTarget.tableBucket, writerKey))
                .isEqualTo(expectedOtherTargetEntry);
    }

    private static void assertIndexEqualsCommittedSourceWal(
            Fixture fixture, long committedSourceEnd) throws Exception {
        Map<Integer, String> sourceRows = decodeCommittedSourceWal(fixture, committedSourceEnd);
        Map<TableBucket, Map<String, String>> expected = emptyIndexProjection(fixture.indexTableId);
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(INDEX_ROW_TYPE);
        try (RowEncoder valueEncoder = RowEncoder.create(KvFormat.COMPACTED, INDEX_ROW_TYPE)) {
            for (Map.Entry<Integer, String> sourceRow : sourceRows.entrySet()) {
                valueEncoder.startNewRow();
                valueEncoder.encodeField(0, fromString(sourceRow.getValue()));
                valueEncoder.encodeField(1, sourceRow.getKey());
                BinaryRow indexRow = valueEncoder.finishRow();
                byte[] key = keyEncoder.encodeKey(indexRow);
                byte[] value = ValueEncoder.encodeValue((short) 1, indexRow);
                TableBucket bucket =
                        new TableBucket(fixture.indexTableId, indexBucket(sourceRow.getValue()));
                String previous = expected.get(bucket).put(encode(key), encode(value));
                assertThat(previous).as("physical index keys are unique").isNull();
            }
        }

        Map<TableBucket, Map<String, String>> actual = emptyIndexProjection(fixture.indexTableId);
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
                .as(
                        "all physical Index Table key/value rows must exactly project committed source WAL [0,%s)",
                        committedSourceEnd)
                .isEqualTo(expected);
    }

    private static Map<Integer, String> decodeCommittedSourceWal(
            Fixture fixture, long committedSourceEnd) throws Exception {
        Replica source = CLUSTER.waitAndGetLeaderReplica(fixture.sourceTableBucket);
        FetchDataInfo fetch =
                source.getLogTablet()
                        .read(0L, Integer.MAX_VALUE, FetchIsolation.HIGH_WATERMARK, true);
        Map<Integer, String> sourceRows = new LinkedHashMap<>();
        long nextBatchOffset = 0L;
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(
                        source.getTableInfo(),
                        false,
                        null,
                        new TestingSchemaGetter(
                                source.getTableInfo().getSchemaId(),
                                source.getTableInfo().getSchema()))) {
            for (LogRecordBatch batch : fetch.getRecords().batches()) {
                assertThat(batch.baseLogOffset()).isEqualTo(nextBatchOffset);
                assertThat(batch.nextLogOffset()).isLessThanOrEqualTo(committedSourceEnd);
                try (CloseableIterator<LogRecord> records = batch.records(readContext)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isLessThan(committedSourceEnd);
                        int primaryKey = record.getRow().getInt(0);
                        ChangeType changeType = record.getChangeType();
                        if (changeType == ChangeType.DELETE
                                || changeType == ChangeType.UPDATE_BEFORE) {
                            sourceRows.remove(primaryKey);
                        } else {
                            sourceRows.put(primaryKey, record.getRow().getString(1).toString());
                        }
                    }
                }
                nextBatchOffset = batch.nextLogOffset();
            }
        }
        assertThat(nextBatchOffset)
                .as("decoded committed source WAL coverage")
                .isEqualTo(committedSourceEnd);
        return sourceRows;
    }

    private static Map<TableBucket, Map<String, String>> emptyIndexProjection(long indexTableId) {
        Map<TableBucket, Map<String, String>> projection = new LinkedHashMap<>();
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            projection.put(new TableBucket(indexTableId, bucket), new LinkedHashMap<>());
        }
        return projection;
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static List<Integer> primaryKeysForBucket(int bucket, int count) {
        List<Integer> keys = new ArrayList<>(count);
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(MAIN_BUCKET_KEY_TYPE);
        for (int candidate = 0; keys.size() < count; candidate++) {
            GenericRow row = new GenericRow(1);
            row.setField(0, candidate);
            if (new FlussBucketingFunction().bucketing(encoder.encodeKey(row), MAIN_BUCKET_COUNT)
                    == bucket) {
                keys.add(candidate);
            }
        }
        return keys;
    }

    private static String valueForBucketPrefix(String prefix, int requiredBucket) {
        for (int suffix = 0; suffix < 10_000; suffix++) {
            String value = prefix + '-' + suffix;
            if (requiredBucket < 0 || indexBucket(value) == requiredBucket) {
                return value;
            }
        }
        throw new AssertionError("unable to find index value for bucket " + requiredBucket);
    }

    private static int indexBucket(String indexedValue) {
        GenericRow bucketKey = new GenericRow(1);
        bucketKey.setField(0, fromString(indexedValue));
        return new FlussBucketingFunction()
                .bucketing(
                        new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE).encodeKey(bucketKey),
                        INDEX_BUCKET_COUNT);
    }

    private static PhysicalRow physicalRow(String indexedValue, int primaryKey) {
        GenericRow value = new GenericRow(2);
        value.setField(0, fromString(indexedValue));
        value.setField(1, primaryKey);
        byte[] key = new CompactedKeyEncoder(INDEX_ROW_TYPE).encodeKey(value);
        return new PhysicalRow(key);
    }

    private static int currentLeader(TableBucket tableBucket) {
        return CLUSTER.waitAndGetLeader(tableBucket);
    }

    private static long staleCount(TabletServer tabletServer) {
        return tabletServer
                .getReplicaManager()
                .getServerMetricGroup()
                .indexPushStaleV1Batches()
                .getCount();
    }

    private static long totalStaleCount(Set<TabletServer> tabletServers) {
        long total = 0L;
        for (TabletServer tabletServer : tabletServers) {
            total += staleCount(tabletServer);
        }
        return total;
    }

    private static void stopTabletServer(int serverId) {
        try {
            CLUSTER.stopTabletServer(serverId);
        } catch (Exception e) {
            throw new AssertionError("failed to stop tablet server " + serverId, e);
        }
    }

    private static void closeGate(@Nullable SequenceGate gate) throws Exception {
        if (gate != null) {
            gate.close();
        }
    }

    private static void awaitStop(@Nullable CompletableFuture<Void> future) throws Exception {
        if (future == null) {
            return;
        }
        future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
        AssertionError cleanupFailure = new AssertionError("ordering IT cleanup failed");
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

    private static void assertSuccess(PutKvResponse response) {
        assertThat(response.getBucketsRespsList())
                .allSatisfy(bucket -> assertThat(bucket.hasErrorCode()).isFalse());
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Exception;
    }

    private static final class SequenceGate implements AutoCloseable {
        private final WriterKey writerKey;
        private final PhysicalRow expectedRow;
        private final boolean delete;
        private final AtomicLong oldSequence = new AtomicLong(-1L);
        private final AtomicLong newSequence = new AtomicLong(-1L);
        private final CountDownLatch oldAdmitted = new CountDownLatch(1);
        private final CountDownLatch oldAbandoned = new CountDownLatch(1);
        private final CountDownLatch newAdmitted = new CountDownLatch(1);
        private final CountDownLatch releaseOld = new CountDownLatch(1);
        private final CountDownLatch releaseNew = new CountDownLatch(1);
        private final AtomicReference<byte[]> capturedBatch = new AtomicReference<>();
        private final AtomicReference<byte[]> dominatingBatch = new AtomicReference<>();
        private final AtomicReference<String> captureFailure = new AtomicReference<>();
        private final AutoCloseable registration;

        private SequenceGate(
                Replica replica, WriterKey writerKey, PhysicalRow expectedRow, boolean delete) {
            this.writerKey = writerKey;
            this.expectedRow = expectedRow;
            this.delete = delete;
            this.registration =
                    ReplicaTestHooks.installAfterPutAdmissionHook(replica, this::afterAdmission);
        }

        private void afterAdmission(KvRecordBatch batch) {
            if (!writerKey.equals(batch.fencedWriterKey())) {
                return;
            }
            long sequence = batch.fencedSequence();
            oldSequence.compareAndSet(-1L, sequence);
            if (sequence == oldSequence.get()) {
                byte[] captured = captureEquivalentBatch(batch);
                if (captured == null) {
                    oldAdmitted.countDown();
                    throw AbandonedOldRequest.INSTANCE;
                }
                if (capturedBatch.compareAndSet(null, captured)) {
                    oldAdmitted.countDown();
                    abandonAfterCancellation();
                }
                throw AbandonedOldRequest.INSTANCE;
            } else if (sequence > oldSequence.get()) {
                byte[] captured = captureEquivalentBatch(batch);
                if (captured == null) {
                    newAdmitted.countDown();
                    throw AbandonedOldRequest.INSTANCE;
                }
                dominatingBatch.compareAndSet(null, captured);
                newSequence.accumulateAndGet(sequence, Math::max);
                newAdmitted.countDown();
                await(releaseNew, "wait to release new source-leader replay");
            }
        }

        @Nullable
        private byte[] captureEquivalentBatch(KvRecordBatch batch) {
            CapturedBatch captured;
            try {
                captured = copyBatch(batch, expectedRow, delete);
            } catch (Exception e) {
                captureFailure.compareAndSet(
                        null, "failed to copy admitted source batch: " + e.getMessage());
                return null;
            }
            KvRecordBatch reconstructed =
                    KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(captured.bytes));
            if (!captured.containsExpectedMutation) {
                captureFailure.compareAndSet(
                        null, "admitted source batch does not contain expected mutation");
            } else if (reconstructed.magic() != batch.magic()
                    || reconstructed.schemaId() != batch.schemaId()
                    || !reconstructed.fencedWriterKey().equals(batch.fencedWriterKey())
                    || reconstructed.fencedSequence() != batch.fencedSequence()
                    || reconstructed.sizeInBytes() != batch.sizeInBytes()
                    || reconstructed.checksum() != batch.checksum()
                    || reconstructed.getRecordCount() != batch.getRecordCount()) {
                captureFailure.compareAndSet(
                        null,
                        "rebuilt batch does not match admitted source batch: size="
                                + reconstructed.sizeInBytes()
                                + '/'
                                + batch.sizeInBytes()
                                + ", checksum="
                                + reconstructed.checksum()
                                + '/'
                                + batch.checksum());
            }
            return captured.bytes;
        }

        private void abandonAfterCancellation() {
            try {
                releaseOld.await();
            } catch (InterruptedException ignored) {
                // Source shutdown may cancel the transport before the test releases the gate.
            } finally {
                oldAbandoned.countDown();
            }
            throw AbandonedOldRequest.INSTANCE;
        }

        private void awaitOldAdmission() {
            await(oldAdmitted, "wait for old source-leader target admission");
            assertThat(captureFailure.get()).isNull();
        }

        private void awaitOldAbandoned() {
            await(oldAbandoned, "wait for admitted old source request cancellation");
        }

        private void awaitNewAdmission() {
            await(newAdmitted, "wait for new source-leader target admission");
            assertThat(captureFailure.get()).isNull();
        }

        private long oldSequence() {
            return oldSequence.get();
        }

        private long newSequence() {
            return newSequence.get();
        }

        private void releaseOld() {
            releaseOld.countDown();
        }

        private void releaseNew() {
            releaseNew.countDown();
        }

        private void releaseAll() {
            releaseNew();
            releaseOld();
        }

        private byte[] capturedBatch() {
            byte[] bytes = capturedBatch.get();
            assertThat(bytes).as("captured admitted old source batch").isNotNull();
            assertThat(captureFailure.get()).isNull();
            return bytes.clone();
        }

        private byte[] dominatingBatch() {
            byte[] bytes = dominatingBatch.get();
            assertThat(bytes).as("captured dominating source batch").isNotNull();
            assertThat(captureFailure.get()).isNull();
            return bytes.clone();
        }

        @Override
        public void close() throws Exception {
            releaseAll();
            registration.close();
        }
    }

    private static final class AbandonedOldRequest extends RuntimeException {
        private static final AbandonedOldRequest INSTANCE = new AbandonedOldRequest();

        private AbandonedOldRequest() {
            super(null, null, false, false);
        }
    }

    private static final class CapturedBatch {
        private final byte[] bytes;
        private final boolean containsExpectedMutation;

        private CapturedBatch(byte[] bytes, boolean containsExpectedMutation) {
            this.bytes = bytes;
            this.containsExpectedMutation = containsExpectedMutation;
        }
    }

    private static final class SourceMutation {
        private final int primaryKey;
        @Nullable private final String value;

        private SourceMutation(int primaryKey, @Nullable String value) {
            this.primaryKey = primaryKey;
            this.value = value;
        }

        private static SourceMutation upsert(int primaryKey, String value) {
            return new SourceMutation(primaryKey, value);
        }

        private static SourceMutation delete(int primaryKey) {
            return new SourceMutation(primaryKey, null);
        }

        @Override
        public String toString() {
            return value == null
                    ? "DELETE(" + primaryKey + ')'
                    : "UPSERT(" + primaryKey + ',' + value + ')';
        }
    }

    private static final class PhysicalRow {
        private final byte[] key;

        private PhysicalRow(byte[] key) {
            this.key = key;
        }
    }

    private static final class Target {
        private final int bucket;
        private final TableBucket tableBucket;
        private final int leader;
        private final Replica replica;

        private Target(int bucket, TableBucket tableBucket, int leader, Replica replica) {
            this.bucket = bucket;
            this.tableBucket = tableBucket;
            this.leader = leader;
            this.replica = replica;
        }

        @Override
        public String toString() {
            return tableBucket + "@" + leader + " isr=" + replica.getIsr();
        }
    }

    private static final class Fixture {
        private final long mainTableId;
        private final long indexTableId;
        private final int sourceBucket;
        private final TableBucket sourceTableBucket;
        private final int sourceLeader;
        private final Replica sourceReplica;
        private final Target failoverTarget;
        private final Target otherTarget;

        private Fixture(
                long mainTableId,
                long indexTableId,
                int sourceBucket,
                TableBucket sourceTableBucket,
                int sourceLeader,
                Replica sourceReplica,
                Target failoverTarget,
                Target otherTarget) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.sourceBucket = sourceBucket;
            this.sourceTableBucket = sourceTableBucket;
            this.sourceLeader = sourceLeader;
            this.sourceReplica = sourceReplica;
            this.failoverTarget = failoverTarget;
            this.otherTarget = otherTarget;
        }
    }
}
