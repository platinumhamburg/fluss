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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Failover ITCase for the index push replication pipeline. Tests that:
 *
 * <ul>
 *   <li>After main-table leader failover, the new leader resumes index push from the checkpointed
 *       indexPushedOffset.
 *   <li>After index-table leader failover, the push pipeline retries and the entry eventually
 *       becomes visible.
 * </ul>
 *
 * <p>Requires a 3-node cluster with replication factor 3 for leader failover.
 */
class IndexPushFailoverITCase {

    private static final String DB = "test_failover_db";
    private static final String INDEX_NAME = "idx_b";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        // Keep a failed target batch visible in its deque long enough for the causal retry check.
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofSeconds(1));
        return conf;
    }

    private static final RowType INDEX_BUCKET_KEY_TYPE =
            DataTypes.ROW(new DataField("b", DataTypes.STRING().copy(false)));

    /**
     * After writing data to the main table and verifying the index entry is pushed, kill the
     * main-table leader. The new leader should resume from the checkpointed offset and continue
     * pushing new writes.
     */
    @Test
    void testMainTableLeaderFailoverResumesIndexPush() throws Exception {
        TablePath mainPath = TablePath.of(DB, "main_failover");
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName("main_failover", INDEX_NAME));

        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, descriptor);
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(() -> new IllegalStateException("Index table not in ZK"))
                        .tableId;

        // Wait for all leaders
        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int originalLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);
        for (int i = 0; i < 3; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }

        // Wait for IndexReplicator to be wired
        Replica mainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        waitUntil(
                () -> mainReplica.getIndexReplicator() != null,
                Duration.ofSeconds(30),
                "wait for IndexReplicator");

        // Write first batch
        FLUSS_CLUSTER_EXTENSION
                .newTabletServerClientForNode(originalLeader)
                .putKv(
                        newPutKvRequest(
                                mainTableId, 0, 1, genKvRecordBatch(new Object[] {1, "hello"})))
                .get();

        // Verify index entry via direct lookup
        byte[] indexKey = encodeIndexKey("hello", 1);
        int indexBucket = computeIndexBucket("hello");
        waitUntil(
                () -> {
                    Replica idxReplica =
                            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                                    new TableBucket(indexTableId, indexBucket));
                    List<byte[]> result = idxReplica.lookups(Collections.singletonList(indexKey));
                    return result.get(0) != null;
                },
                TIMEOUT,
                "wait for first index entry");

        // Capture the pushed offset after the first SYNC index entry is visible, then wait until a
        // completed KV snapshot has persisted the same value via TabletState. A correct failover
        // must restore *exactly* this value on the new leader.
        long offsetBefore =
                waitValue(
                        () -> {
                            long offset = mainReplica.getSyncIndexPushedOffset();
                            return offset > 0L ? Optional.of(offset) : Optional.empty();
                        },
                        TIMEOUT,
                        "wait for sync index pushed offset before failover");
        assertThat(offsetBefore)
                .as("sync write must have advanced the pushed offset before failover")
                .isGreaterThan(0L);
        assertThat(mainReplica.getAllIndexPushedOffset())
                .as("all-index pushed offset must advance with the sync pushed offset")
                .isEqualTo(offsetBefore);
        CompletedSnapshot persistedSnapshot =
                triggerSnapshotPersistingIndexPushedOffset(mainBucket, offsetBefore);
        assertThat(persistedSnapshot.getIndexPushedOffset())
                .as("snapshot must persist the exact indexPushedOffset used for failover restore")
                .isEqualTo(offsetBefore);

        // Kill the original leader
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(originalLeader);

        // Wait for new leader election
        waitUntil(
                () -> {
                    int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);
                    return newLeader != originalLeader;
                },
                TIMEOUT,
                "wait for new main-table leader");

        int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);

        // Wait for new leader's IndexReplicator
        Replica newMainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        waitUntil(
                () -> newMainReplica.getIndexReplicator() != null,
                Duration.ofSeconds(30),
                "wait for IndexReplicator on new leader");

        // The new leader must restore the *exact* checkpointed offset. isEqualTo (not >= 0) is
        // what makes a broken restore path fail: a reset-to-0 bug would replay from the WAL head,
        // a skip-ahead bug would drop un-pushed records — both are caught by exact equality.
        assertThat(newMainReplica.getSyncIndexPushedOffset())
                .as("new leader must restore the exact checkpointed indexPushedOffset")
                .isEqualTo(offsetBefore);

        // Safety: the first batch's index entry must survive the failover (no data loss on
        // recovery). Verified before any new write so survival cannot be masked by WAL re-replay.
        Replica firstIndexReplicaAfterFailover =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                        new TableBucket(indexTableId, indexBucket));
        List<byte[]> firstEntryAfterFailover =
                firstIndexReplicaAfterFailover.lookups(Collections.singletonList(indexKey));
        assertThat(firstEntryAfterFailover.get(0))
                .as("first index entry must survive main-table leader failover")
                .isNotNull();

        // Write second batch to the new leader
        FLUSS_CLUSTER_EXTENSION
                .newTabletServerClientForNode(newLeader)
                .putKv(
                        newPutKvRequest(
                                mainTableId, 0, 1, genKvRecordBatch(new Object[] {2, "world"})))
                .get();

        // Verify the second index entry appears
        byte[] indexKey2 = encodeIndexKey("world", 2);
        int indexBucket2 = computeIndexBucket("world");
        waitUntil(
                () -> {
                    Replica idxReplica =
                            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(
                                    new TableBucket(indexTableId, indexBucket2));
                    List<byte[]> result = idxReplica.lookups(Collections.singletonList(indexKey2));
                    return result.get(0) != null;
                },
                TIMEOUT,
                "wait for second index entry after failover");

        // Restart the stopped server for cluster health
        FLUSS_CLUSTER_EXTENSION.startTabletServer(originalLeader);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
    }

    /**
     * After writing data to the main table, kill the index table's leader. The push pipeline should
     * retry via the LeaderResolver and the entry should eventually appear on the new index leader.
     */
    @Test
    void testIndexTableLeaderFailoverRetries() throws Throwable {
        TablePath mainPath = TablePath.of(DB, "main_idx_failover");
        TablePath indexPath =
                TablePath.of(DB, IndexTableUtils.indexTableName("main_idx_failover", INDEX_NAME));

        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        long mainTableId = createTable(FLUSS_CLUSTER_EXTENSION, mainPath, descriptor);
        long indexTableId =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(indexPath)
                        .orElseThrow(() -> new IllegalStateException("Index table not in ZK"))
                        .tableId;

        TableBucket mainBucket = new TableBucket(mainTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        int mainLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(mainBucket);

        // Wait for all index bucket leaders and IndexReplicator
        for (int i = 0; i < 3; i++) {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(indexTableId, i));
        }
        Replica mainReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(mainBucket);
        waitUntil(
                () -> mainReplica.getIndexReplicator() != null,
                Duration.ofSeconds(30),
                "wait for IndexReplicator");

        // Nothing has been written to the main table yet, so the pushed offset is still its
        // initial sentinel (-1 = nothing pushed).
        assertThat(mainReplica.getSyncIndexPushedOffset())
                .as("pushed offset must be the -1 sentinel before any main-table write")
                .isEqualTo(-1L);

        // Select a physical target bucket whose leader is not the source main-table leader. The
        // written value below is derived from this exact bucket, so the stopped server owns the
        // target that receives the index push.
        int[] indexBucketLeaders = new int[3];
        int targetIdxBucket = -1;
        for (int i = 0; i < 3; i++) {
            int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(indexTableId, i));
            indexBucketLeaders[i] = leader;
            if (targetIdxBucket == -1 && leader != mainLeader) {
                targetIdxBucket = i;
            }
        }
        if (targetIdxBucket == -1) {
            throw new AssertionError(
                    "No index bucket leader differs from source main-table leader "
                            + mainLeader
                            + "; index bucket leaders are "
                            + Arrays.toString(indexBucketLeaders));
        }

        String selectedValue = valueForIndexBucket("target-failover", targetIdxBucket);
        byte[] indexKey = encodeIndexKey(selectedValue, 1);
        TableBucket idxTb = new TableBucket(indexTableId, targetIdxBucket);
        int stoppedTargetLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb);
        assertThat(stoppedTargetLeader)
                .as("selected index bucket leader must differ from the source main-table leader")
                .isNotEqualTo(mainLeader);
        assertThat(stoppedTargetLeader)
                .as("selected index bucket leader must remain the observed target leader")
                .isEqualTo(indexBucketLeaders[targetIdxBucket]);

        IndexAccumulator sourceAccumulator =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(mainLeader)
                        .getReplicaManager()
                        .getIndexAccumulator();
        boolean coordinatorStopped = false;
        boolean targetLeaderStopped = false;
        Throwable primaryFailure = null;
        try {
            // Keep the old target assignment frozen so the source sender must observe the stopped
            // target before a replacement can become available.
            FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
            coordinatorStopped = true;
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedTargetLeader);
            targetLeaderStopped = true;

            // Submit the source write while the exact physical target is unavailable. Do not wait
            // for replacement leadership before the push enters the retry path.
            CompletableFuture<PutKvResponse> sourceWrite =
                    FLUSS_CLUSTER_EXTENSION
                            .newTabletServerClientForNode(mainLeader)
                            .putKv(
                                    newPutKvRequest(
                                            mainTableId,
                                            0,
                                            1,
                                            genKvRecordBatch(
                                                    new Object[] {1, selectedValue})));

            // IndexBatch.attempts() increments only when a failed batch is put back into this
            // exact target bucket's deque. With the coordinator stopped, a replacement cannot
            // race this observation; the source SYNC future and pushed offset must remain pending.
            waitUntil(
                    () -> hasRetriedTargetBatch(sourceAccumulator, idxTb),
                    TIMEOUT,
                    "wait for the selected target index batch to fail and requeue");
            assertThat(sourceWrite.isDone())
                    .as("source SYNC write must remain unresolved while the target is unavailable")
                    .isFalse();
            assertThat(mainReplica.getSyncIndexPushedOffset())
                    .as("sync pushed offset must remain at the pre-write baseline during retry")
                    .isEqualTo(-1L);

            FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
            coordinatorStopped = false;
            waitUntil(
                    () -> FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb) != stoppedTargetLeader,
                    TIMEOUT,
                    "wait for the selected target index bucket to fail over");
            sourceWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Verify the exact physical key on the replacement leader for the exact bucket that
            // was unavailable during the source write.
            waitUntil(
                    () -> {
                        try {
                            Replica idxReplica =
                                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(idxTb);
                            List<byte[]> result =
                                    idxReplica.lookups(Collections.singletonList(indexKey));
                            return result.get(0) != null;
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    TIMEOUT,
                    "wait for index entry after selected target leader failover");
            Replica replacementTargetLeader =
                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(idxTb);
            assertThat(replacementTargetLeader.lookups(Collections.singletonList(indexKey)).get(0))
                    .as("exact physical index key must be present on the replacement target leader")
                    .isNotNull();

            // One source row at offset 0 advances the exact completed prefix to 1. An exact check
            // catches both a retry that never completed and an accidental skip past source WAL.
            waitUntil(
                    () -> mainReplica.getSyncIndexPushedOffset() == 1L,
                    TIMEOUT,
                    "pushed offset must equal the one-row source WAL end after retry");
            assertThat(mainReplica.getSyncIndexPushedOffset()).isEqualTo(1L);
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            if (coordinatorStopped) {
                cleanup(cleanupFailures, FLUSS_CLUSTER_EXTENSION::startCoordinatorServer);
            }
            if (targetLeaderStopped) {
                cleanup(
                        cleanupFailures,
                        () -> FLUSS_CLUSTER_EXTENSION.startTabletServer(stoppedTargetLeader));
            }
            cleanup(cleanupFailures, () -> FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3));
            reportCleanupFailures(primaryFailure, cleanupFailures);
        }
    }

    private static byte[] encodeIndexKey(String b, int a) {
        RowType type =
                DataTypes.ROW(
                        new DataField("b", DataTypes.STRING().copy(false)),
                        new DataField("a", DataTypes.INT().copy(false)));
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(type);
        GenericRow row = new GenericRow(2);
        row.setField(0, fromString(b));
        row.setField(1, a);
        return encoder.encodeKey(row);
    }

    private static int computeIndexBucket(String bValue) {
        CompactedKeyEncoder bucketKeyEncoder = new CompactedKeyEncoder(INDEX_BUCKET_KEY_TYPE);
        GenericRow row = new GenericRow(1);
        row.setField(0, fromString(bValue));
        byte[] bucketKey = bucketKeyEncoder.encodeKey(row);
        return new org.apache.fluss.bucketing.FlussBucketingFunction().bucketing(bucketKey, 3);
    }

    private static String valueForIndexBucket(String prefix, int expectedBucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String candidate = prefix + '-' + suffix;
            if (computeIndexBucket(candidate) == expectedBucket) {
                return candidate;
            }
        }
        throw new AssertionError("No value found for index bucket " + expectedBucket);
    }

    /** Read-only test observation of the package-private retry state for one physical target. */
    @SuppressWarnings("unchecked")
    private static boolean hasRetriedTargetBatch(IndexAccumulator accumulator, TableBucket target)
            throws ReflectiveOperationException {
        Field batchesField = IndexAccumulator.class.getDeclaredField("batches");
        batchesField.setAccessible(true);
        Map<TableBucket, Deque<IndexBatch>> batches =
                (Map<TableBucket, Deque<IndexBatch>>) batchesField.get(accumulator);
        Deque<IndexBatch> deque = batches.get(target);
        if (deque == null) {
            return false;
        }
        synchronized (deque) {
            for (IndexBatch batch : deque) {
                if (batch.targetBucket().equals(target) && batch.attempts() > 0) {
                    return true;
                }
            }
            return false;
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

    private static void reportCleanupFailures(Throwable primaryFailure, List<Throwable> cleanupFailures)
            throws Throwable {
        if (cleanupFailures.isEmpty()) {
            return;
        }
        AssertionError cleanupFailure = new AssertionError("index target failover IT cleanup failed");
        cleanupFailures.forEach(cleanupFailure::addSuppressed);
        if (primaryFailure != null) {
            primaryFailure.addSuppressed(cleanupFailure);
        } else {
            throw cleanupFailure;
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Throwable;
    }

    private static CompletedSnapshot triggerSnapshotPersistingIndexPushedOffset(
            TableBucket tableBucket, long expectedOffset) {
        CompletedSnapshot completedSnapshot =
                FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tableBucket);
        assertThat(completedSnapshot.getIndexPushedOffset())
                .as("triggered KV snapshot must persist indexPushedOffset")
                .isEqualTo(expectedOffset);
        return completedSnapshot;
    }
}
