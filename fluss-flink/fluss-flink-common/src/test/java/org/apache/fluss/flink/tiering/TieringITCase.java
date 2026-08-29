/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.bulkload.BulkLoadBucketFiles;
import org.apache.fluss.client.bulkload.BulkLoadBucketWriter;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.bulkload.BulkLoadClient;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for tiering. */
abstract class TieringITCase extends FlinkTieringTestBase {

    private static final Duration BULKLOAD_TIMEOUT = Duration.ofMinutes(2);

    @BeforeAll
    protected static void beforeAll() {
        FlinkTieringTestBase.beforeAll();
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        FlinkTieringTestBase.afterAll();
    }

    @BeforeEach
    @Override
    void beforeEach() {
        execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(1)
                        .setRuntimeMode(RuntimeExecutionMode.STREAMING);
    }

    @Test
    @MultiVersionTest
    void testTieringReachMaxDuration() throws Exception {
        TablePath logTablePath = TablePath.of("fluss", "logtable");
        createTable(logTablePath, false);
        TablePath pkTablePath = TablePath.of("fluss", "pktable");
        createTable(pkTablePath, true);

        // write some records to log table
        List<InternalRow> rows = new ArrayList<>();
        int recordCount = 6;
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(logTablePath, rows, true);

        rows = new ArrayList<>();
        //  write 6 records to primary key table, each bucket should only contain few record
        for (int i = 0; i < recordCount; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        writeRows(pkTablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(pkTablePath);

        // set tiering duration to a small value for testing purpose
        Configuration lakeTieringConfig = new Configuration();
        try (TieringJobScope ignored = startTieringJob(execEnv, lakeTieringConfig)) {
            // Wait until all records are tiered, then verify that max duration forced tiering to
            // complete in multiple snapshots.
            LakeSnapshot logTableLakeSnapshot = waitUntilFullyTiered(logTablePath, recordCount);
            assertThat(countTieredRecords(logTableLakeSnapshot)).isEqualTo(recordCount);
            assertThat(logTableLakeSnapshot.getSnapshotId()).isGreaterThan(0L);

            LakeSnapshot pkTableLakeSnapshot = waitUntilFullyTiered(pkTablePath, recordCount);
            assertThat(countTieredRecords(pkTableLakeSnapshot)).isEqualTo(recordCount);
            assertThat(pkTableLakeSnapshot.getSnapshotId()).isGreaterThan(0L);
        }
    }

    @Test
    void testTieringReadsRemoteFirstAndSwitchesToLocalTail() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "remote_first_log_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        long tableId = createTable(tablePath, schema);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        int remoteRecordCount = 4;
        List<InternalRow> expectedRows = createRows(0, remoteRecordCount);
        writeRows(tablePath, expectedRows, true);

        Replica replica = getLeaderReplica(tableBucket);
        LogTablet logTablet = replica.getLogTablet();
        logTablet.roll(Optional.empty());

        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tableBucket);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(logTablet.canFetchFromRemoteLog(remoteRecordCount - 1L)).isTrue());

        List<InternalRow> localTailRows = createRows(remoteRecordCount, 2);
        expectedRows.addAll(localTailRows);
        writeRows(tablePath, localTailRows, true);

        assertThat(logTablet.canFetchFromRemoteLog(remoteRecordCount)).isFalse();
        assertThat(logTablet.localLogStartOffset()).isZero();
        assertThat(logTablet.localLogEndOffset()).isEqualTo(expectedRows.size());

        int allLocalBytes = readLocalBytes(logTablet, 0L);
        int localTailBytes = readLocalBytes(logTablet, remoteRecordCount);
        assertThat(localTailBytes).isPositive().isLessThan(allLocalBytes);

        long localBytesOutBefore =
                replica.tableMetrics().getServerMetricGroup().bytesOut().getCount();
        try (TieringJobScope ignored = startTieringJob(execEnv)) {
            assertReplicaStatus(tableBucket, expectedRows.size());
            assertRows(tablePath, expectedRows);

            long localBytesOut =
                    replica.tableMetrics().getServerMetricGroup().bytesOut().getCount()
                            - localBytesOutBefore;
            assertThat(localBytesOut).isEqualTo(localTailBytes);
        }
    }

    private List<InternalRow> createRows(int start, int count) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = start; i < start + count; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("v" + i)));
        }
        return rows;
    }

    private int readLocalBytes(LogTablet logTablet, long offset) throws Exception {
        return logTablet
                .read(offset, Integer.MAX_VALUE, FetchIsolation.LOG_END, true, null, null)
                .getRecords()
                .sizeInBytes();
    }

    private void assertRows(TablePath tablePath, List<InternalRow> expectedRows) {
        List<InternalRow> actualRows = getValuesRecords(tablePath);
        assertThat(actualRows).hasSameSizeAs(expectedRows);
        for (int i = 0; i < expectedRows.size(); i++) {
            InternalRow actual = actualRows.get(i);
            InternalRow expected = expectedRows.get(i);
            assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
            assertThat(actual.getString(1)).isEqualTo(expected.getString(1));
        }
    }

    @Test
    @MultiVersionTest
    void testBulkLoadSnapshotThenTier() throws Exception {
        Configuration clusterConfiguration = new Configuration();
        clusterConfiguration.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        clusterConfiguration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.LANCE);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setClusterConf(clusterConfiguration)
                        .setNumOfTabletServers(3)
                        .build();
        Throwable scenarioFailure = null;
        try {
            cluster.start();
            Configuration featureClientConfiguration = cluster.getClientConfig();
            try (Connection featureConnection =
                            ConnectionFactory.createConnection(featureClientConfiguration);
                    Admin featureAdmin = featureConnection.getAdmin()) {
                runBulkLoadThenTierScenario(
                        featureConnection,
                        featureConnection.getBulkLoadClient(),
                        featureAdmin,
                        featureClientConfiguration);
            }
        } catch (Exception | AssertionError failure) {
            scenarioFailure = failure;
            throw failure;
        } finally {
            try {
                cluster.close();
            } catch (Exception closeFailure) {
                if (scenarioFailure != null) {
                    scenarioFailure.addSuppressed(closeFailure);
                } else {
                    throw new AssertionError(
                            "Cluster close failed after BulkLoad tiering completed.", closeFailure);
                }
            }
        }
    }

    private void runBulkLoadThenTierScenario(
            Connection featureConnection,
            BulkLoadClient bulkLoadClient,
            Admin featureAdmin,
            Configuration featureClientConfiguration)
            throws Exception {
        TablePath tablePath = TablePath.of("fluss", "bulkload_pk_tiering");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();
        featureAdmin
                .createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(5, "a")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS,
                                        Duration.ofMillis(500))
                                .build(),
                        true)
                .get(BULKLOAD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        BulkLoadBuildContext buildContext =
                waitValue(
                        () -> {
                            try {
                                return Optional.of(
                                        bulkLoadClient.begin(
                                                PhysicalTablePath.of(tablePath),
                                                null,
                                                BULKLOAD_TIMEOUT));
                            } catch (Exception failure) {
                                Throwable cause = ExceptionUtils.stripExecutionException(failure);
                                if (cause instanceof org.apache.fluss.exception.TimeoutException) {
                                    return Optional.empty();
                                }
                                throw failure;
                            }
                        },
                        BULKLOAD_TIMEOUT,
                        "public BulkLoad Begin after table role convergence");
        TableInfo tableInfo =
                featureAdmin
                        .getTableInfo(tablePath)
                        .get(BULKLOAD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        int rowsPerBucket = 1101;
        List<List<Object[]>> rowsByBucket = new ArrayList<>();
        Map<Integer, Object[]> expectedRowsById = new LinkedHashMap<>();
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            rowsByBucket.add(new ArrayList<>());
        }
        int nextId = 0;
        while (expectedRowsById.size() < tableInfo.getNumBuckets() * rowsPerBucket) {
            Object[] values =
                    new Object[] {
                        nextId,
                        nextId % 37 == 0 ? null : "bulkload-value-" + nextId + '-' + (nextId % 97)
                    };
            InternalRow inputRow = row(tableInfo.getRowType(), values);
            int bucketId =
                    bucketingFunction.bucketing(
                            bucketKeyEncoder.encodeKey(inputRow), tableInfo.getNumBuckets());
            if (rowsByBucket.get(bucketId).size() < rowsPerBucket) {
                rowsByBucket.get(bucketId).add(values);
                expectedRowsById.put(nextId, values);
            }
            nextId++;
        }
        List<Object[]> expectedTieredRows = new ArrayList<>(expectedRowsById.values());
        List<BulkLoadBucketWriter> bucketWriters = new ArrayList<>();
        List<Path> bucketWorkDirectories = new ArrayList<>();
        long[] bucketOffsets = new long[tableInfo.getNumBuckets()];
        try {
            for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
                Path workDirectory = Files.createTempDirectory("fluss-bulkload-tiering-");
                bucketWorkDirectories.add(workDirectory);
                bucketWriters.add(
                        new BulkLoadBucketWriter(buildContext, bucketId, workDirectory.toFile()));
            }
            for (int bucketId = 0; bucketId < rowsByBucket.size(); bucketId++) {
                for (Object[] values : rowsByBucket.get(bucketId)) {
                    bucketWriters.get(bucketId).add(row(tableInfo.getRowType(), values));
                    bucketOffsets[bucketId]++;
                }
            }
            List<BulkLoadBucketFiles> bucketFiles = new ArrayList<>();
            for (BulkLoadBucketWriter bucketWriter : bucketWriters) {
                bucketFiles.add(bucketWriter.finish());
            }
            BulkLoadStatus committed =
                    bulkLoadClient.commit(buildContext, bucketFiles, BULKLOAD_TIMEOUT);
            assertThat(committed.getState()).isEqualTo(BulkLoadState.COMMITTED);

            JobClient jobClient =
                    buildTieringJob(execEnv, featureClientConfiguration, new Configuration());
            try {
                Map<TableBucket, Long> snapshotBoundaryOffsets = new LinkedHashMap<>();
                for (int bucketId = 0; bucketId < bucketOffsets.length; bucketId++) {
                    snapshotBoundaryOffsets.put(
                            new TableBucket(tableInfo.getTableId(), bucketId),
                            bucketOffsets[bucketId]);
                }
                assertThat(
                                waitLakeSnapshot(
                                                featureAdmin,
                                                tablePath,
                                                BULKLOAD_TIMEOUT,
                                                snapshot ->
                                                        snapshot.getTableBucketsOffset()
                                                                .equals(snapshotBoundaryOffsets))
                                        .getTableBucketsOffset())
                        .containsExactlyInAnyOrderEntriesOf(snapshotBoundaryOffsets);

                List<Object[]> onlineRows = new ArrayList<>();
                for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
                    Object[] imported = rowsByBucket.get(bucketId).get(0);
                    Object[] update =
                            new Object[] {imported[0], "online-update-bucket-" + bucketId};
                    onlineRows.add(update);
                    expectedTieredRows.add(imported);
                    expectedTieredRows.add(update);

                    while (true) {
                        Object[] insert =
                                new Object[] {nextId++, "online-insert-bucket-" + bucketId};
                        InternalRow insertRow = row(tableInfo.getRowType(), insert);
                        int actualBucket =
                                bucketingFunction.bucketing(
                                        bucketKeyEncoder.encodeKey(insertRow),
                                        tableInfo.getNumBuckets());
                        if (actualBucket == bucketId) {
                            onlineRows.add(insert);
                            expectedTieredRows.add(insert);
                            break;
                        }
                    }
                    // Full changelog emits UPDATE_BEFORE, UPDATE_AFTER, and INSERT.
                    bucketOffsets[bucketId] += 3;
                }
                try (Table table = featureConnection.getTable(tablePath)) {
                    UpsertWriter writer = table.newUpsert().createWriter();
                    for (Object[] values : onlineRows) {
                        writer.upsert(row(tableInfo.getRowType(), values))
                                .get(BULKLOAD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }
                    writer.flush();
                }

                Map<TableBucket, Long> expectedOffsets = new LinkedHashMap<>();
                for (int bucketId = 0; bucketId < bucketOffsets.length; bucketId++) {
                    expectedOffsets.put(
                            new TableBucket(tableInfo.getTableId(), bucketId),
                            bucketOffsets[bucketId]);
                }
                assertThat(
                                waitLakeSnapshot(
                                                featureAdmin,
                                                tablePath,
                                                BULKLOAD_TIMEOUT,
                                                snapshot ->
                                                        snapshot.getTableBucketsOffset()
                                                                .equals(expectedOffsets))
                                        .getTableBucketsOffset())
                        .containsExactlyInAnyOrderEntriesOf(expectedOffsets);

                List<InternalRow> tieredRows =
                        waitValue(
                                () -> {
                                    List<InternalRow> current = getValuesRecords(tablePath);
                                    return current.size() == expectedTieredRows.size()
                                            ? Optional.of(current)
                                            : Optional.empty();
                                },
                                BULKLOAD_TIMEOUT,
                                "ordinary Flink Lake Tiering produces the imported baseline");
                assertThat(asComparableInternalRows(tieredRows))
                        .containsExactlyInAnyOrderElementsOf(
                                asComparableObjectRows(expectedTieredRows));
            } finally {
                jobClient.cancel().get(BULKLOAD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } finally {
            try {
                RuntimeException closeFailure = null;
                for (BulkLoadBucketWriter bucketWriter : bucketWriters) {
                    try {
                        bucketWriter.close();
                    } catch (RuntimeException e) {
                        if (closeFailure == null) {
                            closeFailure = e;
                        } else {
                            closeFailure.addSuppressed(e);
                        }
                    }
                }
                if (closeFailure != null) {
                    throw closeFailure;
                }
            } finally {
                for (Path workDirectory : bucketWorkDirectories) {
                    org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly(workDirectory.toFile());
                }
            }
        }
    }

    private List<String> asComparableInternalRows(List<InternalRow> rows) {
        List<String> comparable = new ArrayList<>(rows.size());
        for (InternalRow row : rows) {
            comparable.add(row.getInt(0) + "=" + row.getString(1));
        }
        return comparable;
    }

    private List<String> asComparableObjectRows(List<Object[]> rows) {
        List<String> comparable = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            comparable.add(row[0] + "=" + row[1]);
        }
        return comparable;
    }

    private long countTieredRecords(LakeSnapshot lakeSnapshot) {
        return lakeSnapshot.getTableBucketsOffset().values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    private LakeSnapshot waitUntilFullyTiered(TablePath tablePath, long expectedRecordCount) {
        return waitLakeSnapshot(
                admin,
                tablePath,
                Duration.ofSeconds(30),
                snapshot -> countTieredRecords(snapshot) == expectedRecordCount);
    }

    private LakeSnapshot waitLakeSnapshot(TablePath tablePath) {
        return waitLakeSnapshot(admin, tablePath, Duration.ofSeconds(30));
    }

    private LakeSnapshot waitLakeSnapshot(
            Admin snapshotAdmin, TablePath tablePath, Duration timeout) {
        return waitLakeSnapshot(snapshotAdmin, tablePath, timeout, snapshot -> true);
    }

    private LakeSnapshot waitLakeSnapshot(
            Admin snapshotAdmin,
            TablePath tablePath,
            Duration timeout,
            Predicate<LakeSnapshot> completionPredicate) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        return waitValue(
                () -> {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        return Optional.empty();
                    }
                    try {
                        LakeSnapshot snapshot =
                                snapshotAdmin
                                        .getLatestLakeSnapshot(tablePath)
                                        .get(remainingNanos, TimeUnit.NANOSECONDS);
                        return completionPredicate.test(snapshot)
                                ? Optional.of(snapshot)
                                : Optional.empty();
                    } catch (TimeoutException timeoutException) {
                        return Optional.empty();
                    } catch (Exception e) {
                        if (ExceptionUtils.stripExecutionException(e)
                                instanceof LakeTableSnapshotNotExistException) {
                            return Optional.empty();
                        }
                        throw e;
                    }
                },
                timeout,
                "Fail to wait for one round of tiering finish for table " + tablePath);
    }

    private void createTable(TablePath tablePath, boolean isPrimaryKeyTable) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());
        if (isPrimaryKeyTable) {
            schemaBuilder.primaryKey("a");
        }

        // see TestingPaimonStoragePlugin#TestingPaimonWriter, we set write-pause
        // to 1s to make it easy to mock tiering reach max duration
        Map<String, String> customProperties = Collections.singletonMap("write-pause", "1s");
        createTable(
                tablePath,
                3,
                Collections.singletonList("a"),
                schemaBuilder.build(),
                customProperties);
    }
}
