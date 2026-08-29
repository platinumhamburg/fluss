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

package org.apache.fluss.flink.sink.bulkload;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.bulkload.BulkLoadBucketWriter;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * End-to-end ITCases of the BulkLoad batch {@code INSERT INTO} for a primary-key table, covering
 * the whole feature path: the connector options, the eligibility gate, the Begin/Build/Commit
 * operators and the server-side BulkLoad protocol against a real Fluss cluster.
 *
 * <p>The cluster is shared within this single test class; every case uses its own table so the
 * sequential cases never interfere with each other. The BulkLoad sink is enabled either through the
 * table option {@code sink.bulk-load.enabled} or through the per-statement {@code OPTIONS} hint;
 * the cases that still need the regular sink path (sibling partition writes, non-empty target
 * setup, fence-leak checks) use the hint channel so the table itself stays writable through the
 * regular path.
 */
@MultiVersionTest
class BulkLoadSinkITCase {

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "test-flink-db";
    private static final int FLINK_SLOTS = 4;

    @TempDir private static Path taskManagerTmpParent;

    private static Path[] taskManagerTmpRoots;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    // Mirror the cluster configuration of the client-side
                                    // BulkLoadE2EITCase: full replication and quiesced
                                    // background tasks, so leader roles stay stable for the
                                    // duration of a BulkLoad round.
                                    .set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3)
                                    .set(
                                            ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION,
                                            Duration.ofHours(1))
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1)))
                    .setNumOfTabletServers(3)
                    .build();

    private static Connection conn;
    private static Admin admin;
    private static String bootstrapServers;
    private static MiniCluster miniCluster;

    @BeforeAll
    static void beforeAll() throws Exception {
        taskManagerTmpRoots = createTaskManagerTmpRoots();
        org.apache.flink.configuration.Configuration flinkConfiguration =
                taskManagerTmpDirectoryConfiguration();
        flinkConfiguration.set(JobManagerOptions.PORT, 0);
        flinkConfiguration.set(RestOptions.BIND_PORT, "0");
        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(flinkConfiguration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(FLINK_SLOTS)
                                .build());
        miniCluster.start();
        TestStreamEnvironment.setAsContext(miniCluster, FLINK_SLOTS);

        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            admin = null;
            try {
                if (conn != null) {
                    conn.close();
                }
            } finally {
                conn = null;
                try {
                    TestStreamEnvironment.unsetAsContext();
                } finally {
                    try {
                        if (miniCluster != null) {
                            miniCluster.closeAsync().get();
                        }
                    } finally {
                        miniCluster = null;
                        if (taskManagerTmpParent != null) {
                            FileUtils.deleteDirectory(taskManagerTmpParent.toFile());
                        }
                    }
                }
            }
        }
    }

    private StreamExecutionEnvironment batchExecEnv;
    private StreamTableEnvironment tBatchEnv;
    private StreamTableEnvironment tStreamEnv;

    @BeforeEach
    void before() throws Exception {
        admin.createDatabase(DEFAULT_DB, DatabaseDescriptor.EMPTY, true).get();
        String catalogDdl =
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers);
        batchExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        batchExecEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tBatchEnv =
                StreamTableEnvironment.create(
                        batchExecEnv, EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(catalogDdl);
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tBatchEnv.useDatabase(DEFAULT_DB);

        // In batch mode Fluss only supports queries on datalake-enabled tables or primary-key
        // point queries, so the read-back assertions go through a streaming environment.
        tStreamEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        tStreamEnv.executeSql(catalogDdl);
        tStreamEnv.executeSql("use catalog " + CATALOG_NAME);
        tStreamEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tStreamEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() throws Exception {
        try {
            if (tBatchEnv != null) {
                tBatchEnv.useDatabase(BUILTIN_DATABASE);
                tBatchEnv.executeSql(String.format("drop database `%s` cascade", DEFAULT_DB));
            }
        } finally {
            if (miniCluster != null && miniCluster.isRunning()) {
                for (JobStatusMessage job : miniCluster.listJobs().get()) {
                    if (!job.getJobState().isTerminalState()) {
                        try {
                            miniCluster.cancelJob(job.getJobId()).get();
                        } catch (Exception ignored) {
                            // Match AbstractTestBase cleanup: attempt every running job.
                        }
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Case 1: non-partitioned table, FULL changelog image, end to end
    // ---------------------------------------------------------------------------------------------

    @Test
    void testFullImageEndToEnd() throws Exception {
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 3);
        tBatchEnv.executeSql(
                "CREATE TABLE bl_full (id INT NOT NULL, name STRING, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('sink.bulk-load.enabled' = 'true', 'bucket.num' = '7')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "bl_full");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        List<Object[]> rows = fullImageRows(tableInfo, 1101);
        List<Row> inputRows = new ArrayList<>();
        for (Object[] row : rows) {
            inputRows.add(Row.of(row));
            if (((Integer) row[0]) % 97 == 0) {
                inputRows.add(Row.of(row));
            }
        }
        RowTypeInfo inputType =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING, Types.LONG},
                        new String[] {"id", "name", "amount"});
        DataStream<Row> input = batchExecEnv.fromCollection(inputRows).returns(inputType);
        tBatchEnv.createTemporaryView(
                "full_input",
                tBatchEnv.fromDataStream(
                        input,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("amount", DataTypes.BIGINT())
                                .build()));
        Set<Path> preExistingBucketDirectories = bucketWorkDirectories();
        TableResult insertResult =
                tBatchEnv.executeSql("INSERT INTO bl_full SELECT * FROM full_input");
        Map<Path, Set<Path>> observedBucketDirectories = new LinkedHashMap<>();
        for (Path tmpRoot : taskManagerTmpRoots) {
            observedBucketDirectories.put(tmpRoot, new LinkedHashSet<>());
        }
        waitUntil(
                () -> {
                    for (Path tmpRoot : taskManagerTmpRoots) {
                        Set<Path> current = bucketWorkDirectories(tmpRoot);
                        current.removeAll(preExistingBucketDirectories);
                        observedBucketDirectories.get(tmpRoot).addAll(current);
                    }
                    return insertResult
                            .getJobClient()
                            .orElseThrow(
                                    () -> new AssertionError("BulkLoad INSERT has no JobClient."))
                            .getJobStatus()
                            .get()
                            .isTerminalState();
                },
                Duration.ofSeconds(30),
                "BulkLoad did not expose bucket work directories before job termination.");
        insertResult.await();
        for (Set<Path> observed : observedBucketDirectories.values()) {
            for (Path attemptChild : observed) {
                assertThat(attemptChild)
                        .as("writer-owned attempt child after BulkLoad job completion")
                        .doesNotExist();
            }
        }
        Set<Path> remainingBucketDirectories = bucketWorkDirectories();
        remainingBucketDirectories.removeAll(preExistingBucketDirectories);
        assertThat(remainingBucketDirectories)
                .as("no new BulkLoad attempt child remains after job completion")
                .isEmpty();

        List<Path> usedTmpRoots =
                observedBucketDirectories.entrySet().stream()
                        .filter(entry -> !entry.getValue().isEmpty())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        assertThat(usedTmpRoots)
                .as(
                        "TaskManager tmp roots used by real BulkLoad bucket directories: %s",
                        observedBucketDirectories)
                .containsExactlyInAnyOrder(taskManagerTmpRoots);

        List<String> expectedRows = comparableRows(rows);
        assertThat(collectRows("SELECT * FROM bl_full", expectedRows.size()))
                .containsExactlyInAnyOrderElementsOf(expectedRows);

        Map<Integer, Long> offsets = latestOffsets(tablePath, tableInfo.getNumBuckets());
        assertThat(tableInfo.getNumBuckets()).isEqualTo(7);
        assertThat(offsets).hasSize(7);
        assertThat(offsets.values()).allMatch(offset -> offset == 1101L);
        assertThat(offsets.values().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(expectedRows.size());
        assertThat(inputRows.size()).isGreaterThan(expectedRows.size());
        assertStandardBulkLoadFiles(tablePath, tableInfo, offsets.keySet());
    }

    @Test
    void testMovesTransformedRowsBetweenFlussTables() throws Exception {
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tBatchEnv.executeSql(
                "CREATE TABLE bl_move_source (id INT NOT NULL, raw_name STRING,"
                        + " raw_amount BIGINT, keep_row BOOLEAN,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ('bucket.num' = '5')");
        tBatchEnv.executeSql(
                "CREATE TABLE bl_move_target (id INT NOT NULL, name STRING, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('sink.bulk-load.enabled' = 'true', 'bucket.num' = '7')");

        TablePath sourcePath = TablePath.of(DEFAULT_DB, "bl_move_source");
        TablePath targetPath = TablePath.of(DEFAULT_DB, "bl_move_target");
        TableInfo sourceInfo = admin.getTableInfo(sourcePath).get();
        TableInfo targetInfo = admin.getTableInfo(targetPath).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(sourceInfo.getTableId());
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(targetInfo.getTableId());

        int rowsPerTargetBucket = 256;
        int filteredRows = 200;
        List<Object[]> sourceRows = new ArrayList<>();
        List<Object[]> expectedTargetRows = new ArrayList<>();
        int[] targetBucketCounts = new int[targetInfo.getNumBuckets()];
        KeyEncoder targetKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        targetInfo.getRowType(),
                        targetInfo.getBucketKeys(),
                        targetInfo.getTableConfig().getDataLakeFormat().orElse(null));
        BucketingFunction bucketing = BucketingFunction.of(null);
        for (int sourceId = 0;
                expectedTargetRows.size() < targetInfo.getNumBuckets() * rowsPerTargetBucket;
                sourceId++) {
            int targetId = 1_000_000 + sourceId;
            String rawName = sourceId % 37 == 0 ? null : "source-" + sourceId;
            long rawAmount = 20_000L + sourceId;
            Object[] targetValues =
                    new Object[] {
                        targetId, rawName == null ? null : "moved-" + rawName, rawAmount * 3 + 7
                    };
            InternalRow targetRow = row(targetInfo.getRowType(), targetValues);
            int targetBucket =
                    bucketing.bucketing(
                            targetKeyEncoder.encodeKey(targetRow), targetInfo.getNumBuckets());
            if (targetBucketCounts[targetBucket] < rowsPerTargetBucket) {
                sourceRows.add(new Object[] {sourceId, rawName, rawAmount, true});
                expectedTargetRows.add(targetValues);
                targetBucketCounts[targetBucket]++;
            }
        }
        int filteredId = 2_000_000;
        while (filteredRows-- > 0) {
            sourceRows.add(
                    new Object[] {
                        filteredId, "filtered-" + filteredId, 90_000L + filteredId, false
                    });
            filteredId++;
        }

        try (Table sourceTable = conn.getTable(sourcePath)) {
            UpsertWriter writer = sourceTable.newUpsert().createWriter();
            for (Object[] sourceValues : sourceRows) {
                writer.upsert(row(sourceInfo.getRowType(), sourceValues));
            }
            writer.flush();
        }

        assertThat(sourceRows).hasSize(1992);
        Map<Integer, Long> sourceOffsets = latestOffsets(sourcePath, sourceInfo.getNumBuckets());
        assertThat(sourceOffsets).hasSize(5);
        assertThat(sourceOffsets.values()).allMatch(offset -> offset > 0);
        assertThat(sourceOffsets.values().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(sourceRows.size());

        tBatchEnv
                .executeSql(
                        "INSERT INTO bl_move_target"
                                + " SELECT id + 1000000, CONCAT('moved-', raw_name),"
                                + " raw_amount * 3 + 7"
                                + " FROM (SELECT id, raw_name, raw_amount, keep_row"
                                + " FROM bl_move_source LIMIT 1992)"
                                + " WHERE keep_row")
                .await();

        List<String> expectedRows = comparableRows(expectedTargetRows);
        assertThat(collectRows("SELECT * FROM bl_move_target", expectedRows.size()))
                .containsExactlyInAnyOrderElementsOf(expectedRows);
        Map<Integer, Long> targetOffsets = latestOffsets(targetPath, targetInfo.getNumBuckets());
        assertThat(targetOffsets).hasSize(7);
        assertThat(targetOffsets.values()).allMatch(offset -> offset == rowsPerTargetBucket);
        assertThat(targetOffsets.values().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(expectedRows.size());
        assertStandardBulkLoadFiles(targetPath, targetInfo, targetOffsets.keySet());
    }

    // ---------------------------------------------------------------------------------------------
    // Case 2: WAL image in one static partition; its sibling stays writable via the regular path
    // ---------------------------------------------------------------------------------------------

    @Test
    void testStaticPartition() throws Exception {
        tBatchEnv.executeSql(
                "CREATE TABLE bl_part (id INT NOT NULL, name STRING, dt STRING,"
                        + " PRIMARY KEY (id, dt) NOT ENFORCED) PARTITIONED BY (dt)"
                        + " WITH ('table.changelog.image' = 'wal', 'bucket.num' = '3')");
        tBatchEnv.executeSql("ALTER TABLE bl_part ADD PARTITION (dt = '2026-08-17')");
        tBatchEnv.executeSql("ALTER TABLE bl_part ADD PARTITION (dt = '2026-08-18')");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "bl_part");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        List<Object[]> targetRows = staticPartitionRows(tableInfo, "2026-08-17", 3);
        StringBuilder values = new StringBuilder();
        for (Object[] targetRow : targetRows) {
            if (values.length() > 0) {
                values.append(", ");
            }
            values.append('(').append(targetRow[0]).append(", '").append(targetRow[1]).append("')");
        }

        // The planner materializes the statically-specified partition constant into the
        // full-schema sink input rows; the BulkLoad build path validates every row's partition
        // column value against the static partition spec and fails fast on a mismatch.
        tBatchEnv
                .executeSql(
                        "INSERT INTO bl_part /*+ OPTIONS('sink.bulk-load.enabled' = 'true') */"
                                + " PARTITION (dt = '2026-08-17') VALUES "
                                + values)
                .await();

        List<String> expectedRows = comparableRows(targetRows);
        assertThat(collectRows("SELECT * FROM bl_part", targetRows.size()))
                .containsExactlyInAnyOrderElementsOf(expectedRows);
        Map<Integer, Long> targetOffsets =
                latestOffsets(tablePath, "2026-08-17", tableInfo.getNumBuckets());
        assertThat(targetOffsets).hasSize(3);
        assertThat(targetOffsets.values()).allMatch(offset -> offset == 3L);

        // The sibling partition is not fenced by the BulkLoad of the other partition: a regular
        // write (no hint) goes through the normal sink path.
        tBatchEnv
                .executeSql("INSERT INTO bl_part PARTITION (dt = '2026-08-18') VALUES (10, 'x')")
                .await();
        expectedRows.add("+I[10, x, 2026-08-18]");
        assertThat(collectRows("SELECT * FROM bl_part", expectedRows.size()))
                .containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    // ---------------------------------------------------------------------------------------------
    // Case 3: an empty input commits legally and leaks no fence
    // ---------------------------------------------------------------------------------------------

    @Test
    void testEmptyInputCommits() throws Exception {
        tBatchEnv.executeSql(
                "CREATE TABLE bl_empty (id INT NOT NULL, name STRING, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)");
        tBatchEnv
                .executeSql(
                        "INSERT INTO bl_empty /*+ OPTIONS('sink.bulk-load.enabled' = 'true') */"
                                + " SELECT id, name, amount FROM (VALUES (1, 'a', 100))"
                                + " AS v (id, name, amount) WHERE 1 = 0")
                .await();

        // The committed empty transaction must not leak any fence: regular writes keep working
        // and the table stays readable.
        tBatchEnv.executeSql("INSERT INTO bl_empty VALUES (7, 'g', 700)").await();
        assertThat(collectRows("SELECT * FROM bl_empty", 1)).containsExactly("+I[7, g, 700]");
    }

    @Test
    void testCommitWaitsForEndOfInput() throws Exception {
        tBatchEnv.executeSql(
                "CREATE TABLE bl_commit_lifecycle (id INT NOT NULL, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ('bucket.num' = '1')");
        PhysicalTablePath target =
                PhysicalTablePath.of(TablePath.of(DEFAULT_DB, "bl_commit_lifecycle"));
        TableInfo tableInfo = admin.getTableInfo(target.getTablePath()).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());
        BulkLoadBuildContext buildContext =
                conn.getBulkLoadClient().begin(target, null, Duration.ofMinutes(2));
        Path localWorkParent =
                Files.createTempDirectory(taskManagerTmpParent, "bulk-load-commit-lifecycle-");
        try (BulkLoadBucketWriter bucketWriter =
                new BulkLoadBucketWriter(buildContext, 0, localWorkParent.toFile())) {
            BulkLoadCommittable committable =
                    new BulkLoadCommittable(buildContext, bucketWriter.finish());
            BulkLoadCommitSink sink =
                    new BulkLoadCommitSink(
                            FLUSS_CLUSTER_EXTENSION.getClientConfig(), Duration.ofMinutes(2));
            assertThat(sink).isInstanceOf(SupportsCommitter.class);

            SinkWriter<BulkLoadCommittable> writer = sink.createWriter(null, null, 0);
            CommittingSinkWriter<BulkLoadCommittable, BulkLoadCommittable> committingWriter =
                    (CommittingSinkWriter<BulkLoadCommittable, BulkLoadCommittable>) writer;
            try {
                writer.write(committable, null);
                writer.flush(false);
                assertThat(committingWriter.prepareCommit()).isEmpty();
                writer.flush(true);
                assertThat(committingWriter.prepareCommit()).containsExactly(committable);
            } finally {
                writer.close();
            }
        } finally {
            try {
                conn.getBulkLoadClient().abort(buildContext.getHandle());
            } finally {
                FileUtils.deleteDirectory(localWorkParent.toFile());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Case 4: EXPLAIN has zero side effects
    // ---------------------------------------------------------------------------------------------

    @Test
    void testExplainCreatesNoTransaction() throws Exception {
        tBatchEnv.executeSql(
                "CREATE TABLE bl_explain (id INT NOT NULL, name STRING, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('sink.bulk-load.enabled' = 'true')");
        assertThat(tBatchEnv.explainSql("INSERT INTO bl_explain VALUES (1, 'a', 100)"))
                .isNotBlank();

        // EXPLAIN must neither create a BulkLoad transaction nor install a fence: beginning a
        // transaction on the same target right afterwards must create a brand new one.
        PhysicalTablePath target = PhysicalTablePath.of(TablePath.of(DEFAULT_DB, "bl_explain"));
        BulkLoadBuildContext probe =
                conn.getBulkLoadClient().begin(target, null, Duration.ofMinutes(2));
        assertThat(conn.getBulkLoadClient().abort(probe.getHandle()).getState())
                .isEqualTo(BulkLoadState.ABORTED);
    }

    // ---------------------------------------------------------------------------------------------
    // Case 5: the eligibility gate fails fast with a ValidationException
    // ---------------------------------------------------------------------------------------------

    @Test
    void testEligibilityFailsFast() {
        // 1) streaming mode is rejected even for an eligible table
        tStreamEnv.executeSql(
                "CREATE TABLE bl_stream (id INT NOT NULL, name STRING, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('sink.bulk-load.enabled' = 'true')");
        assertEligibilityRejection(
                () -> tStreamEnv.executeSql("INSERT INTO bl_stream VALUES (1, 'a', 100)"),
                "requires batch execution mode");

        // 2) a log table (no primary key) is rejected
        tBatchEnv.executeSql(
                "CREATE TABLE bl_log (id INT, name STRING, amount BIGINT)"
                        + " WITH ('sink.bulk-load.enabled' = 'true')");
        assertEligibilityRejection(
                () -> tBatchEnv.executeSql("INSERT INTO bl_log VALUES (1, 'a', 100)"),
                "only supports primary key tables");

        // 3) a primary-key table with the aggregation merge engine is rejected
        tBatchEnv.executeSql(
                "CREATE TABLE bl_agg (id INT NOT NULL, amount BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('sink.bulk-load.enabled' = 'true',"
                        + " 'table.merge-engine' = 'aggregation', 'fields.amount.agg' = 'sum')");
        assertEligibilityRejection(
                () -> tBatchEnv.executeSql("INSERT INTO bl_agg VALUES (1, 100)"),
                "only supports primary key tables with the default merge engine");

        // 4) a dynamic partition insert (no static partition spec) is rejected
        tBatchEnv.executeSql(
                "CREATE TABLE bl_dyn_part (id INT NOT NULL, name STRING, dt STRING,"
                        + " PRIMARY KEY (id, dt) NOT ENFORCED) PARTITIONED BY (dt)"
                        + " WITH ('sink.bulk-load.enabled' = 'true')");
        assertEligibilityRejection(
                () -> tBatchEnv.executeSql("INSERT INTO bl_dyn_part VALUES (1, 'a', '2026-08-17')"),
                "requires a complete static partition spec");
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static Path[] createTaskManagerTmpRoots() throws IOException {
        return new Path[] {
            Files.createDirectory(taskManagerTmpParent.resolve("root-0")).toAbsolutePath(),
            Files.createDirectory(taskManagerTmpParent.resolve("root-1")).toAbsolutePath()
        };
    }

    private static org.apache.flink.configuration.Configuration
            taskManagerTmpDirectoryConfiguration() {
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();
        configuration.set(
                CoreOptions.TMP_DIRS,
                Arrays.stream(taskManagerTmpRoots)
                        .map(Path::toString)
                        .collect(Collectors.joining(File.pathSeparator)));
        return configuration;
    }

    private static Set<Path> bucketWorkDirectories() {
        Set<Path> directories = new LinkedHashSet<>();
        for (Path tmpRoot : taskManagerTmpRoots) {
            directories.addAll(bucketWorkDirectories(tmpRoot));
        }
        return directories;
    }

    private static Set<Path> bucketWorkDirectories(Path tmpRoot) {
        Set<Path> directories = new LinkedHashSet<>();
        File[] attemptDirectories =
                tmpRoot.toFile()
                        .listFiles(
                                file ->
                                        file.isDirectory()
                                                && file.getName().startsWith("fluss-bulkload-")
                                                && file.getName().contains("-bucket-"));
        if (attemptDirectories == null) {
            return directories;
        }
        for (File attemptDirectory : attemptDirectories) {
            directories.add(attemptDirectory.toPath().toAbsolutePath());
        }
        return directories;
    }

    /** Asserts the rejection of an ineligible BulkLoad statement with the expected reason. */
    private static void assertEligibilityRejection(
            ThrowableAssert.ThrowingCallable statement, String expectedReason) {
        Throwable thrown = catchThrowable(statement);
        assertThat(thrown).as("The statement must fail.").isNotNull();
        assertThat(isInCauseChain(thrown, ValidationException.class))
                .as("A ValidationException must be in the cause chain.")
                .isTrue();
        assertThat(thrown).hasStackTraceContaining(expectedReason);
    }

    private static boolean isInCauseChain(Throwable throwable, Class<? extends Throwable> type) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static List<Object[]> fullImageRows(TableInfo tableInfo, int rowsPerBucket) {
        List<Object[]> rows = new ArrayList<>(tableInfo.getNumBuckets() * rowsPerBucket);
        int[] counts = new int[tableInfo.getNumBuckets()];
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        BucketingFunction bucketing = BucketingFunction.of(null);
        for (int id = 0; rows.size() < tableInfo.getNumBuckets() * rowsPerBucket; id++) {
            String name =
                    id % 29 == 0
                            ? null
                            : (id % 31 == 0 ? "数据-" : "name-") + id + payloadSuffix(id % 43);
            Object[] values = new Object[] {id, name, 10_000L + id};
            InternalRow row =
                    org.apache.fluss.testutils.DataTestUtils.row(tableInfo.getRowType(), values);
            int bucket = bucketing.bucketing(encoder.encodeKey(row), tableInfo.getNumBuckets());
            if (counts[bucket] < rowsPerBucket) {
                rows.add(values);
                counts[bucket]++;
            }
        }
        return rows;
    }

    private static List<Object[]> staticPartitionRows(
            TableInfo tableInfo, String partition, int rowsPerBucket) {
        List<Object[]> rows = new ArrayList<>(tableInfo.getNumBuckets() * rowsPerBucket);
        int[] counts = new int[tableInfo.getNumBuckets()];
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        BucketingFunction bucketing = BucketingFunction.of(null);
        for (int id = 0; rows.size() < tableInfo.getNumBuckets() * rowsPerBucket; id++) {
            Object[] values = new Object[] {id, "partition-value-" + id, partition};
            InternalRow row =
                    org.apache.fluss.testutils.DataTestUtils.row(tableInfo.getRowType(), values);
            int bucket = bucketing.bucketing(encoder.encodeKey(row), tableInfo.getNumBuckets());
            if (counts[bucket] < rowsPerBucket) {
                rows.add(values);
                counts[bucket]++;
            }
        }
        return rows;
    }

    private static String payloadSuffix(int length) {
        StringBuilder suffix = new StringBuilder(length);
        for (int index = 0; index < length; index++) {
            suffix.append((char) ('a' + index % 26));
        }
        return suffix.toString();
    }

    private static List<String> comparableRows(List<Object[]> rows) {
        List<String> comparable = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            comparable.add(String.format("+I[%s, %s, %s]", row[0], row[1], row[2]));
        }
        return comparable;
    }

    private static Map<Integer, Long> latestOffsets(TablePath tablePath, int bucketCount)
            throws Exception {
        return latestOffsets(tablePath, null, bucketCount);
    }

    private static Map<Integer, Long> latestOffsets(
            TablePath tablePath, String partitionName, int bucketCount) throws Exception {
        List<Integer> buckets = new ArrayList<>(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            buckets.add(bucket);
        }
        return partitionName == null
                ? admin.listOffsets(tablePath, buckets, new OffsetSpec.LatestSpec()).all().get()
                : admin.listOffsets(tablePath, partitionName, buckets, new OffsetSpec.LatestSpec())
                        .all()
                        .get();
    }

    private static void assertStandardBulkLoadFiles(
            TablePath tablePath, TableInfo tableInfo, Set<Integer> expectedBuckets)
            throws Exception {
        Path remoteRoot = Paths.get(URI.create(tableInfo.getRemoteDataDir()));
        String tableDirectory = tablePath.getTableName() + "-" + tableInfo.getTableId();
        Path kvTable =
                remoteRoot
                        .resolve("kv")
                        .resolve(tablePath.getDatabaseName())
                        .resolve(tableDirectory);

        Map<Integer, KvSnapshotFileMetadata> snapshots = new HashMap<>();
        for (Path metadataPath : filesEndingWith(kvTable, "_METADATA")) {
            KvSnapshotFileMetadata metadata =
                    JsonSerdeUtils.readValue(
                            Files.readAllBytes(metadataPath),
                            KvSnapshotFileMetadataJsonSerde.INSTANCE);
            assertThat(metadata.getTableBucket().getTableId()).isEqualTo(tableInfo.getTableId());
            assertThat(metadata.getLogOffset()).isPositive();
            assertThat(metadata.getRowCount()).isPositive();
            for (KvSnapshotFileMetadata.FileHandle handle :
                    Stream.concat(
                                    metadata.getSharedFiles().stream(),
                                    metadata.getPrivateFiles().stream())
                            .collect(Collectors.toList())) {
                Path file = Paths.get(URI.create(handle.getPath()));
                assertThat(Files.size(file)).isEqualTo(handle.getSize()).isPositive();
            }
            assertThat(snapshots.put(metadata.getTableBucket().getBucket(), metadata)).isNull();
        }
        assertThat(snapshots.keySet()).containsExactlyInAnyOrderElementsOf(expectedBuckets);
    }

    private static List<Path> filesEndingWith(Path root, String suffix) throws Exception {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(suffix))
                    .collect(Collectors.toList());
        }
    }

    /** Collects {@code expectedCount} rows of a streaming read-back query. */
    private List<String> collectRows(String sql, int expectedCount) throws Exception {
        return collectRowsWithTimeout(tStreamEnv.executeSql(sql).collect(), expectedCount, true);
    }
}
