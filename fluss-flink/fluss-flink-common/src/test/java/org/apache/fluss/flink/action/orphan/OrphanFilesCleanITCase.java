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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.bulkload.BulkLoadBucketWriter;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.bulkload.BulkLoadClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ZkData.BucketSnapshotsZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for orphan files cleanup safety scenarios. */
abstract class OrphanFilesCleanITCase extends AbstractTestBase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(buildClusterConf())
                    .setNumOfTabletServers(1)
                    .build();

    private static Configuration buildClusterConf() {
        Configuration clusterConf = new Configuration();
        clusterConf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 2);
        return clusterConf;
    }

    private static Connection connection;
    private static Admin admin;
    private static String bootstrapServers;

    private CapturingAppender auditAppender;
    private LoggerConfig auditLoggerConfig;
    private Level previousAuditLevel;

    @BeforeAll
    static void beforeAll() {
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        Configuration clientConfig = new Configuration();
        clientConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        connection = ConnectionFactory.createConnection(clientConfig);
        admin = connection.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @BeforeEach
    void setUp() {
        attachAuditAppender();
    }

    @AfterEach
    void tearDown() {
        detachAuditAppender();
    }

    private Path remoteDataRoot() {
        return Paths.get(URI.create(FLUSS_CLUSTER_EXTENSION.getRemoteDataDir()));
    }

    private List<String> auditMessages() {
        return auditAppender.messages();
    }

    private void attachAuditAppender() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
        auditAppender = new CapturingAppender("orphan-clean-it-audit");
        auditAppender.start();
        auditLoggerConfig = config.getLoggerConfig("fluss.orphan.audit");
        previousAuditLevel = auditLoggerConfig.getLevel();
        auditLoggerConfig.setLevel(Level.DEBUG);
        auditLoggerConfig.addAppender(auditAppender, Level.DEBUG, null);
        context.updateLoggers();
    }

    private void detachAuditAppender() {
        if (auditLoggerConfig != null && auditAppender != null) {
            auditLoggerConfig.removeAppender(auditAppender.getName());
            auditLoggerConfig.setLevel(previousAuditLevel);
            ((LoggerContext) LogManager.getContext(false)).updateLoggers();
            auditAppender.stop();
        }
    }

    private static final Duration OLD_ENOUGH = Duration.ofDays(2);

    @Test
    void bulkLoadFilesRemainActiveUntilAbortCompletes() throws Exception {
        String dbName = newDatabaseName("bulkload");
        TablePath tablePath = createPrimaryKeyTable(dbName, "abort_cleanup", 3);
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());

        BulkLoadClient bulkLoadClient = connection.getBulkLoadClient();
        BulkLoadBuildContext buildContext =
                bulkLoadClient.begin(PhysicalTablePath.of(tablePath), null, Duration.ofMinutes(2));
        BulkLoadHandle handle = buildContext.getHandle();
        Path workDirectory = Files.createTempDirectory("fluss-bulkload-orphan-clean-");
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        BucketingFunction bucketingFunction = BucketingFunction.of(null);
        int nextId = 0;
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            try (BulkLoadBucketWriter builder =
                    new BulkLoadBucketWriter(buildContext, bucketId, workDirectory.toFile())) {
                while (true) {
                    InternalRow candidate =
                            row(tableInfo.getRowType(), nextId++, "value-" + bucketId);
                    int actualBucket =
                            bucketingFunction.bucketing(
                                    bucketKeyEncoder.encodeKey(candidate),
                                    tableInfo.getNumBuckets());
                    if (actualBucket == bucketId) {
                        builder.add(candidate);
                        break;
                    }
                }
                builder.finish();
            }
        }
        List<Path> standardFiles = new ArrayList<>();
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            List<Path> bucketFiles = bulkLoadStandardFiles(buildContext, bucketId);
            assertThat(bucketFiles)
                    .anyMatch(path -> path.getFileName().toString().equals("_METADATA"));
            standardFiles.addAll(bucketFiles);
        }
        assertThat(standardFiles).doesNotHaveDuplicates();

        for (Path file : standardFiles) {
            makeOld(file);
        }

        runCleanerForDatabase(false, dbName, "--allow-delete-manifest");
        assertThat(standardFiles).allMatch(file -> Files.exists(file));

        BulkLoadStatus aborted = bulkLoadClient.abort(handle);
        assertThat(aborted.getState()).isEqualTo(BulkLoadState.ABORTED);
        runCleanerForDatabase(false, dbName, "--allow-delete-manifest");
        assertThat(standardFiles).noneMatch(file -> Files.exists(file));
    }

    @Test
    @MultiVersionTest
    void mixedOrphanAndActiveFilesInSameBucket() throws Exception {
        String dbName = newDatabaseName("mixed");
        TablePath tablePath = createLogTable(dbName, "mixed_bucket");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        FsPath remoteLogTabletDir =
                FlussPaths.remoteLogTabletDir(
                        new FsPath(remoteDataRoot().resolve("log").toUri().toString()),
                        PhysicalTablePath.of(tablePath),
                        tableBucket);

        // Two active segments registered in manifest
        String activeId1 = UUID.randomUUID().toString();
        String activeId2 = UUID.randomUUID().toString();
        FsPath manifestPath =
                new FsPath(
                        localPath(remoteLogTabletDir)
                                .resolve("metadata/p0.manifest")
                                .toUri()
                                .toString());
        Path manifest = localPath(manifestPath);
        Files.createDirectories(manifest.getParent());
        String manifestContent =
                "{\"version\":1,"
                        + "\"database\":\"db\","
                        + "\"table\":\"t\","
                        + "\"table_id\":0,"
                        + "\"bucket_id\":0,"
                        + "\"remote_log_segments\":["
                        + "{\"segment_id\":\""
                        + activeId1
                        + "\",\"start_offset\":0,\"end_offset\":99,"
                        + "\"max_timestamp\":0,\"size_in_bytes\":1},"
                        + "{\"segment_id\":\""
                        + activeId2
                        + "\",\"start_offset\":100,\"end_offset\":199,"
                        + "\"max_timestamp\":0,\"size_in_bytes\":1}"
                        + "]}";
        Files.write(manifest, manifestContent.getBytes(StandardCharsets.UTF_8));
        makeOld(manifest);
        upsertManifest(tableBucket, manifestPath, 199L);

        Path activeFile1 = writeSegmentFile(remoteLogTabletDir, activeId1, 0L);
        Path activeFile2 = writeSegmentFile(remoteLogTabletDir, activeId2, 100L);

        // Two orphan segments NOT in manifest
        String orphanId1 = UUID.randomUUID().toString();
        String orphanId2 = UUID.randomUUID().toString();
        Path orphanFile1 = writeSegmentFile(remoteLogTabletDir, orphanId1, 500L);
        Path orphanFile2 = writeSegmentFile(remoteLogTabletDir, orphanId2, 600L);

        runCleanerForDatabase(false, dbName);

        // Active files must survive
        assertThat(Files.exists(activeFile1)).as("active segment 1 must survive cleanup").isTrue();
        assertThat(Files.exists(activeFile2)).as("active segment 2 must survive cleanup").isTrue();

        // Orphan files must be deleted
        assertThat(Files.exists(orphanFile1)).as("orphan segment 1 must be deleted").isFalse();
        assertThat(Files.exists(orphanFile2)).as("orphan segment 2 must be deleted").isFalse();

        // Audit confirms deletions for both orphans
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(orphanFile1.toString()));
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(orphanFile2.toString()));

        // No deletion audit for active files
        assertThat(auditMessages())
                .noneMatch(m -> m.contains("action=deleted") && m.contains(activeFile1.toString()));
        assertThat(auditMessages())
                .noneMatch(m -> m.contains("action=deleted") && m.contains(activeFile2.toString()));
    }

    @Test
    void dryRunDoesNotDeleteFiles() throws Exception {
        String dbName = newDatabaseName("dryrun");
        TablePath tablePath = createLogTable(dbName, "dry_run");
        Path activeSegment = seedActiveBucketManifest(tablePath);
        Path orphan = createOldSegmentFile(tablePath, "99999999999999999999.log");

        runCleanerForDatabase(true, dbName);

        assertThat(Files.exists(orphan)).isTrue();
        assertThat(Files.exists(activeSegment)).isTrue();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=would_delete")
                                        && m.contains("rule=log-segment")
                                        && m.contains(orphan.toString()));
        assertThat(auditMessages()).noneMatch(m -> m.contains("action=deleted"));
        // Catch a regression that targets the active segment with a would_delete intent: the
        // file-existence checks above would silently pass under dry-run even if the planner
        // mis-marked the active segment, because dry-run never touches disk.
        assertThat(auditMessages())
                .noneMatch(
                        m ->
                                m.contains("action=would_delete")
                                        && m.contains(activeSegment.toString()));
    }

    /**
     * Seeds a remote log manifest + matching active segment under a freshly-allocated UUID so the
     * active-file cleanup reaches {@code ManifestReadStatus.RESOLVED} for bucket 0 of the given log
     * table. Returns the active segment's {@code .log} path so callers can assert it survives
     * cleanup.
     *
     * <p>The manifest also establishes one active segment so tests can distinguish it from orphan
     * files in the same bucket.
     */
    private Path seedActiveBucketManifest(TablePath tablePath) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        FsPath remoteLogTabletDir =
                FlussPaths.remoteLogTabletDir(
                        new FsPath(remoteDataRoot().resolve("log").toUri().toString()),
                        PhysicalTablePath.of(tablePath),
                        tableBucket);
        FsPath manifestPath =
                new FsPath(
                        localPath(remoteLogTabletDir)
                                .resolve("metadata/p0.manifest")
                                .toUri()
                                .toString());
        String activeSegmentId = UUID.randomUUID().toString();
        Path activeSegment =
                seedManifestAndSegment(remoteLogTabletDir, manifestPath, activeSegmentId, 0L, 1L);
        upsertManifest(tableBucket, manifestPath, 1L);
        return activeSegment;
    }

    @Test
    void defaultDoesNotEnterOrphanTableDir() throws Exception {
        String dbName = newDatabaseName("defaultskip");
        long tableId = allocateDroppedTableId(dbName, "seed_table");
        createLogTable(dbName, "live_anchor");
        OrphanTableLayout layout =
                createOldOrphanTableLayout(
                        remoteDataRoot(),
                        dbName,
                        tableId,
                        "ghost_table",
                        "99999999999999999999.log");

        runCleanerForAllDatabases(false);

        assertThat(Files.exists(layout.orphanFile)).isTrue();
        assertThat(Files.exists(layout.tableDir)).isTrue();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=skip_orphan_table")
                                        && m.contains("default-conservative")
                                        && m.contains(layout.tableDir.toString()));
    }

    @Test
    void optInCleansOrphanTableDirWhenEnabled() throws Exception {
        String dbName = newDatabaseName("optin");
        long tableId = allocateDroppedTableId(dbName, "seed_table");
        createLogTable(dbName, "live_anchor");
        OrphanTableLayout layout =
                createOldOrphanTableLayout(
                        remoteDataRoot(),
                        dbName,
                        tableId,
                        "ghost_table",
                        "99999999999999999999.log");

        runCleanerForAllDatabases(false, "--allow-clean-orphan-tables");

        assertThat(Files.exists(layout.orphanFile)).isFalse();
        assertThat(Files.exists(layout.tableDir)).isFalse();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(layout.orphanFile.toString()));
    }

    @Test
    void pkOrphanTableRetainsSharedSstEvenWithOptIn() throws Exception {
        String dbName = newDatabaseName("orphankv");
        long tableId = allocateDroppedPrimaryKeyTableId(dbName, "seed_pk_table");
        createLogTable(dbName, "live_anchor");
        OrphanTableLayout layout =
                createOldOrphanKvTableLayout(
                        remoteDataRoot(),
                        dbName,
                        tableId,
                        "ghost_pk_table",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa-orphan.sst");

        runCleanerForDatabase(false, dbName, "--allow-clean-orphan-tables");

        assertThat(Files.exists(layout.orphanFile)).isTrue();
        assertThat(Files.exists(layout.tableDir)).isTrue();
        assertThat(auditMessages())
                .noneMatch(
                        m ->
                                m.contains("rule=kv-shared-sst")
                                        && m.contains(layout.orphanFile.toString()));
    }

    @Test
    void manifestPreservedByDefault() throws Exception {
        String dbName = newDatabaseName("manifest");
        TablePath tablePath = createLogTable(dbName, "manifest_default");
        Path orphanManifest = createOldLogManifestFile(tablePath, "orphan.manifest");

        runCleanerForDatabase(false, dbName);

        assertThat(Files.exists(orphanManifest)).isTrue();
        assertThat(auditMessages())
                .noneMatch(
                        m ->
                                m.contains("rule=log-manifest")
                                        && m.contains(orphanManifest.toString()));
    }

    @Test
    void retainedNonLatestSnapshotPreserved() throws Exception {
        String dbName = newDatabaseName("retained");
        TablePath tablePath = createPrimaryKeyTable(dbName, "retained_pk");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        FsPath remoteKvTabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(remoteDataRoot().resolve("kv").toUri().toString()),
                        PhysicalTablePath.of(tablePath),
                        tableBucket);

        seedKvSnapshots(tableBucket, remoteKvTabletDir, new long[] {1L, 2L, 3L, 4L});

        // Drop a snapshot directory locally without registering it in ZK to model a
        // crash-leftover. The active set is derived from ZK references, so this
        // unreferenced snapshot must still be cleaned — guarding the assertions below
        // from passing trivially when the cleaner fails to scan at all.
        long unreferencedSnapshotId = 99L;
        Path unreferencedSnapshotDir =
                localPath(
                        FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, unreferencedSnapshotId));
        Files.createDirectories(unreferencedSnapshotDir);
        Path unreferencedMeta = unreferencedSnapshotDir.resolve("_METADATA");
        Files.write(unreferencedMeta, new byte[] {0x33});
        makeOld(unreferencedMeta);
        makeOld(unreferencedSnapshotDir);

        runCleanerForDatabase(false, dbName);

        // Every snapshot still referenced in ZK is preserved, regardless of recency.
        assertThat(Files.exists(localPath(FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 1L))))
                .isTrue();
        assertThat(Files.exists(localPath(FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 2L))))
                .isTrue();
        assertThat(Files.exists(localPath(FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 3L))))
                .isTrue();
        assertThat(Files.exists(localPath(FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 4L))))
                .isTrue();
        assertThat(Files.exists(unreferencedSnapshotDir)).isFalse();
    }

    @Test
    void listPartitionInfosFailureScopesToSingleTable() throws Exception {
        String dbName = newDatabaseName("partfail");
        PartitionedTableLayout tableA = createPartitionedLogTable(dbName, "table_a", "pa");
        PartitionedTableLayout tableB = createPartitionedLogTable(dbName, "table_b", "pb");

        long orphanPartitionIdForA =
                Math.max(
                        tableA.partitionInfo.getPartitionId(),
                        tableB.partitionInfo.getPartitionId());
        long orphanPartitionIdForB =
                Math.min(
                        tableA.partitionInfo.getPartitionId(),
                        tableB.partitionInfo.getPartitionId());

        OrphanPartitionLayout orphanA =
                createOldOrphanPartitionLayout(
                        remoteDataRoot(),
                        tableA.tablePath,
                        tableA.tableId,
                        "ghost-a",
                        orphanPartitionIdForA,
                        "99999999999999999999.log");
        OrphanPartitionLayout orphanB =
                createOldOrphanPartitionLayout(
                        remoteDataRoot(),
                        tableB.tablePath,
                        tableB.tableId,
                        "ghost-b",
                        orphanPartitionIdForB,
                        "99999999999999999999.log");

        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        String brokenPartitionPath =
                PartitionZNode.path(tableA.tablePath, tableA.partitionInfo.getPartitionName());
        byte[] originalPartitionBytes =
                zk.getCuratorClient().getData().forPath(brokenPartitionPath);
        zk.getCuratorClient()
                .setData()
                .forPath(brokenPartitionPath, "not-json".getBytes(StandardCharsets.UTF_8));
        try {
            runCleanerForDatabase(false, dbName, "--allow-clean-orphan-partitions");
        } finally {
            zk.getCuratorClient().setData().forPath(brokenPartitionPath, originalPartitionBytes);
        }

        assertThat(Files.exists(orphanA.partitionDir)).isTrue();
        assertThat(Files.exists(orphanA.orphanFile)).isTrue();
        assertThat(Files.exists(orphanB.partitionDir)).isFalse();
        assertThat(Files.exists(orphanB.orphanFile)).isFalse();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=skip_partition_list")
                                        && m.contains("table=" + tableA.tablePath.getTableName()));
    }

    @Test
    void multipleRoundsConvergeAfterManifestUpsert() throws Exception {
        String dbName = newDatabaseName("converge");
        TablePath tablePath = createLogTable(dbName, "converge_log");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        FsPath remoteLogTabletDir =
                FlussPaths.remoteLogTabletDir(
                        new FsPath(remoteDataRoot().resolve("log").toUri().toString()),
                        PhysicalTablePath.of(tablePath),
                        tableBucket);

        String segmentId = UUID.randomUUID().toString();
        FsPath manifest0 =
                new FsPath(
                        localPath(remoteLogTabletDir)
                                .resolve("metadata/p0.manifest")
                                .toUri()
                                .toString());
        Path oldSegment = seedManifestAndSegment(remoteLogTabletDir, manifest0, segmentId, 0L, 1L);
        upsertManifest(tableBucket, manifest0, 1L);

        runCleanerForDatabase(false, dbName);

        assertThat(Files.exists(oldSegment)).isTrue();

        FsPath manifest1 =
                new FsPath(
                        localPath(remoteLogTabletDir)
                                .resolve("metadata/p1.manifest")
                                .toUri()
                                .toString());
        Path newSegment =
                seedManifestAndSegment(remoteLogTabletDir, manifest1, segmentId, 100L, 101L);
        upsertManifest(tableBucket, manifest1, 101L);

        runCleanerForDatabase(false, dbName);

        assertThat(Files.exists(oldSegment)).isFalse();
        assertThat(Files.exists(newSegment)).isTrue();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(oldSegment.toString()));
    }

    @Test
    void singleTableModeSkipsOrphanTableScan() throws Exception {
        String dbName = newDatabaseName("singletable");
        long orphanTableId = allocateDroppedTableId(dbName, "orphan_seed");
        TablePath liveTable = createLogTable(dbName, "live_target");
        OrphanTableLayout layout =
                createOldOrphanTableLayout(
                        remoteDataRoot(),
                        dbName,
                        orphanTableId,
                        "ghost_table",
                        "99999999999999999999.log");

        runCleanerForDatabase(
                false, dbName, "--table", liveTable.getTableName(), "--allow-clean-orphan-tables");

        // The orphan-table scan must skip because tableInfosComplete=false in --table
        // single-table mode.
        // Sibling orphan must be preserved even with --allow-clean-orphan-tables set.
        assertThat(Files.exists(layout.orphanFile)).isTrue();
        assertThat(Files.exists(layout.tableDir)).isTrue();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=skip_orphan_table_scan")
                                        && m.contains("reason=tableInfos-incomplete")
                                        && m.contains("db=" + dbName));
        // Must use the dedicated event, not the older skip_db.
        assertThat(auditMessages())
                .noneMatch(m -> m.contains("action=skip_db") && m.contains("db=" + dbName));
    }

    @Test
    void failedKvReferenceListingAbandonsWholePhysicalTarget() throws Exception {
        String dbName = newDatabaseName("crossflow");
        TablePath tablePath = createPrimaryKeyTable(dbName, "fail_kv_keep_log");
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);

        // Seed a valid KV snapshot in ZK so listBucketSnapshots returns a child to decode.
        FsPath remoteKvTabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(remoteDataRoot().resolve("kv").toUri().toString()),
                        PhysicalTablePath.of(tablePath),
                        tableBucket);
        long activeSnapshotId = 1L;
        seedKvSnapshots(tableBucket, remoteKvTabletDir, new long[] {activeSnapshotId});

        // Seed a log manifest + active segment so the log bucket reaches RESOLVED in the
        // active-file cleanup.
        Path activeLogSegment = seedActiveBucketManifest(tablePath);

        // -----------------------------------------------------------------
        // Step 1 — baseline (no fault injection)
        // Plant an orphan KV snapshot dir under snap-99 (NOT registered in ZK) plus an
        // orphan log segment. With the cluster wired normally, cleanup MUST delete them:
        // this establishes the negative control that proves the phase-2 preservation
        // claim is meaningful and not just an accidental no-op.
        // -----------------------------------------------------------------
        long baselineOrphanSnapshotId = 99L;
        FsPath baselineOrphanKvDir =
                FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, baselineOrphanSnapshotId);
        Path baselineOrphanKvMetadata = localPath(baselineOrphanKvDir).resolve("_METADATA");
        Path baselineOrphanKvSst =
                localPath(baselineOrphanKvDir).resolve(baselineOrphanSnapshotId + ".sst");
        Files.createDirectories(localPath(baselineOrphanKvDir));
        Files.write(baselineOrphanKvMetadata, new byte[] {0x55});
        Files.write(baselineOrphanKvSst, new byte[] {0x66});
        makeOld(baselineOrphanKvMetadata);
        makeOld(baselineOrphanKvSst);

        Path baselineOrphanLogSegment = createOldSegmentFile(tablePath, "99999999999999999999.log");

        runCleanerForDatabase(false, dbName);

        // Baseline: snap-99 files were DELETED, proving normal cleanup would have killed
        // them. Path-specific assertions guarantee these audit events refer to phase 1.
        assertThat(Files.exists(baselineOrphanKvMetadata))
                .as(
                        "phase 1 baseline: snap-99/_METADATA must be DELETED "
                                + "(cleanup would normally remove orphan KV files)")
                .isFalse();
        assertThat(Files.exists(baselineOrphanKvSst))
                .as("phase 1 baseline: snap-99/<id>.sst must be DELETED")
                .isFalse();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=kv-snapshot-file")
                                        && m.contains(baselineOrphanKvMetadata.toString()));
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=kv-snapshot-file")
                                        && m.contains(baselineOrphanKvSst.toString()));
        // Baseline: orphan log segment was DELETED and the active segment survived. Phase 1's
        // log deletion is asserted both via Files.exists and via the audit stream so the final
        // phase-2 assertion can require EXACTLY ONE deletion event on the same path (phase 2
        // must not add another).
        assertThat(Files.exists(baselineOrphanLogSegment))
                .as("phase 1 baseline: orphan log segment must be DELETED")
                .isFalse();
        assertThat(Files.exists(activeLogSegment))
                .as("phase 1: active log segment must survive cleanup")
                .isTrue();
        assertThat(auditMessages())
                .filteredOn(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(baselineOrphanLogSegment.toString()))
                .as("phase 1 baseline: orphan log segment deletion must appear in audit stream")
                .hasSizeGreaterThanOrEqualTo(1);

        // -----------------------------------------------------------------
        // Step 2 — fault injection
        // Re-plant orphan KV files under a DIFFERENT snap-77 dir so path-specific audit
        // assertions are unambiguous (phase-1 audits target snap-99, phase-2 audits
        // target snap-77). Re-plant the orphan log segment at its original path (phase 1
        // deleted it) so we can verify the whole physical target is abandoned — its log unit
        // included — when KV active references cannot be listed consistently.
        // -----------------------------------------------------------------
        long faultInjectionOrphanSnapshotId = 77L;
        FsPath faultInjectionOrphanKvDir =
                FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, faultInjectionOrphanSnapshotId);
        Path faultInjectionOrphanKvMetadata =
                localPath(faultInjectionOrphanKvDir).resolve("_METADATA");
        Path faultInjectionOrphanKvSst =
                localPath(faultInjectionOrphanKvDir)
                        .resolve(faultInjectionOrphanSnapshotId + ".sst");
        Files.createDirectories(localPath(faultInjectionOrphanKvDir));
        Files.write(faultInjectionOrphanKvMetadata, new byte[] {0x55});
        Files.write(faultInjectionOrphanKvSst, new byte[] {0x66});
        makeOld(faultInjectionOrphanKvMetadata);
        makeOld(faultInjectionOrphanKvSst);

        // Re-planted at the SAME path as baselineOrphanLogSegment (createOldSegmentFile uses a
        // fixed UUID + filename). Under the fail-closed LIST_KV_SNAPSHOTS contract the audit
        // stream keeps exactly ONE delete event for this path (phase 1's); the final
        // filteredOn(...).hasSize(1) assertion below pins that phase 2 adds none.
        Path faultInjectionOrphanLogSegment =
                createOldSegmentFile(tablePath, "99999999999999999999.log");

        // Inject a non-numeric child znode under BucketSnapshotsZNode so the server-side
        // snapshot-id listing hits NumberFormatException on Long.parseLong. The server fails
        // closed: the bounded read-recheck retries keep failing and the RPC returns the
        // retriable REQUEST_TIME_OUT error, so the cleaner emits skip_kv_target and abandons the
        // whole physical target for this round (design doc section 14.1).
        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        String invalidChildPath = BucketSnapshotsZNode.path(tableBucket) + "/not-a-long";
        zk.getCuratorClient().create().forPath(invalidChildPath, new byte[0]);
        try {
            runCleanerForDatabase(false, dbName);
        } finally {
            zk.getCuratorClient().delete().forPath(invalidChildPath);
        }

        // KV reference listing failed: skip_kv_target audit fires AND snap-77 orphan files
        // preserved.
        assertThat(auditMessages())
                .as(
                        "phase 2: skip_kv_target audit must fire when KV snapshot references "
                                + "cannot be listed consistently")
                .anyMatch(
                        m ->
                                m.contains("action=skip_kv_target")
                                        && m.contains("reason=RPC failure")
                                        && m.contains("table_id=" + tableInfo.getTableId()));
        assertThat(Files.exists(faultInjectionOrphanKvMetadata))
                .as(
                        "phase 2: snap-77/_METADATA must be PRESERVED "
                                + "(KV target failure must short-circuit cleanup)")
                .isTrue();
        assertThat(Files.exists(faultInjectionOrphanKvSst))
                .as("phase 2: snap-77/<id>.sst must be PRESERVED")
                .isTrue();
        // Defensive: nothing in the audit stream ever marked snap-77 files for deletion.
        assertThat(auditMessages())
                .noneMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=kv-snapshot-file")
                                        && m.contains(faultInjectionOrphanKvMetadata.toString()));
        assertThat(auditMessages())
                .noneMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=kv-snapshot-file")
                                        && m.contains(faultInjectionOrphanKvSst.toString()));

        // Fail-closed whole-target abandonment: a failed KV reference read abandons the
        // ENTIRE physical target for the round, log unit included — a result-unknown BulkLoad
        // commit may already have published remote log segments that a failed reference
        // read cannot distinguish from orphans. The re-planted segment lives at the same path
        // as baselineOrphanLogSegment, so the audit stream must contain exactly ONE deletion
        // event for this path (phase 1's); anyMatch alone could be satisfied by phase 1's
        // event in isolation, which is why we count instead.
        assertThat(Files.exists(faultInjectionOrphanLogSegment))
                .as(
                        "phase 2: orphan log segment must be PRESERVED (failed KV reference "
                                + "listing abandons the whole physical target, design doc section"
                                + " 14.1)")
                .isTrue();
        assertThat(Files.exists(activeLogSegment))
                .as("phase 2: active log segment must still survive cleanup")
                .isTrue();
        assertThat(auditMessages())
                .filteredOn(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(faultInjectionOrphanLogSegment.toString()))
                .as(
                        "orphan log segment must be deleted in phase 1 (baseline) only -- "
                                + "phase 2 abandons the target before any deletion")
                .hasSize(1);
    }

    @Test
    void optInCleansOrphanPartitionDir() throws Exception {
        String dbName = newDatabaseName("orphanpart");
        // Create two partitioned tables so the tracker observes both partition IDs.
        // The second table's partition ID is higher. We plant an orphan under the second
        // table using the first table's (lower) ID so the guard passes:
        // orphanId <= maxKnownPartitionId.
        PartitionedTableLayout tableA = createPartitionedLogTable(dbName, "table_a", "pa");
        PartitionedTableLayout tableB = createPartitionedLogTable(dbName, "table_b", "pb");

        long orphanPartitionId =
                Math.min(
                        tableA.partitionInfo.getPartitionId(),
                        tableB.partitionInfo.getPartitionId());
        // Plant orphan under whichever table does NOT own the lower-ID partition.
        PartitionedTableLayout targetTable =
                (tableA.partitionInfo.getPartitionId() == orphanPartitionId) ? tableB : tableA;

        OrphanPartitionLayout orphan =
                createOldOrphanPartitionLayout(
                        remoteDataRoot(),
                        targetTable.tablePath,
                        targetTable.tableId,
                        "ghost",
                        orphanPartitionId,
                        "99999999999999999999.log");

        runCleanerForDatabase(false, dbName, "--allow-clean-orphan-partitions");

        assertThat(Files.exists(orphan.orphanFile))
                .as("orphan partition file must be deleted")
                .isFalse();
        assertThat(Files.exists(orphan.partitionDir))
                .as("orphan partition dir must be removed")
                .isFalse();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(orphan.orphanFile.toString()));
    }

    @Test
    void emptyDirsSweptAfterOrphanFileDeletion() throws Exception {
        String dbName = newDatabaseName("emptydir");
        TablePath tablePath = createLogTable(dbName, "emptydir_table");
        Path activeSegment = seedActiveBucketManifest(tablePath);

        // Create an orphan file as the sole content of its UUID directory.
        Path orphan = createOldSegmentFile(tablePath, "99999999999999999999.log");
        Path orphanSegmentDir = orphan.getParent();

        // Pre-condition: the segment directory exists before cleanup.
        assertThat(Files.exists(orphanSegmentDir)).isTrue();

        runCleanerForDatabase(false, dbName);

        // The orphan file must be deleted.
        assertThat(Files.exists(orphan)).as("orphan file must be deleted").isFalse();
        // The now-empty UUID directory must also be swept.
        assertThat(Files.exists(orphanSegmentDir))
                .as("empty segment dir must be swept after cleanup")
                .isFalse();
        // Active segment and its directory survive.
        assertThat(Files.exists(activeSegment)).as("active segment must survive").isTrue();
        assertThat(auditMessages())
                .anyMatch(
                        m ->
                                m.contains("action=deleted")
                                        && m.contains("rule=log-segment")
                                        && m.contains(orphan.toString()));
    }

    private TablePath createLogTable(String databaseName, String tableName) throws Exception {
        admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, true).get();
        TablePath tablePath = TablePath.of(databaseName, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        admin.createTable(tablePath, descriptor, true).get();
        return tablePath;
    }

    private TablePath createPrimaryKeyTable(String databaseName, String tableName)
            throws Exception {
        return createPrimaryKeyTable(databaseName, tableName, 1);
    }

    private TablePath createPrimaryKeyTable(String databaseName, String tableName, int bucketCount)
            throws Exception {
        admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, true).get();
        TablePath tablePath = TablePath.of(databaseName, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(bucketCount, "id").build();
        admin.createTable(tablePath, descriptor, true).get();
        return tablePath;
    }

    private long allocateDroppedTableId(String databaseName, String tableName) throws Exception {
        TablePath tablePath = createLogTable(databaseName, tableName);
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        admin.dropTable(tablePath, false).get();
        return tableId;
    }

    private long allocateDroppedPrimaryKeyTableId(String databaseName, String tableName)
            throws Exception {
        TablePath tablePath = createPrimaryKeyTable(databaseName, tableName);
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        admin.dropTable(tablePath, false).get();
        return tableId;
    }

    private Path createOldSegmentFile(TablePath tablePath, String fileName) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.fs.FsPath tabletDir =
                FlussPaths.remoteLogTabletDir(
                        new org.apache.fluss.fs.FsPath(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir()
                                        + "/"
                                        + FlussPaths.REMOTE_LOG_DIR_NAME),
                        PhysicalTablePath.of(tablePath),
                        new TableBucket(tableInfo.getTableId(), 0));
        Path segmentDir =
                Paths.get(java.net.URI.create(tabletDir.toString()))
                        .resolve(
                                UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").toString());
        Files.createDirectories(segmentDir);
        Path file = segmentDir.resolve(fileName);
        Files.write(file, new byte[] {0x42});
        makeOld(file);
        makeOld(segmentDir);
        return file;
    }

    private Path createOldLogManifestFile(TablePath tablePath, String fileName) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.fs.FsPath tabletDir =
                FlussPaths.remoteLogTabletDir(
                        new org.apache.fluss.fs.FsPath(
                                FLUSS_CLUSTER_EXTENSION.getRemoteDataDir()
                                        + "/"
                                        + FlussPaths.REMOTE_LOG_DIR_NAME),
                        PhysicalTablePath.of(tablePath),
                        new TableBucket(tableInfo.getTableId(), 0));
        Path metadataDir = Paths.get(java.net.URI.create(tabletDir.toString())).resolve("metadata");
        Files.createDirectories(metadataDir);
        Path file = metadataDir.resolve(fileName);
        Files.write(file, new byte[] {0x11});
        makeOld(file);
        return file;
    }

    private PartitionedTableLayout createPartitionedLogTable(
            String databaseName, String tableName, String partitionValue) throws Exception {
        admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, true).get();
        TablePath tablePath = TablePath.of(databaseName, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .partitionedBy("pt")
                        .build();
        admin.createTable(tablePath, descriptor, true).get();
        admin.createPartition(tablePath, partitionSpec("pt", partitionValue), false).get();

        Map<String, Long> partitionIds =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        long partitionId = partitionIds.get(partitionValue);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableInfo.getTableId(), partitionId);
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos).hasSize(1);
        return new PartitionedTableLayout(tablePath, tableInfo.getTableId(), partitionInfos.get(0));
    }

    private void seedKvSnapshots(
            TableBucket tableBucket, FsPath remoteKvTabletDir, long[] snapshotIds)
            throws Exception {
        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        for (long snapshotId : snapshotIds) {
            FsPath snapshotDir = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId);
            Path localSnapshotDir = localPath(snapshotDir);
            Files.createDirectories(localSnapshotDir);

            Path metadataFile = localSnapshotDir.resolve("_METADATA");
            Files.write(metadataFile, new byte[] {0x33});
            makeOld(metadataFile);

            Path dataFile = localSnapshotDir.resolve(snapshotId + ".sst");
            Files.write(dataFile, new byte[] {0x44});
            makeOld(dataFile);

            makeOld(localSnapshotDir);

            zk.registerTableBucketSnapshot(
                    tableBucket,
                    new BucketSnapshot(
                            snapshotId, snapshotId, snapshotDir.toString() + "/_METADATA"));
        }
    }

    private Path seedManifestAndSegment(
            FsPath remoteLogTabletDir,
            FsPath manifestPath,
            String segmentId,
            long startOffset,
            long endOffset)
            throws Exception {
        Path manifest = localPath(manifestPath);
        Files.createDirectories(manifest.getParent());
        Files.write(
                manifest,
                manifestJson(segmentId, startOffset, endOffset).getBytes(StandardCharsets.UTF_8));
        makeOld(manifest);

        FsPath segmentDir = new FsPath(remoteLogTabletDir, segmentId);
        Path localSegmentDir = localPath(segmentDir);
        Files.createDirectories(localSegmentDir);
        Path logFile =
                localSegmentDir.resolve(FlussPaths.filenamePrefixFromOffset(startOffset) + ".log");
        Files.write(logFile, new byte[] {0x55});
        makeOld(logFile);
        return logFile;
    }

    private Path writeSegmentFile(FsPath remoteLogTabletDir, String segmentId, long startOffset)
            throws Exception {
        FsPath segmentDir = new FsPath(remoteLogTabletDir, segmentId);
        Path localSegmentDir = localPath(segmentDir);
        Files.createDirectories(localSegmentDir);
        Path logFile =
                localSegmentDir.resolve(FlussPaths.filenamePrefixFromOffset(startOffset) + ".log");
        Files.write(logFile, new byte[] {0x55});
        makeOld(logFile);
        return logFile;
    }

    private void upsertManifest(TableBucket tableBucket, FsPath manifestPath, long endOffset)
            throws Exception {
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .upsertRemoteLogManifestHandle(
                        tableBucket, new RemoteLogManifestHandle(manifestPath, endOffset));
    }

    private void runCleanerForDatabase(boolean dryRun, String databaseName, String... extraArgs)
            throws Exception {
        List<String> args = new ArrayList<String>();
        args.add("--bootstrap-server");
        args.add(bootstrapServers);
        args.add("--database");
        args.add(databaseName);
        appendCommonArgs(args, dryRun, extraArgs);
        OrphanCleanConfig config =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                args.toArray(new String[args.size()])));
        new OrphanFilesCleanAction(config).run();
    }

    private void runCleanerForAllDatabases(boolean dryRun, String... extraArgs) throws Exception {
        List<String> args = new ArrayList<String>();
        args.add("--bootstrap-server");
        args.add(bootstrapServers);
        args.add("--all-databases");
        appendCommonArgs(args, dryRun, extraArgs);
        OrphanCleanConfig config =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                args.toArray(new String[args.size()])));
        new OrphanFilesCleanAction(config).run();
    }

    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static void appendCommonArgs(List<String> args, boolean dryRun, String... extraArgs) {
        // Tests back-date their orphan files to now - 2d via makeOld(); a cutoff at now - 1d
        // safely puts those files strictly before the cutoff (mtime < cutoff → DELETE-eligible).
        String cutoff = OffsetDateTime.now(ZoneOffset.UTC).minusDays(1).format(CUTOFF_FORMATTER);
        args.add("--older-than");
        args.add(cutoff);
        for (String extraArg : extraArgs) {
            args.add(extraArg);
        }
        if (dryRun) {
            args.add("--dry-run");
        }
    }

    private OrphanPartitionLayout createOldOrphanPartitionLayout(
            Path remoteRoot,
            TablePath tablePath,
            long tableId,
            String partitionName,
            long partitionId,
            String fileName)
            throws Exception {
        Path tableDir =
                remoteRoot
                        .resolve("log")
                        .resolve(tablePath.getDatabaseName())
                        .resolve(tablePath.getTableName() + "-" + tableId);
        Path partitionDir = tableDir.resolve(partitionName + "-p" + partitionId);
        Path segmentDir =
                partitionDir
                        .resolve("0")
                        .resolve(
                                UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").toString());
        Files.createDirectories(segmentDir);
        Path orphanFile = segmentDir.resolve(fileName);
        Files.write(orphanFile, new byte[] {0x66});
        makeOld(orphanFile);
        makeOld(segmentDir);
        makeOld(segmentDir.getParent());
        makeOld(partitionDir);
        return new OrphanPartitionLayout(partitionDir, orphanFile);
    }

    private OrphanTableLayout createOldOrphanTableLayout(
            Path remoteRoot, String dbName, long tableId, String tableName, String fileName)
            throws Exception {
        Path tableDir =
                remoteRoot.resolve("log").resolve(dbName).resolve(tableName + "-" + tableId);
        Path segmentDir =
                tableDir.resolve("0")
                        .resolve(
                                UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").toString());
        Files.createDirectories(segmentDir);
        Path orphanFile = segmentDir.resolve(fileName);
        Files.write(orphanFile, new byte[] {0x42});
        makeOld(orphanFile);
        makeOld(segmentDir);
        makeOld(segmentDir.getParent());
        makeOld(tableDir);
        return new OrphanTableLayout(tableDir, orphanFile);
    }

    private OrphanTableLayout createOldOrphanKvTableLayout(
            Path remoteRoot, String dbName, long tableId, String tableName, String fileName)
            throws Exception {
        Path tableDir = remoteRoot.resolve("kv").resolve(dbName).resolve(tableName + "-" + tableId);
        Path sharedDir = tableDir.resolve("0").resolve("shared");
        Files.createDirectories(sharedDir);
        Path orphanFile = sharedDir.resolve(fileName);
        Files.write(orphanFile, new byte[] {0x24});
        makeOld(orphanFile);
        makeOld(sharedDir);
        makeOld(sharedDir.getParent());
        makeOld(tableDir);
        return new OrphanTableLayout(tableDir, orphanFile);
    }

    private static String newDatabaseName(String prefix) {
        return prefix + Long.toString(System.nanoTime());
    }

    private static PartitionSpec partitionSpec(String key, String value) {
        return new PartitionSpec(Collections.singletonMap(key, value));
    }

    private static Path localPath(FsPath path) {
        return Paths.get(java.net.URI.create(path.toString()));
    }

    private static List<Path> bulkLoadStandardFiles(BulkLoadBuildContext context, int bucketId)
            throws Exception {
        List<Path> paths = new ArrayList<>();
        BulkLoadHandle handle = context.getHandle();
        TableBucket tableBucket =
                new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
        String remoteDataDir = context.getTableInfo().getRemoteDataDir();
        addRegularFiles(
                localPath(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                                handle.getTarget(),
                                tableBucket)),
                paths);
        return paths;
    }

    private static void addRegularFiles(Path root, List<Path> files) throws Exception {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(path -> Files.isRegularFile(path)).forEach(files::add);
        }
    }

    private static String manifestJson(String segmentId, long startOffset, long endOffset) {
        return "{\"version\":1,"
                + "\"database\":\"db\","
                + "\"table\":\"t\","
                + "\"table_id\":0,"
                + "\"bucket_id\":0,"
                + "\"remote_log_segments\":[{"
                + "\"segment_id\":\""
                + segmentId
                + "\",\"start_offset\":"
                + startOffset
                + ",\"end_offset\":"
                + endOffset
                + ",\"max_timestamp\":0,"
                + "\"size_in_bytes\":1"
                + "}]}";
    }

    private void makeOld(Path path) throws Exception {
        Files.setLastModifiedTime(
                path, FileTime.fromMillis(System.currentTimeMillis() - OLD_ENOUGH.toMillis()));
    }

    private static final class PartitionedTableLayout {
        private final TablePath tablePath;
        private final long tableId;
        private final PartitionInfo partitionInfo;

        private PartitionedTableLayout(
                TablePath tablePath, long tableId, PartitionInfo partitionInfo) {
            this.tablePath = tablePath;
            this.tableId = tableId;
            this.partitionInfo = partitionInfo;
        }
    }

    private static final class OrphanPartitionLayout {
        private final Path partitionDir;
        private final Path orphanFile;

        private OrphanPartitionLayout(Path partitionDir, Path orphanFile) {
            this.partitionDir = partitionDir;
            this.orphanFile = orphanFile;
        }
    }

    private static final class OrphanTableLayout {
        private final Path tableDir;
        private final Path orphanFile;

        private OrphanTableLayout(Path tableDir, Path orphanFile) {
            this.tableDir = tableDir;
            this.orphanFile = orphanFile;
        }
    }

    private static final class CapturingAppender extends AbstractAppender {

        private final List<String> messages = new CopyOnWriteArrayList<String>();

        CapturingAppender(String name) {
            super(
                    name,
                    null,
                    null,
                    true,
                    org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        List<String> messages() {
            return new ArrayList<String>(messages);
        }
    }
}
