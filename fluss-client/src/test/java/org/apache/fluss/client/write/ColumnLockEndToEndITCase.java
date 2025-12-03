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

package org.apache.fluss.client.write;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ColumnLockConflictException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end integration tests for Column Lock functionality. */
public class ColumnLockEndToEndITCase {

    private static final String DEFAULT_DB = "test_db";
    private static final int NUM_BUCKETS = 4;
    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setClusterConf(initConfig())
                    .build();

    private Connection conn;
    private Admin admin;
    private Configuration clientConf;

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        return conf;
    }

    /**
     * Test 1: testColumnLock_ConcurrentWriteDifferentColumns
     *
     * <p>Verifies that concurrent writes to different column sets do not conflict.
     */
    @Test
    public void testColumnLock_ConcurrentWriteDifferentColumns() throws Exception {
        // Create table with 4 columns: id, col1, col2, col3
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_concurrent_write");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.INT())
                        .column("col3", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED.key(), "true")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Writer A: write to [id, col1, col2]
        Configuration confA = new Configuration(clientConf);
        confA.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-a");
        Connection connA = ConnectionFactory.createConnection(confA);
        Table tableA = connA.getTable(tablePath);

        UpsertWriter writerA =
                tableA.newUpsert().partialUpdate("id", "col1", "col2").createWriter();

        // Writer B: write to [id, col3] (should succeed - no conflict)
        Configuration confB = new Configuration(clientConf);
        confB.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-b");
        Connection connB = ConnectionFactory.createConnection(confB);
        Table tableB = connB.getTable(tablePath);

        UpsertWriter writerB = tableB.newUpsert().partialUpdate("id", "col3").createWriter();

        // Both writers should be able to write concurrently
        InternalRow rowA = row(1, 100, 200, null);
        InternalRow rowB = row(1, null, null, 300);

        CompletableFuture<?> futureA = writerA.upsert(rowA);
        CompletableFuture<?> futureB = writerB.upsert(rowB);

        // Both should complete successfully
        futureA.get(5, TimeUnit.SECONDS);
        futureB.get(5, TimeUnit.SECONDS);

        writerA.flush();
        writerB.flush();

        // Close writers (releases locks)
        writerA.close();
        writerB.close();
        connA.close();
        connB.close();

        // Verify: locks are released, another writer can acquire
        Configuration confC = new Configuration(clientConf);
        confC.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-c");
        Connection connC = ConnectionFactory.createConnection(confC);
        Table tableC = connC.getTable(tablePath);
        UpsertWriter writerC = tableC.newUpsert().partialUpdate("id", "col1").createWriter();
        writerC.close();
        connC.close();
    }

    /**
     * Test 2: testColumnLock_ConflictDetection
     *
     * <p>Verifies that writes to overlapping column sets are properly blocked.
     */
    @Test
    public void testColumnLock_ConflictDetection() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_conflict_detection");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.INT())
                        .column("col3", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED.key(), "true")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Writer A: lock [id, col1, col2]
        Configuration confA = new Configuration(clientConf);
        confA.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-a");
        Connection connA = ConnectionFactory.createConnection(confA);
        Table tableA = connA.getTable(tablePath);
        UpsertWriter writerA =
                tableA.newUpsert().partialUpdate("id", "col1", "col2").createWriter();

        // Writer B: attempt to lock [id, col2, col3] (has intersection with A: col2)
        Configuration confB = new Configuration(clientConf);
        confB.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-b");
        Connection connB = ConnectionFactory.createConnection(confB);
        Table tableB = connB.getTable(tablePath);

        assertThatThrownBy(
                        () -> tableB.newUpsert().partialUpdate("id", "col2", "col3").createWriter())
                .hasCauseInstanceOf(ColumnLockConflictException.class)
                .hasMessageContaining("Column lock conflict");

        // Writer C: lock [id, col3] (no intersection with A)
        Configuration confC = new Configuration(clientConf);
        confC.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-c");
        Connection connC = ConnectionFactory.createConnection(confC);
        Table tableC = connC.getTable(tablePath);
        UpsertWriter writerC = tableC.newUpsert().partialUpdate("id", "col3").createWriter();

        // Clean up
        writerA.close();
        writerC.close();
        connA.close();
        connB.close();
        connC.close();
    }

    /**
     * Test 3: testColumnLock_AutoRenewalAndRelease
     *
     * <p>Verifies automatic lock renewal and release on writer close.
     */
    @Test
    public void testColumnLock_AutoRenewalAndRelease() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_renewal_release");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(NUM_BUCKETS, "id")
                        .property(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED.key(), "true")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        // Create writer with lock
        Configuration confA = new Configuration(clientConf);
        confA.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-renewal");
        Connection connA = ConnectionFactory.createConnection(confA);
        Table tableA = connA.getTable(tablePath);
        UpsertWriter writer = tableA.newUpsert().partialUpdate("id", "col1").createWriter();

        // Write data for a duration longer than half TTL to trigger renewal
        // TTL = 10s, renewal interval = 3s, so sleep 6s should trigger at least 1 renewal
        for (int i = 0; i < 60; i++) {
            writer.upsert(row(i, 100 + i)).get();
            Thread.sleep(100); // Simulate slow writes (6 seconds total)
        }

        // Lock should still be valid (auto-renewed)
        writer.upsert(row(9999, 9999)).get();
        writer.flush();

        // Close writer
        writer.close();
        connA.close();

        // Verify: lock is released, another writer can acquire
        Configuration confB = new Configuration(clientConf);
        confB.setString(ConfigOptions.CLIENT_WRITER_LOCK_OWNER_ID, "writer-after-release");
        Connection connB = ConnectionFactory.createConnection(confB);
        Table tableB = connB.getTable(tablePath);
        UpsertWriter writer2 = tableB.newUpsert().partialUpdate("id", "col1").createWriter();
        writer2.close();
        connB.close();
    }

    // ==================== Helper Methods ====================

    private long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createDatabase(
                        tablePath.getDatabaseName(),
                        DatabaseDescriptor.EMPTY,
                        true /* ignoreIfExists */)
                .get();
        admin.createTable(tablePath, tableDescriptor, true /* ignoreIfExists */).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }
}
