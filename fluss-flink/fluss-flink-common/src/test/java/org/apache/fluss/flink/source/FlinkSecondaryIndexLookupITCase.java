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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.FlussTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Flink lookup join through secondary indexes.
 *
 * <p>Validates the full path: DDL with secondary-index config -> LookupNormalizer routing ->
 * FlinkLookupFunction/FlinkAsyncLookupFunction -> FlussTable.getSecondaryIndexLookuper -> results.
 */
abstract class FlinkSecondaryIndexLookupITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .build();

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "idx_lookup_db";
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    private StreamExecutionEnvironment execEnv;
    private StreamTableEnvironment tEnv;
    private static Connection conn;
    private static Configuration clientConf;

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
    }

    @BeforeEach
    void before() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("DROP DATABASE %s CASCADE", DEFAULT_DB));
    }

    @Test
    void testSecondaryIndexLookupJoinSingleColumn() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_single ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_single"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com"));
            writer.upsert(row(2, "bob", "bob@x.com"));
            writer.upsert(row(3, "carol", "carol@x.com"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name", row("alice"), row("bob"));
        }

        // First: verify PK lookup works with exact same source pattern
        {
            List<Row> srcData = Arrays.asList(Row.of(1), Row.of(2));
            Schema srcSchema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .columnByExpression("proc", "PROCTIME()")
                            .build();
            RowTypeInfo srcTypeInfo =
                    new RowTypeInfo(new TypeInformation[] {Types.INT}, new String[] {"id"});
            DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(srcTypeInfo);
            tEnv.createTemporaryView("pk_src", tEnv.fromDataStream(srcDs, srcSchema));

            String pkQuery =
                    "SELECT s.id, d.name, d.email FROM pk_src s "
                            + "JOIN dim_single FOR SYSTEM_TIME AS OF s.proc AS d "
                            + "ON s.id = d.id";

            CloseableIterator<Row> pkResult = tEnv.executeSql(pkQuery).collect();
            List<String> pkActual =
                    org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils
                            .collectRowsWithTimeout(pkResult, 2, true);
            assertThat(pkActual).hasSize(2);
        }

        // Second: secondary index lookup with exact same pattern
        {
            registerSourceTable("name_src", new String[] {"name"});

            String idxQuery =
                    "SELECT s.name, d.id, d.email FROM name_src s "
                            + "JOIN dim_single FOR SYSTEM_TIME AS OF s.proc AS d "
                            + "ON s.name = d.name";

            CloseableIterator<Row> idxResult = tEnv.executeSql(idxQuery).collect();
            List<String> idxActual =
                    org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils
                            .collectRowsWithTimeout(idxResult, 2, true);
            List<String> expected =
                    Arrays.asList("+I[alice, 1, alice@x.com]", "+I[bob, 2, bob@x.com]");
            assertThat(idxActual).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testSecondaryIndexLookupJoinCompositeIndex() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_composite ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " age INT,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name_age.columns' = 'name,age',"
                        + " 'secondary-index.idx_name_age.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_composite"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", 25, "alice@x.com"));
            writer.upsert(row(2, "alice", 30, "alice30@x.com"));
            writer.upsert(row(3, "bob", 25, "bob@x.com"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name_age", row("alice", 25), row("bob", 25));
        }

        List<Row> srcData = Arrays.asList(Row.of("alice", 25), Row.of("bob", 25));
        Schema srcSchema =
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo srcTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.STRING, Types.INT},
                        new String[] {"name", "age"});
        DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(srcTypeInfo);
        tEnv.createTemporaryView("composite_src", tEnv.fromDataStream(srcDs, srcSchema));

        String query =
                "SELECT s.name, s.age, d.id, d.email FROM composite_src s "
                        + "JOIN dim_composite FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.name = d.name AND s.age = d.age";

        CloseableIterator<Row> result = tEnv.executeSql(query).collect();
        List<String> expected =
                Arrays.asList("+I[alice, 25, 1, alice@x.com]", "+I[bob, 25, 3, bob@x.com]");
        assertResultsIgnoreOrder(result, expected, true);
    }

    @Test
    void testSecondaryIndexLookupJoinAsync() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_async ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'lookup.async' = 'true',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_async"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com"));
            writer.upsert(row(2, "bob", "bob@x.com"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name", row("alice"), row("bob"));
        }

        registerSourceTable("async_src", new String[] {"name"});

        String query =
                "SELECT s.name, d.id, d.email FROM async_src s "
                        + "JOIN dim_async FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.name = d.name";

        CloseableIterator<Row> result = tEnv.executeSql(query).collect();
        List<String> expected = Arrays.asList("+I[alice, 1, alice@x.com]", "+I[bob, 2, bob@x.com]");
        assertResultsIgnoreOrder(result, expected, true);
    }

    @Test
    void testSecondaryIndexLookupJoinWithProjection() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_proj ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " city STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_proj"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com", "NYC"));
            writer.upsert(row(2, "bob", "bob@x.com", "LA"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name", row("alice"), row("bob"));
        }

        registerSourceTable("proj_src", new String[] {"name"});

        // Only project 'city' from dim — not all columns
        String query =
                "SELECT s.name, d.city FROM proj_src s "
                        + "JOIN dim_proj FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.name = d.name";

        CloseableIterator<Row> result = tEnv.executeSql(query).collect();
        List<String> expected = Arrays.asList("+I[alice, NYC]", "+I[bob, LA]");
        assertResultsIgnoreOrder(result, expected, true);
    }

    @Test
    void testSecondaryIndexLookupJoinWithRemainingFilter() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_filter ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " age INT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_filter"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com", 25));
            writer.upsert(row(2, "bob", "bob@x.com", 30));
            writer.upsert(row(3, "carol", "carol@x.com", 25));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name", row("alice"), row("bob"));
        }

        registerSourceTable("filter_src", new String[] {"name"});

        // Join on name (secondary index) with additional constant filter on age
        String query =
                "SELECT s.name, d.id, d.email FROM filter_src s "
                        + "JOIN dim_filter FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.name = d.name AND d.age = 25";

        CloseableIterator<Row> result = tEnv.executeSql(query).collect();
        // Only alice matches (name='alice' AND age=25); bob has age=30 so filtered out
        List<String> expected = Collections.singletonList("+I[alice, 1, alice@x.com]");
        assertResultsIgnoreOrder(result, expected, true);
    }

    @Test
    void testSecondaryIndexPriorityOverPrefixLookup() throws Exception {
        // Table with composite PK (id, name), bucket key = id (prefix of PK).
        // Also has secondary index on 'email'.
        // Join on 'email' should use SECONDARY_INDEX_LOOKUP (not fail or use prefix).
        tEnv.executeSql(
                "CREATE TABLE dim_priority ("
                        + " id INT NOT NULL,"
                        + " name STRING NOT NULL,"
                        + " email STRING,"
                        + " PRIMARY KEY (id, name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'bucket.key' = 'id',"
                        + " 'secondary-index.idx_email.columns' = 'email',"
                        + " 'secondary-index.idx_email.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_priority"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com"));
            writer.upsert(row(2, "bob", "bob@x.com"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_email", row("alice@x.com"), row("bob@x.com"));
        }

        List<Row> srcData = Arrays.asList(Row.of("alice@x.com"), Row.of("bob@x.com"));
        Schema srcSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo srcTypeInfo =
                new RowTypeInfo(new TypeInformation[] {Types.STRING}, new String[] {"email"});
        DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(srcTypeInfo);
        tEnv.createTemporaryView("priority_src", tEnv.fromDataStream(srcDs, srcSchema));

        String query =
                "SELECT s.email, d.id, d.name FROM priority_src s "
                        + "JOIN dim_priority FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.email = d.email";

        CloseableIterator<Row> result = tEnv.executeSql(query).collect();
        List<String> expected = Arrays.asList("+I[alice@x.com, 1, alice]", "+I[bob@x.com, 2, bob]");
        assertResultsIgnoreOrder(result, expected, true);
    }

    @Test
    void testMixedVisibilitySecondaryIndexesAreStoredInTableMetadata() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_catalog_check ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_sync.columns' = 'name',"
                        + " 'secondary-index.idx_sync.visibility' = 'sync',"
                        + " 'secondary-index.idx_sync.bucket.num' = '2',"
                        + " 'secondary-index.idx_async.columns' = 'email',"
                        + " 'secondary-index.idx_async.visibility' = 'async',"
                        + " 'secondary-index.idx_async.bucket.num' = '5'"
                        + ")");

        // The catalog should expose secondary index columns in the schema indexes
        CatalogTable catalogTable =
                (CatalogTable)
                        tEnv.getCatalog(CATALOG_NAME)
                                .get()
                                .getTable(new ObjectPath(DEFAULT_DB, "dim_catalog_check"));
        assertThat(catalogTable).isNotNull();

        assertThat(catalogTable.getUnresolvedSchema().getColumns()).hasSize(3);

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_catalog_check"))) {
            List<org.apache.fluss.metadata.Schema.Index> indexes =
                    table.getTableInfo().getSchema().getIndexes();
            org.apache.fluss.metadata.Schema.Index syncIndex = findIndex(indexes, "idx_sync");
            org.apache.fluss.metadata.Schema.Index asyncIndex = findIndex(indexes, "idx_async");

            assertThat(syncIndex.getColumnNames()).containsExactly("name");
            assertThat(syncIndex.getVisibility()).isEqualTo(IndexVisibility.SYNC);
            assertThat(syncIndex.getBucketCount()).hasValue(2);
            assertThat(asyncIndex.getColumnNames()).containsExactly("email");
            assertThat(asyncIndex.getVisibility()).isEqualTo(IndexVisibility.ASYNC);
            assertThat(asyncIndex.getBucketCount()).hasValue(5);
        }
    }

    @Test
    void testLookupOnNonIndexColumnFailsWithMessage() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_no_match ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        List<Row> srcData = Collections.singletonList(Row.of("test@x.com"));
        Schema srcSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo srcTypeInfo =
                new RowTypeInfo(new TypeInformation[] {Types.STRING}, new String[] {"email"});
        DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(srcTypeInfo);
        tEnv.createTemporaryView("nomatch_src", tEnv.fromDataStream(srcDs, srcSchema));

        // Join on 'email' which is NOT the PK and NOT a secondary index column
        String query =
                "SELECT s.email, d.id FROM nomatch_src s "
                        + "JOIN dim_no_match FOR SYSTEM_TIME AS OF s.proc AS d "
                        + "ON s.email = d.email";

        assertThatThrownBy(() -> tEnv.executeSql(query)).hasStackTraceContaining("secondary index");
    }

    /**
     * Direct invocation test that verifies the SecondaryIndexLookuper works correctly when called
     * with per-call FlinkAsFlussRow instances (the pattern used by the fixed async function).
     */
    @Test
    void testDirectLookupWithPerCallFlinkAsFlussRow() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE dim_direct ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " email STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket.num' = '3',"
                        + " 'secondary-index.idx_name.columns' = 'name',"
                        + " 'secondary-index.idx_name.bucket.num' = '3'"
                        + ")");

        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "dim_direct"))) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "alice", "alice@x.com"));
            writer.upsert(row(2, "bob", "bob@x.com"));
            writer.flush();
            waitForSecondaryIndex(table, "idx_name", row("alice"), row("bob"));

            Lookuper lookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");

            // Per-call FlinkAsFlussRow (matches fixed FlinkAsyncLookupFunction pattern)
            GenericRowData key1 = new GenericRowData(1);
            key1.setField(0, StringData.fromString("alice"));
            LookupResult r1 = lookuper.lookup(new FlinkAsFlussRow(key1)).get();
            assertThat(r1.getRowList()).as("Per-call lookup for 'alice'").hasSize(1);

            GenericRowData key2 = new GenericRowData(1);
            key2.setField(0, StringData.fromString("bob"));
            LookupResult r2 = lookuper.lookup(new FlinkAsFlussRow(key2)).get();
            assertThat(r2.getRowList()).as("Per-call lookup for 'bob'").hasSize(1);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static org.apache.fluss.metadata.Schema.Index findIndex(
            List<org.apache.fluss.metadata.Schema.Index> indexes, String indexName) {
        return indexes.stream()
                .filter(index -> index.getIndexName().equals(indexName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing secondary index " + indexName));
    }

    private void registerSourceTable(String viewName, String[] joinColumns) {
        List<Row> srcData = Arrays.asList(Row.of("alice"), Row.of("bob"));
        Schema srcSchema =
                Schema.newBuilder()
                        .column(joinColumns[0], DataTypes.STRING())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo srcTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.STRING}, new String[] {joinColumns[0]});
        DataStream<Row> srcDs = execEnv.fromCollection(srcData).returns(srcTypeInfo);
        tEnv.createTemporaryView(viewName, tEnv.fromDataStream(srcDs, srcSchema));
    }

    /**
     * Polls the secondary index via {@link FlussTable#getSecondaryIndexLookuper} until all expected
     * lookup keys return non-empty results.
     */
    private static void waitForSecondaryIndex(
            Table table, String indexName, org.apache.fluss.row.InternalRow... expectedKeys) {
        Lookuper lookuper = ((FlussTable) table).getSecondaryIndexLookuper(indexName);
        for (org.apache.fluss.row.InternalRow key : expectedKeys) {
            waitUntil(
                    () -> !lookuper.lookup(key).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for secondary index entry present for key: " + key);
        }
    }
}
