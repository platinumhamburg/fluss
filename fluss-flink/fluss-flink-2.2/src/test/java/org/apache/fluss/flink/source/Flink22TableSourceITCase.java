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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link FlinkTableSource} in Flink 2.2. */
public class Flink22TableSourceITCase extends FlinkTableSourceITCase {

    @Test
    void testDeltaJoin() throws Exception {
        // start two jobs for this test: one for DML involving the delta join, and the other for DQL
        // to query the results of the sink table
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                // currently, delta join only support append-only source
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L),
                        row(4, "v4", 400L, 4, 40000L));
        // write records
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                // currently, delta join only support append-only source
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v3", 200L, 2, 20000L),
                        row(3, "v4", 300L, 4, 30000L),
                        row(4, "v4", 500L, 4, 50000L));
        // write records
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c1, d1, c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "-U[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "+U[1, v1, 100, 1, 10000, 1, v1, 100, 1, 10000]",
                        "+I[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]",
                        "-U[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]",
                        "+U[2, v2, 200, 2, 20000, 2, v3, 200, 2, 20000]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithProjectionAndFilter() throws Exception {
        // start two jobs for this test: one for DML involving the delta join, and the other for DQL
        // to query the results of the sink table
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " b1 varchar, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " e1 bigint, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v2", 200L, 2, 20000L),
                        row(3, "v1", 300L, 3, 30000L));
        TablePath leftTablePath = TablePath.of(DEFAULT_DB, leftTableName);
        writeRows(conn, leftTablePath, rows1, false);

        String rightTableName = "right_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " b2 varchar, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " e2 bigint, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 =
                Arrays.asList(
                        row(1, "v1", 100L, 1, 10000L),
                        row(2, "v3", 200L, 2, 20000L),
                        row(3, "v4", 300L, 4, 30000L));
        TablePath rightTablePath = TablePath.of(DEFAULT_DB, rightTableName);
        writeRows(conn, rightTablePath, rows2, false);

        String sinkTableName = "sink_table_proj";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " a2 int, "
                                + " primary key (c1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Test with projection and filter
        String sql =
                String.format(
                        "INSERT INTO %s SELECT a1, c1, a2 FROM %s INNER JOIN %s ON c1 = c2 AND d1 = d2 WHERE a1 > 1",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected = Arrays.asList("+I[2, 200, 2]", "-U[2, 200, 2]", "+U[2, 200, 2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testDeltaJoinWithLookupCache() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        String leftTableName = "left_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " c1 bigint, "
                                + " d1 int, "
                                + " primary key (c1, d1) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c1', "
                                + " 'table.merge-engine' = 'first_row' "
                                + ")",
                        leftTableName));
        List<InternalRow> rows1 = Arrays.asList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, leftTableName), rows1, false);

        String rightTableName = "right_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a2 int, "
                                + " c2 bigint, "
                                + " d2 int, "
                                + " primary key (c2, d2) NOT ENFORCED"
                                + ") with ("
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'c2', "
                                + " 'table.merge-engine' = 'first_row', "
                                + " 'lookup.cache' = 'partial', "
                                + " 'lookup.partial-cache.max-rows' = '100' "
                                + ")",
                        rightTableName));
        List<InternalRow> rows2 = Arrays.asList(row(1, 100L, 1));
        writeRows(conn, TablePath.of(DEFAULT_DB, rightTableName), rows2, false);

        String sinkTableName = "sink_table_cache";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a1 int, "
                                + " a2 int, "
                                + " primary key (a1) NOT ENFORCED" // Dummy PK
                                + ") with ("
                                + " 'connector' = 'fluss' "
                                + ")",
                        sinkTableName));

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                String.format(
                        "INSERT INTO %s SELECT T1.a1, T2.a2 FROM %s AS T1 INNER JOIN %s AS T2 ON T1.c1 = T2.c2 AND T1.d1 = T2.d2",
                        sinkTableName, leftTableName, rightTableName);

        assertThat(tEnv.explainSql(sql))
                .contains("DeltaJoin(joinType=[InnerJoin], where=[((c1 = c2) AND (d1 = d2))]");

        tEnv.executeSql(sql);

        CloseableIterator<Row> collected =
                tEnv.executeSql(String.format("select * from %s", sinkTableName)).collect();
        List<String> expected = Arrays.asList("+I[1, 1]", "-U[1, 1]", "+U[1, 1]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * Reproduces a real-world three-table delta join scenario that fails during optimization.
     *
     * <p>Pattern: parking INNER JOIN clip ON parking.clip_id = clip.id INNER JOIN dim_vehicle ON
     * clip.vehicle_model = dim_vehicle.vehicle_model
     *
     * <p>All tables have secondary indexes on the join columns. However, Flink 2.2 does not support
     * cascade joins (A JOIN B JOIN C) in delta join optimization. Each join input must come
     * directly from a table source. This test verifies that the expected ValidationException is
     * thrown when attempting to use FORCE delta join strategy with cascade joins.
     */
    @Test
    void testThreeTableDeltaJoinWithInnerJoin() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        // Table A: parking-like table, PK=id, bucket.key=id
        // Secondary index on clip_id (used as join key to clip)
        tEnv.executeSql(
                "create table parking ("
                        + " id varchar, "
                        + " clip_id varchar, "
                        + " val1 int, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'clip_id' "
                        + ")");

        // Table B: clip-like table, PK=id, bucket.key=id
        // Secondary index on vehicle_model (used as join key to dim_vehicle)
        tEnv.executeSql(
                "create table clip ("
                        + " id varchar, "
                        + " vehicle_model int, "
                        + " val2 varchar, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'vehicle_model' "
                        + ")");

        // Table C: dim_vehicle-like table, PK=id, bucket.key=id
        // Secondary index on vehicle_model
        tEnv.executeSql(
                "create table dim_vehicle ("
                        + " id int, "
                        + " vehicle_model int, "
                        + " model_name varchar, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'vehicle_model' "
                        + ")");

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        String sql =
                "INSERT INTO parking "
                        + "SELECT p.id, p.clip_id, d.vehicle_model "
                        + "FROM parking p "
                        + "INNER JOIN clip c ON p.clip_id = c.id "
                        + "INNER JOIN dim_vehicle d ON c.vehicle_model = d.vehicle_model";

        // Flink 2.2 does not support cascade joins (A JOIN B JOIN C) in delta join.
        // Each join input must come directly from a table source.
        // When FORCE strategy is used, a ValidationException should be thrown.
        assertThatThrownBy(() -> tEnv.explainSql(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
    }

    /**
     * Baseline test: three-table delta join pattern with secondary indexes on join columns.
     *
     * <p>Pattern: parking INNER JOIN clip ON parking.clip_id = clip.id LEFT JOIN dim_vehicle ON
     * clip.vehicle_model = dim_vehicle.vehicle_model
     */
    @Test
    void testThreeTableDeltaJoinBaseline() throws Exception {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        // Table A: parking-like, PK=id, bucket.key=id, secondary index on clip_id
        tEnv.executeSql(
                "create table bl_parking ("
                        + " id varchar, "
                        + " clip_id varchar, "
                        + " val1 int, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'clip_id' "
                        + ")");

        // Table B: clip-like, PK=id, bucket.key=id, secondary index on vehicle_model
        tEnv.executeSql(
                "create table bl_clip ("
                        + " id varchar, "
                        + " vehicle_model int, "
                        + " val2 varchar, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'vehicle_model' "
                        + ")");

        // Table C: dim_vehicle-like, PK=id, bucket.key=id, secondary index on vehicle_model
        tEnv.executeSql(
                "create table bl_dim_vehicle ("
                        + " id int, "
                        + " vehicle_model int, "
                        + " model_name varchar, "
                        + " primary key (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss', "
                        + " 'table.merge-engine' = 'first_row', "
                        + " 'table.secondary-index.columns' = 'vehicle_model' "
                        + ")");

        // Write test data
        TablePath parkingPath = TablePath.of(DEFAULT_DB, "bl_parking");
        writeRows(
                conn,
                parkingPath,
                Arrays.asList(row("p1", "c1", 10), row("p2", "c2", 20), row("p3", "c1", 30)),
                false);

        TablePath clipPath = TablePath.of(DEFAULT_DB, "bl_clip");
        writeRows(
                conn,
                clipPath,
                Arrays.asList(row("c1", 100, "clip_a"), row("c2", 200, "clip_b")),
                false);

        TablePath dimVehiclePath = TablePath.of(DEFAULT_DB, "bl_dim_vehicle");
        writeRows(
                conn,
                dimVehiclePath,
                Arrays.asList(row(1, 100, "ModelX"), row(2, 200, "ModelY")),
                false);

        // Sink table to receive delta join results
        tEnv.executeSql(
                "create table bl_sink ("
                        + " parking_id varchar, "
                        + " clip_id varchar, "
                        + " val1 int, "
                        + " clip_val varchar, "
                        + " model_name varchar, "
                        + " primary key (parking_id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'fluss' "
                        + ")");

        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
                        OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

        // Customer's original query pattern:
        // parking INNER JOIN clip ON parking.clip_id = clip.id
        // LEFT JOIN dim_vehicle ON clip.vehicle_model = dim_vehicle.vehicle_model
        String sql =
                "INSERT INTO bl_sink "
                        + "SELECT p.id, p.clip_id, p.val1, c.val2, d.model_name "
                        + "FROM bl_parking p "
                        + "INNER JOIN bl_clip c ON p.clip_id = c.id "
                        + "LEFT JOIN bl_dim_vehicle d ON c.vehicle_model = d.vehicle_model";

        // Execute the delta join
        tEnv.executeSql(sql);

        // Query the sink and collect results
        CloseableIterator<Row> collected = tEnv.executeSql("select * from bl_sink").collect();

        // Expected: p1 joins c1 (vehicle_model=100 -> ModelX),
        //           p2 joins c2 (vehicle_model=200 -> ModelY),
        //           p3 joins c1 (vehicle_model=100 -> ModelX)
        List<String> expected =
                Arrays.asList(
                        "+I[p1, c1, 10, clip_a, ModelX]",
                        "+I[p2, c2, 20, clip_b, ModelY]",
                        "+I[p3, c1, 30, clip_a, ModelX]");
        assertResultsIgnoreOrder(collected, expected, true);
    }
}
