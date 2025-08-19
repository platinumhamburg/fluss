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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.utils.FlinkConnectorOptionsUtils;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.predicate.Predicate;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive unit tests for filter pushdown functionality in FlinkTableSource. Tests cover
 * various scenarios including streaming/batch modes, different table types, and different filter
 * pushdown types (primary key, partition key, record batch filter).
 */
public class FlinkTableSourceFilterPushDownTest {

    @Nested
    class NonPartitionedKvTableTests {
        private FlinkTableSource tableSource;

        @BeforeEach
        void setUp() {
            // Create a non-partitioned KV table schema: id (PK), name, value
            RowType tableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()))
                                    .getLogicalType();

            TablePath tablePath = TablePath.of("test_db", "test_kv_table");
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;

            tableSource =
                    new FlinkTableSource(
                            tablePath,
                            flussConfig,
                            tableOutputType,
                            new int[] {0}, // primary key indexes (id)
                            new int[] {}, // bucket key indexes
                            new int[] {}, // partition key indexes
                            true, // streaming
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            false, // is data lake enabled
                            null // merge engine type
                            );
        }

        @Test
        void testStreamingModeNonPrimaryKeyPushdown() {
            // Test non-primary key filter in streaming mode for KV table
            // KV tables should not support record batch filter pushdown for non-primary key fields
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression literal = new ValueLiteralExpression("test");
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // For KV tables, non-primary key filters should not be pushed down
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(1);
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }

        @Test
        void testStreamingModeMultipleFilters() {
            // Test multiple filters in streaming mode for KV table
            // KV tables should not support record batch filter pushdown
            FieldReferenceExpression idFieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            FieldReferenceExpression nameFieldRef =
                    new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression idLiteral = new ValueLiteralExpression(5);
            ValueLiteralExpression nameLiteral = new ValueLiteralExpression("test");

            CallExpression idEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(idFieldRef, idLiteral),
                            DataTypes.BOOLEAN());

            CallExpression nameEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(nameFieldRef, nameLiteral),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(idEqualCall, nameEqualCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // For KV tables in streaming mode, no filters should be pushed down
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(2);
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }
    }

    @Nested
    class NonPartitionedLogTableTests {
        private FlinkTableSource tableSource;

        @BeforeEach
        void setUp() {
            // Create a non-partitioned log table schema: id, name, value (no primary key)
            RowType tableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()),
                                            DataTypes.FIELD("region", DataTypes.BIGINT()))
                                    .getLogicalType();

            TablePath tablePath = TablePath.of("test_db", "test_log_table");
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;

            tableSource =
                    new FlinkTableSource(
                            tablePath,
                            flussConfig,
                            tableOutputType,
                            new int[] {}, // no primary key indexes
                            new int[] {}, // bucket key indexes
                            new int[] {}, // partition key indexes
                            true, // streaming
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            false, // is data lake enabled
                            null // merge engine type
                            );
        }

        @Test
        void testLogTableRecordBatchFilterPushdown() {
            // Test record batch filter pushdown for log table
            // Log tables should support record batch filter pushdown
            FieldReferenceExpression valueFieldRef =
                    new FieldReferenceExpression("value", DataTypes.STRING(), 0, 2);
            FieldReferenceExpression regionFieldRef =
                    new FieldReferenceExpression("region", DataTypes.STRING(), 0, 3);

            CallExpression regionEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(regionFieldRef, new ValueLiteralExpression("HangZhou")),
                            DataTypes.BOOLEAN());
            CallExpression valueRangeCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            Arrays.asList(regionFieldRef, new ValueLiteralExpression(1000)),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(regionEqualCall, valueRangeCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            assertThat(result.getAcceptedFilters()).hasSize(2);
            assertThat(result.getRemainingFilters()).hasSize(2);
            assertThat(tableSource.getLogRecordBatchFilter()).isNotNull();
            assertThat(tableSource.getSingleRowFilter()).isNull();
        }

        @Test
        void testLogTableRangeFilterPushdown() {
            // Test range filter pushdown for log table
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            ValueLiteralExpression literal1 = new ValueLiteralExpression(3);
            ValueLiteralExpression literal2 = new ValueLiteralExpression(10);

            CallExpression greaterThanCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            Arrays.asList(fieldRef, literal1),
                            DataTypes.BOOLEAN());

            CallExpression lessThanCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.LESS_THAN,
                            Arrays.asList(fieldRef, literal2),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(greaterThanCall, lessThanCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Range filters should be pushed down as record batch filter
            assertThat(result.getAcceptedFilters()).hasSize(2);
            assertThat(result.getRemainingFilters()).hasSize(2);
            assertThat(tableSource.getLogRecordBatchFilter()).isNotNull();
        }

        @Test
        void testEmptyFilters() {
            // Test with empty filters
            List<ResolvedExpression> filters = Collections.emptyList();

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // No filters should be accepted or remain
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).isEmpty();
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }
    }

    @Nested
    class PartitionedKvTableTests {
        private FlinkTableSource tableSource;

        @BeforeEach
        void setUp() {
            // Create a partitioned KV table schema: id (PK), name, value, region (partition key)
            RowType tableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()),
                                            DataTypes.FIELD("region", DataTypes.STRING()))
                                    .getLogicalType();

            TablePath tablePath = TablePath.of("test_db", "test_partitioned_kv_table");
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;

            tableSource =
                    new FlinkTableSource(
                            tablePath,
                            flussConfig,
                            tableOutputType,
                            new int[] {0}, // primary key indexes (id)
                            new int[] {}, // bucket key indexes
                            new int[] {3}, // partition key indexes (region)
                            true, // streaming
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            false, // is data lake enabled
                            null // merge engine type
                            );
        }

        @Test
        void testPartitionKeyPushdown() {
            // Test partition key pushdown
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("region", DataTypes.STRING(), 0, 3);
            ValueLiteralExpression literal =
                    new ValueLiteralExpression("us-east", DataTypes.STRING().notNull());
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Partition key filters should be pushed down as partition filters
            assertThat(result.getAcceptedFilters()).hasSize(1);
            assertThat(result.getRemainingFilters()).isEmpty();
            assertThat(tableSource.getPartitionFilters()).hasSize(1);
            assertThat(tableSource.getPartitionFilters().get(0).fieldIndex).isEqualTo(3);
            assertThat(tableSource.getPartitionFilters().get(0).equalValue).isEqualTo("us-east");
        }

        @Test
        void testRecordBatchFilterNotPushedDownInKvTable() {
            // Test partition key pushdown
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression literal =
                    new ValueLiteralExpression("bob", DataTypes.STRING().notNull());
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(1);
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }
    }

    @Nested
    class PartitionedLogTableTests {
        private FlinkTableSource tableSource;

        @BeforeEach
        void setUp() {
            // Create a partitioned log table schema: id, name, value, region (partition key)
            RowType tableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()),
                                            DataTypes.FIELD("region", DataTypes.STRING()),
                                            DataTypes.FIELD("attachment", DataTypes.BYTES()))
                                    .getLogicalType();

            TablePath tablePath = TablePath.of("test_db", "test_partitioned_log_table");
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;

            tableSource =
                    new FlinkTableSource(
                            tablePath,
                            flussConfig,
                            tableOutputType,
                            new int[] {}, // no primary key indexes
                            new int[] {}, // bucket key indexes
                            new int[] {3}, // partition key indexes (region)
                            true, // streaming
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            false, // is data lake enabled
                            null // merge engine type
                            );
        }

        @Test
        void testPartitionedLogTablePartitionKeyPushdown() {
            // Test partition key pushdown for partitioned log table
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("region", DataTypes.STRING(), 0, 3);
            ValueLiteralExpression literal =
                    new ValueLiteralExpression("us-east", DataTypes.STRING().notNull());
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Partition key filters should be pushed down as partition filters
            assertThat(result.getAcceptedFilters()).hasSize(1);
            assertThat(result.getRemainingFilters()).isEmpty();
            assertThat(tableSource.getPartitionFilters()).hasSize(1);
            assertThat(tableSource.getPartitionFilters().get(0).fieldIndex).isEqualTo(3);
            assertThat(tableSource.getPartitionFilters().get(0).equalValue).isEqualTo("us-east");
        }

        @Test
        void testPartitionedLogTableRecordBatchFilterPushdown() {
            // Test record batch filter pushdown for partitioned log table
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            ValueLiteralExpression literal = new ValueLiteralExpression(5);
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            assertThat(result.getAcceptedFilters()).hasSize(1);
            assertThat(result.getRemainingFilters()).hasSize(1);
            // record batch filter should be successfully pushdown
            assertThat(tableSource.getLogRecordBatchFilter()).isNotNull();
        }

        @Test
        void testCopyPreservesRecordBatchFilters() {
            // Test that copying preserves all filter states
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            ValueLiteralExpression literal = new ValueLiteralExpression(5);
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);
            tableSource.applyFilters(filters);

            // Verify original has filters
            Predicate originalFilter = tableSource.getLogRecordBatchFilter();
            assertThat(originalFilter).isNotNull();

            // Copy the table source
            FlinkTableSource copiedSource = (FlinkTableSource) tableSource.copy();

            // Verify copied source has the same filters
            Predicate copiedFilter = copiedSource.getLogRecordBatchFilter();
            assertThat(copiedFilter).isNotNull();
            assertThat(copiedFilter).isEqualTo(originalFilter);
        }

        @Test
        void testUnsupportedRecordBatchFilterColumnTypes() {
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("attachment", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression literal =
                    new ValueLiteralExpression(new byte[] {}, DataTypes.BYTES().notNull());
            CallExpression likeCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(likeCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Unsupported column should not be pushed down as record batch filter
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(1);
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }

        @Test
        void testMixedFilterTypesPushdown() {
            // Test mixed filter types: partition key + regular column
            FieldReferenceExpression regionFieldRef =
                    new FieldReferenceExpression("region", DataTypes.STRING(), 0, 3);
            FieldReferenceExpression idFieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            FieldReferenceExpression nameFieldRef =
                    new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression regionLiteral =
                    new ValueLiteralExpression("us-east", DataTypes.STRING().notNull());
            ValueLiteralExpression idLiteral = new ValueLiteralExpression(5);
            ValueLiteralExpression nameLiteral =
                    new ValueLiteralExpression("test", DataTypes.STRING().notNull());

            CallExpression regionEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(regionFieldRef, regionLiteral),
                            DataTypes.BOOLEAN());

            CallExpression idEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(idFieldRef, idLiteral),
                            DataTypes.BOOLEAN());

            CallExpression nameEqualCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(nameFieldRef, nameLiteral),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters =
                    Arrays.asList(regionEqualCall, idEqualCall, nameEqualCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Partition key should be pushed down as partition filter
            // Regular columns should be pushed down as record batch filter
            assertThat(result.getAcceptedFilters()).hasSize(3);
            assertThat(result.getRemainingFilters()).hasSize(2); // id and name remain
            assertThat(tableSource.getPartitionFilters()).hasSize(1);
            assertThat(tableSource.getLogRecordBatchFilter())
                    .isNotNull(); // No record batch filter in partitioned tables
        }
    }

    @Nested
    class BatchModePrimaryKeyPushdownTests {
        private FlinkTableSource tableSource;

        @BeforeEach
        void setUp() {
            // Create a KV table schema for batch mode testing
            RowType tableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()))
                                    .getLogicalType();

            TablePath tablePath = TablePath.of("test_db", "test_batch_table");
            Configuration flussConfig = new Configuration();
            flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.FULL;

            tableSource =
                    new FlinkTableSource(
                            tablePath,
                            flussConfig,
                            tableOutputType,
                            new int[] {0}, // primary key indexes (id)
                            new int[] {}, // bucket key indexes
                            new int[] {}, // partition key indexes
                            false, // batch mode
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            true, // is data lake enabled
                            null // merge engine type
                            );
        }

        @Test
        void testBatchModePrimaryKeyPushdown() {
            // Test primary key pushdown in batch mode with FULL startup mode
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            ValueLiteralExpression literal = new ValueLiteralExpression(5);
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // In batch mode with FULL startup and complete primary key filter,
            // should use single row filter for point lookup
            assertThat(result.getAcceptedFilters()).hasSize(1);
            assertThat(result.getRemainingFilters()).isEmpty();
            assertThat(tableSource.getSingleRowFilter()).isNotNull();
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();

            // Verify the single row filter contains the correct value
            GenericRowData singleRowFilter = tableSource.getSingleRowFilter();
            assertThat(singleRowFilter.getInt(0)).isEqualTo(5);
        }

        @Test
        void testBatchModeIncompletePrimaryKeyPushdown() {
            // Test incomplete primary key filter in batch mode (should not push down)
            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression literal = new ValueLiteralExpression("test");
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = tableSource.applyFilters(filters);

            // Non-primary key filters should not be pushed down in batch mode
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(1);
            assertThat(tableSource.getSingleRowFilter()).isNull();
            assertThat(tableSource.getLogRecordBatchFilter()).isNull();
        }

        @Test
        void testBatchModeNonFullStartupMode() {
            // Test batch mode with non-FULL startup mode
            FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                    new FlinkConnectorOptionsUtils.StartupOptions();
            startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;

            // Create a new table output type for the non-full startup test
            RowType nonFullStartupTableOutputType =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.INT()),
                                            DataTypes.FIELD("name", DataTypes.STRING()),
                                            DataTypes.FIELD("value", DataTypes.BIGINT()))
                                    .getLogicalType();

            FlinkTableSource nonFullStartupTableSource =
                    new FlinkTableSource(
                            TablePath.of("test_db", "test_batch_table"),
                            new Configuration(),
                            nonFullStartupTableOutputType,
                            new int[] {0}, // primary key indexes
                            new int[] {}, // bucket key indexes
                            new int[] {}, // partition key indexes
                            false, // batch mode
                            startupOptions,
                            3, // lookup max retry times
                            false, // lookup async
                            null, // cache
                            1000L, // scan partition discovery interval
                            true, // is data lake enabled
                            null // merge engine type
                            );

            FieldReferenceExpression fieldRef =
                    new FieldReferenceExpression("id", DataTypes.INT(), 0, 0);
            ValueLiteralExpression literal = new ValueLiteralExpression(5);
            CallExpression equalCall =
                    new CallExpression(
                            BuiltInFunctionDefinitions.EQUALS,
                            Arrays.asList(fieldRef, literal),
                            DataTypes.BOOLEAN());

            List<ResolvedExpression> filters = Arrays.asList(equalCall);

            FlinkTableSource.Result result = nonFullStartupTableSource.applyFilters(filters);

            // With non-FULL startup mode, should not use single row filter
            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).hasSize(1);
            assertThat(nonFullStartupTableSource.getSingleRowFilter()).isNull();
        }
    }
}
