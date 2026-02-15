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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Filter}. */
class RecordBatchFilterTest {

    private static final int TEST_SCHEMA_ID = 123;

    // Helper method to create test predicate
    private Predicate createTestPredicate() {
        PredicateBuilder builder = new PredicateBuilder(TestData.FILTER_TEST_ROW_TYPE);
        return builder.greaterThan(0, 5L); // id > 5
    }

    // Helper method to create a basic SchemaGetter for tests
    private TestingSchemaGetter createBasicSchemaGetter() {
        return new TestingSchemaGetter(new SchemaInfo(TestData.FILTER_TEST_SCHEMA, TEST_SCHEMA_ID));
    }

    // Helper method to create statistics for testing schema-aware behavior
    private DefaultLogRecordBatchStatistics createTestStatistics(int schemaId) {
        try {
            int[] statsMapping = new int[] {0, 1, 2}; // map all columns

            // Create collector and simulate processing some data
            LogRecordBatchStatisticsCollector collector =
                    new LogRecordBatchStatisticsCollector(
                            TestData.FILTER_TEST_ROW_TYPE, statsMapping);

            // Process some test rows to generate statistics
            for (Object[] data : TestData.FILTER_TEST_DATA) {
                GenericRow row = new GenericRow(3);
                row.setField(0, data[0]);
                row.setField(1, BinaryString.fromString((String) data[1]));
                row.setField(2, data[2]);
                collector.processRow(row);
            }

            // Serialize the statistics to memory
            MemorySegmentOutputView outputView = new MemorySegmentOutputView(1024);

            collector.writeStatistics(outputView);

            // Get the written data
            MemorySegment segment = outputView.getMemorySegment();

            // Parse the serialized statistics back

            return LogRecordBatchStatisticsParser.parseStatistics(
                    segment, 0, TestData.FILTER_TEST_ROW_TYPE, schemaId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test statistics", e);
        }
    }

    @Test
    void testSchemaAwareStatisticsWithMatchingSchemaId() {
        Predicate testPredicate = createTestPredicate(); // id > 5
        DefaultLogRecordBatchStatistics testStats = createTestStatistics(TEST_SCHEMA_ID);

        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, testStats);

        // The statistics have max id=10 > 5, so predicate should return true (possible match)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaAwareStatisticsWithMismatchedSchemaIdNoEvolution() {
        Predicate testPredicate = createTestPredicate(); // id > 5
        DefaultLogRecordBatchStatistics mismatchedStats =
                createTestStatistics(456); // Different schema ID

        // Filter without schema evolution support
        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, mismatchedStats);

        // With mismatched schema ID and no schema evolution support, should return true
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaAwareStatisticsWithNullStats() {
        Predicate testPredicate = createTestPredicate(); // id > 5

        Filter filter = new Filter(testPredicate, TEST_SCHEMA_ID);
        boolean result = filter.test(100L, (DefaultLogRecordBatchStatistics) null);

        // With null statistics, should return true (cannot filter)
        assertThat(result).isTrue();
    }

    // =====================================================
    // Schema Evolution Tests
    // =====================================================

    // Schema V1: id(0), name(1), score(2) with columnIds 1, 2, 3
    private static final Schema SCHEMA_V1 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("id", DataTypes.BIGINT(), null, 1),
                                    new Schema.Column("name", DataTypes.STRING(), null, 2),
                                    new Schema.Column("score", DataTypes.DOUBLE(), null, 3)))
                    .build();

    // Schema V2: id(0), name(1), age(2), score(3) - added 'age' column with columnId 4
    // Column order: id(columnId=1), name(columnId=2), age(columnId=4), score(columnId=3)
    private static final Schema SCHEMA_V2 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("id", DataTypes.BIGINT(), null, 1),
                                    new Schema.Column("name", DataTypes.STRING(), null, 2),
                                    new Schema.Column(
                                            "age", DataTypes.INT(), null, 4), // New column
                                    new Schema.Column("score", DataTypes.DOUBLE(), null, 3)))
                    .build();

    private static final int SCHEMA_ID_V1 = 1;
    private static final int SCHEMA_ID_V2 = 2;

    /** Create a TestingSchemaGetter with multiple schemas. */
    private TestingSchemaGetter createSchemaGetter(
            int latestSchemaId, Schema latestSchema, int otherSchemaId, Schema otherSchema) {
        TestingSchemaGetter getter =
                new TestingSchemaGetter(new SchemaInfo(latestSchema, latestSchemaId));
        getter.updateLatestSchemaInfo(new SchemaInfo(otherSchema, otherSchemaId));
        return getter;
    }

    /** Create statistics for a given schema. */
    private DefaultLogRecordBatchStatistics createStatisticsForSchema(
            Schema schema, int schemaId, List<Object[]> testData) {
        try {
            RowType rowType = schema.getRowType();
            int[] statsMapping = new int[rowType.getFieldCount()];
            for (int i = 0; i < statsMapping.length; i++) {
                statsMapping[i] = i;
            }

            LogRecordBatchStatisticsCollector collector =
                    new LogRecordBatchStatisticsCollector(rowType, statsMapping);

            for (Object[] data : testData) {
                GenericRow row = new GenericRow(data.length);
                for (int i = 0; i < data.length; i++) {
                    if (data[i] instanceof String) {
                        row.setField(i, BinaryString.fromString((String) data[i]));
                    } else {
                        row.setField(i, data[i]);
                    }
                }
                collector.processRow(row);
            }

            MemorySegmentOutputView outputView = new MemorySegmentOutputView(1024);
            collector.writeStatistics(outputView);
            MemorySegment segment = outputView.getMemorySegment();

            return LogRecordBatchStatisticsParser.parseStatistics(segment, 0, rowType, schemaId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test statistics", e);
        }
    }

    @Test
    void testSchemaEvolution_FilterWithSameSchema() {
        // Create predicate using RowType (no columnId needed - all schema evolution handled by
        // Filter)
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V1.getRowType());
        Predicate predicate = builder.greaterThan(0, 5L); // id > 5

        // Create statistics for schema V1
        List<Object[]> v1Data =
                Arrays.asList(
                        new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0}); // max id = 10
        DefaultLogRecordBatchStatistics stats =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Filter with same schema ID (no schema evolution needed)
        Filter filter = new Filter(predicate, SCHEMA_ID_V1);
        boolean result = filter.test(100L, stats);

        // max(id)=10 > 5, should return true (possible match)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaEvolution_FilterOldDataWithNewSchema() {
        // Create predicate using V2's RowType
        // Filter on 'id' column (index 0 in V2)
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V2.getRowType());
        Predicate predicate = builder.greaterThan(0, 5L); // id > 5

        // Statistics from old data (Schema V1, schemaId = 1)
        List<Object[]> v1Data =
                Arrays.asList(
                        new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0}); // max id = 10
        DefaultLogRecordBatchStatistics statsV1 =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Create SchemaGetter for schema evolution support
        TestingSchemaGetter schemaGetter =
                createSchemaGetter(SCHEMA_ID_V2, SCHEMA_V2, SCHEMA_ID_V1, SCHEMA_V1);

        // Filter with V2 schema and schema evolution support
        Filter filter = new Filter(predicate, SCHEMA_ID_V2, schemaGetter);
        boolean result = filter.test(100L, statsV1);

        // Schema evolution: using SchemaUtil.getIndexMapping(statsSchema=V1, filterSchema=V2)
        // Filter field index 0 (id) maps to stats field index 0
        // max(id)=10 > 5 => true
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaEvolution_FilterWithNewColumnPredicate() {
        // Predicate on 'age' column (only exists in V2)
        // 'age' is at index 2 in V2
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V2.getRowType());
        Predicate predicate = builder.greaterThan(2, 18); // age > 18

        // Statistics from old data (Schema V1, doesn't have 'age')
        List<Object[]> v1Data =
                Arrays.asList(new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0});
        DefaultLogRecordBatchStatistics statsV1 =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Create SchemaGetter for schema evolution support
        TestingSchemaGetter schemaGetter =
                createSchemaGetter(SCHEMA_ID_V2, SCHEMA_V2, SCHEMA_ID_V1, SCHEMA_V1);

        // Filter with V2 schema
        Filter filter = new Filter(predicate, SCHEMA_ID_V2, schemaGetter);
        boolean result = filter.test(100L, statsV1);

        // 'age' (index 2 in V2, columnId=4) doesn't exist in V1
        // indexMapping[2] = UNEXIST_MAPPING
        // Cannot adapt predicate, should return true (include batch, safe fallback)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaEvolution_FilterCanReject() {
        // Predicate: id > 100
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V2.getRowType());
        Predicate predicate = builder.greaterThan(0, 100L); // id > 100

        // Statistics with max id = 10 (all data id <= 10)
        List<Object[]> v1Data =
                Arrays.asList(
                        new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0}); // max id = 10
        DefaultLogRecordBatchStatistics statsV1 =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Create SchemaGetter for schema evolution support
        TestingSchemaGetter schemaGetter =
                createSchemaGetter(SCHEMA_ID_V2, SCHEMA_V2, SCHEMA_ID_V1, SCHEMA_V1);

        Filter filter = new Filter(predicate, SCHEMA_ID_V2, schemaGetter);
        boolean result = filter.test(100L, statsV1);

        // max(id)=10 < 100, predicate id > 100 can never be satisfied
        // Should return false (reject batch)
        assertThat(result).isFalse();
    }

    @Test
    void testSchemaEvolution_CompoundPredicate() {
        // Compound predicate: id > 5 AND score < 50
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V2.getRowType());
        Predicate idPredicate = builder.greaterThan(0, 5L); // id > 5
        Predicate scorePredicate = builder.lessThan(3, 50.0); // score < 50 (index 3 in V2)
        Predicate compoundPredicate = PredicateBuilder.and(idPredicate, scorePredicate);

        // Statistics: id range [1, 10], score range [1.0, 100.0]
        List<Object[]> v1Data =
                Arrays.asList(new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0});
        DefaultLogRecordBatchStatistics statsV1 =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Create SchemaGetter for schema evolution support
        TestingSchemaGetter schemaGetter =
                createSchemaGetter(SCHEMA_ID_V2, SCHEMA_V2, SCHEMA_ID_V1, SCHEMA_V1);

        Filter filter = new Filter(compoundPredicate, SCHEMA_ID_V2, schemaGetter);
        boolean result = filter.test(100L, statsV1);

        // Both conditions can potentially be satisfied:
        // - max(id)=10 > 5: true
        // - min(score)=1.0 < 50: true
        // Should return true (possible match)
        assertThat(result).isTrue();
    }

    @Test
    void testSchemaEvolution_WithoutSchemaGetter() {
        // Predicate created with V2's RowType
        PredicateBuilder builder = new PredicateBuilder(SCHEMA_V2.getRowType());
        Predicate predicate = builder.greaterThan(0, 5L); // id > 5

        // Statistics from old data (Schema V1)
        List<Object[]> v1Data =
                Arrays.asList(new Object[] {1L, "a", 1.0}, new Object[] {10L, "z", 100.0});
        DefaultLogRecordBatchStatistics statsV1 =
                createStatisticsForSchema(SCHEMA_V1, SCHEMA_ID_V1, v1Data);

        // Filter WITHOUT SchemaGetter (no schema evolution support)
        Filter filter = new Filter(predicate, SCHEMA_ID_V2);
        boolean result = filter.test(100L, statsV1);

        // Without SchemaGetter, cannot perform schema evolution
        // Should return true (safe fallback)
        assertThat(result).isTrue();
    }
}
