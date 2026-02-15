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

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.testutils.ListLogRecords;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link LogRecordBatchIterator} and {@link
 * LogRecordBatchIterator.FilteredLogRecordBatchIterator}.
 */
public class LogRecordBatchIteratorTest {

    @Test
    void testBasicLogRecordBatchIterator() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch).isNotNull();
        assertThat(batch.getRecordCount()).isEqualTo(DATA1.size());
        assertThat(batch.baseLogOffset()).isEqualTo(0L);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isPresent();
        }
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testLogRecordBatchIteratorWithTargetOffset() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()),
                        5L);

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch).isNotNull();
        assertThat(batch.lastLogOffset()).isGreaterThanOrEqualTo(5L);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithEqualPredicate() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();
            assertThat(batch).isNotNull();
            assertThat(batch.getStatistics(readContext)).isPresent();
            assertThat(hasRecordMatching(batch, readContext, row -> row.getInt(0) == 5)).isTrue();
            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithGreaterThanPredicate() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(
                            new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();
            assertThat(batch).isNotNull();
            assertThat(batch.getStatistics(readContext)).isPresent();
            assertThat(hasRecordMatching(batch, readContext, row -> row.getInt(0) > 3)).isTrue();
            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithIsNotNullPredicate() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate isNotNullPredicate = builder.isNotNull(0);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(isNotNullPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();
            assertThat(batch).isNotNull();
            assertThat(hasRecordMatching(batch, readContext, row -> !row.isNullAt(0))).isTrue();
            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNoMatchingData() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 999);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithEmptyLogRecords() throws Exception {
        MemoryLogRecords emptyLogRecords = MemoryLogRecords.EMPTY;

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(emptyLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithMultipleBatches() throws Exception {
        List<Object[]> batch1Data = Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"});
        List<Object[]> batch2Data = Arrays.asList(new Object[] {3, "c"}, new Object[] {4, "d"});
        List<Object[]> batch3Data = Arrays.asList(new Object[] {5, "e"}, new Object[] {6, "f"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 2L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 4L, DEFAULT_SCHEMA_ID);

        ListLogRecords combinedRecords = new ListLogRecords(Arrays.asList(batch1, batch2, batch3));
        Iterator<LogRecordBatch> batchIterator = combinedRecords.batches().iterator();
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        () -> batchIterator.hasNext() ? batchIterator.next() : null);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 2);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(
                            new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

            // Should get two matching batches (batch2 and batch3)
            int batchCount = 0;
            while (filteredIterator.hasNext()) {
                LogRecordBatch batch = filteredIterator.next();
                batchCount++;
                assertThat(batch).isNotNull();
                assertThat(hasRecordMatching(batch, readContext, row -> row.getInt(0) > 2))
                        .isTrue();
            }
            assertThat(batchCount).isEqualTo(2);
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNullStatistics() throws Exception {
        // Create a batch without statistics (using old version magic value)
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject((byte) 0, DATA1);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

            // When no statistics, should return all batches
            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();
            assertThat(batch).isNotNull();
            assertThat(batch.getStatistics(readContext)).isEmpty();
            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithComplexPredicate() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate complexPredicate =
                PredicateBuilder.and(builder.greaterThan(0, 3), builder.lessThan(0, 7));

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(new Filter(complexPredicate, DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();
            assertThat(batch).isNotNull();
            assertThat(
                            hasRecordMatching(
                                    batch,
                                    readContext,
                                    row -> row.getInt(0) > 3 && row.getInt(0) < 7))
                    .isTrue();
            assertThat(filteredIterator.hasNext()).isFalse();
        }
    }

    @Test
    void testGetStatisticsWithNullReadContext() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch.getStatistics(null)).isEmpty();
    }

    @Test
    void testGetStatisticsWithV0Magic() throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject((byte) 0, DATA1);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isEmpty();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorEnsuresNoUnmatchingBatches() throws Exception {
        // Batch 1: values 1-2 (should not match predicate > 5)
        // Batch 2: values 4-5 (should not match predicate > 5)
        // Batch 3: values 7-8 (should match predicate > 5)
        List<Object[]> batch1Data = Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"});
        List<Object[]> batch2Data = Arrays.asList(new Object[] {4, "c"}, new Object[] {5, "d"});
        List<Object[]> batch3Data = Arrays.asList(new Object[] {7, "e"}, new Object[] {8, "f"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 2L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 4L, DEFAULT_SCHEMA_ID);

        ListLogRecords combinedRecords = new ListLogRecords(Arrays.asList(batch1, batch2, batch3));
        Iterator<LogRecordBatch> batchIterator = combinedRecords.batches().iterator();
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        () -> batchIterator.hasNext() ? batchIterator.next() : null);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(
                            new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

            // Should have exactly one batch returned (batch3)
            int batchCount = 0;
            while (filteredIterator.hasNext()) {
                LogRecordBatch batch = filteredIterator.next();
                batchCount++;
                assertThat(hasRecordMatching(batch, readContext, row -> row.getInt(0) > 5))
                        .isTrue();
            }
            assertThat(batchCount).isEqualTo(1);
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsBoundaryValues() throws Exception {
        List<Object[]> boundaryData =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {5, "b"}, new Object[] {10, "c"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        boundaryData, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Test equal to min value (should match)
            assertFilterResult(
                    memoryLogRecords, builder.equal(0, 1), DEFAULT_SCHEMA_ID, readContext, true);

            // Test equal to max value (should match)
            assertFilterResult(
                    memoryLogRecords, builder.equal(0, 10), DEFAULT_SCHEMA_ID, readContext, true);

            // Test greater than max value (should not match)
            assertFilterResult(
                    memoryLogRecords,
                    builder.greaterThan(0, 10),
                    DEFAULT_SCHEMA_ID,
                    readContext,
                    false);

            // Test less than min value (should not match)
            assertFilterResult(
                    memoryLogRecords,
                    builder.lessThan(0, 1),
                    DEFAULT_SCHEMA_ID,
                    readContext,
                    false);
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNullValuesInStatistics() throws Exception {
        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a"},
                        new Object[] {null, "b"},
                        new Object[] {5, "c"},
                        new Object[] {null, "d"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        dataWithNulls, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Test IS NOT NULL predicate
            LogRecordBatchIterator<LogRecordBatch> iterator1 = createIterator(memoryLogRecords);
            LogRecordBatchIterator<LogRecordBatch> filtered1 =
                    iterator1.filter(
                            new Filter(builder.isNotNull(0), DEFAULT_SCHEMA_ID), readContext);
            assertThat(filtered1.hasNext()).isTrue();
            assertThat(hasRecordMatching(filtered1.next(), readContext, row -> !row.isNullAt(0)))
                    .isTrue();

            // Test IS NULL predicate
            LogRecordBatchIterator<LogRecordBatch> iterator2 = createIterator(memoryLogRecords);
            LogRecordBatchIterator<LogRecordBatch> filtered2 =
                    iterator2.filter(new Filter(builder.isNull(0), DEFAULT_SCHEMA_ID), readContext);
            assertThat(filtered2.hasNext()).isTrue();
            assertThat(hasRecordMatching(filtered2.next(), readContext, row -> row.isNullAt(0)))
                    .isTrue();
        }
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsPrecision() throws Exception {
        List<Object[]> preciseData =
                Arrays.asList(
                        new Object[] {3, "a"},
                        new Object[] {4, "b"},
                        new Object[] {5, "c"},
                        new Object[] {6, "d"},
                        new Object[] {7, "e"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        preciseData, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Test range predicate that should match exactly
            LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
            LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                    iterator.filter(
                            new Filter(builder.greaterThan(0, 4), DEFAULT_SCHEMA_ID), readContext);

            assertThat(filteredIterator.hasNext()).isTrue();
            LogRecordBatch batch = filteredIterator.next();

            List<Integer> matchingValues = new ArrayList<>();
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    int value = recordIterator.next().getRow().getInt(0);
                    if (value > 4) {
                        matchingValues.add(value);
                    }
                }
            }
            assertThat(matchingValues).containsExactlyInAnyOrder(5, 6, 7);

            // Test that statistics correctly exclude non-matching batches
            assertFilterResult(
                    memoryLogRecords,
                    builder.greaterThan(0, 10),
                    DEFAULT_SCHEMA_ID,
                    readContext,
                    false);
        }
    }

    // ==================== Helper Methods ====================

    private LogRecordBatchIterator<LogRecordBatch> createIterator(MemoryLogRecords memoryLogRecords)
            throws Exception {
        return new LogRecordBatchIterator<>(
                new MemorySegmentLogInputStream(
                        memoryLogRecords.getMemorySegment(),
                        memoryLogRecords.getPosition(),
                        memoryLogRecords.sizeInBytes()));
    }

    @FunctionalInterface
    private interface RowPredicate {
        boolean test(org.apache.fluss.row.InternalRow row);
    }

    private boolean hasRecordMatching(
            LogRecordBatch batch, LogRecordReadContext readContext, RowPredicate predicate)
            throws Exception {
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                if (predicate.test(recordIterator.next().getRow())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void assertFilterResult(
            MemoryLogRecords memoryLogRecords,
            Predicate predicate,
            int schemaId,
            LogRecordReadContext readContext,
            boolean expectMatch)
            throws Exception {
        LogRecordBatchIterator<LogRecordBatch> iterator = createIterator(memoryLogRecords);
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(predicate, schemaId), readContext);
        assertThat(filteredIterator.hasNext()).isEqualTo(expectMatch);
    }
}
