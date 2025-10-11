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

package org.apache.fluss.server.index;

import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Comprehensive test for {@link IndexBucketRowCache}. */
class IndexBucketRowCacheTest {

    private static final RowType DATA_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()));

    private MemorySegmentPool memoryPool;
    private IndexBucketRowCache cache;

    @BeforeEach
    void setUp() {
        memoryPool = new TestingMemorySegmentPool(4096); // 4KB page size
        cache = new IndexBucketRowCache(memoryPool);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (cache != null) {
            cache.close();
        }
    }

    @Test
    void testGetUnloadedRanges_EmptyCache() {
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(10L, 20L);

        assertThat(unloadedRanges).hasSize(1);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(10L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(20L);
    }

    @Test
    void testGetUnloadedRanges_InvalidRange() {
        // Start offset >= end offset should return empty list
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(20L, 10L);
        assertThat(unloadedRanges).isEmpty();

        unloadedRanges = cache.getUnloadedRanges(10L, 10L);
        assertThat(unloadedRanges).isEmpty();
    }

    @Test
    void testGetUnloadedRanges_SingleRangeFullyCovered() throws IOException {
        // Write data to create a range [10, 15)
        for (long offset = 10L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Query within the existing range - should return empty
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(10L, 15L);
        assertThat(unloadedRanges).isEmpty();

        // Query subset within the existing range - should return empty
        unloadedRanges = cache.getUnloadedRanges(11L, 13L);
        assertThat(unloadedRanges).isEmpty();
    }

    @Test
    void testGetUnloadedRanges_SingleRangePartialOverlap() throws IOException {
        // Write data to create a range [10, 15)
        for (long offset = 10L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Query with partial overlap at the beginning [5, 12) - should return gap [5, 10)
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(5L, 12L);
        assertThat(unloadedRanges).hasSize(1);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(5L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(10L);

        // Query with partial overlap at the end [12, 20) - should return gap [15, 20)
        unloadedRanges = cache.getUnloadedRanges(12L, 20L);
        assertThat(unloadedRanges).hasSize(1);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(15L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(20L);

        // Query with overlap on both sides [5, 20) - should return gaps [5, 10) and [15, 20)
        unloadedRanges = cache.getUnloadedRanges(5L, 20L);
        assertThat(unloadedRanges).hasSize(2);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(5L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(10L);
        assertThat(unloadedRanges.get(1).getStartOffset()).isEqualTo(15L);
        assertThat(unloadedRanges.get(1).getEndOffset()).isEqualTo(20L);
    }

    @Test
    void testGetUnloadedRanges_NoIntersection() throws IOException {
        // Write data to create a range [10, 15)
        for (long offset = 10L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Query completely before existing range [5, 8) - should return entire query range
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(5L, 8L);
        assertThat(unloadedRanges).hasSize(1);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(5L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(8L);

        // Query completely after existing range [20, 25) - should return entire query range
        unloadedRanges = cache.getUnloadedRanges(20L, 25L);
        assertThat(unloadedRanges).hasSize(1);
        assertThat(unloadedRanges.get(0).getStartOffset()).isEqualTo(20L);
        assertThat(unloadedRanges.get(0).getEndOffset()).isEqualTo(25L);
    }

    @Test
    void testGetUnloadedRanges_MultipleRanges() throws IOException {
        // Create multiple non-adjacent ranges [10, 12), [15, 17), [20, 22)
        long[][] ranges = {{10L, 12L}, {15L, 17L}, {20L, 22L}};

        for (long[] range : ranges) {
            for (long offset = range[0]; offset < range[1]; offset++) {
                IndexedRow row =
                        DataTestUtils.indexedRow(
                                DATA_ROW_TYPE,
                                new Object[] {(int) offset, "test" + offset, "email" + offset});
                cache.writeIndexedRow(offset, ChangeType.INSERT, row);
            }
        }

        // Query spanning all ranges [8, 25) - should return gaps [8, 10), [12, 15), [17, 20), [22,
        // 25)
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(8L, 25L);
        assertThat(unloadedRanges).hasSize(4);
        assertThat(unloadedRanges.get(0)).isEqualTo(new OffsetRangeInfo(8L, 10L));
        assertThat(unloadedRanges.get(1)).isEqualTo(new OffsetRangeInfo(12L, 15L));
        assertThat(unloadedRanges.get(2)).isEqualTo(new OffsetRangeInfo(17L, 20L));
        assertThat(unloadedRanges.get(3)).isEqualTo(new OffsetRangeInfo(22L, 25L));

        // Query between ranges [12, 20) - should return gaps [12, 15) and [17, 20)
        unloadedRanges = cache.getUnloadedRanges(12L, 20L);
        assertThat(unloadedRanges).hasSize(2);
        assertThat(unloadedRanges.get(0)).isEqualTo(new OffsetRangeInfo(12L, 15L));
        assertThat(unloadedRanges.get(1)).isEqualTo(new OffsetRangeInfo(17L, 20L));
    }

    @Test
    void testGetUnloadedRanges_SparseRanges() throws IOException {
        // Create ranges with sparse indexing (boundary expansion only)
        cache.writeEmptyRowForOffset(10L); // Range expansion only [10, 11)
        cache.writeEmptyRowForOffset(11L); // Extend to [10, 12)

        // Add actual data at offset 15
        IndexedRow row =
                DataTestUtils.indexedRow(DATA_ROW_TYPE, new Object[] {15, "test15", "email15"});
        cache.writeIndexedRow(15L, ChangeType.INSERT, row); // Creates [15, 16)

        // Query [10, 20) should return unloaded ranges [12, 15) and [16, 20)
        // because [10, 12) has no actual data and [15, 16) has data
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(10L, 20L);
        assertThat(unloadedRanges).hasSize(2);
        assertThat(unloadedRanges.get(0)).isEqualTo(new OffsetRangeInfo(12L, 15L));
        assertThat(unloadedRanges.get(1)).isEqualTo(new OffsetRangeInfo(16L, 20L));
    }

    @Test
    void testGetRangeLogRecords_DataConsistency() throws IOException {
        // Create test data with different change types
        List<TestRecord> expectedRecords = new ArrayList<>();
        expectedRecords.add(
                new TestRecord(
                        10L, ChangeType.INSERT, new Object[] {100, "Alice", "alice@test.com"}));
        expectedRecords.add(
                new TestRecord(
                        11L, ChangeType.UPDATE_AFTER, new Object[] {101, "Bob", "bob@test.com"}));
        expectedRecords.add(
                new TestRecord(
                        12L, ChangeType.DELETE, new Object[] {102, "Charlie", "charlie@test.com"}));
        expectedRecords.add(
                new TestRecord(
                        13L, ChangeType.INSERT, new Object[] {103, "Diana", "diana@test.com"}));

        // Write all test records to cache
        for (TestRecord record : expectedRecords) {
            IndexedRow row = DataTestUtils.indexedRow(DATA_ROW_TYPE, record.data);
            cache.writeIndexedRow(record.offset, record.changeType, row);
        }

        // Verify basic state
        assertThat(cache.totalEntries()).isEqualTo(4);
        assertThat(cache.getRangeCount()).isEqualTo(1); // All adjacent, should be one range
        assertThat(cache.getTotalSize()).isGreaterThan(0);

        // Test getRangeLogRecords and verify data consistency
        LogRecords logRecords = cache.getRangeLogRecords(10L, 14L, DEFAULT_SCHEMA_ID);
        assertThat(logRecords).isNotNull();
        assertThat(logRecords.sizeInBytes()).isGreaterThan(0);

        // Iterate through the LogRecords and verify each record matches the expected data
        CloseableIterator<LogRecord> iterator = null;
        try {
            // Get the first batch and iterate through its records
            LogRecordBatch batch = logRecords.batches().iterator().next();
            LogRecordReadContext context =
                    LogRecordReadContext.createIndexedReadContext(DATA_ROW_TYPE, DEFAULT_SCHEMA_ID);
            iterator = batch.records(context);
            List<TestRecord> actualRecords = new ArrayList<>();

            int recordIndex = 0;
            while (iterator.hasNext()) {
                LogRecord logRecord = iterator.next();
                TestRecord expected = expectedRecords.get(recordIndex);

                // Verify change type
                assertThat(logRecord.getChangeType()).isEqualTo(expected.changeType);

                // Verify IndexedRow data
                IndexedRow actualRow = (IndexedRow) logRecord.getRow();
                assertThat(actualRow.getInt(0)).isEqualTo((Integer) expected.data[0]);
                assertThat(actualRow.getString(1).toString()).isEqualTo((String) expected.data[1]);
                assertThat(actualRow.getString(2).toString()).isEqualTo((String) expected.data[2]);

                actualRecords.add(
                        new TestRecord(
                                expected.offset,
                                logRecord.getChangeType(),
                                new Object[] {
                                    actualRow.getInt(0),
                                    actualRow.getString(1).toString(),
                                    actualRow.getString(2).toString()
                                }));

                recordIndex++;
            }

            // Verify we got all expected records
            assertThat(actualRecords).hasSize(expectedRecords.size());
            for (int i = 0; i < expectedRecords.size(); i++) {
                TestRecord expected = expectedRecords.get(i);
                TestRecord actual = actualRecords.get(i);

                assertThat(actual.changeType).isEqualTo(expected.changeType);
                assertThat(actual.data[0]).isEqualTo(expected.data[0]);
                assertThat(actual.data[1]).isEqualTo(expected.data[1]);
                assertThat(actual.data[2]).isEqualTo(expected.data[2]);
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    void testGetRangeLogRecords_PartialRange() throws IOException {
        // Write data for range [10, 15)
        for (long offset = 10L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {
                                (int) offset, "name" + offset, "email" + offset + "@test.com"
                            });
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Test partial range queries
        LogRecords logRecords = cache.getRangeLogRecords(11L, 13L, DEFAULT_SCHEMA_ID);
        assertThat(logRecords).isNotNull();
        assertThat(logRecords.sizeInBytes()).isGreaterThan(0);

        // Verify we get exactly 2 records (offsets 11, 12)
        CloseableIterator<LogRecord> iterator = null;
        try {
            LogRecordBatch batch = logRecords.batches().iterator().next();
            LogRecordReadContext context =
                    LogRecordReadContext.createIndexedReadContext(DATA_ROW_TYPE, DEFAULT_SCHEMA_ID);
            iterator = batch.records(context);
            int count = 0;
            long expectedOffset = 11L;

            while (iterator.hasNext()) {
                LogRecord logRecord = iterator.next();
                IndexedRow row = (IndexedRow) logRecord.getRow();

                assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(row.getInt(0)).isEqualTo((int) expectedOffset);
                assertThat(row.getString(1).toString()).isEqualTo("name" + expectedOffset);
                assertThat(row.getString(2).toString())
                        .isEqualTo("email" + expectedOffset + "@test.com");

                expectedOffset++;
                count++;
            }

            assertThat(count).isEqualTo(2);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    void testGetRangeLogRecords_EmptyResult() {
        // Query on empty cache
        LogRecords logRecords = cache.getRangeLogRecords(10L, 20L, DEFAULT_SCHEMA_ID);
        assertThat(logRecords).isNotNull();
        assertThat(logRecords.sizeInBytes()).isEqualTo(0);

        // For empty LogRecords, there should be no batches to iterate
        assertThat(logRecords.batches().iterator().hasNext()).isFalse();
    }

    @Test
    void testGetRangeLogRecords_SpanMultipleRanges() throws IOException {
        // Create two separate ranges [10, 12) and [15, 17)
        for (long offset = 10L; offset < 12L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "name" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        for (long offset = 15L; offset < 17L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "name" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Query spanning multiple ranges should throw exception
        assertThatThrownBy(() -> cache.getRangeLogRecords(10L, 17L, DEFAULT_SCHEMA_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query range")
                .hasMessageContaining("spans multiple ranges");
    }

    @Test
    void testContainsOffset_WithSparseIndexing() throws IOException {
        // Create a sparse range [10, 13) with only data at offset 11
        cache.writeEmptyRowForOffset(10L); // Range expansion only

        IndexedRow row =
                DataTestUtils.indexedRow(
                        DATA_ROW_TYPE, new Object[] {11, "test", "test@email.com"});
        cache.writeIndexedRow(11L, ChangeType.INSERT, row); // Actual data

        cache.writeEmptyRowForOffset(12L); // Range expansion only

        // containsOffset should only return true for offsets with actual data
        assertThat(cache.containsOffset(10L)).isFalse(); // No data, just range boundary
        assertThat(cache.containsOffset(11L)).isTrue(); // Has actual data
        assertThat(cache.containsOffset(12L)).isFalse(); // No data, just range boundary
        assertThat(cache.containsOffset(9L)).isFalse(); // Outside range
        assertThat(cache.containsOffset(13L)).isFalse(); // Outside range
    }

    @Test
    void testRangeAutoMerge() throws IOException {
        // Test automatic merging of adjacent ranges

        // Create first range [10, 12)
        for (long offset = 10L; offset < 12L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // At this point we should have 1 range
        assertThat(cache.getRangeCount()).isEqualTo(1);
        assertThat(cache.totalEntries()).isEqualTo(2);

        // Create another range [15, 17) - non-adjacent
        for (long offset = 15L; offset < 17L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Now we should have 2 separate ranges
        assertThat(cache.getRangeCount()).isEqualTo(2);
        assertThat(cache.totalEntries()).isEqualTo(4);

        // Fill the gap [12, 15) to make ranges adjacent
        for (long offset = 12L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "test" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // After filling the gap, ranges should auto-merge into 1 range [10, 17)
        assertThat(cache.getRangeCount()).isEqualTo(1);
        assertThat(cache.totalEntries()).isEqualTo(7);

        // Verify the merged range covers the entire span
        LogRecords logRecords = cache.getRangeLogRecords(10L, 17L, DEFAULT_SCHEMA_ID);
        assertThat(logRecords).isNotNull();
        assertThat(logRecords.sizeInBytes()).isGreaterThan(0);
    }

    @Test
    void testCleanupBelowHorizon_WithUnloadedRanges() throws IOException {
        // Create ranges with some data
        for (long offset = 10L; offset < 15L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "name" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        for (long offset = 20L; offset < 25L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {(int) offset, "name" + offset, "email" + offset});
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        assertThat(cache.totalEntries()).isEqualTo(10);
        assertThat(cache.getRangeCount()).isEqualTo(2);

        // Test unloaded ranges before cleanup
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(5L, 30L);
        assertThat(unloadedRanges).hasSize(3); // [5,10), [15,20), [25,30)

        // Cleanup below horizon 18 (should remove first range completely via MemorySegment cleanup)
        cache.cleanupBelowHorizon(18L);

        // With new MemorySegment-based cleanup logic:
        // - First range [10,15) has all offsets < 18, so entire segments will be reclaimed
        // - Second range [20,25) has all offsets >= 18, so will be completely preserved
        assertThat(cache.totalEntries()).isEqualTo(5); // Offsets 20-24 remain (entire second range)
        assertThat(cache.getRangeCount()).isEqualTo(1); // Only second range remains

        // Test unloaded ranges after cleanup
        unloadedRanges = cache.getUnloadedRanges(5L, 30L);
        assertThat(unloadedRanges).hasSize(2); // [5,20), [25,30)
        assertThat(unloadedRanges.get(0)).isEqualTo(new OffsetRangeInfo(5L, 20L));
        assertThat(unloadedRanges.get(1)).isEqualTo(new OffsetRangeInfo(25L, 30L));
    }

    @Test
    void testCleanupBelowHorizon_MemorySegmentLevel() throws IOException {
        // Test MemorySegment-level cleanup behavior

        // Create data that will likely span multiple segments
        // Each IndexedRow is estimated to be around 50-100 bytes with our test data
        // TestingMemorySegmentPool typically uses 4KB pages, so we need more data

        // Create Range 1: offsets 0-19 (likely 1-2 segments)
        for (long offset = 0L; offset < 20L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {
                                (int) offset,
                                "very_long_name_to_increase_size_"
                                        + offset
                                        + "_"
                                        + "xxxxxxxxxxxxxxxxxxxx",
                                "very_long_email_to_increase_size_"
                                        + offset
                                        + "@test.com_"
                                        + "xxxxxxxxxxxxxxxxxxxx"
                            });
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        // Create Range 2: offsets 50-69 (separate segments)
        for (long offset = 50L; offset < 70L; offset++) {
            IndexedRow row =
                    DataTestUtils.indexedRow(
                            DATA_ROW_TYPE,
                            new Object[] {
                                (int) offset,
                                "very_long_name_to_increase_size_"
                                        + offset
                                        + "_"
                                        + "xxxxxxxxxxxxxxxxxxxx",
                                "very_long_email_to_increase_size_"
                                        + offset
                                        + "@test.com_"
                                        + "xxxxxxxxxxxxxxxxxxxx"
                            });
            cache.writeIndexedRow(offset, ChangeType.INSERT, row);
        }

        int initialSize = cache.totalEntries();
        int initialRangeCount = cache.getRangeCount();
        long initialTotalSize = cache.getTotalSize();

        assertThat(initialSize).isEqualTo(40); // 20 + 20 records
        assertThat(initialRangeCount).isEqualTo(2); // Two separate ranges
        assertThat(initialTotalSize).isGreaterThan(0);

        // Cleanup below horizon 25 (should remove Range 1 completely)
        // All offsets in Range 1 (0-19) are < 25, so entire segments should be reclaimed
        // All offsets in Range 2 (50-69) are >= 25, so should be preserved
        cache.cleanupBelowHorizon(25L);

        // Verify cleanup results
        assertThat(cache.totalEntries()).isEqualTo(20); // Only Range 2 remains (offsets 50-69)
        assertThat(cache.getRangeCount()).isEqualTo(1); // Only Range 2 remains
        assertThat(cache.getTotalSize()).isLessThan(initialTotalSize); // Memory reclaimed

        // Verify specific offsets
        assertThat(cache.containsOffset(10L)).isFalse(); // Range 1 completely removed
        assertThat(cache.containsOffset(60L)).isTrue(); // Range 2 preserved

        // Verify we can still read from remaining range
        LogRecords logRecords = cache.getRangeLogRecords(50L, 70L, DEFAULT_SCHEMA_ID);
        assertThat(logRecords).isNotNull();
        assertThat(logRecords.sizeInBytes()).isGreaterThan(0);

        // Test unloaded ranges
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(0L, 80L);
        assertThat(unloadedRanges).hasSize(2); // [0,50), [70,80)
        assertThat(unloadedRanges.get(0)).isEqualTo(new OffsetRangeInfo(0L, 50L));
        assertThat(unloadedRanges.get(1)).isEqualTo(new OffsetRangeInfo(70L, 80L));
    }

    @Test
    void testGetUnloadedRanges_AfterClose() throws IOException {
        // Write some data
        IndexedRow row =
                DataTestUtils.indexedRow(DATA_ROW_TYPE, new Object[] {1, "test", "test@email.com"});
        cache.writeIndexedRow(10L, ChangeType.INSERT, row);

        // Close the cache
        cache.close();

        // getUnloadedRanges should return empty list for closed cache
        List<OffsetRangeInfo> unloadedRanges = cache.getUnloadedRanges(10L, 20L);
        assertThat(unloadedRanges).isEmpty();
    }

    // Helper class to represent test records
    private static class TestRecord {
        final long offset;
        final ChangeType changeType;
        final Object[] data;

        TestRecord(long offset, ChangeType changeType, Object[] data) {
            this.offset = offset;
            this.changeType = changeType;
            this.data = data;
        }
    }
}
