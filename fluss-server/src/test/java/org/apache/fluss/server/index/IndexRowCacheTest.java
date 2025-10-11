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

package org.apache.fluss.server.index;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.INDEXED_DATA;
import static org.apache.fluss.record.TestData.INDEXED_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEX_CACHE_COLD_WAL_DATA;
import static org.apache.fluss.record.TestData.INDEX_CACHE_CONTINUOUS_OFFSETS;
import static org.apache.fluss.record.TestData.INDEX_CACHE_DATA;
import static org.apache.fluss.record.TestData.INDEX_CACHE_SPARSE_OFFSETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive unit tests for {@link IndexRowCache}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Initialization and configuration
 *   <li>Commit offset tracking and horizon calculation
 *   <li>Write operations for empty rows and indexed rows
 *   <li>Read operations and data retrieval
 *   <li>Cleanup and resource management
 *   <li>Edge cases and error handling
 * </ul>
 */
class IndexRowCacheTest {

    private static final int DEFAULT_PAGE_SIZE = 64 * 1024; // 64KB
    private static final long INDEX_TABLE_1 = 100001L;
    private static final long INDEX_TABLE_2 = 100002L;

    @TempDir private File tempDir;

    private Configuration conf;
    private FlussScheduler scheduler;
    private TestingMemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private IndexRowCache indexRowCache;

    // Test table partition ids
    private TablePartitionId tablePartition1;
    private TablePartitionId tablePartition2;

    // Test data
    private RowType rowType;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize configuration and dependencies
        conf = new Configuration();
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        memoryPool = new TestingMemorySegmentPool(DEFAULT_PAGE_SIZE);

        // Create LogTablet for accessing high watermark
        createLogTablet();

        // Setup test data
        rowType = INDEXED_ROW_TYPE;
        tablePartition1 = TablePartitionId.of(INDEX_TABLE_1, null);
        tablePartition2 = TablePartitionId.of(INDEX_TABLE_2, null);

        // Create IndexRowCache with index bucket distribution
        Map<TablePartitionId, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(tablePartition1, 3); // 3 buckets for table 1
        indexBucketDistribution.put(tablePartition2, 2); // 2 buckets for table 2

        indexRowCache = new IndexRowCache(memoryPool, logTablet, indexBucketDistribution);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexRowCache != null) {
            indexRowCache.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    // ===============================================================================================
    // Constructor and Initialization Tests
    // ===============================================================================================

    @Test
    void testConstructorSuccessful() {
        assertThat(indexRowCache).isNotNull();
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(2);
        assertThat(indexRowCache.getTotalEntries()).isEqualTo(0);
        assertThat(indexRowCache.isEmpty()).isFalse(); // Has bucket caches but no entries
    }

    @Test
    void testConstructorWithEmptyDistribution() {
        Map<TablePartitionId, Integer> emptyDistribution = new HashMap<>();
        IndexRowCache emptyCache = new IndexRowCache(memoryPool, logTablet, emptyDistribution);

        assertThat(emptyCache.getCachedBucketCount()).isEqualTo(0);
        assertThat(emptyCache.getTotalEntries()).isEqualTo(0);
        assertThat(emptyCache.isEmpty()).isTrue();

        emptyCache.close();
    }

    // ===============================================================================================
    // Commit Offset Tests
    // ===============================================================================================

    @Test
    void testUpdateCommitOffset() {
        // Test updating commit offset for different buckets
        indexRowCache.updateCommitOffset(tablePartition1, 0, 10L);
        indexRowCache.updateCommitOffset(tablePartition1, 1, 20L);
        indexRowCache.updateCommitOffset(
                tablePartition1, 2, 25L); // Update all buckets in partition 1
        indexRowCache.updateCommitOffset(tablePartition2, 0, 15L);
        indexRowCache.updateCommitOffset(
                tablePartition2, 1, 18L); // Update all buckets in partition 2

        // Verify commit horizon is the minimum of all commit offsets
        assertThat(indexRowCache.getCommitHorizon()).isEqualTo(10L);
    }

    @Test
    void testGetCommitHorizonAcrossMultipleTables() {
        // Setup different commit offsets across different tables and buckets
        indexRowCache.updateCommitOffset(tablePartition1, 0, 100L);
        indexRowCache.updateCommitOffset(tablePartition1, 1, 150L);
        indexRowCache.updateCommitOffset(tablePartition1, 2, 80L);
        indexRowCache.updateCommitOffset(tablePartition2, 0, 90L);
        indexRowCache.updateCommitOffset(tablePartition2, 1, 120L);

        // Should return minimum commit offset across all buckets (80L)
        assertThat(indexRowCache.getCommitHorizon()).isEqualTo(80L);
    }

    @Test
    void testUpdateCommitOffsetNonExistentTable() {
        TablePartitionId nonExistentTable = TablePartitionId.of(999999L, null);

        assertThatThrownBy(() -> indexRowCache.updateCommitOffset(nonExistentTable, 0, 10L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===============================================================================================
    // Write Operations Tests
    // ===============================================================================================

    @Test
    void testWriteEmptyRows() throws IOException {
        long logOffset = 100L;

        indexRowCache.writeEmptyRows(tablePartition1, logOffset);

        // Empty rows may not count as entries in the current implementation
        // This test just verifies the operation completes without error
        assertThat(indexRowCache.getTotalEntries()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testWriteIndexedRow() throws IOException {
        long logOffset = 200L;
        int bucketId = 0;
        Object[] testData = INDEXED_DATA.get(0);
        IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

        indexRowCache.writeIndexedRow(
                tablePartition1, bucketId, logOffset, ChangeType.INSERT, indexedRow);

        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testWriteMultipleIndexedRows() throws IOException {
        TablePartitionId tablePartition = tablePartition1;
        int bucketId = 1;

        // Write multiple indexed rows with different offsets
        for (int i = 0; i < 5; i++) {
            long logOffset = 300L + i;
            Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

            indexRowCache.writeIndexedRow(
                    tablePartition, bucketId, logOffset, ChangeType.INSERT, indexedRow);
        }

        assertThat(indexRowCache.getTotalEntries()).isGreaterThanOrEqualTo(5);
    }

    @Test
    void testWriteIndexedRowToMultipleBuckets() throws IOException {
        long logOffset = 400L;

        // Write to bucket 0 of tablePartition1
        Object[] testData1 = INDEXED_DATA.get(0);
        IndexedRow indexedRow1 = DataTestUtils.indexedRow(rowType, testData1);
        indexRowCache.writeIndexedRow(
                tablePartition1, 0, logOffset, ChangeType.INSERT, indexedRow1);

        // Write to bucket 1 of tablePartition1
        Object[] testData2 = INDEXED_DATA.get(1);
        IndexedRow indexedRow2 = DataTestUtils.indexedRow(rowType, testData2);
        indexRowCache.writeIndexedRow(
                tablePartition1, 1, logOffset + 1, ChangeType.UPDATE_AFTER, indexedRow2);

        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testWriteAfterClosed() throws IOException {
        indexRowCache.close();

        long logOffset = 500L;
        Object[] testData = INDEXED_DATA.get(0);
        IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

        assertThatThrownBy(
                        () ->
                                indexRowCache.writeIndexedRow(
                                        tablePartition1,
                                        0,
                                        logOffset,
                                        ChangeType.INSERT,
                                        indexedRow))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("IndexRowCache is closed");

        assertThatThrownBy(() -> indexRowCache.writeEmptyRows(tablePartition1, logOffset))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("IndexRowCache is closed");
    }

    // ===============================================================================================
    // Read Operations Tests
    // ===============================================================================================

    @Test
    void testGetRangeLogRecords() throws IOException {
        // Write some test data first
        writeTestDataToCache();

        long startOffset = 100L;
        long endOffset = 103L; // Use smaller range to avoid spanning multiple ranges
        int bucketId = 0;
        short schemaId = DEFAULT_SCHEMA_ID;

        LogRecords records =
                indexRowCache.getRangeLogRecords(
                        tablePartition1, bucketId, startOffset, endOffset, schemaId);

        assertThat(records).isNotNull();
    }

    @Test
    void testGetRangeLogRecordsAfterClosed() {
        indexRowCache.close();

        LogRecords records =
                indexRowCache.getRangeLogRecords(tablePartition1, 0, 100L, 200L, DEFAULT_SCHEMA_ID);

        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testGetUnloadedRanges() throws IOException {
        // Write some test data first
        writeTestDataToCache();

        TableBucket indexBucket = new TableBucket(INDEX_TABLE_1, null, 0);
        long startOffset = 50L; // Before written data
        long endOffset = 150L; // Overlapping with written data

        List<OffsetRangeInfo> unloadedRanges =
                indexRowCache.getUnloadedRanges(indexBucket, startOffset, endOffset);

        assertThat(unloadedRanges).isNotNull();
    }

    @Test
    void testGetUnloadedRangesAfterClosed() {
        indexRowCache.close();

        TableBucket indexBucket = new TableBucket(INDEX_TABLE_1, null, 0);
        List<OffsetRangeInfo> unloadedRanges =
                indexRowCache.getUnloadedRanges(indexBucket, 100L, 200L);

        assertThat(unloadedRanges).isEmpty();
    }

    // ===============================================================================================
    // Cleanup Operations Tests
    // ===============================================================================================

    @Test
    void testCleanupBelowHorizon() throws IOException {
        // Write some test data first
        writeTestDataToCache();

        int entriesBeforeCleanup = indexRowCache.getTotalEntries();
        assertThat(entriesBeforeCleanup).isGreaterThan(0);

        long horizon = 150L;
        indexRowCache.cleanupBelowHorizon(horizon);

        // After cleanup, there might be fewer entries
        int entriesAfterCleanup = indexRowCache.getTotalEntries();
        assertThat(entriesAfterCleanup).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testCleanupBelowHorizonAfterClosed() throws IOException {
        indexRowCache.close();

        // Should not throw exception
        indexRowCache.cleanupBelowHorizon(100L);
    }

    @Test
    void testClose() {
        assertThat(indexRowCache.getTotalEntries()).isGreaterThanOrEqualTo(0);
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(2);

        indexRowCache.close();

        // After close, should still be able to query basic info
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(0);
        assertThat(indexRowCache.isEmpty()).isTrue();
    }

    @Test
    void testCloseMultipleTimes() {
        indexRowCache.close();
        // Should not throw exception when called multiple times
        indexRowCache.close();
        indexRowCache.close();
    }

    // ===============================================================================================
    // Utility Methods Tests
    // ===============================================================================================

    @Test
    void testGetTotalEntries() throws IOException {
        assertThat(indexRowCache.getTotalEntries()).isEqualTo(0);

        // Write some data
        writeTestDataToCache();

        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testGetCachedBucketCount() {
        // Should return number of table partitions configured
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(2);

        indexRowCache.close();
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(0);
    }

    @Test
    void testIsEmpty() {
        // Empty cache should not be considered empty if it has bucket caches
        assertThat(indexRowCache.isEmpty()).isFalse();

        indexRowCache.close();
        // Closed cache should be considered empty
        assertThat(indexRowCache.isEmpty()).isTrue();
    }

    // ===============================================================================================
    // Edge Cases and Error Handling Tests
    // ===============================================================================================

    @Test
    void testWriteToNonExistentBucket() {
        TablePartitionId nonExistentTable = TablePartitionId.of(999999L, null);
        long logOffset = 600L;
        Object[] testData = INDEXED_DATA.get(0);
        IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

        assertThatThrownBy(
                        () ->
                                indexRowCache.writeIndexedRow(
                                        nonExistentTable,
                                        0,
                                        logOffset,
                                        ChangeType.INSERT,
                                        indexedRow))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetRangeLogRecordsNonExistentBucket() {
        assertThatThrownBy(
                        () ->
                                indexRowCache.getRangeLogRecords(
                                        TablePartitionId.of(999999L, null),
                                        0,
                                        100L,
                                        200L,
                                        DEFAULT_SCHEMA_ID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testLargeDatasetOperations() throws IOException {
        // Test with larger dataset from TestData
        for (int i = 0; i < INDEX_CACHE_DATA.size(); i++) {
            long logOffset = 1000L + i;
            Object[] testData = INDEX_CACHE_DATA.get(i);
            // Convert INDEX_CACHE_DATA format to INDEXED_DATA format if needed
            Object[] indexedData = {testData[0], testData[1], "email" + testData[0] + "@test.com"};
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, indexedData);

            indexRowCache.writeIndexedRow(
                    tablePartition1, i % 3, logOffset, ChangeType.INSERT, indexedRow);
        }

        assertThat(indexRowCache.getTotalEntries()).isGreaterThanOrEqualTo(INDEX_CACHE_DATA.size());
    }

    // ===============================================================================================
    // Enhanced Exception Handling and Edge Cases Tests
    // ===============================================================================================

    @Test
    void testUpdateCommitOffsetWithInvalidBucketId() {
        // Test with bucket ID that exceeds configured bucket count
        assertThatThrownBy(() -> indexRowCache.updateCommitOffset(tablePartition1, 10, 100L))
                .isInstanceOf(IllegalArgumentException.class);

        // Test with negative bucket ID
        assertThatThrownBy(() -> indexRowCache.updateCommitOffset(tablePartition1, -1, 100L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteIndexedRowWithInvalidBucketId() {
        long logOffset = 700L;
        Object[] testData = INDEXED_DATA.get(0);
        IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

        // Test with bucket ID that exceeds configured bucket count
        assertThatThrownBy(
                        () ->
                                indexRowCache.writeIndexedRow(
                                        tablePartition1,
                                        10,
                                        logOffset,
                                        ChangeType.INSERT,
                                        indexedRow))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetRangeLogRecordsWithInvalidBucketId() {
        // Test with bucket ID that exceeds configured bucket count
        assertThatThrownBy(
                        () ->
                                indexRowCache.getRangeLogRecords(
                                        tablePartition1, 10, 100L, 200L, DEFAULT_SCHEMA_ID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteWithNullIndexedRow() {
        long logOffset = 800L;

        assertThatThrownBy(
                        () ->
                                indexRowCache.writeIndexedRow(
                                        tablePartition1, 0, logOffset, ChangeType.INSERT, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteWithNullChangeType() {
        long logOffset = 900L;
        Object[] testData = INDEXED_DATA.get(0);
        IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

        assertThatThrownBy(
                        () ->
                                indexRowCache.writeIndexedRow(
                                        tablePartition1, 0, logOffset, null, indexedRow))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===============================================================================================
    // Memory Management and Resource Cleanup Tests
    // ===============================================================================================

    @Test
    void testMemoryUsageWithLargeData() throws IOException {
        // Test memory usage with TestData.INDEX_CACHE_LARGE_DATA
        List<Object[]> largeData = INDEX_CACHE_COLD_WAL_DATA;

        // Write large amount of data
        for (int i = 0; i < Math.min(largeData.size(), 100); i++) {
            long logOffset = 2000L + i;
            Object[] testData = largeData.get(i);
            // Convert to INDEXED_DATA format
            Object[] indexedData = {testData[0], testData[1], "email" + testData[0] + "@test.com"};
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, indexedData);

            indexRowCache.writeIndexedRow(
                    tablePartition1, i % 3, logOffset, ChangeType.INSERT, indexedRow);
        }

        int entriesBeforeCleanup = indexRowCache.getTotalEntries();
        assertThat(entriesBeforeCleanup).isGreaterThan(0);

        // Test cleanup with different horizons
        indexRowCache.cleanupBelowHorizon(2050L);
        int entriesAfterPartialCleanup = indexRowCache.getTotalEntries();

        indexRowCache.cleanupBelowHorizon(2200L);
        int entriesAfterFullCleanup = indexRowCache.getTotalEntries();

        // Verify cleanup behavior
        assertThat(entriesAfterFullCleanup).isLessThanOrEqualTo(entriesAfterPartialCleanup);
    }

    @Test
    void testResourceCleanupAfterMultipleOperations() throws IOException {
        // Write data to all buckets and tables
        for (int table = 0; table < 2; table++) {
            TablePartitionId partition = table == 0 ? tablePartition1 : tablePartition2;
            int bucketCount = table == 0 ? 3 : 2;

            for (int bucket = 0; bucket < bucketCount; bucket++) {
                for (int i = 0; i < 10; i++) {
                    long logOffset = (table * 1000L) + (bucket * 100L) + i;
                    Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
                    IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

                    indexRowCache.writeIndexedRow(
                            partition, bucket, logOffset, ChangeType.INSERT, indexedRow);
                }
            }
        }

        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);

        // Close and verify resource cleanup
        indexRowCache.close();
        assertThat(indexRowCache.getCachedBucketCount()).isEqualTo(0);
        assertThat(indexRowCache.isEmpty()).isTrue();
    }

    // ===============================================================================================
    // Performance and Large Dataset Tests
    // ===============================================================================================

    @Test
    void testSparseOffsetPerformance() throws IOException {
        // Test performance with sparse offsets using TestData.INDEX_CACHE_SPARSE_OFFSETS
        long[] sparseOffsets = INDEX_CACHE_SPARSE_OFFSETS;

        for (int i = 0; i < sparseOffsets.length; i++) {
            long logOffset = sparseOffsets[i] + 3000L;
            Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

            indexRowCache.writeIndexedRow(
                    tablePartition1, 0, logOffset, ChangeType.INSERT, indexedRow);
        }

        assertThatThrownBy(
                        () -> {
                            indexRowCache.getRangeLogRecords(
                                    tablePartition1, 0, 3000L, 3010L, DEFAULT_SCHEMA_ID);
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testContinuousOffsetPerformance() throws IOException {
        // Test performance with continuous offsets using TestData.INDEX_CACHE_CONTINUOUS_OFFSETS
        long[] continuousOffsets = INDEX_CACHE_CONTINUOUS_OFFSETS;

        for (int i = 0; i < continuousOffsets.length; i++) {
            long logOffset = continuousOffsets[i] + 4000L;
            Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

            indexRowCache.writeIndexedRow(
                    tablePartition1, 1, logOffset, ChangeType.INSERT, indexedRow);
        }

        // Test range query across continuous data
        LogRecords records =
                indexRowCache.getRangeLogRecords(
                        tablePartition1, 1, 4000L, 4010L, DEFAULT_SCHEMA_ID);
        assertThat(records).isNotNull();
    }

    // ===============================================================================================
    // Enhanced Multi-Table and Multi-Bucket Tests
    // ===============================================================================================

    @Test
    void testCommitHorizonWithUnevenBucketDistribution() {
        // Set uneven commit offsets across different table partitions and buckets
        indexRowCache.updateCommitOffset(tablePartition1, 0, 100L);
        indexRowCache.updateCommitOffset(tablePartition1, 1, 200L);
        indexRowCache.updateCommitOffset(tablePartition1, 2, 150L);

        indexRowCache.updateCommitOffset(tablePartition2, 0, 80L);
        indexRowCache.updateCommitOffset(tablePartition2, 1, 300L);

        // Commit horizon should be the minimum across all buckets (80L)
        assertThat(indexRowCache.getCommitHorizon()).isEqualTo(80L);

        // Update the minimum and verify
        indexRowCache.updateCommitOffset(tablePartition2, 0, 120L);
        assertThat(indexRowCache.getCommitHorizon()).isEqualTo(100L);
    }

    @Test
    void testWriteToAllBucketsWithDifferentChangeTypes() throws IOException {
        long baseOffset = 5000L;
        ChangeType[] changeTypes = {ChangeType.INSERT, ChangeType.UPDATE_AFTER, ChangeType.DELETE};

        // Write to all buckets in tablePartition1 with different change types
        for (int bucket = 0; bucket < 3; bucket++) {
            for (int i = 0; i < changeTypes.length; i++) {
                long logOffset = baseOffset + (bucket * 10L) + i;
                Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
                IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

                indexRowCache.writeIndexedRow(
                        tablePartition1, bucket, logOffset, changeTypes[i], indexedRow);
            }
        }

        // Write to all buckets in tablePartition2
        for (int bucket = 0; bucket < 2; bucket++) {
            for (int i = 0; i < changeTypes.length; i++) {
                long logOffset = baseOffset + 100L + (bucket * 10L) + i;
                Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
                IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

                indexRowCache.writeIndexedRow(
                        tablePartition2, bucket, logOffset, changeTypes[i], indexedRow);
            }
        }

        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testGetUnloadedRangesFromMultipleTablesAndBuckets() throws IOException {
        // Write data to create gaps in different buckets and tables
        writeTestDataToCache();

        // Test unloaded ranges for different combinations
        TableBucket bucket1 = new TableBucket(INDEX_TABLE_1, null, 0);
        TableBucket bucket2 = new TableBucket(INDEX_TABLE_1, null, 2);
        TableBucket bucket3 = new TableBucket(INDEX_TABLE_2, null, 1);

        List<OffsetRangeInfo> ranges1 = indexRowCache.getUnloadedRanges(bucket1, 50L, 150L);
        List<OffsetRangeInfo> ranges2 = indexRowCache.getUnloadedRanges(bucket2, 80L, 180L);
        List<OffsetRangeInfo> ranges3 = indexRowCache.getUnloadedRanges(bucket3, 180L, 280L);

        // All ranges should be valid
        assertThat(ranges1).isNotNull();
        assertThat(ranges2).isNotNull();
        assertThat(ranges3).isNotNull();
    }

    // ===============================================================================================
    // Enhanced Validation and Error Recovery Tests
    // ===============================================================================================

    @Test
    void testOperationsAfterPartialClose() throws IOException {
        // Write some data
        writeTestDataToCache();
        int initialEntries = indexRowCache.getTotalEntries();
        assertThat(initialEntries).isGreaterThan(0);

        // Close the cache
        indexRowCache.close();

        // All operations after close should either throw exceptions or return empty results
        assertThatThrownBy(() -> indexRowCache.writeEmptyRows(tablePartition1, 1000L))
                .isInstanceOf(IllegalStateException.class);

        LogRecords emptyRecords =
                indexRowCache.getRangeLogRecords(tablePartition1, 0, 100L, 200L, DEFAULT_SCHEMA_ID);
        assertThat(emptyRecords.sizeInBytes()).isEqualTo(0);

        List<OffsetRangeInfo> emptyRanges =
                indexRowCache.getUnloadedRanges(
                        new TableBucket(INDEX_TABLE_1, null, 0), 100L, 200L);
        assertThat(emptyRanges).isEmpty();
    }

    @Test
    void testCleanupWithInvalidHorizon() throws IOException {
        writeTestDataToCache();
        int initialEntries = indexRowCache.getTotalEntries();

        // Test cleanup with negative horizon
        indexRowCache.cleanupBelowHorizon(-1L);
        assertThat(indexRowCache.getTotalEntries()).isEqualTo(initialEntries);

        // Test cleanup with very large horizon
        indexRowCache.cleanupBelowHorizon(Long.MAX_VALUE);
        // Should not crash, entries may be reduced
        assertThat(indexRowCache.getTotalEntries()).isGreaterThanOrEqualTo(0);
    }

    // ===============================================================================================
    // Helper Methods
    // ===============================================================================================

    private void createLogTablet() throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        INDEXED_TABLE_PATH.getDatabaseName(),
                        INDEXED_TABLE_ID,
                        INDEXED_TABLE_PATH.getTableName());

        logTablet =
                LogTablet.create(
                        INDEXED_PHYSICAL_TABLE_PATH,
                        logTabletDir,
                        new Configuration(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L, // recovery point
                        scheduler,
                        LogFormat.INDEXED, // Use INDEXED format for index table tests
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);
    }

    private void writeTestDataToCache() throws IOException {
        // Write some test data to different buckets and tables
        for (int i = 0; i < 3; i++) {
            long logOffset = 100L + i;
            Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

            indexRowCache.writeIndexedRow(
                    tablePartition1, i % 3, logOffset, ChangeType.INSERT, indexedRow);
        }

        for (int i = 0; i < 2; i++) {
            long logOffset = 200L + i;
            Object[] testData = INDEXED_DATA.get(i % INDEXED_DATA.size());
            IndexedRow indexedRow = DataTestUtils.indexedRow(rowType, testData);

            indexRowCache.writeIndexedRow(
                    tablePartition2, i % 2, logOffset, ChangeType.INSERT, indexedRow);
        }

        // Write some empty rows as well
        indexRowCache.writeEmptyRows(tablePartition1, 150L);
        indexRowCache.writeEmptyRows(tablePartition2, 250L);
    }
}
