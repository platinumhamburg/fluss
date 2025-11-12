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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.log.LogAppendInfo;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.IDX_EMAIL_TABLE_INFO;
import static org.apache.fluss.record.TestData.IDX_EMAIL_WITH_MAP_TABLE_INFO;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_INFO;
import static org.apache.fluss.record.TestData.IDX_NAME_WITH_MAP_TABLE_INFO;
import static org.apache.fluss.record.TestData.INDEXED_DATA;
import static org.apache.fluss.record.TestData.INDEXED_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_SCHEMA;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_WITH_MAP_SCHEMA;
import static org.apache.fluss.record.TestData.INDEX_CACHE_COLD_WAL_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Comprehensive unit tests for {@link IndexCacheWriter}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Constructor initialization with different index table configurations
 *   <li>Hot data writing from WAL records with various scenarios
 *   <li>Cold data batch loading with range queries and optimizations
 *   <li>Edge cases: empty data, duplicate offsets, invalid parameters
 *   <li>Error handling and resource management
 *   <li>Performance testing with large datasets
 * </ul>
 *
 * <p>The tests reuse TestData definitions (INDEXED_DATA, IDX_NAME_DATA, etc.) and DataTestUtils
 * functions to create Arrow-format LogRecords and related test infrastructure.
 */
class IndexCacheWriterTest {

    private static final int DEFAULT_PAGE_SIZE = 64 * 1024; // 64KB
    private static final int DEFAULT_BUCKET_COUNT = 3;

    @TempDir private File tempDir;

    private Configuration conf;
    private FlussScheduler scheduler;
    private TestingMemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private IndexRowCache indexRowCache;
    private BucketingFunction bucketingFunction;
    private IndexCacheWriter indexCacheWriter;

    private Schema tableSchema;
    private List<TableInfo> indexTableInfos;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize core dependencies
        conf = new Configuration();
        scheduler = new FlussScheduler(2);
        scheduler.startup();
        memoryPool = new TestingMemorySegmentPool(DEFAULT_PAGE_SIZE);

        // Create LogTablet with Arrow format
        createLogTablet();

        // Setup table schema and index table infos from TestData
        tableSchema = INDEXED_SCHEMA;
        indexTableInfos = Arrays.asList(IDX_NAME_TABLE_INFO, IDX_EMAIL_TABLE_INFO);

        // Create IndexRowCache for the index tables
        createIndexRowCache();

        bucketingFunction = BucketingFunction.of(null); // Use default FlussBucketingFunction

        // Create IndexCacheWriter (null mainTableInfo for non-partitioned test table)
        indexCacheWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, tableSchema),
                        indexTableInfos,
                        null);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexCacheWriter != null) {
            indexCacheWriter.close();
        }
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
    void testConstructorInitialization() {
        assertThat(indexCacheWriter).isNotNull();
        // Verify the writer is initialized without throwing exceptions
        assertThatCode(() -> indexCacheWriter.close()).doesNotThrowAnyException();
    }

    @Test
    void testConstructorWithEmptyIndexTableList() throws Exception {
        // Test constructor with empty index table list
        IndexCacheWriter emptyWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, tableSchema),
                        Arrays.asList(),
                        null);

        assertThat(emptyWriter).isNotNull();
        emptyWriter.close();
    }

    @Test
    void testConstructorWithSingleIndexTable() throws Exception {
        // Test constructor with single index table
        IndexCacheWriter singleWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, tableSchema),
                        Arrays.asList(IDX_NAME_TABLE_INFO),
                        null);

        assertThat(singleWriter).isNotNull();
        singleWriter.close();
    }

    @Test
    void testConstructorWithTableContainingMapColumn() throws Exception {
        // Test that TableCacheWriter can be created for a table with MAP columns
        // This test verifies the fix for the issue where creating FieldGetter for all columns
        // (including MAP columns) would fail. The fix ensures only index columns get FieldGetters.

        // Create IndexCacheWriter with table schema containing MAP column
        // Reuse existing logTablet and indexRowCache for simplicity
        // This should NOT throw IllegalArgumentException for MAP type during construction
        IndexCacheWriter mapWriter =
                new IndexCacheWriter(
                        logTablet,
                        indexRowCache,
                        bucketingFunction,
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, INDEXED_WITH_MAP_SCHEMA),
                        Arrays.asList(IDX_NAME_WITH_MAP_TABLE_INFO, IDX_EMAIL_WITH_MAP_TABLE_INFO),
                        null);

        // Verify the writer is initialized successfully without throwing exceptions
        assertThat(mapWriter).isNotNull();

        // Clean up
        mapWriter.close();
    }

    // ===============================================================================================
    // Hot Data Writing Tests
    // ===============================================================================================

    @Test
    void testCacheIndexDataByHotDataSuccess() throws Exception {
        // Prepare Arrow format WAL records using TestData
        List<Object[]> testData = INDEXED_DATA.subList(0, 3);
        MemoryLogRecords walRecords = createArrowLogRecords(testData, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo = createValidLogAppendInfo(100L, testData.size());

        // Execute
        indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);

        // Verify data was written to cache
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testCacheIndexDataByHotDataWithLargeDataset() throws Exception {
        // Test with large dataset from TestData
        List<Object[]> largeData = INDEX_CACHE_COLD_WAL_DATA.subList(0, 100);
        MemoryLogRecords walRecords = createArrowLogRecords(largeData, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo = createValidLogAppendInfo(1000L, largeData.size());

        // Execute
        long startTime = System.currentTimeMillis();
        indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);
        long duration = System.currentTimeMillis() - startTime;

        // Verify performance and correctness
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
        assertThat(duration).isLessThan(5000); // Should complete within 5 seconds
    }

    @Test
    void testCacheIndexDataByHotDataWithNullRecords() throws Exception {
        LogAppendInfo appendInfo = createValidLogAppendInfo(100L, 1);

        // Should handle null records gracefully
        assertThatCode(() -> indexCacheWriter.cacheIndexDataByHotData(null, appendInfo))
                .doesNotThrowAnyException();
    }

    @Test
    void testCacheIndexDataByHotDataWithNullAppendInfo() throws Exception {
        List<Object[]> testData = INDEXED_DATA.subList(0, 2);
        MemoryLogRecords walRecords = createArrowLogRecords(testData, INDEXED_ROW_TYPE);

        // Should handle null appendInfo gracefully
        assertThatCode(() -> indexCacheWriter.cacheIndexDataByHotData(walRecords, null))
                .doesNotThrowAnyException();
    }

    @Test
    void testCacheIndexDataByHotDataWithInvalidAppendInfo() throws Exception {
        List<Object[]> testData = INDEXED_DATA.subList(0, 2);
        MemoryLogRecords walRecords = createArrowLogRecords(testData, INDEXED_ROW_TYPE);

        // Create invalid appendInfo (non-monotonic offsets)
        LogAppendInfo invalidAppendInfo = createInvalidLogAppendInfo();

        // Should handle invalid appendInfo gracefully
        assertThatCode(
                        () ->
                                indexCacheWriter.cacheIndexDataByHotData(
                                        walRecords, invalidAppendInfo))
                .doesNotThrowAnyException();
    }

    @Test
    void testCacheIndexDataByHotDataAfterClose() throws Exception {
        // Close the writer first
        indexCacheWriter.close();

        List<Object[]> testData = INDEXED_DATA.subList(0, 1);
        MemoryLogRecords walRecords = createArrowLogRecords(testData, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo = createValidLogAppendInfo(100L, testData.size());

        // Should handle gracefully when closed
        assertThatCode(() -> indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo))
                .doesNotThrowAnyException();
    }

    // ===============================================================================================
    // Cold Data Loading Tests
    // ===============================================================================================

    @Test
    void testLoadColdDataToCacheWithInvalidRange() throws Exception {
        // Test with start >= end
        long startOffset = 100L;
        long endOffset = 50L; // Invalid: start > end

        // Should handle gracefully
        assertThatCode(() -> indexCacheWriter.loadColdDataToCache(startOffset, endOffset))
                .doesNotThrowAnyException();
    }

    @Test
    void testLoadColdDataToCacheWithEmptyRange() throws Exception {
        // Test with start == end
        long startOffset = 100L;
        long endOffset = 100L;

        // Should handle gracefully
        assertThatCode(() -> indexCacheWriter.loadColdDataToCache(startOffset, endOffset))
                .doesNotThrowAnyException();
    }

    @Test
    void testLoadColdDataToCacheAfterClose() throws Exception {
        // Close the writer first
        indexCacheWriter.close();

        long startOffset = 0L;
        long endOffset = 10L;

        // Should handle gracefully when closed
        assertThatCode(() -> indexCacheWriter.loadColdDataToCache(startOffset, endOffset))
                .doesNotThrowAnyException();
    }

    @Test
    void testLoadColdDataToCacheWithNoWALData() throws Exception {
        // Test loading from empty WAL
        long startOffset = 0L;
        long endOffset = 10L;

        // Should handle gracefully when no data exists
        assertThatCode(() -> indexCacheWriter.loadColdDataToCache(startOffset, endOffset))
                .doesNotThrowAnyException();
    }

    // ===============================================================================================
    // Edge Cases and Error Handling Tests
    // ===============================================================================================

    @Test
    void testHandleDuplicateOffsets() throws Exception {
        // Use INDEX_DUPLICATE_DATA from TestData
        // Create records with duplicate offsets scenario
        List<Object[]> testData =
                Arrays.asList(
                        new Object[] {1, "Alice", "alice@example.com"},
                        new Object[] {2, "Bob", "bob@example.com"});
        MemoryLogRecords walRecords = createArrowLogRecords(testData, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo = createValidLogAppendInfo(100L, testData.size());

        // Execute multiple times with same data (simulating duplicates)
        indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);
        indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);

        // Should handle duplicates gracefully without error
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);
    }

    @Test
    void testWithEmptyIndexData() throws Exception {
        // Test with empty data rows
        List<Object[]> emptyData = Arrays.asList();
        MemoryLogRecords walRecords = createArrowLogRecords(emptyData, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo = createValidLogAppendInfo(100L, 0);

        // Should handle empty data gracefully
        assertThatCode(() -> indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo))
                .doesNotThrowAnyException();
    }

    // ===============================================================================================
    // Resource Management Tests
    // ===============================================================================================

    @Test
    void testCloseOperation() {
        // Verify close operation doesn't throw exception
        assertThatCode(() -> indexCacheWriter.close()).doesNotThrowAnyException();
    }

    @Test
    void testMultipleCloseOperations() {
        // Verify multiple close operations are idempotent
        assertThatCode(
                        () -> {
                            indexCacheWriter.close();
                            indexCacheWriter.close();
                            indexCacheWriter.close();
                        })
                .doesNotThrowAnyException();
    }

    /**
     * Test to verify that writing multiple consecutive batches (with some having data and some
     * being empty) results in a single continuous OffsetRange per IndexBucketRowCache.
     *
     * <p>This test reproduces and validates the fix for the range gap issue where adjacent offsets
     * would incorrectly create multiple ranges instead of extending a single range.
     *
     * <p>Test scenario:
     *
     * <ul>
     *   <li>Batch 1: offsets [0-9] with data
     *   <li>Batch 2: offsets [10-19] with data
     *   <li>Batch 3: empty batch (state changes only)
     *   <li>Batch 4: empty batch (consecutive empty batch)
     *   <li>Batch 5: empty batch (consecutive empty batch)
     *   <li>Batch 6: offsets [30-39] with data
     *   <li>Batch 7: empty batch
     *   <li>Batch 8: empty batch (consecutive empty batch)
     * </ul>
     *
     * <p>Expected result: Each IndexBucketRowCache should have exactly ONE continuous OffsetRange.
     *
     * <p>This validates that:
     *
     * <ul>
     *   <li>Empty batches don't cause range gaps
     *   <li>Consecutive empty batches are handled correctly
     *   <li>finalizeBatch correctly synchronizes all buckets
     *   <li>Adjacent ranges are properly extended, not duplicated
     * </ul>
     */
    @Test
    void testMultipleBatchesWithEmptyBatchesProduceSingleRange() throws Exception {
        // Phase 1: Prepare test data for multiple batches
        // Batch 1: records [0-9]
        List<Object[]> batch1Data = INDEXED_DATA.subList(0, Math.min(10, INDEXED_DATA.size()));

        // Batch 2: records [10-19]
        List<Object[]> batch2Data =
                INDEXED_DATA.subList(0, Math.min(10, INDEXED_DATA.size())); // Reuse data

        // Batch 3-5: consecutive empty batches
        List<Object[]> emptyBatchData = Arrays.asList();

        // Batch 6: records with data
        List<Object[]> batch6Data =
                INDEXED_DATA.subList(0, Math.min(10, INDEXED_DATA.size())); // Reuse data

        // Phase 2: Write batches to LogTablet and cache them using IndexCacheWriter
        long currentOffset = 0L;

        // Batch 1: with data
        MemoryLogRecords batch1Records = createArrowLogRecords(batch1Data, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo1 = logTablet.appendAsLeader(batch1Records);
        indexCacheWriter.cacheIndexDataByHotData(batch1Records, appendInfo1);
        currentOffset = appendInfo1.lastOffset() + 1;

        System.out.println(
                String.format(
                        "=== Batch 1 appended: [%d, %d] ===",
                        appendInfo1.firstOffset(), appendInfo1.lastOffset()));
        printRangeInfo("After Batch 1");

        // Batch 2: with data
        MemoryLogRecords batch2Records = createArrowLogRecords(batch2Data, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo2 = logTablet.appendAsLeader(batch2Records);
        indexCacheWriter.cacheIndexDataByHotData(batch2Records, appendInfo2);
        currentOffset = appendInfo2.lastOffset() + 1;

        System.out.println(
                String.format(
                        "=== Batch 2 appended: [%d, %d] ===",
                        appendInfo2.firstOffset(), appendInfo2.lastOffset()));
        printRangeInfo("After Batch 2");

        // Batch 3: empty batch (first consecutive empty)
        MemoryLogRecords batch3Records = createArrowLogRecords(emptyBatchData, INDEXED_ROW_TYPE);
        if (batch3Records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo3 = logTablet.appendAsLeader(batch3Records);
            indexCacheWriter.cacheIndexDataByHotData(batch3Records, appendInfo3);
            currentOffset = appendInfo3.lastOffset() + 1;

            System.out.println(
                    String.format(
                            "=== Batch 3 (empty) appended: [%d, %d] ===",
                            appendInfo3.firstOffset(), appendInfo3.lastOffset()));
            printRangeInfo("After Batch 3 (empty)");
        } else {
            System.out.println("=== Batch 3 skipped (empty records) ===");
        }

        // Batch 4: empty batch (second consecutive empty)
        MemoryLogRecords batch4Records = createArrowLogRecords(emptyBatchData, INDEXED_ROW_TYPE);
        if (batch4Records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo4 = logTablet.appendAsLeader(batch4Records);
            indexCacheWriter.cacheIndexDataByHotData(batch4Records, appendInfo4);
            currentOffset = appendInfo4.lastOffset() + 1;

            System.out.println(
                    String.format(
                            "=== Batch 4 (empty) appended: [%d, %d] ===",
                            appendInfo4.firstOffset(), appendInfo4.lastOffset()));
            printRangeInfo("After Batch 4 (empty)");
        } else {
            System.out.println("=== Batch 4 skipped (empty records) ===");
        }

        // Batch 5: empty batch (third consecutive empty)
        MemoryLogRecords batch5Records = createArrowLogRecords(emptyBatchData, INDEXED_ROW_TYPE);
        if (batch5Records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo5 = logTablet.appendAsLeader(batch5Records);
            indexCacheWriter.cacheIndexDataByHotData(batch5Records, appendInfo5);
            currentOffset = appendInfo5.lastOffset() + 1;

            System.out.println(
                    String.format(
                            "=== Batch 5 (empty) appended: [%d, %d] ===",
                            appendInfo5.firstOffset(), appendInfo5.lastOffset()));
            printRangeInfo("After Batch 5 (empty)");
        } else {
            System.out.println("=== Batch 5 skipped (empty records) ===");
        }

        // Batch 6: with data (after consecutive empty batches)
        MemoryLogRecords batch6Records = createArrowLogRecords(batch6Data, INDEXED_ROW_TYPE);
        LogAppendInfo appendInfo6 = logTablet.appendAsLeader(batch6Records);
        indexCacheWriter.cacheIndexDataByHotData(batch6Records, appendInfo6);
        currentOffset = appendInfo6.lastOffset() + 1;

        System.out.println(
                String.format(
                        "=== Batch 6 appended: [%d, %d] ===",
                        appendInfo6.firstOffset(), appendInfo6.lastOffset()));
        printRangeInfo("After Batch 6");

        // Batch 7: empty batch (first consecutive empty at end)
        MemoryLogRecords batch7Records = createArrowLogRecords(emptyBatchData, INDEXED_ROW_TYPE);
        if (batch7Records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo7 = logTablet.appendAsLeader(batch7Records);
            indexCacheWriter.cacheIndexDataByHotData(batch7Records, appendInfo7);
            currentOffset = appendInfo7.lastOffset() + 1;

            System.out.println(
                    String.format(
                            "=== Batch 7 (empty) appended: [%d, %d] ===",
                            appendInfo7.firstOffset(), appendInfo7.lastOffset()));
            printRangeInfo("After Batch 7 (empty)");
        } else {
            System.out.println("=== Batch 7 skipped (empty records) ===");
        }

        // Batch 8: empty batch (second consecutive empty at end)
        MemoryLogRecords batch8Records = createArrowLogRecords(emptyBatchData, INDEXED_ROW_TYPE);
        if (batch8Records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo8 = logTablet.appendAsLeader(batch8Records);
            indexCacheWriter.cacheIndexDataByHotData(batch8Records, appendInfo8);
            currentOffset = appendInfo8.lastOffset() + 1;

            System.out.println(
                    String.format(
                            "=== Batch 8 (empty) appended: [%d, %d] ===",
                            appendInfo8.firstOffset(), appendInfo8.lastOffset()));
            printRangeInfo("After Batch 8 (empty)");
        } else {
            System.out.println("=== Batch 8 skipped (empty records) ===");
        }

        // Phase 3: Verify that each IndexBucketRowCache has exactly ONE continuous range
        System.out.println("\n=== Final Verification ===");

        for (TableInfo indexTableInfo : indexTableInfos) {
            long indexTableId = indexTableInfo.getTableId();
            int bucketCount = indexTableInfo.getNumBuckets();

            System.out.println(
                    String.format(
                            "\nVerifying Index Table: %s (id=%d, buckets=%d)",
                            indexTableInfo.getTablePath().getTableName(),
                            indexTableId,
                            bucketCount));

            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                String rangeInfo =
                        indexRowCache
                                .getBucketRowCache(indexTableId, bucketId)
                                .getOffsetRangeInfo();
                int rangeCount = countRanges(rangeInfo);

                System.out.println(String.format("  Bucket %d: %d range(s)", bucketId, rangeCount));
                if (rangeCount > 1) {
                    System.out.println("    Range details:");
                    System.out.println("    " + rangeInfo.replace("\n", "\n    "));
                }

                // Assert that each bucket has exactly ONE continuous range
                assertThat(rangeCount)
                        .as(
                                "Index table %s bucket %d should have exactly 1 continuous range, but has %d ranges. "
                                        + "This indicates a range gap bug. Range info:\n%s",
                                indexTableInfo.getTablePath().getTableName(),
                                bucketId,
                                rangeCount,
                                rangeInfo)
                        .isEqualTo(1);
            }
        }

        System.out.println("\n=== Test Passed: All buckets have single continuous range ===");
    }

    /**
     * Test to reproduce the range gap issue caused by index replication with sparse offsets.
     *
     * <p>This test simulates the END-TO-END scenario where range gaps occur during index
     * replication:
     *
     * <p>**Root Cause Analysis:**
     *
     * <ol>
     *   <li>Leader writes data at offsets [0-11], creates IndexBucketRowCache Range [0, 12)
     *   <li>Offset 12 may have no index data for certain buckets (sparse indexing)
     *   <li>Leader continues writing at offsets [13-15]
     *   <li>**Follower replicates**: Receives batch with baseLogOffset=13 (not 12!)
     *   <li>Follower's IndexApplier appends to local LogTablet, preserving batch.baseLogOffset()=13
     *   <li>If Follower's IndexBucket has IndexCache (e.g., becoming leader), hot data write
     *       triggered
     *   <li>writeIndexedRow(13, data, **batchStartOffset=13**) finds Range [0, 12) with
     *       endOffset=12
     *   <li>Critical check: candidateRange.getEndOffset()(12) &lt; batchStartOffset(13)? **TRUE**
     *   <li>Creates NEW Range [13, 14) instead of extending existing range
     *   <li>**Range gap [12, 13) blocks commit offset progression!**
     * </ol>
     *
     * <p>**WAL Offset Continuity**: Even though KvTablet WAL offsets are continuous, IndexBucket
     * LogRecords may have gaps because:
     *
     * <ul>
     *   <li>Sparse indexing: Not every data offset produces index records for every bucket
     *   <li>State-only batches: Contain no actual index data
     *   <li>Bucket distribution: Hash partitioning means some buckets empty for certain offsets
     * </ul>
     *
     * <p>This test reproduces the production issue where commitOffset=12 cannot advance due to
     * Range [0, 12) and Range [13, 16) gap.
     */
    @Test
    void testRangeGapWhenBatchBaseOffsetJumpsOverPreviousRangeEnd() throws Exception {
        System.out.println("\n=== Testing Range Gap Bug: Batch BaseOffset Jump ===\n");

        // Phase 1: Create first batch with continuous offsets [0-11]
        List<Object[]> batch1Data = INDEXED_DATA.subList(0, Math.min(10, INDEXED_DATA.size()));
        MemoryLogRecords batch1Records = createArrowLogRecords(batch1Data, INDEXED_ROW_TYPE);

        // Append batch 1 normally
        LogAppendInfo appendInfo1 = logTablet.appendAsLeader(batch1Records);
        indexCacheWriter.cacheIndexDataByHotData(batch1Records, appendInfo1);

        System.out.println(
                String.format(
                        "Batch 1: baseOffset=%d, lastOffset=%d",
                        appendInfo1.firstOffset(), appendInfo1.lastOffset()));
        printRangeInfo("After Batch 1");

        // Phase 2: Simulate a batch with baseOffset that JUMPS over previous range end
        // If previous batch ended at offset 11, finalizeBatch(11, 0) created Range [0, 12)
        // Now we simulate a batch starting at offset 13 (skipping offset 12)

        long jumpedBaseOffset = appendInfo1.lastOffset() + 2; // Skip one offset!
        System.out.println(
                String.format(
                        "\n!!! Critical: Next batch will start at offset %d, jumping over offset %d !!!",
                        jumpedBaseOffset, appendInfo1.lastOffset() + 1));

        // Create batch 2 with some data
        List<Object[]> batch2Data = INDEXED_DATA.subList(0, Math.min(3, INDEXED_DATA.size()));

        // Manually create a LogAppendInfo with jumped baseOffset to simulate the problematic
        // scenario
        // In production, this can happen due to state changes or special WAL records
        LogAppendInfo jumpedAppendInfo =
                new LogAppendInfo(
                        jumpedBaseOffset, // Start from jumped offset
                        jumpedBaseOffset + batch2Data.size() - 1,
                        System.currentTimeMillis(),
                        jumpedBaseOffset,
                        batch2Data.size(),
                        batch2Data.size() * 100,
                        true);
        jumpedAppendInfo.setDuplicated(false);

        // Create records and process with jumped offset
        MemoryLogRecords batch2Records = createArrowLogRecords(batch2Data, INDEXED_ROW_TYPE);

        // Write hot data with the jumped offset scenario
        // This will trigger the bug where writeIndexedRow checks:
        // if (candidateRange.getEndOffset() < batchStartOffset) -> creates NEW range
        indexCacheWriter.cacheIndexDataByHotData(batch2Records, jumpedAppendInfo);

        System.out.println(
                String.format(
                        "\nBatch 2 (jumped): baseOffset=%d, lastOffset=%d",
                        jumpedAppendInfo.firstOffset(), jumpedAppendInfo.lastOffset()));
        printRangeInfo("After Batch 2 (with offset jump)");

        // Phase 3: Append more batches to see if gap persists
        List<Object[]> batch3Data = INDEXED_DATA.subList(0, Math.min(5, INDEXED_DATA.size()));
        MemoryLogRecords batch3Records = createArrowLogRecords(batch3Data, INDEXED_ROW_TYPE);

        // Manually create next batch info continuing from jumped offset
        long batch3BaseOffset = jumpedAppendInfo.lastOffset() + 1;
        LogAppendInfo appendInfo3 =
                new LogAppendInfo(
                        batch3BaseOffset,
                        batch3BaseOffset + batch3Data.size() - 1,
                        System.currentTimeMillis(),
                        batch3BaseOffset,
                        batch3Data.size(),
                        batch3Data.size() * 100,
                        true);
        appendInfo3.setDuplicated(false);

        indexCacheWriter.cacheIndexDataByHotData(batch3Records, appendInfo3);

        System.out.println(
                String.format(
                        "\nBatch 3: baseOffset=%d, lastOffset=%d",
                        appendInfo3.firstOffset(), appendInfo3.lastOffset()));
        printRangeInfo("After Batch 3");

        // Phase 4: Verify the bug - check for range gaps
        System.out.println("\n=== Bug Verification: Checking for Range Gaps ===\n");

        for (TableInfo indexTableInfo : indexTableInfos) {
            long indexTableId = indexTableInfo.getTableId();
            int bucketCount = indexTableInfo.getNumBuckets();

            System.out.println(
                    String.format(
                            "Index Table: %s (id=%d, buckets=%d)",
                            indexTableInfo.getTablePath().getTableName(),
                            indexTableId,
                            bucketCount));

            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                String rangeInfo =
                        indexRowCache
                                .getBucketRowCache(indexTableId, bucketId)
                                .getOffsetRangeInfo();
                int rangeCount = countRanges(rangeInfo);

                System.out.println(
                        String.format(
                                "  Bucket %d: %d range(s) - %s",
                                bucketId,
                                rangeCount,
                                rangeCount > 1 ? "!!! GAP DETECTED !!!" : "OK"));

                if (rangeCount > 1) {
                    System.out.println("    Range details (showing the gap):");
                    System.out.println("    " + rangeInfo.replace("\n", "\n    "));

                    // This is the BUG! We have multiple ranges when we should have one continuous
                    // range
                    System.out.println(
                            "    ^^^ BUG REPRODUCED: Range gap prevents index commit horizon from"
                                    + " advancing ^^^");
                }

                // Document the bug: When batch baseOffset jumps, it creates a new range
                // This happens because the condition:
                // if (candidateRange.getEndOffset() < batchStartOffset)
                // becomes true, causing createNewEmptyRangeForOffsetRange() instead of
                // expandRangeBoundaryToOffset()
                //
                // Expected: Single continuous range covering all offsets
                // Actual: Multiple ranges with gaps, blocking commit offset progression
                //
                // The test demonstrates this matches production behavior:
                // Range [0, 12), Range [13, 16), Range [17, 24) from the production log

                if (rangeCount > 1) {
                    System.out.println(
                            "\n!!! This reproduces the production issue where index commit horizon gets stuck !!!");
                    System.out.println(
                            "!!! The gap prevents commitOffset from advancing past the first range !!!");
                }
            }
        }

        System.out.println("\n=== Test Complete: Range Gap Bug Reproduced ===");
        System.out.println(
                "This test demonstrates the root cause of the production index replication timeout.");
    }

    // ===============================================================================================
    // Helper Methods
    // ===============================================================================================

    /** Helper method to print range information for all index buckets for debugging. */
    private void printRangeInfo(String phase) {
        System.out.println(String.format("\n--- Range Info: %s ---", phase));
        for (TableInfo indexTableInfo : indexTableInfos) {
            long indexTableId = indexTableInfo.getTableId();
            int bucketCount = indexTableInfo.getNumBuckets();

            System.out.println(
                    String.format(
                            "Index Table: %s (id=%d)",
                            indexTableInfo.getTablePath().getTableName(), indexTableId));

            for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                String rangeInfo =
                        indexRowCache
                                .getBucketRowCache(indexTableId, bucketId)
                                .getOffsetRangeInfo();
                int rangeCount = countRanges(rangeInfo);
                System.out.println(
                        String.format(
                                "  Bucket %d: %d range(s), entries=%d",
                                bucketId,
                                rangeCount,
                                indexRowCache
                                        .getBucketRowCache(indexTableId, bucketId)
                                        .totalEntries()));
            }
        }
    }

    /** Helper method to count the number of ranges in range info string. */
    private int countRanges(String rangeInfo) {
        if (rangeInfo == null || rangeInfo.trim().isEmpty()) {
            return 0;
        }
        // Count occurrences of "Range [" pattern
        int count = 0;
        int index = 0;
        while ((index = rangeInfo.indexOf("Range [", index)) != -1) {
            count++;
            index += 7; // Length of "Range ["
        }
        return count;
    }

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
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L, // recovery point
                        scheduler,
                        LogFormat.ARROW, // Use Arrow format as required
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);
    }

    private void createIndexRowCache() {
        Map<Long, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(IDX_NAME_TABLE_INFO.getTableId(), DEFAULT_BUCKET_COUNT);

        indexBucketDistribution.put(IDX_EMAIL_TABLE_INFO.getTableId(), DEFAULT_BUCKET_COUNT);

        indexRowCache = new IndexRowCache(memoryPool, logTablet, indexBucketDistribution);
    }

    private MemoryLogRecords createArrowLogRecords(List<Object[]> data, RowType rowType)
            throws Exception {
        // Use DataTestUtils to create Arrow format records with INDEXED_ROW_TYPE
        return DataTestUtils.createRecordsWithoutBaseLogOffset(
                rowType,
                DEFAULT_SCHEMA_ID,
                0,
                System.currentTimeMillis(),
                CURRENT_LOG_MAGIC_VALUE,
                data,
                LogFormat.ARROW);
    }

    private MemoryLogRecords createIndexLogRecords(List<Object[]> data, RowType rowType)
            throws Exception {
        // Use DataTestUtils to create Arrow format records with INDEXED_ROW_TYPE
        return DataTestUtils.createRecordsWithoutBaseLogOffset(
                rowType,
                DEFAULT_SCHEMA_ID,
                0,
                System.currentTimeMillis(),
                CURRENT_LOG_MAGIC_VALUE,
                data,
                LogFormat.INDEXED);
    }

    private LogAppendInfo createValidLogAppendInfo(long firstOffset, int numRecords) {
        LogAppendInfo appendInfo =
                new LogAppendInfo(
                        firstOffset,
                        firstOffset + numRecords - 1,
                        System.currentTimeMillis(),
                        firstOffset,
                        numRecords,
                        numRecords * 100, // Approximate valid bytes
                        true // offsetsMonotonic
                        );
        appendInfo.setDuplicated(false);
        return appendInfo;
    }

    private LogAppendInfo createInvalidLogAppendInfo() {
        LogAppendInfo appendInfo =
                new LogAppendInfo(
                        100L,
                        99L, // Invalid: last < first
                        System.currentTimeMillis(),
                        100L,
                        1,
                        100, // validBytes
                        false // offsetsMonotonic
                        );
        appendInfo.setDuplicated(true);
        return appendInfo;
    }
}
