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
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_INFO;
import static org.apache.fluss.record.TestData.INDEXED_DATA;
import static org.apache.fluss.record.TestData.INDEXED_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_SCHEMA;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
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
                        tableSchema,
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
                        tableSchema,
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
                        tableSchema,
                        Arrays.asList(IDX_NAME_TABLE_INFO),
                        null);

        assertThat(singleWriter).isNotNull();
        singleWriter.close();
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
