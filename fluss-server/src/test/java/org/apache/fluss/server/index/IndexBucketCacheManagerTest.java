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
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IndexBucketCacheManager}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Initialization with index bucket distribution
 *   <li>Write operations to target buckets
 *   <li>Read operations from cache
 *   <li>Commit offset management and horizon calculation
 *   <li>Synchronization of all buckets to offset
 *   <li>Diagnostic information retrieval
 *   <li>Resource cleanup and close operations
 * </ul>
 */
class IndexBucketCacheManagerTest {

    private static final RowType DATA_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()));

    private static final int PAGE_SIZE = 64 * 1024; // 64KB
    private static final long DATA_TABLE_ID = 1000L;
    private static final long INDEX_TABLE_1 = 10001L;
    private static final long INDEX_TABLE_2 = 10002L;
    private static final int BUCKET_COUNT = 3;
    private static final TablePath DATA_TABLE_PATH = TablePath.of("test_db", "test_table");

    @TempDir private File tempDir;

    private MemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private FlussScheduler scheduler;
    private IndexBucketCacheManager cacheManager;

    @BeforeEach
    void setUp() throws Exception {
        memoryPool = new TestingMemorySegmentPool(PAGE_SIZE);
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        // Create LogTablet
        logTablet = createLogTablet();

        // Create index bucket distribution
        Map<Long, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(INDEX_TABLE_1, BUCKET_COUNT);
        indexBucketDistribution.put(INDEX_TABLE_2, BUCKET_COUNT);

        cacheManager = new IndexBucketCacheManager(memoryPool, logTablet, indexBucketDistribution);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    // ================================================================================================
    // Initialization Tests
    // ================================================================================================

    @Test
    void testInitialization() {
        assertThat(cacheManager.isEmpty()).isFalse();
        assertThat(cacheManager.getCachedBucketCount()).isEqualTo(2); // 2 index tables
        assertThat(cacheManager.getTotalEntries()).isEqualTo(0);
    }

    @Test
    void testInitialCommitOffsets() {
        // All commit offsets should be 0 initially (not -1)
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            assertThat(cacheManager.getCommitOffset(INDEX_TABLE_1, bucket)).isEqualTo(0L);
            assertThat(cacheManager.getCommitOffset(INDEX_TABLE_2, bucket)).isEqualTo(0L);
        }
    }

    @Test
    void testInitialCommitHorizon() {
        // Initial commit horizon should be 0 (minimum of all commit offsets)
        assertThat(cacheManager.getCommitHorizon()).isEqualTo(0L);
    }

    // ================================================================================================
    // Write Operation Tests
    // ================================================================================================

    @Test
    void testWriteIndexedRowToTargetBucket() throws IOException {
        IndexedRow row = createTestRow(1, "Alice", "alice@example.com");

        cacheManager.writeIndexedRowToTargetBucket(
                INDEX_TABLE_1, 0, 0L, ChangeType.INSERT, row, 0L, null);

        assertThat(cacheManager.getTotalEntries()).isEqualTo(1);
    }

    @Test
    void testWriteToMultipleBuckets() throws IOException {
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            IndexedRow row =
                    createTestRow(bucket, "User" + bucket, "user" + bucket + "@example.com");
            cacheManager.writeIndexedRowToTargetBucket(
                    INDEX_TABLE_1, bucket, bucket, ChangeType.INSERT, row, bucket, null);
        }

        assertThat(cacheManager.getTotalEntries()).isEqualTo(BUCKET_COUNT);
    }

    @Test
    void testWriteToMultipleIndexTables() throws IOException {
        IndexedRow row1 = createTestRow(1, "User1", "user1@example.com");
        IndexedRow row2 = createTestRow(2, "User2", "user2@example.com");

        cacheManager.writeIndexedRowToTargetBucket(
                INDEX_TABLE_1, 0, 0L, ChangeType.INSERT, row1, 0L, null);
        cacheManager.writeIndexedRowToTargetBucket(
                INDEX_TABLE_2, 0, 0L, ChangeType.INSERT, row2, 0L, null);

        assertThat(cacheManager.getTotalEntries()).isEqualTo(2);
    }

    @Test
    void testWriteWithNullChangeTypeThrows() {
        IndexedRow row = createTestRow(1, "User", "user@example.com");

        assertThatThrownBy(
                        () ->
                                cacheManager.writeIndexedRowToTargetBucket(
                                        INDEX_TABLE_1, 0, 0L, null, row, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteWithNullRowThrows() {
        assertThatThrownBy(
                        () ->
                                cacheManager.writeIndexedRowToTargetBucket(
                                        INDEX_TABLE_1, 0, 0L, ChangeType.INSERT, null, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteToInvalidBucketThrows() {
        IndexedRow row = createTestRow(1, "User", "user@example.com");

        assertThatThrownBy(
                        () ->
                                cacheManager.writeIndexedRowToTargetBucket(
                                        INDEX_TABLE_1, 999, 0L, ChangeType.INSERT, row, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWriteToInvalidIndexTableThrows() {
        IndexedRow row = createTestRow(1, "User", "user@example.com");

        assertThatThrownBy(
                        () ->
                                cacheManager.writeIndexedRowToTargetBucket(
                                        99999L, 0, 0L, ChangeType.INSERT, row, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ================================================================================================
    // Read Operation Tests
    // ================================================================================================

    @Test
    void testReadIndexLogRecordsSuccess() throws IOException {
        // Write some data
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cacheManager.writeIndexedRowToTargetBucket(
                    INDEX_TABLE_1, 0, i, ChangeType.INSERT, row, 0L, null);
        }

        IndexBucketCacheManager.ReadResult result =
                cacheManager.readIndexLogRecords(INDEX_TABLE_1, 0, 0L, 5L);

        assertThat(result.getStatus()).isEqualTo(IndexBucketCacheManager.ReadStatus.LOADED);
        assertThat(result.getRecords()).isNotNull();
        assertThat(result.getRecords().sizeInBytes()).isGreaterThan(0);
    }

    @Test
    void testReadIndexLogRecordsNotLoaded() {
        // Read from empty cache
        IndexBucketCacheManager.ReadResult result =
                cacheManager.readIndexLogRecords(INDEX_TABLE_1, 0, 0L, 10L);

        assertThat(result.getStatus()).isEqualTo(IndexBucketCacheManager.ReadStatus.NOT_LOADED);
    }

    @Test
    void testReadFromNonExistentRange() throws IOException {
        // Write data at offset 0
        IndexedRow row = createTestRow(0, "User", "user@example.com");
        cacheManager.writeIndexedRowToTargetBucket(
                INDEX_TABLE_1, 0, 0L, ChangeType.INSERT, row, 0L, null);

        // Try to read from offset 100 (not loaded)
        IndexBucketCacheManager.ReadResult result =
                cacheManager.readIndexLogRecords(INDEX_TABLE_1, 0, 100L, 110L);

        assertThat(result.getStatus()).isEqualTo(IndexBucketCacheManager.ReadStatus.NOT_LOADED);
    }

    // ================================================================================================
    // Commit Offset Management Tests
    // ================================================================================================

    @Test
    void testUpdateCommitOffset() {
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 0, 100L, 100L, 1);

        assertThat(cacheManager.getCommitOffset(INDEX_TABLE_1, 0)).isEqualTo(100L);
    }

    @Test
    void testCommitHorizonCalculation() {
        // Update different buckets with different offsets
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 0, 100L, 100L, 1);
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 1, 50L, 50L, 1);
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 2, 200L, 200L, 1);

        // Horizon should be minimum across all buckets (including INDEX_TABLE_2 which is still 0)
        assertThat(cacheManager.getCommitHorizon()).isEqualTo(0L);

        // Update all INDEX_TABLE_2 buckets
        for (int i = 0; i < BUCKET_COUNT; i++) {
            cacheManager.updateCommitOffset(INDEX_TABLE_2, i, 30L, 30L, 1);
        }

        // Now horizon should be 30 (minimum of all)
        assertThat(cacheManager.getCommitHorizon()).isEqualTo(30L);
    }

    @Test
    void testGetBlockingBucketDiagnostics() {
        // Update some buckets
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 0, 100L, 100L, 1);
        cacheManager.updateCommitOffset(INDEX_TABLE_1, 1, 50L, 50L, 2);

        List<String> diagnostics = cacheManager.getBlockingBucketDiagnostics();

        // Should contain diagnostics for buckets at the commit horizon (0)
        assertThat(diagnostics).isNotEmpty();
    }

    // ================================================================================================
    // Synchronization Tests
    // ================================================================================================

    @Test
    void testSynchronizeAllBucketsToOffset() throws IOException {
        cacheManager.synchronizeAllBucketsToOffset(INDEX_TABLE_1, 10L, 0L);

        // All buckets should have range covering offset 10
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            IndexBucketCache bucketCache = cacheManager.getBucketCache(INDEX_TABLE_1, bucket);
            assertThat(bucketCache.hasRangeContaining(9L)).isTrue();
        }
    }

    // ================================================================================================
    // Diagnostic Information Tests
    // ================================================================================================

    @Test
    void testGetMinCacheWatermark() throws IOException {
        // Initially no data
        assertThat(cacheManager.getMinCacheWatermark()).isEqualTo(-1L);

        // Write data starting at offset 5
        IndexedRow row = createTestRow(5, "User", "user@example.com");
        cacheManager.writeIndexedRowToTargetBucket(
                INDEX_TABLE_1, 0, 5L, ChangeType.INSERT, row, 5L, null);

        assertThat(cacheManager.getMinCacheWatermark()).isEqualTo(5L);
    }

    @Test
    void testGetTotalMemorySegmentCount() throws IOException {
        assertThat(cacheManager.getTotalMemorySegmentCount()).isEqualTo(0);

        // Write enough data to allocate memory segments
        for (int i = 0; i < 100; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cacheManager.writeIndexedRowToTargetBucket(
                    INDEX_TABLE_1, 0, i, ChangeType.INSERT, row, 0L, null);
        }

        assertThat(cacheManager.getTotalMemorySegmentCount()).isGreaterThan(0);
    }

    @Test
    void testGetIndexBucketRangeInfo() throws IOException {
        // Write some data
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cacheManager.writeIndexedRowToTargetBucket(
                    INDEX_TABLE_1, 0, i, ChangeType.INSERT, row, 0L, null);
        }

        String rangeInfo = cacheManager.getIndexBucketRangeInfo(INDEX_TABLE_1, 0);

        assertThat(rangeInfo).contains("Range [0, 5)");
    }

    @Test
    void testGetIndexBucketRangeInfoEmpty() {
        String rangeInfo = cacheManager.getIndexBucketRangeInfo(INDEX_TABLE_1, 0);
        assertThat(rangeInfo).isEmpty();
    }

    @Test
    void testGetCachedBytesOfIndexBucketInRange() throws IOException {
        // Write some data
        for (int i = 0; i < 10; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cacheManager.writeIndexedRowToTargetBucket(
                    INDEX_TABLE_1, 0, i, ChangeType.INSERT, row, 0L, null);
        }

        TableBucket indexBucket = new TableBucket(INDEX_TABLE_1, 0);
        int bytes = cacheManager.getCachedBytesOfIndexBucketInRange(indexBucket, 0L, 10L);

        assertThat(bytes).isGreaterThan(0);
    }

    // ================================================================================================
    // Close and Cleanup Tests
    // ================================================================================================

    @Test
    void testClose() {
        cacheManager.close();

        // After close, operations should fail or return safe defaults
        assertThatThrownBy(
                        () ->
                                cacheManager.writeIndexedRowToTargetBucket(
                                        INDEX_TABLE_1,
                                        0,
                                        0L,
                                        ChangeType.INSERT,
                                        createTestRow(0, "User", "user@example.com"),
                                        0L,
                                        null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testDoubleClose() {
        cacheManager.close();
        // Second close should not throw
        cacheManager.close();
    }

    @Test
    void testReadAfterClose() {
        cacheManager.close();

        IndexBucketCacheManager.ReadResult result =
                cacheManager.readIndexLogRecords(INDEX_TABLE_1, 0, 0L, 10L);

        assertThat(result.getStatus()).isEqualTo(IndexBucketCacheManager.ReadStatus.NOT_LOADED);
    }

    // ================================================================================================
    // Helper Methods
    // ================================================================================================

    private LogTablet createLogTablet() throws Exception {
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA_TABLE_PATH.getDatabaseName(),
                        DATA_TABLE_ID,
                        DATA_TABLE_PATH.getTableName());

        return LogTablet.create(
                PhysicalTablePath.of(DATA_TABLE_PATH),
                logDir,
                new Configuration(),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0L,
                scheduler,
                LogFormat.INDEXED,
                1,
                false,
                org.apache.fluss.utils.clock.SystemClock.getInstance(),
                true);
    }

    private IndexedRow createTestRow(int id, String name, String email) {
        return DataTestUtils.indexedRow(DATA_ROW_TYPE, new Object[] {id, name, email});
    }
}
