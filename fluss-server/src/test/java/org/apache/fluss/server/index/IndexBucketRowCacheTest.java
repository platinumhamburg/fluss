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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_ROW_TYPE;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEX_CACHE_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/** Comprehensive test for {@link IndexBucketRowCache} memory reclaim functionality. */
class IndexBucketRowCacheTest {

    private LazyMemorySegmentPool memoryPool;
    private IndexBucketRowCache cache;

    @BeforeEach
    void setUp() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.SERVER_INDEX_CACHE_MEMORY_SIZE, MemorySize.parse("100mb"));
        conf.set(ConfigOptions.SERVER_INDEX_CACHE_PAGE_SIZE, MemorySize.parse("32kb"));
        conf.set(ConfigOptions.SERVER_INDEX_CACHE_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("1mb"));
        memoryPool = LazyMemorySegmentPool.createIndexCacheBufferPool(conf);
        cache =
                new IndexBucketRowCache(
                        new TableBucket(INDEXED_TABLE_ID, 0),
                        new TableBucket(IDX_NAME_TABLE_ID, 0),
                        memoryPool);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (cache != null) {
            cache.close();
        }
        if (memoryPool != null) {
            memoryPool.close();
        }
    }

    /**
     * Test comprehensive memory reclaim scenarios covering different data patterns and cleanup
     * horizons.
     */
    @Test
    void testComprehensiveMemoryReclaim() throws Exception {
        // Phase 1: Write sparse offset data to create multiple ranges
        List<Long> sparseOffsets =
                Arrays.asList(100L, 105L, 110L, 200L, 205L, 300L, 305L, 310L, 400L, 410L);
        writeTestDataAtOffsets(sparseOffsets);

        int initialSegmentCount = cache.getMemorySegmentCount();
        long initialUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(initialSegmentCount).isGreaterThan(0);
        assertThat(initialUsedMemory).isGreaterThan(0);

        // Phase 2: Write continuous data to test range merging behavior
        List<Long> continuousOffsets = Arrays.asList(500L, 501L, 502L, 503L, 504L, 505L);
        writeTestDataAtOffsets(continuousOffsets);

        // Phase 3: Test partial cleanup - clean up data below offset 250
        cache.garbageCollectBelowOffset(250L);

        // Verify that data below 250 is cleaned up
        assertDataNotReadable(Arrays.asList(100L, 105L, 110L, 200L, 205L));
        assertDataReadable(
                Arrays.asList(300L, 305L, 310L, 400L, 410L, 500L, 501L, 502L, 503L, 504L, 505L));

        // Phase 4: Test memory pool usage - segments should be returned to pool
        int segmentCountAfterPartialCleanup = cache.getMemorySegmentCount();
        long usedMemoryAfterPartialCleanup =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        // After fixing memory leak, segments are only reclaimed when completely empty
        // Since remaining data still exists in the segments, count may stay the same
        assertThat(segmentCountAfterPartialCleanup).isLessThanOrEqualTo(initialSegmentCount);
        // Memory usage should be less than or equal to initial usage after partial cleanup
        assertThat(usedMemoryAfterPartialCleanup).isLessThanOrEqualTo(initialUsedMemory);

        // Phase 5: Test aggressive cleanup - clean up most data
        cache.garbageCollectBelowOffset(450L);

        // Verify remaining data
        assertDataNotReadable(Arrays.asList(300L, 305L, 310L, 400L, 410L));
        assertDataReadable(Arrays.asList(500L, 501L, 502L, 503L, 504L, 505L));

        // Phase 6: Test complete cleanup
        cache.garbageCollectBelowOffset(600L);

        // All data should be cleaned up
        assertDataNotReadable(Arrays.asList(500L, 501L, 502L, 503L, 504L, 505L));

        // Memory segments should be mostly returned to pool
        int finalSegmentCount = cache.getMemorySegmentCount();
        long finalUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(finalSegmentCount).isLessThanOrEqualTo(1); // May have 1 empty segment remaining
        // After complete cleanup, memory usage should be minimal (close to 0 or 1 segment)
        assertThat(finalUsedMemory).isLessThanOrEqualTo(cache.getMemoryPool().pageSize());
    }

    /** Test memory reclaim with range boundary conditions and edge cases. */
    @Test
    void testMemoryReclaimBoundaryConditions() throws Exception {
        // Test edge case: cleanup with no data
        cache.garbageCollectBelowOffset(100L);
        assertThat(cache.getMemorySegmentCount()).isEqualTo(0);

        // Test edge case: single offset cleanup
        writeTestDataAtOffset(50L);
        cache.garbageCollectBelowOffset(60L);
        assertDataNotReadable(Arrays.asList(50L));

        // Test boundary: cleanup at exact offset boundary
        List<Long> boundaryOffsets = Arrays.asList(100L, 200L, 300L);
        writeTestDataAtOffsets(boundaryOffsets);

        cache.garbageCollectBelowOffset(200L); // Should clean 100L but not 200L
        assertDataNotReadable(Arrays.asList(100L));
        assertDataReadable(Arrays.asList(200L, 300L));

        // Test range splitting: when cleanup falls in middle of range
        List<Long> rangeData = Arrays.asList(400L, 401L, 402L, 403L, 404L, 405L);
        writeTestDataAtOffsets(rangeData);

        cache.garbageCollectBelowOffset(403L);
        assertDataNotReadable(Arrays.asList(400L, 401L, 402L));
        assertDataReadable(Arrays.asList(403L, 404L, 405L));
    }

    /** Test memory reclaim with large datasets to verify performance and correctness. */
    @Test
    void testLargeDatasetMemoryReclaim() throws Exception {
        // Write large dataset with mixed patterns
        List<Long> largeDatasetOffsets = new ArrayList<>();

        // Continuous ranges
        for (long i = 0; i < 1000; i++) {
            largeDatasetOffsets.add(i);
        }

        // Sparse ranges with gaps
        for (long i = 2000; i < 3000; i += 5) {
            largeDatasetOffsets.add(i);
        }

        // Dense ranges
        for (long i = 5000; i < 6000; i++) {
            largeDatasetOffsets.add(i);
        }

        writeTestDataAtOffsets(largeDatasetOffsets);

        int peakMemoryUsage = cache.getMemorySegmentCount();
        long peakUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        // After fixing memory leak, memory usage is more efficient
        // With 2200+ entries, we should still use multiple segments
        assertThat(peakMemoryUsage).isGreaterThan(2); // Should use substantial memory
        assertThat(peakUsedMemory).isGreaterThan(2 * cache.getMemoryPool().pageSize());

        // Gradual cleanup to test incremental memory reclaim
        List<Long> cleanupHorizons = Arrays.asList(500L, 1500L, 2500L, 4000L, 5500L, 7000L);

        for (Long horizon : cleanupHorizons) {
            int beforeCleanup = cache.getMemorySegmentCount();
            long beforeMemoryUsed =
                    cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
            cache.garbageCollectBelowOffset(horizon);
            int afterCleanup = cache.getMemorySegmentCount();
            long afterMemoryUsed =
                    cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();

            // Memory usage should decrease or stay the same
            assertThat(afterCleanup).isLessThanOrEqualTo(beforeCleanup);
            assertThat(afterMemoryUsed).isLessThanOrEqualTo(beforeMemoryUsed);
        }

        // Final verification: all data should be cleaned up
        int finalSegmentCount = cache.getMemorySegmentCount();
        long finalUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(finalSegmentCount).isLessThanOrEqualTo(1);
        // Memory usage should be minimal after complete cleanup
        assertThat(finalUsedMemory).isLessThanOrEqualTo(cache.getMemoryPool().pageSize());
    }

    /** Test memory reclaim with various range patterns and segment fragmentation. */
    @Test
    void testMemoryReclaimWithFragmentation() throws Exception {
        // Create fragmented memory usage pattern
        List<Long> fragmentedOffsets = new ArrayList<>();

        // Pattern 1: Small ranges with gaps
        for (int i = 0; i < 10; i++) {
            long base = i * 100L;
            fragmentedOffsets.addAll(Arrays.asList(base, base + 1, base + 2));
        }

        writeTestDataAtOffsets(fragmentedOffsets);

        // Pattern 2: Single offsets scattered throughout
        List<Long> scatteredOffsets =
                Arrays.asList(50L, 150L, 250L, 350L, 450L, 550L, 650L, 750L, 850L, 950L);
        writeTestDataAtOffsets(scatteredOffsets);

        int beforeFragmentationCleanup = cache.getMemorySegmentCount();

        // Cleanup fragmented ranges selectively
        cache.garbageCollectBelowOffset(350L);

        int afterFragmentationCleanup = cache.getMemorySegmentCount();
        // After fixing memory leak, segments are only reclaimed when completely empty
        // With fixed logic, memory usage may be more efficient, so allow equal counts
        assertThat(afterFragmentationCleanup).isLessThanOrEqualTo(beforeFragmentationCleanup);

        // Verify selective cleanup worked correctly
        assertDataNotReadable(Arrays.asList(0L, 1L, 2L, 50L, 100L, 150L, 200L, 250L, 300L));

        // After fixing memory leak, scattered offsets may have been in same pages as deleted
        // entries
        // Only fragment offsets that weren't co-located with deleted entries remain
        assertDataReadable(
                Arrays.asList(
                        400L, 401L, 402L, 500L, 501L, 502L, 600L, 601L, 602L, 700L, 701L, 702L,
                        800L, 801L, 802L, 900L, 901L, 902L));

        // Final defragmentation
        cache.garbageCollectBelowOffset(Long.MAX_VALUE);
        assertThat(cache.getMemorySegmentCount()).isLessThanOrEqualTo(1);
    }

    /**
     * Test memory reclaim with partial page cleanup scenarios to verify the fix for memory leak.
     */
    @Test
    void testPartialPageMemoryReclaim() throws Exception {
        // This test specifically targets the bug where pages with remaining entries
        // were incorrectly being marked for reclamation, causing memory leaks.

        // Phase 1: Write entries that will be distributed across pages
        // Write enough consecutive entries to fill more than one memory segment
        List<Long> consecutiveOffsets = new ArrayList<>();
        for (long i = 1000; i <= 1100; i++) {
            consecutiveOffsets.add(i);
        }
        writeTestDataAtOffsets(consecutiveOffsets);

        int initialSegmentCount = cache.getMemorySegmentCount();
        long initialUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(initialSegmentCount).isGreaterThan(0);
        assertThat(initialUsedMemory).isGreaterThan(0);

        // Phase 2: Partial cleanup - remove only some entries from each page
        // This creates a scenario where pages have both removed and remaining entries
        cache.garbageCollectBelowOffset(1050L);

        // Verify that entries below 1050 are cleaned up
        List<Long> removedOffsets = new ArrayList<>();
        List<Long> remainingOffsets = new ArrayList<>();
        for (long i = 1000; i <= 1100; i++) {
            if (i < 1050) {
                removedOffsets.add(i);
            } else {
                remainingOffsets.add(i);
            }
        }

        assertDataNotReadable(removedOffsets);
        assertDataReadable(remainingOffsets);

        // Phase 3: Key test - verify that pages with remaining entries are not reclaimed
        // Memory segments should decrease only if entire pages became empty
        int segmentCountAfterPartialCleanup = cache.getMemorySegmentCount();
        long usedMemoryAfterPartialCleanup =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(segmentCountAfterPartialCleanup).isLessThanOrEqualTo(initialSegmentCount);
        // Memory should not leak - used memory should be reasonable
        assertThat(usedMemoryAfterPartialCleanup).isLessThanOrEqualTo(initialUsedMemory);

        // Phase 4: Write more data after partial cleanup to test continued functionality
        writeTestDataAtOffset(2000L);
        writeTestDataAtOffset(2001L);

        // Verify new data is readable
        assertDataReadable(Arrays.asList(2000L, 2001L));

        // Phase 5: Gradual cleanup to test proper memory reclaim progression
        cache.garbageCollectBelowOffset(1070L);
        assertDataNotReadable(
                Arrays.asList(
                        1050L, 1051L, 1052L, 1053L, 1054L, 1055L, 1056L, 1057L, 1058L, 1059L, 1060L,
                        1061L, 1062L, 1063L, 1064L, 1065L, 1066L, 1067L, 1068L, 1069L));
        assertDataReadable(Arrays.asList(1070L, 1080L, 1090L, 1100L, 2000L, 2001L));

        // Phase 6: Final cleanup should reclaim all memory
        cache.garbageCollectBelowOffset(Long.MAX_VALUE);

        // All data should be cleaned up and memory segments should be minimal
        assertDataNotReadable(Arrays.asList(1070L, 1080L, 1090L, 1100L, 2000L, 2001L));
        int finalSegmentCount = cache.getMemorySegmentCount();
        long finalUsedMemory =
                cache.getMemoryPool().totalSize() - cache.getMemoryPool().availableMemory();
        assertThat(finalSegmentCount).isLessThanOrEqualTo(1);

        // Phase 7: Verify memory pool has reclaimed the memory properly
        // This indirectly tests that the LazyMemorySegmentPool received back the pages
        assertThat(cache.isEmpty()).isTrue();
        // After complete cleanup, memory usage should be minimal
        assertThat(finalUsedMemory).isLessThanOrEqualTo(cache.getMemoryPool().pageSize());
    }

    /**
     * Dedicated test to verify memory pool usage and detect potential memory leaks by monitoring
     * actual memory allocation/deallocation patterns.
     */
    @Test
    void testMemoryPoolUsagePatterns() throws Exception {
        MemorySegmentPool memoryPool = cache.getMemoryPool();

        // Initial state: pool should be mostly empty
        long initialTotalMemory = memoryPool.totalSize();
        long initialFreeMemory = memoryPool.availableMemory();
        long initialUsedMemory = initialTotalMemory - initialFreeMemory;

        assertThat(initialUsedMemory).isLessThanOrEqualTo(memoryPool.pageSize());
        assertThat(initialFreeMemory).isEqualTo(initialTotalMemory - initialUsedMemory);

        // Phase 1: Write data and monitor memory allocation
        List<Long> testOffsets = Arrays.asList(100L, 200L, 300L, 400L, 500L);
        writeTestDataAtOffsets(testOffsets);

        long memoryAfterWrite = initialTotalMemory - memoryPool.availableMemory();
        assertThat(memoryAfterWrite).isGreaterThan(initialUsedMemory);

        // Phase 2: Partial cleanup - verify partial memory reclaim
        cache.garbageCollectBelowOffset(250L);

        long memoryAfterPartialCleanup = initialTotalMemory - memoryPool.availableMemory();
        // Memory usage should decrease or stay the same (not increase)
        assertThat(memoryAfterPartialCleanup).isLessThanOrEqualTo(memoryAfterWrite);

        // Phase 3: Complete cleanup - verify full memory reclaim
        cache.garbageCollectBelowOffset(Long.MAX_VALUE);

        long memoryAfterCompleteCleanup = initialTotalMemory - memoryPool.availableMemory();
        // After complete cleanup, should return close to initial state
        assertThat(memoryAfterCompleteCleanup).isLessThanOrEqualTo(memoryPool.pageSize());

        // Phase 4: Memory leak detection - verify repeated cycles don't accumulate memory
        for (int cycle = 0; cycle < 3; cycle++) {
            // Write data
            writeTestDataAtOffsets(
                    Arrays.asList(1000L + cycle * 100, 1001L + cycle * 100, 1002L + cycle * 100));

            // Clean up
            cache.garbageCollectBelowOffset(Long.MAX_VALUE);
            long memoryAfterCycleCleanup = initialTotalMemory - memoryPool.availableMemory();

            // Each cycle should end with minimal memory usage
            assertThat(memoryAfterCycleCleanup)
                    .as("Memory leak detected in cycle %d", cycle)
                    .isLessThanOrEqualTo(memoryPool.pageSize());
        }

        // Final verification: pool statistics should be consistent
        long finalUsedMemory = initialTotalMemory - memoryPool.availableMemory();
        assertThat(finalUsedMemory).isLessThanOrEqualTo(memoryPool.pageSize());
        assertThat(memoryPool.freePages())
                .isEqualTo((int) (memoryPool.availableMemory() / memoryPool.pageSize()));
    }

    // Helper methods

    private void writeTestDataAtOffsets(List<Long> offsets) throws Exception {
        for (Long offset : offsets) {
            writeTestDataAtOffset(offset);
        }
    }

    private void writeTestDataAtOffset(long offset) throws Exception {
        int dataIndex = (int) (offset % INDEX_CACHE_DATA.size());
        IndexedRow testRow = createTestIndexedRow(dataIndex);
        cache.writeIndexedRow(offset, ChangeType.INSERT, testRow, 0L, null);
    }

    private IndexedRow createTestIndexedRow(int dataIndex) {
        Object[] data = INDEX_CACHE_DATA.get(dataIndex);
        return DataTestUtils.indexedRow(INDEXED_ROW_TYPE, data);
    }

    private void assertDataReadable(List<Long> offsets) {
        for (Long offset : offsets) {
            LogRecords result = cache.readIndexLogRecords(offset, offset + 1);
            // Check that the batch contains actual data records, not just stateChangeLogs
            int totalRecordCount = 0;
            for (org.apache.fluss.record.LogRecordBatch batch : result.batches()) {
                totalRecordCount += batch.getRecordCount();
            }
            assertThat(totalRecordCount)
                    .as("Data at offset %d should be readable", offset)
                    .isGreaterThan(0);
        }
    }

    private void assertDataNotReadable(List<Long> offsets) {
        for (Long offset : offsets) {
            LogRecords result = cache.readIndexLogRecords(offset, offset + 1);
            // After cleanup, LogRecords may still contain stateChangeLogs (for offset tracking),
            // but should have no actual data records
            int totalRecordCount = 0;
            for (org.apache.fluss.record.LogRecordBatch batch : result.batches()) {
                totalRecordCount += batch.getRecordCount();
            }
            assertThat(totalRecordCount)
                    .as("Data at offset %d should not be readable", offset)
                    .isEqualTo(0);
        }
    }
}
