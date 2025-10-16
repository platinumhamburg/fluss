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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
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

    private static final int PAGE_SIZE = 32 * 1024; // 32KB
    private static final int POOL_SIZE = 100; // 100 pages in pool
    private static final Duration TEST_TIMEOUT = Duration.ofMinutes(2);

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
        assertThat(initialSegmentCount).isGreaterThan(0);

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
        assertThat(segmentCountAfterPartialCleanup).isLessThan(initialSegmentCount);

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
        assertThat(finalSegmentCount).isLessThanOrEqualTo(1); // May have 1 empty segment remaining
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
        assertThat(peakMemoryUsage).isGreaterThan(10); // Should use substantial memory

        // Gradual cleanup to test incremental memory reclaim
        List<Long> cleanupHorizons = Arrays.asList(500L, 1500L, 2500L, 4000L, 5500L, 7000L);

        for (Long horizon : cleanupHorizons) {
            int beforeCleanup = cache.getMemorySegmentCount();
            cache.garbageCollectBelowOffset(horizon);
            int afterCleanup = cache.getMemorySegmentCount();

            // Memory usage should decrease or stay the same
            assertThat(afterCleanup).isLessThanOrEqualTo(beforeCleanup);
        }

        // Final verification: all data should be cleaned up
        assertThat(cache.getMemorySegmentCount()).isLessThanOrEqualTo(1);
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
        assertThat(afterFragmentationCleanup).isLessThan(beforeFragmentationCleanup);

        // Verify selective cleanup worked correctly
        assertDataNotReadable(Arrays.asList(0L, 1L, 2L, 50L, 100L, 150L, 200L, 250L, 300L));
        assertDataReadable(Arrays.asList(400L, 401L, 402L, 450L, 500L, 550L));

        // Final defragmentation
        cache.garbageCollectBelowOffset(Long.MAX_VALUE);
        assertThat(cache.getMemorySegmentCount()).isLessThanOrEqualTo(1);
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
        cache.writeIndexedRow(offset, ChangeType.INSERT, testRow, 0L);
    }

    private IndexedRow createTestIndexedRow(int dataIndex) {
        Object[] data = INDEX_CACHE_DATA.get(dataIndex);
        return DataTestUtils.indexedRow(INDEXED_ROW_TYPE, data);
    }

    private void assertDataReadable(List<Long> offsets) {
        for (Long offset : offsets) {
            LogRecords result = cache.readIndexLogRecords(offset, offset + 1);
            assertThat(result.sizeInBytes())
                    .as("Data at offset %d should be readable", offset)
                    .isGreaterThan(0);
        }
    }

    private void assertDataNotReadable(List<Long> offsets) {
        for (Long offset : offsets) {
            LogRecords result = cache.readIndexLogRecords(offset, offset + 1);
            assertThat(result.sizeInBytes())
                    .as("Data at offset %d should not be readable", offset)
                    .isEqualTo(0);
        }
    }
}
