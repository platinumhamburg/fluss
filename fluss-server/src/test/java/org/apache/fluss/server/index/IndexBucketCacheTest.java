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

import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IndexBucketCache}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Basic write and read operations
 *   <li>Range management and merging
 *   <li>Empty row writes for sparse indexing
 *   <li>Garbage collection below offset
 *   <li>Memory segment management
 *   <li>Edge cases and error handling
 *   <li>Thread safety under concurrent access
 * </ul>
 */
class IndexBucketCacheTest {

    private static final RowType DATA_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()));

    private static final int PAGE_SIZE = 64 * 1024; // 64KB

    private MemorySegmentPool memoryPool;
    private IndexBucketCache cache;
    private TableBucket dataBucket;
    private TableBucket indexBucket;

    @BeforeEach
    void setUp() {
        memoryPool = new TestingMemorySegmentPool(PAGE_SIZE);
        dataBucket = new TableBucket(1000L, 0);
        indexBucket = new TableBucket(2000L, 0);
        cache = new IndexBucketCache(dataBucket, indexBucket, memoryPool);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (cache != null) {
            cache.close();
        }
    }

    // ================================================================================================
    // Basic Write and Read Tests
    // ================================================================================================

    @Test
    void testWriteAndReadSingleRow() throws IOException {
        IndexedRow row = createTestRow(1, "Alice", "alice@example.com");

        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null);

        assertThat(cache.totalEntries()).isEqualTo(1);
        assertThat(cache.hasRangeContaining(0L)).isTrue();
    }

    @Test
    void testWriteMultipleConsecutiveRows() throws IOException {
        for (int i = 0; i < 10; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        assertThat(cache.totalEntries()).isEqualTo(10);
        assertThat(cache.getMinCachedOffset()).isEqualTo(0L);

        // All offsets should be in the same range
        for (int i = 0; i < 10; i++) {
            assertThat(cache.hasRangeContaining(i)).isTrue();
        }
    }

    @Test
    void testReadIndexLogRecords() throws IOException {
        // Write some rows
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        // Read records
        LogRecords records = cache.readIndexLogRecords(0L, 5L);

        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isGreaterThan(0);
    }

    @Test
    void testReadEmptyRange() throws IOException {
        // Read from empty cache
        LogRecords records = cache.readIndexLogRecords(0L, 10L);

        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testReadPartialRange() throws IOException {
        // Write rows [0, 10)
        for (int i = 0; i < 10; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        // Read partial range [3, 7)
        LogRecords records = cache.readIndexLogRecords(3L, 7L);

        assertThat(records).isNotNull();
        assertThat(records.sizeInBytes()).isGreaterThan(0);
    }

    // ================================================================================================
    // Empty Row Write Tests (Sparse Indexing)
    // ================================================================================================

    @Test
    void testWriteEmptyRowForSparseIndexing() throws IOException {
        // Write empty row to expand range without actual data
        cache.writeIndexedRow(5L, null, null, 0L, null);

        // Range should be expanded but no entries
        assertThat(cache.totalEntries()).isEqualTo(0);
        // The range should cover [0, 6)
        assertThat(cache.hasRangeContaining(0L)).isTrue();
        assertThat(cache.hasRangeContaining(5L)).isTrue();
    }

    @Test
    void testMixedEmptyAndDataRows() throws IOException {
        // Write data row at offset 0
        IndexedRow row0 = createTestRow(0, "User0", "user0@example.com");
        cache.writeIndexedRow(0L, ChangeType.INSERT, row0, 0L, null);

        // Write empty rows to expand range
        cache.writeIndexedRow(5L, null, null, 0L, null);

        // Write data row at offset 6
        IndexedRow row6 = createTestRow(6, "User6", "user6@example.com");
        cache.writeIndexedRow(6L, ChangeType.INSERT, row6, 0L, null);

        assertThat(cache.totalEntries()).isEqualTo(2);
        assertThat(cache.hasRangeContaining(0L)).isTrue();
        assertThat(cache.hasRangeContaining(3L)).isTrue();
        assertThat(cache.hasRangeContaining(6L)).isTrue();
    }

    // ================================================================================================
    // Range Management Tests
    // ================================================================================================

    @Test
    void testRangeMerging() throws IOException {
        // Write rows [0, 5)
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        // Write rows [5, 10) - should merge with previous range
        for (int i = 5; i < 10; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        assertThat(cache.totalEntries()).isEqualTo(10);

        // All should be in one continuous range
        for (int i = 0; i < 10; i++) {
            assertThat(cache.hasRangeContaining(i)).isTrue();
        }
    }

    @Test
    void testGapFilling() throws IOException {
        // Write row at offset 0
        IndexedRow row0 = createTestRow(0, "User0", "user0@example.com");
        cache.writeIndexedRow(0L, ChangeType.INSERT, row0, 0L, null);

        // Write row at offset 10 with batchStartOffset=5
        // This should create a gap-filled range
        IndexedRow row10 = createTestRow(10, "User10", "user10@example.com");
        cache.writeIndexedRow(10L, ChangeType.INSERT, row10, 5L, null);

        assertThat(cache.totalEntries()).isEqualTo(2);
    }

    // ================================================================================================
    // Garbage Collection Tests
    // ================================================================================================

    @Test
    void testGarbageCollectBelowOffset() throws IOException {
        // Write rows [0, 20)
        for (int i = 0; i < 20; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        int entriesBefore = cache.totalEntries();
        assertThat(entriesBefore).isEqualTo(20);

        // Garbage collect below offset 10
        cache.garbageCollectBelowOffset(10L);

        // Entries below 10 should be removed
        int entriesAfter = cache.totalEntries();
        assertThat(entriesAfter).isLessThan(entriesBefore);
    }

    @Test
    void testGarbageCollectEmptyCache() throws IOException {
        // Should not throw on empty cache
        cache.garbageCollectBelowOffset(100L);

        assertThat(cache.totalEntries()).isEqualTo(0);
    }

    // ================================================================================================
    // Memory Segment Management Tests
    // ================================================================================================

    @Test
    void testMemorySegmentCount() throws IOException {
        assertThat(cache.getMemorySegmentCount()).isEqualTo(0);

        // Write enough data to allocate memory segments
        for (int i = 0; i < 100; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        assertThat(cache.getMemorySegmentCount()).isGreaterThan(0);
    }

    @Test
    void testGetMemoryPool() {
        assertThat(cache.getMemoryPool()).isSameAs(memoryPool);
    }

    // ================================================================================================
    // Edge Cases and Error Handling Tests
    // ================================================================================================

    @Test
    void testIsEmpty() throws IOException {
        assertThat(cache.isEmpty()).isTrue();

        IndexedRow row = createTestRow(0, "User", "user@example.com");
        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null);

        assertThat(cache.isEmpty()).isFalse();
    }

    @Test
    void testGetMinCachedOffsetEmpty() {
        assertThat(cache.getMinCachedOffset()).isEqualTo(-1L);
    }

    @Test
    void testGetMinCachedOffsetWithData() throws IOException {
        // Write starting at offset 5
        IndexedRow row = createTestRow(5, "User", "user@example.com");
        cache.writeIndexedRow(5L, ChangeType.INSERT, row, 5L, null);

        assertThat(cache.getMinCachedOffset()).isEqualTo(5L);
    }

    @Test
    void testGetIndexBucket() {
        assertThat(cache.getIndexBucket()).isEqualTo(indexBucket);
    }

    @Test
    void testGetOffsetRangeInfoEmpty() {
        assertThat(cache.getOffsetRangeInfo()).isEmpty();
    }

    @Test
    void testGetOffsetRangeInfoWithData() throws IOException {
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        String rangeInfo = cache.getOffsetRangeInfo();
        assertThat(rangeInfo).contains("Range [0, 5)");
        assertThat(rangeInfo).contains("entries=5");
    }

    @Test
    void testWriteAfterClose() throws IOException {
        cache.close();

        IndexedRow row = createTestRow(0, "User", "user@example.com");

        assertThatThrownBy(() -> cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testReadAfterClose() throws IOException {
        // Write some data first
        IndexedRow row = createTestRow(0, "User", "user@example.com");
        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null);

        cache.close();

        // Read should return empty records
        LogRecords records = cache.readIndexLogRecords(0L, 1L);
        assertThat(records.sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testHasRangeContainingAfterClose() throws IOException {
        IndexedRow row = createTestRow(0, "User", "user@example.com");
        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null);

        cache.close();

        assertThat(cache.hasRangeContaining(0L)).isFalse();
    }

    @Test
    void testInvalidReadRange() throws IOException {
        // Write some data
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        // Start >= end should return empty
        LogRecords records = cache.readIndexLogRecords(5L, 3L);
        assertThat(records.sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testGetCachedRecordBytesInRange() throws IOException {
        for (int i = 0; i < 10; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        int bytes = cache.getCachedRecordBytesInRange(0L, 10L);
        assertThat(bytes).isGreaterThan(0);

        // Empty range
        int emptyBytes = cache.getCachedRecordBytesInRange(100L, 200L);
        assertThat(emptyBytes).isEqualTo(0);
    }

    @Test
    void testGetCachedRecordBytesInRangeWithinExistingRange() throws IOException {
        for (int i = 0; i < 5; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        int bytes = cache.getCachedRecordBytesInRange(2L, 5L);
        assertThat(bytes).isGreaterThan(0);
    }

    // ================================================================================================
    // Timestamp Tests
    // ================================================================================================

    @Test
    void testWriteWithTimestamp() throws IOException {
        IndexedRow row = createTestRow(0, "User", "user@example.com");
        Long timestamp = System.currentTimeMillis();

        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, timestamp);

        assertThat(cache.totalEntries()).isEqualTo(1);
    }

    @Test
    void testWriteWithNullTimestamp() throws IOException {
        IndexedRow row = createTestRow(0, "User", "user@example.com");

        cache.writeIndexedRow(0L, ChangeType.INSERT, row, 0L, null);

        assertThat(cache.totalEntries()).isEqualTo(1);
    }

    // ================================================================================================
    // Thread Safety Tests
    // ================================================================================================

    @Test
    void testConcurrentWrites() throws InterruptedException {
        int numThreads = 4;
        int writesPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        List<Exception> exceptions = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < writesPerThread; i++) {
                                int offset = threadId * writesPerThread + i;
                                IndexedRow row =
                                        createTestRow(
                                                offset,
                                                "User" + offset,
                                                "user" + offset + "@example.com");
                                cache.writeIndexedRow(offset, ChangeType.INSERT, row, offset, null);
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(exceptions).isEmpty();
        assertThat(cache.totalEntries()).isEqualTo(numThreads * writesPerThread);
    }

    @Test
    void testConcurrentReadWrite() throws InterruptedException, IOException {
        // Pre-populate some data
        for (int i = 0; i < 50; i++) {
            IndexedRow row = createTestRow(i, "User" + i, "user" + i + "@example.com");
            cache.writeIndexedRow(i, ChangeType.INSERT, row, 0L, null);
        }

        int numWriters = 2;
        int numReaders = 4;
        int operationsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numWriters + numReaders);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numWriters + numReaders);
        List<Exception> exceptions = new ArrayList<>();

        // Writers
        for (int t = 0; t < numWriters; t++) {
            final int threadId = t;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < operationsPerThread; i++) {
                                int offset = 50 + threadId * operationsPerThread + i;
                                IndexedRow row =
                                        createTestRow(
                                                offset,
                                                "User" + offset,
                                                "user" + offset + "@example.com");
                                cache.writeIndexedRow(offset, ChangeType.INSERT, row, offset, null);
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        // Readers
        for (int t = 0; t < numReaders; t++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < operationsPerThread; i++) {
                                cache.readIndexLogRecords(0L, 50L);
                                cache.hasRangeContaining(i % 50);
                                cache.totalEntries();
                                cache.getMemorySegmentCount();
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(exceptions).isEmpty();
    }

    // ================================================================================================
    // Helper Methods
    // ================================================================================================

    private IndexedRow createTestRow(int id, String name, String email) {
        return DataTestUtils.indexedRow(DATA_ROW_TYPE, new Object[] {id, name, email});
    }
}
