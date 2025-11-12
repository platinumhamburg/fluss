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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive unit tests for {@link IndexApplier}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Basic functionality: initialization, application, commit offset retrieval
 *   <li>Index application status tracking for multiple data buckets
 *   <li>High watermark updates and commit offset calculations
 *   <li>Thread safety and concurrent operations
 *   <li>Error handling and edge cases
 *   <li>Idempotent operations and gap detection
 * </ul>
 */
class IndexApplierTest {

    private static final int DEFAULT_PAGE_SIZE = 64 * 1024; // 64KB

    private @TempDir File tempDir;
    private @TempDir File kvDir;

    private Configuration conf;
    private FlussScheduler scheduler;
    private TestingMemorySegmentPool memoryPool;
    private BufferAllocator arrowBufferAllocator;

    private LogTablet logTablet;
    private KvTablet kvTablet;
    private IndexApplier indexApplier;

    private Schema indexSchema;
    private RowType indexRowType;

    // Test data buckets - representing different upstream data buckets
    private TableBucket dataBucket1;
    private TableBucket dataBucket2;
    private TableBucket dataBucket3;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize configuration
        conf = new Configuration();

        // Initialize memory pool
        memoryPool = new TestingMemorySegmentPool(DEFAULT_PAGE_SIZE);

        // Create scheduler
        scheduler = new FlussScheduler(2);
        scheduler.startup();

        // Create Arrow buffer allocator
        arrowBufferAllocator = new RootAllocator(Long.MAX_VALUE);

        // Use test data schema for index table
        indexSchema = DATA1_SCHEMA_PK;
        indexRowType = indexSchema.getRowType();

        // Create LogTablet and KvTablet
        createLogTablet();
        createKvTablet();

        // Create IndexApplier with null DataLakeFormat (for testing)
        indexApplier =
                new IndexApplier(
                        kvTablet,
                        logTablet,
                        indexSchema,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

        // Setup test data buckets - these represent upstream data table buckets
        dataBucket1 = new TableBucket(DATA1_TABLE_ID, 0L, 0);
        dataBucket2 = new TableBucket(DATA1_TABLE_ID, 0L, 1);
        dataBucket3 = new TableBucket(DATA2_TABLE_ID, 0L, 0);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexApplier != null) {
            indexApplier.close();
        }
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
        if (arrowBufferAllocator != null) {
            arrowBufferAllocator.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    // ================================================================================================
    // Basic Functionality Tests
    // ================================================================================================

    @Test
    void testApplyIndexRecordsAfterClose() throws Exception {
        // Close the applier first
        indexApplier.close();

        // Try to apply records after close
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 2), dataBucket1, 2L);

        long result = indexApplier.applyIndexRecords(records, 0L, 2L, dataBucket1);

        // Should return start offset when closed (idempotent)
        assertThat(result).isEqualTo(0L);
    }

    // ================================================================================================
    // Index Application Status Tests
    // ================================================================================================

    @Test
    void testMultipleDataBucketsIndependentProgress() throws Exception {
        // Apply records for first data bucket
        MemoryLogRecords records1 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 3), dataBucket1, 3L);
        long applied1 = indexApplier.applyIndexRecords(records1, 0L, 3L, dataBucket1);
        assertThat(applied1).isEqualTo(3L);

        // Apply records for second data bucket
        MemoryLogRecords records2 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(3, 6), dataBucket2, 3L);
        long applied2 = indexApplier.applyIndexRecords(records2, 0L, 3L, dataBucket2);
        assertThat(applied2).isEqualTo(3L);

        updateHighWatermark(3);

        // Verify independent progress
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(3L);
        // idempotent
        assertThat(indexApplier.getIndexCommitOffset(dataBucket2)).isEqualTo(0L);
        // untouched
        assertThat(indexApplier.getIndexCommitOffset(dataBucket3)).isEqualTo(0L);
    }

    @Test
    void testSequentialIndexApplication() throws Exception {
        // Apply the first batch of index records
        // Index records [0, 2) correspond to data bucket [0, 2)
        MemoryLogRecords records1 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 2), dataBucket1, 2L);
        long applied1 = indexApplier.applyIndexRecords(records1, 0L, 2L, dataBucket1);
        assertThat(applied1).isEqualTo(2L);

        // Apply the second batch of index records (adjacent to the first batch)
        // Index records [2, 4) correspond to data bucket [2, 4)
        MemoryLogRecords records2 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(2, 4), dataBucket1, 4L);
        long applied2 = indexApplier.applyIndexRecords(records2, 2L, 4L, dataBucket1);
        assertThat(applied2).isEqualTo(4L);
        updateHighWatermark(4);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(4L);

        // Empty batch can be committed directly when no partial commit is pending
        MemoryLogRecords emptyRecords1 = createEmptyIndexMemoryLogRecordsWithState(dataBucket1, 7L);
        long applied3 = indexApplier.applyIndexRecords(emptyRecords1, 4L, 7L, dataBucket1);
        assertThat(applied3).isEqualTo(7L);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(7L);

        // Partial commit scenario
        MemoryLogRecords records3 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(4, 6), dataBucket1, 10L);
        long applied4 = indexApplier.applyIndexRecords(records3, 7L, 10L, dataBucket1);
        assertThat(applied4).isEqualTo(10L);
        updateHighWatermark(6L);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(9L);

        // Empty batch cannot be committed directly when partial commit is pending
        MemoryLogRecords emptyRecords2 =
                createEmptyIndexMemoryLogRecordsWithState(dataBucket1, 12L);
        long applied5 = indexApplier.applyIndexRecords(emptyRecords2, 10L, 12L, dataBucket1);
        assertThat(applied5).isEqualTo(12L);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(9L);

        // When high watermark is updated, all pending commits should be committed
        updateHighWatermark(7L);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(12L);
    }

    @Test
    void testGapDetectionInIndexApplication() throws Exception {
        // Apply initial batch [0, 3)
        MemoryLogRecords records1 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 3), dataBucket1, 3L);
        long applied1 = indexApplier.applyIndexRecords(records1, 0L, 3L, dataBucket1);
        assertThat(applied1).isEqualTo(3L);

        // Try to apply with gap [5, 8) - should be idempotent
        MemoryLogRecords records2 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(5, 8), dataBucket1, 8L);
        long applied2 = indexApplier.applyIndexRecords(records2, 5L, 8L, dataBucket1);
        assertThat(applied2).isEqualTo(3L); // Should return current applied offset due to gap

        // Apply correct next batch [3, 6) - should work
        MemoryLogRecords records3 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(3, 6), dataBucket1, 6L);
        long applied3 = indexApplier.applyIndexRecords(records3, 3L, 6L, dataBucket1);
        assertThat(applied3).isEqualTo(6L);
    }

    // ================================================================================================
    // High Watermark and Commit Offset Tests
    // ================================================================================================

    @Test
    void testHighWatermarkUpdateWithSingleDataBucket() throws Exception {
        // Apply index records [0, 5)
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 5), dataBucket1, 5L);

        // Apply the records and get the index bucket offset range
        long applied = indexApplier.applyIndexRecords(records, 0L, 5L, dataBucket1);
        assertThat(applied).isEqualTo(5L);

        // Get the current log tablet's high watermark to determine index offset range
        long indexStartOffset = 0L; // First offset in log
        long indexEndOffset = 5L; // Based on 5 records applied

        // Test when high watermark is in the middle of applied range
        long hwInMiddle = indexStartOffset + 2;
        updateHighWatermark(hwInMiddle);

        // indexCommitDataOffset should be calculated as:
        // hwInMiddle - indexStartOffset + dataStartOffset = hwInMiddle - indexStartOffset + 0
        long expectedCommitOffset = hwInMiddle - indexStartOffset;
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(expectedCommitOffset);

        // Test when high watermark covers all applied range
        updateHighWatermark(indexEndOffset);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(5L); // dataEndOffset

        // Test when high watermark is beyond applied range
        long hwBeyond = indexEndOffset + 10;
        updateHighWatermark(hwBeyond);
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1))
                .isEqualTo(5L); // should stay same
    }

    @Test
    void testHighWatermarkUpdateWithMultipleDataBuckets() throws Exception {
        // Apply records for first data bucket [0, 3)
        MemoryLogRecords records1 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 3), dataBucket1, 3L);
        indexApplier.applyIndexRecords(records1, 0L, 3L, dataBucket1);

        // Apply records for second data bucket [0, 4) - use DATA1 to match schema
        MemoryLogRecords records2 =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 4), dataBucket2, 4L);
        indexApplier.applyIndexRecords(records2, 0L, 4L, dataBucket2);

        // Update high watermark to cover partially both ranges
        // First batch: index offsets [0, 3), Second batch: index offsets [3, 7)
        long hwPartial = 3L + 1; // Covers first fully (3 records), second partially (1 record)
        updateHighWatermark(hwPartial);

        // First data bucket should be fully committed
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1)).isEqualTo(3L);

        // Second data bucket should be partially committed
        // hwPartial (4) - indexStartOffset of second bucket (3) + dataStartOffset (0) = 1
        long expectedPartial = hwPartial - 3L;
        assertThat(indexApplier.getIndexCommitOffset(dataBucket2)).isEqualTo(expectedPartial);
    }

    @Test
    void testHighWatermarkUpdateWhenClosed() throws Exception {
        // Apply some records first
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 2), dataBucket1, 2L);
        indexApplier.applyIndexRecords(records, 0L, 2L, dataBucket1);
        long commitOffsetBeforeClose = indexApplier.getIndexCommitOffset(dataBucket1);

        // Close the applier
        indexApplier.close();

        // Try to update high watermark after close
        updateHighWatermark(100L);

        // State should remain unchanged
        assertThat(indexApplier.getIndexCommitOffset(dataBucket1))
                .isEqualTo(commitOffsetBeforeClose);
    }

    // ================================================================================================
    // Thread Safety and Concurrency Tests
    // ================================================================================================

    @Test
    void testConcurrentIndexApplication() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(3);

        try {
            // Prepare different data buckets for concurrent application
            List<MemoryLogRecords> recordsList =
                    Arrays.asList(
                            createIndexMemoryLogRecordsWithState(
                                    DATA1.subList(0, 2), dataBucket1, 2L),
                            createIndexMemoryLogRecordsWithState(
                                    DATA1.subList(0, 2), dataBucket2, 2L),
                            createIndexMemoryLogRecordsWithState(
                                    DATA1.subList(0, 2), dataBucket3, 2L));

            List<TableBucket> dataBuckets = Arrays.asList(dataBucket1, dataBucket2, dataBucket3);

            List<Long> appliedOffsets = new ArrayList<>();
            List<Exception> exceptions = new ArrayList<>();

            // Submit concurrent applications for different data buckets
            for (int i = 0; i < 3; i++) {
                final int index = i;
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                long applied =
                                        indexApplier.applyIndexRecords(
                                                recordsList.get(index),
                                                0L,
                                                2L,
                                                dataBuckets.get(index));
                                synchronized (appliedOffsets) {
                                    appliedOffsets.add(applied);
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

            // Start all threads simultaneously
            startLatch.countDown();
            assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // Check that all operations completed successfully
            assertThat(exceptions).isEmpty();
            assertThat(appliedOffsets).hasSize(3);

            // All applications should have succeeded
            for (Long appliedOffset : appliedOffsets) {
                assertThat(appliedOffset).isEqualTo(2L);
            }

            updateHighWatermark(1);

            int numIndexCommitOffsetIncreased = 0;
            for (TableBucket dataBucket : dataBuckets) {
                if (indexApplier.getIndexCommitOffset(dataBucket) > 0) {
                    numIndexCommitOffsetIncreased++;
                }
            }
            assertThat(numIndexCommitOffsetIncreased).isEqualTo(1);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void testConcurrentHighWatermarkUpdates() throws Exception {
        // Apply some initial records
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 5), dataBucket1, 5L);
        LogAppendInfo appendInfo = logTablet.appendAsLeader(records);
        indexApplier.applyIndexRecords(records, 0L, 5L, dataBucket1);

        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(5);

        try {
            // Submit concurrent high watermark updates
            for (int i = 0; i < 5; i++) {
                final long hw = appendInfo.firstOffset() + i + 1;
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                updateHighWatermark(hw);
                            } catch (Exception e) {
                                // Should not happen
                            } finally {
                                doneLatch.countDown();
                            }
                        });
            }

            // Start all threads
            startLatch.countDown();
            assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // Final commit offset should be reasonable (no exceptions)
            long finalCommitOffset = indexApplier.getIndexCommitOffset(dataBucket1);
            assertThat(finalCommitOffset).isBetween(0L, 5L);

        } finally {
            executor.shutdown();
        }
    }

    @Test
    void testConcurrentReadWriteOperations() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(4);

        try {
            List<Exception> exceptions = new ArrayList<>();

            // Writer 1: Apply records for dataBucket1
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            MemoryLogRecords records =
                                    createIndexMemoryLogRecordsWithState(
                                            DATA1.subList(0, 3), dataBucket1, 3L);
                            indexApplier.applyIndexRecords(records, 0L, 3L, dataBucket1);
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });

            // Writer 2: Apply records for dataBucket2
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            MemoryLogRecords records =
                                    createIndexMemoryLogRecordsWithState(
                                            DATA1.subList(3, 6), dataBucket2, 3L);
                            indexApplier.applyIndexRecords(records, 0L, 3L, dataBucket2);
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });

            // Reader 1: Continuously read commit offsets
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < 100; i++) {
                                indexApplier.getIndexCommitOffset(dataBucket1);
                                indexApplier.getIndexCommitOffset(dataBucket2);
                                Thread.sleep(1);
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });

            // High watermark updater
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < 50; i++) {
                                updateHighWatermark(i);
                                Thread.sleep(2);
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                exceptions.add(e);
                            }
                        } finally {
                            doneLatch.countDown();
                        }
                    });

            // Start all operations
            startLatch.countDown();
            assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

            // All operations should complete without exceptions
            assertThat(exceptions).isEmpty();

        } finally {
            executor.shutdown();
        }
    }

    // ================================================================================================
    // Error Handling and Edge Cases
    // ================================================================================================

    @Test
    void testInvalidOffsetRangeValidation() {
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 2), dataBucket1, 2L);

        // Test invalid offset range
        assertThatThrownBy(() -> indexApplier.applyIndexRecords(records, 10L, 5L, dataBucket1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Start offset must be less than end offset");
    }

    @Test
    void testEmptyIndexRecordsApplication() throws Exception {
        // Create empty records with state
        MemoryLogRecords emptyRecords = createEmptyIndexMemoryLogRecordsWithState(dataBucket1, 1L);

        // Apply empty records
        long applied = indexApplier.applyIndexRecords(emptyRecords, 0L, 1L, dataBucket1);

        // Should handle gracefully
        assertThat(applied).isEqualTo(1L);
    }

    @Test
    void testNullParameterHandling() {
        // Test null records
        assertThatThrownBy(() -> indexApplier.applyIndexRecords(null, 0L, 1L, dataBucket1))
                .isInstanceOf(NullPointerException.class);

        // Test null data bucket
        MemoryLogRecords records =
                createIndexMemoryLogRecordsWithState(DATA1.subList(0, 1), dataBucket1, 1L);

        assertThatThrownBy(() -> indexApplier.applyIndexRecords(records, 0L, 1L, null))
                .isInstanceOf(NullPointerException.class);
    }

    // ================================================================================================
    // Data Class Tests
    // ================================================================================================

    @Test
    void testIndexApplyStatusDataClass() {
        IndexApplier.IndexApplyStatus status =
                new IndexApplier.IndexApplyStatus(
                        5L, // lastApplyRecordsDataEndOffset
                        105L, // lastApplyRecordsIndexEndOffset
                        5L // indexCommitDataOffset
                        );

        assertThat(status.getLastApplyRecordsDataEndOffset()).isEqualTo(5L);
        assertThat(status.getLastApplyRecordsIndexEndOffset()).isEqualTo(105L);
        assertThat(status.getIndexCommitDataOffset()).isEqualTo(5L);

        // Test toString
        String statusString = status.toString();
        assertThat(statusString).contains("[,5)");
        assertThat(statusString).contains("[,105)");
        assertThat(statusString).contains("indexCommitDataOffset=5");
    }

    // ================================================================================================
    // Helper Methods
    // ================================================================================================

    private void createLogTablet() throws Exception {
        // Create log directory
        File logDir =
                new File(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName()
                                + "/"
                                + DATA1_TABLE_PATH.getTableName()
                                + "-"
                                + DATA1_TABLE_ID
                                + "/log-0");

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(DATA1_TABLE_PATH);

        logTablet =
                LogTablet.create(
                        physicalTablePath,
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L, // recovery point
                        scheduler,
                        LogFormat.INDEXED, // Use INDEXED for index table
                        1,
                        false,
                        org.apache.fluss.utils.clock.SystemClock.getInstance(),
                        true);
    }

    private void createKvTablet() throws Exception {
        TableBucket tableBucket = logTablet.getTableBucket();
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(DATA1_TABLE_PATH);

        // Create RowMerger
        TableConfig tableConfig = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConfig, indexSchema, KvFormat.COMPACTED);

        kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        kvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        arrowBufferAllocator,
                        memoryPool,
                        KvFormat.COMPACTED,
                        indexSchema,
                        rowMerger,
                        ArrowCompressionInfo.DEFAULT_COMPRESSION,
                        null); // IndexCache is not needed for this test
    }

    /**
     * Creates index MemoryLogRecords with stateChangeLogs for proper state persistence. This
     * simulates the behavior of IndexCache which adds stateChangeLogs when serving fetchIndex.
     *
     * @param data the data rows
     * @param dataBucket the data bucket these index records correspond to
     * @param dataEndOffset the end offset in the data bucket
     */
    private MemoryLogRecords createIndexMemoryLogRecordsWithState(
            List<Object[]> data, TableBucket dataBucket, long dataEndOffset) {
        try {
            // Convert Object[] to IndexedRow for INDEXED log format
            List<IndexedRow> indexedRows = new ArrayList<>();
            for (Object[] row : data) {
                indexedRows.add(DataTestUtils.indexedRow(indexRowType, row));
            }

            return DataTestUtils.genIndexedMemoryLogRecordsWithState(
                    indexedRows, dataBucket, dataEndOffset);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index memory log records with state", e);
        }
    }

    /**
     * Creates empty MemoryLogRecords with only stateChangeLogs for testing empty index segments.
     * This simulates the behavior of IndexCache when there are no index records but state still
     * needs to be tracked.
     *
     * @param dataBucket the data bucket these empty records correspond to
     * @param dataEndOffset the end offset in the data bucket
     */
    private MemoryLogRecords createEmptyIndexMemoryLogRecordsWithState(
            TableBucket dataBucket, long dataEndOffset) {
        try {
            return DataTestUtils.genIndexedMemoryLogRecordsWithState(
                    new ArrayList<>(), dataBucket, dataEndOffset);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create empty index memory log records with state", e);
        }
    }

    private void updateHighWatermark(long highWatermark) {
        logTablet.updateHighWatermark(highWatermark);
        indexApplier.onUpdateHighWatermark(highWatermark);
    }
}
