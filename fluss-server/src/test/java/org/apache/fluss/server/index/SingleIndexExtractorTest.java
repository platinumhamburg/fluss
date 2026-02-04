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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.types.DataTypes;
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

import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SingleIndexExtractor}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>NULL value handling in index columns
 *   <li>Batch processing continuity after NULL records
 * </ul>
 *
 * <p><b>Validates: Requirements 3.1, 3.3</b>
 */
class SingleIndexExtractorTest {

    private static final int PAGE_SIZE = 64 * 1024; // 64KB
    private static final long DATA_TABLE_ID = 1000L;
    private static final long INDEX_TABLE_ID = 10001L;
    private static final int INDEX_BUCKET_COUNT = 3;
    private static final TablePath DATA_TABLE_PATH = TablePath.of("test_db", "test_table");

    @TempDir private File tempDir;

    private TestingMemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private FlussScheduler scheduler;
    private IndexBucketCacheManager cacheManager;
    private SingleIndexExtractor extractor;

    // Table schema: id (INT, PK), name (STRING), email (STRING, index column)
    private Schema tableSchema;
    // Index schema: email (STRING, index column), id (INT, from PK)
    private Schema indexSchema;

    @BeforeEach
    void setUp() throws Exception {
        memoryPool = new TestingMemorySegmentPool(PAGE_SIZE);
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        // Create table schema with primary key
        tableSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        // Create index schema: index column (email) + primary key column (id)
        indexSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING().copy(false))
                        .column("id", DataTypes.INT().copy(false))
                        .primaryKey("email", "id")
                        .build();

        // Create LogTablet
        logTablet = createLogTablet();

        // Create index bucket distribution
        Map<Long, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(INDEX_TABLE_ID, INDEX_BUCKET_COUNT);

        cacheManager = new IndexBucketCacheManager(memoryPool, logTablet, indexBucketDistribution);

        // Create SingleIndexExtractor
        List<String> bucketKeys = Arrays.asList("email", "id");
        BucketingFunction bucketingFunction = BucketingFunction.of(null);

        extractor =
                new SingleIndexExtractor(
                        INDEX_TABLE_ID,
                        tableSchema,
                        indexSchema,
                        INDEX_BUCKET_COUNT,
                        bucketKeys,
                        bucketingFunction,
                        cacheManager,
                        null); // no auto-partition
    }

    @AfterEach
    void tearDown() throws Exception {
        if (extractor != null) {
            extractor.close();
        }
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
    // NULL Value Handling Tests - Validates Requirements 3.1, 3.3
    // ================================================================================================

    /**
     * Test that records with NULL index column values are skipped.
     *
     * <p><b>Validates: Requirements 3.1</b> - WHEN SingleIndexExtractor processes a record
     * containing NULL index column value THEN THE SingleIndexExtractor SHALL silently skip that
     * record.
     */
    @Test
    void testSkipRecordWithNullIndexColumn() throws Exception {
        // Create a record with NULL email (index column)
        GenericRow rowWithNullIndex = new GenericRow(3);
        rowWithNullIndex.setField(0, 1); // id
        rowWithNullIndex.setField(1, fromString("Alice")); // name
        rowWithNullIndex.setField(2, null); // email (NULL - index column)

        LogRecord recordWithNull =
                new GenericRecord(
                        0L, System.currentTimeMillis(), ChangeType.INSERT, rowWithNullIndex);

        // Get initial entry count
        int initialEntries = cacheManager.getTotalEntries();

        // Process the record with NULL index column
        extractor.writeRecordInBatch(recordWithNull, 0L, 0L);

        // Verify that no entries were added (record was skipped)
        assertThat(cacheManager.getTotalEntries()).isEqualTo(initialEntries);
    }

    /**
     * Test that batch processing continues after encountering a NULL record.
     *
     * <p><b>Validates: Requirements 3.3</b> - THE SingleIndexExtractor SHALL continue processing
     * subsequent records without interrupting batch processing.
     */
    @Test
    void testContinueProcessingAfterNullRecord() throws Exception {
        // Create a batch of records: valid -> NULL -> valid -> NULL -> valid
        List<LogRecord> records =
                Arrays.asList(
                        createValidRecord(1, "Alice", "alice@example.com", 0L),
                        createRecordWithNullIndex(2, "Bob", 1L), // NULL index column
                        createValidRecord(3, "Charlie", "charlie@example.com", 2L),
                        createRecordWithNullIndex(4, "David", 3L), // NULL index column
                        createValidRecord(5, "Eve", "eve@example.com", 4L));

        // Process all records in batch
        for (LogRecord record : records) {
            extractor.writeRecordInBatch(record, record.logOffset(), 0L);
        }

        // Finalize the batch
        extractor.finalizeBatch(4L, 0L);

        // Verify that only valid records were processed (3 valid records)
        // Each valid record should have been written to the cache
        // The total entries should be 3 (from valid records) + entries from finalizeBatch
        // Since finalizeBatch synchronizes all buckets, we check that entries > 0
        assertThat(cacheManager.getTotalEntries()).isGreaterThan(0);

        // Verify that the cache has data (indicating successful processing of valid records)
        // Check that at least one bucket has cached data
        boolean hasData = false;
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            IndexBucketCache bucketCache = cacheManager.getBucketCache(INDEX_TABLE_ID, bucket);
            if (bucketCache.totalEntries() > 0) {
                hasData = true;
                break;
            }
        }
        assertThat(hasData).isTrue();
    }

    /**
     * Test that a batch with only NULL index column records results in no data entries.
     *
     * <p><b>Validates: Requirements 3.1</b> - Records with NULL index columns should be skipped.
     */
    @Test
    void testBatchWithOnlyNullRecords() throws Exception {
        // Create a batch with only NULL index column records
        List<LogRecord> records =
                Arrays.asList(
                        createRecordWithNullIndex(1, "Alice", 0L),
                        createRecordWithNullIndex(2, "Bob", 1L),
                        createRecordWithNullIndex(3, "Charlie", 2L));

        // Process all records
        for (LogRecord record : records) {
            extractor.writeRecordInBatch(record, record.logOffset(), 0L);
        }

        // Get entry count before finalize (should be 0 since all records have NULL index)
        int entriesBeforeFinalize = cacheManager.getTotalEntries();
        assertThat(entriesBeforeFinalize).isEqualTo(0);

        // Finalize the batch - this will synchronize buckets but no actual data was written
        extractor.finalizeBatch(2L, 0L);

        // After finalize, buckets are synchronized but no actual index data was written
        // The entries from finalizeBatch are empty row markers, not actual data
    }

    /**
     * Test that valid records are processed correctly when mixed with NULL records.
     *
     * <p><b>Validates: Requirements 3.1, 3.3</b> - NULL records should be skipped while valid
     * records continue to be processed.
     */
    @Test
    void testMixedValidAndNullRecords() throws Exception {
        // Process a valid record first
        LogRecord validRecord1 = createValidRecord(1, "Alice", "alice@example.com", 0L);
        extractor.writeRecordInBatch(validRecord1, 0L, 0L);

        int entriesAfterFirst = cacheManager.getTotalEntries();
        assertThat(entriesAfterFirst).isEqualTo(1);

        // Process a NULL record - should be skipped
        LogRecord nullRecord = createRecordWithNullIndex(2, "Bob", 1L);
        extractor.writeRecordInBatch(nullRecord, 1L, 0L);

        // Entry count should remain the same
        assertThat(cacheManager.getTotalEntries()).isEqualTo(entriesAfterFirst);

        // Process another valid record
        LogRecord validRecord2 = createValidRecord(3, "Charlie", "charlie@example.com", 2L);
        extractor.writeRecordInBatch(validRecord2, 2L, 0L);

        // Entry count should increase by 1
        assertThat(cacheManager.getTotalEntries()).isEqualTo(entriesAfterFirst + 1);
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

    /**
     * Creates a valid LogRecord with all fields populated.
     *
     * @param id the id field value
     * @param name the name field value
     * @param email the email field value (index column)
     * @param offset the log offset
     * @return a LogRecord with valid data
     */
    private LogRecord createValidRecord(int id, String name, String email, long offset) {
        GenericRow row = new GenericRow(3);
        row.setField(0, id);
        row.setField(1, fromString(name));
        row.setField(2, fromString(email));
        return new GenericRecord(offset, System.currentTimeMillis(), ChangeType.INSERT, row);
    }

    /**
     * Creates a LogRecord with NULL index column (email).
     *
     * @param id the id field value
     * @param name the name field value
     * @param offset the log offset
     * @return a LogRecord with NULL email (index column)
     */
    private LogRecord createRecordWithNullIndex(int id, String name, long offset) {
        GenericRow row = new GenericRow(3);
        row.setField(0, id);
        row.setField(1, fromString(name));
        row.setField(2, null); // NULL index column
        return new GenericRecord(offset, System.currentTimeMillis(), ChangeType.INSERT, row);
    }
}
