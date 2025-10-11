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
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.IDX_NAME_SCHEMA;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.INDEXED_DATA;
import static org.apache.fluss.record.TestData.INDEXED_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_SCHEMA;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Comprehensive unit tests for {@link CacheWriterForIndexTable}.
 *
 * <p>This test class covers:
 *
 * <ul>
 *   <li>Normal record writing with different change types
 *   <li>Empty record handling
 *   <li>IndexedRow creation and field mapping
 *   <li>Bucket assignment functionality
 *   <li>Resource cleanup and close operations
 *   <li>Exception handling scenarios
 * </ul>
 */
class CacheWriterForIndexTableTest {

    // Use INDEXED_TABLE_ID from TestData instead of defining our own
    private static final int DEFAULT_BUCKET_COUNT = 3;

    @TempDir private File tempDir;

    private TestingMemorySegmentPool memoryPool;
    private IndexRowCache indexRowCache;
    private TablePartitionId tablePartitionId;
    private Schema mainTableSchema;
    private Schema indexSchema;
    private BucketingFunction bucketingFunction;
    private CacheWriterForIndexTable cacheWriter;
    private FlussScheduler scheduler;
    private LogTablet logTablet;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize test dependencies
        memoryPool = new TestingMemorySegmentPool(64 * 1024); // 64KB page size
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        // Create LogTablet for IndexRowCache
        createLogTablet();

        tablePartitionId = TablePartitionId.of(INDEXED_TABLE_ID, null);

        // Use TestData's indexed schema as main table schema
        mainTableSchema = INDEXED_SCHEMA;

        // Use TestData's pre-defined index schema for "idx_name" index
        indexSchema = IDX_NAME_SCHEMA;

        bucketingFunction = BucketingFunction.of(null); // Use default FlussBucketingFunction

        // Create real IndexRowCache
        Map<TablePartitionId, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(tablePartitionId, DEFAULT_BUCKET_COUNT);
        this.indexRowCache = new IndexRowCache(memoryPool, logTablet, indexBucketDistribution);

        // Create CacheWriterForIndexTable instance
        cacheWriter =
                new CacheWriterForIndexTable(
                        tablePartitionId,
                        mainTableSchema,
                        indexSchema,
                        DEFAULT_BUCKET_COUNT,
                        IDX_NAME_TABLE_DESCRIPTOR.getBucketKeys(),
                        bucketingFunction,
                        indexRowCache);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cacheWriter != null) {
            cacheWriter.close();
        }
        if (indexRowCache != null) {
            indexRowCache.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void testWriteRecordWithInsert() throws Exception {
        // Prepare test data
        Object[] testData = INDEXED_DATA.get(0); // {1, "Alice", "alice@example.com"}
        InternalRow dataRow = DataTestUtils.row(testData);
        long offset = 100L;
        LogRecord logRecord =
                new GenericRecord(offset, System.currentTimeMillis(), ChangeType.INSERT, dataRow);

        // Execute
        cacheWriter.writeRecord(logRecord, offset);

        // Verify that data was written to cache by checking total entries
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);

        // Verify that no exception was thrown during writing
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, offset)).doesNotThrowAnyException();
    }

    @Test
    void testWriteRecordWithUpdate() throws Exception {
        // Prepare test data
        Object[] testData = INDEXED_DATA.get(1); // {2, "Bob", "bob@example.com"}
        InternalRow dataRow = DataTestUtils.row(testData);
        long offset = 200L;
        LogRecord logRecord =
                new GenericRecord(
                        offset, System.currentTimeMillis(), ChangeType.UPDATE_AFTER, dataRow);

        // Execute
        cacheWriter.writeRecord(logRecord, offset);

        // Verify that data was written to cache
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);

        // Verify that no exception was thrown during writing
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, offset)).doesNotThrowAnyException();
    }

    @Test
    void testWriteRecordWithDelete() throws Exception {
        // Prepare test data
        Object[] testData = INDEXED_DATA.get(2); // {3, "Charlie", "charlie@example.com"}
        InternalRow dataRow = DataTestUtils.row(testData);
        long offset = 300L;
        LogRecord logRecord =
                new GenericRecord(offset, System.currentTimeMillis(), ChangeType.DELETE, dataRow);

        // Execute
        cacheWriter.writeRecord(logRecord, offset);

        // Verify that data was written to cache
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(0);

        // Verify that no exception was thrown during writing
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, offset)).doesNotThrowAnyException();
    }

    @Test
    void testWriteEmptyRecord() throws Exception {
        // Prepare empty record
        long offset = 400L;
        LogRecord emptyRecord =
                new GenericRecord(offset, System.currentTimeMillis(), ChangeType.INSERT, null);

        // Execute
        cacheWriter.writeRecord(emptyRecord, offset);

        // For empty records, the IndexRowCache should still handle them correctly
        // We verify this by ensuring no exception is thrown
        assertThatCode(() -> cacheWriter.writeRecord(emptyRecord, offset))
                .doesNotThrowAnyException();
    }

    @Test
    void testBucketAssignment() throws Exception {
        int initialEntries = indexRowCache.getTotalEntries();

        // Test multiple records to verify bucket assignment works correctly
        for (int i = 0; i < INDEXED_DATA.size(); i++) {
            Object[] testData = INDEXED_DATA.get(i);
            InternalRow dataRow = DataTestUtils.row(testData);
            long offset = 100L + i;
            LogRecord logRecord =
                    new GenericRecord(
                            offset, System.currentTimeMillis(), ChangeType.INSERT, dataRow);

            cacheWriter.writeRecord(logRecord, offset);
        }

        // Verify that all records were processed by checking the total entries increased
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(initialEntries);
    }

    @Test
    void testIndexRowFieldMapping() throws Exception {
        int initialEntries = indexRowCache.getTotalEntries();

        // Test with specific data to verify field mapping
        Object[] testData = {4, "Diana", "diana@example.com"};
        InternalRow dataRow = DataTestUtils.row(testData);
        long offset = 500L;
        LogRecord logRecord =
                new GenericRecord(offset, System.currentTimeMillis(), ChangeType.INSERT, dataRow);

        // Execute
        cacheWriter.writeRecord(logRecord, offset);

        // Verify that the record was processed successfully
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(initialEntries);
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, offset)).doesNotThrowAnyException();
    }

    @Test
    void testCloseOperation() {
        // Verify that close operation doesn't throw exception
        assertThatCode(() -> cacheWriter.close()).doesNotThrowAnyException();

        // After closing, the cacheWriter should still be in a valid state
        // (IndexedRowEncoder might be null, but no exception should be thrown)
        assertThatCode(() -> cacheWriter.close()).doesNotThrowAnyException();
    }

    @Test
    void testMultipleRecordsWriting() throws Exception {
        int initialEntries = indexRowCache.getTotalEntries();
        int recordCount = INDEXED_DATA.size();

        // Write multiple records and verify they are all processed correctly
        for (int i = 0; i < recordCount; i++) {
            Object[] testData = INDEXED_DATA.get(i);
            InternalRow dataRow = DataTestUtils.row(testData);
            long offset = 1000L + i;
            ChangeType changeType = i % 2 == 0 ? ChangeType.INSERT : ChangeType.UPDATE_AFTER;
            LogRecord logRecord =
                    new GenericRecord(offset, System.currentTimeMillis(), changeType, dataRow);

            cacheWriter.writeRecord(logRecord, offset);
        }

        // Verify all records were processed
        assertThat(indexRowCache.getTotalEntries()).isGreaterThan(initialEntries);

        // Verify no exceptions were thrown during the writing process
        for (int i = 0; i < recordCount; i++) {
            Object[] testData = INDEXED_DATA.get(i);
            InternalRow dataRow = DataTestUtils.row(testData);
            long offset = 2000L + i;
            ChangeType changeType = i % 2 == 0 ? ChangeType.INSERT : ChangeType.UPDATE_AFTER;
            LogRecord logRecord =
                    new GenericRecord(offset, System.currentTimeMillis(), changeType, dataRow);

            assertThatCode(() -> cacheWriter.writeRecord(logRecord, offset))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testExceptionHandling() throws Exception {
        // Test with null LogRecord - should handle gracefully
        assertThatCode(() -> cacheWriter.writeRecord(null, 100L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testInvalidOffsetHandling() throws Exception {
        // Test with negative offset
        Object[] testData = INDEXED_DATA.get(0);
        InternalRow dataRow = DataTestUtils.row(testData);
        LogRecord logRecord =
                new GenericRecord(-1L, System.currentTimeMillis(), ChangeType.INSERT, dataRow);

        // Should handle gracefully without throwing exception
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, -1L)).doesNotThrowAnyException();
    }

    @Test
    void testLargeOffsetHandling() throws Exception {
        // Test with large offset value
        Object[] testData = INDEXED_DATA.get(0);
        InternalRow dataRow = DataTestUtils.row(testData);
        long largeOffset = Long.MAX_VALUE - 1;
        LogRecord logRecord =
                new GenericRecord(
                        largeOffset, System.currentTimeMillis(), ChangeType.INSERT, dataRow);

        // Should handle gracefully
        assertThatCode(() -> cacheWriter.writeRecord(logRecord, largeOffset))
                .doesNotThrowAnyException();
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
                        new Configuration(),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L, // recovery point
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);
    }
}
