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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.record.TestData.IDX_EMAIL_TABLE_INFO;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_INFO;
import static org.apache.fluss.record.TestData.INDEXED_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_SCHEMA;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEX_CACHE_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexCache}. */
final class IndexCacheTest {

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int BUCKET_ID = 0;

    @TempDir private File tempDir;

    private TestingMemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private FlussScheduler scheduler;
    private Configuration conf;
    private IndexCache indexCache;
    private TabletServerMetadataCache metadataCache;

    // Test callback for commit horizon updates
    private final AtomicLong lastCommitHorizon = new AtomicLong(-1);
    private final IndexCache.IndexCommitHorizonCallback commitHorizonCallback =
            newHorizon -> lastCommitHorizon.set(newHorizon);

    @BeforeEach
    void setUp() throws Exception {
        // Initialize memory pool
        memoryPool = new TestingMemorySegmentPool(DEFAULT_PAGE_SIZE);

        // Initialize configuration
        conf = new Configuration();

        // Initialize scheduler
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        metadataCache =
                new TestTabletServerMetadataCache(
                        Arrays.asList(IDX_NAME_TABLE_INFO, IDX_EMAIL_TABLE_INFO));
        createLogTablet();

        // Create IndexCache
        createIndexLogCache();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexCache != null) {
            indexCache.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (memoryPool != null) {
            memoryPool.close();
        }
    }

    // ===========================================================================================
    // Test Methods
    // ===========================================================================================

    @Test
    void testFetchEmptyIndexLogData() throws Exception {
        // Test fetching from empty cache
        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();
        TableBucket indexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);

        fetchRequests.put(
                indexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 0L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, false);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(1);

        IndexSegment segment = result.get().get(indexBucket);
        assertThat(segment).isNotNull();
        // For empty segment created with createEmptySegment(0), startOffset will be -1
        assertThat(segment.getStartOffset()).isEqualTo(0);
        assertThat(segment.getEndOffset()).isEqualTo(0L);
    }

    @Test
    void testFetchIndexLogDataHotDataOnly() throws Exception {
        // Test hot data only mode with empty cache should return empty
        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();
        TableBucket indexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);

        fetchRequests.put(
                indexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 0L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, true);

        assertThat(result).isPresent();
        boolean isEmpty = true;
        for (IndexSegment segment : result.get().values()) {
            if (!segment.isEmpty()) {
                isEmpty = false;
            }
        }
        assertThat(isEmpty).isTrue();
    }

    @Test
    void testWriteHotDataFromWAL() throws Exception {
        // Create test WAL data using indexed data
        List<Object[]> testData = INDEX_CACHE_DATA.subList(0, 2);
        MemoryLogRecords walRecords = DataTestUtils.genIndexedMemoryLogRecordsByObject(testData);

        LogAppendInfo appendInfo = createLogAppendInfo(0L, 2L);

        // Write hot data to cache
        indexCache.writeHotDataFromWAL(walRecords, appendInfo);

        // Verify the data was written (this test verifies the method doesn't throw)
        // More detailed verification would require access to internal cache state
    }

    @Test
    void testWriteHotDataFromWALWhenClosed() throws Exception {
        // Close the cache first
        indexCache.close();

        List<Object[]> testData = INDEX_CACHE_DATA.subList(0, 1);
        MemoryLogRecords walRecords = DataTestUtils.genIndexedMemoryLogRecordsByObject(testData);

        LogAppendInfo appendInfo = createLogAppendInfo(0L, 1L);

        // Should not throw exception, but log warning
        indexCache.writeHotDataFromWAL(walRecords, appendInfo);
    }

    @Test
    void testFetchIndexLogDataWhenClosed() throws Exception {
        // Close the cache
        indexCache.close();

        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();
        TableBucket indexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);

        fetchRequests.put(
                indexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 0L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, false);

        // Should return empty when cache is closed
        assertThat(result).isEmpty();
    }

    @Test
    void testMultipleIndexTablesFetch() throws Exception {
        // Test fetching data for multiple index tables
        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();

        // Add fetch request for name index
        TableBucket nameIndexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);
        fetchRequests.put(
                nameIndexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 0L, 0L));

        // Add fetch request for email index
        TableBucket emailIndexBucket =
                new TableBucket(IDX_EMAIL_TABLE_INFO.getTableId(), BUCKET_ID);
        fetchRequests.put(
                emailIndexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_EMAIL_TABLE_INFO.getTableId(), 0L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, false);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(2);

        // Verify both index segments are present
        assertThat(result.get()).containsKey(nameIndexBucket);
        assertThat(result.get()).containsKey(emailIndexBucket);
    }

    @Test
    void testFetchWithStartOffsetBeyondHighWatermark() throws Exception {
        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();
        TableBucket indexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);

        // Request data from offset that is beyond high watermark
        fetchRequests.put(
                indexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 100L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, false);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(1);

        IndexSegment segment = result.get().get(indexBucket);
        assertThat(segment).isNotNull();
        // Should be empty segment when start offset >= high watermark
        // For empty segment, startOffset should be high watermark
    }

    @Test
    void testClose() throws Exception {
        // Verify cache is not closed initially
        assertThat(indexCache).isNotNull();

        // Close the cache
        indexCache.close();

        // Test that operations after close don't crash but may return empty results
        Map<TableBucket, IndexCache.IndexCacheFetchParam> fetchRequests = new HashMap<>();
        TableBucket indexBucket = new TableBucket(IDX_NAME_TABLE_INFO.getTableId(), BUCKET_ID);

        fetchRequests.put(
                indexBucket,
                new IndexCache.IndexCacheFetchParam(IDX_NAME_TABLE_INFO.getTableId(), 0L, 0L));

        Optional<Map<TableBucket, IndexSegment>> result =
                indexCache.fetchIndexLogData(fetchRequests, false);

        assertThat(result).isEmpty();

        // Test multiple close calls don't crash
        indexCache.close();
    }

    @Test
    void testIndexCacheFetchParam() {
        // Test IndexCacheFetchParam data structure
        long tableId = 12345L;
        long fetchOffset = 100L;
        long commitOffset = 99L;

        IndexCache.IndexCacheFetchParam param =
                new IndexCache.IndexCacheFetchParam(tableId, fetchOffset, commitOffset);

        assertThat(param.getIndexTableId()).isEqualTo(tableId);
        assertThat(param.getFetchOffset()).isEqualTo(fetchOffset);
        assertThat(param.getIndexCommitOffset()).isEqualTo(commitOffset);
    }

    // ===========================================================================================
    // Helper Methods
    // ===========================================================================================

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

    private void createIndexLogCache() {
        indexCache =
                new IndexCache(
                        logTablet,
                        memoryPool,
                        INDEXED_SCHEMA,
                        INDEXED_PHYSICAL_TABLE_PATH,
                        metadataCache,
                        commitHorizonCallback);
    }

    private LogAppendInfo createLogAppendInfo(long firstOffset, long lastOffset) {
        return new LogAppendInfo(
                firstOffset,
                lastOffset,
                System.currentTimeMillis(), // maxTimestamp
                firstOffset, // offsetOfMaxTimestamp
                (int) (lastOffset - firstOffset + 1), // shallowCount
                100, // validBytes
                true); // offsetsMonotonic
    }

    // ===========================================================================================
    // Test IndexMetadataManager Implementation
    // ===========================================================================================

    /** Test implementation of TabletServerMetadataCache that provides index table metadata. */
    private static class TestTabletServerMetadataCache extends TabletServerMetadataCache {
        private final Map<TablePath, List<TableInfo>> indexTablesMap;

        public TestTabletServerMetadataCache(List<TableInfo> indexTables) {
            super(null); // null dependencies for testing
            this.indexTablesMap = new HashMap<>();
            this.indexTablesMap.put(INDEXED_TABLE_PATH, new ArrayList<>(indexTables));
        }

        @Override
        public List<TableInfo> getRelatedIndexTables(TablePath dataTablePath) {
            return indexTablesMap.getOrDefault(dataTablePath, new ArrayList<>());
        }
    }
}
