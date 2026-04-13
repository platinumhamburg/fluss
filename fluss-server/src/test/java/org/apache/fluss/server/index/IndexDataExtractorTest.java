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
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.server.index.IndexTestUtils.INDEX_BUCKET_COUNT;
import static org.apache.fluss.server.index.IndexTestUtils.INDEX_TABLE_ID;
import static org.apache.fluss.server.index.IndexTestUtils.PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IndexDataExtractor}, focusing on schema evolution support.
 *
 * <p>Verifies that {@code distributeRecordsToCache()} dynamically resolves the correct schema via
 * {@code schemaGetter.getSchema(schemaId)} for each WAL batch, so that ADD COLUMN operations do not
 * break index extraction.
 */
class IndexDataExtractorTest {

    @TempDir private File tempDir;

    private TestingMemorySegmentPool memoryPool;
    private LogTablet logTablet;
    private FlussScheduler scheduler;
    private IndexBucketCacheManager cacheManager;
    private IndexDataExtractor extractor;
    private TestingSchemaGetter schemaGetter;

    // Data table schema v1: id (INT, PK), name (STRING), email (STRING, index column)
    private Schema dataSchemaV1;
    // Data table schema v2: id (INT, PK), name (STRING), email (STRING), age (INT) -- ADD COLUMN
    private Schema dataSchemaV2;
    // Index schema: email (STRING), id (INT) -- derived from data table
    private Schema indexSchema;

    private static final short SCHEMA_ID_V1 = 1;
    private static final short SCHEMA_ID_V2 = 2;

    @BeforeEach
    void setUp() throws Exception {
        memoryPool = new TestingMemorySegmentPool(PAGE_SIZE);
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        // Data table schema v1
        dataSchemaV1 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING().copy(false))
                        .primaryKey("id")
                        .build();

        // Data table schema v2 (ADD COLUMN age)
        dataSchemaV2 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING().copy(false))
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        // Index schema: email + id (index columns + PK)
        indexSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING().copy(false))
                        .column("id", DataTypes.INT().copy(false))
                        .primaryKey("email", "id")
                        .build();

        // Create LogTablet (ARROW format for data table WAL)
        logTablet = IndexTestUtils.createLogTablet(tempDir, scheduler, LogFormat.ARROW);

        // Create SchemaGetter with v1 initially
        schemaGetter = new TestingSchemaGetter(SCHEMA_ID_V1, dataSchemaV1);

        // Create IndexBucketCacheManager
        Map<Long, Integer> indexBucketDistribution = new HashMap<>();
        indexBucketDistribution.put(INDEX_TABLE_ID, INDEX_BUCKET_COUNT);
        cacheManager = new IndexBucketCacheManager(memoryPool, logTablet, indexBucketDistribution);

        // Create IndexDataExtractor
        TableInfo indexTableInfo = createIndexTableInfo();
        extractor =
                new IndexDataExtractor(
                        logTablet,
                        cacheManager,
                        BucketingFunction.of(null),
                        schemaGetter,
                        Collections.singletonList(indexTableInfo),
                        null);
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

    /** Tests hot data extraction handles schema evolution (ADD COLUMN) correctly. */
    @Test
    void testSchemaEvolutionWithAddColumn() throws Exception {
        RowType rowTypeV1 = dataSchemaV1.getRowType();

        // Step 1: Write and process WAL records with schema v1
        List<Object[]> v1Data =
                Arrays.asList(
                        new Object[] {1, "Alice", "alice@example.com"},
                        new Object[] {2, "Bob", "bob@example.com"});

        MemoryLogRecords walRecordsV1 =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        rowTypeV1,
                        SCHEMA_ID_V1,
                        0,
                        System.currentTimeMillis(),
                        CURRENT_LOG_MAGIC_VALUE,
                        v1Data,
                        LogFormat.ARROW);

        // Append to LogTablet and extract index data
        LogAppendInfo appendInfoV1 = logTablet.appendAsLeader(walRecordsV1);
        extractor.cacheIndexDataByHotData(walRecordsV1, appendInfoV1);

        // Verify index data was extracted (cache has entries for 2 records)
        int entriesAfterV1 = cacheManager.getTotalEntries();
        assertThat(entriesAfterV1).isGreaterThanOrEqualTo(2);

        // Step 2: Evolve schema - register v2 in SchemaGetter
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(dataSchemaV2, SCHEMA_ID_V2));

        // Step 3: Write and process WAL records with schema v2 (4 columns)
        RowType rowTypeV2 = dataSchemaV2.getRowType();
        List<Object[]> v2Data =
                Arrays.asList(
                        new Object[] {3, "Charlie", "charlie@example.com", 30},
                        new Object[] {4, "David", "david@example.com", 25});

        MemoryLogRecords walRecordsV2 =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        rowTypeV2,
                        SCHEMA_ID_V2,
                        0,
                        System.currentTimeMillis(),
                        CURRENT_LOG_MAGIC_VALUE,
                        v2Data,
                        LogFormat.ARROW);

        // Append to LogTablet and extract index data
        LogAppendInfo appendInfoV2 = logTablet.appendAsLeader(walRecordsV2);
        extractor.cacheIndexDataByHotData(walRecordsV2, appendInfoV2);

        // Verify more index data was extracted
        int entriesAfterV2 = cacheManager.getTotalEntries();
        assertThat(entriesAfterV2).isGreaterThan(entriesAfterV1);
    }

    /** Tests cold data loading works correctly with mixed schema versions in WAL. */
    @Test
    void testSchemaEvolutionWithColdDataLoading() throws Exception {
        RowType rowTypeV1 = dataSchemaV1.getRowType();

        // Step 1: Write WAL records with schema v1
        List<Object[]> v1Data =
                Arrays.asList(
                        new Object[] {1, "Alice", "alice@example.com"},
                        new Object[] {2, "Bob", "bob@example.com"});

        MemoryLogRecords walRecordsV1 =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        rowTypeV1,
                        SCHEMA_ID_V1,
                        0,
                        System.currentTimeMillis(),
                        CURRENT_LOG_MAGIC_VALUE,
                        v1Data,
                        LogFormat.ARROW);
        logTablet.appendAsLeader(walRecordsV1);

        // Step 2: Evolve schema and write WAL records with schema v2
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(dataSchemaV2, SCHEMA_ID_V2));

        RowType rowTypeV2 = dataSchemaV2.getRowType();
        List<Object[]> v2Data =
                Arrays.asList(
                        new Object[] {3, "Charlie", "charlie@example.com", 30},
                        new Object[] {4, "David", "david@example.com", 25});

        MemoryLogRecords walRecordsV2 =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        rowTypeV2,
                        SCHEMA_ID_V2,
                        0,
                        System.currentTimeMillis(),
                        CURRENT_LOG_MAGIC_VALUE,
                        v2Data,
                        LogFormat.ARROW);
        logTablet.appendAsLeader(walRecordsV2);

        // Step 3: Load all data as cold data -- reads from LogTablet which contains
        // both v1 and v2 batches. distributeRecordsToCache() must resolve schema
        // dynamically for each batch.
        long endOffset = logTablet.localLogEndOffset();
        extractor.loadColdDataToCache(0L, endOffset);

        // Verify index data was extracted for all 4 records (2 v1 + 2 v2)
        assertThat(cacheManager.getTotalEntries()).isGreaterThanOrEqualTo(4);
    }

    // Helper Methods
    private TableInfo createIndexTableInfo() {
        return new TableInfo(
                TablePath.of("test_db", "test_table_idx_email"),
                INDEX_TABLE_ID,
                SCHEMA_ID_V1,
                indexSchema,
                Arrays.asList("email", "id"), // bucket keys
                Collections.emptyList(), // partition keys
                INDEX_BUCKET_COUNT,
                new Configuration(),
                new Configuration(),
                "/tmp/fluss/remote-data", // remoteDataDir
                null, // comment
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
