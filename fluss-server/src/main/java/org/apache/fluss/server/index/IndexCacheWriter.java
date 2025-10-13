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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartitionId;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * IndexCacheWriter is a unified utility class for writing index data to IndexRowCache from both hot
 * data (real-time WAL records) and cold data (on-demand WAL loading).
 *
 * <p>Core Design: Provides a unified processing pipeline that extracts index data from WAL records
 * and writes them directly to IndexBucketRowCache with logOffset information. The class handles:
 *
 * <ul>
 *   <li>Hot data processing: Real-time index data writing from KvTablet WAL records
 *   <li>Cold data loading: On-demand loading from LogTablet when cache misses occur
 *   <li>Duplicate handling: IndexBucketRowCache automatically ignores duplicates by logOffset
 *   <li>IndexedRow creation: Efficient IndexedRow generation from data rows and index definitions
 * </ul>
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Unified hot/cold processing pipeline using distributeRecordsToCache()
 *   <li>Direct IndexRowCache writing instead of intermediate IndexSegment construction
 *   <li>Efficient bucketing and key encoding for proper index distribution
 *   <li>Thread-safe for read operations, requires external synchronization for writes
 * </ul>
 *
 * <p>This class replaces the IndexLogBuildHelper's complex dual-mode design with a simpler,
 * cache-focused approach that eliminates redundant IndexSegment construction.
 */
@Internal
public final class IndexCacheWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCacheWriter.class);

    private final LogTablet logTablet;
    private final Schema tableSchema;

    private volatile boolean closed = false;

    private List<CacheWriterForIndexTable> cacheWriterForIndexTableTables;

    public IndexCacheWriter(
            LogTablet logTablet,
            IndexRowCache indexRowCache,
            BucketingFunction bucketingFunction,
            Schema tableSchema,
            List<TableInfo> indexTableInfos) {
        this.logTablet = logTablet;
        this.tableSchema = tableSchema;

        this.cacheWriterForIndexTableTables =
                indexTableInfos.stream()
                        .map(
                                indexTableInfo ->
                                        new CacheWriterForIndexTable(
                                                TablePartitionId.of(
                                                        indexTableInfo.getTableId(), null),
                                                tableSchema,
                                                indexTableInfo.getSchema(),
                                                indexTableInfo.getNumBuckets(),
                                                indexTableInfo.getBucketKeys(),
                                                bucketingFunction,
                                                indexRowCache))
                        .collect(Collectors.toList());

        LOG.info(
                "IndexCacheWriter initialized for table bucket {} with IndexRowCache",
                logTablet.getTableBucket());
    }

    /**
     * Write hot data from WAL records directly to IndexRowCache. This method processes WAL records
     * generated during KV operations and immediately writes the corresponding index data to cache
     * for real-time availability.
     *
     * @param walRecords the WAL LogRecords from KV processing
     * @param appendInfo the log append information containing offset details
     * @throws Exception if an error occurs during hot data processing
     */
    public void writeHotDataFromWAL(LogRecords walRecords, LogAppendInfo appendInfo)
            throws Exception {

        if (closed) {
            LOG.warn(
                    "IndexCacheWriter is closed, cannot write hot data, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        if (walRecords == null || appendInfo == null) {
            LOG.debug(
                    "Invalid parameters for hot data writing, skipping, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        if (!appendInfo.offsetsMonotonic() || appendInfo.duplicated()) {
            LOG.error(
                    "Invalid appendInfo for hot data writing, skipping, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        LOG.debug(
                "Writing hot data from WAL, base offset: {}, tableBucket {}",
                appendInfo.firstOffset(),
                logTablet.getTableBucket());

        // Use unified distribution pipeline to write to cache
        distributeRecordsToCache(walRecords, appendInfo.firstOffset());

        LOG.debug(
                "Successfully wrote hot data to cache for table bucket {}",
                logTablet.getTableBucket());
    }

    /**
     * Batch loads cold data to cache with optimized single WAL read and conditional writing.
     *
     * <p>This method implements the core optimization for multi-IndexBucket cold data loading:
     *
     * <ul>
     *   <li>Reads WAL once for the global range across all IndexBuckets
     *   <li>For each logOffset, checks OffsetRangeInfos to determine which IndexBuckets need it
     *   <li>Performs data conversion and writing only for IndexBuckets that declared they need the
     *       data
     *   <li>Prevents redundant WAL reads and unnecessary data processing
     * </ul>
     *
     * @throws Exception if an error occurs during batch cold data loading
     */
    public void loadColdDataToCache(long globalStartOffset, long globalEndOffset) throws Exception {

        if (closed) {
            LOG.warn(
                    "IndexCacheWriter is closed, cannot batch load cold data, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        if (globalStartOffset >= globalEndOffset) {
            LOG.debug(
                    "No data to batch load, start offset {} >= end offset {}, tableBucket {}",
                    globalStartOffset,
                    globalEndOffset,
                    logTablet.getTableBucket());
            return;
        }

        LOG.debug(
                "Batch loading cold data from global WAL range [{}, {}), tableBucket {}",
                globalStartOffset,
                globalEndOffset,
                logTablet.getTableBucket());

        // Read WAL data and distribute with conditional writing
        long currentOffset = globalStartOffset;
        while (currentOffset < globalEndOffset) {
            // Use fixed fetch size since maxFetchBytes parameter is removed
            int maxFetchBytes = 64 * 1024 * 1024;

            FetchDataInfo fetchResult =
                    logTablet.read(
                            currentOffset,
                            maxFetchBytes,
                            FetchIsolation.HIGH_WATERMARK,
                            true,
                            null);

            if (fetchResult.getRecords() == null
                    || fetchResult.getRecords() == MemoryLogRecords.EMPTY) {
                break;
            }

            LogRecords walRecords = fetchResult.getRecords();
            long nextOffset =
                    distributeRecordsToCache(
                            walRecords, fetchResult.getFetchOffsetMetadata().getMessageOffset());

            if (nextOffset <= currentOffset) {
                break;
            }
            currentOffset = nextOffset;
        }

        LOG.debug(
                "Successfully batch loaded cold data from global WAL range [{}, {}), tableBucket {}",
                globalStartOffset,
                globalEndOffset,
                logTablet.getTableBucket());
    }

    /**
     * Unified record distribution pipeline that processes WAL records and writes IndexedRow data
     * directly to IndexRowCache. This method handles both hot and cold data processing with
     * optional conditional writing support.
     *
     * @param walRecords the WAL records to process
     * @return the maximum processed offset
     * @throws Exception if an error occurs during processing
     */
    private long distributeRecordsToCache(LogRecords walRecords, long firstOffset)
            throws Exception {
        long currentOffset = firstOffset;
        for (LogRecordBatch batch : walRecords.batches()) {
            short schemaId = batch.schemaId();

            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    tableSchema.getRowType(), schemaId);
                    CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord walRecord = recordIterator.next();
                    for (CacheWriterForIndexTable indexWriter : cacheWriterForIndexTableTables) {
                        indexWriter.writeRecord(walRecord, currentOffset);
                    }
                    currentOffset++;
                }
            }
        }
        return currentOffset;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        LOG.info("IndexCacheWriter closed");
    }
}
