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
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

    private final List<TableCacheWriter> tableTableCacheWriters;

    public IndexCacheWriter(
            LogTablet logTablet,
            IndexRowCache indexRowCache,
            BucketingFunction bucketingFunction,
            Schema tableSchema,
            List<TableInfo> indexTableInfos,
            @Nullable TableInfo mainTableInfo) {
        this.logTablet = logTablet;
        this.tableSchema = tableSchema;

        // Get auto-partition strategy from main table if available and enabled
        AutoPartitionStrategy autoPartitionStrategy =
                (mainTableInfo != null
                                && mainTableInfo.getAutoPartitionStrategy() != null
                                && mainTableInfo
                                        .getAutoPartitionStrategy()
                                        .isAutoPartitionEnabled())
                        ? mainTableInfo.getAutoPartitionStrategy()
                        : null;

        this.tableTableCacheWriters =
                indexTableInfos.stream()
                        .map(
                                indexTableInfo ->
                                        new TableCacheWriter(
                                                indexTableInfo.getTableId(),
                                                tableSchema,
                                                indexTableInfo.getSchema(),
                                                indexTableInfo.getNumBuckets(),
                                                indexTableInfo.getBucketKeys(),
                                                bucketingFunction,
                                                indexRowCache,
                                                autoPartitionStrategy))
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
    public void cacheIndexDataByHotData(LogRecords walRecords, LogAppendInfo appendInfo)
            throws Exception {

        if (closed) {
            LOG.warn(
                    "IndexCacheWriter is closed, cannot write hot data, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        if (walRecords == null || appendInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Invalid parameters for hot data writing, skipping, tableBucket {}",
                        logTablet.getTableBucket());
            }
            return;
        }

        // Note: We trust KvTablet's validation logic. If KvTablet allows writing (not duplicated),
        // we should also write to IndexCache, regardless of offsetsMonotonic status.
        // Extra validation here could cause inconsistency between log and index cache.

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Writing hot data from WAL, base offset: {}, tableBucket {}",
                    appendInfo.firstOffset(),
                    logTablet.getTableBucket());
        }

        // Use unified distribution pipeline to write to cache
        distributeRecordsToCache(walRecords, appendInfo.firstOffset());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Successfully wrote hot data to cache for table bucket {}",
                    logTablet.getTableBucket());
        }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "No data to batch load, start offset {} >= end offset {}, tableBucket {}",
                        globalStartOffset,
                        globalEndOffset,
                        logTablet.getTableBucket());
            }
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Batch loading cold data from global WAL range [{}, {}), tableBucket {}",
                    globalStartOffset,
                    globalEndOffset,
                    logTablet.getTableBucket());
        }

        // Read WAL data and distribute with conditional writing
        long currentOffset = globalStartOffset;
        while (currentOffset < globalEndOffset) {
            // Use fixed fetch size since maxFetchBytes parameter is removed
            int maxFetchBytes = 64 * 1024 * 1024;

            // Use LOG_END isolation for cold data loading because:
            // 1. This is leader loading historical data locally, not serving client reads
            // 2. HIGH_WATERMARK would be limited by indexCommitHorizon which could be 0
            //    during failover recovery, preventing any data from being loaded
            // 3. The data range [globalStartOffset, globalEndOffset) is already validated
            //    to be within logEndOffsetOnLeaderStart, ensuring we only load historical data
            FetchDataInfo fetchResult =
                    logTablet.read(
                            currentOffset, maxFetchBytes, FetchIsolation.LOG_END, true, null);

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

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Successfully batch loaded cold data from global WAL range [{}, {}), tableBucket {}",
                    globalStartOffset,
                    globalEndOffset,
                    logTablet.getTableBucket());
        }
    }

    /**
     * Unified record distribution pipeline that processes WAL records and writes IndexedRow data
     * directly to IndexRowCache. This method handles both hot and cold data processing with
     * optional conditional writing support.
     *
     * @param walRecords the WAL records to process
     * @param firstOffset the first offset of the WAL records
     * @return the maximum processed offset
     * @throws Exception if an error occurs during processing
     */
    private long distributeRecordsToCache(LogRecords walRecords, long firstOffset)
            throws Exception {
        long currentOffset = firstOffset;
        int batchIndex = 0;
        try {
            for (LogRecordBatch batch : walRecords.batches()) {
                short schemaId = batch.schemaId();

                // Use batch.baseLogOffset() as the authoritative source for batch start
                // This is more explicit and handles any edge cases with offset discontinuities
                long batchStartOffset = batch.baseLogOffset();
                long batchEndOffset = batchStartOffset;

                try (LogRecordReadContext readContext =
                                LogRecordReadContext.createArrowReadContext(
                                        tableSchema.getRowType(), schemaId);
                        CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                    while (recordIterator.hasNext()) {
                        LogRecord walRecord = recordIterator.next();
                        // Process each record with batch mode (only write to target buckets)
                        for (TableCacheWriter indexWriter : tableTableCacheWriters) {
                            indexWriter.writeRecordInBatch(
                                    walRecord, batchEndOffset, batchStartOffset);
                        }
                        batchEndOffset++;
                    }
                }

                // After processing all records in the batch, synchronize all buckets to the same
                // offset
                // level. CRITICAL: Must synchronize even for empty batches to prevent range gaps.
                // Empty batches (e.g., containing only state changes) still occupy offset space
                // and must be tracked to ensure continuous offset progression across all index
                // buckets.

                // Use batch.lastLogOffset() as the target for synchronization.
                // synchronizeAllBucketsToOffset(targetOffset) will expand Range to targetOffset+1,
                // which equals batch.nextLogOffset(). This handles all cases including empty
                // batches.
                long lastOffset = batch.lastLogOffset();
                long nextOffset = batch.nextLogOffset();

                // DEBUG: Log finalizeBatch call
                if (shouldLogDebug()) {
                    LOG.info(
                            "[RANGE_GAP_DEBUG] IndexCacheWriter finalizing batch {}: dataBucket={}, "
                                    + "lastOffset={}, batchStartOffset={}, processedEndOffset={}",
                            batchIndex,
                            logTablet.getTableBucket(),
                            lastOffset,
                            batchStartOffset,
                            batchEndOffset);
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "DataBucket {}: Finalizing batch - baseOffset={}, lastOffset={}, nextOffset={}, recordCount={}, processedEndOffset={}",
                            logTablet.getTableBucket(),
                            batch.baseLogOffset(),
                            lastOffset,
                            nextOffset,
                            batch.getRecordCount(),
                            batchEndOffset);
                }

                for (TableCacheWriter indexWriter : tableTableCacheWriters) {
                    // Use lastLogOffset to ensure Range endOffset becomes nextLogOffset
                    indexWriter.finalizeBatch(lastOffset, batchStartOffset);
                }

                currentOffset = nextOffset;
                batchIndex++;
            }
        } catch (Exception e) {
            LOG.error("Error distributing records to cache", e);
            throw e;
        }
        return currentOffset;
    }

    private boolean shouldLogDebug() {
        String tableName = logTablet.getTablePath().getTableName();
        int bucketId = logTablet.getTableBucket().getBucket();
        return "fls_dwd_tt_lg_trd_erp_wms_qk_ri".equals(tableName) && bucketId < 3;
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
