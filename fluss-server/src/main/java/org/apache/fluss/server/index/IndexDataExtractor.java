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
import org.apache.fluss.metadata.SchemaGetter;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * IndexDataExtractor extracts index data from data table WAL records and writes to {@link
 * IndexBucketCacheManager} for both hot data (real-time WAL records) and cold data (on-demand WAL
 * loading).
 *
 * <p>Core Design: Provides a unified processing pipeline that extracts index data from WAL records
 * and writes them directly to IndexBucketCache with logOffset information. The class handles:
 *
 * <ul>
 *   <li>Hot data processing: Real-time index data writing from KvTablet WAL records
 *   <li>Cold data loading: On-demand loading from LogTablet when cache misses occur
 *   <li>Duplicate handling: IndexBucketCache automatically ignores duplicates by logOffset
 *   <li>IndexedRow creation: Efficient IndexedRow generation from data rows and index definitions
 * </ul>
 *
 * <p>Key Features:
 *
 * <ul>
 *   <li>Unified hot/cold processing pipeline using distributeRecordsToCache()
 *   <li>Direct IndexBucketCacheManager writing instead of intermediate IndexSegment construction
 *   <li>Efficient bucketing and key encoding for proper index distribution
 *   <li>Thread-safe for read operations, requires external synchronization for writes
 * </ul>
 */
@Internal
public final class IndexDataExtractor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDataExtractor.class);

    /** Maximum bytes to fetch per cold data loading iteration (64 MB). */
    private static final int COLD_LOAD_MAX_FETCH_BYTES = 64 * 1024 * 1024;

    private final LogTablet logTablet;
    private final SchemaGetter schemaGetter;

    private volatile boolean closed = false;

    private final Map<Short, LogRecordReadContext> readContextCache = new ConcurrentHashMap<>();

    private final List<SingleIndexExtractor> singleIndexExtractors;

    public IndexDataExtractor(
            LogTablet logTablet,
            IndexBucketCacheManager cacheManager,
            BucketingFunction bucketingFunction,
            SchemaGetter schemaGetter,
            List<TableInfo> indexTableInfos,
            @Nullable TableInfo mainTableInfo) {
        this.logTablet = logTablet;
        this.schemaGetter = checkNotNull(schemaGetter, "schemaGetter cannot be null");

        // Get latest schema for SingleIndexExtractor initialization only
        Schema initSchema = schemaGetter.getLatestSchemaInfo().getSchema();

        // Get auto-partition strategy from main table if available and enabled
        AutoPartitionStrategy autoPartitionStrategy =
                (mainTableInfo != null
                                && mainTableInfo.getAutoPartitionStrategy() != null
                                && mainTableInfo
                                        .getAutoPartitionStrategy()
                                        .isAutoPartitionEnabled())
                        ? mainTableInfo.getAutoPartitionStrategy()
                        : null;

        this.singleIndexExtractors =
                indexTableInfos.stream()
                        .map(
                                indexTableInfo ->
                                        new SingleIndexExtractor(
                                                indexTableInfo.getTableId(),
                                                initSchema,
                                                indexTableInfo.getSchema(),
                                                indexTableInfo.getNumBuckets(),
                                                indexTableInfo.getBucketKeys(),
                                                bucketingFunction,
                                                cacheManager,
                                                autoPartitionStrategy))
                        .collect(Collectors.toList());

        LOG.info(
                "IndexDataExtractor initialized for table bucket {} with IndexBucketCacheManager",
                logTablet.getTableBucket());
    }

    /**
     * Write hot data from WAL records directly to cache. This method processes WAL records
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
                    "IndexDataExtractor is closed, cannot write hot data, tableBucket {}",
                    logTablet.getTableBucket());
            return;
        }

        if (walRecords == null || appendInfo == null) {
            LOG.debug(
                    "Invalid parameters for hot data writing, skipping, tableBucket {}",
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
     * @throws Exception if an error occurs during batch cold data loading
     */
    public void loadColdDataToCache(long globalStartOffset, long globalEndOffset) throws Exception {

        if (closed) {
            LOG.warn(
                    "IndexDataExtractor is closed, cannot batch load cold data, tableBucket {}",
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
            int maxFetchBytes = COLD_LOAD_MAX_FETCH_BYTES;

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

        LOG.debug(
                "Successfully batch loaded cold data from global WAL range [{}, {}), tableBucket {}",
                globalStartOffset,
                globalEndOffset,
                logTablet.getTableBucket());
    }

    /**
     * Unified record distribution pipeline that processes WAL records and writes IndexedRow data
     * directly to cache.
     */
    private long distributeRecordsToCache(LogRecords walRecords, long firstOffset)
            throws Exception {
        long currentOffset = firstOffset;
        for (LogRecordBatch batch : walRecords.batches()) {
            short schemaId = batch.schemaId();

            long batchStartOffset = batch.baseLogOffset();
            long batchEndOffset = batchStartOffset;

            LogRecordReadContext readContext =
                    readContextCache.computeIfAbsent(
                            schemaId,
                            id ->
                                    LogRecordReadContext.createArrowReadContext(
                                            schemaGetter.getSchema(id).getRowType(),
                                            id,
                                            schemaGetter));
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord walRecord = recordIterator.next();
                    for (SingleIndexExtractor extractor : singleIndexExtractors) {
                        extractor.writeRecordInBatch(walRecord, batchEndOffset, batchStartOffset);
                    }
                    batchEndOffset++;
                }
            } finally {
                long lastOffset = batch.lastLogOffset();
                // Finalize batch even on exception to maintain offset continuity
                // across all index buckets. Without this, some buckets would have
                // gaps in their offset ranges, causing inconsistency on retry.
                for (SingleIndexExtractor extractor : singleIndexExtractors) {
                    try {
                        extractor.finalizeBatch(lastOffset, batchStartOffset);
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to finalize batch for extractor, "
                                        + "baseOffset={}, lastOffset={}",
                                batchStartOffset,
                                lastOffset,
                                e);
                    }
                }
            }

            long nextOffset = batch.nextLogOffset();

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "DataBucket {}: Finalizing batch - baseOffset={}, lastOffset={}, nextOffset={}, recordCount={}, processedEndOffset={}",
                        logTablet.getTableBucket(),
                        batch.baseLogOffset(),
                        batch.lastLogOffset(),
                        nextOffset,
                        batch.getRecordCount(),
                        batchEndOffset);
            }

            currentOffset = nextOffset;
        }
        return currentOffset;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        for (LogRecordReadContext ctx : readContextCache.values()) {
            try {
                ctx.close();
            } catch (Exception e) {
                LOG.warn("Error closing cached LogRecordReadContext", e);
            }
        }
        readContextCache.clear();
        LOG.info("IndexDataExtractor closed");
    }
}
