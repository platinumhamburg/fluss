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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.DeletionDisabledException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.AutoIncrementUpdater;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rowmerger.DefaultRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.scan.OpenScanResult;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
import org.apache.fluss.server.kv.wal.CompactedWalBuilder;
import org.apache.fluss.server.kv.wal.IndexWalBuilder;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.FencedWriterStateEntry;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.function.SupplierWithException;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);
    private static final long ROW_COUNT_DISABLED = -1;

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    private final LogTablet logTablet;
    private final ArrowWriterProvider arrowWriterProvider;
    private final MemorySegmentPool memorySegmentPool;

    private final File kvTabletDir;
    @Nullable private volatile Runnable afterFencedPrecheck;
    @Nullable private volatile Runnable putLockContentionHook;
    private final long writeBatchSize;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer kvPreWriteBuffer;
    private final TabletServerMetricGroup serverMetricGroup;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();
    private final LogFormat logFormat;
    private final KvFormat kvFormat;
    // defines how to merge rows on the same primary key
    private final RowMerger rowMerger;
    // Pre-created DefaultRowMerger for OVERWRITE mode (undo recovery scenarios)
    // This avoids creating a new instance on every putAsLeader call
    private final RowMerger overwriteRowMerger;
    private final ArrowCompressionInfo arrowCompressionInfo;
    private final AutoIncrementManager autoIncrementManager;

    private final SchemaGetter schemaGetter;

    /**
     * Optional function to extract the tag value from a row for v3 value encoding. When non-null,
     * values are encoded in v3 format: [schemaId(2)][tag(8)][BinaryRow]. For partition TTL on Index
     * Tables, this extracts the partitionId from the row.
     */
    @Nullable private final ToLongFunction<BinaryRow> tagExtractor;

    /** The KV format version of this table (determines value layout). */
    private final int kvFormatVersion;

    /** Version-aware encoder that encapsulates format version and tag extraction. */
    private final ValueEncoder valueEncoder;

    private final KvWriteGuard writeGuard;

    // the changelog image mode for this tablet
    private final ChangelogImage changelogImage;

    // RocksDB statistics accessor for this tablet
    @Nullable private final RocksDBStatistics rocksDBStatistics;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    private volatile long rowCount;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    @GuardedBy("kvLock")
    @Nullable
    private Throwable uncertainWalAppendFailure;

    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            TabletServerMetricGroup serverMetricGroup,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            LogFormat logFormat,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            int kvFormatVersion,
            @Nullable RocksDBStatistics rocksDBStatistics,
            AutoIncrementManager autoIncrementManager,
            @Nullable ToLongFunction<BinaryRow> tagExtractor,
            KvWriteGuard writeGuard) {
        validateValueFormatVersion(kvFormatVersion, tagExtractor);
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.serverMetricGroup = serverMetricGroup;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(createKvBatchWriter(), serverMetricGroup);
        this.logFormat = logFormat;
        this.arrowWriterProvider = new ArrowWriterPool(arrowBufferAllocator);
        this.memorySegmentPool = memorySegmentPool;
        this.kvFormat = kvFormat;
        this.rowMerger = rowMerger;
        // Pre-create DefaultRowMerger for OVERWRITE mode to avoid creating new instances
        // on every putAsLeader call. Used for undo recovery scenarios.
        this.overwriteRowMerger = new DefaultRowMerger(kvFormat, DeleteBehavior.ALLOW);
        this.arrowCompressionInfo = arrowCompressionInfo;
        this.schemaGetter = schemaGetter;
        this.changelogImage = changelogImage;
        this.rocksDBStatistics = rocksDBStatistics;
        this.autoIncrementManager = autoIncrementManager;
        // disable row count for WAL image mode.
        this.rowCount = changelogImage == ChangelogImage.WAL ? ROW_COUNT_DISABLED : 0L;
        this.tagExtractor = tagExtractor;
        this.kvFormatVersion = kvFormatVersion;
        this.valueEncoder = ValueEncoder.forVersion(kvFormatVersion, tagExtractor);
        this.writeGuard = writeGuard;
    }

    private static void validateValueFormatVersion(
            int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> tagExtractor) {
        if (kvFormatVersion >= ConfigOptions.KV_FORMAT_VERSION_3 && tagExtractor == null) {
            throw new IllegalArgumentException(
                    "tagExtractor must be non-null for kvFormatVersion >= 3");
        }
        if (kvFormatVersion < ConfigOptions.KV_FORMAT_VERSION_3 && tagExtractor != null) {
            throw new IllegalArgumentException("tagExtractor must be null for kvFormatVersion < 3");
        }
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            int kvFormatVersion,
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager,
            @Nullable
                    AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
                            compactionFilterFactory,
            @Nullable ToLongFunction<BinaryRow> tagExtractor)
            throws IOException {
        return create(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverConf,
                serverMetricGroup,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                kvFormatVersion,
                sharedRateLimiter,
                autoIncrementManager,
                compactionFilterFactory,
                tagExtractor,
                KvWriteGuard.ACCEPT_ALL);
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            int kvFormatVersion,
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager,
            @Nullable
                    AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
                            compactionFilterFactory,
            @Nullable ToLongFunction<BinaryRow> tagExtractor,
            KvWriteGuard writeGuard)
            throws IOException {
        validateValueFormatVersion(kvFormatVersion, tagExtractor);
        RocksDBKv kv =
                buildRocksDBKv(serverConf, kvTabletDir, sharedRateLimiter, compactionFilterFactory);

        // Create RocksDB statistics accessor (will be registered to TableMetricGroup by Replica)
        // Pass ResourceGuard to ensure thread-safe access during concurrent close operations
        // Pass ColumnFamilyHandle for column family specific properties like num-files-at-level0
        // Pass Cache for accurate block cache memory tracking
        RocksDBStatistics rocksDBStatistics =
                new RocksDBStatistics(
                        kv.getDb(),
                        kv.getStatistics(),
                        kv.getResourceGuard(),
                        kv.getDefaultColumnFamilyHandle(),
                        kv.getBlockCache());

        return new KvTablet(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverMetricGroup,
                kv,
                serverConf.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                logTablet.getLogFormat(),
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                kvFormatVersion,
                rocksDBStatistics,
                autoIncrementManager,
                tagExtractor,
                writeGuard);
    }

    private static RocksDBKv buildRocksDBKv(
            Configuration configuration,
            File kvDir,
            RateLimiter sharedRateLimiter,
            @Nullable
                    AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
                            compactionFilterFactory)
            throws IOException {
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(configuration, kvDir, true, sharedRateLimiter);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        kvDir,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());
        if (compactionFilterFactory != null) {
            rocksDBKvBuilder.setCompactionFilterFactory(compactionFilterFactory);
        }
        return rocksDBKvBuilder.build();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    /** Returns the version-aware ValueEncoder bound to this tablet's format version. */
    public ValueEncoder getValueEncoder() {
        return valueEncoder;
    }

    /** Returns the KV format version of this tablet (e.g. 2 or 3). */
    public int getKvFormatVersion() {
        return kvFormatVersion;
    }

    public long getAutoIncrementCacheSize() {
        return autoIncrementManager.getAutoIncrementCacheSize();
    }

    public void updateAutoIncrementIDRange(AutoIncIDRange newRange) {
        autoIncrementManager.updateIDRange(newRange);
    }

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public File getKvTabletDir() {
        return kvTabletDir;
    }

    /**
     * Get RocksDB statistics accessor for this tablet.
     *
     * @return the RocksDB statistics accessor, or null if not available
     */
    @Nullable
    public RocksDBStatistics getRocksDBStatistics() {
        return rocksDBStatistics;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    /**
     * Installs a value filter used by Index Table replicas to skip entries whose source partition
     * has been tombstoned. The filter is evaluated during point-lookup and prefix-scan paths.
     *
     * @param filter returns {@code true} when the value should be dropped
     */
    private static final java.util.function.Predicate<byte[]> NO_OP_VALUE_FILTER = v -> false;

    private volatile java.util.function.Predicate<byte[]> valueFilter = NO_OP_VALUE_FILTER;
    @Nullable private Runnable beforeWalBuild;

    public void setValueFilter(@Nullable java.util.function.Predicate<byte[]> filter) {
        this.valueFilter = filter == null ? NO_OP_VALUE_FILTER : filter;
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        if (rowCount == ROW_COUNT_DISABLED) {
            throw new InvalidTableException(
                    String.format(
                            "Row count is disabled for this table '%s'. This usually happens when the table is"
                                    + "created before v0.9 or the changelog image is set to WAL, "
                                    + "as maintaining row count in WAL mode is costly and not necessary for most use cases. "
                                    + "If you want to enable row count, please set changelog image to FULL.",
                            getTablePath()));
        }
        return rowCount;
    }

    /**
     * Get the current state of the tablet, including the log offset, row count and auto-increment
     * ID range. This is used for snapshot and recovery to capture the state of the tablet at a
     * specific log offset.
     *
     * <p>Note: this method must be called under the kvLock to ensure the consistency between the
     * returned state and the log offset.
     */
    @GuardedBy("kvLock")
    public TabletState getTabletState() {
        return new TabletState(
                flushedLogOffset,
                rowCount == ROW_COUNT_DISABLED ? null : rowCount,
                autoIncrementManager.getCurrentIDRanges());
    }

    /**
     * Put the KvRecordBatch into the kv storage with default DEFAULT mode.
     *
     * <p>This is a convenience method that calls {@link #putAsLeader(KvRecordBatch, int[],
     * MergeMode)} with {@link MergeMode#DEFAULT}.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     */
    public LogAppendInfo putAsLeader(KvRecordBatch kvRecords, @Nullable int[] targetColumns)
            throws Exception {
        return putAsLeader(kvRecords, targetColumns, MergeMode.DEFAULT);
    }

    /**
     * Put the KvRecordBatch into the kv storage, and return the appended wal log info.
     *
     * <p>Schema Evolution Handling:
     *
     * <p>We don't allow shema of input kv records to be larger than the latest schema id known by
     * the tablet. Besides, we currently only support ADD COLUMN LAST operation, so the input row or
     * old row must have same or fewer columns than latest schema. This helps to simplify the schema
     * change handling.
     *
     * <p>1. We write the kv records into KvStore without converting it into latest schema for
     * performance consideration. We have mechanisms that writer client dynamically use latest
     * schema for writing records.
     *
     * <p>2. We always use the latest schema for writing WAL logs, because it anyway happens
     * deserialization&serialization to convert the compacted format into Arrow format.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     * @param mergeMode the merge mode (DEFAULT or OVERWRITE)
     */
    public LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords, @Nullable int[] targetColumns, MergeMode mergeMode)
            throws Exception {
        return inPutWriteLock(
                () -> {
                    throwIfUncertainWalAppend();
                    rocksDBKv.checkIfRocksDBClosed();

                    KvIdempotenceProtocol tableProtocol = logTablet.getWriterStateProtocol();
                    boolean fenced = tableProtocol == KvIdempotenceProtocol.V1_FENCED;
                    if (fenced) {
                        kvRecords.ensureValid();
                    }
                    int batchProtocolVersion = kvRecords.idempotenceProtocolVersion();
                    if (batchProtocolVersion != tableProtocol.version()) {
                        throw new CorruptRecordException(
                                String.format(
                                        "KV batch protocol V%d does not match table protocol V%d for %s",
                                        batchProtocolVersion,
                                        tableProtocol.version(),
                                        tableBucket));
                    }

                    SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
                    Schema latestSchema = schemaInfo.getSchema();
                    short latestSchemaId = (short) schemaInfo.getSchemaId();
                    validateSchemaId(kvRecords.schemaId(), latestSchemaId);

                    if (fenced) {
                        WriterKey writerKey = kvRecords.fencedWriterKey();
                        long fencedSequence = kvRecords.fencedSequence();
                        if (fencedSequence < 0L) {
                            throw new IllegalArgumentException("sequence must be non-negative");
                        }
                        if (writeGuard.beforeWriterState(writerKey)
                                == KvWriteGuard.Decision.NO_OP) {
                            serverMetricGroup.indexPushTombstoneNoOpBatches().inc();
                            return LogAppendInfo.noAppend();
                        }
                        Optional<FencedWriterStateEntry> stale =
                                logTablet.findStaleFencedBatch(writerKey, fencedSequence);
                        if (stale.isPresent()) {
                            FencedWriterStateEntry entry = stale.get();
                            serverMetricGroup.indexPushStaleV1Batches().inc();
                            return LogAppendInfo.duplicatedAt(
                                    entry.dominatingTargetWalOffset(), entry.lastTimestamp());
                        }
                        Runnable hook = afterFencedPrecheck;
                        if (hook != null) {
                            hook.run();
                        }
                    }

                    AutoIncrementUpdater currentAutoIncrementUpdater =
                            autoIncrementManager.getUpdaterForSchema(kvFormat, latestSchemaId);

                    // Validate targetColumns doesn't contain auto-increment column
                    currentAutoIncrementUpdater.validateTargetColumns(targetColumns);

                    // Determine the row merger based on mergeMode:
                    // - DEFAULT: Use the configured merge engine (rowMerger)
                    // - OVERWRITE: Bypass merge engine, use pre-created overwriteRowMerger
                    //   to directly replace values (for undo recovery scenarios)
                    // We only support ADD COLUMN, so targetColumns is fine to be used directly.
                    RowMerger currentMerger =
                            (mergeMode == MergeMode.OVERWRITE)
                                    ? overwriteRowMerger.configureTargetColumns(
                                            targetColumns, latestSchemaId, latestSchema)
                                    : rowMerger.configureTargetColumns(
                                            targetColumns, latestSchemaId, latestSchema);

                    RowType latestRowType = latestSchema.getRowType();
                    WalBuilder walBuilder = createWalBuilder(latestSchemaId, latestRowType, fenced);
                    if (fenced) {
                        walBuilder.setFencedWriterState(
                                kvRecords.fencedWriterKey(), kvRecords.fencedSequence());
                    } else {
                        walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
                    }
                    // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
                    // only has fewer ending columns than latest schema, so we pad nulls to
                    // the end of the BinaryRow to get the latest schema row.
                    PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
                    // get offset to track the offset corresponded to the kv record
                    long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();

                    boolean appendInvoked = false;
                    try {
                        processKvRecords(
                                kvRecords,
                                kvRecords.schemaId(),
                                currentMerger,
                                currentAutoIncrementUpdater,
                                walBuilder,
                                latestSchemaRow,
                                logEndOffsetOfPrevBatch,
                                fenced ? kvRecords.fencedWriterKey() : null);

                        // There will be a situation that these batches of kvRecordBatch have not
                        // generated any CDC logs, for example, when client attempts to delete
                        // some non-existent keys or MergeEngineType set to FIRST_ROW. In this case,
                        // we cannot simply return, as doing so would cause a
                        // OutOfOrderSequenceException problem. Therefore, here we will build an
                        // empty batch with lastLogOffset to 0L as the baseLogOffset is 0L. As doing
                        // that, the logOffsetDelta in logRecordBatch will be set to 0L. So, we will
                        // put a batch into file with recordCount 0 and offset plus 1L, it will
                        // update the batchSequence corresponding to the writerId and also increment
                        // the CDC log offset by 1.
                        Runnable buildHook = beforeWalBuild;
                        if (buildHook != null) {
                            buildHook.run();
                        }
                        MemoryLogRecords wal = walBuilder.build();
                        appendInvoked = true;
                        LogAppendInfo logAppendInfo = logTablet.appendAsLeader(wal);

                        // if the batch is duplicated, we should truncate the kvPreWriteBuffer
                        // already written.
                        if (logAppendInfo.duplicated()) {
                            if (fenced) {
                                serverMetricGroup.indexPushStaleV1Batches().inc();
                            }
                            kvPreWriteBuffer.truncateTo(
                                    logEndOffsetOfPrevBatch, TruncateReason.DUPLICATED);
                        }
                        return logAppendInfo;
                    } catch (Throwable t) {
                        // While encounter error here, the CDC logs may fail writing to disk,
                        // and the client probably will resend the batch. If we do not remove the
                        // values generated by the erroneous batch from the kvPreWriteBuffer, the
                        // retry-send batch will produce incorrect CDC logs.
                        // TODO for some errors, the cdc logs may already be written to disk, for
                        //  those errors, we should not truncate the kvPreWriteBuffer.
                        if (fenced && appendInvoked) {
                            if (t instanceof Error) {
                                uncertainWalAppendFailure = t;
                                throw (Error) t;
                            }
                            UncertainWalAppendException uncertainty =
                                    new UncertainWalAppendException(tableBucket, t);
                            uncertainWalAppendFailure = uncertainty;
                            throw uncertainty;
                        }
                        kvPreWriteBuffer.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.ERROR);
                        throw t;
                    } finally {
                        // deallocate the memory and arrow writer used by the wal builder
                        walBuilder.deallocate();
                    }
                });
    }

    private <T> T inPutWriteLock(SupplierWithException<T, Exception> action) throws Exception {
        Lock writeLock = kvLock.writeLock();
        Runnable contentionHook = putLockContentionHook;
        if (contentionHook == null) {
            writeLock.lock();
        } else if (!writeLock.tryLock()) {
            contentionHook.run();
            writeLock.lock();
        }
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    @GuardedBy("kvLock")
    private void throwIfUncertainWalAppend() throws UncertainWalAppendException {
        Throwable failure = uncertainWalAppendFailure;
        if (failure == null) {
            return;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw (UncertainWalAppendException) failure;
    }

    @VisibleForTesting
    void setAfterFencedPrecheck(@Nullable Runnable afterFencedPrecheck) {
        this.afterFencedPrecheck = afterFencedPrecheck;
    }

    @VisibleForTesting
    void setBeforeWalBuild(@Nullable Runnable beforeWalBuild) {
        this.beforeWalBuild = beforeWalBuild;
    }

    @VisibleForTesting
    void setPutLockContentionHook(@Nullable Runnable putLockContentionHook) {
        this.putLockContentionHook = putLockContentionHook;
    }

    private void validateSchemaId(short schemaIdOfNewData, short latestSchemaId) {
        if (schemaIdOfNewData > latestSchemaId || schemaIdOfNewData < 0) {
            throw new SchemaNotExistException(
                    "Invalid schema id: "
                            + schemaIdOfNewData
                            + ", latest schema id: "
                            + latestSchemaId);
        }
    }

    private void processKvRecords(
            KvRecordBatch kvRecords,
            short schemaIdOfNewData,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long startLogOffset,
            @Nullable WriterKey writerKey)
            throws Exception {
        long logOffset = startLogOffset;

        // TODO: reuse the read context and decoder
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat, kvFormatVersion);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
            BinaryRow row = kvRecord.getRow();
            if (writerKey != null) {
                writeGuard.validateRecord(writerKey, keyBytes, row);
            }
            BinaryValue currentValue;
            if (row == null) {
                currentValue = null;
            } else {
                currentValue = valueEncoder.createValue(schemaIdOfNewData, row);
            }

            if (currentValue == null) {
                logOffset =
                        processDeletion(
                                key,
                                currentMerger,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset);
            } else {
                logOffset =
                        processUpsert(
                                key,
                                currentValue,
                                currentMerger,
                                autoIncrementUpdater,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset);
            }
        }
    }

    private long processDeletion(
            KvPreWriteBuffer.Key key,
            RowMerger currentMerger,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
        if (deleteBehavior == DeleteBehavior.IGNORE) {
            // skip delete rows if the merger doesn't support yet
            return logOffset;
        } else if (deleteBehavior == DeleteBehavior.DISABLE) {
            throw new DeletionDisabledException(
                    "Delete operations are disabled for this table. "
                            + "The table.delete.behavior is set to 'disable'.");
        }

        byte[] oldValueBytes = getFromBufferOrKv(key);
        if (oldValueBytes == null) {
            LOG.debug(
                    "The specific key can't be found in kv tablet although the kv record is for deletion, "
                            + "ignore it directly as it doesn't exist in the kv tablet yet.");
            return logOffset;
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.delete(oldValue);

        if (newValue == null) {
            long newOffset = applyDelete(key, oldValue, walBuilder, latestSchemaRow, logOffset);

            return newOffset;
        } else {
            long newOffset =
                    applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);

            return newOffset;
        }
    }

    private long processUpsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        java.util.function.Predicate<byte[]> filter = this.valueFilter;
        if (filter != NO_OP_VALUE_FILTER) {
            // The filter contract takes the encoded value bytes (schemaId + row), matching the
            // format produced by BinaryValue#encodeValue and consumed by the read/compaction
            // filter paths. Feeding raw row bytes here would shift the column offsets by
            // SCHEMA_ID_LENGTH and silently misread filter inputs.
            byte[] encodedValueBytes = currentValue.encodeValue();
            if (filter.test(encodedValueBytes)) {
                return logOffset;
            }
        }
        // Optimization: IN WAL mode, when using DefaultRowMerger (full update, not partial update)
        // and there is no auto-increment column, we can skip fetching old value for better
        // performance since the result always reflects the new value. In this case, both INSERT and
        // UPDATE will produce UPDATE_AFTER.
        if (changelogImage == ChangelogImage.WAL
                && !autoIncrementUpdater.hasAutoIncrement()
                && currentMerger instanceof DefaultRowMerger) {
            return applyUpdate(key, null, currentValue, walBuilder, latestSchemaRow, logOffset);
        }

        byte[] oldValueBytes = getFromBufferOrKv(key);
        if (oldValueBytes == null) {
            BinaryValue valueToInsert = currentMerger.merge(null, currentValue);
            long newOffset =
                    applyInsert(
                            key,
                            valueToInsert,
                            walBuilder,
                            latestSchemaRow,
                            logOffset,
                            autoIncrementUpdater);

            return newOffset;
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.merge(oldValue, currentValue);

        if (newValue == oldValue) {
            return logOffset;
        }

        long newOffset =
                applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
        return newOffset;
    }

    private long applyDelete(
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        walBuilder.append(ChangeType.DELETE, latestSchemaRow.replaceRow(oldValue.row));
        kvPreWriteBuffer.delete(key, logOffset);
        return logOffset + 1;
    }

    private long applyInsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            AutoIncrementUpdater autoIncrementUpdater)
            throws Exception {
        BinaryValue newValue = autoIncrementUpdater.updateAutoIncrementColumns(currentValue);
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row));
        kvPreWriteBuffer.insert(key, newValue.encodeValue(), logOffset);
        return logOffset + 1;
    }

    private long applyUpdate(
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            BinaryValue newValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        if (changelogImage.hasUpdateBefore()) {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset + 1);
            return logOffset + 2;
        } else {
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset);
            return logOffset + 1;
        }
    }

    private WalBuilder createWalBuilder(int schemaId, RowType rowType, boolean fenced)
            throws Exception {
        switch (logFormat) {
            case INDEXED:
                if (kvFormat == KvFormat.COMPACTED) {
                    // convert from compacted row to indexed row is time cost, and gain
                    // less benefits, currently we won't support compacted as kv format and
                    // indexed as cdc log format.
                    // so in here we throw exception directly
                    throw new IllegalArgumentException(
                            "Primary Key Table with COMPACTED kv format doesn't support INDEXED cdc log format.");
                }
                return fenced
                        ? IndexWalBuilder.fencedBuilder(schemaId, memorySegmentPool)
                        : new IndexWalBuilder(schemaId, memorySegmentPool);
            case COMPACTED:
                return fenced
                        ? CompactedWalBuilder.fencedBuilder(schemaId, rowType, memorySegmentPool)
                        : new CompactedWalBuilder(schemaId, rowType, memorySegmentPool);
            case ARROW:
                return fenced
                        ? ArrowWalBuilder.fencedBuilder(
                                schemaId,
                                arrowWriterProvider.getOrCreateWriter(
                                        tableBucket.getTableId(),
                                        schemaId,
                                        Integer.MAX_VALUE,
                                        rowType,
                                        arrowCompressionInfo),
                                memorySegmentPool)
                        : new ArrowWalBuilder(
                                schemaId,
                                arrowWriterProvider.getOrCreateWriter(
                                        tableBucket.getTableId(),
                                        schemaId,
                                        Integer.MAX_VALUE,
                                        rowType,
                                        arrowCompressionInfo),
                                memorySegmentPool);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    public void flush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        // todo: need to introduce a backpressure mechanism
        // to avoid too much records in kvPreWriteBuffer
        inWriteLock(
                kvLock,
                () -> {
                    // when kv manager is closed which means kv tablet is already closed,
                    // but the tablet server may still handle fetch log request from follower
                    // as the tablet rpc service is closed asynchronously, then update the watermark
                    // and then flush the pre-write buffer.

                    // In such case, if the tablet is already closed, we won't flush pre-write
                    // buffer, just warning it.
                    if (isClosed) {
                        LOG.warn(
                                "The kv tablet for {} is already closed, ignore flushing kv pre-write buffer.",
                                tableBucket);
                    } else {
                        try {
                            int rowCountDiff = kvPreWriteBuffer.flush(exclusiveUpToLogOffset);
                            flushedLogOffset = exclusiveUpToLogOffset;
                            if (rowCount != ROW_COUNT_DISABLED) {
                                // row count is enabled, we update the row count after flush.
                                long currentRowCount = rowCount;
                                rowCount = currentRowCount + rowCountDiff;
                            }
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new KvStorageException("Failed to flush kv pre-write buffer."));
                        }
                    }
                });
    }

    /** put key,value,logOffset into pre-write buffer directly. */
    void putToPreWriteBuffer(
            ChangeType changeType, byte[] key, @Nullable byte[] value, long logOffset) {
        KvPreWriteBuffer.Key wrapKey = KvPreWriteBuffer.Key.of(key);
        if (changeType == ChangeType.DELETE && value == null) {
            kvPreWriteBuffer.delete(wrapKey, logOffset);
        } else if (changeType == ChangeType.INSERT) {
            kvPreWriteBuffer.insert(wrapKey, value, logOffset);
        } else if (changeType == ChangeType.UPDATE_AFTER) {
            kvPreWriteBuffer.update(wrapKey, value, logOffset);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported change type for putToPreWriteBuffer: " + changeType);
        }
    }

    /**
     * Get a executor that executes submitted runnable tasks with preventing any concurrent
     * modification to this tablet.
     *
     * @return An executor that wraps task execution within the lock for all modification to this
     *     tablet.
     */
    public Executor getGuardedExecutor() {
        return runnable -> inWriteLock(kvLock, runnable::run);
    }

    // get from kv pre-write buffer first, if can't find, get from rocksdb
    private byte[] getFromBufferOrKv(KvPreWriteBuffer.Key key) throws IOException {
        KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
        if (value == null) {
            return rocksDBKv.get(key.get());
        }
        return value.get();
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.multiGet(keys);
                });
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.prefixLookup(prefixKey);
                });
    }

    public List<byte[]> limitScan(int limit) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.limitScan(limit);
                });
    }

    /**
     * Opens a new full-scan session under the {@code kvLock} read lock. Returns an empty-bucket
     * result (context = {@code null}, all RocksDB resources released internally) when the bucket
     * has no rows. The returned {@link ScannerContext} is unregistered; the caller owns
     * registration and close.
     *
     * @param limit row-count cap across all batches ({@code ≤ 0} means unlimited)
     * @throws IOException if RocksDB is shutting down
     */
    public OpenScanResult openScan(String scannerId, long limit, long initialAccessTimeMs)
            throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
                    Snapshot snapshot = null;
                    ReadOptions readOptions = null;
                    RocksIterator iterator = null;
                    boolean success = false;
                    try {
                        snapshot = rocksDBKv.getDb().getSnapshot();
                        // Capture under kvLock so the offset matches the data visible through
                        // the snapshot.
                        long capturedLogOffset = flushedLogOffset;
                        readOptions = new ReadOptions().setSnapshot(snapshot);
                        iterator =
                                rocksDBKv
                                        .getDb()
                                        .newIterator(
                                                rocksDBKv.getDefaultColumnFamilyHandle(),
                                                readOptions);
                        iterator.seekToFirst();
                        if (!iterator.isValid()) {
                            return new OpenScanResult(null, capturedLogOffset);
                        }
                        ScannerContext context =
                                new ScannerContext(
                                        scannerId,
                                        tableBucket,
                                        rocksDBKv,
                                        iterator,
                                        readOptions,
                                        snapshot,
                                        lease,
                                        limit,
                                        capturedLogOffset,
                                        initialAccessTimeMs);
                        success = true;
                        return new OpenScanResult(context, capturedLogOffset);
                    } finally {
                        if (!success) {
                            IOUtils.closeQuietly(iterator);
                            IOUtils.closeQuietly(readOptions);
                            if (snapshot != null) {
                                try {
                                    rocksDBKv.getDb().releaseSnapshot(snapshot);
                                } catch (Throwable t) {
                                    LOG.warn("Error releasing RocksDB snapshot.", t);
                                }
                                IOUtils.closeQuietly(snapshot);
                            }
                            IOUtils.closeQuietly(lease);
                        }
                    }
                });
    }

    public KvBatchWriter createKvBatchWriter() {
        return rocksDBKv.newWriteBatch(
                writeBatchSize,
                serverMetricGroup.kvFlushCount(),
                serverMetricGroup.kvFlushLatencyHistogram());
    }

    public void close() throws Exception {
        LOG.info("close kv tablet {} for table {}.", tableBucket, physicalPath);
        inWriteLock(
                kvLock,
                () -> {
                    if (isClosed) {
                        return;
                    }
                    // Note: RocksDB metrics lifecycle is managed by TableMetricGroup
                    // No need to close it here
                    if (rocksDBKv != null) {
                        rocksDBKv.close();
                    }
                    isClosed = true;
                });
    }

    /** Completely delete the kv directory and all contents form the file system with no delay. */
    public void drop() throws Exception {
        inWriteLock(
                kvLock,
                () -> {
                    // first close the kv.
                    close();
                    // then delete the directory.
                    FileUtils.deleteDirectory(kvTabletDir);
                });
    }

    public RocksIncrementalSnapshot createIncrementalSnapshot(
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            long lastCompletedSnapshotId) {
        return new RocksIncrementalSnapshot(
                uploadedSstFiles,
                rocksDBKv.getDb(),
                rocksDBKv.getResourceGuard(),
                kvSnapshotDataUploader,
                kvTabletDir,
                lastCompletedSnapshotId);
    }

    // only for testing.
    @VisibleForTesting
    public KvPreWriteBuffer getKvPreWriteBuffer() {
        return kvPreWriteBuffer;
    }

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }
}
