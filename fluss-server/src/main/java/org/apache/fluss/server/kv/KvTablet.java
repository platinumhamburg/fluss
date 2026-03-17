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
import org.apache.fluss.exception.DeletionDisabledException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
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
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.row.encode.ValueDecoder;
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
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
import org.apache.fluss.server.kv.wal.CompactedWalBuilder;
import org.apache.fluss.server.kv.wal.IndexWalBuilder;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.rocksdb.RateLimiter;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntSupplier;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);
    static final long ROW_COUNT_DISABLED = -1;

    /** Coarsen access timestamp updates to reduce volatile writes on hot path. */
    private static final long ACCESS_TIMESTAMP_GRANULARITY_MS = 1000;

    // ---- Lazy lifecycle states ----

    /** Internal RocksDB lifecycle states. */
    enum LazyState {
        /** Not in lazy mode — RocksDB is always open (traditional behavior). */
        EAGER,
        /** Lazy mode: RocksDB not yet opened. */
        LAZY,
        /** Lazy mode: RocksDB is being opened by another thread. */
        OPENING,
        /** Lazy mode: RocksDB is open and serving requests. */
        OPEN,
        /** Lazy mode: RocksDB is being released (closing without deleting data). */
        RELEASING,
        /** Lazy mode: last open attempt failed, in backoff cooldown. */
        FAILED,
        /** Terminal state: tablet is closed. */
        CLOSED
    }

    /**
     * Callback for the actual RocksDB open work. Provided by Replica since it requires access to
     * snapshot context, log recovery, etc.
     */
    @FunctionalInterface
    public interface OpenCallback {
        /** Performs the open and returns the opened RocksDB KvTablet instance. */
        KvTablet doOpen(boolean hasLocalData) throws Exception;
    }

    /**
     * Callback invoked after a successful open commit. Used by Replica to install the tablet
     * reference and start periodic snapshot.
     */
    @FunctionalInterface
    public interface OpenCommitCallback {
        void onOpenCommitted(KvTablet kvTablet);
    }

    /** Callback for releasing (closing) RocksDB without destroying local data. */
    @FunctionalInterface
    public interface ReleaseCallback {
        void doRelease(KvTablet kvTablet);
    }

    /** Callback for dropping (destroying) a KvTablet and cleaning up associated resources. */
    @FunctionalInterface
    public interface DropCallback {
        void doDrop(KvTablet kvTablet);
    }

    /** Callback for cleaning up local KV directory without an open KvTablet. */
    @FunctionalInterface
    public interface CleanLocalCallback {
        void cleanLocalDirectory();
    }

    /**
     * A guard that prevents the underlying RocksDB from being released while held. Use with
     * try-with-resources. In eager mode, this is a no-op. In lazy mode, it pins the RocksDB
     * instance and ensures it is open before returning.
     */
    public static final class Guard implements AutoCloseable {
        private final @Nullable KvTablet tablet;
        private boolean released;

        /** No-op guard for eager mode. */
        static final Guard NOOP = new Guard(null);

        Guard(@Nullable KvTablet tablet) {
            this.tablet = tablet;
        }

        @Override
        public void close() {
            if (!released && tablet != null) {
                released = true;
                tablet.releasePin();
            }
        }
    }

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    private final LogTablet logTablet;
    private final @Nullable TabletServerMetricGroup serverMetricGroup;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();

    // ---- Fields that are null in lazy sentinel, populated after RocksDB open ----

    private volatile @Nullable ArrowWriterProvider arrowWriterProvider;
    private volatile @Nullable MemorySegmentPool memorySegmentPool;
    private volatile @Nullable File kvTabletDir;
    private volatile long writeBatchSize;
    private volatile @Nullable RocksDBKv rocksDBKv;
    private volatile @Nullable KvPreWriteBuffer kvPreWriteBuffer;
    private volatile LogFormat logFormat;
    private volatile KvFormat kvFormat;
    private volatile @Nullable RowMerger rowMerger;
    private volatile @Nullable RowMerger overwriteRowMerger;
    private volatile @Nullable ArrowCompressionInfo arrowCompressionInfo;
    private volatile @Nullable AutoIncrementManager autoIncrementManager;
    private volatile @Nullable SchemaGetter schemaGetter;
    private volatile ChangelogImage changelogImage;
    private volatile @Nullable RocksDBStatistics rocksDBStatistics;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    private volatile long rowCount;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    // ---- Lazy lifecycle fields (only used when lazyState != EAGER) ----

    private volatile LazyState lazyState = LazyState.EAGER;

    /** Lock guarding lazy state transitions. */
    private final ReentrantLock lazyStateLock = new ReentrantLock();

    private final Condition lazyStateChanged = lazyStateLock.newCondition();

    /** Active pin count — prevents release while operations are in-flight. */
    private final AtomicInteger activePins = new AtomicInteger(0);

    private volatile boolean rejectNewPins;

    /** Generation counter for fencing stale open results. */
    private long openGeneration;

    /** Semaphore for throttling concurrent open operations across all tablets. */
    private @Nullable Semaphore openSemaphore;

    private long openTimeoutMs;
    private @Nullable ExponentialBackoff failedBackoff;
    private Clock clock = SystemClock.getInstance();

    /** FAILED state tracking. */
    private long failedTimestamp;

    private int failureCount;
    private @Nullable Throwable lastFailureCause;

    /** Cached values served when RocksDB is not open. */
    private volatile long cachedRowCount;

    /** Whether local RocksDB data directory exists from a previous open. */
    private boolean hasLocalData;

    /** Access timestamp for idle release decisions (coarsened to reduce volatile writes). */
    private volatile long lastAccessTimestamp;

    /** Epoch suppliers for fencing stale open results. */
    private @Nullable IntSupplier leaderEpochSupplier;

    private @Nullable IntSupplier bucketEpochSupplier;

    /** Callbacks (set by Replica after construction for lazy mode). */
    private @Nullable OpenCallback openCallback;

    private @Nullable OpenCommitCallback commitCallback;
    private @Nullable DropCallback dropCallback;
    private @Nullable ReleaseCallback releaseCallback;
    private @Nullable CleanLocalCallback cleanLocalCallback;
    private @Nullable Runnable openRollbackCallback;

    private long releaseDrainTimeoutMs = 5000;

    /** Full constructor for eager mode (RocksDB is immediately available). */
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
            @Nullable RocksDBStatistics rocksDBStatistics,
            AutoIncrementManager autoIncrementManager) {
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
    }

    /**
     * Lazy sentinel constructor: creates a KvTablet in LAZY state without RocksDB. The RocksDB
     * fields are populated later via {@link #installRocksDB(KvTablet)} when ensureOpen() triggers.
     */
    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            @Nullable TabletServerMetricGroup serverMetricGroup) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.serverMetricGroup = serverMetricGroup;
        this.lazyState = LazyState.LAZY;
    }

    /**
     * Create a lazy sentinel KvTablet that starts in LAZY state without RocksDB. Use {@link
     * #configureLazyOpen} and callback setters to configure before use.
     */
    public static KvTablet createLazySentinel(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            @Nullable TabletServerMetricGroup serverMetricGroup) {
        return new KvTablet(physicalPath, tableBucket, logTablet, serverMetricGroup);
    }

    /**
     * Install RocksDB state from a fully-constructed KvTablet into this lazy sentinel. Called by
     * commitOpenResult() after a successful open. The source tablet is consumed (its RocksDB
     * reference is transferred, not copied).
     */
    void installRocksDB(KvTablet source) {
        this.kvTabletDir = source.kvTabletDir;
        this.rocksDBKv = source.rocksDBKv;
        this.writeBatchSize = source.writeBatchSize;
        this.kvPreWriteBuffer = source.kvPreWriteBuffer;
        this.logFormat = source.logFormat;
        this.arrowWriterProvider = source.arrowWriterProvider;
        this.memorySegmentPool = source.memorySegmentPool;
        this.kvFormat = source.kvFormat;
        this.rowMerger = source.rowMerger;
        this.overwriteRowMerger = source.overwriteRowMerger;
        this.arrowCompressionInfo = source.arrowCompressionInfo;
        this.schemaGetter = source.schemaGetter;
        this.changelogImage = source.changelogImage;
        this.rocksDBStatistics = source.rocksDBStatistics;
        this.autoIncrementManager = source.autoIncrementManager;
        this.rowCount = source.rowCount;
        this.flushedLogOffset = source.flushedLogOffset;
        this.isClosed = false;
    }

    /**
     * Detach RocksDB state from this sentinel (during release). Nulls out RocksDB-related fields so
     * the sentinel returns to a lightweight state.
     */
    void detachRocksDB() {
        this.kvTabletDir = null;
        this.rocksDBKv = null;
        this.kvPreWriteBuffer = null;
        this.rocksDBStatistics = null;
        this.arrowWriterProvider = null;
        this.memorySegmentPool = null;
        this.rowMerger = null;
        this.overwriteRowMerger = null;
        this.autoIncrementManager = null;
        this.schemaGetter = null;
        this.arrowCompressionInfo = null;
        this.isClosed = true;
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
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager)
            throws IOException {
        RocksDBKv kv = buildRocksDBKv(serverConf, kvTabletDir, sharedRateLimiter);

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
                rocksDBStatistics,
                autoIncrementManager);
    }

    private static RocksDBKv buildRocksDBKv(
            Configuration configuration, File kvDir, RateLimiter sharedRateLimiter)
            throws IOException {
        // Enable statistics to support RocksDB statistics collection
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(configuration, kvDir, true, sharedRateLimiter);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        kvDir,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());
        return rocksDBKvBuilder.build();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
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

    /** Returns the log offset up to which data has been flushed from pre-write buffer into KV. */
    public long getFlushedLogOffset() {
        return flushedLogOffset;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        // In LAZY state, RocksDB is not open — return cached value
        if (lazyState == LazyState.LAZY || lazyState == LazyState.FAILED) {
            if (cachedRowCount == ROW_COUNT_DISABLED) {
                throw new InvalidTableException(
                        String.format(
                                "Row count is disabled for this table '%s'. "
                                        + "This usually happens when the table is "
                                        + "created before v0.9 or the changelog image is set to WAL.",
                                getTablePath()));
            }
            return cachedRowCount;
        }
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
        return inWriteLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();

                    SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
                    Schema latestSchema = schemaInfo.getSchema();
                    short latestSchemaId = (short) schemaInfo.getSchemaId();
                    validateSchemaId(kvRecords.schemaId(), latestSchemaId);

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
                    WalBuilder walBuilder = createWalBuilder(latestSchemaId, latestRowType);
                    walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
                    // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
                    // only has fewer ending columns than latest schema, so we pad nulls to
                    // the end of the BinaryRow to get the latest schema row.
                    PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
                    // get offset to track the offset corresponded to the kv record
                    long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();

                    try {
                        processKvRecords(
                                kvRecords,
                                kvRecords.schemaId(),
                                currentMerger,
                                currentAutoIncrementUpdater,
                                walBuilder,
                                latestSchemaRow,
                                logEndOffsetOfPrevBatch);

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
                        LogAppendInfo logAppendInfo = logTablet.appendAsLeader(walBuilder.build());

                        // if the batch is duplicated, we should truncate the kvPreWriteBuffer
                        // already written.
                        if (logAppendInfo.duplicated()) {
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
                        kvPreWriteBuffer.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.ERROR);
                        throw t;
                    } finally {
                        // deallocate the memory and arrow writer used by the wal builder
                        walBuilder.deallocate();
                    }
                });
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
            long startLogOffset)
            throws Exception {
        long logOffset = startLogOffset;

        // TODO: reuse the read context and decoder
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
            BinaryRow row = kvRecord.getRow();
            BinaryValue currentValue = row == null ? null : new BinaryValue(schemaIdOfNewData, row);

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

        // if newValue is null, it means the row should be deleted
        if (newValue == null) {
            return applyDelete(key, oldValue, walBuilder, latestSchemaRow, logOffset);
        } else {
            return applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
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
        // Optimization: IN WAL mode，when using DefaultRowMerger (full update, not partial update)
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
            return applyInsert(
                    key,
                    currentValue,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    autoIncrementUpdater);
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.merge(oldValue, currentValue);

        if (newValue == oldValue) {
            // no actual change, skip this record
            return logOffset;
        }

        return applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
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
        if (changelogImage == ChangelogImage.WAL) {
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset);
            return logOffset + 1;
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset + 1);
            return logOffset + 2;
        }
    }

    private WalBuilder createWalBuilder(int schemaId, RowType rowType) throws Exception {
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
                return new IndexWalBuilder(schemaId, memorySegmentPool);
            case COMPACTED:
                return new CompactedWalBuilder(schemaId, rowType, memorySegmentPool);
            case ARROW:
                return new ArrowWalBuilder(
                        schemaId,
                        arrowWriterProvider.getOrCreateWriter(
                                tableBucket.getTableId(),
                                schemaId,
                                // we don't limit size of the arrow batch, because all the
                                // changelogs should be in a single batch
                                Integer.MAX_VALUE,
                                rowType,
                                arrowCompressionInfo),
                        memorySegmentPool);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    public void flush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        // In lazy sentinel state, no RocksDB is open — nothing to flush
        if (kvPreWriteBuffer == null) {
            return;
        }
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
    KvPreWriteBuffer getKvPreWriteBuffer() {
        return kvPreWriteBuffer;
    }

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }

    // ========================================================================
    //  Lazy lifecycle management
    // ========================================================================

    /** Returns true if this tablet is in lazy mode (as opposed to eager/traditional mode). */
    public boolean isLazyMode() {
        return lazyState != LazyState.EAGER;
    }

    /** Returns true if this tablet is in lazy mode and currently OPEN (RocksDB loaded). */
    public boolean isLazyOpen() {
        return lazyState == LazyState.OPEN;
    }

    /** Returns the current lazy state. */
    public LazyState getLazyState() {
        return lazyState;
    }

    /**
     * Acquire a guard that prevents RocksDB from being released while held. In eager mode, returns
     * a no-op guard immediately. In lazy mode, ensures RocksDB is open (blocking if necessary) and
     * pins it.
     *
     * <p>Must be called OUTSIDE any Replica-level locks (e.g. leaderIsrUpdateLock) to avoid
     * blocking leader transitions during slow opens.
     */
    public Guard acquireGuard() {
        if (lazyState == LazyState.EAGER) {
            return Guard.NOOP;
        }

        // Fast path: try to pin without blocking
        Guard fast = tryAcquirePinAsGuard();
        if (fast != null) {
            return fast;
        }

        // Slow path: ensure open, then pin
        ensureOpen();
        // Between ensureOpen() returning and pin, a concurrent release could happen.
        // Retry up to 3 times.
        for (int attempt = 0; ; attempt++) {
            lazyStateLock.lock();
            try {
                if (lazyState == LazyState.OPEN && !rejectNewPins) {
                    activePins.incrementAndGet();
                    touchAccessTimestamp();
                    return new Guard(this);
                }
                if (lazyState == LazyState.CLOSED) {
                    throw new KvStorageException("KvTablet is closed for " + tableBucket);
                }
            } finally {
                lazyStateLock.unlock();
            }
            if (attempt >= 2) {
                throw new KvStorageException(
                        "Failed to pin KvTablet after open for " + tableBucket);
            }
            // Re-open and retry
            ensureOpen();
        }
    }

    /**
     * Fast-path pin attempt. Returns Guard if OPEN and accepting pins; null otherwise.
     *
     * <p>Memory ordering: we read {@code lazyState} before {@code rejectNewPins}. The release path
     * writes {@code rejectNewPins = true} before {@code lazyState = RELEASING} (both volatile).
     * This ensures that if we see OPEN, rejectNewPins has not yet been set.
     */
    @Nullable
    private Guard tryAcquirePinAsGuard() {
        if (lazyState == LazyState.OPEN && !rejectNewPins) {
            activePins.incrementAndGet();
            if (lazyState == LazyState.OPEN && !rejectNewPins) {
                touchAccessTimestamp();
                return new Guard(this);
            }
            decrementAndMaybeSignal();
        }
        return null;
    }

    /** Release a pin. Called by Guard.close(). */
    private void releasePin() {
        decrementAndMaybeSignal();
    }

    private void decrementAndMaybeSignal() {
        if (activePins.decrementAndGet() == 0) {
            if (rejectNewPins) {
                lazyStateLock.lock();
                try {
                    lazyStateChanged.signalAll();
                } finally {
                    lazyStateLock.unlock();
                }
            }
        }
    }

    private void touchAccessTimestamp() {
        long now = clock.milliseconds();
        if (now - lastAccessTimestamp > ACCESS_TIMESTAMP_GRANULARITY_MS) {
            lastAccessTimestamp = now;
        }
    }

    /**
     * Ensures RocksDB is open. If already OPEN, returns immediately. If LAZY or FAILED (past
     * cooldown), triggers a slow-path open. If OPENING or RELEASING, blocks until state changes.
     */
    private void ensureOpen() {
        lazyStateLock.lock();
        try {
            while (true) {
                switch (lazyState) {
                    case EAGER:
                    case OPEN:
                        return;

                    case OPENING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out for " + tableBucket);
                        }
                        continue;

                    case RELEASING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out waiting for release: " + tableBucket);
                        }
                        continue;

                    case FAILED:
                        long elapsed = clock.milliseconds() - failedTimestamp;
                        long cooldown = failedBackoff.backoff(failureCount);
                        if (elapsed < cooldown) {
                            throw new KvStorageException(
                                    "KvTablet open failed for "
                                            + tableBucket
                                            + ", remaining cooldown: "
                                            + (cooldown - elapsed)
                                            + " ms",
                                    lastFailureCause);
                        }
                        // Fall through to LAZY — cooldown expired, retry open.

                    case LAZY:
                        lazyState = LazyState.OPENING;
                        openGeneration++;
                        long myGeneration = openGeneration;
                        int myLeaderEpoch =
                                leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
                        int myBucketEpoch =
                                bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
                        boolean localDataExists = hasLocalData;
                        lazyStateLock.unlock();
                        try {
                            doSlowOpen(myGeneration, myLeaderEpoch, myBucketEpoch, localDataExists);
                        } catch (Throwable t) {
                            // doSlowOpen already called commitOpenResult() which transitions
                            // to FAILED, so no need to handle state transition here.
                            if (t instanceof RuntimeException) {
                                throw (RuntimeException) t;
                            }
                            throw new KvStorageException(
                                    "KvTablet open failed for " + tableBucket, t);
                        }
                        return;

                    case CLOSED:
                        throw new KvStorageException("KvTablet is closed for " + tableBucket);

                    default:
                        throw new IllegalStateException("Unexpected state: " + lazyState);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvStorageException("Interrupted waiting for KvTablet open: " + tableBucket);
        } finally {
            if (lazyStateLock.isHeldByCurrentThread()) {
                lazyStateLock.unlock();
            }
        }
    }

    private void doSlowOpen(
            long myGeneration, int myLeaderEpoch, int myBucketEpoch, boolean localDataExists)
            throws Exception {
        KvTablet newTablet = null;
        boolean committed = false;

        if (openSemaphore != null
                && !openSemaphore.tryAcquire(openTimeoutMs, TimeUnit.MILLISECONDS)) {
            throw new KvStorageException("KvTablet open semaphore timeout for " + tableBucket);
        }

        try {
            newTablet = openCallback.doOpen(localDataExists);
            committed =
                    commitOpenResult(myGeneration, myLeaderEpoch, myBucketEpoch, newTablet, null);
            if (!committed) {
                throw new CancellationException(
                        "open result fenced by concurrent epoch/generation change");
            }
        } catch (Exception e) {
            commitOpenResult(myGeneration, myLeaderEpoch, myBucketEpoch, null, e);
            throw e;
        } finally {
            if (openSemaphore != null) {
                openSemaphore.release();
            }
            if (!committed && newTablet != null) {
                try {
                    newTablet.close();
                } catch (Exception ignored) {
                }
                if (openRollbackCallback != null) {
                    try {
                        openRollbackCallback.run();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private boolean commitOpenResult(
            long myGeneration,
            int myLeaderEpoch,
            int myBucketEpoch,
            @Nullable KvTablet newTablet,
            @Nullable Exception error) {
        lazyStateLock.lock();
        try {
            if (openGeneration != myGeneration) {
                if (lazyState == LazyState.OPENING) {
                    transitionToFailed(
                            new CancellationException(
                                    "open superseded by generation " + openGeneration),
                            true);
                }
                return false;
            }

            if (error != null) {
                transitionToFailed(error, false);
                return false;
            }

            // Epoch fencing
            int currentLeaderEpoch =
                    leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
            int currentBucketEpoch =
                    bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
            if (currentLeaderEpoch != myLeaderEpoch || currentBucketEpoch != myBucketEpoch) {
                transitionToFailed(new CancellationException("epoch changed during open"), true);
                return false;
            }

            // Success — absorb RocksDB state from the newly opened tablet
            installRocksDB(newTablet);
            lazyState = LazyState.OPEN;
            failureCount = 0;
            lastAccessTimestamp = clock.milliseconds();
            rejectNewPins = false;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }

        // Invoke commit callback outside the lock to avoid blocking acquireGuard() callers
        if (commitCallback != null) {
            commitCallback.onOpenCommitted(this);
        }
        return true;
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionToFailed(Throwable cause, boolean resetCount) {
        lazyState = LazyState.FAILED;
        failedTimestamp = clock.milliseconds();
        if (resetCount) {
            failureCount = 0;
        } else {
            failureCount++;
        }
        lastFailureCause = cause;
        lazyStateChanged.signalAll();
    }

    // ---- Release logic (idle release by KvManager) ----

    /**
     * Release RocksDB resources: OPEN -> RELEASING -> LAZY. Caches metadata before close for
     * serving queries while in LAZY state.
     *
     * @return true if release succeeded, false if aborted
     */
    public boolean releaseKv() {
        // Pre-check state outside lock as a fast-path rejection
        if (lazyState != LazyState.OPEN) {
            return false;
        }

        lazyStateLock.lock();
        try {
            if (lazyState != LazyState.OPEN) {
                return false;
            }

            // Read logEndOffset under lock to avoid TOCTOU — new records could be
            // appended between an outside read and this check.
            long logEndOffset = logTablet.localLogEndOffset();
            if (flushedLogOffset < logEndOffset) {
                return false;
            }

            // Cache metadata before close
            cachedRowCount = rowCount;
            // Transition to RELEASING — write rejectNewPins before lazyState so that
            // tryAcquirePinAsGuard() sees the reject flag if it reads OPEN.
            rejectNewPins = true;
            lazyState = LazyState.RELEASING;

            if (!drainPins()) {
                LOG.warn("{} release drain timeout, aborting release", tableBucket);
                lazyState = LazyState.OPEN;
                rejectNewPins = false;
                lazyStateChanged.signalAll();
                return false;
            }
        } finally {
            lazyStateLock.unlock();
        }

        // All pins drained, safe to close
        boolean releaseSucceeded = false;
        try {
            if (releaseCallback != null) {
                releaseCallback.doRelease(this);
            }
            detachRocksDB();
            releaseSucceeded = true;
        } catch (Exception e) {
            LOG.warn("{} release callback failed, rolling back to OPEN", tableBucket, e);
        } finally {
            lazyStateLock.lock();
            try {
                if (releaseSucceeded) {
                    lazyState = LazyState.LAZY;
                    hasLocalData = true;
                } else {
                    lazyState = LazyState.OPEN;
                }
                rejectNewPins = false;
                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
        }
        return releaseSucceeded;
    }

    /**
     * Full-state cleanup for lazy mode: handles all states, waits for in-progress operations, then
     * transitions to CLOSED. Called by Replica.dropKv().
     */
    public void dropKvLazy() {
        lazyStateLock.lock();
        try {
            switch (lazyState) {
                case EAGER:
                case CLOSED:
                    break;

                case LAZY:
                case FAILED:
                    if (cleanLocalCallback != null) {
                        cleanLocalCallback.cleanLocalDirectory();
                    }
                    break;

                case OPENING:
                    openGeneration++;
                    awaitStateExit(LazyState.OPENING, openTimeoutMs * 2);
                    if (lazyState == LazyState.OPEN) {
                        rejectNewPins = true;
                        drainPinsForDrop();
                        doDropViaCallback();
                    } else if (lazyState != LazyState.OPENING) {
                        if (cleanLocalCallback != null) {
                            cleanLocalCallback.cleanLocalDirectory();
                        }
                    }
                    break;

                case OPEN:
                    rejectNewPins = true;
                    drainPinsForDrop();
                    doDropViaCallback();
                    break;

                case RELEASING:
                    awaitStateExit(LazyState.RELEASING, releaseDrainTimeoutMs * 2);
                    if (cleanLocalCallback != null) {
                        cleanLocalCallback.cleanLocalDirectory();
                    }
                    break;

                default:
                    throw new IllegalStateException("Unexpected state in dropKvLazy: " + lazyState);
            }

            lazyState = LazyState.CLOSED;
            hasLocalData = false;
            rejectNewPins = false;
            activePins.set(0);
            failureCount = 0;
            failedTimestamp = 0;
            lastFailureCause = null;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    private void drainPinsForDrop() {
        if (!drainPins()) {
            LOG.warn("{} drop drain timeout, force-resetting pins", tableBucket);
            activePins.set(0);
        }
    }

    private boolean drainPins() {
        long deadline = clock.milliseconds() + releaseDrainTimeoutMs;
        while (activePins.get() > 0) {
            long remaining = deadline - clock.milliseconds();
            if (remaining <= 0) {
                return false;
            }
            try {
                lazyStateChanged.await(remaining, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * Wait (under lazyStateLock) until {@code lazyState} is no longer {@code state}, or timeout.
     */
    private void awaitStateExit(LazyState state, long timeoutMs) {
        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (lazyState == state) {
            try {
                long remainNanos = deadlineNanos - clock.nanoseconds();
                if (remainNanos <= 0
                        || !lazyStateChanged.await(remainNanos, TimeUnit.NANOSECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doDropViaCallback() {
        if (dropCallback != null) {
            dropCallback.doDrop(this);
        }
    }

    /** Pre-check for idle release eligibility. */
    public boolean canRelease(long closeIdleIntervalMs, long nowMs) {
        if (lazyState != LazyState.OPEN) {
            return false;
        }
        if (activePins.get() > 0) {
            return false;
        }
        return nowMs - lastAccessTimestamp >= closeIdleIntervalMs;
    }

    // ---- Lazy mode initialization ----

    /** Configure lazy open parameters. Must be called before entering lazy mode. */
    public void configureLazyOpen(
            Clock clock,
            Semaphore openSemaphore,
            long openTimeoutMs,
            long failedBackoffBaseMs,
            long failedBackoffMaxMs) {
        this.clock = clock;
        this.openSemaphore = openSemaphore;
        this.openTimeoutMs = openTimeoutMs;
        this.failedBackoff = new ExponentialBackoff(failedBackoffBaseMs, 2, failedBackoffMaxMs, 0);
    }

    public void setOpenCallback(OpenCallback callback) {
        this.openCallback = callback;
    }

    public void setCommitCallback(OpenCommitCallback callback) {
        this.commitCallback = callback;
    }

    public void setDropCallback(DropCallback callback) {
        this.dropCallback = callback;
    }

    public void setReleaseCallback(ReleaseCallback callback) {
        this.releaseCallback = callback;
    }

    public void setCleanLocalCallback(CleanLocalCallback callback) {
        this.cleanLocalCallback = callback;
    }

    public void setOpenRollbackCallback(Runnable callback) {
        this.openRollbackCallback = callback;
    }

    public void setLeaderEpochSupplier(IntSupplier supplier) {
        this.leaderEpochSupplier = supplier;
    }

    public void setBucketEpochSupplier(IntSupplier supplier) {
        this.bucketEpochSupplier = supplier;
    }

    public void initCachedRowCount(long rowCount) {
        this.cachedRowCount = rowCount;
    }

    // ---- Lazy state queries ----

    public long getCachedRowCount() {
        return cachedRowCount;
    }

    public long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public int getActivePins() {
        return activePins.get();
    }

    // ---- Test helpers ----

    @VisibleForTesting
    void setLazyStateForTesting(LazyState state) {
        lazyStateLock.lock();
        try {
            this.lazyState = state;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    @VisibleForTesting
    void setLastAccessTimestampForTesting(long timestamp) {
        this.lastAccessTimestamp = timestamp;
    }

    @VisibleForTesting
    void setClockForTesting(Clock clock) {
        this.clock = clock;
    }
}
