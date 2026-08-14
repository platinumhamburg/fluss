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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.CorruptSnapshotException;
import org.apache.fluss.exception.DuplicateSequenceException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidTimestampException;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.server.log.LocalLog.SegmentDeletionReason;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;
import org.apache.fluss.utils.types.Either;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

import static org.apache.fluss.utils.FileUtils.flushFileIfExists;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A LogTablet which presents a unified view of local and tiered log segments.
 *
 * <p>LogTablet is a physical entity that is responsible for managing the log segments for a
 * particular table bucket.
 */
@ThreadSafe
public final class LogTablet {

    private static final Logger LOG = LoggerFactory.getLogger(LogTablet.class);
    private static final Predicate<WriterKey> RETAIN_ALL_PROGRESS_WRITERS = ignored -> true;

    // Configured local storage root that owns this tablet, for example /data-0.
    private final File dataDir;
    // Logical table/partition identity of this tablet.
    private final PhysicalTablePath physicalPath;

    // Concrete log tablet directory under dataDir, for example /data-0/db/table/log-0.
    @GuardedBy("lock")
    private final LocalLog localLog;

    private final int maxSegmentFileSize;
    private final long logFlushIntervalMessages;
    // A lock that guards all modifications to the localLog.
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final WriterStateManager writerStateManager;

    private final Scheduler scheduler;
    private final ScheduledFuture<?> writerExpireCheck;
    private final LogFormat logFormat;
    private volatile int tieredLogLocalSegments;
    private final Clock clock;
    private volatile AppendFaultInjector appendFaultInjector = AppendFaultInjector.NO_OP;
    private final boolean isChangeLog;
    private final long logTtlMs;
    private final TabletServerMetricGroup serverMetricGroup;

    @GuardedBy("lock")
    private volatile LogOffsetMetadata highWatermarkMetadata;

    /** The leader end offset snapshot when become leader. */
    private volatile long leaderEndOffsetSnapshot = -1L;

    // The minimum offset that should be retained in the local log. This is used to ensure that,
    // the offset of kv snapshot should be retained, otherwise, kv recovery will fail.
    private volatile long minRetainOffset;
    // tracking the log start offset in remote storage
    private volatile long remoteLogStartOffset = Long.MAX_VALUE;
    // tracking the log end offset in remote storage
    private volatile long remoteLogEndOffset = -1L;
    // tracking the log size in remote storage
    private volatile long remoteLogSize = 0;

    // tracking if the data lake enabled
    private volatile boolean isDataLakeEnabled = false;
    // tracking the log start/end offset in lakehouse storage
    private volatile long lakeTableSnapshotId = -1;
    // note: currently, for primary key table, the log start offset nerve be updated
    private volatile long lakeLogStartOffset = Long.MAX_VALUE;
    private volatile long lakeLogEndOffset = -1L;
    private volatile long lakeMaxTimestamp = -1;
    // Best-effort estimate under concurrent high-watermark and lake-progress updates.
    // Metric reads are allowed to observe transient intermediate states.
    private volatile long estimatedPendingStartTimeMs = -1L;

    private LogTablet(
            File dataDir,
            PhysicalTablePath physicalPath,
            LocalLog localLog,
            Configuration conf,
            Scheduler scheduler,
            WriterStateManager writerStateManager,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            long logTtlMs,
            boolean isChangelog,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup) {
        this.dataDir = dataDir;
        this.physicalPath = physicalPath;
        this.localLog = localLog;
        this.maxSegmentFileSize = (int) conf.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE).getBytes();
        this.logFlushIntervalMessages = conf.get(ConfigOptions.LOG_FLUSH_INTERVAL_MESSAGES);
        int writerExpirationCheckIntervalMs =
                (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_CHECK_INTERVAL).toMillis();
        this.writerStateManager = writerStateManager;
        this.highWatermarkMetadata = new LogOffsetMetadata(0L);
        this.logTtlMs = logTtlMs;

        this.scheduler = scheduler;
        // scheduler the writer expiration interval check.
        writerExpireCheck =
                scheduler.schedule(
                        "PeriodicWriterIdExpirationCheck",
                        () -> removeExpiredWriter(System.currentTimeMillis()),
                        writerExpirationCheckIntervalMs,
                        writerExpirationCheckIntervalMs);
        this.logFormat = logFormat;
        checkArgument(
                tieredLogLocalSegments > 0,
                "log segments to retain in local must be greater than 0");
        this.tieredLogLocalSegments = tieredLogLocalSegments;

        this.clock = clock;
        this.isChangeLog = isChangelog;
        this.serverMetricGroup = serverMetricGroup;
        // Default value to 0L for changelog to avoid cleaning up any segments in case of not
        // updating this value in time. Default value to Long.MAX_VALUE for normal log table,
        // as we don't need to retain logs for kv recovery.
        this.minRetainOffset = isChangelog ? 0L : Long.MAX_VALUE;
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalPath;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public boolean canFetchFromLakeLog(long fetchOffset) {
        // currently, if is change log, we can't fetch log from lakehouse as
        // since currently, we don't support client read changelog directly
        // todo: should support to read from changelog directly, so that
        // we can read changelog directly
        if (isChangeLog) {
            return false;
        }
        return lakeLogStartOffset <= fetchOffset && fetchOffset < lakeLogEndOffset;
    }

    public boolean canFetchFromRemoteLog(long fetchOffset) {
        return remoteLogStartOffset <= fetchOffset && fetchOffset < remoteLogEndOffset;
    }

    /** The available start offset of the log tablet, maybe on local log or remote log. */
    public long logStartOffset() {
        return Math.min(Math.min(localLogStartOffset(), remoteLogStartOffset), lakeLogStartOffset);
    }

    public long localLogStartOffset() {
        return localLog.getLocalLogStartOffset();
    }

    public long localLogEndOffset() {
        return localLog.getLocalLogEndOffset();
    }

    public long localMaxTimestamp() {
        return localLog.getLocalMaxTimestamp();
    }

    public LogSegment activeLogSegment() {
        synchronized (lock) {
            return localLog.getSegments().activeSegment();
        }
    }

    /** Returns the configured local data directory that owns this tablet. */
    public File getDataDir() {
        return dataDir;
    }

    /** Returns the concrete log tablet directory under the owning local data directory. */
    public File getLogDir() {
        return localLog.getLogTabletDir();
    }

    public long getRecoveryPoint() {
        return localLog.getRecoveryPoint();
    }

    public TableBucket getTableBucket() {
        return localLog.getTableBucket();
    }

    public long getRowCount() {
        return getHighWatermark() - logStartOffset();
    }

    public long getHighWatermark() {
        return highWatermarkMetadata.getMessageOffset();
    }

    public LogOffsetMetadata getLocalEndOffsetMetadata() {
        return localLog.getLocalLogEndOffsetMetadata();
    }

    public boolean isDataLakeEnabled() {
        return isDataLakeEnabled;
    }

    public long getLakeTableSnapshotId() {
        return lakeTableSnapshotId;
    }

    public long getLakeLogStartOffset() {
        return lakeLogStartOffset;
    }

    public long getLakeLogEndOffset() {
        return lakeLogEndOffset;
    }

    public long getLakeMaxTimestamp() {
        return lakeMaxTimestamp;
    }

    /**
     * Returns the timestamp lag between the latest local log record and the latest tiered lake log
     * record.
     */
    public long getTimestampLag() {
        return lakeMaxTimestamp < 0L ? -1L : localMaxTimestamp() - lakeMaxTimestamp;
    }

    /**
     * Returns the elapsed time, in milliseconds, since the oldest committed record in this bucket
     * became pending for lake tiering.
     *
     * <p>Returns 0 when there are no committed records pending lake tiering or when the pending
     * start time estimate is not initialized yet.
     */
    public long getPendingRecordsLag(long currentTimeMs) {
        if (estimatedPendingStartTimeMs < 0L || !hasPendingLakeTieringRecords()) {
            return 0L;
        }
        return Math.max(0L, currentTimeMs - estimatedPendingStartTimeMs);
    }

    /** Returns the estimated start time of the current pending lake-tiering backlog for tests. */
    @VisibleForTesting
    public long getEstimatedPendingStartTimeMs() {
        return estimatedPendingStartTimeMs;
    }

    public int getWriterIdCount() {
        return writerStateManager.writerIdCount();
    }

    public KvIdempotenceProtocol getWriterStateProtocol() {
        return writerStateManager.protocol();
    }

    public Map<Long, WriterStateEntry> activeWriters() {
        return writerStateManager.activeWriters();
    }

    public ScheduledFuture<?> writerExpireCheck() {
        return writerExpireCheck;
    }

    public LogFormat getLogFormat() {
        return logFormat;
    }

    public long getLeaderEndOffsetSnapshot() {
        return leaderEndOffsetSnapshot;
    }

    @VisibleForTesting
    public WriterStateManager writerStateManager() {
        return writerStateManager;
    }

    /** Finds committed WriterState which makes the supplied progress stale. */
    public Optional<WriterProgressStateEntry> findStaleProgressBatch(
            WriterKey writerKey, long progress) {
        synchronized (lock) {
            return writerStateManager.findStaleProgressBatch(writerKey, progress);
        }
    }

    /** Retires cumulative-progress WriterState while preserving the LogTablet lock boundary. */
    public void removeProgressWriters(Predicate<WriterKey> predicate) {
        synchronized (lock) {
            writerStateManager.removeProgressWriters(predicate);
        }
    }

    public static LogTablet create(
            File dataDir,
            PhysicalTablePath tablePath,
            File tabletDir,
            Configuration conf,
            TabletServerMetricGroup serverMetricGroup,
            long recoveryPoint,
            Scheduler scheduler,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            long logTtlMs,
            boolean isChangelog,
            Clock clock,
            boolean isCleanShutdown)
            throws Exception {
        return create(
                dataDir,
                tablePath,
                tabletDir,
                conf,
                serverMetricGroup,
                recoveryPoint,
                scheduler,
                logFormat,
                tieredLogLocalSegments,
                logTtlMs,
                isChangelog,
                clock,
                isCleanShutdown,
                KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE);
    }

    public static LogTablet create(
            File dataDir,
            PhysicalTablePath tablePath,
            File tabletDir,
            Configuration conf,
            TabletServerMetricGroup serverMetricGroup,
            long recoveryPoint,
            Scheduler scheduler,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            long logTtlMs,
            boolean isChangelog,
            Clock clock,
            boolean isCleanShutdown,
            KvIdempotenceProtocol protocol)
            throws Exception {
        // create the log directory if it doesn't exist
        Files.createDirectories(tabletDir.toPath());

        TableBucket tableBucket = FlussPaths.parseTabletDir(tabletDir).f1;
        LogSegments segments = new LogSegments(tableBucket);

        // writerStateManager to store and manager the writer id.
        WriterStateManager writerStateManager =
                new WriterStateManager(
                        tableBucket,
                        tabletDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis(),
                        protocol);

        LoadedLogOffsets offsets =
                new LogLoader(
                                tabletDir,
                                conf,
                                segments,
                                recoveryPoint,
                                logFormat,
                                writerStateManager,
                                isCleanShutdown,
                                serverMetricGroup.indexReplicationFailures())
                        .load();

        LocalLog log =
                new LocalLog(
                        tabletDir,
                        conf,
                        serverMetricGroup,
                        segments,
                        recoveryPoint,
                        offsets.getNextOffsetMetadata(),
                        tableBucket,
                        logFormat);

        return new LogTablet(
                dataDir,
                tablePath,
                log,
                conf,
                scheduler,
                writerStateManager,
                logFormat,
                tieredLogLocalSegments,
                logTtlMs,
                isChangelog,
                clock,
                serverMetricGroup);
    }

    @VisibleForTesting
    public static LogTablet create(
            File dataDir,
            PhysicalTablePath tablePath,
            File tabletDir,
            Configuration conf,
            TabletServerMetricGroup serverMetricGroup,
            long recoveryPoint,
            Scheduler scheduler,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            boolean isChangelog,
            Clock clock,
            boolean isCleanShutdown,
            KvIdempotenceProtocol protocol)
            throws Exception {
        TableConfig tableConfig = new TableConfig(new Configuration());
        return create(
                dataDir,
                tablePath,
                tabletDir,
                conf,
                serverMetricGroup,
                recoveryPoint,
                scheduler,
                logFormat,
                tieredLogLocalSegments,
                tableConfig.getLogTTLMs(),
                isChangelog,
                clock,
                isCleanShutdown,
                protocol);
    }

    @VisibleForTesting
    public static LogTablet create(
            File dataDir,
            PhysicalTablePath tablePath,
            File tabletDir,
            Configuration conf,
            TabletServerMetricGroup serverMetricGroup,
            long recoveryPoint,
            Scheduler scheduler,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            boolean isChangelog,
            Clock clock,
            boolean isCleanShutdown)
            throws Exception {
        TableConfig tableConfig = new TableConfig(new Configuration());
        return create(
                dataDir,
                tablePath,
                tabletDir,
                conf,
                serverMetricGroup,
                recoveryPoint,
                scheduler,
                logFormat,
                tieredLogLocalSegments,
                tableConfig.getLogTTLMs(),
                isChangelog,
                clock,
                isCleanShutdown);
    }

    /** Register metrics for this log tablet in the metric group. */
    public void registerMetrics(BucketMetricGroup bucketMetricGroup) {
        MetricGroup metricGroup = bucketMetricGroup.addGroup("log");
        metricGroup.gauge(
                MetricNames.LOG_NUM_SEGMENTS, () -> localLog.getSegments().numberOfSegments());
        metricGroup.gauge(MetricNames.LOG_START_OFFSET, localLog::getLocalLogStartOffset);
        metricGroup.gauge(MetricNames.LOG_END_OFFSET, localLog::getLocalLogEndOffset);
    }

    public long logSize() {
        return localLog.getSegments().sizeInBytes();
    }

    public long logicalStorageSize() {
        if (remoteLogEndOffset <= 0L) {
            return localLog.getSegments().sizeInBytes();
        } else {
            return localLog.getSegments().higherSegments(remoteLogEndOffset).stream()
                    .mapToLong(LogSegment::getSizeInBytes)
                    .reduce(remoteLogSize, Long::sum);
        }
    }

    public void updateLeaderEndOffsetSnapshot() {
        synchronized (lock) {
            LOG.info(
                    "Update leaderEndOffsetSnapshot to {} for tb {} while become leader",
                    localLogEndOffset(),
                    localLog.getTableBucket());
            leaderEndOffsetSnapshot = localLog.getLocalLogEndOffset();
        }
    }

    /**
     * Append this message set to the active segment of the local log, assigning offsets and Bucket
     * Leader Epochs.
     */
    public LogAppendInfo appendAsLeader(MemoryLogRecords records) throws Exception {
        return append(records, true, RETAIN_ALL_PROGRESS_WRITERS);
    }

    /** Append this message set to the active segment of the local log without assigning offsets. */
    public LogAppendInfo appendAsFollower(MemoryLogRecords records) throws Exception {
        return appendAsFollower(records, RETAIN_ALL_PROGRESS_WRITERS);
    }

    /**
     * Append this message set as a follower and retain matching progress WriterState after the WAL
     * append.
     */
    public LogAppendInfo appendAsFollower(
            MemoryLogRecords records, Predicate<WriterKey> retainWriterProgress) throws Exception {
        return append(records, false, Objects.requireNonNull(retainWriterProgress));
    }

    /** Read messages from the local log without projection or filter. */
    public FetchDataInfo read(
            long readOffset, int maxLength, FetchIsolation fetchIsolation, boolean minOneMessage)
            throws IOException {
        return read(readOffset, maxLength, fetchIsolation, minOneMessage, null, null);
    }

    /** Read messages from the local log with projection but without filter. */
    public FetchDataInfo read(
            long readOffset,
            int maxLength,
            FetchIsolation fetchIsolation,
            boolean minOneMessage,
            @Nullable FileLogProjection projection)
            throws IOException {
        return read(readOffset, maxLength, fetchIsolation, minOneMessage, projection, null);
    }

    /**
     * Read messages from the local log.
     *
     * @param readOffset the offset to start reading from
     * @param maxLength the maximum number of bytes to read
     * @param fetchIsolation the fetch isolation level
     * @param minOneMessage if true, at least one message is returned even if it exceeds maxLength
     * @param projection the column projection to apply, or null for no projection
     * @param filterContext the filter context for server-side filter pushdown, or null for no
     *     filtering
     */
    public FetchDataInfo read(
            long readOffset,
            int maxLength,
            FetchIsolation fetchIsolation,
            boolean minOneMessage,
            @Nullable FileLogProjection projection,
            @Nullable FilterContext filterContext)
            throws IOException {
        LogOffsetMetadata maxOffsetMetadata = null;
        if (fetchIsolation == FetchIsolation.LOG_END) {
            maxOffsetMetadata = localLog.getLocalLogEndOffsetMetadata();
        } else if (fetchIsolation == FetchIsolation.HIGH_WATERMARK) {
            maxOffsetMetadata = fetchHighWatermarkMetadata();
        }

        return localLog.read(
                readOffset, maxLength, minOneMessage, maxOffsetMetadata, projection, filterContext);
    }

    /**
     * Update the high watermark to a new offset. The new high watermark will be lowed bounded by
     * the log end offset.
     *
     * <p>This is intended to be called by the leader when initializing the high watermark.
     *
     * @param highWatermark the suggested new value for the high watermark.
     */
    public void updateHighWatermark(long highWatermark) {
        LogOffsetMetadata highWatermarkMetadata = new LogOffsetMetadata(highWatermark);
        LogOffsetMetadata endOffsetMetadata = localLog.getLocalLogEndOffsetMetadata();
        LogOffsetMetadata newHighWatermarkMetadata;
        if (highWatermarkMetadata.getMessageOffset() >= endOffsetMetadata.getMessageOffset()) {
            newHighWatermarkMetadata = endOffsetMetadata;
        } else {
            newHighWatermarkMetadata = highWatermarkMetadata;
        }
        updateHighWatermarkMetadata(newHighWatermarkMetadata);
    }

    private void updateHighWatermarkMetadata(LogOffsetMetadata newHighWatermark) {
        if (newHighWatermark.getMessageOffset() < 0) {
            throw new IllegalArgumentException("High watermark offset should be non-negative");
        }
        long previousHighWatermark = highWatermarkMetadata.getMessageOffset();
        synchronized (lock) {
            if (newHighWatermark.getMessageOffset() < highWatermarkMetadata.getMessageOffset()) {
                LOG.warn(
                        "Non-monotonic update of high watermark from {} to {} for bucket {}",
                        highWatermarkMetadata,
                        newHighWatermark,
                        localLog.getTableBucket());
            }
            highWatermarkMetadata = newHighWatermark;
            // TODO log offset listener to update log offset.
        }
        onHighWatermarkUpdated(previousHighWatermark, newHighWatermark.getMessageOffset());
        LOG.trace(
                "Setting high watermark {} for bucket {}",
                newHighWatermark,
                localLog.getTableBucket());
    }

    /**
     * Update the highWatermark to a new value if and only if it is larger than the old value. It is
     * an error to update to a value which is larger than the log end offset.
     *
     * <p>This method is intended to be used by the leader to update the highWatermark after
     * follower fetch offsets have been updated.
     */
    public Optional<LogOffsetMetadata> maybeIncrementHighWatermark(
            LogOffsetMetadata newHighWatermark) throws IOException {
        if (newHighWatermark.getMessageOffset() > localLogEndOffset()) {
            throw new IllegalArgumentException(
                    String.format(
                            "HighWatermark %s update exceeds current log end offset %s",
                            newHighWatermark, localLog.getLocalLogEndOffsetMetadata()));
        }
        synchronized (lock) {
            LogOffsetMetadata oldHighWatermark = fetchHighWatermarkMetadata();
            // Ensure that the highWatermark increases monotonically. We also update the
            // highWatermark when the new offset metadata is on a newer segment, which occurs
            // whenever the log is rolled to a new segment.
            if (oldHighWatermark.getMessageOffset() < newHighWatermark.getMessageOffset()
                    || (oldHighWatermark.getMessageOffset() == newHighWatermark.getMessageOffset()
                            && oldHighWatermark.onOlderSegment(newHighWatermark))) {
                updateHighWatermarkMetadata(newHighWatermark);
                return Optional.of(oldHighWatermark);
            } else {
                return Optional.empty();
            }
        }
    }

    public long lookupOffsetForTimestamp(long startTimestamp) throws IOException {
        long findOffset = localLog.lookupOffsetForTimestamp(startTimestamp);
        if (findOffset == -1L) {
            throw new InvalidTimestampException(
                    String.format(
                            "Lookup offset error for table bucket %s, "
                                    + "the fetch timestamp %s is larger than the max timestamp %s",
                            getTableBucket(), startTimestamp, localLog.getLocalMaxTimestamp()));
        }
        return findOffset;
    }

    public void updateRemoteLogStartOffset(long remoteLogStartOffset) {
        long prev = this.remoteLogStartOffset;
        if (prev == Long.MAX_VALUE || remoteLogStartOffset > prev) {
            this.remoteLogStartOffset = remoteLogStartOffset;
        }
    }

    public void updateRemoteLogSize(long remoteLogSize) {
        this.remoteLogSize = remoteLogSize;
    }

    public void updateRemoteLogEndOffset(long remoteLogEndOffset) {
        if (remoteLogEndOffset > this.remoteLogEndOffset) {
            this.remoteLogEndOffset = remoteLogEndOffset;

            // try to delete these segments already exist in remote storage.
            deleteSegmentsAlreadyExistsInRemote();
        }
    }

    public void updateMinRetainOffset(long minRetainOffset) {
        if (minRetainOffset > this.minRetainOffset) {
            this.minRetainOffset = minRetainOffset;

            // try to delete the old segments that are not needed.
            deleteSegmentsAlreadyExistsInRemote();
        }
    }

    public void updateIsDataLakeEnabled(boolean isDataLakeEnabled) {
        this.isDataLakeEnabled = isDataLakeEnabled;
    }

    public void updateTieredLogLocalSegments(int tieredLogLocalSegments) {
        this.tieredLogLocalSegments = tieredLogLocalSegments;
    }

    public int getTieredLogLocalSegments() {
        return tieredLogLocalSegments;
    }

    public void updateLakeTableSnapshotId(long snapshotId) {
        if (snapshotId > this.lakeTableSnapshotId) {
            this.lakeTableSnapshotId = snapshotId;
        }
    }

    public void updateLakeLogStartOffset(long lakeHouseLogStartOffset) {
        long prev = this.lakeLogStartOffset;
        if (prev == Long.MAX_VALUE || lakeHouseLogStartOffset > prev) {
            this.lakeLogStartOffset = lakeHouseLogStartOffset;
        }
    }

    public void updateLakeLogEndOffset(long lakeLogEndOffset) {
        if (lakeLogEndOffset > this.lakeLogEndOffset) {
            this.lakeLogEndOffset = lakeLogEndOffset;
            // Lake-tiering progress advanced via the end offset; re-estimate the pending start
            // time so the lag is corrected (and cleared once caught up) even when the lake max
            // timestamp is not updated in the same notification.
            onLakeProgressUpdated();
        }
    }

    public void updateLakeMaxTimestamp(long lakeMaxTimestamp) {
        if (lakeMaxTimestamp > this.lakeMaxTimestamp) {
            this.lakeMaxTimestamp = lakeMaxTimestamp;
            onLakeProgressUpdated();
        }
    }

    private void onHighWatermarkUpdated(long previousHighWatermark, long currentHighWatermark) {
        if (!isDataLakeEnabled) {
            return;
        }
        long firstPendingOffset = Math.max(lakeLogEndOffset, localLogStartOffset());
        boolean hadPendingBeforeUpdate = firstPendingOffset < previousHighWatermark;
        boolean hasPendingAfterUpdate = firstPendingOffset < currentHighWatermark;
        if (!hasPendingAfterUpdate) {
            clearPendingStartTime();
        } else if (!hadPendingBeforeUpdate) {
            markPendingStartTimeNow();
        }
    }

    private void onLakeProgressUpdated() {
        if (!hasPendingLakeTieringRecords()) {
            clearPendingStartTime();
            return;
        }
        long timestampLag = getTimestampLag();
        if (timestampLag < 0L) {
            return;
        }
        long candidatePendingStartTimeMs = Math.max(0L, clock.milliseconds() - timestampLag);
        advancePendingStartTime(candidatePendingStartTimeMs);
    }

    private boolean hasPendingLakeTieringRecords() {
        return Math.max(lakeLogEndOffset, localLogStartOffset()) < getHighWatermark();
    }

    private void clearPendingStartTime() {
        estimatedPendingStartTimeMs = -1L;
    }

    private void markPendingStartTimeNow() {
        estimatedPendingStartTimeMs = clock.milliseconds();
    }

    private void advancePendingStartTime(long candidatePendingStartTimeMs) {
        estimatedPendingStartTimeMs =
                Math.max(estimatedPendingStartTimeMs, candidatePendingStartTimeMs);
    }

    public void loadWriterSnapshot(long lastOffset) throws IOException {
        synchronized (lock) {
            rebuildWriterState(lastOffset, writerStateManager);
            updateHighWatermark(localLog.getLocalLogEndOffsetMetadata().getMessageOffset());
        }
    }

    public void deleteSegmentsAlreadyExistsInRemote() {
        deleteSegments(
                remoteLogEndOffset,
                SegmentDeletionReason.LOG_MOVE_TO_REMOTE,
                this::deletableRemoteSegments);
    }

    /** Deletes inactive local segments that have expired according to the table log TTL. */
    public void deleteExpiredSegments() {
        // A missing remote end offset can mean either that no segment has been uploaded or that
        // all remote segments have expired. In both cases, table.log.ttl remains authoritative for
        // local retention, while the high watermark and minRetainOffset still protect data that
        // cannot be deleted yet.
        long cleanupToOffset = remoteLogEndOffset == -1L ? getHighWatermark() : remoteLogEndOffset;
        deleteSegments(
                cleanupToOffset,
                SegmentDeletionReason.LOG_RETENTION,
                this::deletableExpiredSegments);
    }

    /**
     * Fully materialize and return an offset snapshot including segment position info. This method
     * will update the LogOffsetMetadata for the high watermark if they are message-only. Throws an
     * offset out of range error if the segment info cannot be loaded.
     */
    public LogOffsetSnapshot fetchOffsetSnapshot() throws IOException {
        LogOffsetMetadata highWatermark = fetchHighWatermarkMetadata();
        return new LogOffsetSnapshot(
                logStartOffset(),
                localLogStartOffset(),
                localLog.getLocalLogEndOffsetMetadata(),
                highWatermark);
    }

    private void deleteSegments(
            long cleanUpToOffset,
            SegmentDeletionReason reason,
            DeletableSegmentsFinder deletableSegmentsFinder) {
        // cache to local variables
        long localLogStartOffset = localLog.getLocalLogStartOffset();
        if (cleanUpToOffset < localLogStartOffset) {
            LOG.debug(
                    "Ignore the delete segments action for bucket {} while the input cleanUpToOffset {} "
                            + "is smaller than the current localLogStartOffset {}",
                    getTableBucket(),
                    cleanUpToOffset,
                    localLogStartOffset);
            return;
        }

        if (cleanUpToOffset > getHighWatermark()) {
            LOG.warn(
                    "Ignore the delete segments action for bucket {} while the input cleanUpToOffset {} "
                            + "is larger than the current highWatermark {}",
                    getTableBucket(),
                    cleanUpToOffset,
                    getHighWatermark());
            return;
        }

        try {
            // shouldn't clean up segments that will be used by kv recovery.
            long cleanupToOffset = Math.min(minRetainOffset, cleanUpToOffset);
            deleteOldSegments(cleanupToOffset, reason, deletableSegmentsFinder);
        } catch (IOException e) {
            LOG.error(
                    "Failed to delete the local log segments to cleanUpToOffset {} for table-bucket {}.",
                    cleanUpToOffset,
                    getTableBucket(),
                    e);
            // do not re-throw exception as it is not critical.
        }
    }

    /**
     * Get the offset and metadata for the current high watermark. If offset metadata is not known,
     * this will do a lookup in the index and cache the result.
     */
    LogOffsetMetadata fetchHighWatermarkMetadata() throws IOException {
        localLog.checkIfMemoryMappedBufferClosed();
        LogOffsetMetadata offsetMetadata = highWatermarkMetadata;
        if (offsetMetadata.messageOffsetOnly()) {
            synchronized (lock) {
                LogOffsetMetadata fullOffset = convertToOffsetMetadataOrThrow(getHighWatermark());
                updateHighWatermarkMetadata(fullOffset);
                return fullOffset;
            }
        } else {
            return offsetMetadata;
        }
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log. If the message
     * offset is out of range, throw an {@link LogOffsetOutOfRangeException}
     */
    private LogOffsetMetadata convertToOffsetMetadataOrThrow(long offset) throws IOException {
        return localLog.convertToOffsetMetadataOrThrow(offset);
    }

    /**
     * Append this message set to the active segment of the local log, rolling over to a fresh
     * segment if necessary.
     *
     * <p>This method will generally be responsible for assigning offsets to the messages, however
     * if the appendAsLeader=false flag is passed we will only check that the existing offsets are
     * valid.
     */
    private LogAppendInfo append(
            MemoryLogRecords records,
            boolean appendAsLeader,
            Predicate<WriterKey> retainWriterProgress)
            throws Exception {
        LogAppendInfo appendInfo = analyzeAndValidateRecords(records);

        // return if we have no valid records.
        if (appendInfo.shallowCount() == 0) {
            return appendInfo;
        }

        // trim any invalid bytes or partial messages before appending it to the on-disk log.
        MemoryLogRecords validRecords = trimInvalidBytes(records, appendInfo);

        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            if (appendAsLeader) {
                long offset = localLog.getLocalLogEndOffset();
                // assign offsets to the message set.
                appendInfo.setFirstOffset(offset);

                AssignResult result =
                        assignOffsetAndTimestamp(
                                validRecords,
                                offset,
                                Math.max(localLog.getLocalMaxTimestamp(), clock.milliseconds()));
                appendInfo.setLastOffset(result.lastOffset);
                appendInfo.setMaxTimestamp(result.maxTimestamp);
                appendInfo.setStartOffsetOfMaxTimestamp(result.startOffsetOfMaxTimestampMs);
            } else {
                if (!appendInfo.offsetsMonotonic()) {
                    throw new FlussRuntimeException("Out of order offsets found.");
                }
            }

            Collection<WriterAppendInfo> updatedWriters = Collections.emptyList();
            Collection<WriterProgressAppendInfo> updatedProgressWriters = Collections.emptyList();
            if (writerStateManager.protocol() == KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE) {
                // Preserve the compact protocol's existing roll-before-duplicate-check behavior.
                maybeRoll(validRecords.sizeInBytes(), appendInfo);
                Either<WriterStateEntry.BatchMetadata, Collection<WriterAppendInfo>>
                        validateResult =
                                analyzeAndValidateWriterState(validRecords, appendAsLeader);
                if (validateResult.isRight()) {
                    updatedWriters = validateResult.right();
                } else {
                    // have duplicated batch metadata, skip the append and update append info.
                    WriterStateEntry.BatchMetadata duplicatedBatch = validateResult.left();
                    long startOffset = duplicatedBatch.firstOffset();
                    if (appendAsLeader) {
                        appendInfo.setFirstOffset(startOffset);
                        appendInfo.setLastOffset(duplicatedBatch.lastOffset);
                        appendInfo.setMaxTimestamp(duplicatedBatch.timestamp);
                        appendInfo.setStartOffsetOfMaxTimestamp(startOffset);
                        appendInfo.setDuplicated(true);
                    } else {
                        String errorMsg =
                                String.format(
                                        "Found duplicated batch for table bucket %s, duplicated offset is %s, "
                                                + "writer id is %s and batch sequence is: %s",
                                        getTableBucket(),
                                        duplicatedBatch.lastOffset,
                                        duplicatedBatch.writerId,
                                        duplicatedBatch.batchSequence);
                        LOG.error(errorMsg);
                        throw new DuplicateSequenceException(errorMsg);
                    }
                    return appendInfo;
                }
            } else {
                ProgressValidationResult validateResult =
                        analyzeAndValidateWriterProgress(validRecords);
                if (validateResult.allStale()) {
                    if (appendAsLeader) {
                        return LogAppendInfo.duplicatedAt(
                                validateResult.requiredWalOffset(),
                                validateResult.requiredTimestamp());
                    }
                    throw new DuplicateSequenceException(
                            String.format(
                                    "Found an all-stale writer progress append for table bucket %s; required WAL offset is %s",
                                    getTableBucket(), validateResult.requiredWalOffset()));
                }
                updatedProgressWriters = validateResult.updates();
                // A stale progress batch returned above without rolling or mutating the WAL.
                maybeRoll(validRecords.sizeInBytes(), appendInfo);
            }

            // Publish WriterState only after the corresponding WAL append succeeds.
            appendFaultInjector.inject(AppendPhase.BEFORE_LOCAL_APPEND);
            localLog.append(
                    appendInfo.lastOffset(),
                    appendInfo.maxTimestamp(),
                    appendInfo.startOffsetOfMaxTimestamp(),
                    validRecords);
            appendFaultInjector.inject(AppendPhase.AFTER_LOCAL_APPEND);
            updateHighWatermarkWithLogEndOffset();
            updatedWriters.forEach(writerStateManager::update);
            updatedProgressWriters.stream()
                    .filter(update -> retainWriterProgress.test(update.writerKey()))
                    .forEach(writerStateManager::updateProgress);
            if (writerStateManager.protocol() == KvIdempotenceProtocol.CUMULATIVE_PROGRESS) {
                appendFaultInjector.inject(AppendPhase.AFTER_WRITER_STATE_UPDATE);
            }
            writerStateManager.updateMapEndOffset(appendInfo.lastOffset() + 1);

            // todo update the first unstable offset (which is used to compute lso)

            LOG.trace(
                    "Appended message set with last offset: {}, first offset {}, next offset: {} "
                            + "and messages {} for bucket {}",
                    appendInfo.lastOffset(),
                    appendInfo.firstOffset(),
                    localLog.getLocalLogEndOffset(),
                    validRecords,
                    getTableBucket());

            if (localLog.unflushedMessages() >= logFlushIntervalMessages) {
                flush(false);
            }
            return appendInfo;
        }
    }

    void setAppendFaultInjector(AppendFaultInjector appendFaultInjector) {
        this.appendFaultInjector = appendFaultInjector;
    }

    enum AppendPhase {
        BEFORE_LOCAL_APPEND,
        AFTER_LOCAL_APPEND,
        AFTER_WRITER_STATE_UPDATE,
        DURING_WRITER_RECOVERY
    }

    interface AppendFaultInjector {
        AppendFaultInjector NO_OP = phase -> {};

        void inject(AppendPhase phase) throws Exception;
    }

    private void updateHighWatermarkWithLogEndOffset() {
        // Update the high watermark in case it has gotten ahead of the log end offset following a
        // truncation or if a new segment has been rolled and the offset metadata needs to be
        // updated.
        if (getHighWatermark() >= localLog.getLocalLogEndOffset()) {
            updateHighWatermarkMetadata(localLog.getLocalLogEndOffsetMetadata());
        }
    }

    private AssignResult assignOffsetAndTimestamp(
            MemoryLogRecords records, long baseLogOffset, long commitTimestamp) {
        long initialOffset = baseLogOffset;
        for (LogRecordBatch batch : records.batches()) {
            if (batch instanceof DefaultLogRecordBatch) {
                DefaultLogRecordBatch defaultLogRecordBatch = (DefaultLogRecordBatch) batch;
                defaultLogRecordBatch.setBaseLogOffset(initialOffset);
                defaultLogRecordBatch.setCommitTimestamp(commitTimestamp);
            } else {
                throw new FlussRuntimeException(
                        "Currently, we only support DefaultLogRecordBatch.");
            }

            initialOffset = batch.nextLogOffset();
        }

        return new AssignResult(initialOffset - 1, commitTimestamp, baseLogOffset);
    }

    /** Flush all local log segments. */
    public void flush(boolean forceFlushActiveSegment) throws IOException {
        flush(localLog.getLocalLogEndOffset(), forceFlushActiveSegment);
    }

    /**
     * Flush local log segments for all offsets up to offset - 1.
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    void flushUptoOffsetExclusive(long offset) {
        try {
            flush(offset, false);
        } catch (IOException e) {
            throw new LogStorageException(e);
        }
    }

    /**
     * Flush local log segments for all offsets up to offset-1 if includingOffset=false; up to
     * offset if includingOffset=true. The recovery point is set to offset.
     */
    private void flush(long offset, boolean includingOffset) throws IOException {
        long flushOffset = includingOffset ? offset + 1 : offset;
        String includingOffsetStr = includingOffset ? "inclusive" : "exclusive";

        if (flushOffset > localLog.getRecoveryPoint()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Flushing log up to offset {} ({}) with recovery point {}, unflushed: {}, for bucket {}",
                        offset,
                        includingOffsetStr,
                        flushOffset,
                        localLog.unflushedMessages(),
                        getTableBucket());
            }

            localLog.flush(flushOffset);

            synchronized (lock) {
                localLog.markFlushed(offset);
            }
        }
    }

    private void maybeRoll(int messageSize, LogAppendInfo appendInfo) throws Exception {
        synchronized (lock) {
            LogSegment segment = localLog.getSegments().activeSegment();

            if (segment.shouldRoll(
                    new RollParams(maxSegmentFileSize, appendInfo.lastOffset(), messageSize))) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Rolling new log segment for bucket {} (log_size = {}/{}), offset_index_size = {}/{}, "
                                    + "time_index_size = {}/{}",
                            getTableBucket(),
                            segment.getSizeInBytes(),
                            maxSegmentFileSize,
                            segment.offsetIndex().entries(),
                            segment.offsetIndex().maxEntries(),
                            segment.timeIndex().entries(),
                            segment.timeIndex().maxEntries());
                }

                roll(Optional.of(appendInfo.firstOffset()));
            }
        }
    }

    /**
     * Roll the local log over to a new active segment starting with the expectedNextOffset (when
     * provided), or localLog.logEndOffset otherwise. This will trim the index to the exact size of
     * the number of entries it currently contains.
     */
    @VisibleForTesting
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public void roll(Optional<Long> expectedNextOffset) throws Exception {
        synchronized (lock) {
            LogSegment segment = localLog.roll(expectedNextOffset);
            // Take a snapshot of the writer state to facilitate recovery. It is useful to have
            // the snapshot offset align with the new segment offset since this ensures we can
            // recover the segment by beginning with the corresponding snapshot file and scanning
            // the segment data. Because the segment base offset may actually be ahead of the
            // current writer state end offset (which corresponds to the log end offset),
            // we manually override the state offset here prior to taking the snapshot.
            writerStateManager.updateMapEndOffset(segment.getBaseOffset());
            writerStateManager.takeSnapshot();
            updateHighWatermarkWithLogEndOffset();

            scheduler.scheduleOnce(
                    "flush-log",
                    () -> {
                        flushUptoOffsetExclusive(segment.getBaseOffset());
                    });
        }
    }

    /** Truncate this log so that it ends with the greatest offset < targetOffset. */
    boolean truncateTo(long targetOffset) throws LogStorageException {
        if (targetOffset < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot truncate bucket %s to a negative offset %s",
                            getTableBucket(), targetOffset));
        }

        if (targetOffset >= localLog.getLocalLogEndOffset()) {
            LOG.info(
                    "Truncate to {} for bucket {} has no effect as the largest offset in the log is {}.",
                    targetOffset,
                    getTableBucket(),
                    localLog.getLocalLogEndOffset() - 1);
            return false;
        } else {
            LOG.info("Truncating to offset {} for bucket {}", targetOffset, getTableBucket());
            synchronized (lock) {
                try {
                    localLog.checkIfMemoryMappedBufferClosed();
                    if (localLog.getSegments()
                            .firstSegmentBaseOffset()
                            .filter(offset -> offset > targetOffset)
                            .isPresent()) {
                        truncateFullyAndStartAt(targetOffset);
                    } else {
                        List<LogSegment> deletedSegments = localLog.truncateTo(targetOffset);

                        deleteWriterSnapshots(deletedSegments, writerStateManager);
                        rebuildWriterState(targetOffset, writerStateManager);

                        if (getHighWatermark() >= localLog.getLocalLogEndOffset()) {
                            updateHighWatermark(localLog.getLocalLogEndOffset());
                        }
                    }

                    return true;
                } catch (IOException e) {
                    throw new LogStorageException(
                            String.format(
                                    "Error while truncating log for bucket %s to offset %s.",
                                    getTableBucket(), targetOffset),
                            e);
                }
            }
        }
    }

    /** Delete all data in the log and start at the new offset. */
    void truncateFullyAndStartAt(long newOffset) throws LogStorageException {
        LOG.debug("Truncate and start at offset {} for bucket {}", newOffset, getTableBucket());
        synchronized (lock) {
            try {
                localLog.truncateFullyAndStartAt(newOffset);
                writerStateManager.truncateFullyAndStartAt(newOffset);
                rebuildWriterState(newOffset, writerStateManager);
                updateHighWatermark(localLog.getLocalLogEndOffset());
            } catch (IOException e) {
                throw new LogStorageException(
                        String.format(
                                "Error while truncating log for bucket %s to offset %s.",
                                getTableBucket(), newOffset),
                        e);
            }
        }
    }

    /** Prepare remote WriterState restore without rebuilding before its snapshot is downloaded. */
    public void prepareRemoteWriterStateRecovery(long newOffset) throws LogStorageException {
        synchronized (lock) {
            try {
                localLog.truncateFullyAndStartAt(newOffset);
                writerStateManager.truncateFullyAndStartAt(0L);
                updateHighWatermark(localLog.getLocalLogEndOffset());
            } catch (IOException e) {
                throw new LogStorageException(
                        String.format(
                                "Error while preparing remote WriterState recovery for bucket %s at offset %s.",
                                getTableBucket(), newOffset),
                        e);
            }
        }
    }

    /**
     * Completely delete the local log directory and all contents form the file system with no
     * delay.
     */
    void drop() {
        synchronized (lock) {
            try {
                localLog.checkIfMemoryMappedBufferClosed();
                writerExpireCheck.cancel(true);
                localLog.deleteAllSegments();
                localLog.deleteEmptyDir();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<LogSegment> logSegments(long from, long to) {
        synchronized (lock) {
            return localLog.getSegments().values(from, to);
        }
    }

    /** All the log segments in this log ordered from oldest to newest. */
    public List<LogSegment> logSegments() {
        synchronized (lock) {
            return localLog.getSegments().values();
        }
    }

    public void close() {
        LOG.debug("close log tablet for bucket {}", getTableBucket());
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            writerExpireCheck.cancel(true);
            try {
                writerStateManager.takeSnapshot();
            } catch (IOException e) {
                LOG.error("Error while taking writer snapshot for bucket {}.", getTableBucket(), e);
            }
            localLog.close();
        }
    }

    private MemoryLogRecords trimInvalidBytes(MemoryLogRecords records, LogAppendInfo info) {
        int validBytes = info.validBytes();
        if (validBytes < 0) {
            throw new CorruptRecordException(
                    String.format(
                            "Cannot append record batch with illegal length %s to log "
                                    + "for %s. A possible cause is a corrupted produce request.",
                            validBytes, localLog.getTableBucket()));
        }

        if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            // trim invalid bytes.
            return MemoryLogRecords.readableRecords(records, validBytes);
        }
    }

    private LogAppendInfo analyzeAndValidateRecords(MemoryLogRecords records) {
        int shallowMessageCount = 0;
        int validBytesCount = 0;
        long firstOffset = -1L;
        long lastOffset = -1L;
        long maxTimestamp = -1L;
        long startOffsetOfMaxTimestamp = -1L;
        boolean monotonic = true;
        boolean readFirstMessage = false;

        for (LogRecordBatch batch : records.batches()) {
            if (!readFirstMessage) {
                firstOffset = batch.baseLogOffset();
                readFirstMessage = true;
            }

            if (lastOffset >= batch.lastLogOffset()) {
                monotonic = false;
            }

            lastOffset = batch.lastLogOffset();

            int batchSize = batch.sizeInBytes();
            if (!batch.isValid()) {
                throw new CorruptRecordException(
                        String.format(
                                "Record is corrupt (stored crc = %s) in table bucket %s",
                                batch.checksum(), localLog.getTableBucket()));
            }

            long batchAppendTimestamp = batch.commitTimestamp();
            if (batchAppendTimestamp > maxTimestamp) {
                maxTimestamp = batchAppendTimestamp;
                startOffsetOfMaxTimestamp = batch.baseLogOffset();
            }

            shallowMessageCount += 1;
            validBytesCount += batchSize;
        }

        return new LogAppendInfo(
                firstOffset,
                lastOffset,
                maxTimestamp,
                startOffsetOfMaxTimestamp,
                shallowMessageCount,
                validBytesCount,
                monotonic);
    }

    /** Returns either the duplicated batch metadata (left) or the updated writers (right). */
    private Either<WriterStateEntry.BatchMetadata, Collection<WriterAppendInfo>>
            analyzeAndValidateWriterState(MemoryLogRecords records, boolean isAppendAsLeader) {
        Map<Long, WriterAppendInfo> updatedWriters = new HashMap<>();

        for (LogRecordBatch batch : records.batches()) {
            validateWalProtocol(batch, KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE);
            if (batch.hasWriterId()) {
                // if this is a write request, there will be up to 5 batches which could
                // have been duplicated. If we find a duplicate, we return the metadata of the
                // appended batch to the writer.
                Optional<WriterStateEntry> maybeLastEntry =
                        writerStateManager.lastEntry(batch.writerId());
                Optional<WriterStateEntry.BatchMetadata> duplicateBatch =
                        maybeLastEntry.flatMap(entry -> entry.findDuplicateBatch(batch));
                if (duplicateBatch.isPresent()) {
                    return Either.left(duplicateBatch.get());
                }

                // update write append info.
                updateWriterAppendInfo(writerStateManager, batch, updatedWriters, isAppendAsLeader);
            }
        }

        return Either.right(updatedWriters.values());
    }

    private ProgressValidationResult analyzeAndValidateWriterProgress(MemoryLogRecords records) {
        List<WriterProgressAppendInfo> updates = new ArrayList<>();
        Map<WriterKey, WriterProgressStateEntry> staged = new HashMap<>();
        boolean sawFresh = false;
        boolean sawCommittedStale = false;
        long requiredWalOffset = -1L;
        long requiredTimestamp = -1L;
        for (LogRecordBatch batch : records.batches()) {
            validateWalProtocol(batch, KvIdempotenceProtocol.CUMULATIVE_PROGRESS);
            WriterKey writerKey = batch.writerKey();
            WriterProgressStateEntry committed =
                    writerStateManager.lastProgressEntry(writerKey).orElse(null);
            WriterProgressStateEntry stagedCurrent = staged.get(writerKey);
            if (stagedCurrent != null && batch.writerProgress() <= stagedCurrent.lastProgress()) {
                throw progressOrderingError(
                        writerKey, batch.writerProgress(), stagedCurrent.lastProgress(), "staged");
            }
            if (committed != null && batch.writerProgress() <= committed.lastProgress()) {
                if (sawFresh) {
                    throw mixedProgressAppendError();
                }
                sawCommittedStale = true;
                if (committed.progressWalOffset() > requiredWalOffset) {
                    requiredWalOffset = committed.progressWalOffset();
                    requiredTimestamp = committed.lastTimestamp();
                }
                continue;
            }
            if (sawCommittedStale) {
                throw mixedProgressAppendError();
            }
            sawFresh = true;
            WriterProgressStateEntry current = stagedCurrent != null ? stagedCurrent : committed;
            WriterProgressAppendInfo update =
                    new WriterProgressAppendInfo(writerKey, getTableBucket(), current);
            update.append(batch.writerProgress(), batch.lastLogOffset(), batch.commitTimestamp());
            updates.add(update);
            staged.put(writerKey, update.updatedEntry());
        }
        return sawCommittedStale
                ? ProgressValidationResult.allStale(requiredWalOffset, requiredTimestamp)
                : ProgressValidationResult.allFresh(updates);
    }

    private void validateWalProtocol(LogRecordBatch batch, KvIdempotenceProtocol expectedProtocol) {
        if (batch.idempotenceProtocolVersion() != expectedProtocol.version()) {
            throw new CorruptRecordException(
                    String.format(
                            "Target WAL magic %s does not match table mode %s (value %s) for %s",
                            batch.magic(),
                            expectedProtocol,
                            expectedProtocol.version(),
                            getTableBucket()));
        }
    }

    private OutOfOrderSequenceException progressOrderingError(
            WriterKey writerKey, long incoming, long current, String stateKind) {
        return new OutOfOrderSequenceException(
                String.format(
                        "Non-increasing progress %s for writer %s in %s; %s progress is %s",
                        incoming, writerKey, getTableBucket(), stateKind, current));
    }

    private OutOfOrderSequenceException mixedProgressAppendError() {
        return new OutOfOrderSequenceException(
                "A cumulative-progress WAL append cannot mix committed-stale and fresh batches for "
                        + getTableBucket());
    }

    private static final class ProgressValidationResult {
        private final Collection<WriterProgressAppendInfo> updates;
        private final long requiredWalOffset;
        private final long requiredTimestamp;

        private ProgressValidationResult(
                Collection<WriterProgressAppendInfo> updates,
                long requiredWalOffset,
                long requiredTimestamp) {
            this.updates = updates;
            this.requiredWalOffset = requiredWalOffset;
            this.requiredTimestamp = requiredTimestamp;
        }

        private static ProgressValidationResult allFresh(
                Collection<WriterProgressAppendInfo> updates) {
            return new ProgressValidationResult(updates, -1L, -1L);
        }

        private static ProgressValidationResult allStale(long offset, long timestamp) {
            return new ProgressValidationResult(Collections.emptyList(), offset, timestamp);
        }

        private boolean allStale() {
            return requiredWalOffset >= 0L;
        }

        private Collection<WriterProgressAppendInfo> updates() {
            return updates;
        }

        private long requiredWalOffset() {
            return requiredWalOffset;
        }

        private long requiredTimestamp() {
            return requiredTimestamp;
        }
    }

    @VisibleForTesting
    public void removeExpiredWriter(long currentTimeMs) {
        synchronized (lock) {
            writerStateManager.removeExpiredWriters(currentTimeMs);
        }
    }

    /**
     * Rebuild writer state until lastOffset. This method may be called from the recovery code path,
     * and thus must be free of all side effects, i.e. it must not update any log-specific state.
     */
    private void rebuildWriterState(long lastOffset, WriterStateManager writerStateManager)
            throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            long retainedLogStartOffset =
                    writerStateManager.protocol() == KvIdempotenceProtocol.CUMULATIVE_PROGRESS
                            ? localLog.getSegments()
                                    .firstSegment()
                                    .map(LogSegment::getBaseOffset)
                                    .orElse(lastOffset)
                            : 0L;
            rebuildWriterState(
                    writerStateManager,
                    localLog.getSegments(),
                    retainedLogStartOffset,
                    lastOffset,
                    false,
                    appendFaultInjector,
                    serverMetricGroup.indexReplicationFailures());
        }
    }

    private void flushWriterStateSnapshot(Path snapshot) {
        try {
            flushFileIfExists(snapshot);
        } catch (IOException e) {
            throw new LogStorageException(
                    String.format(
                            "Error while flushing writer state snapshot %s for %s in dir %s",
                            snapshot, getTableBucket(), getLogDir().getParent()),
                    e);
        }
    }

    private void deleteOldSegments(
            long endOffset,
            SegmentDeletionReason reason,
            DeletableSegmentsFinder deletableSegmentsFinder)
            throws IOException {
        synchronized (lock) {
            List<LogSegment> deletableSegments = deletableSegmentsFinder.find(endOffset);
            if (!deletableSegments.isEmpty()) {
                deleteSegments(deletableSegments, reason);
            }
        }
    }

    /** Returns uploaded segments that exceed the configured local segment retention count. */
    private List<LogSegment> deletableRemoteSegments(long endOffset) {
        if (localLog.getSegments().isEmpty()) {
            return Collections.emptyList();
        }

        // TODO introduce the read reference count to avoid deleting segments while there are
        // readers is in progress.
        List<LogSegment> deletableSegments = new ArrayList<>();
        List<LogSegment> logSegments = localLog.getSegments().values();

        // ignore the segments configured to be retained
        for (int i = 0; i < logSegments.size() - tieredLogLocalSegments; i++) {
            if (logSegments.get(i + 1).getBaseOffset() <= endOffset) {
                deletableSegments.add(logSegments.get(i));
            } else {
                break;
            }
        }
        return deletableSegments;
    }

    /** Returns the contiguous prefix of inactive segments that has expired. */
    private List<LogSegment> deletableExpiredSegments(long endOffset) throws IOException {
        if (localLog.getSegments().isEmpty()) {
            return Collections.emptyList();
        }

        List<LogSegment> deletableSegments = new ArrayList<>();
        List<LogSegment> logSegments = localLog.getSegments().values();
        long now = clock.milliseconds();

        for (int i = 0; i < logSegments.size() - 1; i++) {
            if (logSegments.get(i + 1).getBaseOffset() > endOffset
                    || !isSegmentExpired(now, logSegments.get(i), logTtlMs)) {
                break;
            }
            deletableSegments.add(logSegments.get(i));
        }
        return deletableSegments;
    }

    @FunctionalInterface
    private interface DeletableSegmentsFinder {
        List<LogSegment> find(long endOffset) throws IOException;
    }

    private boolean isSegmentExpired(long now, LogSegment segment, long expirationTimeMs)
            throws IOException {
        if (expirationTimeMs <= 0L) {
            return false;
        }
        return now - segment.maxTimestampSoFar() > expirationTimeMs;
    }

    private void deleteSegments(List<LogSegment> deletableSegments, SegmentDeletionReason reason)
            throws IOException {
        localLog.checkIfMemoryMappedBufferClosed();
        localLog.removeAndDeleteSegments(deletableSegments, reason);
        deleteWriterSnapshots(deletableSegments, writerStateManager);
    }

    private static void updateWriterAppendInfo(
            WriterStateManager writerStateManager,
            LogRecordBatch batch,
            Map<Long, WriterAppendInfo> writers,
            boolean isAppendAsLeader) {
        long writerId = batch.writerId();
        // update writers.
        WriterAppendInfo appendInfo =
                writers.computeIfAbsent(writerId, id -> writerStateManager.prepareUpdate(writerId));
        appendInfo.append(
                batch,
                writerStateManager.isWriterInBatchExpired(System.currentTimeMillis(), batch),
                isAppendAsLeader);
    }

    static void rebuildWriterState(
            WriterStateManager writerStateManager,
            LogSegments segments,
            long logStartOffset,
            long lastOffset,
            boolean reloadFromCleanShutdown)
            throws IOException {
        rebuildWriterState(
                writerStateManager,
                segments,
                logStartOffset,
                lastOffset,
                reloadFromCleanShutdown,
                AppendFaultInjector.NO_OP,
                new SimpleCounter());
    }

    static void rebuildWriterState(
            WriterStateManager writerStateManager,
            LogSegments segments,
            long logStartOffset,
            long lastOffset,
            boolean reloadFromCleanShutdown,
            Counter recoveryCoverageFailures)
            throws IOException {
        rebuildWriterState(
                writerStateManager,
                segments,
                logStartOffset,
                lastOffset,
                reloadFromCleanShutdown,
                AppendFaultInjector.NO_OP,
                recoveryCoverageFailures);
    }

    private static void rebuildWriterState(
            WriterStateManager writerStateManager,
            LogSegments segments,
            long logStartOffset,
            long lastOffset,
            boolean reloadFromCleanShutdown,
            AppendFaultInjector recoveryFaultInjector,
            Counter recoveryCoverageFailures)
            throws IOException {
        List<Optional<Long>> offsetsToSnapshot = new ArrayList<>();
        if (!segments.isEmpty()) {
            long lastSegmentBaseOffset = segments.lastSegment().get().getBaseOffset();
            Optional<Long> nextLatestSegmentBaseOffset =
                    segments.lowerSegment(lastSegmentBaseOffset).map(LogSegment::getBaseOffset);
            offsetsToSnapshot.add(nextLatestSegmentBaseOffset);
            offsetsToSnapshot.add(Optional.of(lastSegmentBaseOffset));
            offsetsToSnapshot.add(Optional.of(lastOffset));
        } else {
            offsetsToSnapshot.add(Optional.of(lastOffset));
        }
        LOG.info(
                "Loading writer state for bucket {} till offset {}",
                segments.getTableBucket(),
                lastOffset);
        // We want to avoid unnecessary scanning of the log to build the writer state when the
        // tablet server is being upgraded. The basic idea is to use the absence of writer
        // snapshot files to detect the upgrade case, but we have to be careful not to assume too
        // much in the presence of tablet server failures. The most common upgrade cases in
        // which we expect to find no snapshots are the following:
        //
        // 1. The tablet server has been upgraded, the table is on the new message format, and we
        // had a clean shutdown.
        //
        // If we hit either of these cases, we skip writer state loading and write a new
        // snapshot at the log end offset (see below). The next time the log is reloaded, we will
        // load writer state using this snapshot (or later snapshots). Otherwise, if there is
        // no snapshot file, then we have to rebuild writer state from the first segment.
        if (writerStateManager.protocol() == KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE
                && !writerStateManager.latestSnapshotOffset().isPresent()
                && reloadFromCleanShutdown) {
            // To avoid an expensive scan through all the segments, we take empty snapshots from
            // the start of the last two segments and the last offset. This should avoid the full
            // scan in the case that the log needs truncation.
            for (Optional<Long> offset : offsetsToSnapshot) {
                if (offset.isPresent()) {
                    writerStateManager.updateMapEndOffset(offset.get());
                    writerStateManager.takeSnapshot();
                }
            }
        } else {
            LOG.info(
                    "Reloading from writer snapshot and rebuilding writer state for bucket {} from offset {}",
                    segments.getTableBucket(),
                    lastOffset);
            boolean isEmptyBeforeTruncation =
                    writerStateManager.isEmpty() && writerStateManager.mapEndOffset() >= lastOffset;
            long writerStateLoadStart = System.currentTimeMillis();
            if (writerStateManager.protocol() == KvIdempotenceProtocol.CUMULATIVE_PROGRESS) {
                try {
                    rebuildWriterProgressState(
                            writerStateManager,
                            segments,
                            logStartOffset,
                            lastOffset,
                            recoveryFaultInjector);
                } catch (CorruptSnapshotException | CorruptRecordException failure) {
                    recoveryCoverageFailures.inc();
                    throw failure;
                }
                writerStateManager.takeSnapshot();
                LOG.info(
                        "Writer state recovery took {} ms for bucket {} from offset {}",
                        System.currentTimeMillis() - writerStateLoadStart,
                        segments.getTableBucket(),
                        lastOffset);
                return;
            }

            writerStateManager.truncateAndReload(
                    logStartOffset, lastOffset, System.currentTimeMillis());
            long segmentRecoveryStart = System.currentTimeMillis();
            // Cumulative-progress recovery always scans the complete candidate range before
            // publishing it. The contiguous-sequence writer map retains the existing shortcut.
            if (lastOffset > writerStateManager.mapEndOffset() && !isEmptyBeforeTruncation) {
                reloadWriterStateFromLog(
                        writerStateManager,
                        segments,
                        logStartOffset,
                        lastOffset,
                        offsetsToSnapshot,
                        recoveryFaultInjector);
            }
            writerStateManager.updateMapEndOffset(lastOffset);
            writerStateManager.takeSnapshot();
            LOG.info(
                    "Writer state recovery took {} ms for snapshot load and {} ms for segment recovery for bucket {} from offset {}",
                    segmentRecoveryStart - writerStateLoadStart,
                    System.currentTimeMillis() - segmentRecoveryStart,
                    segments.getTableBucket(),
                    lastOffset);
        }
    }

    private static void rebuildWriterProgressState(
            WriterStateManager writerStateManager,
            LogSegments segments,
            long logStartOffset,
            long recoveryEndOffset,
            AppendFaultInjector recoveryFaultInjector)
            throws IOException {
        RuntimeException latestSemanticFailure = null;
        for (Optional<Long> snapshotOffset :
                writerStateManager.progressRecoveryCandidateOffsets(
                        logStartOffset, recoveryEndOffset)) {
            try {
                WriterStateManager candidate =
                        writerStateManager.progressRecoveryCandidate(
                                logStartOffset, recoveryEndOffset, snapshotOffset);
                reloadWriterStateFromLog(
                        candidate,
                        segments,
                        logStartOffset,
                        recoveryEndOffset,
                        Collections.emptyList(),
                        recoveryFaultInjector);
                candidate.validateRecoveryCoverage(logStartOffset, recoveryEndOffset);
                writerStateManager.publishProgressRecovery(candidate, recoveryEndOffset);
                return;
            } catch (CorruptSnapshotException | CorruptRecordException semanticFailure) {
                latestSemanticFailure = semanticFailure;
                LOG.warn(
                        "Ignoring invalid writer progress recovery candidate {} for bucket {}",
                        snapshotOffset,
                        segments.getTableBucket(),
                        semanticFailure);
            }
        }
        String failureDetail =
                latestSemanticFailure == null
                        ? "no snapshot is allowed because retained WAL starts at " + logStartOffset
                        : latestSemanticFailure.getMessage();
        throw new CorruptSnapshotException(
                String.format(
                        "No writer progress snapshot and retained WAL provide continuous recovery over [%d,%d): %s",
                        logStartOffset, recoveryEndOffset, failureDetail),
                latestSemanticFailure);
    }

    private static void reloadWriterStateFromLog(
            WriterStateManager recoveryStateManager,
            LogSegments segments,
            long logStartOffset,
            long recoveryEndOffset,
            List<Optional<Long>> offsetsToSnapshot,
            AppendFaultInjector recoveryFaultInjector)
            throws IOException {
        if (recoveryEndOffset <= recoveryStateManager.mapEndOffset()) {
            return;
        }

        Optional<LogSegment> segmentOfLastOffset = segments.floorSegment(recoveryEndOffset);
        List<LogSegment> segmentsList =
                segments.values(recoveryStateManager.mapEndOffset(), recoveryEndOffset);
        for (LogSegment segment : segmentsList) {
            long startOffset =
                    Math.max(
                            Math.max(segment.getBaseOffset(), recoveryStateManager.mapEndOffset()),
                            logStartOffset);
            if (recoveryStateManager.protocol() == KvIdempotenceProtocol.CUMULATIVE_PROGRESS) {
                if (startOffset > recoveryStateManager.mapEndOffset()) {
                    throw recoveryGap(recoveryStateManager, startOffset, recoveryEndOffset);
                }
            } else {
                recoveryStateManager.updateMapEndOffset(startOffset);
                if (offsetsToSnapshot.contains(Optional.of(segment.getBaseOffset()))) {
                    recoveryStateManager.takeSnapshot();
                }
            }

            int maxPosition = segment.getSizeInBytes();
            if (segmentOfLastOffset.isPresent() && segmentOfLastOffset.get() == segment) {
                FileLogRecords.LogOffsetPosition logOffsetPosition =
                        segment.translateOffset(recoveryEndOffset);
                if (logOffsetPosition != null) {
                    maxPosition = logOffsetPosition.position;
                }
            }

            FetchDataInfo fetchDataInfo =
                    segment.read(startOffset, Integer.MAX_VALUE, maxPosition, false);
            if (fetchDataInfo != null) {
                loadWritersFromRecords(
                        recoveryStateManager, fetchDataInfo.getRecords(), recoveryFaultInjector);
            }
        }
    }

    private static void loadWritersFromRecords(
            WriterStateManager writerStateManager,
            LogRecords records,
            AppendFaultInjector recoveryFaultInjector)
            throws IOException {
        if (writerStateManager.protocol() == KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE) {
            Map<Long, WriterAppendInfo> loadedWriters = new HashMap<>();
            for (LogRecordBatch batch : records.batches()) {
                if (batch.idempotenceProtocolVersion() != 0) {
                    throw new CorruptRecordException(
                            "Cumulative-progress target WAL found while recovering a contiguous-sequence table");
                }
                if (batch.hasWriterId()) {
                    updateWriterAppendInfo(writerStateManager, batch, loadedWriters, false);
                }
            }
            loadedWriters.values().forEach(writerStateManager::update);
        } else {
            for (LogRecordBatch batch : records.batches()) {
                batch.ensureValid();
                long expectedOffset = writerStateManager.mapEndOffset();
                if (batch.lastLogOffset() < expectedOffset) {
                    continue;
                }
                if (batch.baseLogOffset() != expectedOffset) {
                    throw recoveryGap(
                            writerStateManager, batch.baseLogOffset(), batch.nextLogOffset());
                }
                if (batch.idempotenceProtocolVersion() != 1) {
                    throw new CorruptRecordException(
                            "Contiguous-sequence target WAL found while recovering a cumulative-progress table");
                }
                Optional<WriterProgressStateEntry> stale =
                        writerStateManager.findStaleProgressBatch(
                                batch.writerKey(), batch.writerProgress());
                if (stale.isPresent()) {
                    throw new CorruptRecordException(
                            "Non-increasing writer progress found while recovering target WAL");
                }
                WriterProgressAppendInfo update =
                        writerStateManager.prepareProgressUpdate(batch.writerKey());
                update.append(
                        batch.writerProgress(), batch.lastLogOffset(), batch.commitTimestamp());
                writerStateManager.updateProgress(update);
                writerStateManager.updateMapEndOffset(batch.nextLogOffset());
                try {
                    recoveryFaultInjector.inject(AppendPhase.DURING_WRITER_RECOVERY);
                } catch (Error error) {
                    throw error;
                } catch (Exception failure) {
                    throw new IOException(
                            "Injected cumulative-progress WriterState recovery failure", failure);
                }
            }
        }
    }

    private static CorruptSnapshotException recoveryGap(
            WriterStateManager writerStateManager, long nextOffset, long recoveryEnd) {
        return new CorruptSnapshotException(
                String.format(
                        "Writer progress recovery has a WAL gap [%d,%d) before recovery end %d",
                        writerStateManager.mapEndOffset(), nextOffset, recoveryEnd));
    }

    public static void deleteWriterSnapshots(
            List<LogSegment> segments, WriterStateManager writerStateManager) throws IOException {
        for (LogSegment segment : segments) {
            writerStateManager.removeAndDeleteSnapshot(segment.getBaseOffset());
        }
    }

    private static class AssignResult {
        private final long lastOffset;
        private final long maxTimestamp;
        private final long startOffsetOfMaxTimestampMs;

        private AssignResult(long lastOffset, long maxTimestamp, long startOffsetOfMaxTimestampMs) {
            this.lastOffset = lastOffset;
            this.maxTimestamp = maxTimestamp;
            this.startOffsetOfMaxTimestampMs = startOffsetOfMaxTimestampMs;
        }
    }

    @VisibleForTesting
    public List<LogSegment> getSegments() {
        return localLog.getSegments().values();
    }

    @VisibleForTesting
    public long getMinRetainOffset() {
        return minRetainOffset;
    }
}
