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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LogSegmentOffsetOverflowException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.server.log.state.BucketStateManager;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Loader to load log segments. */
final class LogLoader {
    private static final Logger LOG = LoggerFactory.getLogger(LogLoader.class);

    private final File logTabletDir;
    private final Configuration conf;
    private final LogSegments logSegments;
    private final long recoveryPointCheckpoint;
    private final LogFormat logFormat;
    private final WriterStateManager writerStateManager;
    private final BucketStateManager bucketStateManager;
    private final boolean isCleanShutdown;

    public LogLoader(
            File logTabletDir,
            Configuration conf,
            LogSegments logSegments,
            long recoveryPointCheckpoint,
            LogFormat logFormat,
            WriterStateManager writerStateManager,
            BucketStateManager bucketStateManager,
            boolean isCleanShutdown) {
        this.logTabletDir = logTabletDir;
        this.conf = conf;
        this.logSegments = logSegments;
        this.recoveryPointCheckpoint = recoveryPointCheckpoint;
        this.logFormat = logFormat;
        this.writerStateManager = writerStateManager;
        this.bucketStateManager = bucketStateManager;
        this.isCleanShutdown = isCleanShutdown;
    }

    /**
     * Load the log segments from the log files on disk, and returns the components of the loaded
     * log.
     *
     * <p>In the context of the calling thread, this function does not need to convert IOException
     * to {@link LogStorageException} because it is only called before all logs are loaded.
     *
     * @return the offsets of the Log successfully loaded from disk
     */
    public LoadedLogOffsets load() throws IOException {
        // load all the log and index files.
        logSegments.close();
        logSegments.clear();
        loadSegmentFiles();
        long newRecoveryPoint;
        long nextOffset;
        Tuple2<Long, Long> result = recoverLog();
        newRecoveryPoint = result.f0;
        nextOffset = result.f1;

        // Any segment loading or recovery code must not use writerStateManager, so that we can
        // build the full state here from scratch.
        if (!writerStateManager.isEmpty()) {
            throw new IllegalStateException("Writer state must be empty during log initialization");
        }

        if (!logSegments.isEmpty() && newRecoveryPoint > 0) {
            List<Long> baseOffsets = logSegments.baseOffsets();
            NavigableMap<Long, SnapshotFile> snapshotFiles =
                    SnapshotUtils.loadStateSnapshots(logTabletDir);

            // Find the latest available snapshot that has a corresponding log segment
            Map.Entry<Long, SnapshotFile> latestAvailableSnapshot = null;
            for (Map.Entry<Long, SnapshotFile> entry : snapshotFiles.descendingMap().entrySet()) {
                long snapshotOffset = entry.getKey();
                // Find a snapshot whose offset is less than or equal to one of the segment base
                // offsets
                if (baseOffsets.contains(snapshotOffset)
                        || snapshotOffset < baseOffsets.get(0)
                        || (baseOffsets.size() > 0 && snapshotOffset <= newRecoveryPoint)) {
                    latestAvailableSnapshot = entry;
                    break;
                }
            }

            if (latestAvailableSnapshot != null) {
                LOG.info(
                        "Restoring bucket state from snapshot at offset {} for bucket {}",
                        latestAvailableSnapshot.getKey(),
                        logSegments.getTableBucket());
                bucketStateManager.restore(
                        latestAvailableSnapshot.getValue().file(),
                        latestAvailableSnapshot.getValue().offset);
            } else {
                LOG.info(
                        "No valid state snapshot found for bucket {}, starting with empty state",
                        logSegments.getTableBucket());
            }
        }

        // Reload all snapshots into the WriterStateManager cache, the intermediate
        // WriterStateManager used during log recovery may have deleted some files without the
        // LogLoader.writerStateManager instance witnessing the deletion.
        writerStateManager.removeStraySnapshots(logSegments.baseOffsets());

        // Also reload BucketStateManager snapshots to clean up stray files
        bucketStateManager.removeStraySnapshots(logSegments.baseOffsets());

        // TODO, Here, we use 0 as the logStartOffset passed into rebuildWriterState. The reason is
        // that the current implementation of logStartOffset in Fluss is not yet fully refined, and
        // there may be cases where logStartOffset is not updated. As a result, logStartOffset is
        // not yet reliable. Once the issue with correctly updating logStartOffset is resolved in
        // issue https://github.com/apache/fluss/issues/744, we can use logStartOffset here.
        // Additionally, using 0 versus using logStartOffset does not affect correctness—they both
        // can restore the complete WriterState. The only difference is that using logStartOffset
        // can potentially skip over more segments.
        LogTablet.rebuildWriterState(
                writerStateManager, logSegments, 0, nextOffset, isCleanShutdown);

        // Rebuild bucket state by replaying records from the last checkpoint
        rebuildBucketState(bucketStateManager, logSegments, 0, nextOffset, isCleanShutdown);

        LogSegment activeSegment = logSegments.lastSegment().get();
        activeSegment.resizeIndexes((int) conf.get(ConfigOptions.LOG_INDEX_FILE_SIZE).getBytes());
        return new LoadedLogOffsets(
                newRecoveryPoint,
                new LogOffsetMetadata(
                        nextOffset, activeSegment.getBaseOffset(), activeSegment.getSizeInBytes()));
    }

    /**
     * Rebuild bucket state by replaying state change logs from the last checkpoint.
     *
     * @param bucketStateManager the bucket state manager to rebuild
     * @param segments the log segments
     * @param logStartOffset the log start offset
     * @param lastOffset the last offset to rebuild up to
     * @param reloadFromCleanShutdown whether this is a clean shutdown
     */
    private void rebuildBucketState(
            BucketStateManager bucketStateManager,
            LogSegments segments,
            long logStartOffset,
            long lastOffset,
            boolean reloadFromCleanShutdown)
            throws IOException {
        if (segments.isEmpty()) {
            return;
        }

        LOG.info(
                "Loading bucket state for bucket {} till offset {}",
                segments.getTableBucket(),
                lastOffset);

        // If no snapshot exists and this is a clean shutdown, we don't need to do anything
        // State will be built up naturally as records are applied
        if (!bucketStateManager.latestSnapshotOffset().isPresent()) {
            if (reloadFromCleanShutdown) {
                LOG.info(
                        "No bucket state snapshot found for bucket {}, will build state from records",
                        segments.getTableBucket());
            }
            return;
        }

        // Load from the latest snapshot and replay any records after it
        LOG.info(
                "Reloading from bucket state snapshot and rebuilding state for bucket {}",
                segments.getTableBucket());

        long stateLoadStart = System.currentTimeMillis();
        bucketStateManager.truncateAndReload(logStartOffset, lastOffset);
        long segmentRecoveryStart = System.currentTimeMillis();

        // Replay records from the snapshot offset to the last offset
        long snapshotOffset = bucketStateManager.latestSnapshotOffset().orElse(logStartOffset);
        if (lastOffset > snapshotOffset) {
            List<LogSegment> segmentsList = segments.values(snapshotOffset, lastOffset);
            for (LogSegment segment : segmentsList) {
                long startOffset = Math.max(segment.getBaseOffset(), snapshotOffset);

                int maxPosition = segment.getSizeInBytes();
                Optional<LogSegment> segmentOfLastOffset = segments.floorSegment(lastOffset);
                if (segmentOfLastOffset.isPresent() && segmentOfLastOffset.get() == segment) {
                    FileLogRecords.LogOffsetPosition logOffsetPosition =
                            segment.translateOffset(lastOffset);
                    if (logOffsetPosition != null) {
                        maxPosition = logOffsetPosition.position;
                    }
                }

                FetchDataInfo fetchDataInfo =
                        segment.read(startOffset, Integer.MAX_VALUE, maxPosition, false);
                if (fetchDataInfo != null) {
                    loadStateFromRecords(bucketStateManager, fetchDataInfo.getRecords());
                }
            }
        }

        LOG.info(
                "Bucket state recovery took {} ms for snapshot load and {} ms for segment recovery for bucket {}",
                segmentRecoveryStart - stateLoadStart,
                System.currentTimeMillis() - segmentRecoveryStart,
                segments.getTableBucket());
    }

    /**
     * Load bucket state from records by applying state change logs.
     *
     * @param bucketStateManager the bucket state manager
     * @param records the log records to load state from
     */
    private void loadStateFromRecords(BucketStateManager bucketStateManager, LogRecords records) {
        for (LogRecordBatch batch : records.batches()) {
            batch.stateChangeLogs()
                    .ifPresent(
                            stateChangeLogs ->
                                    bucketStateManager.apply(
                                            batch.lastLogOffset(), stateChangeLogs.iters()));
        }
    }

    /**
     * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
     * active segment, and returns the updated recovery point and next offset after recovery.
     *
     * <p>This method does not need to convert IOException to {@link LogStorageException} because it
     * is only called before all logs are loaded.
     *
     * @return a tuple containing (newRecoveryPoint, nextOffset).
     * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset
     *     overflow
     */
    private Tuple2<Long, Long> recoverLog() throws IOException {
        // TODO truncate log to recover maybe unflush segments.
        if (logSegments.isEmpty()) {
            logSegments.add(LogSegment.open(logTabletDir, 0L, conf, logFormat));
        }
        long logEndOffset = logSegments.lastSegment().get().readNextOffset();
        return Tuple2.of(recoveryPointCheckpoint, logEndOffset);
    }

    /** Loads segments from disk into the provided segments. */
    private void loadSegmentFiles() throws IOException {
        File[] sortedFiles = logTabletDir.listFiles();
        if (sortedFiles != null) {
            Arrays.sort(sortedFiles, Comparator.comparing(File::getName));
            for (File file : sortedFiles) {
                if (file.isFile()) {
                    if (LocalLog.isIndexFile(file)) {
                        long offset = FlussPaths.offsetFromFile(file);
                        File logFile = FlussPaths.logFile(logTabletDir, offset);
                        if (!logFile.exists()) {
                            LOG.warn(
                                    "Found an orphaned index file {} for bucket {}, with no corresponding log file.",
                                    logSegments.getTableBucket(),
                                    file.getAbsolutePath());
                            Files.deleteIfExists(file.toPath());
                        }
                    } else if (LocalLog.isLogFile(file)) {
                        long baseOffset = FlussPaths.offsetFromFile(file);
                        LogSegment segment =
                                LogSegment.open(logTabletDir, baseOffset, conf, true, 0, logFormat);
                        logSegments.add(segment);
                    }
                }
            }
        }
    }
}
