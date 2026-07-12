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
import org.apache.fluss.exception.CorruptSnapshotException;
import org.apache.fluss.exception.UnknownWriterIdException;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.writerSnapshotFile;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Maintains a mapping from Writer Ids to metadata about the last appended entries (e.g. batch
 * sequence.)
 *
 * <p>The batch sequence is the last number successfully appended to the bucket for given
 * identifier.
 *
 * <p>As long as a writer id is contained in the map, the corresponding writer can continue to write
 * data. However, writer ids can be expired due to lack of recent use or if the last written entry
 * has been deleted from the log (e.g. if the retention policy is "delete").
 */
@NotThreadSafe
public class WriterStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriterStateManager.class);

    private final TableBucket tableBucket;
    private final int writerExpirationMs;
    private final KvIdempotenceProtocol protocol;
    @Nullable private final Map<Long, WriterStateEntry> writers;
    @Nullable private Map<WriterKey, FencedWriterStateEntry> fencedWriters;

    private final File logTabletDir;
    /** The selected protocol map size, available without acquiring the manager's owning lock. */
    private volatile int writerIdCount = 0;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;
    private long lastMapOffset = 0L;
    private long lastSnapOffset = 0L;
    @Nullable private Long loadedSnapshotOffset;

    public WriterStateManager(TableBucket tableBucket, File logTabletDir, int writerExpirationMs)
            throws IOException {
        this(tableBucket, logTabletDir, writerExpirationMs, KvIdempotenceProtocol.V0_COMPACT);
    }

    public WriterStateManager(
            TableBucket tableBucket,
            File logTabletDir,
            int writerExpirationMs,
            KvIdempotenceProtocol protocol)
            throws IOException {
        this.tableBucket = tableBucket;
        this.writerExpirationMs = writerExpirationMs;
        this.logTabletDir = logTabletDir;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.writers = protocol == KvIdempotenceProtocol.V0_COMPACT ? new HashMap<>() : null;
        this.fencedWriters = protocol == KvIdempotenceProtocol.V1_FENCED ? new HashMap<>() : null;
        this.snapshots = loadSnapshots();
    }

    public KvIdempotenceProtocol protocol() {
        return protocol;
    }

    public int writerExpirationMs() {
        return writerExpirationMs;
    }

    public int writerIdCount() {
        return writerIdCount;
    }

    /** Returns the last offset of this map. */
    public long mapEndOffset() {
        return lastMapOffset;
    }

    public void updateMapEndOffset(long lastOffset) {
        lastMapOffset = lastOffset;
    }

    /** Validate continuous V1 WriterState coverage over the half-open recovery range. */
    public void validateRecoveryCoverage(long logStartOffset, long recoveryEndOffset)
            throws CorruptSnapshotException {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        if (logStartOffset < 0L || recoveryEndOffset < logStartOffset) {
            throw new CorruptSnapshotException(
                    String.format(
                            "Invalid WriterState recovery range [%d,%d)",
                            logStartOffset, recoveryEndOffset));
        }
        if (loadedSnapshotOffset == null && logStartOffset > 0L) {
            throw new CorruptSnapshotException(
                    String.format(
                            "No V1 WriterState snapshot covers recovery end %d and retained WAL starts at %d",
                            recoveryEndOffset, logStartOffset));
        }
        if (loadedSnapshotOffset != null
                && (loadedSnapshotOffset < logStartOffset
                        || loadedSnapshotOffset > recoveryEndOffset)) {
            throw new CorruptSnapshotException(
                    String.format(
                            "V1 WriterState snapshot at %d does not cover retained WAL range [%d,%d)",
                            loadedSnapshotOffset, logStartOffset, recoveryEndOffset));
        }
        if (lastMapOffset != recoveryEndOffset) {
            throw new CorruptSnapshotException(
                    String.format(
                            "WriterState recovery ends at %d, not recovery end %d; WAL coverage has gap [%d,%d)",
                            lastMapOffset, recoveryEndOffset, lastMapOffset, recoveryEndOffset));
        }
    }

    /** Validate that a V1 snapshot is a complete proof for the exact exclusive end offset. */
    public static void validateFencedSnapshot(File snapshotFile, long expectedEndOffset) {
        if (expectedEndOffset < 0L) {
            throw new CorruptSnapshotException(
                    "Invalid V1 writer snapshot end offset " + expectedEndOffset);
        }
        Path expectedPath =
                writerSnapshotFile(snapshotFile.getParentFile(), expectedEndOffset)
                        .toPath()
                        .toAbsolutePath()
                        .normalize();
        Path actualPath = snapshotFile.toPath().toAbsolutePath().normalize();
        if (!actualPath.equals(expectedPath)) {
            throw new CorruptSnapshotException(
                    String.format(
                            "V1 writer snapshot %s does not prove exact end offset %d",
                            snapshotFile, expectedEndOffset));
        }

        for (FencedWriterStateEntry entry : readFencedSnapshot(snapshotFile)) {
            long targetWalOffset = entry.dominatingTargetWalOffset();
            if (targetWalOffset < 0L || targetWalOffset >= expectedEndOffset) {
                throw new CorruptSnapshotException(
                        String.format(
                                "V1 writer snapshot at %d contains target WAL offset %d outside [0,%d)",
                                expectedEndOffset, targetWalOffset, expectedEndOffset));
            }
        }
    }

    /** Get the last written entry for the given writer id. */
    public Optional<WriterStateEntry> lastEntry(long writerId) {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        return Optional.ofNullable(writers.get(writerId));
    }

    /** Get a copy of the active writers. */
    public Map<Long, WriterStateEntry> activeWriters() {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        return Collections.unmodifiableMap(writers);
    }

    public boolean isEmpty() {
        return protocol == KvIdempotenceProtocol.V0_COMPACT
                ? writers.isEmpty()
                : fencedWriters.isEmpty();
    }

    public void removeExpiredWriters(long currentTimeMs) {
        if (protocol == KvIdempotenceProtocol.V1_FENCED) {
            return;
        }
        List<Long> keys =
                writers.entrySet().stream()
                        .filter(entry -> isWriterExpired(currentTimeMs, entry.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        removeWriterIds(keys);
    }

    /**
     * Truncate the writer id mapping to the given offset range and reload the entries from the most
     * recent snapshot in range (if there is one). We delete snapshot files prior to the
     * logStartOffset but do not remove writer state from the map. This means that in-memory and
     * on-disk state can diverge, and in the case of tablet server failover or unclean shutdown, any
     * in-memory state not persisted in the snapshots will be lost, which would lead to {@link
     * UnknownWriterIdException} errors. Note that the log end offset is assumed to be less than or
     * equal to the high watermark.
     */
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs)
            throws IOException {
        if (protocol == KvIdempotenceProtocol.V1_FENCED) {
            truncateAndReloadFenced(logStartOffset, logEndOffset, true);
            return;
        }

        // remove all out of range snapshots.
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }

        if (logEndOffset != mapEndOffset()) {
            clearWriterIds();
            loadFromSnapshot(logStartOffset, currentTimeMs);
        } else {
            if (lastMapOffset < logStartOffset) {
                lastMapOffset = logStartOffset;
            }
            lastSnapOffset = latestSnapshotOffset().orElse(logStartOffset);
        }
    }

    WriterStateManager fencedRecoveryCandidate(long logStartOffset, long logEndOffset)
            throws IOException {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        CorruptSnapshotException latestFailure = null;
        for (Optional<Long> snapshotOffset :
                fencedRecoveryCandidateOffsets(logStartOffset, logEndOffset)) {
            try {
                return fencedRecoveryCandidate(logStartOffset, logEndOffset, snapshotOffset);
            } catch (CorruptSnapshotException failure) {
                if (latestFailure == null) {
                    latestFailure = failure;
                }
                snapshotOffset.ifPresent(
                        offset ->
                                LOG.warn(
                                        "Ignoring invalid V1 writer snapshot at {} while looking for a covering snapshot",
                                        offset,
                                        failure));
            }
        }
        throw noFencedRecoveryCandidate(logStartOffset, logEndOffset, latestFailure);
    }

    List<Optional<Long>> fencedRecoveryCandidateOffsets(long logStartOffset, long logEndOffset) {
        validateFencedRecoveryRange(logStartOffset, logEndOffset);
        List<Optional<Long>> candidateOffsets = new ArrayList<>();
        for (SnapshotFile snapshot :
                snapshots.headMap(logEndOffset, true).descendingMap().values()) {
            if (snapshot.offset < logStartOffset) {
                break;
            }
            candidateOffsets.add(Optional.of(snapshot.offset));
        }
        if (logStartOffset == 0L) {
            candidateOffsets.add(Optional.empty());
        }
        return candidateOffsets;
    }

    WriterStateManager fencedRecoveryCandidate(
            long logStartOffset, long logEndOffset, Optional<Long> snapshotOffset)
            throws IOException {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        WriterStateManager candidate =
                new WriterStateManager(tableBucket, logTabletDir, writerExpirationMs, protocol);
        candidate.loadFencedRecoveryCandidate(logStartOffset, logEndOffset, snapshotOffset);
        return candidate;
    }

    void publishFencedRecovery(WriterStateManager candidate, long recoveryEndOffset)
            throws IOException {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        candidate.requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        if (!tableBucket.equals(candidate.tableBucket)
                || !logTabletDir.equals(candidate.logTabletDir)
                || candidate.lastMapOffset != recoveryEndOffset) {
            throw new IllegalArgumentException("Invalid V1 WriterState recovery candidate");
        }
        for (SnapshotFile snapshot : snapshots.tailMap(recoveryEndOffset, false).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
        fencedWriters = new HashMap<>(candidate.fencedWriters);
        writerIdCount = fencedWriters.size();
        loadedSnapshotOffset = candidate.loadedSnapshotOffset;
        lastSnapOffset = candidate.lastSnapOffset;
        lastMapOffset = candidate.lastMapOffset;
    }

    private void truncateAndReloadFenced(
            long logStartOffset, long logEndOffset, boolean deleteFutureSnapshots)
            throws IOException {
        WriterStateManager candidate = fencedRecoveryCandidate(logStartOffset, logEndOffset);
        fencedWriters = new HashMap<>(candidate.fencedWriters);
        writerIdCount = fencedWriters.size();
        loadedSnapshotOffset = candidate.loadedSnapshotOffset;
        lastSnapOffset = candidate.lastSnapOffset;
        lastMapOffset = candidate.lastMapOffset;

        if (deleteFutureSnapshots) {
            for (SnapshotFile snapshot : snapshots.tailMap(logEndOffset, false).values()) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }
    }

    private void loadFencedRecoveryCandidate(
            long logStartOffset, long logEndOffset, Optional<Long> snapshotOffset) {
        validateFencedRecoveryRange(logStartOffset, logEndOffset);
        if (!snapshotOffset.isPresent()) {
            if (logStartOffset != 0L) {
                throw noFencedRecoveryCandidate(logStartOffset, logEndOffset, null);
            }
            fencedWriters = new HashMap<>();
            writerIdCount = 0;
            loadedSnapshotOffset = null;
            lastSnapOffset = 0L;
            lastMapOffset = 0L;
            return;
        }

        long offset = snapshotOffset.get();
        if (offset < logStartOffset || offset > logEndOffset) {
            throw new CorruptSnapshotException(
                    String.format(
                            "V1 WriterState snapshot at %d is outside recovery range [%d,%d)",
                            offset, logStartOffset, logEndOffset));
        }
        SnapshotFile snapshot = snapshots.get(offset);
        if (snapshot == null) {
            throw new CorruptSnapshotException("Missing V1 writer snapshot at " + offset);
        }

        LOG.info("Loading fenced writer state from snapshot file '{}'", snapshot);
        validateFencedSnapshot(snapshot.file(), offset);
        Map<WriterKey, FencedWriterStateEntry> candidateWriters = new HashMap<>();
        for (FencedWriterStateEntry entry : readFencedSnapshot(snapshot.file())) {
            FencedWriterStateEntry previous = candidateWriters.put(entry.writerKey(), entry);
            if (previous != null) {
                throw new CorruptSnapshotException(
                        "Duplicate fenced writer key in snapshot: " + entry.writerKey());
            }
        }
        fencedWriters = candidateWriters;
        writerIdCount = fencedWriters.size();
        loadedSnapshotOffset = offset;
        lastSnapOffset = offset;
        lastMapOffset = offset;
    }

    private static void validateFencedRecoveryRange(long logStartOffset, long logEndOffset) {
        if (logStartOffset < 0L || logEndOffset < logStartOffset) {
            throw new CorruptSnapshotException(
                    String.format(
                            "Invalid WriterState recovery range [%d,%d)",
                            logStartOffset, logEndOffset));
        }
    }

    private static CorruptSnapshotException noFencedRecoveryCandidate(
            long logStartOffset,
            long logEndOffset,
            @Nullable CorruptSnapshotException latestFailure) {
        return new CorruptSnapshotException(
                String.format(
                        "No snapshot can provide continuous WriterState recovery to recovery end %d because retained WAL starts at %d",
                        logEndOffset, logStartOffset),
                latestFailure);
    }

    public void truncateFullyAndStartAt(long offset) throws IOException {
        clearWriterIds();
        for (SnapshotFile snapshot : snapshots.values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
        lastSnapOffset = 0L;
        lastMapOffset = offset;
        loadedSnapshotOffset = null;
    }

    public void reloadSnapshots() throws IOException {
        LOG.info("Reloading the writer state snapshots");
        snapshots = loadSnapshots();
    }

    public void truncateFullyAndReloadSnapshots() throws IOException {
        LOG.info("Reloading the writer state snapshots");
        truncateFullyAndStartAt(0L);
        snapshots = loadSnapshots();
    }

    /**
     * Take a snapshot at the current end offset if one does not already exist with syncing the
     * change to the device.
     */
    public void takeSnapshot() throws IOException {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile =
                    new SnapshotFile(writerSnapshotFile(logTabletDir, lastMapOffset));
            long start = System.currentTimeMillis();
            if (protocol == KvIdempotenceProtocol.V0_COMPACT) {
                writeSnapshot(snapshotFile.file(), writers);
            } else {
                writeFencedSnapshot(snapshotFile.file(), fencedWriters);
            }
            LOG.info(
                    "Wrote writer snapshot at offset {} with {} producer ids for table bucket {} in {} ms.",
                    lastMapOffset,
                    writerIdCount,
                    tableBucket,
                    System.currentTimeMillis() - start);

            snapshots.put(snapshotFile.offset, snapshotFile);

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;
        }
    }

    /**
     * Deletes the writer snapshot files until the given offset (exclusive) in a thread safe manner.
     */
    @VisibleForTesting
    public void deleteSnapshotsBefore(long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
    }

    /** Fetch the snapshot file for the end offset of the log segment. */
    public Optional<File> fetchSnapshot(long offset) {
        return Optional.ofNullable(snapshots.get(offset)).map(SnapshotFile::file);
    }

    /** Returns the bytes retained by the latest WriterState snapshot file. */
    public long latestSnapshotBytes() {
        return latestSnapshotFile().map(snapshot -> snapshot.file().length()).orElse(0L);
    }

    public WriterAppendInfo prepareUpdate(long writerId) {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        WriterStateEntry currentEntry =
                lastEntry(writerId).orElse(WriterStateEntry.empty(writerId));
        return new WriterAppendInfo(writerId, tableBucket, currentEntry);
    }

    /** Update the mapping with the given append information. */
    public void update(WriterAppendInfo appendInfo) {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        long writerId = appendInfo.writerId();
        if (writerId == NO_WRITER_ID) {
            throw new IllegalArgumentException(
                    "Invalid writer id "
                            + writerId
                            + " passed to update "
                            + "for bucket "
                            + tableBucket);
        }

        LOG.trace("Updated writer id {} state to {}", writerId, appendInfo);
        WriterStateEntry updatedEntry = appendInfo.toEntry();
        WriterStateEntry currentEntry = writers.get(writerId);
        if (currentEntry != null) {
            currentEntry.update(updatedEntry);
        } else {
            addWriterId(writerId, updatedEntry);
        }
    }

    /** Get the latest accepted V1 fence for the opaque writer key. */
    public Optional<FencedWriterStateEntry> lastFencedEntry(WriterKey writerKey) {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        return Optional.ofNullable(fencedWriters.get(writerKey));
    }

    /** Return the state which dominates a stale V1 sequence, if one exists. */
    public Optional<FencedWriterStateEntry> findStaleFencedBatch(
            WriterKey writerKey, long sequence) {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        if (sequence < 0L) {
            throw new IllegalArgumentException("sequence must be non-negative");
        }
        return lastFencedEntry(writerKey).filter(entry -> sequence <= entry.lastSequence());
    }

    /** Prepare a V1 update without mutating the published state. */
    public FencedWriterAppendInfo prepareFencedUpdate(WriterKey writerKey) {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        return new FencedWriterAppendInfo(writerKey, tableBucket, fencedWriters.get(writerKey));
    }

    /** Publish a prepared V1 update after the corresponding target WAL append succeeds. */
    public void updateFenced(FencedWriterAppendInfo appendInfo) {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        Objects.requireNonNull(appendInfo, "appendInfo");
        if (!tableBucket.equals(appendInfo.tableBucket())) {
            throw new IllegalArgumentException(
                    "Fenced writer update belongs to a different table bucket");
        }
        Optional<FencedWriterStateEntry> current =
                Optional.ofNullable(fencedWriters.get(appendInfo.writerKey()));
        if (!current.equals(appendInfo.currentEntry())) {
            throw new IllegalStateException(
                    "Fenced writer state changed after the update was prepared");
        }
        FencedWriterStateEntry updatedEntry = appendInfo.takeUpdatedEntryForPublish();
        fencedWriters.put(appendInfo.writerKey(), updatedEntry);
        writerIdCount = fencedWriters.size();
    }

    /** Explicitly retire V1 writer keys matching the predicate. */
    public void removeFencedWriters(Predicate<WriterKey> predicate) {
        requireProtocol(KvIdempotenceProtocol.V1_FENCED);
        Objects.requireNonNull(predicate, "predicate");
        fencedWriters.keySet().removeIf(predicate);
        writerIdCount = fencedWriters.size();
    }

    /**
     * Scans the log directory, gathering all writer snapshot files. Snapshot files which do not
     * have an offset corresponding to one of the provided offsets in segmentBaseOffsets will be
     * removed, except in the case that there is a snapshot file at a higher offset than any offset
     * in segmentBaseOffsets.
     *
     * <p>The goal here is to remove any snapshot files which do not have an associated segment
     * file, but not to remove the largest stray snapshot file which was emitted during clean
     * shutdown.
     */
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        OptionalLong maxSegmentBaseOffset =
                segmentBaseOffsets.isEmpty()
                        ? OptionalLong.empty()
                        : OptionalLong.of(segmentBaseOffsets.stream().max(Long::compare).get());

        HashSet<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> snapshots = loadSnapshots();
        for (SnapshotFile snapshot : snapshots.values()) {
            long key = snapshot.offset;
            if (latestStraySnapshot.isPresent()) {
                SnapshotFile prev = latestStraySnapshot.get();
                if (!baseOffsets.contains(key)) {
                    // this snapshot is now the largest stray snapshot.
                    prev.deleteIfExists();
                    snapshots.remove(prev.offset);
                    latestStraySnapshot = Optional.of(snapshot);
                }
            } else {
                if (!baseOffsets.contains(key)) {
                    latestStraySnapshot = Optional.of(snapshot);
                }
            }
        }

        // Check to see if the latestStraySnapshot is larger than the largest segment base offset,
        // if it is not, delete the largestStraySnapshot.
        if (latestStraySnapshot.isPresent() && maxSegmentBaseOffset.isPresent()) {
            long strayOffset = latestStraySnapshot.get().offset;
            long maxOffset = maxSegmentBaseOffset.getAsLong();
            if (strayOffset < maxOffset) {
                SnapshotFile removedSnapshot = snapshots.remove(strayOffset);
                if (removedSnapshot != null) {
                    removedSnapshot.deleteIfExists();
                }
            }
        }

        this.snapshots = snapshots;
    }

    private void loadFromSnapshot(long logStartOffset, long currentTime) throws IOException {
        while (true) {
            Optional<SnapshotFile> latestSnapshotFileOptional = latestSnapshotFile();
            if (latestSnapshotFileOptional.isPresent()) {
                SnapshotFile snapshot = latestSnapshotFileOptional.get();
                try {
                    LOG.info("Loading writer state from snapshot file '{}'", snapshot);
                    Stream<WriterStateEntry> loadedWriters =
                            readSnapshot(snapshot.file()).stream()
                                    .filter(
                                            writerStateEntry ->
                                                    !isWriterExpired(
                                                            currentTime, writerStateEntry));
                    loadedWriters.forEach(this::loadWriterEntry);
                    lastSnapOffset = snapshot.offset;
                    lastMapOffset = lastSnapOffset;
                    return;
                } catch (CorruptSnapshotException e) {
                    LOG.warn(
                            "Failed to load writer snapshot from '{}': {}",
                            snapshot.file(),
                            e.getMessage());
                    removeAndDeleteSnapshot(snapshot.offset);
                }
            } else {
                lastSnapOffset = logStartOffset;
                lastMapOffset = logStartOffset;
                return;
            }
        }
    }

    /** Load writer state snapshots by scanning the logDir. */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        List<SnapshotFile> snapshotFiles = listSnapshotFiles(logTabletDir);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            offsetToSnapshots.put(snapshotFile.offset, snapshotFile);
        }
        return offsetToSnapshots;
    }

    private void addWriterId(long writerId, WriterStateEntry entry) {
        writers.put(writerId, entry);
        writerIdCount = writers.size();
    }

    private void removeWriterIds(List<Long> keys) {
        keys.forEach(writers::remove);
        writerIdCount = writers.size();
    }

    private void clearWriterIds() {
        if (protocol == KvIdempotenceProtocol.V0_COMPACT) {
            writers.clear();
        } else {
            fencedWriters.clear();
        }
        writerIdCount = 0;
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.ofNullable(snapshots.lastEntry()).map(Map.Entry::getValue);
    }

    /** Get the last offset (exclusive) of the latest snapshot file. */
    public Optional<Long> latestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = latestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> snapshotFile.offset);
    }

    public Optional<Long> oldestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = oldestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> snapshotFile.offset);
    }

    @VisibleForTesting
    public static List<SnapshotFile> listSnapshotFiles(File dir) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.list(dir.toPath())) {
                return paths.filter(WriterStateManager::isSnapshotFile)
                        .map(path -> new SnapshotFile(path.toFile()))
                        .collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

    private Optional<SnapshotFile> oldestSnapshotFile() {
        return Optional.ofNullable(snapshots.firstEntry()).map(Map.Entry::getValue);
    }

    /**
     * Removes the writer state snapshot file metadata corresponding to the provided offset if it
     * exists from this WriterStateManager, and deletes the backing snapshot file.
     */
    public void removeAndDeleteSnapshot(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) {
            snapshotFile.deleteIfExists();
        }
    }

    private static boolean isSnapshotFile(Path path) {
        return Files.isRegularFile(path)
                && path.getFileName().toString().endsWith(WRITER_SNAPSHOT_FILE_SUFFIX);
    }

    @VisibleForTesting
    public void loadWriterEntry(WriterStateEntry entry) {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        long writerId = entry.writerId();
        addWriterId(writerId, entry);
    }

    private boolean isWriterExpired(long currentTimeMs, WriterStateEntry writerStateEntry) {
        return currentTimeMs - writerStateEntry.lastBatchTimestamp() > writerExpirationMs;
    }

    public boolean isWriterInBatchExpired(long currentTimeMs, LogRecordBatch recordBatch) {
        requireProtocol(KvIdempotenceProtocol.V0_COMPACT);
        return currentTimeMs - recordBatch.commitTimestamp() > writerExpirationMs;
    }

    private void requireProtocol(KvIdempotenceProtocol requiredProtocol) {
        if (protocol != requiredProtocol) {
            throw new IllegalStateException(
                    String.format(
                            "WriterState API requires protocol %s, but manager uses %s",
                            requiredProtocol, protocol));
        }
    }

    private static List<WriterStateEntry> readSnapshot(File file) {
        try {
            byte[] json = Files.readAllBytes(file.toPath());
            WriterSnapshotMap writerSnapshotMap = WriterSnapshotMap.fromJsonBytes(json);

            List<WriterStateEntry> writerIdEntries = new ArrayList<>();
            writerSnapshotMap.snapshotEntries.forEach(
                    snapshotEntry ->
                            writerIdEntries.add(
                                    new WriterStateEntry(
                                            snapshotEntry.writerId,
                                            snapshotEntry.lastBatchTimestamp,
                                            new WriterStateEntry.BatchMetadata(
                                                    snapshotEntry.writerId,
                                                    snapshotEntry.lastBatchSequence,
                                                    snapshotEntry.lastBatchBaseOffset,
                                                    snapshotEntry.lastBatchOffsetDelta,
                                                    snapshotEntry.lastBatchTimestamp))));
            return writerIdEntries;
        } catch (IOException | RuntimeException e) {
            throw new CorruptSnapshotException("Failed to read snapshot file " + file, e);
        }
    }

    private static List<FencedWriterStateEntry> readFencedSnapshot(File file) {
        try {
            byte[] json = Files.readAllBytes(file.toPath());
            FencedWriterSnapshotMap snapshotMap = FencedWriterSnapshotMap.fromJsonBytes(json);
            return snapshotMap.snapshotEntries.stream()
                    .map(
                            entry ->
                                    new FencedWriterStateEntry(
                                            entry.writerKey,
                                            entry.lastSequence,
                                            entry.lastTargetWalOffset,
                                            entry.lastTimestamp))
                    .collect(Collectors.toList());
        } catch (RuntimeException | IOException e) {
            throw new CorruptSnapshotException("Failed to read snapshot file " + file, e);
        }
    }

    private static void writeSnapshot(File file, Map<Long, WriterStateEntry> entries)
            throws IOException {
        List<WriterSnapshotEntry> snapshotEntries = new ArrayList<>();
        entries.forEach(
                (writerId, writerStateEntry) ->
                        snapshotEntries.add(
                                new WriterSnapshotEntry(
                                        writerId,
                                        writerStateEntry.lastBatchSequence(),
                                        writerStateEntry.lastDataOffset(),
                                        writerStateEntry.lastOffsetDelta(),
                                        writerStateEntry.lastBatchTimestamp())));
        byte[] jsonBytes = new WriterSnapshotMap(snapshotEntries).toJsonBytes();

        ByteBuffer buffer = ByteBuffer.allocate(jsonBytes.length);
        buffer.put(jsonBytes);
        buffer.flip();

        try (FileChannel fileChannel =
                FileChannel.open(
                        file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            fileChannel.force(true);
        }
    }

    private static void writeFencedSnapshot(
            File file, Map<WriterKey, FencedWriterStateEntry> entries) throws IOException {
        List<FencedWriterSnapshotEntry> snapshotEntries = new ArrayList<>();
        entries.forEach(
                (writerKey, entry) ->
                        snapshotEntries.add(
                                new FencedWriterSnapshotEntry(
                                        writerKey,
                                        entry.lastSequence(),
                                        entry.dominatingTargetWalOffset(),
                                        entry.lastTimestamp())));
        byte[] jsonBytes = new FencedWriterSnapshotMap(snapshotEntries).toJsonBytes();

        ByteBuffer buffer = ByteBuffer.allocate(jsonBytes.length);
        buffer.put(jsonBytes);
        buffer.flip();

        try (FileChannel fileChannel =
                FileChannel.open(
                        file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            fileChannel.force(true);
        }
    }

    /** Writer snapshot map json serde. */
    public static class WriterSnapshotMapJsonSerde
            implements JsonSerializer<WriterSnapshotMap>, JsonDeserializer<WriterSnapshotMap> {
        public static final WriterSnapshotMapJsonSerde INSTANCE = new WriterSnapshotMapJsonSerde();

        private static final String VERSION_KEY = "version";
        private static final String WRITER_ID_ENTRIES_FILED = "writer_id_entries";
        private static final String WRITER_ID_FILED = "writer_id";
        private static final String LAST_BATCH_SEQUENCE_FILED = "last_batch_sequence";
        private static final String LAST_BATCH_BASE_OFFSET_FILED = "last_batch_base_offset";
        private static final String LAST_BATCH_OFFSET_DELTA_FILED = "offset_delta";
        private static final String LAST_BATCH_TIMESTAMP_FILED = "last_batch_timestamp";
        private static final int WRITER_ID_SNAPSHOT_VERSION = 1;

        @Override
        public void serialize(WriterSnapshotMap writerSnapshotMap, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();

            // serialize data version.
            generator.writeNumberField(VERSION_KEY, WRITER_ID_SNAPSHOT_VERSION);

            // serialize writer id entries.
            generator.writeArrayFieldStart(WRITER_ID_ENTRIES_FILED);
            for (WriterSnapshotEntry entry : writerSnapshotMap.snapshotEntries) {
                generator.writeStartObject();
                generator.writeNumberField(WRITER_ID_FILED, entry.writerId);
                generator.writeNumberField(LAST_BATCH_SEQUENCE_FILED, entry.lastBatchSequence);
                generator.writeNumberField(LAST_BATCH_BASE_OFFSET_FILED, entry.lastBatchBaseOffset);
                generator.writeNumberField(
                        LAST_BATCH_OFFSET_DELTA_FILED, entry.lastBatchOffsetDelta);
                generator.writeNumberField(LAST_BATCH_TIMESTAMP_FILED, entry.lastBatchTimestamp);
                generator.writeEndObject();
            }
            generator.writeEndArray();

            generator.writeEndObject();
        }

        @Override
        public WriterSnapshotMap deserialize(JsonNode node) {
            JsonNode versionNode = node.get(VERSION_KEY);
            if (versionNode != null && versionNode.asInt() != WRITER_ID_SNAPSHOT_VERSION) {
                throw new IllegalArgumentException(
                        "Unsupported V0 writer snapshot version " + versionNode);
            }
            Iterator<JsonNode> entriesJson = node.get(WRITER_ID_ENTRIES_FILED).elements();
            List<WriterSnapshotEntry> snapshotEntries = new ArrayList<>();
            while (entriesJson.hasNext()) {
                JsonNode entryJson = entriesJson.next();
                long writerId = entryJson.get(WRITER_ID_FILED).asLong();
                int batchSequenceNumber = entryJson.get(LAST_BATCH_SEQUENCE_FILED).asInt();
                long lastBatchBaseOffset = entryJson.get(LAST_BATCH_BASE_OFFSET_FILED).asLong();
                int lastBatchOffsetDelta = entryJson.get(LAST_BATCH_OFFSET_DELTA_FILED).asInt();
                long lastBatchTimestamp = entryJson.get(LAST_BATCH_TIMESTAMP_FILED).asLong();
                snapshotEntries.add(
                        new WriterSnapshotEntry(
                                writerId,
                                batchSequenceNumber,
                                lastBatchBaseOffset,
                                lastBatchOffsetDelta,
                                lastBatchTimestamp));
            }

            return new WriterSnapshotMap(snapshotEntries);
        }
    }

    /** V1 fenced writer snapshot map json serde. */
    public static class FencedWriterSnapshotMapJsonSerde
            implements JsonSerializer<FencedWriterSnapshotMap>,
                    JsonDeserializer<FencedWriterSnapshotMap> {
        public static final FencedWriterSnapshotMapJsonSerde INSTANCE =
                new FencedWriterSnapshotMapJsonSerde();

        private static final String VERSION_KEY = "version";
        private static final String PROTOCOL_VERSION_KEY = "kv_idempotence_protocol_version";
        private static final String WRITER_ENTRIES_FIELD = "writer_entries";
        private static final String WRITER_KEY_HIGH_FIELD = "writer_key_high";
        private static final String WRITER_KEY_LOW_FIELD = "writer_key_low";
        private static final String LAST_SEQUENCE_FIELD = "last_sequence";
        private static final String LAST_TARGET_WAL_OFFSET_FIELD = "last_target_wal_offset";
        private static final String LAST_TIMESTAMP_FIELD = "last_timestamp";
        private static final int SNAPSHOT_VERSION = 2;

        @Override
        public void serialize(FencedWriterSnapshotMap snapshotMap, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, SNAPSHOT_VERSION);
            generator.writeNumberField(
                    PROTOCOL_VERSION_KEY, KvIdempotenceProtocol.V1_FENCED.version());
            generator.writeArrayFieldStart(WRITER_ENTRIES_FIELD);
            for (FencedWriterSnapshotEntry entry : snapshotMap.snapshotEntries) {
                generator.writeStartObject();
                generator.writeNumberField(WRITER_KEY_HIGH_FIELD, entry.writerKey.high());
                generator.writeNumberField(WRITER_KEY_LOW_FIELD, entry.writerKey.low());
                generator.writeNumberField(LAST_SEQUENCE_FIELD, entry.lastSequence);
                generator.writeNumberField(LAST_TARGET_WAL_OFFSET_FIELD, entry.lastTargetWalOffset);
                generator.writeNumberField(LAST_TIMESTAMP_FIELD, entry.lastTimestamp);
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }

        @Override
        public FencedWriterSnapshotMap deserialize(JsonNode node) {
            requireExactValue(node, VERSION_KEY, SNAPSHOT_VERSION);
            requireExactValue(
                    node, PROTOCOL_VERSION_KEY, KvIdempotenceProtocol.V1_FENCED.version());

            JsonNode entriesNode = node.get(WRITER_ENTRIES_FIELD);
            if (entriesNode == null || !entriesNode.isArray()) {
                throw new IllegalArgumentException(
                        "Missing or malformed field " + WRITER_ENTRIES_FIELD);
            }

            List<FencedWriterSnapshotEntry> entries = new ArrayList<>();
            HashSet<WriterKey> writerKeys = new HashSet<>();
            Iterator<JsonNode> entriesJson = entriesNode.elements();
            while (entriesJson.hasNext()) {
                JsonNode entryJson = entriesJson.next();
                if (!entryJson.isObject()) {
                    throw new IllegalArgumentException("Malformed fenced writer snapshot entry");
                }
                WriterKey writerKey =
                        new WriterKey(
                                requireLong(entryJson, WRITER_KEY_HIGH_FIELD),
                                requireLong(entryJson, WRITER_KEY_LOW_FIELD));
                long lastSequence = requireLong(entryJson, LAST_SEQUENCE_FIELD);
                if (lastSequence < 0L) {
                    throw new IllegalArgumentException("last_sequence must be non-negative");
                }
                long lastTargetWalOffset = requireLong(entryJson, LAST_TARGET_WAL_OFFSET_FIELD);
                long lastTimestamp = requireLong(entryJson, LAST_TIMESTAMP_FIELD);
                if (!writerKeys.add(writerKey)) {
                    throw new IllegalArgumentException(
                            "Duplicate WriterKey in fenced writer snapshot");
                }
                entries.add(
                        new FencedWriterSnapshotEntry(
                                writerKey, lastSequence, lastTargetWalOffset, lastTimestamp));
            }
            return new FencedWriterSnapshotMap(entries);
        }

        private static void requireExactValue(JsonNode node, String field, long expected) {
            long actual = requireLong(node, field);
            if (actual != expected) {
                throw new IllegalArgumentException(
                        String.format("Unsupported %s %s; expected %s", field, actual, expected));
            }
        }

        private static long requireLong(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
                throw new IllegalArgumentException("Missing or malformed field " + field);
            }
            return value.longValue();
        }
    }

    /** Serialized V1 fenced writer entry. */
    public static class FencedWriterSnapshotEntry {
        public final WriterKey writerKey;
        public final long lastSequence;
        public final long lastTargetWalOffset;
        public final long lastTimestamp;

        public FencedWriterSnapshotEntry(
                WriterKey writerKey,
                long lastSequence,
                long lastTargetWalOffset,
                long lastTimestamp) {
            this.writerKey = Objects.requireNonNull(writerKey, "writerKey");
            if (lastSequence < 0L) {
                throw new IllegalArgumentException("lastSequence must be non-negative");
            }
            this.lastSequence = lastSequence;
            this.lastTargetWalOffset = lastTargetWalOffset;
            this.lastTimestamp = lastTimestamp;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof FencedWriterSnapshotEntry)) {
                return false;
            }
            FencedWriterSnapshotEntry that = (FencedWriterSnapshotEntry) other;
            return lastSequence == that.lastSequence
                    && lastTargetWalOffset == that.lastTargetWalOffset
                    && lastTimestamp == that.lastTimestamp
                    && writerKey.equals(that.writerKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(writerKey, lastSequence, lastTargetWalOffset, lastTimestamp);
        }
    }

    /** Serialized V1 fenced writer map. */
    public static class FencedWriterSnapshotMap {
        private final List<FencedWriterSnapshotEntry> snapshotEntries;

        public FencedWriterSnapshotMap(List<FencedWriterSnapshotEntry> snapshotEntries) {
            this.snapshotEntries = new ArrayList<>(snapshotEntries);
        }

        private static FencedWriterSnapshotMap fromJsonBytes(byte[] json) {
            return JsonSerdeUtils.readValue(json, FencedWriterSnapshotMapJsonSerde.INSTANCE);
        }

        private byte[] toJsonBytes() {
            return JsonSerdeUtils.writeValueAsBytes(
                    this, FencedWriterSnapshotMapJsonSerde.INSTANCE);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof FencedWriterSnapshotMap)) {
                return false;
            }
            FencedWriterSnapshotMap that = (FencedWriterSnapshotMap) other;
            return snapshotEntries.equals(that.snapshotEntries);
        }

        @Override
        public int hashCode() {
            return snapshotEntries.hashCode();
        }
    }

    /** Writer snapshot entry. */
    public static class WriterSnapshotEntry {
        public final long writerId;
        public final int lastBatchSequence;
        public final long lastBatchBaseOffset;
        public final int lastBatchOffsetDelta;
        public final long lastBatchTimestamp;

        public WriterSnapshotEntry(
                long writerId,
                int lastBatchSequence,
                long lastBatchBaseOffset,
                int lastBatchOffsetDelta,
                long lastBatchTimestamp) {
            this.writerId = writerId;
            this.lastBatchSequence = lastBatchSequence;
            this.lastBatchBaseOffset = lastBatchBaseOffset;
            this.lastBatchOffsetDelta = lastBatchOffsetDelta;
            this.lastBatchTimestamp = lastBatchTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriterSnapshotEntry that = (WriterSnapshotEntry) o;
            return writerId == that.writerId
                    && lastBatchSequence == that.lastBatchSequence
                    && lastBatchBaseOffset == that.lastBatchBaseOffset
                    && lastBatchOffsetDelta == that.lastBatchOffsetDelta
                    && lastBatchTimestamp == that.lastBatchTimestamp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    writerId,
                    lastBatchSequence,
                    lastBatchBaseOffset,
                    lastBatchOffsetDelta,
                    lastBatchTimestamp);
        }

        @Override
        public String toString() {
            return "WriterSnapshotEntry{"
                    + "writerId="
                    + writerId
                    + ", lastBatchSequence="
                    + lastBatchSequence
                    + ", lastBatchBaseOffset="
                    + lastBatchBaseOffset
                    + ", lastBatchOffsetDelta="
                    + lastBatchOffsetDelta
                    + ", lastBatchTimestamp="
                    + lastBatchTimestamp
                    + '}';
        }
    }

    /** Writer snapshot map. */
    public static class WriterSnapshotMap {
        // Version of the snapshot file.
        private final List<WriterSnapshotEntry> snapshotEntries;

        public WriterSnapshotMap(List<WriterSnapshotEntry> snapshotEntries) {
            this.snapshotEntries = snapshotEntries;
        }

        private static WriterSnapshotMap fromJsonBytes(byte[] json) {
            return JsonSerdeUtils.readValue(json, WriterSnapshotMapJsonSerde.INSTANCE);
        }

        private byte[] toJsonBytes() {
            return JsonSerdeUtils.writeValueAsBytes(this, WriterSnapshotMapJsonSerde.INSTANCE);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriterSnapshotMap that = (WriterSnapshotMap) o;
            return Objects.equals(snapshotEntries, that.snapshotEntries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotEntries);
        }

        @Override
        public String toString() {
            return "WriterSnapshotMap{" + "snapshotEntries=" + snapshotEntries + '}';
        }
    }
}
