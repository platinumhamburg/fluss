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

package org.apache.fluss.server.log.state;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.StateChangeLog;
import org.apache.fluss.record.StateDefs;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.SnapshotFile;
import org.apache.fluss.server.log.checkpoint.StateCheckpointFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.FlussPaths.stateSnapshotFile;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** Manages bucket state with checkpoint and recovery mechanisms. */
public class BucketStateManager {

    /** Wrapper class for state value with its corresponding log offset. */
    public static class StateValueWithOffset {
        private final Object value;
        private final long offset;

        public StateValueWithOffset(Object value, long offset) {
            this.value = value;
            this.offset = offset;
        }

        public Object getValue() {
            return value;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(BucketStateManager.class);

    private final TableBucket tableBucket;
    private final File logTabletDir;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private long lastAppliedOffset = -1;
    private long lastCommittedOffset = -1;
    private long lastSnapshotOffset = -1;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;

    private final Map<StateDefs, Map<Object, MvccStateValue>> inMemoryState = new HashMap<>();

    public BucketStateManager(TableBucket tableBucket, File logTabletDir) throws IOException {
        this.tableBucket = tableBucket;
        this.logTabletDir = logTabletDir;
        this.snapshots = loadSnapshots();
    }

    public StateValueWithOffset getState(StateDefs stateDef, Object key, boolean readCommitted) {
        return inReadLock(
                lock,
                () -> {
                    Map<Object, MvccStateValue> stateValueMap = inMemoryState.get(stateDef);
                    if (stateValueMap == null) {
                        return null;
                    }
                    MvccStateValue mvccStateValue = stateValueMap.get(key);
                    if (mvccStateValue == null || !mvccStateValue.isValid()) {
                        return null;
                    }
                    MvccStateValue.ValueWithOffset valueWithOffset =
                            mvccStateValue.getValueWithOffset(readCommitted);
                    if (valueWithOffset == null) {
                        return null;
                    }
                    return new StateValueWithOffset(
                            valueWithOffset.getValue(), valueWithOffset.getOffset());
                });
    }

    public long checkpoint(File file) throws IOException {
        return inWriteLock(
                lock,
                () -> {
                    StateCheckpointFile checkpointFile = new StateCheckpointFile(file);
                    Map<Integer, Map<String, String>> states = new HashMap<>();
                    for (Map.Entry<StateDefs, Map<Object, MvccStateValue>> stateDefEntry :
                            inMemoryState.entrySet()) {
                        StateDefs stateDef = stateDefEntry.getKey();
                        for (Map.Entry<Object, MvccStateValue> stateEntry :
                                stateDefEntry.getValue().entrySet()) {
                            Map<String, String> stateDefStates =
                                    states.computeIfAbsent(stateDef.getId(), k -> new HashMap<>());
                            Object stateValue = stateEntry.getValue().getValue(true);
                            if (null != stateValue) {
                                // Use StateDefs key serializer to convert key Object to String
                                String serializedKey =
                                        stateDef.getKeySerializer().serialize(stateEntry.getKey());
                                // Use StateDefs value serializer to convert value Object to String
                                String serializedValue =
                                        stateDef.getValueSerializer().serialize(stateValue);
                                stateDefStates.put(serializedKey, serializedValue);
                            }
                        }
                    }
                    checkpointFile.write(states);
                    return lastCommittedOffset;
                });
    }

    public void restore(File file, long logOffset) throws IOException {
        inWriteLock(
                lock,
                () -> {
                    StateCheckpointFile checkpointFile = new StateCheckpointFile(file);
                    Map<Integer, Map<String, String>> states = checkpointFile.read();
                    for (Map.Entry<Integer, Map<String, String>> stateDefEntry :
                            states.entrySet()) {
                        StateDefs stateDef = StateDefs.fromId(stateDefEntry.getKey());
                        if (stateDef == null) {
                            LOG.warn(
                                    "Unknown StateDef ID {} in checkpoint file, skipping",
                                    stateDefEntry.getKey());
                            continue;
                        }
                        for (Map.Entry<String, String> stateEntry :
                                stateDefEntry.getValue().entrySet()) {
                            // Use StateDefs key deserializer to convert String to key Object
                            Object deserializedKey =
                                    stateDef.getKeySerializer().deserialize(stateEntry.getKey());
                            // Use StateDefs value deserializer to convert String to value Object
                            Object deserializedValue =
                                    stateDef.getValueSerializer()
                                            .deserialize(stateEntry.getValue());
                            inMemoryState
                                    .computeIfAbsent(stateDef, k -> new HashMap<>())
                                    .computeIfAbsent(
                                            deserializedKey,
                                            k -> new MvccStateValue(deserializedValue));
                        }
                    }
                    lastAppliedOffset = logOffset;
                    lastCommittedOffset = logOffset;
                });
    }

    public void replayTo(LogTablet logTablet, long offset) {}

    public void apply(long offset, Iterator<StateChangeLog> stateChangeLogs) {
        inWriteLock(
                lock,
                () -> {
                    while (stateChangeLogs.hasNext()) {
                        StateChangeLog stateChangeLog = stateChangeLogs.next();
                        inMemoryState
                                .computeIfAbsent(stateChangeLog.getStateDef(), k -> new HashMap<>())
                                .computeIfAbsent(stateChangeLog.getKey(), k -> new MvccStateValue())
                                .apply(
                                        offset,
                                        stateChangeLog.getChangeType(),
                                        stateChangeLog.getValue());
                    }
                    lastAppliedOffset = offset;
                });
    }

    public void commitTo(long offset) {
        inWriteLock(
                lock,
                () -> {
                    inMemoryState
                            .values()
                            .forEach(
                                    state ->
                                            state.values()
                                                    .forEach(
                                                            mvccStateValue ->
                                                                    mvccStateValue.commitTo(
                                                                            offset)));
                    lastCommittedOffset = offset;
                });
    }

    public boolean isEmpty() {
        return inReadLock(lock, () -> inMemoryState.isEmpty());
    }

    /** Get the last committed offset. */
    public long lastCommittedOffset() {
        return lastCommittedOffset;
    }

    /** Get the last offset (exclusive) of the latest snapshot file. */
    public Optional<Long> latestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = latestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> snapshotFile.offset);
    }

    /** Take a snapshot at the current committed offset if one does not already exist. */
    public void takeSnapshot() throws IOException {
        // If not a new committed offset, then it is not worth taking another snapshot
        if (lastCommittedOffset > lastSnapshotOffset) {
            SnapshotFile snapshotFile =
                    new SnapshotFile(stateSnapshotFile(logTabletDir, lastCommittedOffset));
            long start = System.currentTimeMillis();
            checkpoint(snapshotFile.file());
            LOG.info(
                    "Wrote state snapshot at committed offset {} for table bucket {} in {} ms.",
                    lastCommittedOffset,
                    tableBucket,
                    System.currentTimeMillis() - start);

            snapshots.put(snapshotFile.offset, snapshotFile);
            lastSnapshotOffset = lastCommittedOffset;
        }
    }

    /**
     * Truncate the state to the given offset range and reload from the most recent snapshot.
     * Snapshots outside the range will be deleted.
     */
    public void truncateAndReload(long logStartOffset, long logEndOffset) throws IOException {
        // Remove all out of range snapshots
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }

        // Clear current state and reload from the latest available snapshot
        clearState();
        loadFromSnapshot(logEndOffset);
    }

    /**
     * Scans the log directory, gathering all state snapshot files. Snapshot files which do not have
     * an offset corresponding to one of the provided offsets in segmentBaseOffsets will be removed,
     * except in the case that there is a snapshot file at a higher offset than any offset in
     * segmentBaseOffsets.
     *
     * <p>The goal here is to remove any snapshot files which do not have an associated segment
     * file, but not to remove the largest stray snapshot file which was emitted during clean
     * shutdown.
     */
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        Optional<Long> maxSegmentBaseOffset =
                segmentBaseOffsets.isEmpty()
                        ? Optional.empty()
                        : segmentBaseOffsets.stream().max(Long::compare);

        HashSet<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> loadedSnapshots = loadSnapshots();
        for (SnapshotFile snapshot : loadedSnapshots.values()) {
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
                if (maxSegmentBaseOffset.isPresent() && key > maxSegmentBaseOffset.get()) {
                    latestStraySnapshot = Optional.of(snapshot);
                } else if (!baseOffsets.contains(key)) {
                    snapshot.deleteIfExists();
                    snapshots.remove(key);
                }
            }
        }
        this.snapshots = loadedSnapshots;
    }

    /**
     * Deletes the state snapshot files until the given offset (exclusive) in a thread safe manner.
     */
    @VisibleForTesting
    public void deleteSnapshotsBefore(long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
    }

    /** Fetch the snapshot file for the committed offset. */
    public Optional<File> fetchSnapshot(long offset) {
        return Optional.ofNullable(snapshots.get(offset)).map(SnapshotFile::file);
    }

    /** Load state snapshots by scanning the logDir. */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        return org.apache.fluss.server.log.SnapshotUtils.loadStateSnapshots(logTabletDir);
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.ofNullable(snapshots.lastEntry()).map(Map.Entry::getValue);
    }

    /**
     * Removes the bucket state snapshot file metadata corresponding to the provided offset if it
     * exists from this BucketStateManager, and deletes the backing snapshot file.
     */
    public void removeAndDeleteSnapshot(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) {
            snapshotFile.deleteIfExists();
        }
    }

    private void clearState() {
        inWriteLock(
                lock,
                () -> {
                    inMemoryState.clear();
                    lastAppliedOffset = -1;
                    lastCommittedOffset = -1;
                });
    }

    /** Loads the most recent snapshot file that is not greater than the given offset. */
    private void loadFromSnapshot(long maxOffset) throws IOException {
        inWriteLock(
                lock,
                () -> {
                    Optional<SnapshotFile> latestSnapshot = Optional.empty();
                    // Find the latest snapshot not greater than maxOffset
                    for (SnapshotFile snapshot : snapshots.values()) {
                        if (snapshot.offset <= maxOffset) {
                            latestSnapshot = Optional.of(snapshot);
                        }
                    }

                    if (latestSnapshot.isPresent()) {
                        SnapshotFile snapshotFile = latestSnapshot.get();
                        LOG.info(
                                "Loading state from snapshot file {} at offset {} for bucket {}",
                                snapshotFile.file(),
                                snapshotFile.offset,
                                tableBucket);
                        restore(snapshotFile.file(), snapshotFile.offset);
                    }
                    lastSnapshotOffset = latestSnapshotOffset().orElse(-1L);
                });
    }
}
