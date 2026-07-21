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
import org.apache.fluss.exception.CorruptSnapshotException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.server.log.WriterStateManager.listSnapshotFiles;
import static org.apache.fluss.utils.FlussPaths.offsetFromFile;
import static org.apache.fluss.utils.FlussPaths.writerSnapshotFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link WriterStateManager}. */
public class WriterStateManagerTest {

    private static final byte[] CONTIGUOUS_SEQUENCE_SNAPSHOT_FIXTURE =
            ("{\"version\":1,\"writer_id_entries\":[{\"writer_id\":5,"
                            + "\"last_batch_sequence\":0,\"last_batch_base_offset\":8,"
                            + "\"offset_delta\":0,\"last_batch_timestamp\":9}]}")
                    .getBytes(StandardCharsets.UTF_8);

    private @TempDir File tempDir;
    private final long writerId = 1L;
    private File logDir;
    private TableBucket tableBucket;
    private Configuration conf;
    private WriterStateManager stateManager;

    @BeforeEach
    public void setup() throws Exception {
        long tableId = 1001;
        logDir = LogTestUtils.makeRandomLogTabletDir(tempDir, "testDb", tableId, "testTable");
        tableBucket = new TableBucket(tableId, 0);
        conf = new Configuration();
        stateManager =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
    }

    @Test
    void testThreeArgumentConstructorDefaultsToContiguousBatchSequence() {
        append(stateManager, 5L, 0, 8L, false, 9L);

        assertThat(stateManager.protocol())
                .isEqualTo(KvIdempotenceProtocol.CONTIGUOUS_BATCH_SEQUENCE);
        assertThat(stateManager.activeWriters()).containsOnlyKeys(5L);
        assertThat(stateManager.writerIdCount()).isEqualTo(1);
    }

    @Test
    void testSnapshotWriteDrainsShortWritingChannel() throws Exception {
        ByteBuffer source = ByteBuffer.wrap("complete-snapshot".getBytes(StandardCharsets.UTF_8));
        ByteBuffer sink = ByteBuffer.allocate(source.remaining());
        WritableByteChannel shortWriter =
                new WritableByteChannel() {
                    private boolean open = true;

                    @Override
                    public int write(ByteBuffer input) {
                        int bytes = Math.min(3, input.remaining());
                        for (int i = 0; i < bytes; i++) {
                            sink.put(input.get());
                        }
                        return bytes;
                    }

                    @Override
                    public boolean isOpen() {
                        return open;
                    }

                    @Override
                    public void close() {
                        open = false;
                    }
                };

        WriterStateManager.writeFully(shortWriter, source);

        assertThat(source.hasRemaining()).isFalse();
        assertThat(new String(sink.array(), StandardCharsets.UTF_8)).isEqualTo("complete-snapshot");
    }

    @Test
    void testContiguousSequenceSnapshotMatchesLiteralFixture() throws Exception {
        append(stateManager, 5L, 0, 8L, false, 9L);
        stateManager.takeSnapshot();

        assertThat(Files.readAllBytes(writerSnapshotFile(logDir, 9L).toPath()))
                .isEqualTo(CONTIGUOUS_SEQUENCE_SNAPSHOT_FIXTURE);
    }

    @Test
    void testCumulativeProgressKeepsLatestValueAcrossGaps() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey key = new WriterKey(4L, 5L);

        appendProgress(manager, key, 100L, 10L, 1L);
        appendProgress(manager, key, 500L, 20L, 2L);
        appendProgress(manager, key, (long) Integer.MAX_VALUE + 1L, 30L, 3L);

        WriterProgressStateEntry entry =
                manager.lastProgressEntry(key).orElseThrow(AssertionError::new);
        assertThat(entry.lastProgress()).isEqualTo((long) Integer.MAX_VALUE + 1L);
        assertThat(entry.progressWalOffset()).isEqualTo(30L);
        assertThat(manager.findStaleProgressBatch(key, 500L)).contains(entry);
        assertThat(manager.findStaleProgressBatch(key, entry.lastProgress())).contains(entry);
        assertThat(manager.findStaleProgressBatch(key, entry.lastProgress() + 1L)).isEmpty();
        assertThat(manager.writerIdCount()).isEqualTo(1);
    }

    @Test
    void testProgressUpdateIsOneShotAndPublishesOnlyOnUpdate() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey key = new WriterKey(4L, 5L);
        WriterProgressAppendInfo appendInfo = manager.prepareProgressUpdate(key);
        WriterProgressAppendInfo superseded = manager.prepareProgressUpdate(key);

        appendInfo.append(100L, 10L, 1L);
        superseded.append(101L, 11L, 2L);
        assertThat(manager.lastProgressEntry(key)).isEmpty();
        assertThatThrownBy(() -> appendInfo.append(101L, 11L, 2L))
                .isInstanceOf(IllegalStateException.class);

        manager.updateProgress(appendInfo);
        assertThat(manager.lastProgressEntry(key)).contains(appendInfo.updatedEntry());
        assertThatThrownBy(() -> manager.updateProgress(superseded))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> manager.updateProgress(manager.prepareProgressUpdate(key)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testProgressRejectsEqualLowerAndNegativeFreshUpdates() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey key = new WriterKey(4L, 5L);
        appendProgress(manager, key, 100L, 10L, 1L);

        assertThatThrownBy(() -> manager.prepareProgressUpdate(key).append(100L, 11L, 2L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> manager.prepareProgressUpdate(key).append(99L, 11L, 2L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                manager.prepareProgressUpdate(new WriterKey(6L, 7L))
                                        .append(-1L, 11L, 2L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testProgressWriterDoesNotExpireAndCanBeExplicitlyRetired() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey retained = new WriterKey(4L, 5L);
        WriterKey retired = new WriterKey(6L, 7L);
        appendProgress(manager, retained, 100L, 10L, 1L);
        appendProgress(manager, retired, 200L, 20L, 2L);

        manager.removeExpiredWriters(Long.MAX_VALUE);
        assertThat(manager.writerIdCount()).isEqualTo(2);

        manager.removeProgressWriters(retired::equals);
        assertThat(manager.lastProgressEntry(retained)).isPresent();
        assertThat(manager.lastProgressEntry(retired)).isEmpty();
        assertThat(manager.writerIdCount()).isEqualTo(1);
        assertThat(manager.isEmpty()).isFalse();
        manager.removeProgressWriters(key -> true);
        assertThat(manager.isEmpty()).isTrue();
    }

    @Test
    void testProtocolSpecificApisFailFastAcrossProtocols() throws Exception {
        WriterStateManager progress = progressManager();
        WriterKey key = new WriterKey(4L, 5L);

        assertThatThrownBy(() -> stateManager.lastProgressEntry(key))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stateManager.findStaleProgressBatch(key, 0L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stateManager.prepareProgressUpdate(key))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stateManager.removeProgressWriters(ignored -> true))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stateManager.updateProgress(progress.prepareProgressUpdate(key)))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> progress.lastEntry(1L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(progress::activeWriters).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> progress.prepareUpdate(1L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> progress.update(stateManager.prepareUpdate(1L)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> progress.loadWriterEntry(WriterStateEntry.empty(1L)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testProgressSnapshotRoundTripPreservesFullKeyAndLongValues() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey key = new WriterKey(Long.MAX_VALUE, Long.MIN_VALUE | 3L);
        appendProgress(manager, key, (long) Integer.MAX_VALUE + 1L, Long.MAX_VALUE - 1L, 42L);
        manager.updateMapEndOffset(Long.MAX_VALUE);
        manager.takeSnapshot();

        WriterStateManager recovered = progressManager();
        recovered.truncateAndReload(0L, Long.MAX_VALUE, Long.MAX_VALUE);

        assertThat(recovered.lastProgressEntry(key))
                .contains(
                        new WriterProgressStateEntry(
                                key, (long) Integer.MAX_VALUE + 1L, Long.MAX_VALUE - 1L, 42L));
    }

    @Test
    void testSnapshotReplacesStaleTemporaryFileAndPublishesCompleteContents() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey key = new WriterKey(4L, 5L);
        appendProgress(manager, key, 100L, 10L, 1L);
        manager.updateMapEndOffset(11L);
        Path snapshot = writerSnapshotFile(logDir, 11L).toPath();
        Path temporary = snapshot.resolveSibling(snapshot.getFileName() + ".tmp");
        Files.write(temporary, "stale-partial-snapshot".getBytes(StandardCharsets.UTF_8));

        manager.takeSnapshot();

        assertThat(temporary).doesNotExist();
        assertThat(Files.readAllBytes(snapshot)).isEqualTo(v2SnapshotBytes(100L, 10L));
        WriterStateManager recovered = progressManager();
        recovered.truncateAndReload(0L, 11L, Long.MAX_VALUE);
        assertThat(recovered.lastProgressEntry(key))
                .contains(new WriterProgressStateEntry(key, 100L, 10L, 1L));
    }

    @Test
    void testProtocolSnapshotMismatchIsRejected() throws Exception {
        Files.write(writerSnapshotFile(logDir, 1L).toPath(), v2SnapshotBytes());
        stateManager.reloadSnapshots();
        stateManager.truncateAndReload(0L, 1L, Long.MAX_VALUE);
        assertThat(writerSnapshotFile(logDir, 1L)).doesNotExist();

        Files.write(writerSnapshotFile(logDir, 2L).toPath(), CONTIGUOUS_SEQUENCE_SNAPSHOT_FIXTURE);
        WriterStateManager progress = progressManager();
        assertThatThrownBy(() -> progress.truncateAndReload(1L, 2L, Long.MAX_VALUE))
                .isInstanceOf(CorruptSnapshotException.class);
        assertThat(writerSnapshotFile(logDir, 2L)).exists();
    }

    @Test
    void testProgressCorruptSnapshotIsPropagatedWithoutFallback() throws Exception {
        File snapshot = writerSnapshotFile(logDir, 1L);
        Files.write(snapshot.toPath(), "{\"version\":2}".getBytes(StandardCharsets.UTF_8));
        WriterStateManager progress = progressManager();

        assertThatThrownBy(() -> progress.truncateAndReload(1L, 1L, Long.MAX_VALUE))
                .isInstanceOf(CorruptSnapshotException.class);
        assertThat(snapshot).exists();
    }

    @Test
    void testProgressRecoveryCoverageUsesHalfOpenOffsets() throws Exception {
        WriterStateManager empty = progressManager();

        assertThatThrownBy(() -> empty.validateRecoveryCoverage(1L, 1L))
                .isInstanceOf(CorruptSnapshotException.class)
                .hasMessageContaining("retained WAL starts at 1");
        empty.validateRecoveryCoverage(0L, 0L);
        empty.updateMapEndOffset(5L);
        empty.validateRecoveryCoverage(0L, 5L);

        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, new WriterKey(4L, 5L), 100L, 4L, 1L);
        snapshotWriter.updateMapEndOffset(5L);
        snapshotWriter.takeSnapshot();

        WriterStateManager exactEnd = progressManager();
        exactEnd.truncateAndReload(5L, 5L, Long.MAX_VALUE);
        exactEnd.validateRecoveryCoverage(5L, 5L);

        assertThatThrownBy(() -> exactEnd.validateRecoveryCoverage(0L, 6L))
                .isInstanceOf(CorruptSnapshotException.class)
                .hasMessageContaining("ends at 5")
                .hasMessageContaining("recovery end 6");
    }

    @Test
    void testProgressFallsBackToOlderValidSnapshotWithoutDeletingCorruptLatest() throws Exception {
        WriterKey olderKey = new WriterKey(4L, 5L);
        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, olderKey, 100L, 4L, 1L);
        snapshotWriter.updateMapEndOffset(5L);
        snapshotWriter.takeSnapshot();

        File corruptLatest = writerSnapshotFile(logDir, 10L);
        byte[] corruptBytes = "{\"version\":2}".getBytes(StandardCharsets.UTF_8);
        Files.write(corruptLatest.toPath(), corruptBytes);

        WriterStateManager recovered = progressManager();
        recovered.truncateAndReload(5L, 10L, Long.MAX_VALUE);

        assertThat(recovered.lastProgressEntry(olderKey)).isPresent();
        assertThat(recovered.mapEndOffset()).isEqualTo(5L);
        assertThat(recovered.fetchSnapshot(10L)).contains(corruptLatest);
        assertThat(Files.readAllBytes(corruptLatest.toPath())).isEqualTo(corruptBytes);
    }

    @Test
    void testProgressRejectsFallbackWhoseReplayRangeStartsBeforeRetainedWal() throws Exception {
        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, new WriterKey(4L, 5L), 100L, 3L, 1L);
        snapshotWriter.updateMapEndOffset(4L);
        snapshotWriter.takeSnapshot();

        File corruptLatest = writerSnapshotFile(logDir, 10L);
        Files.write(corruptLatest.toPath(), "{\"version\":2}".getBytes(StandardCharsets.UTF_8));

        WriterStateManager recovered = progressManager();
        assertThatThrownBy(() -> recovered.truncateAndReload(5L, 10L, Long.MAX_VALUE))
                .isInstanceOf(CorruptSnapshotException.class)
                .hasMessageContaining("continuous WriterState recovery");
        assertThat(corruptLatest).exists();
        assertThat(writerSnapshotFile(logDir, 4L)).exists();
    }

    @Test
    void testProgressRejectsSnapshotAfterTruncationTarget() throws Exception {
        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, new WriterKey(4L, 5L), 100L, 9L, 1L);
        snapshotWriter.updateMapEndOffset(10L);
        snapshotWriter.takeSnapshot();

        WriterStateManager recovered = progressManager();
        assertThatThrownBy(() -> recovered.truncateAndReload(5L, 8L, Long.MAX_VALUE))
                .isInstanceOf(CorruptSnapshotException.class)
                .hasMessageContaining("recovery end 8");
        assertThat(writerSnapshotFile(logDir, 10L)).exists();
    }

    @Test
    void testProgressFailedReloadPreservesLiveStateAndSnapshotMetadata() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey liveKey = new WriterKey(4L, 5L);
        appendProgress(manager, liveKey, 100L, 10L, 1L);
        manager.updateMapEndOffset(20L);

        File corruptSnapshot = writerSnapshotFile(logDir, 15L);
        byte[] corruptBytes = "{\"version\":2}".getBytes(StandardCharsets.UTF_8);
        Files.write(corruptSnapshot.toPath(), corruptBytes);
        File outOfRangeSnapshot = writerSnapshotFile(logDir, 25L);
        byte[] outOfRangeBytes = v2SnapshotBytes();
        Files.write(outOfRangeSnapshot.toPath(), outOfRangeBytes);
        manager.reloadSnapshots();

        Optional<WriterProgressStateEntry> liveEntry = manager.lastProgressEntry(liveKey);
        Optional<Long> latestSnapshotOffset = manager.latestSnapshotOffset();
        Optional<Long> oldestSnapshotOffset = manager.oldestSnapshotOffset();

        assertThatThrownBy(() -> manager.truncateAndReload(1L, 15L, Long.MAX_VALUE))
                .isInstanceOf(CorruptSnapshotException.class);

        assertThat(manager.lastProgressEntry(liveKey)).isEqualTo(liveEntry);
        assertThat(manager.writerIdCount()).isEqualTo(1);
        assertThat(manager.mapEndOffset()).isEqualTo(20L);
        assertThat(manager.latestSnapshotOffset()).isEqualTo(latestSnapshotOffset);
        assertThat(manager.oldestSnapshotOffset()).isEqualTo(oldestSnapshotOffset);
        assertThat(manager.fetchSnapshot(15L)).contains(corruptSnapshot);
        assertThat(Files.readAllBytes(corruptSnapshot.toPath())).isEqualTo(corruptBytes);
        assertThat(manager.fetchSnapshot(25L)).contains(outOfRangeSnapshot);
        assertThat(Files.readAllBytes(outOfRangeSnapshot.toPath())).isEqualTo(outOfRangeBytes);
    }

    @Test
    void testProgressRecoveryCandidateFailureDoesNotPublishSelectedSnapshot() throws Exception {
        WriterKey writerKey = new WriterKey(4L, 5L);
        WriterStateManager manager = progressManager();
        appendProgress(manager, writerKey, 100L, 4L, 1L);
        manager.updateMapEndOffset(5L);
        manager.takeSnapshot();
        appendProgress(manager, writerKey, 900L, 9L, 2L);
        manager.updateMapEndOffset(10L);

        WriterStateManager candidate = manager.progressRecoveryCandidate(5L, 10L);
        assertThatThrownBy(() -> candidate.validateRecoveryCoverage(5L, 10L))
                .isInstanceOf(CorruptSnapshotException.class);

        assertThat(manager.mapEndOffset()).isEqualTo(10L);
        assertThat(manager.lastProgressEntry(writerKey))
                .get()
                .extracting(WriterProgressStateEntry::lastProgress)
                .isEqualTo(900L);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, 5L})
    void testProgressCandidateRejectsInvalidTargetWalOffsetAndKeepsOlderEligible(
            long invalidTargetOffset) throws Exception {
        WriterKey writerKey = new WriterKey(4L, 5L);
        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, writerKey, 100L, 0L, 1L);
        snapshotWriter.updateMapEndOffset(2L);
        snapshotWriter.takeSnapshot();
        File invalidSnapshot = writerSnapshotFile(logDir, 5L);
        byte[] invalidBytes = v2SnapshotBytes(900L, invalidTargetOffset);
        Files.write(invalidSnapshot.toPath(), invalidBytes);

        WriterStateManager manager = progressManager();
        WriterStateManager candidate = manager.progressRecoveryCandidate(0L, 5L);

        assertThat(candidate.mapEndOffset()).isEqualTo(2L);
        assertThat(candidate.lastProgressEntry(writerKey))
                .get()
                .extracting(WriterProgressStateEntry::lastProgress)
                .isEqualTo(100L);
        assertThat(Files.readAllBytes(invalidSnapshot.toPath())).isEqualTo(invalidBytes);
    }

    @Test
    void testProgressSuccessfulReloadAtomicallyReplacesLiveState() throws Exception {
        WriterStateManager manager = progressManager();
        WriterKey oldKey = new WriterKey(4L, 5L);
        WriterKey snapshotKey = new WriterKey(6L, 7L);
        appendProgress(manager, oldKey, 100L, 10L, 1L);
        manager.updateMapEndOffset(20L);

        WriterStateManager snapshotWriter = progressManager();
        appendProgress(snapshotWriter, snapshotKey, 200L, 14L, 2L);
        snapshotWriter.updateMapEndOffset(15L);
        snapshotWriter.takeSnapshot();
        manager.reloadSnapshots();

        manager.truncateAndReload(0L, 15L, Long.MAX_VALUE);

        assertThat(manager.lastProgressEntry(oldKey)).isEmpty();
        assertThat(manager.lastProgressEntry(snapshotKey))
                .contains(new WriterProgressStateEntry(snapshotKey, 200L, 14L, 2L));
        assertThat(manager.writerIdCount()).isEqualTo(1);
        assertThat(manager.mapEndOffset()).isEqualTo(15L);
    }

    @Test
    void testBasicWriterIdMapping() {
        // First entry for id 0 added.
        append(stateManager, writerId, 0, 0L);

        // Second entry for id 1 added.
        append(stateManager, writerId, 1, 0L);

        // Duplicates are checked separately and should result in OutOfOrderSequence if appended
        assertThatThrownBy(() -> append(stateManager, writerId, 1, 0L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 0 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0} "
                                + ": 1 (incoming batch seq.), 1 (current batch seq.)");

        // Invalid batch sequence (greater than next expected batch sequence).
        assertThatThrownBy(() -> append(stateManager, writerId, 5, 0L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 0 in"
                                + " table-bucket TableBucket{tableId=1001, bucket=0} "
                                + ": 5 (incoming batch seq.), 1 (current batch seq.)");
    }

    @Test
    void testValidationOnFirstEntryWhenLoadingLog() {
        // When the first entry is added, the batch sequence should only be 0.
        int batchSequence = 16;
        long offset = 735L;
        assertThatThrownBy(() -> append(stateManager, writerId, batchSequence, offset))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 735 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 16 (incoming batch seq.), -1 (current batch seq.)");

        append(stateManager, writerId, 0, offset);
        Optional<WriterStateEntry> maybeLastEntry = stateManager.lastEntry(writerId);
        assertThat(maybeLastEntry).isPresent();

        WriterStateEntry lastEntry = maybeLastEntry.get();
        assertThat(lastEntry.firstBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.firstDataOffset()).isEqualTo(offset);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(offset);
    }

    @Test
    void testPrepareUpdateDoesNotMutate() {
        WriterAppendInfo appendInfo = stateManager.prepareUpdate(writerId);
        appendInfo.appendDataBatch(
                0, new LogOffsetMetadata(15L), 20L, false, true, System.currentTimeMillis());
        assertThat(stateManager.lastEntry(writerId)).isNotPresent();
        stateManager.update(appendInfo);
        assertThat(stateManager.lastEntry(writerId)).isPresent();

        WriterAppendInfo nextAppendInfo = stateManager.prepareUpdate(writerId);
        nextAppendInfo.appendDataBatch(
                1, new LogOffsetMetadata(26L), 30L, false, true, System.currentTimeMillis());
        assertThat(stateManager.lastEntry(writerId)).isPresent();

        WriterStateEntry lastEntry = stateManager.lastEntry(writerId).get();
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(20L);

        stateManager.update(nextAppendInfo);
        lastEntry = stateManager.lastEntry(writerId).get();
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(1);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(30L);
    }

    @Test
    void testTruncateAndReloadRemovesOutOfRangeSnapshots() throws IOException {
        for (int i = 0; i < 5; i++) {
            append(stateManager, writerId, i, i);
            stateManager.takeSnapshot();
        }

        stateManager.truncateAndReload(1L, 3L, System.currentTimeMillis());
        assertThat(stateManager.oldestSnapshotOffset()).isPresent();
        assertThat(stateManager.oldestSnapshotOffset().get()).isEqualTo(2L);
        assertThat(stateManager.latestSnapshotOffset()).isPresent();
        assertThat(stateManager.latestSnapshotOffset().get()).isEqualTo(3L);
    }

    @Test
    void testTakeSnapshot() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);

        // Take snapshot.
        stateManager.takeSnapshot();

        String[] fileList = logDir.list();
        assertThat(fileList).isNotNull();
        assertThat(fileList.length).isEqualTo(1);
        assertThat(new File(logDir, fileList[0]).length() > 0).isTrue();
    }

    @Test
    void testFetchSnapshotEmptySnapshot() {
        assertThat(stateManager.fetchSnapshot(1)).isEmpty();
    }

    @Test
    void testRemoveExpiredWritersOnReload() throws IOException {
        append(stateManager, writerId, 0, 0L, false, 0);
        append(stateManager, writerId, 1, 1L, false, 1);

        stateManager.takeSnapshot();
        WriterStateManager recoveredMapping =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        recoveredMapping.truncateAndReload(0L, 1L, 70000);

        // Entry added after recovery. The writer id should be expired now, and would not exist in
        // the writer mapping. If writing with the same writerId and non-zero batch sequence, the
        // OutOfOrderSequenceException will throw. If you want to continue to write, you need to get
        // a new writer id.
        assertThatThrownBy(() -> append(recoveredMapping, writerId, 2, 2L, false, 3000L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 2 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 2 (incoming batch seq.), -1 (current batch seq.)");

        append(recoveredMapping, 2L, 0, 2L, false, 70002);

        assertThat(recoveredMapping.activeWriters().size()).isEqualTo(1);
        assertThat(recoveredMapping.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(0);
        assertThat(recoveredMapping.mapEndOffset()).isEqualTo(3L);
    }

    @Test
    void testAppendAnExpiredBatchWithEmptyWriterStatus() throws Exception {
        ManualClock clock = new ManualClock(5000L);

        // 2 seconds to expire the writer.
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofSeconds(2));
        WriterStateManager stateManager1 =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());

        // If we try to append an expired batch with none zero batch sequence, the
        // OutOfOrderSequenceException will not been throw.
        append(stateManager1, 1L, 10, 10L, true, clock.milliseconds());
        assertThat(stateManager1.activeWriters().size()).isEqualTo(1);
        assertThat(stateManager1.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(10);

        // If we try to append a none-expired batch with none zero batch sequence, the
        // OutOfOrderSequenceException will throw.
        assertThatThrownBy(() -> append(stateManager1, 2L, 10, 10L, false, 1000L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 2 at offset 10 in table-bucket "
                                + "TableBucket{tableId=1001, bucket=0} : 10 (incoming batch seq.), -1 (current batch seq.)");
    }

    @Test
    void testDeleteSnapshotsBefore() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(2L));

        append(stateManager, writerId, 2, 2L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(2L, 3L)));

        stateManager.deleteSnapshotsBefore(3L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(3L));

        stateManager.deleteSnapshotsBefore(4L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(0);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.emptySet());
    }

    @Test
    void testTruncateFullyAndStartAt() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(2L));

        append(stateManager, writerId, 2, 2L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(2L, 3L)));

        stateManager.truncateFullyAndStartAt(0L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(0);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.emptySet());

        append(stateManager, writerId, 0, 0L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));
    }

    @Test
    void testReloadSnapshots() throws Exception {
        append(stateManager, writerId, 0, 1L);
        append(stateManager, writerId, 1, 2L);
        stateManager.takeSnapshot();

        Set<Tuple2<Path, byte[]>> pathAndDataList =
                Arrays.stream(Objects.requireNonNull(logDir.listFiles()))
                        .map(
                                file -> {
                                    try {
                                        return Tuple2.of(
                                                file.toPath(), Files.readAllBytes(file.toPath()));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toSet());

        append(stateManager, writerId, 2, 3L);
        append(stateManager, writerId, 3, 4L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(3L, 5L)));

        // Truncate to the range (3, 5), this will delete the earlier snapshot until offset 3.
        stateManager.truncateAndReload(3L, 5L, System.currentTimeMillis());
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(5L));

        // Add the snapshot files until offset 3 to the log dir.
        for (Tuple2<Path, byte[]> pathAndData : pathAndDataList) {
            Files.write(pathAndData.f0, pathAndData.f1);
        }
        // Cleanup the in-memory snapshots and reload the snapshots from log dir.
        // It loads the earlier written snapshot files from log dir.
        stateManager.truncateFullyAndReloadSnapshots();

        assertThat(stateManager.latestSnapshotOffset().get()).isEqualTo(3);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(3L));
    }

    @Test
    void testLoadFromSnapshotRetainsNonExpiredWriters() throws IOException {
        long writerId1 = 1L;
        long writerId2 = 2L;

        append(stateManager, writerId1, 0, 0L);
        append(stateManager, writerId2, 0, 1L);
        stateManager.takeSnapshot();
        assertThat(stateManager.activeWriters().size()).isEqualTo(2);

        stateManager.truncateAndReload(1L, 2L, System.currentTimeMillis());
        assertThat(stateManager.activeWriters().size()).isEqualTo(2);

        Optional<WriterStateEntry> entry1 = stateManager.lastEntry(writerId1);
        assertThat(entry1).isPresent();
        assertThat(entry1.get().lastBatchSequence()).isEqualTo(0);
        assertThat(entry1.get().lastDataOffset()).isEqualTo(0L);

        Optional<WriterStateEntry> entry2 = stateManager.lastEntry(writerId2);
        assertThat(entry2).isPresent();
        assertThat(entry2.get().lastBatchSequence()).isEqualTo(0);
        assertThat(entry2.get().lastDataOffset()).isEqualTo(1L);
    }

    @Test
    void testSkipSnapshotIfOffsetUnchanged() throws IOException {
        append(stateManager, writerId, 0, 0L, false, 0L);

        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));

        // nothing changed so there should be no new snapshot.
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));
    }

    @Test
    void testWriterExpirationTimeout() throws Exception {
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofSeconds(3));
        WriterStateManager stateManager1 =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        append(stateManager1, writerId, 0, 1L);
        stateManager1.removeExpiredWriters(System.currentTimeMillis() + 4000L);

        assertThatThrownBy(() -> append(stateManager1, writerId, 2, 2L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 2 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 2 (incoming batch seq.), -1 (current batch seq.)");

        append(stateManager1, writerId, 0, 2L);
        assertThat(stateManager1.activeWriters().size()).isEqualTo(1);
        assertThat(stateManager1.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(0);
        assertThat(stateManager1.mapEndOffset()).isEqualTo(3L);
    }

    @Test
    void testLoadFromEmptySnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        fileChannel.truncate(0L);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testLoadFromTruncatedSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        // truncate to some arbitrary point in the middle of the snapshot.
                        assertThat(fileChannel.size()).isGreaterThan(2);
                        fileChannel.truncate(fileChannel.size() / 2);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testLoadFromCorruptSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        // write some garbage somewhere in the file.
                        assertThat(fileChannel.size()).isGreaterThan(2);
                        fileChannel.write(
                                ByteBuffer.wrap(new byte[] {1, 2, 3}), fileChannel.size() / 2);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testRemoveStraySnapshotsKeepCleanShutdownSnapshot() throws IOException {
        // Test that when stray snapshots are removed, the largest stray snapshot is kept around.
        // This covers the case where the tablet server shutdown cleanly and emitted a snapshot file
        // larger than the base offset of the active segment.

        // Create 3 snapshot files at different offsets.
        Files.createFile(writerSnapshotFile(logDir, 5).toPath()); // not stray.
        Files.createFile(writerSnapshotFile(logDir, 2).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 42).toPath()); // not stray.

        // claim that we only have one segment with a base offset of 5.
        stateManager.removeStraySnapshots(Collections.singleton(5L));

        // The snapshot file at offset 2 should be considered a stray, but the snapshot at 42 should
        // be kept around because it is the largest snapshot.
        assertThat(stateManager.latestSnapshotOffset()).isEqualTo(Optional.of(42L));
        assertThat(stateManager.oldestSnapshotOffset()).isEqualTo(Optional.of(5L));
        assertThat(listSnapshotFiles(logDir).stream().map(snapshotFile -> snapshotFile.offset))
                .containsExactlyInAnyOrderElementsOf(new HashSet<>(Arrays.asList(5L, 42L)));
    }

    @Test
    void testRemoveAllStraySnapshots() throws IOException {
        // Test that when stray snapshots are removed, we remove only the stray snapshots below the
        // largest segment base offset. Snapshots associated with an offset in the list of segment
        // base offsets should remain.

        // Create 3 snapshot files at different offsets.
        Files.createFile(writerSnapshotFile(logDir, 5).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 2).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 42).toPath()); // not stray.

        stateManager.removeStraySnapshots(Collections.singleton(42L));
        assertThat(listSnapshotFiles(logDir).stream().map(snapshotFile -> snapshotFile.offset))
                .containsExactlyInAnyOrderElementsOf(Collections.singleton(42L));
    }

    private void testLoadFromCorruptSnapshot(Consumer<FileChannel> makeFileCorrupt)
            throws IOException {
        long writerId = 1L;

        append(stateManager, writerId, 0, 0L);
        stateManager.takeSnapshot();
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();

        // Truncate the last snapshot.
        Optional<Long> latestSnapshotOffset = stateManager.latestSnapshotOffset();
        assertThat(latestSnapshotOffset.get()).isEqualTo(2L);

        File snapshotToTruncate = writerSnapshotFile(logDir, latestSnapshotOffset.get());

        try (FileChannel channel =
                FileChannel.open(snapshotToTruncate.toPath(), StandardOpenOption.WRITE)) {
            makeFileCorrupt.accept(channel);
        }

        // Ensure that the truncated snapshot is deleted and writer state is loaded from the
        // previous snapshot.
        WriterStateManager reloadedStateManager =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        reloadedStateManager.truncateAndReload(0L, 20L, System.currentTimeMillis());
        assertThat(snapshotToTruncate.exists()).isFalse();

        WriterStateEntry loadedWriterState = reloadedStateManager.activeWriters().get(writerId);
        assertThat(loadedWriterState).isNotNull();
        assertThat(loadedWriterState.lastDataOffset()).isEqualTo(0L);
    }

    private void append(
            WriterStateManager stateManager, long writerId, int batchSequence, long offset) {
        append(stateManager, writerId, batchSequence, offset, false, System.currentTimeMillis());
    }

    private void append(
            WriterStateManager stateManager,
            long writerId,
            int batchSequence,
            long offset,
            boolean isWriterInBatchExpired,
            long lastTimestamp) {
        WriterAppendInfo appendInfo = stateManager.prepareUpdate(writerId);
        appendInfo.appendDataBatch(
                batchSequence,
                new LogOffsetMetadata(offset),
                offset,
                isWriterInBatchExpired,
                true,
                lastTimestamp);
        stateManager.update(appendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private WriterStateManager progressManager() throws IOException {
        return new WriterStateManager(
                tableBucket,
                logDir,
                (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis(),
                KvIdempotenceProtocol.CUMULATIVE_PROGRESS);
    }

    private static void appendProgress(
            WriterStateManager manager,
            WriterKey writerKey,
            long progress,
            long targetWalOffset,
            long timestamp) {
        WriterProgressAppendInfo appendInfo = manager.prepareProgressUpdate(writerKey);
        appendInfo.append(progress, targetWalOffset, timestamp);
        manager.updateProgress(appendInfo);
    }

    private static byte[] v2SnapshotBytes() {
        return v2SnapshotBytes(100L, 10L);
    }

    private static byte[] v2SnapshotBytes(long progress, long targetOffset) {
        return ("{\"version\":2,\"kv_idempotence_protocol_version\":1,\"writer_entries\":[{"
                        + "\"writer_key_high\":4,\"writer_key_low\":5,\"last_sequence\":"
                        + progress
                        + ",\"last_target_wal_offset\":"
                        + targetOffset
                        + ",\"last_timestamp\":1}]}")
                .getBytes(StandardCharsets.UTF_8);
    }

    private Set<Long> currentSnapshotOffsets() {
        Set<Long> offsets = new HashSet<>();
        for (File file : Objects.requireNonNull(logDir.listFiles())) {
            offsets.add(offsetFromFile(file));
        }

        return offsets;
    }
}
