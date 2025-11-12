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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultStateChangeLogsBuilder;
import org.apache.fluss.server.log.LogTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.StateDefs.DATA_BUCKET_OFFSET_OF_INDEX;
import static org.apache.fluss.utils.FlussPaths.stateSnapshotFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketStateManager}. */
public class BucketStateManagerTest {

    private @TempDir File tempDir;
    private File logDir;
    private TableBucket tableBucket;
    private BucketStateManager stateManager;

    @BeforeEach
    public void setup() throws Exception {
        long tableId = 1001;
        logDir = LogTestUtils.makeRandomLogTabletDir(tempDir, "testDb", tableId, "testTable");
        tableBucket = new TableBucket(tableId, 0);
        stateManager = new BucketStateManager(tableBucket, logDir);
    }

    @Test
    void testBasicApplyAndGetState() {
        // Apply state changes
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        TableBucket key3 = new TableBucket(1L, 2);
        TableBucket key4 = new TableBucket(1L, 3);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key3, 300L);

        stateManager.apply(100L, builder.build().iters());

        // Verify uncommitted state can be read with readCommitted=false
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getOffset())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, false).getValue())
                .isEqualTo(300L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key4, false)).isNull();

        // Verify committed state cannot be read with readCommitted=true before commit
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)).isNull();
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)).isNull();

        // Commit and verify committed state can be read
        stateManager.commitTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getOffset())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true).getValue())
                .isEqualTo(300L);
    }

    @Test
    void testUpdateAndDeleteState() {
        // Insert
        DefaultStateChangeLogsBuilder insertBuilder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        insertBuilder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, insertBuilder.build().iters());
        // Read uncommitted state
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(100L);
        // Cannot read committed state before commit
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();

        // Commit and verify
        stateManager.commitTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);

        // Update
        DefaultStateChangeLogsBuilder updateBuilder = new DefaultStateChangeLogsBuilder();
        updateBuilder.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 200L);
        stateManager.apply(200L, updateBuilder.build().iters());
        // Read uncommitted state - should see updated value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(200L);
        // Read committed state - should still see old value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);

        // Commit and verify
        stateManager.commitTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(200L);

        // Delete
        DefaultStateChangeLogsBuilder deleteBuilder = new DefaultStateChangeLogsBuilder();
        deleteBuilder.addLog(ChangeType.DELETE, DATA_BUCKET_OFFSET_OF_INDEX, key1, null);
        stateManager.apply(300L, deleteBuilder.build().iters());
        // Read uncommitted state - should see null
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false)).isNull();
        // Read committed state - should still see old value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(200L);

        // Commit and verify
        stateManager.commitTo(300L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();
    }

    @Test
    void testCommitTo() {
        // Apply uncommitted state changes
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder.build().iters());

        // Read uncommitted should return the value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(100L);
        // Read committed should return null before commit
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();

        // Commit the state
        stateManager.commitTo(100L);

        // Read committed should now return the value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        // Read uncommitted should still return the value (now from imageValue)
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(100L);
    }

    @Test
    void testTakeSnapshot() throws IOException {
        // Apply state changes
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateManager.apply(100L, builder.build().iters());
        stateManager.commitTo(100L);

        // Take snapshot (based on committed offset)
        stateManager.takeSnapshot();

        // Verify snapshot file exists
        File snapshotFile = stateSnapshotFile(logDir, 100L);
        assertThat(snapshotFile).exists();

        // Verify latest snapshot offset
        assertThat(stateManager.latestSnapshotOffset()).isPresent();
        assertThat(stateManager.latestSnapshotOffset().get()).isEqualTo(100L);
    }

    @Test
    void testRestoreFromSnapshot() throws IOException {
        // Apply and snapshot state
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateManager.apply(100L, builder.build().iters());
        stateManager.commitTo(100L);
        stateManager.takeSnapshot();

        // Create a new state manager and restore
        BucketStateManager newStateManager = new BucketStateManager(tableBucket, logDir);
        File snapshotFile = stateSnapshotFile(logDir, 100L);
        newStateManager.restore(snapshotFile, 100L);

        // Verify restored state
        assertThat(newStateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        assertThat(newStateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(200L);
        assertThat(newStateManager.lastCommittedOffset()).isEqualTo(100L);
    }

    @Test
    void testSnapshotOnlyTakenOnceForOffset() throws IOException {
        // Apply state changes
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder.build().iters());
        stateManager.commitTo(100L);

        // Take snapshot
        stateManager.takeSnapshot();

        // Take snapshot again at the same committed offset
        stateManager.takeSnapshot();

        // Verify only one snapshot file exists
        File snapshotFile = stateSnapshotFile(logDir, 100L);
        assertThat(snapshotFile).exists();

        // Count snapshot files
        File[] files = logDir.listFiles((dir, name) -> name.endsWith(".state_snapshot"));
        assertThat(files).hasSize(1);
    }

    @Test
    void testDeleteSnapshotsBefore() throws IOException {
        // Create multiple snapshots
        TableBucket key1 = new TableBucket(1L, 0);
        for (long offset = 100L; offset <= 500L; offset += 100) {
            DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
            builder.addLog(
                    ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, Long.valueOf(offset));
            stateManager.apply(offset, builder.build().iters());
            stateManager.commitTo(offset);
            stateManager.takeSnapshot();
        }

        // Verify all snapshots exist
        assertThat(stateSnapshotFile(logDir, 100L)).exists();
        assertThat(stateSnapshotFile(logDir, 200L)).exists();
        assertThat(stateSnapshotFile(logDir, 300L)).exists();
        assertThat(stateSnapshotFile(logDir, 400L)).exists();
        assertThat(stateSnapshotFile(logDir, 500L)).exists();

        // Delete snapshots before offset 300
        stateManager.deleteSnapshotsBefore(300L);

        // Verify deleted snapshots
        assertThat(stateSnapshotFile(logDir, 100L)).doesNotExist();
        assertThat(stateSnapshotFile(logDir, 200L)).doesNotExist();
        assertThat(stateSnapshotFile(logDir, 300L)).exists();
        assertThat(stateSnapshotFile(logDir, 400L)).exists();
        assertThat(stateSnapshotFile(logDir, 500L)).exists();
    }

    @Test
    void testTruncateAndReload() throws IOException {
        // Create snapshots at different offsets
        TableBucket key1 = new TableBucket(1L, 0);
        for (long offset = 100L; offset <= 500L; offset += 100) {
            DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
            builder.addLog(
                    ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, Long.valueOf(offset));
            stateManager.apply(offset, builder.build().iters());
            stateManager.commitTo(offset);
            stateManager.takeSnapshot();
        }

        // Truncate to offset 300
        stateManager.truncateAndReload(0L, 300L);

        // Verify only snapshots in range remain
        assertThat(stateSnapshotFile(logDir, 100L)).exists();
        assertThat(stateSnapshotFile(logDir, 200L)).exists();
        assertThat(stateSnapshotFile(logDir, 300L)).exists();
        assertThat(stateSnapshotFile(logDir, 400L)).doesNotExist();
        assertThat(stateSnapshotFile(logDir, 500L)).doesNotExist();

        // Verify state is restored from the latest snapshot in range
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(300L);
    }

    @Test
    void testRemoveStraySnapshots() throws IOException {
        // Create snapshots at offsets 100, 200, 300, 400, 500
        TableBucket key1 = new TableBucket(1L, 0);
        for (long offset = 100L; offset <= 500L; offset += 100) {
            DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
            builder.addLog(
                    ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, Long.valueOf(offset));
            stateManager.apply(offset, builder.build().iters());
            stateManager.commitTo(offset);
            stateManager.takeSnapshot();
        }

        // Simulate segment base offsets at 100, 300, 500
        List<Long> segmentBaseOffsets = Arrays.asList(100L, 300L, 500L);

        // Remove stray snapshots
        stateManager.removeStraySnapshots(segmentBaseOffsets);

        // Verify only snapshots with corresponding segments remain
        assertThat(stateSnapshotFile(logDir, 100L)).exists();
        assertThat(stateSnapshotFile(logDir, 300L)).exists();
        assertThat(stateSnapshotFile(logDir, 500L)).exists();

        // Stray snapshots (200, 400) should be removed, except the latest one (400)
        // which is kept for recovery
        assertThat(stateSnapshotFile(logDir, 200L)).doesNotExist();
    }

    @Test
    void testIsEmpty() throws IOException {
        // Initially empty
        assertThat(stateManager.isEmpty()).isTrue();

        // After applying state changes
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder.build().iters());
        assertThat(stateManager.isEmpty()).isFalse();
    }

    @Test
    void testLastCommittedOffset() {
        assertThat(stateManager.lastCommittedOffset()).isEqualTo(-1L);

        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder.build().iters());
        stateManager.commitTo(100L);
        assertThat(stateManager.lastCommittedOffset()).isEqualTo(100L);

        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        TableBucket key2 = new TableBucket(1L, 1);
        builder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateManager.apply(200L, builder2.build().iters());
        stateManager.commitTo(200L);
        assertThat(stateManager.lastCommittedOffset()).isEqualTo(200L);
    }

    @Test
    void testCompleteCheckpointAndRecoveryFlow() throws IOException {
        // Step 1: Apply state changes at offset 100
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        TableBucket key3 = new TableBucket(1L, 2);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateManager.apply(100L, builder1.build().iters());
        stateManager.commitTo(100L);

        // Step 2: Take snapshot at committed offset 100
        stateManager.takeSnapshot();

        // Step 3: Apply more state changes at offset 200
        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 1001L);
        builder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key3, 300L);
        stateManager.apply(200L, builder2.build().iters());
        stateManager.commitTo(200L);

        // Step 4: Take snapshot at committed offset 200
        stateManager.takeSnapshot();

        // Step 5: Simulate recovery - create new state manager
        BucketStateManager recoveredStateManager = new BucketStateManager(tableBucket, logDir);

        // Step 6: Restore from latest snapshot (offset 200)
        File snapshotFile = stateSnapshotFile(logDir, 200L);
        recoveredStateManager.restore(snapshotFile, 200L);

        // Step 7: Verify recovered state
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                .getValue())
                .isEqualTo(1001L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)
                                .getValue())
                .isEqualTo(200L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                .getValue())
                .isEqualTo(300L);
        assertThat(recoveredStateManager.lastCommittedOffset()).isEqualTo(200L);
    }

    @Test
    void testRecoveryWithReplay() throws IOException {
        // Step 1: Apply and checkpoint at offset 100
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder1.build().iters());
        stateManager.commitTo(100L);
        stateManager.takeSnapshot();

        // Step 2: Apply more changes at offset 200 but don't checkpoint
        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateManager.apply(200L, builder2.build().iters());
        stateManager.commitTo(200L);

        // Step 3: Simulate recovery - create new state manager
        BucketStateManager recoveredStateManager = new BucketStateManager(tableBucket, logDir);

        // Step 4: Restore from checkpoint at offset 100
        File snapshotFile = stateSnapshotFile(logDir, 100L);
        recoveredStateManager.restore(snapshotFile, 100L);

        // Step 5: Replay changes from offset 100 to 200
        DefaultStateChangeLogsBuilder replayBuilder = new DefaultStateChangeLogsBuilder();
        replayBuilder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        recoveredStateManager.apply(200L, replayBuilder.build().iters());
        recoveredStateManager.commitTo(200L);

        // Step 6: Verify recovered state includes both checkpointed and replayed data
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                .getValue())
                .isEqualTo(100L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)
                                .getValue())
                .isEqualTo(200L);
    }

    @Test
    void testVisibilityControl() {
        // Apply multiple state changes at different offsets
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder1.build().iters());

        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 200L);
        stateManager.apply(200L, builder2.build().iters());

        DefaultStateChangeLogsBuilder builder3 = new DefaultStateChangeLogsBuilder();
        builder3.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 300L);
        stateManager.apply(300L, builder3.build().iters());

        // Uncommitted read should see the latest value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(300L);

        // Committed read should see nothing before any commit
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();

        // Commit to offset 100
        stateManager.commitTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(300L);

        // Commit to offset 200
        stateManager.commitTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(300L);

        // Commit to offset 300
        stateManager.commitTo(300L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(300L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(300L);
    }

    @Test
    void testReadCommittedVsUncommitted() {
        // Test scenario: multiple keys with mixed committed and uncommitted states
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        TableBucket key3 = new TableBucket(1L, 2);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 1000L);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 2000L);
        stateManager.apply(100L, builder1.build().iters());
        stateManager.commitTo(100L);

        // Both reads should see committed values
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(1000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(1000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(2000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(2000L);

        // Apply uncommitted changes
        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 1001L);
        builder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key3, 3001L);
        stateManager.apply(200L, builder2.build().iters());

        // Committed read: key1 should see old value, key2 unchanged, key3 not visible
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(1000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(2000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)).isNull();

        // Uncommitted read: key1 should see new value, key2 unchanged, key3 visible
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(1001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(2000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, false).getValue())
                .isEqualTo(3001L);

        // Commit and verify all reads see committed values
        stateManager.commitTo(200L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(1001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(2000L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true).getValue())
                .isEqualTo(3001L);
    }

    @Test
    void testDeleteVisibility() {
        // Insert and commit
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateManager.apply(100L, builder1.build().iters());
        stateManager.commitTo(100L);

        // Verify key exists
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(100L);

        // Delete without commit
        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.DELETE, DATA_BUCKET_OFFSET_OF_INDEX, key1, null);
        stateManager.apply(200L, builder2.build().iters());

        // Committed read should still see the value
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        // Uncommitted read should see null
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false)).isNull();

        // Commit delete
        stateManager.commitTo(200L);
        // Both reads should see null
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false)).isNull();
    }

    @Test
    void testMultipleNamespacesVisibility() {
        // Apply changes to multiple keys
        DefaultStateChangeLogsBuilder builder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 10001L);
        builder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 20001L);
        stateManager.apply(100L, builder1.build().iters());

        // Uncommitted read should see both
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(10001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(20001L);

        // Committed read should see nothing
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)).isNull();
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)).isNull();

        // Commit
        stateManager.commitTo(100L);

        // Both committed and uncommitted reads should see values
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(10001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(20001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(10001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(20001L);

        // Update only key1 without commit
        DefaultStateChangeLogsBuilder builder2 = new DefaultStateChangeLogsBuilder();
        builder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 10002L);
        stateManager.apply(200L, builder2.build().iters());

        // Committed read: key1 sees old value, key2 unchanged
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(10001L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(20001L);

        // Uncommitted read: key1 sees new value, key2 unchanged
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, false).getValue())
                .isEqualTo(10002L);
        assertThat(stateManager.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, false).getValue())
                .isEqualTo(20001L);
    }
}
