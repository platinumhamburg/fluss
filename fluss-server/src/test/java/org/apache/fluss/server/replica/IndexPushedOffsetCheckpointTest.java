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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.server.replica.ReplicaManager.INDEX_PUSHED_OFFSET_CHECKPOINT_FILE_NAME;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code index-pushed-offset-checkpoint} file written by {@link
 * ReplicaManager#checkpointIndexPushedOffsets()} (P2T10).
 */
final class IndexPushedOffsetCheckpointTest extends ReplicaTestBase {

    @Test
    void testOffsetCheckpointFileRoundTrip(@TempDir File tmpDir) throws Exception {
        File file = new File(tmpDir, INDEX_PUSHED_OFFSET_CHECKPOINT_FILE_NAME);
        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(file);

        // empty round-trip.
        checkpoint.write(new HashMap<>());
        assertThat(checkpoint.read()).isEmpty();

        // three entries round-trip (mix of unpartitioned and partitioned buckets).
        Map<TableBucket, Long> offsets = new HashMap<>();
        TableBucket tb0 = new TableBucket(150001L, 0);
        TableBucket tb1 = new TableBucket(150001L, 1);
        TableBucket tbPart = new TableBucket(150002L, 20241121L, 2);
        offsets.put(tb0, 10L);
        offsets.put(tb1, 25L);
        offsets.put(tbPart, 999L);
        checkpoint.write(offsets);

        Map<TableBucket, Long> read = checkpoint.read();
        assertThat(read).hasSize(3);
        assertThat(read).containsEntry(tb0, 10L);
        assertThat(read).containsEntry(tb1, 25L);
        assertThat(read).containsEntry(tbPart, 999L);
    }

    @Test
    void testReplicaManagerCheckpointIndexPushedOffsets() throws Exception {
        // No replicas yet -> writing checkpoint is a no-op and produces no file content.
        replicaManager.checkpointIndexPushedOffsets();
        assertThat(indexPushedOffsetFor(new TableBucket(DATA1_TABLE_ID, 0))).isEqualTo(-1L);

        TableBucket tableBucket0 = new TableBucket(DATA1_TABLE_ID, 0);
        TableBucket tableBucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tableBucket0.getBucket());
        makeLogTableAsLeader(tableBucket1.getBucket());

        Replica replica0 = replicaManager.getReplicaOrException(tableBucket0);
        Replica replica1 = replicaManager.getReplicaOrException(tableBucket1);
        replica0.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        replica1.appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);

        // Replicas with no advanced offset (still -1L) are skipped.
        replicaManager.checkpointIndexPushedOffsets();
        assertThat(indexPushedOffsetFor(tableBucket0)).isEqualTo(-1L);
        assertThat(indexPushedOffsetFor(tableBucket1)).isEqualTo(-1L);

        replica0.advanceIndexPushedOffset(7L);
        replica1.advanceIndexPushedOffset(3L);
        replicaManager.checkpointIndexPushedOffsets();
        assertThat(indexPushedOffsetFor(tableBucket0)).isEqualTo(7L);
        assertThat(indexPushedOffsetFor(tableBucket1)).isEqualTo(3L);

        // Advancing only one replica persists the new value and keeps the other.
        replica0.advanceIndexPushedOffset(9L);
        replicaManager.checkpointIndexPushedOffsets();
        assertThat(indexPushedOffsetFor(tableBucket0)).isEqualTo(9L);
        assertThat(indexPushedOffsetFor(tableBucket1)).isEqualTo(3L);
    }

    private long indexPushedOffsetFor(TableBucket tableBucket) throws Exception {
        return new OffsetCheckpointFile(
                        new File(
                                conf.getString(ConfigOptions.DATA_DIR),
                                INDEX_PUSHED_OFFSET_CHECKPOINT_FILE_NAME))
                .read()
                .getOrDefault(tableBucket, -1L);
    }
}
