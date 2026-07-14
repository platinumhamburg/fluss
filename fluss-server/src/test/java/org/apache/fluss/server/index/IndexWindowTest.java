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

package org.apache.fluss.server.index;

import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexWindow} acknowledgement counting and offset advancement. */
public class IndexWindowTest {

    private static IndexReplicator newReplicator(
            long initialOffset, IndexReplicator.IndexProgressListener onAdvanced) {
        // logTablet / readContext are not touched by onWindowComplete, so null is safe here.
        return new IndexReplicator(
                null,
                Collections.singletonList(spec("idx", IndexVisibility.SYNC)),
                new IndexAccumulator(),
                null,
                initialOffset,
                1024,
                onAdvanced);
    }

    private static IndexReplicator newReplicator(
            long initialOffset,
            IndexSpec first,
            IndexSpec second,
            IndexReplicator.IndexProgressListener onAdvanced) {
        return new IndexReplicator(
                null,
                Arrays.asList(first, second),
                new IndexAccumulator(),
                null,
                initialOffset,
                1024,
                onAdvanced);
    }

    private static IndexSpec spec(String indexName, IndexVisibility visibility) {
        RowEncoder rowEncoder =
                RowEncoder.create(KvFormat.COMPACTED, new DataType[] {DataTypes.BIGINT()});
        return new IndexSpec(
                indexName,
                visibility,
                1L,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> {
                    rowEncoder.startNewRow();
                    rowEncoder.encodeField(0, row.getLong(0));
                    BinaryRow value = rowEncoder.finishRow();
                    return new IndexSpec.IndexEntry(new byte[] {1}, value, 0);
                });
    }

    private static IndexBatch batch(IndexWindow window, int bucket) {
        byte[] bytes = new byte[] {1};
        return new IndexBatch(
                new TableBucket(1L, bucket),
                new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, bytes.length),
                window);
    }

    @Test
    void admissionStateIsSeparateFromWindowActivity() {
        IndexReplicator replicator = newReplicator(0L, (sync, all) -> {});
        IndexWindow window = new IndexWindow("idx", 10L, 1, replicator);
        IndexBatch batch = batch(window, 0);

        assertThat(window.isActive()).isTrue();
        assertThat(window.isAdmitted()).isFalse();
        assertThat(window.expectedBatchCount()).isEqualTo(1);

        window.markAdmitted();
        assertThat(window.isActive()).isTrue();
        assertThat(window.isAdmitted()).isTrue();

        assertThat(window.tryRetireAndDrain()).containsExactly(batch);
        assertThat(window.isActive()).isFalse();
        assertThat(window.isAdmitted()).isTrue();
        assertThat(window.expectedBatchCount()).isEqualTo(1);
    }

    @Test
    void offsetAdvancesOnlyAfterAllBatchesAcked() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(0L, (sync, all) -> advanced.set(sync));
        IndexWindow window = new IndexWindow("idx", 100L, 3, replicator);
        IndexBatch first = batch(window, 0);
        IndexBatch second = batch(window, 1);
        IndexBatch third = batch(window, 2);

        window.onBatchAcked(first);
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        window.onBatchAcked(second);
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        // The final ack completes the window and advances the pushed offset.
        window.onBatchAcked(third);
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(100L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(100L);
        assertThat(advanced.get()).isEqualTo(100L);
    }

    @Test
    void singleBatchWindowCompletesImmediately() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(10L, (sync, all) -> advanced.set(sync));
        IndexWindow window = new IndexWindow("idx", 42L, 1, replicator);
        IndexBatch batch = batch(window, 0);

        window.onBatchAcked(batch);
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(42L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(42L);
        assertThat(advanced.get()).isEqualTo(42L);
    }

    @Test
    void completionAfterOwnerCloseCannotAdvanceProgress() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(10L, (sync, all) -> advanced.set(sync));
        replicator.close();

        replicator.onWindowComplete("idx", 42L);

        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(advanced).hasValue(-1L);
    }

    @Test
    void terminalBatchFailurePreventsWindowFromAdvancing() {
        IndexReplicator replicator = newReplicator(10L, (sync, all) -> {});
        IndexWindow window = new IndexWindow("idx", 42L, 1, replicator);
        IndexBatch batch = batch(window, 0);
        RecordTooLargeException failure = new RecordTooLargeException("too large");

        assertThat(window.tryFailAndDrain(failure)).containsExactly(batch);
        window.onBatchAcked(batch);

        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(replicator.terminalFailure()).isSameAs(failure);
    }

    @Test
    void windowCompletionFiresWakeupSignal() {
        IndexReplicator replicator = newReplicator(0L, (sync, all) -> {});
        AtomicBoolean woke = new AtomicBoolean(false);
        replicator.setWakeupSignal(() -> woke.set(true));

        IndexWindow window = new IndexWindow("idx", 7L, 1, replicator);
        window.onBatchAcked(batch(window, 0));

        assertThat(woke).isTrue();
    }

    @Test
    void asyncIndexWindowDoesNotBlockSyncProgress() {
        AtomicLong syncProgress = new AtomicLong(-1L);
        AtomicLong allProgress = new AtomicLong(-1L);
        IndexReplicator replicator =
                newReplicator(
                        0L,
                        spec("idx_sync", IndexVisibility.SYNC),
                        spec("idx_async", IndexVisibility.ASYNC),
                        (sync, all) -> {
                            syncProgress.set(sync);
                            allProgress.set(all);
                        });

        IndexWindow window = new IndexWindow("idx_sync", 100L, 1, replicator);
        window.onBatchAcked(batch(window, 0));

        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(100L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(0L);
        assertThat(syncProgress.get()).isEqualTo(100L);
        assertThat(allProgress.get()).isEqualTo(0L);
    }

    @Test
    void syncWatermarkIsMinOfSyncIndexesOnly() {
        IndexReplicator replicator =
                newReplicator(
                        0L,
                        spec("idx_sync_a", IndexVisibility.SYNC),
                        spec("idx_sync_b", IndexVisibility.SYNC),
                        (sync, all) -> {});

        IndexWindow first = new IndexWindow("idx_sync_a", 20L, 1, replicator);
        first.onBatchAcked(batch(first, 0));
        IndexWindow second = new IndexWindow("idx_sync_b", 10L, 1, replicator);
        second.onBatchAcked(batch(second, 1));

        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(10L);
    }
}
