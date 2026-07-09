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

import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;

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
        return new IndexSpec(
                indexName,
                visibility,
                1L,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                row -> new byte[] {1},
                row -> null,
                row -> 0);
    }

    @Test
    void offsetAdvancesOnlyAfterAllBatchesAcked() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(0L, (sync, all) -> advanced.set(sync));
        IndexWindow window = new IndexWindow("idx", 100L, 3, replicator);

        window.onBatchAcked();
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        window.onBatchAcked();
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        // The final ack completes the window and advances the pushed offset.
        window.onBatchAcked();
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(100L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(100L);
        assertThat(advanced.get()).isEqualTo(100L);
    }

    @Test
    void singleBatchWindowCompletesImmediately() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(10L, (sync, all) -> advanced.set(sync));
        IndexWindow window = new IndexWindow("idx", 42L, 1, replicator);

        window.onBatchAcked();
        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(42L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(42L);
        assertThat(advanced.get()).isEqualTo(42L);
    }

    @Test
    void windowCompletionFiresWakeupSignal() {
        IndexReplicator replicator = newReplicator(0L, (sync, all) -> {});
        AtomicBoolean woke = new AtomicBoolean(false);
        replicator.setWakeupSignal(() -> woke.set(true));

        IndexWindow window = new IndexWindow("idx", 7L, 1, replicator);
        window.onBatchAcked();

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

        new IndexWindow("idx_sync", 100L, 1, replicator).onBatchAcked();

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

        new IndexWindow("idx_sync_a", 20L, 1, replicator).onBatchAcked();
        new IndexWindow("idx_sync_b", 10L, 1, replicator).onBatchAcked();

        assertThat(replicator.getSyncIndexPushedOffset()).isEqualTo(10L);
        assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(10L);
    }
}
