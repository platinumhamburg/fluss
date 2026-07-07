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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexWindow} acknowledgement counting and offset advancement. */
public class IndexWindowTest {

    private static IndexReplicator newReplicator(long initialOffset, LongConsumer onAdvanced) {
        // logTablet / readContext are not touched by onWindowComplete, so null is safe here.
        return new IndexReplicator(
                null,
                Collections.emptyList(),
                new IndexAccumulator(),
                null,
                initialOffset,
                1024,
                onAdvanced);
    }

    @Test
    void offsetAdvancesOnlyAfterAllBatchesAcked() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(0L, advanced::set);
        IndexWindow window = new IndexWindow(100L, 3, replicator);

        window.onBatchAcked();
        assertThat(replicator.getIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        window.onBatchAcked();
        assertThat(replicator.getIndexPushedOffset()).isEqualTo(0L);
        assertThat(advanced.get()).isEqualTo(-1L);

        // The final ack completes the window and advances the pushed offset.
        window.onBatchAcked();
        assertThat(replicator.getIndexPushedOffset()).isEqualTo(100L);
        assertThat(advanced.get()).isEqualTo(100L);
    }

    @Test
    void singleBatchWindowCompletesImmediately() {
        AtomicLong advanced = new AtomicLong(-1L);
        IndexReplicator replicator = newReplicator(10L, advanced::set);
        IndexWindow window = new IndexWindow(42L, 1, replicator);

        window.onBatchAcked();
        assertThat(replicator.getIndexPushedOffset()).isEqualTo(42L);
        assertThat(advanced.get()).isEqualTo(42L);
    }

    @Test
    void windowCompletionFiresWakeupSignal() {
        IndexReplicator replicator = newReplicator(0L, off -> {});
        AtomicBoolean woke = new AtomicBoolean(false);
        replicator.setWakeupSignal(() -> woke.set(true));

        IndexWindow window = new IndexWindow(7L, 1, replicator);
        window.onBatchAcked();

        assertThat(woke).isTrue();
    }
}
