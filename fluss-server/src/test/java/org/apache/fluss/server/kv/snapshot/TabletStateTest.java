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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TabletState}. */
class TabletStateTest {

    @Test
    void minRetainLogOffsetFallsBackToFlushedLogOffsetWithoutIndexWatermark() {
        TabletState state = new TabletState(100L, 10L, null);

        assertThat(state.getMinRetainLogOffset()).isEqualTo(100L);
    }

    @Test
    void minRetainLogOffsetKeepsEarlierIndexWatermark() {
        TabletState state = new TabletState(100L, 10L, 60L, null);

        assertThat(state.getMinRetainLogOffset()).isEqualTo(60L);
    }

    @Test
    void minRetainLogOffsetUsesFlushedLogOffsetWhenIndexHasCaughtUp() {
        TabletState state = new TabletState(100L, 10L, 120L, null);

        assertThat(state.getMinRetainLogOffset()).isEqualTo(100L);
    }

    @Test
    void completedSnapshotMinRetainLogOffsetKeepsEarlierIndexWatermark() {
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        new TableBucket(1L, 0),
                        1L,
                        new FsPath("file:///tmp/snapshot"),
                        null,
                        100L,
                        10L,
                        60L,
                        null);

        assertThat(snapshot.getMinRetainLogOffset()).isEqualTo(60L);
    }
}
