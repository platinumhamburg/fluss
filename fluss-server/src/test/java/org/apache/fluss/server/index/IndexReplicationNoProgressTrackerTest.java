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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class IndexReplicationNoProgressTrackerTest {

    @Test
    void tracksOnlyTimeBehindWithoutPushedOffsetProgress() {
        AtomicLong nowNanos = new AtomicLong();
        IndexReplicationNoProgressTracker tracker =
                new IndexReplicationNoProgressTracker(10L, 10L, nowNanos::get);

        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(5L));
        assertThat(tracker.noProgressTimeMs(10L, 10L)).isZero();

        tracker.update(10L, 12L);
        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(2L));
        assertThat(tracker.noProgressTimeMs(10L, 12L)).isEqualTo(2_000L);

        tracker.update(10L, 15L);
        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        assertThat(tracker.noProgressTimeMs(10L, 15L))
                .as("a growing source high watermark is not replication progress")
                .isEqualTo(3_000L);

        tracker.update(11L, 15L);
        assertThat(tracker.noProgressTimeMs(11L, 15L)).isZero();
        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(750L));
        assertThat(tracker.noProgressTimeMs(11L, 15L)).isEqualTo(750L);

        tracker.update(15L, 15L);
        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(3L));
        assertThat(tracker.noProgressTimeMs(15L, 15L)).isZero();
    }
}
