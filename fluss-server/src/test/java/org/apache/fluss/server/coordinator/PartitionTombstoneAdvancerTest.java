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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.PartitionTombstone;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionTombstoneAdvancer}. */
class PartitionTombstoneAdvancerTest {

    @Test
    void testDropPartitionAddsToExplicitSetAndIncrementsVersion() {
        PartitionTombstone before = PartitionTombstone.EMPTY;
        PartitionTombstone after = PartitionTombstoneAdvancer.dropPartition(before, 5L);
        assertThat(after.getFloor()).isEqualTo(-1L);
        assertThat(after.getExplicitSet()).containsExactly(5L);
        assertThat(after.getVersion()).isEqualTo(1L);
    }

    @Test
    void testDropContiguousAdvancesFloor() {
        PartitionTombstone t0 = PartitionTombstone.EMPTY;
        PartitionTombstone t1 = PartitionTombstoneAdvancer.dropPartition(t0, 0L);
        // 0 is contiguous with floor (-1), so floor advances to 0.
        assertThat(t1.getFloor()).isEqualTo(0L);
        assertThat(t1.getExplicitSet()).isEmpty();

        PartitionTombstone t2 = PartitionTombstoneAdvancer.dropPartition(t1, 1L);
        assertThat(t2.getFloor()).isEqualTo(1L);
        assertThat(t2.getExplicitSet()).isEmpty();
    }

    @Test
    void testDropOutOfOrderAbsorbsBelowOnFloorAdvance() {
        PartitionTombstone t0 = PartitionTombstone.EMPTY;
        PartitionTombstone t1 = PartitionTombstoneAdvancer.dropPartition(t0, 2L); // {2}
        PartitionTombstone t2 = PartitionTombstoneAdvancer.dropPartition(t1, 0L); // floor=0, {2}
        PartitionTombstone t3 = PartitionTombstoneAdvancer.dropPartition(t2, 1L); // floor=2, {}

        assertThat(t3.getFloor()).isEqualTo(2L);
        assertThat(t3.getExplicitSet()).isEmpty();
        assertThat(t3.getVersion()).isEqualTo(3L);
    }

    @Test
    void testDropAlreadyTombstonedIsNoOpExceptVersionBump() {
        Set<Long> empty = new HashSet<>();
        PartitionTombstone before = new PartitionTombstone(5L, empty, 10L);
        PartitionTombstone after = PartitionTombstoneAdvancer.dropPartition(before, 3L);
        assertThat(after.getFloor()).isEqualTo(5L);
        assertThat(after.getExplicitSet()).isEmpty();
        assertThat(after.getVersion()).isEqualTo(11L);
    }

    @Test
    void testDropContiguousChainAdvancesFloorMultipleTimes() {
        Set<Long> existing = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        PartitionTombstone before = new PartitionTombstone(-1L, existing, 5L);
        PartitionTombstone after = PartitionTombstoneAdvancer.dropPartition(before, 0L);
        // dropping 0: explicit becomes {0,1,2,3}, then floor advances 0→1→2→3, explicit becomes {}.
        assertThat(after.getFloor()).isEqualTo(3L);
        assertThat(after.getExplicitSet()).isEmpty();
        assertThat(after.getVersion()).isEqualTo(6L);
    }
}
