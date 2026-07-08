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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionTombstoneAdvancer}. */
class PartitionTombstoneAdvancerTest {

    @Test
    void testSparseAlivePartitionsAdvanceFloorToBeforeMinimumAlive() {
        PartitionTombstone before = new PartitionTombstone(0L, asSet(2L, 10L), 3L);

        PartitionTombstone after =
                PartitionTombstoneAdvancer.dropPartition(before, 100L, asSet(101L, 300L));

        assertThat(after.getFloor()).isEqualTo(100L);
        assertThat(after.getExplicitSet()).isEmpty();
        assertThat(after.getVersion()).isEqualTo(4L);
        assertThat(after.isTombstoned(2L)).isTrue();
        assertThat(after.isTombstoned(100L)).isTrue();
        assertThat(after.isTombstoned(101L)).isFalse();
    }

    @Test
    void testSparseHighDropAboveMinimumAliveStaysExplicit() {
        PartitionTombstone before = new PartitionTombstone(0L, Collections.emptySet(), 1L);

        PartitionTombstone after =
                PartitionTombstoneAdvancer.dropPartition(before, 100L, asSet(10L, 200L));

        assertThat(after.getFloor()).isEqualTo(9L);
        assertThat(after.getExplicitSet()).containsExactly(100L);
        assertThat(after.isTombstoned(9L)).isTrue();
        assertThat(after.isTombstoned(10L)).isFalse();
        assertThat(after.isTombstoned(100L)).isTrue();
    }

    @Test
    void testEmptyAliveSetFoldsKnownDroppedIdsIntoFloor() {
        PartitionTombstone before = new PartitionTombstone(0L, asSet(2L, 50L), 7L);

        PartitionTombstone after =
                PartitionTombstoneAdvancer.dropPartition(before, 100L, Collections.emptySet());

        assertThat(after.getFloor()).isEqualTo(100L);
        assertThat(after.getExplicitSet()).isEmpty();
    }

    @Test
    void testConservativeDropWithoutAliveSnapshotOnlyAddsExplicit() {
        PartitionTombstone before = new PartitionTombstone(0L, Collections.emptySet(), 1L);

        PartitionTombstone after = PartitionTombstoneAdvancer.dropPartition(before, 100L);

        assertThat(after.getFloor()).isEqualTo(0L);
        assertThat(after.getExplicitSet()).containsExactly(100L);
    }

    @Test
    void testDropAlreadyTombstonedIsNoOpExceptVersionBump() {
        PartitionTombstone before = new PartitionTombstone(5L, Collections.emptySet(), 10L);

        PartitionTombstone after = PartitionTombstoneAdvancer.dropPartition(before, 3L);

        assertThat(after.getFloor()).isEqualTo(5L);
        assertThat(after.getExplicitSet()).isEmpty();
        assertThat(after.getVersion()).isEqualTo(11L);
    }

    @Test
    void testAlivePartitionAtOrBelowExistingFloorIsRejected() {
        PartitionTombstone before = new PartitionTombstone(10L, Collections.emptySet(), 1L);

        assertThatThrownBy(() -> PartitionTombstoneAdvancer.dropPartition(before, 20L, asSet(10L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than tombstone floor");
    }

    @Test
    void testDroppedPartitionStillAliveIsRejected() {
        PartitionTombstone before = new PartitionTombstone(0L, Collections.emptySet(), 1L);

        assertThatThrownBy(
                        () -> PartitionTombstoneAdvancer.dropPartition(before, 20L, asSet(20L, 30L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not remain in alive partition ids");
    }

    @Test
    void testValidateNewPartitionIdRejectsPartitionAtFloor() {
        PartitionTombstone tombstone = new PartitionTombstone(100L, Collections.emptySet(), 1L);

        assertThatThrownBy(() -> PartitionTombstoneAdvancer.validateNewPartitionId(tombstone, 100L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than tombstone floor");
    }

    @Test
    void testValidateNewPartitionIdAcceptsPartitionAboveFloor() {
        PartitionTombstone tombstone = new PartitionTombstone(100L, Collections.emptySet(), 1L);

        PartitionTombstoneAdvancer.validateNewPartitionId(tombstone, 101L);
    }

    @Test
    void testShouldWarnWhenExplicitSetReachesThreshold() {
        PartitionTombstone tombstone =
                new PartitionTombstone(
                        -1L,
                        LongStream.range(0L, PartitionTombstoneAdvancer.EXPLICIT_SET_WARNING_THRESHOLD)
                                .boxed()
                                .collect(Collectors.toSet()),
                        1L);

        assertThat(PartitionTombstoneAdvancer.shouldWarnForLargeTombstone(tombstone)).isTrue();
    }

    @Test
    void testShouldNotWarnForSmallTombstone() {
        PartitionTombstone tombstone = new PartitionTombstone(10L, asSet(20L, 30L), 1L);

        assertThat(PartitionTombstoneAdvancer.shouldWarnForLargeTombstone(tombstone)).isFalse();
    }

    private static Set<Long> asSet(long... values) {
        return LongStream.of(values).boxed().collect(Collectors.toSet());
    }
}
