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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionTombstoneTest {

    @Test
    void testEmptyTombstoneAcceptsAllPartitions() {
        PartitionTombstone tombstone = PartitionTombstone.EMPTY;
        assertThat(tombstone.getFloor()).isEqualTo(-1L);
        assertThat(tombstone.getExplicitSet()).isEmpty();
        assertThat(tombstone.getVersion()).isEqualTo(0L);
        assertThat(tombstone.isTombstoned(0L)).isFalse();
        assertThat(tombstone.isTombstoned(100L)).isFalse();
    }

    @Test
    void testFloorTombstonesAllAtOrBelow() {
        PartitionTombstone tombstone =
                new PartitionTombstone(5L, java.util.Collections.emptySet(), 1L);
        assertThat(tombstone.isTombstoned(0L)).isTrue();
        assertThat(tombstone.isTombstoned(5L)).isTrue();
        assertThat(tombstone.isTombstoned(6L)).isFalse();
    }

    @Test
    void testExplicitSetTombstonesListedPartitions() {
        Set<Long> set = new HashSet<>();
        set.add(7L);
        set.add(9L);
        PartitionTombstone tombstone = new PartitionTombstone(-1L, set, 2L);
        assertThat(tombstone.isTombstoned(7L)).isTrue();
        assertThat(tombstone.isTombstoned(9L)).isTrue();
        assertThat(tombstone.isTombstoned(8L)).isFalse();
    }

    @Test
    void testFloorAndExplicitCombineWithUnionSemantics() {
        Set<Long> set = new HashSet<>();
        set.add(10L);
        PartitionTombstone tombstone = new PartitionTombstone(5L, set, 3L);
        assertThat(tombstone.isTombstoned(3L)).isTrue();
        assertThat(tombstone.isTombstoned(7L)).isFalse();
        assertThat(tombstone.isTombstoned(10L)).isTrue();
    }

    @Test
    void testExplicitSetIsDefensiveCopy() {
        Set<Long> set = new HashSet<>();
        set.add(5L);
        PartitionTombstone tombstone = new PartitionTombstone(-1L, set, 1L);
        set.add(99L);
        assertThat(tombstone.isTombstoned(99L)).isFalse();
    }

    @Test
    void testGetExplicitSetIsImmutable() {
        Set<Long> set = new HashSet<>();
        set.add(5L);
        PartitionTombstone tombstone = new PartitionTombstone(-1L, set, 1L);
        assertThatThrownBy(() -> tombstone.getExplicitSet().add(99L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEqualsAndHashCodeUseAllFields() {
        PartitionTombstone a =
                new PartitionTombstone(5L, java.util.Collections.singleton(7L), 1L);
        PartitionTombstone b =
                new PartitionTombstone(5L, java.util.Collections.singleton(7L), 1L);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void testRejectsNullExplicitSet() {
        assertThatThrownBy(() -> new PartitionTombstone(0L, null, 1L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testRejectsNegativeVersion() {
        assertThatThrownBy(
                        () -> new PartitionTombstone(0L, java.util.Collections.emptySet(), -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
