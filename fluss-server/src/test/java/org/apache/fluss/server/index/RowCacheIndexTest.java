/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.junit.jupiter.api.Test;

import java.util.NavigableMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowCacheIndex}. */
class RowCacheIndexTest {

    @Test
    void testBasicIndexOperations() {
        RowCacheIndex index = new RowCacheIndex();

        // Test initial state
        assertThat(index.isEmpty()).isTrue();
        assertThat(index.size()).isEqualTo(0);

        // Add some index entries
        RowCacheIndexEntry entry1 = new RowCacheIndexEntry(0, 0, 100);
        RowCacheIndexEntry entry2 = new RowCacheIndexEntry(0, 100, 150);
        RowCacheIndexEntry entry3 = new RowCacheIndexEntry(1, 0, 200);

        index.addIndexEntry(10L, entry1);
        index.addIndexEntry(20L, entry2);
        index.addIndexEntry(30L, entry3);

        // Verify basic operations
        assertThat(index.isEmpty()).isFalse();
        assertThat(index.size()).isEqualTo(3);

        // Test retrieval
        assertThat(index.getIndexEntry(10L)).isEqualTo(entry1);
        assertThat(index.getIndexEntry(20L)).isEqualTo(entry2);
        assertThat(index.getIndexEntry(30L)).isEqualTo(entry3);
        assertThat(index.getIndexEntry(40L)).isNull();
    }

    @Test
    void testRangeQueries() {
        RowCacheIndex index = new RowCacheIndex();

        // Add test data
        index.addIndexEntry(10L, new RowCacheIndexEntry(0, 0, 100));
        index.addIndexEntry(20L, new RowCacheIndexEntry(0, 100, 150));
        index.addIndexEntry(30L, new RowCacheIndexEntry(1, 0, 200));
        index.addIndexEntry(40L, new RowCacheIndexEntry(1, 200, 250));

        // Test range queries
        NavigableMap<Long, RowCacheIndexEntry> range1 = index.getEntriesInRange(10L, 25L);
        assertThat(range1).hasSize(2);
        assertThat(range1.keySet()).containsExactly(10L, 20L);

        NavigableMap<Long, RowCacheIndexEntry> range2 = index.getEntriesInRange(25L, 45L);
        assertThat(range2).hasSize(2);
        assertThat(range2.keySet()).containsExactly(30L, 40L);

        NavigableMap<Long, RowCacheIndexEntry> range3 = index.getEntriesInRange(35L, 50L);
        assertThat(range3).hasSize(1);
        assertThat(range3.keySet()).containsExactly(40L);
    }
}
