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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionTombstoneFilter}. */
class PartitionTombstoneFilterTest {

    private static TabletServerMetadataCache newCache() {
        return new TabletServerMetadataCache(
                new MetadataManager(
                        null,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true)));
    }

    @Test
    void testDropsValueWhosePartitionIdIsTombstoned() {
        byte[] body = new byte[] {1, 2};
        byte[] value = IndexTableUtils.prependPartitionIdPrefix(5L, body);
        TabletServerMetadataCache cache = newCache();
        cache.updatePartitionTombstone(
                42L, new PartitionTombstone(5L, Collections.emptySet(), 1L));
        assertThat(PartitionTombstoneFilter.shouldDrop(value, 42L, cache)).isTrue();
    }

    @Test
    void testKeepsValueWhosePartitionIdIsAlive() {
        byte[] body = new byte[] {1, 2};
        byte[] value = IndexTableUtils.prependPartitionIdPrefix(10L, body);
        TabletServerMetadataCache cache = newCache();
        cache.updatePartitionTombstone(
                42L, new PartitionTombstone(5L, Collections.emptySet(), 1L));
        assertThat(PartitionTombstoneFilter.shouldDrop(value, 42L, cache)).isFalse();
    }

    @Test
    void testNullValueIsNeverDropped() {
        TabletServerMetadataCache cache = newCache();
        cache.updatePartitionTombstone(
                42L, new PartitionTombstone(99L, Collections.emptySet(), 1L));
        assertThat(PartitionTombstoneFilter.shouldDrop(null, 42L, cache)).isFalse();
    }

    @Test
    void testValueShorterThanPrefixIsNeverDropped() {
        TabletServerMetadataCache cache = newCache();
        cache.updatePartitionTombstone(
                42L, new PartitionTombstone(99L, Collections.emptySet(), 1L));
        assertThat(PartitionTombstoneFilter.shouldDrop(new byte[] {0, 1, 2}, 42L, cache))
                .isFalse();
    }

    @Test
    void testEmptyTombstoneKeepsAllValues() {
        byte[] value = IndexTableUtils.prependPartitionIdPrefix(5L, new byte[] {1});
        TabletServerMetadataCache cache = newCache();
        // No tombstone update — getPartitionTombstone returns EMPTY by default.
        assertThat(PartitionTombstoneFilter.shouldDrop(value, 42L, cache)).isFalse();
    }
}
