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

package org.apache.fluss.server.metadata;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link PartitionTombstone} entries flow from the Coordinator to the TabletServer
 * via the existing {@code UpdateMetadataRequest} channel (P3T6).
 *
 * <p>Two layers are exercised:
 *
 * <ol>
 *   <li>Wire round-trip: {@link ServerRpcMessageUtils#makeUpdateMetadataRequest} → {@code
 *       toByteArray} → {@code parseFrom} → {@link
 *       ServerRpcMessageUtils#getUpdateMetadataRequestData} preserves all tombstone fields.
 *   <li>Cache application: the {@link ClusterMetadata} produced by the receiver path is fed into
 *       {@link TabletServerMetadataCache#updateClusterMetadata}, which is the same call made by
 *       {@code ReplicaManager.maybeUpdateMetadataCache} from {@code TabletService.updateMetadata}.
 *       The cache must reflect every entry afterwards.
 * </ol>
 */
class PartitionTombstoneMetadataPropagationTest {

    private static final long TABLE_A = 1001L;
    private static final long TABLE_B = 1002L;

    @Test
    void testWireRoundTripCarriesTombstoneEntries() {
        Map<Long, PartitionTombstone> tombstones = new LinkedHashMap<>();
        tombstones.put(TABLE_A, new PartitionTombstone(7L, asSet(11L, 13L), 5L));
        tombstones.put(TABLE_B, new PartitionTombstone(-1L, Collections.emptySet(), 1L));

        UpdateMetadataRequest req =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tombstones);

        // serialize and parse back to guarantee the proto wire format carries the new field.
        UpdateMetadataRequest parsed = new UpdateMetadataRequest();
        parsed.parseFrom(req.toByteArray());

        ClusterMetadata clusterMetadata =
                ServerRpcMessageUtils.getUpdateMetadataRequestData(parsed);
        Map<Long, PartitionTombstone> received = clusterMetadata.getPartitionTombstones();

        assertThat(received).hasSize(2);
        assertThat(received.get(TABLE_A))
                .isEqualTo(new PartitionTombstone(7L, asSet(11L, 13L), 5L));
        assertThat(received.get(TABLE_B))
                .isEqualTo(new PartitionTombstone(-1L, Collections.emptySet(), 1L));
    }

    @Test
    void testEmptyTombstoneMapProducesEmptyClusterMetadata() {
        UpdateMetadataRequest req =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        UpdateMetadataRequest parsed = new UpdateMetadataRequest();
        parsed.parseFrom(req.toByteArray());

        ClusterMetadata clusterMetadata =
                ServerRpcMessageUtils.getUpdateMetadataRequestData(parsed);
        assertThat(clusterMetadata.getPartitionTombstones()).isEmpty();
    }

    @Test
    void testLegacyMakeUpdateMetadataRequestDoesNotInjectTombstones() {
        // The 5-arg overload is the existing call used by callers that know nothing about
        // tombstones (e.g. NotifyLeaderAndIsr fan-out). It must remain a no-op for the new field.
        UpdateMetadataRequest req =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList());

        UpdateMetadataRequest parsed = new UpdateMetadataRequest();
        parsed.parseFrom(req.toByteArray());

        assertThat(parsed.getPartitionTombstonesCount()).isZero();
        assertThat(
                        ServerRpcMessageUtils.getUpdateMetadataRequestData(parsed)
                                .getPartitionTombstones())
                .isEmpty();
    }

    @Test
    void testTabletServerMetadataCacheAppliesTombstonesFromClusterMetadata() {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(new EmptyMetadataManager());

        PartitionTombstone tA = new PartitionTombstone(7L, asSet(11L, 13L), 5L);
        PartitionTombstone tB = new PartitionTombstone(-1L, Collections.emptySet(), 1L);

        Map<Long, PartitionTombstone> tombstones = new HashMap<>();
        tombstones.put(TABLE_A, tA);
        tombstones.put(TABLE_B, tB);

        cache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        new HashSet<>(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tombstones));

        assertThat(cache.getPartitionTombstone(TABLE_A)).isEqualTo(tA);
        assertThat(cache.getPartitionTombstone(TABLE_B)).isEqualTo(tB);
        // Unknown tables fall through to EMPTY (no cache entry).
        assertThat(cache.getPartitionTombstone(9999L)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testTombstoneOverridesPreviousVersion() {
        TabletServerMetadataCache cache = new TabletServerMetadataCache(new EmptyMetadataManager());

        PartitionTombstone first = new PartitionTombstone(0L, Collections.emptySet(), 1L);
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        new HashSet<>(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(TABLE_A, first)));
        assertThat(cache.getPartitionTombstone(TABLE_A)).isEqualTo(first);

        PartitionTombstone second = new PartitionTombstone(20L, asSet(99L), 2L);
        cache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        new HashSet<>(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(TABLE_A, second)));
        assertThat(cache.getPartitionTombstone(TABLE_A)).isEqualTo(second);
    }

    @Test
    void testEndToEndPropagationViaUpdateMetadataRequestAndCache() {
        // Exercises the exact wiring used by TabletService.updateMetadata: build an
        // UpdateMetadataRequest carrying tombstones, decode via getUpdateMetadataRequestData,
        // then apply via TabletServerMetadataCache.updateClusterMetadata. After this the
        // metadataCache.getPartitionTombstone(...) must return the propagated value.
        Map<Long, PartitionTombstone> tombstones = new LinkedHashMap<>();
        tombstones.put(TABLE_A, new PartitionTombstone(7L, asSet(11L, 13L), 5L));

        UpdateMetadataRequest req =
                ServerRpcMessageUtils.makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tombstones);

        UpdateMetadataRequest parsed = new UpdateMetadataRequest();
        parsed.parseFrom(req.toByteArray());

        ClusterMetadata clusterMetadata =
                ServerRpcMessageUtils.getUpdateMetadataRequestData(parsed);

        TabletServerMetadataCache cache = new TabletServerMetadataCache(new EmptyMetadataManager());
        cache.updateClusterMetadata(clusterMetadata);

        assertThat(cache.getPartitionTombstone(TABLE_A))
                .isEqualTo(new PartitionTombstone(7L, asSet(11L, 13L), 5L));
    }

    private static java.util.Set<Long> asSet(long... values) {
        java.util.Set<Long> s = new java.util.HashSet<>();
        for (long v : values) {
            s.add(v);
        }
        return s;
    }

    private static final class EmptyMetadataManager extends MetadataManager {
        EmptyMetadataManager() {
            super(
                    null,
                    new Configuration(),
                    new LakeCatalogDynamicLoader(new Configuration(), null, true));
        }

        @Override
        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
    }
}
