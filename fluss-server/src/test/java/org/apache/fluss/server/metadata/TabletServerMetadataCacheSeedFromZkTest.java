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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableType;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkData.PartitionTombstoneZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TabletServerMetadataCache#seedPartitionTombstonesFromZk(ZooKeeperClient,
 * java.util.Collection)}, which rebuilds the partition-tombstone cache from ZK on TabletServer
 * startup (Plan 3 Task 12). Without this seed, a freshly-started TabletServer would not know about
 * partitions that were dropped before it came up — the Coordinator only ships tombstones in {@code
 * UpdateMetadataRequest} when it advances them, not on every metadata push.
 */
class TabletServerMetadataCacheSeedFromZkTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    private static final TablePath MAIN_PARTITIONED_PATH =
            TablePath.of("seed_db", "main_partitioned");
    private static final TablePath MAIN_NON_PARTITIONED_PATH =
            TablePath.of("seed_db", "main_non_partitioned");
    private static final TablePath INDEX_TABLE_PATH = TablePath.of("seed_db", "main__idx__col");

    private static final long MAIN_PARTITIONED_ID = 1001L;
    private static final long MAIN_NON_PARTITIONED_ID = 1002L;
    private static final long INDEX_TABLE_ID = 1003L;

    private TabletServerMetadataCache cache;

    @BeforeAll
    static void beforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zkClient.close();
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .build();
    }

    private static TableInfo partitionedMainTable() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema())
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        return TableInfo.of(
                MAIN_PARTITIONED_PATH, MAIN_PARTITIONED_ID, 1, descriptor, null, 0L, 0L);
    }

    private static TableInfo nonPartitionedMainTable() {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema()).distributedBy(3).build();
        return TableInfo.of(
                MAIN_NON_PARTITIONED_PATH, MAIN_NON_PARTITIONED_ID, 1, descriptor, null, 0L, 0L);
    }

    private static TableInfo partitionedIndexTable() {
        // Index table — partitioned just like its main table, but TABLE_TYPE = INDEX_TABLE.
        // The seed must skip it because tombstones live on main tables only.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema())
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .build();
        return TableInfo.of(INDEX_TABLE_PATH, INDEX_TABLE_ID, 1, descriptor, null, 0L, 0L);
    }

    private TabletServerMetadataCache newCache() {
        return new TabletServerMetadataCache(new TestingMetadataManager(Collections.emptyList()));
    }

    @Test
    void testSeedFromZkPopulatesCacheWithTombstone() throws Exception {
        cache = newCache();
        Set<Long> explicit = new HashSet<>();
        explicit.add(7L);
        PartitionTombstone persisted = new PartitionTombstone(5L, explicit, 3L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, persisted);

        cache.seedPartitionTombstonesFromZk(
                zkClient, Collections.singletonList(partitionedMainTable()));

        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(persisted);
    }

    @Test
    void testSeedFromZkSkipsTablesWithoutPersistedTombstone() {
        // No tombstone written to ZK — getPartitionTombstone returns EMPTY. The seed must not
        // pollute the cache with EMPTY entries (apply-path filter relies on EMPTY meaning "no
        // entry yet").
        cache = newCache();

        cache.seedPartitionTombstonesFromZk(
                zkClient, Collections.singletonList(partitionedMainTable()));

        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID))
                .isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testSeedFromZkSkipsNonPartitionedTables() throws Exception {
        // Even if a (corrupt/legacy) tombstone is in ZK for a non-partitioned table, the seed must
        // skip it — partition tombstones only apply to partitioned tables.
        cache = newCache();
        zkClient.setOrCreatePartitionTombstone(
                MAIN_NON_PARTITIONED_PATH, new PartitionTombstone(99L, Collections.emptySet(), 1L));

        cache.seedPartitionTombstonesFromZk(
                zkClient, Collections.singletonList(nonPartitionedMainTable()));

        assertThat(cache.getPartitionTombstone(MAIN_NON_PARTITIONED_ID))
                .isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testSeedFromZkSkipsIndexTables() throws Exception {
        // Even if an Index Table happens to have a tombstone znode, the seed must skip it — only
        // main tables carry tombstones in this design.
        cache = newCache();
        zkClient.setOrCreatePartitionTombstone(
                INDEX_TABLE_PATH, new PartitionTombstone(42L, Collections.emptySet(), 1L));

        cache.seedPartitionTombstonesFromZk(
                zkClient, Collections.singletonList(partitionedIndexTable()));

        assertThat(cache.getPartitionTombstone(INDEX_TABLE_ID)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testSeedFromZkContinuesOnIndividualReadFailure() throws Exception {
        // Seed must not crash startup on a single corrupted tombstone znode. We simulate by
        // writing garbage bytes (un-parseable JSON) directly into the second table's tombstone
        // path via the curator client, while the first table has a valid tombstone. The seed
        // call should log+skip the failing table and still seed the good one.
        cache = newCache();
        Set<Long> explicit = new HashSet<>();
        explicit.add(11L);
        PartitionTombstone good = new PartitionTombstone(0L, explicit, 1L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, good);

        // Corrupt a different partitioned main table's znode with garbage bytes so JSON deserde
        // throws when the seed tries to read it.
        TablePath corruptPath = TablePath.of("seed_db", "main_corrupt");
        long corruptTableId = 1099L;
        zkClient.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        PartitionTombstoneZNode.path(corruptPath),
                        new byte[] {0x00, 0x01, 0x02, 0x03});
        TableInfo corruptTable =
                TableInfo.of(
                        corruptPath,
                        corruptTableId,
                        1,
                        TableDescriptor.builder()
                                .schema(schema())
                                .distributedBy(3)
                                .partitionedBy("b")
                                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                        AutoPartitionTimeUnit.YEAR)
                                .build(),
                        null,
                        0L,
                        0L);

        cache.seedPartitionTombstonesFromZk(
                zkClient, Arrays.asList(corruptTable, partitionedMainTable()));

        // The good table got seeded despite the corrupt znode for the other table.
        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(good);
        // The corrupt table did not pollute the cache.
        assertThat(cache.getPartitionTombstone(corruptTableId)).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testSeedFromZkIsIdempotent() throws Exception {
        cache = newCache();
        PartitionTombstone first = new PartitionTombstone(5L, Collections.emptySet(), 2L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, first);
        List<TableInfo> tables = Collections.singletonList(partitionedMainTable());

        cache.seedPartitionTombstonesFromZk(zkClient, tables);
        cache.seedPartitionTombstonesFromZk(zkClient, tables);

        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(first);

        // A subsequent ZK update + reseed should pick up the new value (no stale latching).
        PartitionTombstone second = new PartitionTombstone(9L, Collections.emptySet(), 3L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, second);
        cache.seedPartitionTombstonesFromZk(zkClient, tables);
        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(second);
    }

    @Test
    void testAutoSeedTriggeredOnFirstClusterMetadataUpdate() throws Exception {
        // The wired seed-on-first-update path inside updateClusterMetadata: setting a ZK client
        // and pushing a non-empty TableMetadataList should cause the cache to read tombstones
        // from ZK for the just-populated tables.
        cache = newCache();
        cache.setZooKeeperClient(zkClient);
        PartitionTombstone persisted = new PartitionTombstone(7L, Collections.emptySet(), 2L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, persisted);

        TableMetadata mainMd = new TableMetadata(partitionedMainTable(), Collections.emptyList());
        ClusterMetadata metadata =
                new ClusterMetadata(
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(mainMd),
                        Collections.emptyList());
        cache.updateClusterMetadata(metadata);

        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(persisted);

        // Idempotent: a second update bringing a fresher ZK value won't re-fire the auto-seed
        // (one-shot guard), but explicitly calling the seed method still works.
        PartitionTombstone fresher = new PartitionTombstone(15L, Collections.emptySet(), 4L);
        zkClient.setOrCreatePartitionTombstone(MAIN_PARTITIONED_PATH, fresher);
        cache.updateClusterMetadata(metadata);
        // The auto-seed only fires once, so the cached value is still 'persisted'.
        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(persisted);

        // But the explicit seed method always re-reads.
        cache.seedPartitionTombstonesFromZk(
                zkClient, Collections.singletonList(partitionedMainTable()));
        assertThat(cache.getPartitionTombstone(MAIN_PARTITIONED_ID)).isEqualTo(fresher);
    }

    /** Minimal MetadataManager stub — TabletServerMetadataCache only needs the constructor arg. */
    private static final class TestingMetadataManager extends MetadataManager {

        private final Map<TablePath, TableInfo> tableInfoMap = new HashMap<>();

        TestingMetadataManager(List<TableInfo> tableInfos) {
            super(
                    null,
                    new Configuration(),
                    new LakeCatalogDynamicLoader(new Configuration(), null, true));
            tableInfos.forEach(tableInfo -> tableInfoMap.put(tableInfo.getTablePath(), tableInfo));
        }

        @Override
        public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
            TableInfo tableInfo = tableInfoMap.get(tablePath);
            if (tableInfo == null) {
                throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
            }
            return tableInfo;
        }
    }
}
