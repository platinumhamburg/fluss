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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.PartitionTombstoneAdvancer;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for TabletServer recovery of persisted partition tombstones. */
class PartitionTombstoneRecoveryITCase {

    private static final String DATABASE = "test_db";
    private static final String INDEX_NAME = "idx_b";
    private static final String PARTITION_NAME = "p1";
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(45);

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(configuration())
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }

    @Test
    void testTabletServerReloadsPersistedTombstoneWithoutCoordinatorNotification()
            throws Exception {
        String tableName = "tombstone_recovery_" + System.nanoTime();
        TablePath mainPath = TablePath.of(DATABASE, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("a", "p")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1, "a")
                                .partitionedBy("p")
                                .build());
        createPartition(CLUSTER, mainPath, partitionSpec(), false);

        long partitionId = CLUSTER.waitUntilPartitionsCreated(mainPath, 1).get(PARTITION_NAME);
        TablePath indexPath =
                TablePath.of(
                        DATABASE,
                        IndexTableUtils.indexTableName(mainPath.getTableName(), INDEX_NAME));
        long indexTableId = CLUSTER.getZooKeeperClient().getTable(indexPath).orElseThrow().tableId;
        TableBucket indexBucket = new TableBucket(indexTableId, 0);
        int indexServerId = CLUSTER.waitAndGetLeader(indexBucket);
        CLUSTER.waitAndGetLeaderReplica(indexBucket);

        TabletServerMetadataCache metadataCache =
                CLUSTER.getTabletServerById(indexServerId).getMetadataCache();
        waitUntil(
                () ->
                        metadataCache
                                .getInitializedPartitionTombstone(mainTableId)
                                .map(PartitionTombstone::isEmpty)
                                .orElse(false),
                Duration.ofSeconds(30),
                "initial empty partition tombstone");

        ZooKeeperClient zkClient = CLUSTER.getZooKeeperClient();
        PartitionRegistration partition =
                zkClient.getPartition(mainPath, PARTITION_NAME).orElseThrow();
        Tuple2<PartitionTombstone, java.util.Optional<Integer>> current =
                zkClient.getPartitionTombstoneWithVersion(mainPath);
        PartitionTombstone persisted =
                PartitionTombstoneAdvancer.dropPartition(
                        current.f0, partitionId, Collections.emptySet());

        CLUSTER.stopCoordinatorServer();
        try {
            zkClient.deletePartitionAndSetTombstone(
                    mainPath,
                    PARTITION_NAME,
                    mainTableId,
                    partition.getPartitionId(),
                    persisted,
                    current.f1,
                    ZkVersion.MATCH_ANY_VERSION.getVersion());

            assertThat(zkClient.getPartition(mainPath, PARTITION_NAME)).isEmpty();
            assertThat(zkClient.getPartitionTombstone(mainPath)).isEqualTo(persisted);
            assertThat(metadataCache.getPartitionTombstone(mainTableId))
                    .as("cache remains stale until TabletServer reloads the ZK value")
                    .isNotEqualTo(persisted);

            waitUntil(
                    () ->
                            metadataCache
                                    .getInitializedPartitionTombstone(mainTableId)
                                    .map(persisted::equals)
                                    .orElse(false),
                    RECOVERY_TIMEOUT,
                    "TabletServer reloads the persisted partition tombstone");
        } finally {
            CLUSTER.startCoordinatorServer();
        }
    }

    private static PartitionSpec partitionSpec() {
        return new PartitionSpec(Collections.singletonMap("p", PARTITION_NAME));
    }
}
