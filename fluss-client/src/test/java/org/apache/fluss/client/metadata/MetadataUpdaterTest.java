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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.utils.MetadataUtils;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** UT Test for update metadata of {@link MetadataUpdater}. */
public class MetadataUpdaterTest {

    private static final ServerNode CS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.COORDINATOR);
    private static final ServerNode TS_NODE =
            new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER);

    @Test
    void testAsyncMetadataMergesAgainstLatestSnapshot() throws Exception {
        RpcClient client = mock(RpcClient.class);
        CompletableFuture<ApiMessage> first = new CompletableFuture<>();
        CompletableFuture<ApiMessage> second = new CompletableFuture<>();
        when(client.sendRequest(any(), any(), any())).thenReturn(first, second);
        Cluster initial =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), initial);
        PhysicalTablePath path1 = PhysicalTablePath.of(TablePath.of("db", "one"), "p");
        PhysicalTablePath path2 = PhysicalTablePath.of(TablePath.of("db", "two"), "p");
        CompletableFuture<Cluster> resolution1 = updater.ensurePartitionMetadataAsync(path1);
        CompletableFuture<Cluster> resolution2 = updater.ensurePartitionMetadataAsync(path2);
        second.complete(partitionResponse(path2, 2, 20, 5));
        resolution2.get(10, TimeUnit.SECONDS);
        first.complete(partitionResponse(path1, 1, 10, 3));
        Cluster result = resolution1.get(10, TimeUnit.SECONDS);
        assertThat(result.getPartitionId(path1)).contains(10L);
        assertThat(result.getPartitionId(path2)).contains(20L);
        assertThat(result.getBucketCount(TableOrPartition.ofPartition(10))).contains(3);
        assertThat(result.getBucketCount(TableOrPartition.ofPartition(20))).contains(5);
    }

    @Test
    void testAsyncMetadataDoesNotCompleteWithOnlyPartitionId() throws Exception {
        RpcClient client = mock(RpcClient.class);
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("db", "one"), "p");
        CompletableFuture<ApiMessage> assignment = new CompletableFuture<>();
        when(client.sendRequest(any(), any(), any()))
                .thenReturn(
                        CompletableFuture.completedFuture(partitionResponse(path, 1, 10, 0)),
                        assignment);
        Cluster initial =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.singletonMap(path.getTablePath(), 1L),
                        Collections.singletonMap(path, 10L),
                        Collections.emptyMap());
        MetadataUpdater updater = new MetadataUpdater(client, new Configuration(), initial);
        CompletableFuture<Cluster> resolution = updater.ensurePartitionMetadataAsync(path);
        assertThat(resolution).isNotDone();
        assignment.complete(partitionResponse(path, 1, 10, 3));
        assertThat(
                        resolution
                                .get(10, TimeUnit.SECONDS)
                                .getBucketCount(TableOrPartition.ofPartition(10)))
                .contains(3);
    }

    private static MetadataResponse partitionResponse(
            PhysicalTablePath path, long tableId, long partitionId, int count) {
        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(TS_NODE.id())
                .setHost(TS_NODE.host())
                .setPort(TS_NODE.port());
        response.addTableMetadata()
                .setTableId(tableId)
                .setTablePath()
                .setDatabaseName(path.getTablePath().getDatabaseName())
                .setTableName(path.getTablePath().getTableName());
        response.addPartitionMetadata()
                .setTableId(tableId)
                .setPartitionId(partitionId)
                .setPartitionName(path.getPartitionName())
                .setBucketCount(count);
        return response;
    }

    @Test
    void testInitializeClusterWithRetries() throws Exception {
        Configuration configuration = new Configuration();
        RpcClient rpcClient =
                RpcClient.create(configuration, TestingClientMetricGroup.newInstance());

        // retry lower than max retry count.
        AdminReadOnlyGateway gateway = new TestingAdminReadOnlyGateway(2);
        Cluster cluster =
                MetadataUpdater.tryToInitializeClusterWithRetries(rpcClient, CS_NODE, gateway, 3);
        assertThat(cluster).isNotNull();
        assertThat(cluster.getCoordinatorServer()).isEqualTo(CS_NODE);
        assertThat(cluster.getAliveTabletServerList()).containsExactly(TS_NODE);

        // retry higher than max retry count.
        AdminReadOnlyGateway gateway2 = new TestingAdminReadOnlyGateway(5);
        assertThatThrownBy(
                        () ->
                                MetadataUpdater.tryToInitializeClusterWithRetries(
                                        rpcClient, CS_NODE, gateway2, 3))
                .isInstanceOf(StaleMetadataException.class)
                .hasMessageContaining("The metadata is stale.");
    }

    @Test
    void testMetadataBucketCountCompatibility() throws Exception {
        long tableId = 1L;
        long legacyPartitionId = 2L;
        long explicitPartitionId = 3L;
        long unassignedPartitionId = 4L;
        TablePath tablePath = TablePath.of("db", "table");

        MetadataResponse response = new MetadataResponse();
        response.addTabletServer()
                .setNodeId(TS_NODE.id())
                .setHost(TS_NODE.host())
                .setPort(TS_NODE.port());

        PbTableMetadata tableMetadata = response.addTableMetadata().setTableId(tableId);
        tableMetadata
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        for (int bucketId = 0; bucketId < 3; bucketId++) {
            tableMetadata.addBucketMetadata().setBucketId(bucketId);
        }

        PbPartitionMetadata legacyPartition =
                response.addPartitionMetadata()
                        .setTableId(tableId)
                        .setPartitionId(legacyPartitionId)
                        .setPartitionName("legacy");
        legacyPartition.addBucketMetadata().setBucketId(0);
        legacyPartition.addBucketMetadata().setBucketId(1);
        assertThat(legacyPartition.hasBucketCount()).isFalse();

        PbPartitionMetadata explicitPartition =
                response.addPartitionMetadata()
                        .setTableId(tableId)
                        .setPartitionId(explicitPartitionId)
                        .setPartitionName("explicit")
                        .setBucketCount(4);
        explicitPartition.addBucketMetadata().setBucketId(0);
        explicitPartition.addBucketMetadata().setBucketId(1);

        response.addPartitionMetadata()
                .setTableId(tableId)
                .setPartitionId(unassignedPartitionId)
                .setPartitionName("unassigned");

        Cluster originCluster =
                new Cluster(
                        Collections.singletonMap(TS_NODE.id(), TS_NODE),
                        CS_NODE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        AdminReadOnlyGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
                        return CompletableFuture.completedFuture(response);
                    }
                };

        Cluster updatedCluster =
                MetadataUtils.sendMetadataRequestAndRebuildCluster(
                        gateway, true, originCluster, null, null, null);

        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofTable(tableId))).hasValue(3);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(legacyPartitionId)))
                .hasValue(2);
        assertThat(updatedCluster.getBucketCount(TableOrPartition.ofPartition(explicitPartitionId)))
                .hasValue(4);
        assertThat(
                        updatedCluster.getBucketCount(
                                TableOrPartition.ofPartition(unassignedPartitionId)))
                .isEmpty();
    }

    private static final class TestingAdminReadOnlyGateway extends TestCoordinatorGateway {

        private final int maxRetryCount;
        private int retryCount;

        public TestingAdminReadOnlyGateway(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            retryCount++;
            if (retryCount <= maxRetryCount) {
                throw new StaleMetadataException("The metadata is stale.");
            } else {
                MetadataResponse metadataResponse =
                        buildMetadataResponse(
                                CS_NODE,
                                Collections.singleton(TS_NODE),
                                Collections.emptyList(),
                                Collections.emptyList());
                return CompletableFuture.completedFuture(metadataResponse);
            }
        }
    }
}
