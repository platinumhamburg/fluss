/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.ServerApiVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Focused assertions for ordinary metadata and role dispatch. */
final class CoordinatorMetadataDispatchTestSupport {

    private CoordinatorMetadataDispatchTestSupport() {}

    static void verifyOrdinaryRestartDispatch(
            ZooKeeperClient zooKeeperClient,
            ZooKeeperExtension zooKeeperExtension,
            CoordinatorEventProcessor eventProcessor,
            MetadataManager metadataManager,
            TestCoordinatorChannelManager channelManager,
            String database,
            String remoteDataDir,
            TableDescriptor tableDescriptor)
            throws Exception {
        int serverId = 31;
        String serverPath = ZkData.ServerIdZNode.path(serverId);
        ZooKeeperClient processClient = newProcessClient(zooKeeperExtension);
        processClient.registerTabletServer(serverId, registration(serverId, false));
        try {
            waitForServer(eventProcessor, serverId, true);
            channelManager.setGateways(gateways(zooKeeperClient, null, serverId));

            TablePath tablePath = TablePath.of(database, "ordinary_restart_dispatch");
            TableAssignment assignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(serverId)).build();
            metadataManager.createTable(
                    tablePath, remoteDataDir, tableDescriptor, assignment, false);
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
            waitValue(
                    () ->
                            fromContext(
                                    eventProcessor, ctx -> ctx.getBucketLeaderAndIsr(tableBucket)),
                    Duration.ofMinutes(1),
                    "leader not elected");

            processClient.close();
            waitForServer(eventProcessor, serverId, false);

            processClient = newProcessClient(zooKeeperExtension);
            ControlledGateway gateway = new ControlledGateway();
            channelManager.setGateways(gateways(zooKeeperClient, gateway, serverId));
            processClient.registerTabletServer(serverId, registration(serverId, true));
            waitForServer(eventProcessor, serverId, true);
            fromContext(eventProcessor, Function.identity());

            assertThat(gateway.sequence())
                    .startsWith("metadata:alive", "metadata:full", "notify-role");
        } finally {
            processClient.close();
            deleteServer(zooKeeperClient, serverPath);
        }
    }

    private static ZooKeeperClient newProcessClient(ZooKeeperExtension zooKeeperExtension)
            throws Exception {
        ZooKeeperClient client = zooKeeperExtension.createZooKeeperClient(NOPErrorHandler.INSTANCE);
        if (!client.getCuratorClient().blockUntilConnected(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Test TabletServer ZooKeeper session did not connect.");
        }
        return client;
    }

    private static Map<Integer, TabletServerGateway> gateways(
            ZooKeeperClient zooKeeperClient,
            TabletServerGateway controlledGateway,
            int controlledServerId)
            throws Exception {
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        for (int serverId : zooKeeperClient.getSortedTabletServerList()) {
            gateways.put(serverId, new TestTabletServerGateway(false, Collections.emptySet()));
        }
        if (controlledGateway != null) {
            gateways.put(controlledServerId, controlledGateway);
        }
        return gateways;
    }

    private static TabletServerRegistration registration(int serverId, boolean feature) {
        return new TabletServerRegistration(
                "rack" + serverId,
                Collections.singletonList(
                        new Endpoint("host" + serverId, 1234, DEFAULT_LISTENER_NAME)),
                System.currentTimeMillis(),
                TabletServerResource.unknown(),
                tabletApis(feature));
    }

    private static List<ServerApiVersion> tabletApis(boolean feature) {
        List<ApiKeys> metadataApis =
                Arrays.asList(
                        ApiKeys.UPDATE_METADATA,
                        ApiKeys.NOTIFY_LEADER_AND_ISR,
                        ApiKeys.STOP_REPLICA);
        List<ServerApiVersion> apiVersions = new ArrayList<>();
        for (ApiKeys apiKey : metadataApis) {
            apiVersions.add(
                    new ServerApiVersion(apiKey.id, (short) 0, feature ? (short) 1 : (short) 0));
        }
        Collections.sort(apiVersions);
        return apiVersions;
    }

    private static void waitForServer(
            CoordinatorEventProcessor eventProcessor, int serverId, boolean expectedLive) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    boolean actual =
                            fromContext(
                                    eventProcessor,
                                    context -> context.liveTabletServerSet().contains(serverId));
                    assertThat(actual).isEqualTo(expectedLive);
                });
    }

    private static void deleteServer(ZooKeeperClient zooKeeperClient, String serverPath)
            throws Exception {
        if (zooKeeperClient.getCuratorClient().checkExists().forPath(serverPath) != null) {
            zooKeeperClient.getCuratorClient().delete().forPath(serverPath);
        }
    }

    private static <T> T fromContext(
            CoordinatorEventProcessor eventProcessor, Function<CoordinatorContext, T> function)
            throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(function);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private static final class ControlledGateway extends TestTabletServerGateway {

        private final CompletableFuture<Void> membershipResponseRelease = new CompletableFuture<>();
        private final CompletableFuture<Void> roleResponseRelease = new CompletableFuture<>();
        private final ConcurrentLinkedDeque<String> sequence = new ConcurrentLinkedDeque<>();

        private ControlledGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                UpdateMetadataRequest request) {
            boolean empty =
                    request.getTableMetadatasCount() == 0
                            && request.getPartitionMetadatasCount() == 0;
            sequence.add(empty ? "metadata:alive" : "metadata:full");
            return empty
                    ? membershipResponseRelease.thenApply(ignored -> new UpdateMetadataResponse())
                    : CompletableFuture.completedFuture(new UpdateMetadataResponse());
        }

        @Override
        public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                NotifyLeaderAndIsrRequest request) {
            sequence.add("notify-role");
            NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
            return roleResponseRelease.thenApply(ignored -> response);
        }

        private List<String> sequence() {
            return sequence.stream().collect(Collectors.toList());
        }
    }
}
