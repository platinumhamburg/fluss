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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.assertj.core.api.Assertions.assertThat;

/** Focused request/response assertions for pending ordinary leader activation. */
final class CoordinatorPendingLeaderActivationTestSupport {

    private CoordinatorPendingLeaderActivationTestSupport() {}

    static void verifyLeaderChangeResponses(
            CoordinatorEventProcessor eventProcessor,
            TestCoordinatorChannelManager channelManager,
            boolean oldRequestFails)
            throws Exception {
        long tableId = oldRequestFails ? 92_001L : 92_000L;
        TableBucket tableBucket = new TableBucket(tableId, 0);
        TablePath tablePath = TablePath.of("db", "pending_leader_" + tableId);
        List<Integer> assignment = Arrays.asList(0, 1);
        ControlledResponseGateway oldLeaderGateway = new ControlledResponseGateway(oldRequestFails);
        ControlledResponseGateway newLeaderGateway = new ControlledResponseGateway(false);
        Map<Integer, TabletServerGateway> gateways = new HashMap<>();
        gateways.put(0, oldLeaderGateway);
        gateways.put(1, newLeaderGateway);
        channelManager.setGateways(gateways);

        LeaderAndIsr oldLeader =
                new LeaderAndIsr(
                        0,
                        0,
                        assignment,
                        Collections.emptyList(),
                        eventProcessor.getCoordinatorContext().getCoordinatorEpoch(),
                        0);
        LeaderAndIsr newLeader =
                new LeaderAndIsr(
                        1,
                        1,
                        oldRequestFails ? Collections.singletonList(1) : assignment,
                        Collections.emptyList(),
                        eventProcessor.getCoordinatorContext().getCoordinatorEpoch(),
                        1);
        CoordinatorRequestBatch requestBatch =
                new CoordinatorRequestBatch(
                        channelManager,
                        eventProcessor.getCoordinatorEventManager(),
                        eventProcessor.getCoordinatorContext());

        fromContext(
                eventProcessor,
                context -> {
                    assertThat(context.liveTabletServerSet()).contains(0, 1);
                    context.putTablePath(tableId, tablePath);
                    context.updateBucketReplicaAssignment(tableBucket, assignment);
                    context.putReplicaState(new TableBucketReplica(tableBucket, 0), OnlineReplica);
                    context.putReplicaState(new TableBucketReplica(tableBucket, 1), OnlineReplica);
                    context.putBucketState(tableBucket, OnlineBucket);
                    context.putBucketLeaderAndIsr(tableBucket, oldLeader);
                    dispatch(requestBatch, context, tablePath, tableBucket, assignment, oldLeader);
                    return null;
                });

        fromContext(
                eventProcessor,
                context -> {
                    context.putBucketLeaderAndIsr(tableBucket, newLeader);
                    dispatch(requestBatch, context, tablePath, tableBucket, assignment, newLeader);
                    return null;
                });
        assertThat(oldLeaderGateway.getRequestCount()).isOne();
        assertThat(newLeaderGateway.getRequestCount()).isOne();
        assertInactive(eventProcessor, tableBucket);

        oldLeaderGateway.releaseResponse();
        assertInactive(eventProcessor, tableBucket);

        newLeaderGateway.releaseResponse();
        fromContext(
                eventProcessor,
                context -> {
                    assertThat(context.getPendingLeaderActivationBuckets()).isEmpty();
                    assertThat(context.isLeaderActive(tableBucket)).isTrue();
                    assertThat(
                                    CoordinatorService.computeClusterHealth(context)
                                            .getActiveLeaderReplicas())
                            .isOne();
                    assertThat(CoordinatorService.computeClusterHealth(context).getStatus())
                            .isEqualTo(oldRequestFails ? 1 : 0);
                    return null;
                });
    }

    private static void dispatch(
            CoordinatorRequestBatch requestBatch,
            CoordinatorContext context,
            TablePath tablePath,
            TableBucket tableBucket,
            List<Integer> assignment,
            LeaderAndIsr leaderAndIsr) {
        requestBatch.newBatch();
        requestBatch.addNotifyLeaderRequestForTabletServers(
                Collections.singleton(leaderAndIsr.leader()),
                PhysicalTablePath.of(tablePath),
                tableBucket,
                assignment,
                leaderAndIsr);
        requestBatch.sendRequestToTabletServers(context.getCoordinatorEpoch());
    }

    private static void assertInactive(
            CoordinatorEventProcessor eventProcessor, TableBucket tableBucket) throws Exception {
        fromContext(
                eventProcessor,
                context -> {
                    assertThat(context.getPendingLeaderActivationBuckets())
                            .containsExactly(tableBucket);
                    assertThat(context.isLeaderActive(tableBucket)).isFalse();
                    assertThat(CoordinatorService.computeClusterHealth(context).getStatus())
                            .isEqualTo(2);
                    return null;
                });
    }

    private static <T> T fromContext(
            CoordinatorEventProcessor eventProcessor, Function<CoordinatorContext, T> function)
            throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(function);
        eventProcessor.getCoordinatorEventManager().put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private static final class ControlledResponseGateway extends TestTabletServerGateway {

        private final CompletableFuture<Void> responseRelease = new CompletableFuture<>();
        private final AtomicInteger requestCount = new AtomicInteger();

        private ControlledResponseGateway(boolean failResponse) {
            super(failResponse, Collections.emptySet());
        }

        @Override
        public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
                NotifyLeaderAndIsrRequest request) {
            requestCount.incrementAndGet();
            NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
            return responseRelease.thenApply(ignored -> response);
        }

        @Override
        public CompletableFuture<UpdateMetadataResponse> updateMetadata(
                UpdateMetadataRequest request) {
            return CompletableFuture.completedFuture(new UpdateMetadataResponse());
        }

        private int getRequestCount() {
            return requestCount.get();
        }

        private void releaseResponse() {
            assertThat(responseRelease.complete(null)).isTrue();
        }
    }
}
