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
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.server.coordinator.event.RefreshPartitionTombstonesEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.ZkEpoch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorRequestBatch}. */
class CoordinatorRequestBatchTest {

    @Test
    void testFailedTombstoneFanoutSchedulesOneFreshMetadataRepair() throws Exception {
        TestingEventManager eventManager = new TestingEventManager();
        List<Runnable> scheduledActions = new ArrayList<>();
        CoordinatorChannelManager channelManager =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendUpdateMetadataRequest(
                            int serverId,
                            UpdateMetadataRequest request,
                            BiConsumer<UpdateMetadataResponse, ? super Throwable> callback) {
                        callback.accept(null, new RuntimeException("injected failure"));
                    }
                };
        try {
            CoordinatorRequestBatch requestBatch =
                    new CoordinatorRequestBatch(
                            channelManager,
                            eventManager,
                            new CoordinatorContext(ZkEpoch.INITIAL_EPOCH),
                            scheduledActions::add);
            PartitionTombstone tombstone =
                    new PartitionTombstone(7L, Collections.singleton(11L), 3L);

            requestBatch.newBatch();
            requestBatch.addUpdateMetadataRequestForTabletServers(
                    new HashSet<>(Arrays.asList(1, 2)),
                    null,
                    null,
                    Collections.emptySet(),
                    Collections.singletonMap(100L, tombstone));
            requestBatch.sendUpdateMetadataRequest();

            assertThat(scheduledActions).hasSize(1);
            assertThat(eventManager.getEvents()).isEmpty();

            scheduledActions.get(0).run();
            assertThat(eventManager.getEvents())
                    .singleElement()
                    .isInstanceOf(RefreshPartitionTombstonesEvent.class);

            requestBatch.newBatch();
            requestBatch.addUpdateMetadataRequestForTabletServers(
                    Collections.singleton(1),
                    null,
                    null,
                    Collections.emptySet(),
                    Collections.singletonMap(100L, tombstone));
            requestBatch.sendUpdateMetadataRequest();

            assertThat(scheduledActions).hasSize(2);
            scheduledActions.get(1).run();
            assertThat(eventManager.getEvents())
                    .hasSize(2)
                    .allMatch(RefreshPartitionTombstonesEvent.class::isInstance);
        } finally {
            channelManager.close();
        }
    }
}
