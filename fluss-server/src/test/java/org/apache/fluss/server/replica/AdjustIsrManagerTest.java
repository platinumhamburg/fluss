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

package org.apache.fluss.server.replica;

import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AdjustIsrManager}. */
class AdjustIsrManagerTest {

    // TODO add more test refer to kafka AlterPartitionManagerTest, See: FLUSS-56278513

    @Test
    void testSubmitShrinkIsr() throws Exception {
        int tabletServerId = 0;
        AdjustIsrManager adjustIsrManager =
                new AdjustIsrManager(
                        new FlussScheduler(1), new TestCoordinatorGateway(), tabletServerId, null);

        // shrink isr
        TableBucket tb = new TableBucket(150001L, 0);
        List<Integer> currentIsr = Arrays.asList(1, 2);
        LeaderAndIsr adjustIsr = new LeaderAndIsr(tabletServerId, 0, currentIsr, 0, 0);
        LeaderAndIsr result = adjustIsrManager.submit(tb, adjustIsr).get();
        assertThat(result)
                .isEqualTo(new LeaderAndIsr(tabletServerId, 0, Arrays.asList(1, 2), 0, 1));

        // expand isr
        currentIsr = Arrays.asList(1, 2, 3);
        adjustIsr = new LeaderAndIsr(tabletServerId, 0, currentIsr, 0, 1);
        result = adjustIsrManager.submit(tb, adjustIsr).get();
        assertThat(result)
                .isEqualTo(new LeaderAndIsr(tabletServerId, 0, Arrays.asList(1, 2, 3), 0, 2));
    }

    @Test
    void testSubmitPropagatesRpcLevelErrorAndAllowsRetry() throws Exception {
        int tabletServerId = 0;
        TestCoordinatorGateway coordinatorGateway = new TestCoordinatorGateway();
        // Make all AdjustIsr requests fail with NetworkException.
        coordinatorGateway.setNetworkIssueEnable(true);
        AdjustIsrManager adjustIsrManager =
                new AdjustIsrManager(
                        new FlussScheduler(1), coordinatorGateway, tabletServerId, null);

        // The RPC-level exception is propagated to the submit future.
        TableBucket tb = new TableBucket(150001L, 0);
        List<Integer> currentIsr = Arrays.asList(1, 2);
        LeaderAndIsr adjustIsr = new LeaderAndIsr(tabletServerId, 0, currentIsr, 0, 0);
        assertThatThrownBy(() -> adjustIsrManager.submit(tb, adjustIsr).get())
                .rootCause()
                .isInstanceOf(NetworkException.class)
                .hasMessage("Mock network issue.");

        coordinatorGateway.setNetworkIssueEnable(false);

        // After the network issue is cleared, a retry should not be blocked by any previous
        // submit.
        LeaderAndIsr result = adjustIsrManager.submit(tb, adjustIsr).get();
        assertThat(result)
                .isEqualTo(new LeaderAndIsr(tabletServerId, 0, Arrays.asList(1, 2), 0, 1));
    }

    @Test
    void testCorrectiveLeaderAndIsrCallbackOnFencedEpoch() throws Exception {
        int tabletServerId = 0;
        TestCoordinatorGateway coordinatorGateway = new TestCoordinatorGateway();
        TableBucket tb = new TableBucket(150001L, 0);

        // Simulate coordinator has advanced the leader epoch to 5
        coordinatorGateway.setCurrentLeaderEpoch(tb, 5);

        // Track corrective LeaderAndIsr received via callback
        ConcurrentMap<TableBucket, LeaderAndIsr> receivedCorrections =
                new ConcurrentHashMap<>();
        AdjustIsrManager adjustIsrManager =
                new AdjustIsrManager(
                        new FlussScheduler(1),
                        coordinatorGateway,
                        tabletServerId,
                        receivedCorrections::put);

        // Submit with stale epoch (0 < 5), should get FencedLeaderEpochException
        List<Integer> currentIsr = Arrays.asList(1, 2);
        LeaderAndIsr adjustIsr = new LeaderAndIsr(tabletServerId, 0, currentIsr, 0, 0);
        assertThatThrownBy(() -> adjustIsrManager.submit(tb, adjustIsr).get())
                .rootCause()
                .isInstanceOf(FencedLeaderEpochException.class);

        // Verify the callback received the corrective LeaderAndIsr
        assertThat(receivedCorrections).containsKey(tb);
        LeaderAndIsr corrective = receivedCorrections.get(tb);
        assertThat(corrective.leaderEpoch()).isEqualTo(5);
        assertThat(corrective.leader()).isEqualTo(tabletServerId);
        assertThat(corrective.isr()).isEqualTo(Arrays.asList(1, 2));
    }
}
