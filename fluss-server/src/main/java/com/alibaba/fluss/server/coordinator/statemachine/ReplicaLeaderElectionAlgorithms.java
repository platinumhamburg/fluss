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

package com.alibaba.fluss.server.coordinator.statemachine;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** The algorithms to elect the replica leader. */
public class ReplicaLeaderElectionAlgorithms {
    public static Optional<Integer> defaultReplicaLeaderElection(
            List<Integer> assignments, List<Integer> aliveReplicas, List<Integer> isr) {
        // currently, we always use the first replica in assignment, which also in aliveReplicas and
        // isr as the leader replica.
        for (int assignment : assignments) {
            if (aliveReplicas.contains(assignment) && isr.contains(assignment)) {
                return Optional.of(assignment);
            }
        }

        return Optional.empty();
    }

    public static Optional<Integer> controlledShutdownReplicaLeaderElection(
            List<Integer> assignments,
            List<Integer> isr,
            List<Integer> aliveReplicas,
            Set<Integer> shutdownTabletServers) {
        Set<Integer> isrSet = new HashSet<>(isr);
        for (Integer id : assignments) {
            if (aliveReplicas.contains(id)
                    && isrSet.contains(id)
                    && !shutdownTabletServers.contains(id)) {
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }
}
