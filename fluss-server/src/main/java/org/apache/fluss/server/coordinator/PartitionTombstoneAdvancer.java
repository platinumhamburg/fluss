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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.HashSet;
import java.util.Set;

/**
 * Pure helper for evolving a {@link PartitionTombstone} as partitions are dropped.
 *
 * <p>On each drop the helper either appends the dropped partition id to {@code explicitSet}
 * (when it sits above the current floor) or treats the call as a no-op for that partition
 * (when it is already covered by the floor). It then compacts {@code explicitSet} into the
 * floor by absorbing any contiguous run of ids starting at {@code floor + 1}. The version is
 * always bumped by one so observers can detect that <em>something</em> happened, even when the
 * dropped partition was already tombstoned.
 */
@Internal
public final class PartitionTombstoneAdvancer {

    private PartitionTombstoneAdvancer() {}

    /**
     * Returns a new {@link PartitionTombstone} reflecting the drop of {@code partitionId}.
     *
     * @param before the current tombstone snapshot
     * @param partitionId the id of the partition being dropped
     * @return a new tombstone with {@code partitionId} absorbed (either into the floor or into
     *     the explicit set) and {@code version = before.getVersion() + 1}
     */
    public static PartitionTombstone dropPartition(
            PartitionTombstone before, long partitionId) {
        Set<Long> explicit = new HashSet<>(before.getExplicitSet());
        long floor = before.getFloor();
        if (partitionId > floor) {
            explicit.add(partitionId);
        }
        while (explicit.contains(floor + 1)) {
            floor = floor + 1;
            explicit.remove(floor);
        }
        return new PartitionTombstone(floor, explicit, before.getVersion() + 1);
    }

    /**
     * Reads the current {@link PartitionTombstone} for {@code tablePath} from ZK, advances it via
     * {@link #dropPartition(PartitionTombstone, long)} for the given {@code partitionId}, persists
     * the new value back to ZK, and returns it. Used by the Coordinator on a partition drop to keep
     * the per-table tombstone in sync before fanning the new value out to TabletServers via
     * {@code UpdateMetadataRequest}.
     */
    public static PartitionTombstone advanceAndPersist(
            ZooKeeperClient zkClient, TablePath tablePath, long partitionId) throws Exception {
        PartitionTombstone current = zkClient.getPartitionTombstone(tablePath);
        PartitionTombstone updated = dropPartition(current, partitionId);
        zkClient.setOrCreatePartitionTombstone(tablePath, updated);
        return updated;
    }
}
