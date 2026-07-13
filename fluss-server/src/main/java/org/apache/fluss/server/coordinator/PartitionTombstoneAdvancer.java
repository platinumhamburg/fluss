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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.serde.PartitionTombstoneBinarySerde;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Pure helper for evolving a {@link PartitionTombstone} as partitions are dropped.
 *
 * <p>On each drop the helper either appends the dropped partition id to {@code explicitSet} (when
 * it sits above the current floor) or treats the call as a no-op for that partition when it is
 * already covered by the floor. When the caller provides the alive partition ids after the drop,
 * the helper safely advances {@code floor} to the highest id below every alive partition. The
 * version is always bumped by one so observers can detect that <em>something</em> happened, even
 * when the dropped partition was already tombstoned.
 */
@Internal
public final class PartitionTombstoneAdvancer {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionTombstoneAdvancer.class);

    @VisibleForTesting static final int EXPLICIT_SET_WARNING_THRESHOLD = 4096;

    @VisibleForTesting static final int SERIALIZED_BYTES_WARNING_THRESHOLD = 256 * 1024;

    private static final int MAX_CAS_ATTEMPTS = 3;

    private PartitionTombstoneAdvancer() {}

    /**
     * Returns a new {@link PartitionTombstone} reflecting the drop of {@code partitionId}.
     *
     * @param before the current tombstone snapshot
     * @param partitionId the id of the partition being dropped
     * @return a new tombstone with {@code partitionId} absorbed (either into the floor or into the
     *     explicit set) and {@code version = before.getVersion() + 1}
     */
    public static PartitionTombstone dropPartition(PartitionTombstone before, long partitionId) {
        return dropPartition(before, partitionId, null);
    }

    /**
     * Returns a new {@link PartitionTombstone} reflecting the drop of {@code partitionId}.
     *
     * <p>When {@code alivePartitionIdsAfterDrop} is present, {@code floor} is advanced only to a
     * value below every alive partition id. When the set is empty, every known dropped id is safe
     * to fold into the floor. When the snapshot is unavailable, the update falls back to a
     * conservative explicit-set append.
     */
    public static PartitionTombstone dropPartition(
            PartitionTombstone before,
            long partitionId,
            @Nullable Collection<Long> alivePartitionIdsAfterDrop) {
        Set<Long> explicit = new HashSet<>(before.getExplicitSet());
        long floor = before.getFloor();
        if (partitionId > floor) {
            explicit.add(partitionId);
        }

        long newFloor = floor;
        if (alivePartitionIdsAfterDrop != null) {
            if (alivePartitionIdsAfterDrop.isEmpty()) {
                newFloor = Math.max(newFloor, partitionId);
                for (long explicitPartitionId : explicit) {
                    newFloor = Math.max(newFloor, explicitPartitionId);
                }
            } else {
                long minAlivePartitionId = Long.MAX_VALUE;
                for (long alivePartitionId : alivePartitionIdsAfterDrop) {
                    checkArgument(
                            alivePartitionId > floor,
                            "Alive partition id %s must be greater than tombstone floor %s.",
                            alivePartitionId,
                            floor);
                    checkArgument(
                            alivePartitionId != partitionId,
                            "Dropped partition id %s must not remain in alive partition ids.",
                            partitionId);
                    minAlivePartitionId = Math.min(minAlivePartitionId, alivePartitionId);
                }
                newFloor = Math.max(newFloor, minAlivePartitionId - 1);
            }
        }

        long floorToKeep = newFloor;
        explicit.removeIf(explicitPartitionId -> explicitPartitionId <= floorToKeep);
        return new PartitionTombstone(newFloor, explicit, before.getVersion() + 1);
    }

    /**
     * Validates that a newly allocated source partition id is not covered by the tombstone floor.
     */
    public static void validateNewPartitionId(PartitionTombstone tombstone, long partitionId) {
        checkArgument(
                partitionId > tombstone.getFloor(),
                "New partition id %s must be greater than tombstone floor %s.",
                partitionId,
                tombstone.getFloor());
    }

    @VisibleForTesting
    static boolean shouldWarnForLargeTombstone(PartitionTombstone tombstone) {
        int serializedBytes = PartitionTombstoneBinarySerde.serialize(tombstone).length;
        return tombstone.getExplicitSet().size() >= EXPLICIT_SET_WARNING_THRESHOLD
                || serializedBytes >= SERIALIZED_BYTES_WARNING_THRESHOLD;
    }

    /**
     * Reads the current {@link PartitionTombstone} for {@code tablePath} from ZK, advances it via
     * {@link #dropPartition(PartitionTombstone, long)} for the given {@code partitionId}, persists
     * the new value back to ZK, and returns it. Used by the Coordinator on a partition drop to keep
     * the per-table tombstone in sync before fanning the new value out to TabletServers via {@code
     * UpdateMetadataRequest}.
     */
    public static PartitionTombstone advanceAndPersist(
            ZooKeeperClient zkClient, TablePath tablePath, long partitionId) throws Exception {
        return advanceAndPersist(zkClient, tablePath, partitionId, null);
    }

    /**
     * Reads the current {@link PartitionTombstone} for {@code tablePath} from ZK, advances it via
     * {@link #dropPartition(PartitionTombstone, long, Collection)} for the given {@code
     * partitionId}, persists the new value back to ZK, and returns it.
     */
    public static PartitionTombstone advanceAndPersist(
            ZooKeeperClient zkClient,
            TablePath tablePath,
            long partitionId,
            @Nullable Collection<Long> alivePartitionIdsAfterDrop)
            throws Exception {
        for (int attempt = 1; ; attempt++) {
            Tuple2<PartitionTombstone, Optional<Integer>> current =
                    zkClient.getPartitionTombstoneWithVersion(tablePath);
            PartitionTombstone updated =
                    dropPartition(current.f0, partitionId, alivePartitionIdsAfterDrop);
            try {
                zkClient.compareAndSetPartitionTombstone(tablePath, updated, current.f1);
                logAdvancement(tablePath, partitionId, current.f0, updated);
                logIfTombstoneLarge(tablePath, updated);
                return updated;
            } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
                if (attempt >= MAX_CAS_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    /** Advances a tombstone only for the expected table identity and coordinator epoch. */
    public static PartitionTombstone advanceAndPersist(
            ZooKeeperClient zkClient,
            TablePath tablePath,
            long expectedTableId,
            long partitionId,
            @Nullable Collection<Long> alivePartitionIdsAfterDrop,
            int expectedZkVersion)
            throws Exception {
        for (int attempt = 1; ; attempt++) {
            Tuple2<PartitionTombstone, Optional<Integer>> current =
                    zkClient.getPartitionTombstoneWithVersion(tablePath);
            PartitionTombstone updated =
                    dropPartition(current.f0, partitionId, alivePartitionIdsAfterDrop);
            try {
                zkClient.compareAndSetPartitionTombstone(
                        tablePath, expectedTableId, updated, current.f1, expectedZkVersion);
                logAdvancement(tablePath, partitionId, current.f0, updated);
                logIfTombstoneLarge(tablePath, updated);
                return updated;
            } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
                if (attempt >= MAX_CAS_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    private static void logAdvancement(
            TablePath tablePath,
            long partitionId,
            PartitionTombstone current,
            PartitionTombstone updated) {
        int explicitSetSizeAfterDrop = current.getExplicitSet().size();
        if (partitionId > current.getFloor() && !current.getExplicitSet().contains(partitionId)) {
            explicitSetSizeAfterDrop++;
        }
        int removedExplicitEntries = explicitSetSizeAfterDrop - updated.getExplicitSet().size();
        LOG.debug(
                "Advanced partition tombstone for table {} after dropping partition {}: "
                        + "floor {} -> {}, explicitSetSize {} -> {}, removedExplicitEntries {}.",
                tablePath,
                partitionId,
                current.getFloor(),
                updated.getFloor(),
                current.getExplicitSet().size(),
                updated.getExplicitSet().size(),
                removedExplicitEntries);
    }

    private static void logIfTombstoneLarge(TablePath tablePath, PartitionTombstone tombstone) {
        int serializedBytes = PartitionTombstoneBinarySerde.serialize(tombstone).length;
        if (tombstone.getExplicitSet().size() >= EXPLICIT_SET_WARNING_THRESHOLD
                || serializedBytes >= SERIALIZED_BYTES_WARNING_THRESHOLD) {
            LOG.warn(
                    "Partition tombstone for table {} is large: floor={}, explicitSetSize={}, "
                            + "serializedBytes={}.",
                    tablePath,
                    tombstone.getFloor(),
                    tombstone.getExplicitSet().size(),
                    serializedBytes);
        }
    }
}
