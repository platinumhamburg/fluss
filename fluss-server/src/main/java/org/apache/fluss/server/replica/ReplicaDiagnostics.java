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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.index.IndexDataProducer;
import org.apache.fluss.server.log.LogTablet;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/** Helper class for building timeout diagnostics messages for delayed write operations. */
@Internal
final class ReplicaDiagnostics {

    private ReplicaDiagnostics() {}

    static String getTimeoutDiagnostics(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            int localTabletServerId,
            Map<Integer, FollowerReplica> followerReplicasMap,
            IsrState isrState,
            IntSupplier minInSyncReplicasSupplier,
            boolean isTableWithIndexes,
            @Nullable IndexDataProducer indexDataProducer,
            long requiredOffset) {
        StringBuilder diagnostics = new StringBuilder();
        diagnostics
                .append("Delayed write operation timed out for table ")
                .append(physicalPath)
                .append(", bucket ")
                .append(tableBucket)
                .append(". Required offset: ")
                .append(requiredOffset)
                .append(". Diagnostics: ");

        appendLogReplicationDiagnostics(
                diagnostics,
                logTablet,
                localTabletServerId,
                followerReplicasMap,
                isrState,
                requiredOffset);
        appendIndexReplicationDiagnostics(
                diagnostics, isTableWithIndexes, indexDataProducer, requiredOffset);
        appendIsrStatusDiagnostics(diagnostics, isrState, minInSyncReplicasSupplier);

        return diagnostics.toString();
    }

    private static void appendLogReplicationDiagnostics(
            StringBuilder diagnostics,
            LogTablet logTablet,
            int localTabletServerId,
            Map<Integer, FollowerReplica> followerReplicasMap,
            IsrState isrState,
            long requiredOffset) {
        long highWatermark = logTablet.getHighWatermark();
        if (highWatermark < requiredOffset) {
            diagnostics
                    .append("[Log Replication] High watermark (")
                    .append(highWatermark)
                    .append(") has not reached required offset, lag: ")
                    .append(requiredOffset - highWatermark)
                    .append(" offsets. ");
            appendFollowerReplicaDiagnostics(
                    diagnostics,
                    localTabletServerId,
                    followerReplicasMap,
                    isrState,
                    requiredOffset);
        } else {
            diagnostics
                    .append("[Log Replication] High watermark (")
                    .append(highWatermark)
                    .append(") has reached required offset. ");
        }
    }

    private static void appendFollowerReplicaDiagnostics(
            StringBuilder diagnostics,
            int localTabletServerId,
            Map<Integer, FollowerReplica> followerReplicasMap,
            IsrState isrState,
            long requiredOffset) {
        List<String> followerInfoList = new ArrayList<>();
        for (Integer replicaId : isrState.maximalIsr()) {
            if (replicaId != localTabletServerId && followerReplicasMap.containsKey(replicaId)) {
                FollowerReplica followerReplica = followerReplicasMap.get(replicaId);
                long followerLogEndOffset = followerReplica.stateSnapshot().getLogEndOffset();
                long followerLag = requiredOffset - followerLogEndOffset;
                String status = followerLogEndOffset >= requiredOffset ? "synced" : "lagging";
                followerInfoList.add(
                        String.format(
                                "server-%d: LEO=%d, lag=%d, status=%s",
                                replicaId, followerLogEndOffset, followerLag, status));
            }
        }
        if (!followerInfoList.isEmpty()) {
            diagnostics
                    .append("Follower replicas: ")
                    .append(String.join(", ", followerInfoList))
                    .append(". ");
        }
    }

    private static void appendIndexReplicationDiagnostics(
            StringBuilder diagnostics,
            boolean isTableWithIndexes,
            @Nullable IndexDataProducer indexDataProducer,
            long requiredOffset) {
        if (!isTableWithIndexes) {
            return;
        }
        if (indexDataProducer == null) {
            diagnostics.append("[Index Replication] IndexDataProducer is not available. ");
            return;
        }
        long indexCommitHorizon = indexDataProducer.getIndexCommitHorizon();
        if (indexCommitHorizon < requiredOffset) {
            diagnostics
                    .append("[Index Replication] Index commit horizon (")
                    .append(indexCommitHorizon)
                    .append(") has not reached required offset, lag: ")
                    .append(requiredOffset - indexCommitHorizon)
                    .append(" offsets. ");
            appendBlockingBucketDiagnostics(diagnostics, indexDataProducer, requiredOffset);
        } else {
            diagnostics
                    .append("[Index Replication] Index commit horizon (")
                    .append(indexCommitHorizon)
                    .append(") has reached required offset. ");
        }
    }

    private static void appendBlockingBucketDiagnostics(
            StringBuilder diagnostics, IndexDataProducer producer, long requiredOffset) {
        Map<TableBucket, String> blockingBuckets =
                producer.getCacheManager().getBlockingBucketDiagnostics();
        if (blockingBuckets.isEmpty()) {
            return;
        }
        diagnostics.append("Blocking index buckets (at commit horizon): ");
        for (String bucketDiag : blockingBuckets.values()) {
            diagnostics.append("\n  - ").append(bucketDiag);
        }
        diagnostics.append("\n========== Detailed Diagnostic for Blocking IndexBuckets ==========");
        for (Map.Entry<TableBucket, String> entry : blockingBuckets.entrySet()) {
            TableBucket indexBucket = entry.getKey();
            String detailedDiag =
                    producer.getIndexBucketDiagnosticInfo(
                            indexBucket, requiredOffset, requiredOffset + 1000);
            diagnostics.append(detailedDiag);
        }
        diagnostics.append("\n============================================. ");
    }

    private static void appendIsrStatusDiagnostics(
            StringBuilder diagnostics, IsrState isrState, IntSupplier minInSyncReplicasSupplier) {
        diagnostics
                .append("[ISR Status] Current ISR: ")
                .append(isrState.isr())
                .append(", Maximal ISR: ")
                .append(isrState.maximalIsr())
                .append(", Min ISR required: ")
                .append(minInSyncReplicasSupplier.getAsInt())
                .append(", ISR size sufficient: ")
                .append(isrState.maximalIsr().size() >= minInSyncReplicasSupplier.getAsInt())
                .append(".");
    }
}
