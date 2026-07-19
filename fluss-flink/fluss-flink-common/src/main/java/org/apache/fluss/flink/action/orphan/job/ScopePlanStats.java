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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;

/** Compact counters describing the cleanup scope and the tasks emitted by stage 1. */
@Internal
public final class ScopePlanStats {

    private long databases;
    private long tables;
    private long partitions;
    private long discoveredBuckets;
    private long bucketTasks;
    private long orphanDirTasks;
    private long skippedNoRemoteManifest;
    private long skippedEmptyKvActiveSet;
    private long skippedOutOfScopeRoot;
    private long metadataFailures;
    private long scopeTargets;
    private long targetBuckets;
    private long kvTargetBuckets;
    private long logResolvedBuckets;
    private long logNoManifestBuckets;
    private long logReadFailedBuckets;
    private long logUnavailableBuckets;
    private long outOfScopeBuckets;
    private long kvOutOfScopeBuckets;
    private long kvActiveBuckets;
    private long kvEmptyBuckets;
    private long kvUnavailableBuckets;
    private long incompleteTargets;

    public void database() {
        databases++;
    }

    public void table() {
        tables++;
    }

    public void partition() {
        partitions++;
    }

    public void discoveredBucket() {
        discoveredBuckets++;
    }

    public void discoveredBuckets(long count) {
        if (count < 0L) {
            throw new IllegalArgumentException("count");
        }
        discoveredBuckets += count;
    }

    public void bucketTask() {
        bucketTasks++;
    }

    public void orphanDirTask() {
        orphanDirTasks++;
    }

    public void skippedNoRemoteManifest() {
        skippedNoRemoteManifest++;
    }

    public void skippedEmptyKvActiveSet() {
        skippedEmptyKvActiveSet++;
    }

    public void skippedOutOfScopeRoot() {
        skippedOutOfScopeRoot++;
    }

    public void metadataFailure() {
        metadataFailures++;
    }

    public void target(ScopeTargetStats target) {
        scopeTargets++;
        targetBuckets += target.expectedBuckets();
        if (target.kvApplicable()) {
            kvTargetBuckets += target.expectedBuckets();
        }
        logResolvedBuckets += target.logResolvedBuckets();
        logNoManifestBuckets += target.logNoManifestBuckets();
        logReadFailedBuckets += target.logReadFailedBuckets();
        logUnavailableBuckets += target.logUnavailableBuckets();
        outOfScopeBuckets += target.outOfScopeBuckets();
        kvOutOfScopeBuckets += target.kvOutOfScopeBuckets();
        kvActiveBuckets += target.kvActiveBuckets();
        kvEmptyBuckets += target.kvEmptyBuckets();
        kvUnavailableBuckets += target.kvUnavailableBuckets();
        if (!target.complete()
                || !target.logCoverageConsistent()
                || !target.kvCoverageConsistent()) {
            incompleteTargets++;
        }
    }

    public long databases() {
        return databases;
    }

    public long tables() {
        return tables;
    }

    public long partitions() {
        return partitions;
    }

    public long discoveredBuckets() {
        return discoveredBuckets;
    }

    public long bucketTasks() {
        return bucketTasks;
    }

    public long orphanDirTasks() {
        return orphanDirTasks;
    }

    public long skippedNoRemoteManifestCount() {
        return skippedNoRemoteManifest;
    }

    public long skippedEmptyKvActiveSetCount() {
        return skippedEmptyKvActiveSet;
    }

    public long skippedOutOfScopeRootCount() {
        return skippedOutOfScopeRoot;
    }

    public long metadataFailures() {
        return metadataFailures;
    }

    public long scopeTargets() {
        return scopeTargets;
    }

    public long targetBuckets() {
        return targetBuckets;
    }

    public long kvTargetBuckets() {
        return kvTargetBuckets;
    }

    public long logResolvedBuckets() {
        return logResolvedBuckets;
    }

    public long logNoManifestBuckets() {
        return logNoManifestBuckets;
    }

    public long logReadFailedBuckets() {
        return logReadFailedBuckets;
    }

    public long logUnavailableBuckets() {
        return logUnavailableBuckets;
    }

    public long outOfScopeBuckets() {
        return outOfScopeBuckets;
    }

    public long kvOutOfScopeBuckets() {
        return kvOutOfScopeBuckets;
    }

    public long kvActiveBuckets() {
        return kvActiveBuckets;
    }

    public long kvEmptyBuckets() {
        return kvEmptyBuckets;
    }

    public long kvUnavailableBuckets() {
        return kvUnavailableBuckets;
    }

    public long incompleteTargets() {
        return incompleteTargets;
    }

    public boolean countersConsistent() {
        return discoveredBuckets == targetBuckets
                && targetBuckets
                        == logResolvedBuckets
                                + logNoManifestBuckets
                                + logReadFailedBuckets
                                + logUnavailableBuckets
                                + outOfScopeBuckets
                && kvTargetBuckets
                        == kvActiveBuckets
                                + kvEmptyBuckets
                                + kvUnavailableBuckets
                                + kvOutOfScopeBuckets;
    }

    public boolean coverageComplete() {
        return metadataFailures == 0L && incompleteTargets == 0L && countersConsistent();
    }
}
