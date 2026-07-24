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
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import javax.annotation.Nullable;

/** Fixed-size coverage counters for one table or partition target attempted by Scope. */
@Internal
public final class ScopeTargetStats {

    private final ScopeIdentity scope;
    private final long expectedBuckets;
    private final boolean kvApplicable;
    private long logResolvedBuckets;
    private long logNoManifestBuckets;
    private long logReadFailedBuckets;
    private long logUnavailableBuckets;
    private long outOfScopeBuckets;
    private long kvActiveBuckets;
    private long kvEmptyBuckets;
    private long kvUnavailableBuckets;
    private long disappearedBuckets;
    private long tasksEmitted;
    private long durationMillis;
    private boolean complete;
    private boolean coverageFailure;
    @Nullable private SkipReasonCode disappearanceReason;

    public ScopeTargetStats(ScopeIdentity scope, long expectedBuckets, boolean kvApplicable) {
        if (expectedBuckets < 0L) {
            throw new IllegalArgumentException("expectedBuckets");
        }
        this.scope = scope;
        this.expectedBuckets = expectedBuckets;
        this.kvApplicable = kvApplicable;
    }

    public void logResolvedBucket() {
        logResolvedBuckets++;
    }

    public void logNoManifestBucket() {
        logNoManifestBuckets++;
    }

    public void logReadFailedBucket() {
        logReadFailedBuckets++;
        coverageFailure = true;
    }

    public void logRpcFailed() {
        logUnavailableBuckets = expectedBuckets;
        coverageFailure = true;
    }

    public void outOfScope() {
        outOfScopeBuckets = expectedBuckets;
        coverageFailure = true;
    }

    public void kvActiveBucket() {
        kvActiveBuckets++;
    }

    public void kvEmptyBucket() {
        kvEmptyBuckets++;
    }

    public void kvRpcFailed() {
        if (kvApplicable) {
            kvUnavailableBuckets = expectedBuckets;
            coverageFailure = true;
        }
    }

    public void targetDisappeared(SkipReasonCode reason) {
        if (reason != SkipReasonCode.PARTITION_NOT_EXIST
                && reason != SkipReasonCode.TABLE_NOT_EXIST) {
            throw new IllegalArgumentException("reason");
        }
        disappearanceReason = reason;
        disappearedBuckets = expectedBuckets;
    }

    public void taskEmitted() {
        tasksEmitted++;
    }

    public void complete(long durationMillis) {
        finish(durationMillis, true);
    }

    public void incomplete(long durationMillis) {
        finish(durationMillis, false);
    }

    private void finish(long durationMillis, boolean complete) {
        if (durationMillis < 0L) {
            throw new IllegalArgumentException("durationMillis");
        }
        this.durationMillis = durationMillis;
        this.complete = complete;
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public long expectedBuckets() {
        return expectedBuckets;
    }

    public boolean kvApplicable() {
        return kvApplicable;
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
        return kvApplicable ? outOfScopeBuckets : 0L;
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

    public boolean disappeared() {
        return disappearanceReason != null;
    }

    @Nullable
    public SkipReasonCode disappearanceReason() {
        return disappearanceReason;
    }

    public long disappearedBuckets() {
        return disappearedBuckets;
    }

    public long kvDisappearedBuckets() {
        return kvApplicable ? disappearedBuckets : 0L;
    }

    public long tasksEmitted() {
        return tasksEmitted;
    }

    public long durationMillis() {
        return durationMillis;
    }

    public boolean complete() {
        return complete;
    }

    public boolean hasCoverageFailure() {
        return coverageFailure;
    }

    public boolean logCoverageConsistent() {
        return expectedBuckets
                == logResolvedBuckets
                        + logNoManifestBuckets
                        + logReadFailedBuckets
                        + logUnavailableBuckets
                        + disappearedBuckets
                        + outOfScopeBuckets;
    }

    public boolean kvCoverageConsistent() {
        if (!kvApplicable) {
            return kvActiveBuckets == 0L && kvEmptyBuckets == 0L && kvUnavailableBuckets == 0L;
        }
        return expectedBuckets
                == kvActiveBuckets
                        + kvEmptyBuckets
                        + kvUnavailableBuckets
                        + disappearedBuckets
                        + outOfScopeBuckets;
    }
}
