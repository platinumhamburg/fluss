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

package org.apache.fluss.server.index;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * State information for tracking remote index bucket fetch operations. This class records both the
 * commit offset and diagnostic information to help identify replication stalls.
 *
 * <p>Thread-safe: All fields use atomic operations for concurrent access.
 */
@ThreadSafe
public class RemoteFetchState {
    private final AtomicLong commitOffset;
    private final AtomicLong lastUpdateTimeMs;
    private final AtomicInteger leaderServerId;
    private final AtomicLong lastFetchOffset;

    public RemoteFetchState() {
        this.commitOffset = new AtomicLong(-1L);
        this.lastUpdateTimeMs = new AtomicLong(-1L);
        this.leaderServerId = new AtomicInteger(-1);
        this.lastFetchOffset = new AtomicLong(-1L);
    }

    /**
     * Updates the commit offset and related metadata.
     *
     * @param newCommitOffset the new commit offset
     * @param leaderServerId the leader server id that this bucket is fetching from
     */
    public void updateCommitOffset(long newCommitOffset, int leaderServerId) {
        this.commitOffset.set(newCommitOffset);
        this.lastUpdateTimeMs.set(System.currentTimeMillis());
        this.leaderServerId.set(leaderServerId);
    }

    /**
     * Records the last fetch offset attempt.
     *
     * @param fetchOffset the offset being fetched
     */
    public void recordFetchOffset(long fetchOffset) {
        this.lastFetchOffset.set(fetchOffset);
    }

    public long getCommitOffset() {
        return commitOffset.get();
    }

    public long getLastUpdateTimeMs() {
        return lastUpdateTimeMs.get();
    }

    public int getLeaderServerId() {
        return leaderServerId.get();
    }

    public long getLastFetchOffset() {
        return lastFetchOffset.get();
    }

    /**
     * Returns a diagnostic string for this fetch state.
     *
     * @param indexTableId the index table id
     * @param bucketId the bucket id
     * @return formatted diagnostic string
     */
    public String getDiagnosticString(long indexTableId, int bucketId) {
        long now = System.currentTimeMillis();
        long lastUpdate = lastUpdateTimeMs.get();
        long timeSinceUpdate = lastUpdate > 0 ? (now - lastUpdate) : -1;

        return String.format(
                "IndexBucket{tableId=%d, bucket=%d}: commitOffset=%d, lastFetchOffset=%d, "
                        + "leaderServerId=%d, lastUpdateTimeAgo=%dms",
                indexTableId,
                bucketId,
                commitOffset.get(),
                lastFetchOffset.get(),
                leaderServerId.get(),
                timeSinceUpdate);
    }

    @Override
    public String toString() {
        return String.format(
                "RemoteFetchState{commitOffset=%d, lastFetchOffset=%d, leaderServerId=%d, lastUpdateTimeMs=%d}",
                commitOffset.get(),
                lastFetchOffset.get(),
                leaderServerId.get(),
                lastUpdateTimeMs.get());
    }
}
