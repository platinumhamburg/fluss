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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.index.IndexApplier;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Represents a fetch target for index replication.
 *
 * <p>Each target describes that an IndexBucket should fetch index data from a specific DataBucket.
 * The target tracks its lifecycle state and failure information for retry management.
 *
 * <h2>State Machine</h2>
 *
 * <pre>
 *     PENDING ──────────────────────────────────────┐
 *        │                                          │
 *        ▼                                          │
 *     CREATING ─────────────────────────────────────┤
 *        │                                          │
 *        ▼                                          │
 *     RUNNING ──────────────────────────────────────┤
 *        │                                          │
 *        ▼                                          │
 *     FAILED ───────────────────────────────────────┘
 *        │                                    (retry)
 *        └──────────────────────────────────────────┘
 * </pre>
 */
class IndexFetcherTarget {

    /** Target lifecycle states. */
    enum State {
        /** Initial state, waiting to be processed. */
        PENDING,
        /** Fetcher is being created. */
        CREATING,
        /** Fetcher is running successfully. */
        RUNNING,
        /** Failed, will retry after backoff. */
        FAILED
    }

    // ==================== Constants ====================

    /** Minimum backoff time for failed targets (5 seconds). */
    private static final long MIN_RETRY_BACKOFF_MS = 5000L;

    /** Invalid leader server ID marker. */
    private static final int INVALID_LEADER_ID = -1;

    // ==================== Immutable Fields ====================

    private final DataIndexTableBucket dataIndexTableBucket;
    private final TablePath indexTablePath;
    private final IndexApplier indexApplier;

    // ==================== Mutable State ====================

    private volatile State state = State.PENDING;
    private volatile long lastAttemptTimestamp = 0;
    private volatile int failureCount = 0;
    @Nullable private volatile String lastFailureReason;
    private volatile int lastKnownLeaderServerId = INVALID_LEADER_ID;

    // ==================== Constructor ====================

    IndexFetcherTarget(
            DataIndexTableBucket dataIndexTableBucket,
            TablePath indexTablePath,
            IndexApplier indexApplier) {
        this.dataIndexTableBucket = dataIndexTableBucket;
        this.indexTablePath = indexTablePath;
        this.indexApplier = indexApplier;
    }

    // ==================== Getters ====================

    DataIndexTableBucket getDataIndexTableBucket() {
        return dataIndexTableBucket;
    }

    TableBucket getIndexBucket() {
        return dataIndexTableBucket.getIndexBucket();
    }

    TableBucket getDataBucket() {
        return dataIndexTableBucket.getDataBucket();
    }

    TablePath getIndexTablePath() {
        return indexTablePath;
    }

    IndexApplier getIndexApplier() {
        return indexApplier;
    }

    State getState() {
        return state;
    }

    int getFailureCount() {
        return failureCount;
    }

    @Nullable
    String getLastFailureReason() {
        return lastFailureReason;
    }

    int getLastKnownLeaderServerId() {
        return lastKnownLeaderServerId;
    }

    // ==================== State Transitions ====================

    void setState(State state) {
        this.state = state;
    }

    void setState(State state, long timestamp) {
        this.state = state;
        this.lastAttemptTimestamp = timestamp;
    }

    void setLastKnownLeaderServerId(int serverId) {
        this.lastKnownLeaderServerId = serverId;
    }

    /**
     * Marks this target as failed.
     *
     * @param reason failure reason
     * @param timestamp current timestamp
     */
    void markFailed(String reason, long timestamp) {
        this.state = State.FAILED;
        this.lastFailureReason = reason;
        this.failureCount++;
        this.lastAttemptTimestamp = timestamp;
    }

    /** Marks this target as running successfully. */
    void markRunning() {
        this.state = State.RUNNING;
        this.lastFailureReason = null;
        this.failureCount = 0;
    }

    /** Resets this target to pending state. */
    void resetToPending() {
        this.state = State.PENDING;
        this.failureCount = 0;
        this.lastFailureReason = null;
    }

    // ==================== Retry Logic ====================

    /**
     * Checks if this target is ready for retry.
     *
     * <p>Retry policy:
     *
     * <ul>
     *   <li>PENDING: Ready immediately
     *   <li>FAILED: Ready after MIN_RETRY_BACKOFF_MS (5 seconds)
     *   <li>Other states: Not ready
     * </ul>
     *
     * @param currentTimeMs current timestamp
     * @return true if ready for retry
     */
    boolean isReadyForRetry(long currentTimeMs) {
        if (state == State.PENDING) {
            return true;
        }
        if (state == State.FAILED) {
            return currentTimeMs - lastAttemptTimestamp >= MIN_RETRY_BACKOFF_MS;
        }
        return false;
    }

    /**
     * Checks if this target has been stuck in CREATING state.
     *
     * @param currentTimeMs current timestamp
     * @param timeoutMs timeout threshold
     * @return true if stuck
     */
    boolean isCreatingTimeout(long currentTimeMs, long timeoutMs) {
        return state == State.CREATING && currentTimeMs - lastAttemptTimestamp >= timeoutMs;
    }

    // ==================== Object Methods ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFetcherTarget that = (IndexFetcherTarget) o;
        return dataIndexTableBucket.equals(that.dataIndexTableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataIndexTableBucket);
    }

    @Override
    public String toString() {
        return String.format(
                "IndexFetcherTarget{bucket=%s, leader=%s, state=%s, failures=%d, reason='%s'}",
                dataIndexTableBucket,
                lastKnownLeaderServerId >= 0 ? lastKnownLeaderServerId : "unknown",
                state,
                failureCount,
                lastFailureReason);
    }
}
