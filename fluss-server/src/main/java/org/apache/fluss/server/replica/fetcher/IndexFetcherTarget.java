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
 * Represents the goal state for an index fetcher connection. Each goal describes that an
 * IndexBucket should maintain a fetcher connection to fetch data from a specific DataBucket.
 *
 * <p>The goal is abstract and does not store the specific DataBucket leader server ID, as the
 * leader can change over time. The actual leader server ID is queried dynamically during each
 * reconciliation attempt.
 *
 * <p>The system continuously works towards achieving this goal state through async retry mechanism.
 * Various events (IndexBucket becoming leader, DataBucket leader change, fetch failures) can
 * trigger re-evaluation of the goal state.
 */
class IndexFetcherTarget {

    /** Current state of the goal. */
    enum State {
        /** Initial state, waiting to be processed. */
        PENDING,
        /** Currently creating the fetcher. */
        CREATING,
        /** Fetcher is running successfully. */
        RUNNING,
        /** Failed to create or run fetcher, will retry. */
        FAILED
    }

    private final DataIndexTableBucket dataIndexTableBucket;
    private final TablePath indexTablePath;
    private final IndexApplier indexApplier;

    private volatile State state;
    private volatile long lastAttemptTimestamp;
    private volatile int failureCount;
    @Nullable private volatile String lastFailureReason;
    // Track the last known leader server ID for logging and comparison purposes only
    private volatile int lastKnownLeaderServerId = -1;

    public IndexFetcherTarget(
            DataIndexTableBucket dataIndexTableBucket,
            TablePath indexTablePath,
            IndexApplier indexApplier) {
        this.dataIndexTableBucket = dataIndexTableBucket;
        this.indexTablePath = indexTablePath;
        this.indexApplier = indexApplier;
        this.state = State.PENDING;
        this.lastAttemptTimestamp = 0;
        this.failureCount = 0;
        this.lastFailureReason = null;
    }

    public DataIndexTableBucket getDataIndexTableBucket() {
        return dataIndexTableBucket;
    }

    public TableBucket getIndexBucket() {
        return dataIndexTableBucket.getIndexBucket();
    }

    public TableBucket getDataBucket() {
        return dataIndexTableBucket.getDataBucket();
    }

    public TablePath getIndexTablePath() {
        return indexTablePath;
    }

    public IndexApplier getIndexApplier() {
        return indexApplier;
    }

    public int getLastKnownLeaderServerId() {
        return lastKnownLeaderServerId;
    }

    public void setLastKnownLeaderServerId(int lastKnownLeaderServerId) {
        this.lastKnownLeaderServerId = lastKnownLeaderServerId;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    /**
     * Set the state and update the last attempt timestamp.
     *
     * @param state the new state
     * @param currentTimeMs the current timestamp in milliseconds
     */
    public void setState(State state, long currentTimeMs) {
        this.state = state;
        this.lastAttemptTimestamp = currentTimeMs;
    }

    public void setLastAttemptTimestamp(long lastAttemptTimestamp) {
        this.lastAttemptTimestamp = lastAttemptTimestamp;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void resetFailureCount() {
        this.failureCount = 0;
    }

    @Nullable
    public String getLastFailureReason() {
        return lastFailureReason;
    }

    /**
     * Mark this goal as failed with a reason. This will increment the failure count and update the
     * last failure reason and timestamp.
     *
     * @param reason the failure reason
     * @param currentTimeMs the current timestamp in milliseconds
     */
    public void markFailed(String reason, long currentTimeMs) {
        this.state = State.FAILED;
        this.lastFailureReason = reason;
        this.failureCount++;
        this.lastAttemptTimestamp = currentTimeMs;
    }

    /**
     * Mark this goal as running successfully. This will reset the failure count and clear the last
     * failure reason.
     */
    public void markRunning() {
        this.state = State.RUNNING;
        this.lastFailureReason = null;
        this.failureCount = 0;
    }

    /** Check if this goal is ready for retry based on exponential backoff strategy. */
    public boolean isReadyForRetry(long currentTimeMs, long baseBackoffMs) {
        if (state != State.FAILED && state != State.PENDING) {
            return false;
        }

        // Exponential backoff with max cap at 60 seconds
        long backoffMs = Math.min(baseBackoffMs * (1L << Math.min(failureCount, 10)), 60000);
        return currentTimeMs - lastAttemptTimestamp >= backoffMs;
    }

    /**
     * Check if this goal has been stuck in CREATING state for too long.
     *
     * @param currentTimeMs current timestamp
     * @param timeoutMs timeout threshold in milliseconds
     * @return true if stuck in CREATING state beyond timeout
     */
    public boolean isCreatingTimeout(long currentTimeMs, long timeoutMs) {
        return state == State.CREATING && currentTimeMs - lastAttemptTimestamp >= timeoutMs;
    }

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
        return "IndexFetcherTarget{"
                + "dataIndexBucket="
                + dataIndexTableBucket
                + ", lastKnownLeader="
                + (lastKnownLeaderServerId >= 0 ? lastKnownLeaderServerId : "unknown")
                + ", state="
                + state
                + ", failureCount="
                + failureCount
                + ", lastFailure='"
                + lastFailureReason
                + '\''
                + '}';
    }
}
