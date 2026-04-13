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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.FutureUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link LeaderEndpoint} test double that always fails with a configurable exception. Simulates
 * connecting to a dead or unreachable server (e.g., "Server X not in cache").
 *
 * <p>Tracks the number of {@code fetchLog} and {@code fetchIndex} attempts via {@link
 * #getAttemptCount()}.
 */
class FailingLeaderEndpoint implements LeaderEndpoint {
    private final int serverId;
    private final RuntimeException exception;
    private final AtomicInteger attemptCount = new AtomicInteger(0);

    FailingLeaderEndpoint(int serverId, RuntimeException exception) {
        this.serverId = serverId;
        this.exception = exception;
    }

    /** Convenience constructor that creates a default "Server X not in cache" exception. */
    FailingLeaderEndpoint(int serverId) {
        this(serverId, new RuntimeException("Server " + serverId + " not in cache"));
    }

    int getAttemptCount() {
        return attemptCount.get();
    }

    @Override
    public int leaderServerId() {
        return serverId;
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        return FutureUtils.failedCompletableFuture(exception);
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        return FutureUtils.failedCompletableFuture(exception);
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        return FutureUtils.failedCompletableFuture(exception);
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        attemptCount.incrementAndGet();
        return FutureUtils.failedCompletableFuture(exception);
    }

    @Override
    public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext) {
        attemptCount.incrementAndGet();
        return FutureUtils.failedCompletableFuture(exception);
    }

    @Override
    public Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas) {
        return Optional.empty();
    }

    @Override
    public void close() {
        // no-op
    }
}
