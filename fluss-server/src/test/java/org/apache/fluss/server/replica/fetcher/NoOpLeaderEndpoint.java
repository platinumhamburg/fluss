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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link LeaderEndpoint} test double that does nothing harmful. All offset methods return 0L.
 *
 * <p>Two modes are supported:
 *
 * <ul>
 *   <li><b>Idle mode</b> (default): {@code fetchLog} and {@code fetchIndex} return futures that
 *       never complete, keeping targets in RUNNING state without triggering any processing.
 *   <li><b>Immediate mode</b>: {@code fetchLog} and {@code fetchIndex} return immediately completed
 *       futures with {@code null}, simulating a successful but empty fetch.
 * </ul>
 */
class NoOpLeaderEndpoint implements LeaderEndpoint {
    private final int serverId;
    private final boolean completeImmediately;

    /** Creates a no-op endpoint in idle mode (fetches never complete). */
    NoOpLeaderEndpoint(int serverId) {
        this(serverId, false);
    }

    /**
     * Creates a no-op endpoint.
     *
     * @param serverId the leader server ID to report
     * @param completeImmediately if true, fetches complete immediately with null; if false, fetches
     *     never complete
     */
    NoOpLeaderEndpoint(int serverId, boolean completeImmediately) {
        this.serverId = serverId;
        this.completeImmediately = completeImmediately;
    }

    @Override
    public int leaderServerId() {
        return serverId;
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        return completeImmediately
                ? CompletableFuture.completedFuture(null)
                : new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext) {
        return completeImmediately
                ? CompletableFuture.completedFuture(null)
                : new CompletableFuture<>();
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
