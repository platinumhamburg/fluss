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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchIndexRequest;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchIndexRespForIndexTableBucket;
import org.apache.fluss.rpc.messages.PbFetchIndexRespForTableBucket;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.ListOffsetsParam;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchIndexLogResultForBucket;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchLogResultForBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsRequest;

/** Connection health state for rate limiting. */
enum EndpointHealthState {
    // Normal operation, no rate limiting.
    HEALTHY,
    // Network/client/metadata issues detected, rate limiting enabled.
    UNHEALTHY
}

/** Facilitates fetches from a remote replica leader in one tablet server. */
final class RemoteLeaderEndpoint implements LeaderEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLeaderEndpoint.class);

    private final int followerServerId;
    private final int remoteServerId;
    private final TabletServerGateway tabletServerGateway;
    /** The max size for the fetch response. */
    private final int maxFetchSize;
    /** The max fetch size for a bucket in bytes. */
    private final int maxFetchSizeForBucket;

    private final int minFetchBytes;
    private final int maxFetchWaitMs;

    /** Connection health state management. */
    private AtomicReference<EndpointHealthState> connectionHealth =
            new AtomicReference<>(EndpointHealthState.HEALTHY);
    /** Rate limiter for unhealthy connections - 1 request per second. */
    private final RateLimiter unhealthyRateLimiter = RateLimiter.create(1.0);

    RemoteLeaderEndpoint(
            Configuration conf,
            int followerServerId,
            int remoteServerId,
            TabletServerGateway tabletServerGateway) {
        this.followerServerId = followerServerId;
        this.remoteServerId = remoteServerId;
        this.maxFetchSize = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes();
        this.maxFetchSizeForBucket =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES_FOR_BUCKET).getBytes();
        this.minFetchBytes = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MIN_BYTES).getBytes();
        this.maxFetchWaitMs =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_WAIT_MAX_TIME).toMillis();
        this.tabletServerGateway = tabletServerGateway;
    }

    @Override
    public int leaderServerId() {
        return remoteServerId;
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.LATEST_OFFSET_TYPE);
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.EARLIEST_OFFSET_TYPE);
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        return fetchLogOffset(tableBucket, ListOffsetsParam.LEADER_END_OFFSET_SNAPSHOT_TYPE);
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        // Check rate limiting before making request
        checkAndTryWaitHealthy();

        FetchLogRequest fetchLogRequest = fetchLogContext.getFetchLogRequest();
        try {
            return tabletServerGateway
                    .fetchLog(fetchLogRequest)
                    .thenApply(
                            fetchLogResponse -> {
                                // Mark as healthy on successful response
                                markEndpointHealthy();

                                Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap =
                                        new HashMap<>();
                                List<PbFetchLogRespForTable> tablesRespList =
                                        fetchLogResponse.getTablesRespsList();
                                for (PbFetchLogRespForTable tableResp : tablesRespList) {
                                    long tableId = tableResp.getTableId();
                                    List<PbFetchLogRespForBucket> bucketsRespList =
                                            tableResp.getBucketsRespsList();
                                    for (PbFetchLogRespForBucket bucketResp : bucketsRespList) {
                                        TableBucket tableBucket =
                                                new TableBucket(
                                                        tableId,
                                                        bucketResp.hasPartitionId()
                                                                ? bucketResp.getPartitionId()
                                                                : null,
                                                        bucketResp.getBucketId());
                                        TablePath tablePath = fetchLogContext.getTablePath(tableId);
                                        FetchLogResultForBucket fetchLogResultForBucket =
                                                getFetchLogResultForBucket(
                                                        tableBucket, tablePath, bucketResp);
                                        fetchLogResultMap.put(tableBucket, fetchLogResultForBucket);
                                    }
                                }

                                return new FetchData(fetchLogResponse, fetchLogResultMap);
                            })
                    .exceptionally(
                            throwable -> {
                                // Mark as unhealthy on network exception
                                if (isNetworkException(throwable)) {
                                    markEndpointUnhealthy();
                                }
                                throw new RuntimeException(throwable);
                            });
        } catch (Exception e) {
            if (ExceptionUtils.findThrowableWithMessage(e, "Netty client is closed.").isPresent()) {
                markEndpointUnhealthy();
            }
            if (ExceptionUtils.findThrowableWithMessage(e, "is not available in metadata cache.")
                    .isPresent()) {
                markEndpointUnhealthy();
            }
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext) {
        // Check rate limiting before making request
        checkAndTryWaitHealthy();

        FetchIndexRequest fetchIndexRequest = fetchIndexContext.getFetchIndexRequest();
        try {
            return tabletServerGateway
                    .fetchIndex(fetchIndexRequest)
                    .thenApply(
                            fetchIndexResponse -> {
                                // Mark as healthy on successful response
                                markEndpointHealthy();

                                Map<DataIndexTableBucket, FetchIndexLogResultForBucket>
                                        fetchIndexResultMap = new HashMap<>();
                                List<PbFetchIndexRespForIndexTableBucket> indexBucketResponseList =
                                        fetchIndexResponse.getRespForIndexTableBucketsList();
                                for (PbFetchIndexRespForIndexTableBucket indexBucketResponse :
                                        indexBucketResponseList) {
                                    TableBucket indexTableBucket =
                                            new TableBucket(
                                                    indexBucketResponse.getTableId(),
                                                    indexBucketResponse.hasPartitionId()
                                                            ? indexBucketResponse.getPartitionId()
                                                            : null,
                                                    indexBucketResponse.getBucketId());
                                    List<PbFetchIndexRespForTableBucket> tableBucketsResponseList =
                                            indexBucketResponse.getRespForTablesList();
                                    for (PbFetchIndexRespForTableBucket tableBucketResponse :
                                            tableBucketsResponseList) {
                                        TableBucket dataTableBucket =
                                                new TableBucket(
                                                        tableBucketResponse.getTableId(),
                                                        tableBucketResponse.hasPartitionId()
                                                                ? tableBucketResponse
                                                                        .getPartitionId()
                                                                : null,
                                                        tableBucketResponse.getBucketId());
                                        FetchIndexLogResultForBucket fetchIndexLogResultForBucket =
                                                getFetchIndexLogResultForBucket(
                                                        tableBucketResponse);
                                        fetchIndexResultMap.put(
                                                new DataIndexTableBucket(
                                                        dataTableBucket, indexTableBucket),
                                                fetchIndexLogResultForBucket);
                                    }
                                }
                                return new FetchIndexData(fetchIndexResponse, fetchIndexResultMap);
                            })
                    .exceptionally(
                            throwable -> {
                                // Mark as unhealthy on network exception
                                if (isNetworkException(throwable)) {
                                    markEndpointUnhealthy();
                                }
                                throw new RuntimeException(throwable);
                            });
        } catch (Exception e) {
            if (ExceptionUtils.findThrowableWithMessage(e, "Netty client is closed.").isPresent()) {
                markEndpointUnhealthy();
            }
            if (ExceptionUtils.findThrowableWithMessage(e, "is not available in metadata cache.")
                    .isPresent()) {
                markEndpointUnhealthy();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas) {
        return buildFetchLogContext(
                replicas,
                followerServerId,
                maxFetchSize,
                maxFetchSizeForBucket,
                minFetchBytes,
                maxFetchWaitMs);
    }

    @Override
    public void close() {
        // nothing to do now.
    }

    static Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas,
            int followerServerId,
            int maxFetchSize,
            int maxFetchSizeForBucket,
            int minFetchBytes,
            int maxFetchWaitMs) {
        Map<Long, TablePath> tableIdToTablePath = new HashMap<>();
        FetchLogRequest fetchRequest =
                new FetchLogRequest()
                        .setFollowerServerId(followerServerId)
                        .setMaxBytes(maxFetchSize)
                        .setMinBytes(minFetchBytes)
                        .setMaxWaitMs(maxFetchWaitMs);
        Map<Long, List<PbFetchLogReqForBucket>> fetchLogReqForBuckets = new HashMap<>();
        int readyForFetchCount = 0;
        for (Map.Entry<TableBucket, BucketFetchStatus> entry : replicas.entrySet()) {
            TableBucket tb = entry.getKey();
            BucketFetchStatus bucketFetchStatus = entry.getValue();
            if (bucketFetchStatus.isReadyForFetch()) {
                PbFetchLogReqForBucket fetchLogReqForBucket =
                        new PbFetchLogReqForBucket()
                                .setBucketId(tb.getBucket())
                                .setFetchOffset(bucketFetchStatus.fetchOffset())
                                .setMaxFetchBytes(maxFetchSizeForBucket);
                if (tb.getPartitionId() != null) {
                    fetchLogReqForBucket.setPartitionId(tb.getPartitionId());
                }
                fetchLogReqForBuckets
                        .computeIfAbsent(tb.getTableId(), key -> new ArrayList<>())
                        .add(fetchLogReqForBucket);

                tableIdToTablePath.put(tb.getTableId(), bucketFetchStatus.tablePath());
                readyForFetchCount++;
            }
        }

        if (readyForFetchCount == 0) {
            return Optional.empty();
        } else {
            fetchLogReqForBuckets.forEach(
                    (tableId, buckets) ->
                            fetchRequest
                                    .addTablesReq()
                                    .setProjectionPushdownEnabled(false)
                                    .setTableId(tableId)
                                    .addAllBucketsReqs(buckets));
            return Optional.of(new FetchLogContext(tableIdToTablePath, fetchRequest));
        }
    }

    /** Fetch log offset with given offset type. */
    private CompletableFuture<Long> fetchLogOffset(TableBucket tableBucket, int offsetType) {
        // Check rate limiting before making request
        checkAndTryWaitHealthy();

        return tabletServerGateway
                .listOffsets(
                        makeListOffsetsRequest(
                                followerServerId,
                                offsetType,
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                tableBucket.getBucket()))
                .thenApply(
                        response -> {
                            // Mark as healthy on successful response
                            markEndpointHealthy();

                            PbListOffsetsRespForBucket respForBucket =
                                    response.getBucketsRespsList().get(0);
                            if (respForBucket.hasErrorCode()) {
                                throw Errors.forCode(respForBucket.getErrorCode())
                                        .exception(respForBucket.getErrorMessage());
                            } else {
                                return respForBucket.getOffset();
                            }
                        })
                .exceptionally(
                        throwable -> {
                            // Mark as unhealthy on network exception
                            if (isNetworkException(throwable)) {
                                markEndpointUnhealthy();
                            }
                            throw new RuntimeException(throwable);
                        });
    }

    /**
     * Check connection health and apply rate limiting if necessary. This is a private method to
     * handle rate limiting globally across all requests.
     */
    private void checkAndTryWaitHealthy() {
        if (connectionHealth.get().equals(EndpointHealthState.UNHEALTHY)) {
            // Block until rate limiter allows the request
            unhealthyRateLimiter.acquire();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Rate limited request to remote server {} due to unhealthy connection",
                        remoteServerId);
            }
        }
    }

    /** Mark connection as healthy and disable rate limiting. */
    private void markEndpointHealthy() {
        if (connectionHealth.compareAndSet(
                EndpointHealthState.UNHEALTHY, EndpointHealthState.HEALTHY)) {
            LOG.info(
                    "Connection to remote server {} recovered, disabling rate limiting",
                    remoteServerId);
        }
    }

    /** Mark connection as unhealthy and enable rate limiting. */
    private void markEndpointUnhealthy() {
        if (connectionHealth.compareAndSet(
                EndpointHealthState.HEALTHY, EndpointHealthState.UNHEALTHY)) {
            LOG.warn(
                    "Endpoint issues detected with remote server {}, enabling rate limiting (1 req/sec)",
                    remoteServerId);
        }
    }

    /**
     * Check if the given throwable represents a network exception that should trigger rate
     * limiting.
     */
    private boolean isNetworkException(Throwable throwable) {
        // Unwrap CompletionException if present
        if (ExceptionUtils.findThrowable(throwable, NetworkException.class).isPresent()) {
            return true;
        }
        if (ExceptionUtils.findThrowable(throwable, ConnectException.class).isPresent()) {
            return true;
        }
        return ExceptionUtils.findThrowable(throwable, ClosedChannelException.class).isPresent();
    }
}
