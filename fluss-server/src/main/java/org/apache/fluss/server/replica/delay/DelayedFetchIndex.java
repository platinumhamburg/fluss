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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.exception.FetchIndexEarlyFireException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.DataBucketIndexFetchResult;
import org.apache.fluss.server.entity.FetchIndexReqInfo;
import org.apache.fluss.server.index.FetchIndexParams;
import org.apache.fluss.server.index.IndexSegment;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Sets;
import org.apache.fluss.utils.StringUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A delayed fetch index operation that can be created by the {@link ReplicaManager} and watched in
 * the delayed fetch index operation manager. This operation is used to delay index fetch requests
 * when there is insufficient index data available or when index data is being built.
 */
public class DelayedFetchIndex extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedFetchIndex.class);

    private final FetchIndexParams params;
    private final ReplicaManager replicaManager;
    private final Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> dataBucketRequests;
    private final Map<TableBucket, DataBucketIndexFetchResult> completeFetches;
    private final Consumer<Map<TableBucket, DataBucketIndexFetchResult>> responseCallback;
    private final TabletServerMetricGroup serverMetricGroup;
    private final AtomicInteger fetchedBytes;

    public DelayedFetchIndex(
            FetchIndexParams params,
            ReplicaManager replicaManager,
            Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> dataBucketRequests,
            int alreadyFetchedBytes,
            Map<TableBucket, DataBucketIndexFetchResult> completeFetches,
            Consumer<Map<TableBucket, DataBucketIndexFetchResult>> responseCallback,
            TabletServerMetricGroup serverMetricGroup) {
        super(params.maxWaitMs());
        this.params = params;
        this.replicaManager = replicaManager;
        this.dataBucketRequests = dataBucketRequests;
        this.completeFetches = completeFetches;
        this.responseCallback = responseCallback;
        this.serverMetricGroup = serverMetricGroup;
        this.fetchedBytes = new AtomicInteger(alreadyFetchedBytes);
    }

    /** Upon completion, read whatever index data is available and pass to the complete callback. */
    @Override
    public void onComplete() {
        Set<TableBucket> remainingDataBuckets =
                Sets.difference(dataBucketRequests.keySet(), completeFetches.keySet());

        if (remainingDataBuckets.isEmpty()) {
            // No more data buckets to fetch
            responseCallback.accept(completeFetches);
            return;
        }

        for (TableBucket dataBucket : remainingDataBuckets) {
            if (fetchedBytes.get() > params.maxBytes()) {
                break;
            }

            Map<TableBucket, FetchIndexReqInfo> indexBucketFetchRequests =
                    dataBucketRequests.get(dataBucket);
            Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults = new HashMap<>();
            try {
                Replica dataReplica = replicaManager.getReplicaOrException(dataBucket);
                Tuple2<Integer, Optional<Map<TableBucket, IndexSegment>>> fetchResult =
                        dataReplica.fetchIndex(
                                indexBucketFetchRequests,
                                params.minAdvancedOffset(),
                                params.maxBytes() - fetchedBytes.get(),
                                true);
                int newFetchedBytes = fetchResult.f0;
                Optional<Map<TableBucket, IndexSegment>> fetchResultForDataBucketOpt =
                        fetchResult.f1;
                fetchedBytes.addAndGet(newFetchedBytes);

                // Handle the case when IndexCache is closed or fetch result is empty
                if (!fetchResultForDataBucketOpt.isPresent()) {
                    LOG.warn(
                            "Fetch result is empty for data bucket {}, IndexCache may be closed",
                            dataBucket);
                    // Return error result indicating the data bucket is no longer available
                    for (TableBucket indexBucket : indexBucketFetchRequests.keySet()) {
                        indexBucketResults.put(
                                indexBucket,
                                new FetchIndexLogResultForBucket(
                                        ApiError.fromThrowable(
                                                new IllegalStateException(
                                                        "IndexCache is closed or unavailable for data bucket "
                                                                + dataBucket))));
                    }
                    completeFetches.put(
                            dataBucket, new DataBucketIndexFetchResult(indexBucketResults));
                    continue;
                }

                for (TableBucket indexBucket : indexBucketFetchRequests.keySet()) {
                    IndexSegment segment = fetchResultForDataBucketOpt.get().get(indexBucket);
                    checkNotNull(
                            segment, "Index segment for index bucket " + indexBucket + " is null");
                    // Convert IndexSegment.DataStatus to boolean isDataReady
                    boolean isDataReady = segment.getStatus() == IndexSegment.DataStatus.LOADED;
                    indexBucketResults.put(
                            indexBucket,
                            new FetchIndexLogResultForBucket(
                                    segment.getRecords(),
                                    segment.getStartOffset(),
                                    segment.getEndOffset(),
                                    isDataReady));
                }
                completeFetches.put(dataBucket, new DataBucketIndexFetchResult(indexBucketResults));
            } catch (Exception e) {
                LOG.error("Failed to fetch index for data bucket {}", dataBucket, e);
                // Return error result
                for (TableBucket indexBucket : indexBucketFetchRequests.keySet()) {
                    indexBucketResults.put(
                            indexBucket,
                            new FetchIndexLogResultForBucket(ApiError.fromThrowable(e)));
                }
                completeFetches.put(dataBucket, new DataBucketIndexFetchResult(indexBucketResults));
            }
        }

        ApiError errorResultForDataBucket =
                ApiError.fromThrowable(new FetchIndexEarlyFireException(StringUtils.EMPTY_STRING));
        Sets.difference(dataBucketRequests.keySet(), completeFetches.keySet())
                .forEach(
                        dataBucket ->
                                completeFetches.put(
                                        dataBucket,
                                        DataBucketIndexFetchResult.errorResult(
                                                dataBucketRequests.get(dataBucket).keySet(),
                                                errorResultForDataBucket)));

        responseCallback.accept(completeFetches);
    }

    /**
     * The delayed index fetch operation can be completed if:
     *
     * <ul>
     *   <li>Case A: The data bucket server is no longer the leader for some data buckets it tries
     *       to fetch from
     *   <li>Case B: The data replica is no longer available on this server
     *   <li>Case C: This server doesn't know of some data buckets it tries to fetch from
     *   <li>Case D: The accumulated index records from all the fetching buckets exceeds the minimum
     *       records
     *   <li>Case E: The accumulated index records from all the fetching buckets exceeds the maximum
     *       records
     * </ul>
     *
     * <p>Upon completion, should return whatever index data is available for each valid data
     * bucket.
     *
     * <p>This implementation follows the DelayedFetchLog pattern: tryComplete only checks
     * conditions and calculates offset information without reading actual data, while onComplete
     * performs the actual IndexCache fetch operation.
     */
    @Override
    public boolean tryComplete() {
        if (checkCompletionConditions()) {
            return true;
        }

        Set<TableBucket> remainingDataBuckets =
                Sets.difference(dataBucketRequests.keySet(), completeFetches.keySet());

        for (TableBucket dataBucket : remainingDataBuckets) {
            Map<TableBucket, FetchIndexReqInfo> indexBucketFetchRequests =
                    dataBucketRequests.get(dataBucket);
            Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults = new HashMap<>();
            try {
                Replica dataReplica = replicaManager.getReplicaOrException(dataBucket);
                checkArgument(
                        dataReplica.isLeader(), "Data bucket " + dataBucket + " is not leader");

                // In tryComplete, use hotDataOnly=true to only check hot data availability
                // This follows the same pattern as ReplicaManager.fetchIndexFromCache
                Tuple2<Integer, Optional<Map<TableBucket, IndexSegment>>> retchResult =
                        dataReplica.fetchIndex(
                                indexBucketFetchRequests,
                                params.minAdvancedOffset(),
                                params.maxBytes() - fetchedBytes.get(),
                                false);
                fetchedBytes.addAndGet(retchResult.f0);
                Optional<Map<TableBucket, IndexSegment>> fetchResultForDataBucketOpt =
                        retchResult.f1;

                if (fetchResultForDataBucketOpt.isPresent()) {
                    Map<TableBucket, IndexSegment> fetchResults = fetchResultForDataBucketOpt.get();
                    for (TableBucket indexBucket : indexBucketFetchRequests.keySet()) {
                        IndexSegment segment = fetchResults.get(indexBucket);
                        checkNotNull(
                                segment,
                                "Index segment for index bucket " + indexBucket + " is null");
                        // Only add result if data is loaded
                        if (Objects.requireNonNull(segment.getStatus())
                                == IndexSegment.DataStatus.LOADED) {
                            indexBucketResults.put(
                                    indexBucket,
                                    new FetchIndexLogResultForBucket(
                                            segment.getRecords(),
                                            segment.getStartOffset(),
                                            segment.getEndOffset(),
                                            true));
                        }
                    }
                    if (indexBucketResults.size() != indexBucketFetchRequests.size()) {
                        LOG.warn(
                                "Some index buckets data are not loaded for data bucket {}",
                                dataBucket);
                        continue;
                    }
                    completeFetches.put(
                            dataBucket, new DataBucketIndexFetchResult(indexBucketResults));
                }
            } catch (Exception e) {
                LOG.error("Failed to get data replica for data bucket {}", dataBucket, e);
                // Return error result
                for (TableBucket indexBucket : indexBucketFetchRequests.keySet()) {
                    indexBucketResults.put(
                            indexBucket,
                            new FetchIndexLogResultForBucket(ApiError.fromThrowable(e)));
                }
                completeFetches.put(dataBucket, new DataBucketIndexFetchResult(indexBucketResults));
            }
        }

        return checkCompletionConditions();
    }

    private boolean checkCompletionConditions() {
        if (completeFetches.size() == dataBucketRequests.size()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "All data bucket requests are completed, satisfy delayFetchIndex immediately.");
            }
            responseCallback.accept(completeFetches);
            return true;
        }

        if (fetchedBytes.get() >= params.maxBytes()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Accumulated bytes {} exceeds the maximum bytes {}, satisfy delayFetchIndex immediately.",
                        fetchedBytes,
                        params.maxBytes());
            }
            return forceComplete();
        }
        return false;
    }

    @Override
    public void onExpiration() {
        // Index fetch requests only come from index table bucket leaders (followers)
        serverMetricGroup.delayedIndexFetchFromFollowerExpireCount().inc();
    }

    @Override
    public String toString() {
        return "DelayedFetchIndex{"
                + "params="
                + params
                + ", numDataBuckets="
                + dataBucketRequests.size()
                + '}';
    }
}
