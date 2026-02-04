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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.entity.FetchIndexStatus;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.DataBucketIndexFetchResult;
import org.apache.fluss.server.entity.FetchIndexReqInfo;
import org.apache.fluss.server.index.FetchIndexParams;
import org.apache.fluss.server.index.IndexSegment;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Sets;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A delayed fetch index operation that can be created by the {@link ReplicaManager} and watched in
 * the delayed fetch index operation manager.
 *
 * <p>This operation delays index fetch requests when there is insufficient index data available or
 * when index data is being built. It follows the same pattern as {@link DelayedFetchLog}.
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

    @Override
    public boolean tryComplete() {
        if (isComplete()) {
            return true;
        }

        // Try to fetch data for remaining buckets
        fetchRemainingBuckets(false);

        return checkAndComplete();
    }

    @Override
    public void onComplete() {
        // Fetch any remaining data buckets with force mode
        fetchRemainingBuckets(true);

        // Mark remaining unfetched buckets as NO_DATA_AVAILABLE
        getRemainingBuckets()
                .forEach(
                        dataBucket ->
                                completeFetches.put(
                                        dataBucket,
                                        DataBucketIndexFetchResult.withStatus(
                                                dataBucketRequests.get(dataBucket).keySet(),
                                                FetchIndexStatus.NO_DATA_AVAILABLE)));

        responseCallback.accept(completeFetches);
    }

    @Override
    public void onExpiration() {
        serverMetricGroup.delayedIndexFetchFromFollowerExpireCount().inc();

        Set<TableBucket> remaining = getRemainingBuckets();
        if (!remaining.isEmpty()) {
            LOG.warn(
                    "Delayed fetch index timed out. Pending: {}, completed: {}, bytes: {}/{}",
                    remaining,
                    completeFetches.keySet(),
                    fetchedBytes.get(),
                    params.maxBytes());
        }
    }

    /**
     * Fetch index data for remaining data buckets.
     *
     * @param forceIncludeNotReady if true, include data even if not fully loaded (for onComplete)
     */
    private void fetchRemainingBuckets(boolean forceIncludeNotReady) {
        for (TableBucket dataBucket : getRemainingBuckets()) {
            if (fetchedBytes.get() >= params.maxBytes()) {
                break;
            }
            fetchDataBucket(dataBucket, forceIncludeNotReady);
        }
    }

    /**
     * Fetch index data for a single data bucket.
     *
     * @param dataBucket the data bucket to fetch from
     * @param forceIncludeNotReady if true, include data even if not fully loaded
     */
    private void fetchDataBucket(TableBucket dataBucket, boolean forceIncludeNotReady) {
        Map<TableBucket, FetchIndexReqInfo> indexRequests = dataBucketRequests.get(dataBucket);

        try {
            Replica dataReplica = replicaManager.getReplicaOrException(dataBucket);
            if (!forceIncludeNotReady && !dataReplica.isLeader()) {
                return; // Skip non-leader in tryComplete
            }

            Tuple2<Integer, Optional<Map<TableBucket, IndexSegment>>> result =
                    dataReplica.fetchIndex(
                            indexRequests,
                            params.minAdvancedOffset(),
                            params.maxBytes() - fetchedBytes.get(),
                            forceIncludeNotReady);

            fetchedBytes.addAndGet(result.f0);

            if (!result.f1.isPresent()) {
                if (forceIncludeNotReady) {
                    // IndexCache closed - return error
                    completeFetches.put(
                            dataBucket,
                            createErrorResult(
                                    indexRequests.keySet(),
                                    new IllegalStateException(
                                            "IndexCache unavailable for " + dataBucket)));
                }
                return;
            }

            Map<TableBucket, IndexSegment> segments = result.f1.get();
            Map<TableBucket, FetchIndexLogResultForBucket> bucketResults =
                    buildBucketResults(indexRequests.keySet(), segments, forceIncludeNotReady);

            // Only add to completeFetches if all buckets have results (or force mode)
            if (bucketResults.size() == indexRequests.size()) {
                completeFetches.put(dataBucket, new DataBucketIndexFetchResult(bucketResults));
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch index for data bucket {}", dataBucket, e);
            completeFetches.put(dataBucket, createErrorResult(indexRequests.keySet(), e));
        }
    }

    /**
     * Build fetch results from index segments.
     *
     * @param indexBuckets the index buckets to build results for
     * @param segments the fetched segments
     * @param includeNotReady if true, include segments that are not fully loaded
     * @return map of bucket to fetch result (may be smaller than indexBuckets if not ready)
     */
    private Map<TableBucket, FetchIndexLogResultForBucket> buildBucketResults(
            Set<TableBucket> indexBuckets,
            Map<TableBucket, IndexSegment> segments,
            boolean includeNotReady) {
        Map<TableBucket, FetchIndexLogResultForBucket> results = new HashMap<>();

        for (TableBucket indexBucket : indexBuckets) {
            IndexSegment segment = segments.get(indexBucket);
            if (segment == null) {
                continue;
            }

            boolean isReady = segment.getStatus() == IndexSegment.DataStatus.LOADED;
            if (isReady || includeNotReady) {
                results.put(
                        indexBucket,
                        new FetchIndexLogResultForBucket(
                                segment.getRecords(),
                                segment.getStartOffset(),
                                segment.getEndOffset(),
                                isReady));
            }
        }
        return results;
    }

    /** Create an error result for all index buckets. */
    private DataBucketIndexFetchResult createErrorResult(
            Set<TableBucket> indexBuckets, Exception e) {
        Map<TableBucket, FetchIndexLogResultForBucket> results = new HashMap<>();
        ApiError error = ApiError.fromThrowable(e);
        for (TableBucket indexBucket : indexBuckets) {
            results.put(indexBucket, new FetchIndexLogResultForBucket(error));
        }
        return new DataBucketIndexFetchResult(results);
    }

    /** Get the set of data buckets that haven't been fetched yet. */
    private Set<TableBucket> getRemainingBuckets() {
        return Sets.difference(dataBucketRequests.keySet(), completeFetches.keySet());
    }

    /** Check if all requests are complete. */
    private boolean isComplete() {
        return completeFetches.size() == dataBucketRequests.size();
    }

    /** Check completion conditions and trigger completion if met. */
    private boolean checkAndComplete() {
        if (isComplete()) {
            responseCallback.accept(completeFetches);
            return true;
        }

        if (fetchedBytes.get() >= params.maxBytes()) {
            LOG.debug(
                    "Bytes limit reached ({}/{}), completing delayed fetch index",
                    fetchedBytes.get(),
                    params.maxBytes());
            return forceComplete();
        }
        return false;
    }

    @Override
    public String toString() {
        return "DelayedFetchIndex{params="
                + params
                + ", buckets="
                + dataBucketRequests.size()
                + '}';
    }
}
