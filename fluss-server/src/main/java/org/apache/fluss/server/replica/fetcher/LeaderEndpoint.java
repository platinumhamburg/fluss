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
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchIndexResponse;
import org.apache.fluss.rpc.messages.FetchLogResponse;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Defines the interface to be used to access a tablet server that is a leader. */
public interface LeaderEndpoint {

    /** The tablet server id we want to connect to. */
    int leaderServerId();

    /** Fetches the local log end offset of the given table bucket. */
    CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket);

    /** Fetches the local log start offset of the given table bucket. */
    CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket);

    CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket);

    /**
     * Given a fetchLogRequest, carries out the expected request and returns the results from
     * fetching from the leader.
     *
     * @param fetchLogContext The fetch log context we want to carry out.
     * @return fetchData.
     */
    CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext);

    /**
     * Given a fetchIndexContext, carries out the expected request and returns the results from
     * fetching from the leader.
     *
     * @param fetchIndexContext The fetch index context we want to carry out.
     * @return fetchIndexData.
     */
    CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext);

    /**
     * Builds a fetch request, given a bucket map.
     *
     * @param replicas A map of table replicas to their respective bucket fetch state.
     * @return fetchLogContext.
     */
    Optional<FetchLogContext> buildFetchLogContext(Map<TableBucket, BucketFetchStatus> replicas);

    /** Closes access to fetch from leader. */
    void close();

    /** Fetch data returned by fetchLog method. */
    final class FetchData {
        private final FetchLogResponse fetchLogResponse;
        private final Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap;

        public FetchData(
                FetchLogResponse fetchLogResponse,
                Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap) {
            this.fetchLogResponse = fetchLogResponse;
            this.fetchLogResultMap = fetchLogResultMap;
        }

        public FetchLogResponse getFetchLogResponse() {
            return fetchLogResponse;
        }

        public Map<TableBucket, FetchLogResultForBucket> getFetchLogResultMap() {
            return fetchLogResultMap;
        }
    }

    /** Fetch data returned by fetchIndex method. */
    final class FetchIndexData {
        private final FetchIndexResponse fetchIndexResponse;
        private final Map<DataIndexTableBucket, FetchIndexLogResultForBucket>
                fetchIndexLogResultMap;

        public FetchIndexData(
                FetchIndexResponse fetchIndexResponse,
                Map<DataIndexTableBucket, FetchIndexLogResultForBucket> fetchIndexLogResultMap) {
            this.fetchIndexResponse = fetchIndexResponse;
            this.fetchIndexLogResultMap = fetchIndexLogResultMap;
        }

        public FetchIndexResponse getFetchIndexResponse() {
            return fetchIndexResponse;
        }

        public Map<DataIndexTableBucket, FetchIndexLogResultForBucket> getFetchIndexLogResultMap() {
            return fetchIndexLogResultMap;
        }
    }
}
