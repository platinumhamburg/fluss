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

package org.apache.fluss.server.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The structure of index fetch result information for all index buckets associated with a data
 * bucket. This manages a collection of FetchIndexLogResultForBucket results grouped by data table
 * bucket.
 */
@Internal
public final class DataBucketIndexFetchResult {
    // Map from index bucket to its fetch result
    private final Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults;

    public DataBucketIndexFetchResult(
            Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults) {
        this.indexBucketResults = new HashMap<>(indexBucketResults);
    }

    public Map<TableBucket, FetchIndexLogResultForBucket> getIndexBucketResults() {
        return indexBucketResults;
    }

    public FetchIndexLogResultForBucket getIndexBucketResult(TableBucket indexBucket) {
        return indexBucketResults.get(indexBucket);
    }

    /**
     * Check if any index bucket result failed.
     *
     * @return true if any result failed
     */
    public boolean hasFailures() {
        return indexBucketResults.values().stream()
                .anyMatch(FetchIndexLogResultForBucket::hasError);
    }

    public static DataBucketIndexFetchResult errorResult(
            Set<TableBucket> indexBucketsToFetch, ApiError error) {
        Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults = new HashMap<>();
        for (TableBucket indexBucket : indexBucketsToFetch) {
            indexBucketResults.put(indexBucket, new FetchIndexLogResultForBucket(error));
        }
        return new DataBucketIndexFetchResult(indexBucketResults);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataBucketIndexFetchResult that = (DataBucketIndexFetchResult) o;
        return Objects.equals(indexBucketResults, that.indexBucketResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexBucketResults);
    }

    @Override
    public String toString() {
        return "DataBucketIndexFetchResult{" + "indexBucketResults=" + indexBucketResults + '}';
    }
}
