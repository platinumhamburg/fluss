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
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.server.entity.DataBucketIndexFetchResult;
import org.apache.fluss.server.entity.FetchIndexReqInfo;
import org.apache.fluss.server.index.FetchIndexParams;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.ReplicaTestBase;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.fluss.record.TestData.IDX_EMAIL_TABLE_ID;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_ID;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_ID;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelayedFetchIndex}. */
public class DelayedFetchIndexTest extends ReplicaTestBase {

    @Test
    void testCompleteDelayedFetchIndex() throws Exception {
        // Setup data bucket and index buckets
        TableBucket dataBucket = new TableBucket(INDEXED_TABLE_ID, 0);
        TableBucket idxNameBucket = new TableBucket(IDX_NAME_TABLE_ID, 0);
        TableBucket idxEmailBucket = new TableBucket(IDX_EMAIL_TABLE_ID, 0);

        // Make data bucket as leader
        makeIndexedTableAsLeader(dataBucket.getBucket());

        // Setup fetch request info for index buckets
        Map<TableBucket, FetchIndexReqInfo> indexBucketFetchRequests = new HashMap<>();
        indexBucketFetchRequests.put(idxNameBucket, new FetchIndexReqInfo(0L, 0L));
        indexBucketFetchRequests.put(idxEmailBucket, new FetchIndexReqInfo(0L, 0L));

        Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> dataBucketRequests = new HashMap<>();
        dataBucketRequests.put(dataBucket, indexBucketFetchRequests);

        // Create delayed response future
        CompletableFuture<Map<TableBucket, DataBucketIndexFetchResult>> delayedResponse =
                new CompletableFuture<>();

        // Create DelayedFetchIndex
        DelayedFetchIndex delayedFetchIndex =
                createDelayedFetchIndexRequest(
                        10, // maxFetchRecords
                        Duration.ofMinutes(3).toMillis(), // max wait ms large enough
                        dataBucketRequests,
                        new HashMap<>(), // empty complete fetches initially
                        delayedResponse::complete);

        DelayedOperationManager<DelayedFetchIndex> delayedFetchIndexManager =
                replicaManager.getDelayedFetchIndexManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(dataBucket);

        // Try to complete else watch - should not complete immediately since we have no index data
        // yet
        boolean completed =
                delayedFetchIndexManager.tryCompleteElseWatch(
                        delayedFetchIndex, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();
        assertThat(delayedFetchIndexManager.numDelayed()).isEqualTo(1);
        assertThat(delayedFetchIndexManager.watched()).isEqualTo(1);

        // Check and complete manually - should not complete yet
        int numComplete = delayedFetchIndexManager.checkAndComplete(delayedTableBucketKey);
        assertThat(numComplete).isEqualTo(0);
        assertThat(delayedFetchIndexManager.numDelayed()).isEqualTo(1);
        assertThat(delayedFetchIndexManager.watched()).isEqualTo(1);

        // Response should not be done yet
        assertThat(delayedResponse.isDone()).isFalse();
    }

    @Test
    void testDelayFetchIndexTimeout() {
        // Setup data bucket and index buckets
        TableBucket dataBucket = new TableBucket(INDEXED_TABLE_ID, 0);
        TableBucket idxNameBucket = new TableBucket(IDX_NAME_TABLE_ID, 0);

        // Make data bucket as leader
        makeIndexedTableAsLeader(dataBucket.getBucket());

        // Setup fetch request info for index buckets
        Map<TableBucket, FetchIndexReqInfo> indexBucketFetchRequests = new HashMap<>();
        indexBucketFetchRequests.put(idxNameBucket, new FetchIndexReqInfo(0L, 0L));

        Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> dataBucketRequests = new HashMap<>();
        dataBucketRequests.put(dataBucket, indexBucketFetchRequests);

        // Create delayed response future
        CompletableFuture<Map<TableBucket, DataBucketIndexFetchResult>> delayedResponse =
                new CompletableFuture<>();

        // Create DelayedFetchIndex with short timeout
        DelayedFetchIndex delayedFetchIndex =
                createDelayedFetchIndexRequest(
                        10, // maxFetchRecords
                        1000, // wait time is small enough
                        dataBucketRequests,
                        new HashMap<>(), // empty complete fetches initially
                        delayedResponse::complete);

        DelayedOperationManager<DelayedFetchIndex> delayedFetchIndexManager =
                replicaManager.getDelayedFetchIndexManager();
        DelayedTableBucketKey delayedTableBucketKey = new DelayedTableBucketKey(dataBucket);

        // Try to complete else watch - should not complete immediately
        boolean completed =
                delayedFetchIndexManager.tryCompleteElseWatch(
                        delayedFetchIndex, Collections.singletonList(delayedTableBucketKey));
        assertThat(completed).isFalse();

        // Wait for timeout and verify completion
        retry(
                Duration.ofMinutes(1),
                () -> {
                    delayedFetchIndexManager.checkAndComplete(delayedTableBucketKey);
                    assertThat(delayedFetchIndexManager.numDelayed()).isEqualTo(0);
                    assertThat(delayedFetchIndexManager.watched()).isEqualTo(0);

                    assertThat(delayedResponse.isDone()).isTrue();
                    Map<TableBucket, DataBucketIndexFetchResult> result = delayedResponse.get();
                    assertThat(result).containsKey(dataBucket);

                    DataBucketIndexFetchResult fetchResult = result.get(dataBucket);
                    assertThat(fetchResult).isNotNull();
                    assertThat(fetchResult.getIndexBucketResults()).hasSize(1);
                    assertThat(fetchResult.getIndexBucketResults()).containsKey(idxNameBucket);

                    // Verify that the result contains empty records (due to timeout)
                    FetchIndexLogResultForBucket indexResult =
                            fetchResult.getIndexBucketResult(idxNameBucket);
                    assertThat(indexResult).isNotNull();
                    assertThat(indexResult.recordsOrEmpty()).isEqualTo(MemoryLogRecords.EMPTY);
                });
    }

    private DelayedFetchIndex createDelayedFetchIndexRequest(
            int maxFetchRecords,
            long maxWaitMs,
            Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> dataBucketRequests,
            Map<TableBucket, DataBucketIndexFetchResult> completeFetches,
            Consumer<Map<TableBucket, DataBucketIndexFetchResult>> responseCallback) {
        FetchIndexParams fetchIndexParams = new FetchIndexParams(maxFetchRecords, maxWaitMs);
        return new DelayedFetchIndex(
                fetchIndexParams,
                replicaManager,
                dataBucketRequests,
                completeFetches,
                responseCallback,
                TestingMetricGroups.TABLET_SERVER_METRICS);
    }
}
