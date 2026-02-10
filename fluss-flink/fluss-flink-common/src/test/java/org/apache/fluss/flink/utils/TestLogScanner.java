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

package org.apache.fluss.flink.utils;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test implementation of {@link LogScanner} for testing.
 *
 * <p>Allows pre-configuring records per bucket and returns them on poll(). Supports:
 *
 * <ul>
 *   <li>Simulating empty polls for testing exponential backoff behavior
 *   <li>Batch size control for realistic multi-poll scenarios
 *   <li>Always-empty mode for testing fatal exception on max empty polls
 *   <li>Position tracking via subscribe/position/unsubscribe
 * </ul>
 */
public class TestLogScanner implements LogScanner {
    private final Map<TableBucket, List<ScanRecord>> recordsByBucket = new HashMap<>();
    private final Map<TableBucket, AtomicInteger> pollIndexByBucket = new HashMap<>();

    // Position tracking: keyed by bucket int for non-partitioned,
    // by "partitionId:bucket" string for partitioned
    private final Map<String, Long> positions = new HashMap<>();

    /** Number of empty polls to return before returning actual records. */
    private int emptyPollsBeforeData = 0;

    private int currentEmptyPollCount = 0;

    /** If true, always return empty records (for testing fatal exception on max empty polls). */
    private boolean alwaysReturnEmpty = false;

    /** Maximum number of records to return per bucket per poll (0 = unlimited). */
    private int batchSize = 0;

    public void setRecordsForBucket(TableBucket bucket, List<ScanRecord> records) {
        recordsByBucket.put(bucket, new ArrayList<>(records));
        pollIndexByBucket.put(bucket, new AtomicInteger(0));
    }

    /**
     * Sets the number of empty polls to return before returning actual records.
     *
     * @param count number of empty polls
     */
    public void setEmptyPollsBeforeData(int count) {
        this.emptyPollsBeforeData = count;
        this.currentEmptyPollCount = 0;
    }

    /**
     * Sets whether to always return empty records (for testing fatal exception).
     *
     * @param alwaysEmpty if true, poll() always returns empty
     */
    public void setAlwaysReturnEmpty(boolean alwaysEmpty) {
        this.alwaysReturnEmpty = alwaysEmpty;
    }

    /**
     * Sets the maximum number of records to return per bucket per poll.
     *
     * @param batchSize max records per bucket per poll (0 = unlimited, returns all remaining)
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        if (alwaysReturnEmpty) {
            return ScanRecords.EMPTY;
        }

        if (currentEmptyPollCount < emptyPollsBeforeData) {
            currentEmptyPollCount++;
            return ScanRecords.EMPTY;
        }

        Map<TableBucket, List<ScanRecord>> result = new HashMap<>();

        for (Map.Entry<TableBucket, List<ScanRecord>> entry : recordsByBucket.entrySet()) {
            TableBucket bucket = entry.getKey();
            List<ScanRecord> allRecords = entry.getValue();
            AtomicInteger index = pollIndexByBucket.get(bucket);

            if (index.get() < allRecords.size()) {
                int startIndex = index.get();
                int endIndex;
                if (batchSize > 0) {
                    endIndex = Math.min(startIndex + batchSize, allRecords.size());
                } else {
                    endIndex = allRecords.size();
                }
                List<ScanRecord> batch = allRecords.subList(startIndex, endIndex);
                result.put(bucket, new ArrayList<>(batch));
                index.set(endIndex);
                // Advance scanner position to next offset after last returned record
                ScanRecord lastRecord = batch.get(batch.size() - 1);
                String key = positionKey(bucket);
                if (positions.containsKey(key)) {
                    positions.put(key, lastRecord.logOffset() + 1);
                }
            }
        }

        // For subscribed buckets with no records configured, advance position to simulate
        // the scanner consuming empty LogRecordBatches that produce zero ScanRecords.
        // Skip this when alwaysReturnEmpty is set (simulating network issues, not empty batches).
        if (!alwaysReturnEmpty) {
            for (Map.Entry<String, Long> entry : new HashMap<>(positions).entrySet()) {
                String key = entry.getKey();
                boolean hasRecords = false;
                for (TableBucket tb : recordsByBucket.keySet()) {
                    if (positionKey(tb).equals(key)) {
                        hasRecords = true;
                        break;
                    }
                }
                if (!hasRecords) {
                    positions.put(key, entry.getValue() + 1);
                }
            }
        }

        return result.isEmpty() ? ScanRecords.EMPTY : new ScanRecords(result);
    }

    @Override
    public void subscribe(int bucket, long offset) {
        positions.put(bucketKey(bucket), offset);
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        positions.put(partitionBucketKey(partitionId, bucket), offset);
    }

    @Override
    public Long position(TableBucket tableBucket) {
        return positions.get(positionKey(tableBucket));
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        positions.remove(partitionBucketKey(partitionId, bucket));
    }

    @Override
    public void unsubscribe(int bucket) {
        positions.remove(bucketKey(bucket));
    }

    @Override
    public void wakeup() {}

    @Override
    public void close() {}

    public void reset() {
        recordsByBucket.clear();
        pollIndexByBucket.clear();
        positions.clear();
        emptyPollsBeforeData = 0;
        currentEmptyPollCount = 0;
        alwaysReturnEmpty = false;
        batchSize = 0;
    }

    private static String bucketKey(int bucket) {
        return "b:" + bucket;
    }

    private static String partitionBucketKey(long partitionId, int bucket) {
        return "p:" + partitionId + ":b:" + bucket;
    }

    private static String positionKey(TableBucket tb) {
        if (tb.getPartitionId() != null) {
            return partitionBucketKey(tb.getPartitionId(), tb.getBucket());
        } else {
            return bucketKey(tb.getBucket());
        }
    }
}
