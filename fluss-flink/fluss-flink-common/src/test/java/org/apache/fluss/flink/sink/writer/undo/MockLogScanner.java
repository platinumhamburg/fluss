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

package org.apache.fluss.flink.sink.writer.undo;

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
 * Mock implementation of {@link LogScanner} for testing.
 *
 * <p>Allows pre-configuring records per bucket and returns them on poll().
 */
class MockLogScanner implements LogScanner {
    private final Map<TableBucket, List<ScanRecord>> recordsByBucket = new HashMap<>();
    private final Map<TableBucket, AtomicInteger> pollIndexByBucket = new HashMap<>();

    void setRecordsForBucket(TableBucket bucket, List<ScanRecord> records) {
        recordsByBucket.put(bucket, new ArrayList<>(records));
        pollIndexByBucket.put(bucket, new AtomicInteger(0));
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        Map<TableBucket, List<ScanRecord>> result = new HashMap<>();

        for (Map.Entry<TableBucket, List<ScanRecord>> entry : recordsByBucket.entrySet()) {
            TableBucket bucket = entry.getKey();
            List<ScanRecord> allRecords = entry.getValue();
            AtomicInteger index = pollIndexByBucket.get(bucket);

            if (index.get() < allRecords.size()) {
                List<ScanRecord> remaining = allRecords.subList(index.get(), allRecords.size());
                result.put(bucket, new ArrayList<>(remaining));
                index.set(allRecords.size());
            }
        }

        return result.isEmpty() ? ScanRecords.EMPTY : new ScanRecords(result);
    }

    @Override
    public void subscribe(int bucket, long offset) {}

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {}

    @Override
    public void unsubscribe(long partitionId, int bucket) {}

    @Override
    public void wakeup() {}

    @Override
    public void close() {}

    void reset() {
        recordsByBucket.clear();
        pollIndexByBucket.clear();
    }
}
