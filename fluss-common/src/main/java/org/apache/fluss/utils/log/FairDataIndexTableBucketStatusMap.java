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

package org.apache.fluss.utils.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.DataIndexTableBucket;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is a useful building block for doing index fetch requests where data-index table
 * bucket pairs have to be rotated via round-robin to ensure fairness and some level of determinism
 * given the existence of a limit on the fetch response size. Because the serialization of fetch
 * requests is more efficient if all buckets for the same data table are grouped together, we do
 * such grouping in the method `set`.
 *
 * <p>As data-index buckets are moved to the end, the same data table may be repeated more than
 * once. In the optimal case, a single data table would "wrap around" and appear twice. However, as
 * buckets are fetched in different orders and bucket leadership changes, we will deviate from the
 * optimal. If this turns out to be an issue in practice, we can improve it by tracking the buckets
 * per node or calling `set` every so often.
 *
 * <p>Note that this class is not thread-safe except {@link #size()} which returns the number of
 * data-index buckets currently tracked.
 */
@NotThreadSafe
@Internal
public final class FairDataIndexTableBucketStatusMap<S> {

    private final Map<DataIndexTableBucket, S> map = new LinkedHashMap<>();

    /**
     * The number of data-index buckets that are currently assigned available in a thread safe
     * manner.
     */
    private volatile int size = 0;

    public FairDataIndexTableBucketStatusMap() {}

    public void moveToEnd(DataIndexTableBucket dataIndexBucket) {
        S status = map.remove(dataIndexBucket);
        if (status != null) {
            map.put(dataIndexBucket, status);
        }
    }

    public void updateAndMoveToEnd(DataIndexTableBucket dataIndexBucket, S status) {
        map.remove(dataIndexBucket);
        map.put(dataIndexBucket, status);
        updateSize();
    }

    public void update(DataIndexTableBucket dataIndexBucket, S status) {
        map.put(dataIndexBucket, status);
        updateSize();
    }

    public void remove(DataIndexTableBucket dataIndexBucket) {
        map.remove(dataIndexBucket);
        updateSize();
    }

    public void clear() {
        map.clear();
        updateSize();
    }

    public void removeIf(Predicate<DataIndexTableBucket> predicate) {
        map.keySet().removeIf(predicate);
        updateSize();
    }

    public boolean contains(DataIndexTableBucket dataIndexBucket) {
        return map.containsKey(dataIndexBucket);
    }

    public Map<DataIndexTableBucket, S> bucketStatusMap() {
        return Collections.unmodifiableMap(map);
    }

    /** Returns the bucket status values in order. */
    public List<S> bucketStatusValues() {
        return new ArrayList<>(map.values());
    }

    public S statusValue(DataIndexTableBucket dataIndexBucket) {
        return map.get(dataIndexBucket);
    }

    public void forEach(BiConsumer<DataIndexTableBucket, S> biConsumer) {
        map.forEach(biConsumer);
    }

    /**
     * Get the number of data-index buckets that are currently being tracked. This is thread-safe.
     */
    public int size() {
        return size;
    }

    /**
     * Update the builder to have the received map as its status (i.e. the previous status is
     * cleared). The builder will "batch by data table", then "batch by index table", so if we have
     * data tables a, b and c, each with index tables idx1, idx2, and each index table has two
     * data-index buckets, we may end up with something like the following (the order is dependent
     * on the iteration order of the received map): a_idx1_0, a_idx1_1, a_idx2_0, a_idx2_1,
     * b_idx1_0, b_idx1_1, b_idx2_0, b_idx2_1, c_idx1_0, c_idx1_1, c_idx2_0, c_idx2_1.
     */
    public void set(Map<DataIndexTableBucket, S> bucketToStatus) {
        map.clear();
        update(bucketToStatus);
        updateSize();
    }

    private void updateSize() {
        size = map.size();
    }

    private void update(Map<DataIndexTableBucket, S> bucketToStatus) {
        // Two-level grouping: first by data table ID, then by index table ID
        LinkedHashMap<Long, LinkedHashMap<Long, List<DataIndexTableBucket>>>
                dataTableToIndexTableToBuckets = new LinkedHashMap<>();

        // First pass: group by data table ID, then by index table ID
        for (DataIndexTableBucket dataIndexBucket : bucketToStatus.keySet()) {
            Long dataTableId = dataIndexBucket.getDataBucket().getTableId();
            Long indexTableId = dataIndexBucket.getIndexBucket().getTableId();

            LinkedHashMap<Long, List<DataIndexTableBucket>> indexTableToBuckets =
                    dataTableToIndexTableToBuckets.computeIfAbsent(
                            dataTableId, k -> new LinkedHashMap<>());
            List<DataIndexTableBucket> buckets =
                    indexTableToBuckets.computeIfAbsent(indexTableId, k -> new ArrayList<>());
            buckets.add(dataIndexBucket);
        }

        // Second pass: iterate in the grouped order to maintain batch-by-table structure
        for (Map.Entry<Long, LinkedHashMap<Long, List<DataIndexTableBucket>>> dataTableEntry :
                dataTableToIndexTableToBuckets.entrySet()) {
            for (Map.Entry<Long, List<DataIndexTableBucket>> indexTableEntry :
                    dataTableEntry.getValue().entrySet()) {
                for (DataIndexTableBucket dataIndexBucket : indexTableEntry.getValue()) {
                    S status = bucketToStatus.get(dataIndexBucket);
                    map.put(dataIndexBucket, status);
                }
            }
        }
    }
}
