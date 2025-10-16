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

package org.apache.fluss.client.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.lookup.SecondaryIndexLookuper;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metrics for lookup operations, including {@link SecondaryIndexLookuper} and lookup queue
 * operations.
 */
@Internal
public class LookuperMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "lookuper";

    // Secondary index specific metrics
    private final Counter secondaryIndexRequestCount;
    private final Counter secondaryIndexErrorCount;

    // Latency metrics (volatile for thread safety)
    private volatile long secondaryIndexLookupLatencyMs = -1;
    private volatile int lookupQueueSize = 0;
    private volatile int secondaryIndexLookupPkMappingCount = 0;

    public LookuperMetricGroup(ClientMetricGroup parent) {
        super(parent.getMetricRegistry(), makeScope(parent, NAME), parent);

        // Secondary index specific counters and rates
        secondaryIndexRequestCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.LOOKUPER_SECONDARY_INDEX_REQUESTS_RATE,
                new MeterView(secondaryIndexRequestCount));

        secondaryIndexErrorCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.LOOKUPER_SECONDARY_INDEX_ERRORS_RATE,
                new MeterView(secondaryIndexErrorCount));

        // Latency and queue size gauges
        gauge(MetricNames.LOOKUPER_SECONDARY_INDEX_LATENCY_MS, () -> secondaryIndexLookupLatencyMs);
        gauge(MetricNames.LOOKUPER_QUEUE_SIZE, () -> lookupQueueSize);
        gauge(
                MetricNames.LOOKUPER_SECONDARY_INDEX_PK_MAPPING_COUNT,
                () -> secondaryIndexLookupPkMappingCount);
    }

    /** Update the current lookup queue size. */
    public void updateLookupQueueSize(int size) {
        this.lookupQueueSize = size;
    }

    /** Update the secondary index lookup latency in milliseconds. */
    public void updateSecondaryIndexLookupLatency(long latencyMs) {
        this.secondaryIndexLookupLatencyMs = latencyMs;
    }

    /** Update the secondary index lookup PK mapping count. */
    public void updateSecondaryIndexLookupPkMappingCount(int count) {
        this.secondaryIndexLookupPkMappingCount = count;
    }

    /** Get the secondary index request counter for external usage. */
    public Counter secondaryIndexRequestCount() {
        return secondaryIndexRequestCount;
    }

    /** Get the secondary index error counter for external usage. */
    public Counter secondaryIndexErrorCount() {
        return secondaryIndexErrorCount;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }
}
