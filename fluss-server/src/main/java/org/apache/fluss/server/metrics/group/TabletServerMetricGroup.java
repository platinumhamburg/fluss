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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.utils.MapUtils;

import java.util.Map;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";

    private final Map<PhysicalTablePath, PhysicalTableMetricGroup> metricGroupByPhysicalTable =
            MapUtils.newConcurrentHashMap();

    protected final String clusterId;
    protected final String rack;
    protected final String hostname;
    protected final int serverId;

    // ---- metrics ----
    private final Counter replicationBytesIn;
    private final Counter replicationBytesOut;
    private final Counter delayedWriteExpireCount;
    private final Counter delayedFetchFromFollowerExpireCount;
    private final Counter delayedFetchFromClientExpireCount;

    private final Counter logRecordBatchStatisticsProcessCount;
    private final Counter logRecordBatchStatisticsFilterOutCount;
    private final Counter processedRecordBatchCount;

    public TabletServerMetricGroup(
            MetricRegistry registry, String clusterId, String rack, String hostname, int serverId) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.rack = rack;
        this.hostname = hostname;
        this.serverId = serverId;

        replicationBytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_IN_RATE, new MeterView(replicationBytesIn));
        replicationBytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_OUT_RATE, new MeterView(replicationBytesOut));

        delayedWriteExpireCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.DELAYED_WRITE_EXPIRES_RATE, new MeterView(delayedWriteExpireCount));
        delayedFetchFromFollowerExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_FOLLOWER_EXPIRES_RATE,
                new MeterView(delayedFetchFromFollowerExpireCount));
        delayedFetchFromClientExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_CLIENT_EXPIRES_RATE,
                new MeterView(delayedFetchFromClientExpireCount));
        logRecordBatchStatisticsProcessCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.LOG_RECORD_BATCH_STATISTICS_PROCESS_COUNT,
                new MeterView(logRecordBatchStatisticsProcessCount));
        logRecordBatchStatisticsFilterOutCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.LOG_RECORD_BATCH_STATISTICS_FILTER_OUT_COUNT,
                new MeterView(logRecordBatchStatisticsFilterOutCount));
        processedRecordBatchCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.LOG_RECORD_BATCH_PROCESSED_COUNT,
                new MeterView(processedRecordBatchCount));
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        if (rack != null) {
            variables.put("rack", rack);
        } else {
            // The value of an empty string indicates no rack
            variables.put("rack", "");
        }
        variables.put("host", hostname);
        variables.put("server_id", String.valueOf(serverId));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    public Counter replicationBytesIn() {
        return replicationBytesIn;
    }

    public Counter replicationBytesOut() {
        return replicationBytesOut;
    }

    public Counter delayedWriteExpireCount() {
        return delayedWriteExpireCount;
    }

    public Counter delayedFetchFromFollowerExpireCount() {
        return delayedFetchFromFollowerExpireCount;
    }

    public Counter delayedFetchFromClientExpireCount() {
        return delayedFetchFromClientExpireCount;
    }

    public Counter logRecordBatchStatisticsProcessCount() {
        return logRecordBatchStatisticsProcessCount;
    }

    public Counter logRecordBatchStatisticsFilterOutCount() {
        return logRecordBatchStatisticsFilterOutCount;
    }

    public Counter processedRecordBatchCount() {
        return processedRecordBatchCount;
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addPhysicalTableBucketMetricGroup(
            PhysicalTablePath physicalTablePath, int bucket, boolean isKvTable) {
        PhysicalTableMetricGroup physicalTableMetricGroup =
                metricGroupByPhysicalTable.computeIfAbsent(
                        physicalTablePath,
                        table ->
                                new PhysicalTableMetricGroup(
                                        registry, physicalTablePath, isKvTable, this));
        return physicalTableMetricGroup.addBucketMetricGroup(bucket);
    }

    public void removeTableBucketMetricGroup(PhysicalTablePath physicalTablePath, int bucket) {
        // get the metric group of the physical table
        PhysicalTableMetricGroup physicalTableMetricGroup =
                metricGroupByPhysicalTable.get(physicalTablePath);
        // if get the physical table metric group
        if (physicalTableMetricGroup != null) {
            // remove the bucket metric group
            physicalTableMetricGroup.removeBucketMetricGroup(bucket);
            // if no any bucket groups remain in the physical table metrics group,
            // close and remove the physical table metric group
            if (physicalTableMetricGroup.bucketGroupsCount() == 0) {
                physicalTableMetricGroup.close();
                metricGroupByPhysicalTable.remove(physicalTablePath);
            }
        }
    }
}
