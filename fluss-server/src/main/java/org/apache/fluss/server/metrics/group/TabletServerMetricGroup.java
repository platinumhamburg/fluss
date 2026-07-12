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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";
    private static final int WINDOW_SIZE = 1024;

    private final Map<TablePath, TableMetricGroup> metricGroupByTable = new ConcurrentHashMap<>();

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

    // aggregated metrics
    private final Counter messagesIn;
    private final Counter bytesIn;
    private final Counter bytesOut;

    // aggregated log metrics
    private final Counter logFlushCount;
    private final Histogram logFlushLatencyHistogram;

    // aggregated kv metrics
    private final Counter kvFlushCount;
    private final Histogram kvFlushLatencyHistogram;
    private final Counter kvTruncateAsDuplicatedCount;
    private final Counter kvTruncateAsErrorCount;

    // aggregated replica metrics
    private final Counter isrShrinks;
    private final Counter isrExpands;
    private final Counter failedIsrUpdates;

    // aggregated index push metrics
    private final Counter indexPushRequests;
    private final Counter indexPushErrors;
    private final Histogram indexPushLatencyHistogram;
    private final Counter partitionTombstoneApplyDrops;
    private final Counter indexPushStaleV1Batches;
    private final Counter indexSourceRemoteReadBytes;
    private final Counter indexSourceRemoteReadFailures;
    private final Counter indexPushRecordTooLargeFailures;
    private final Counter indexPushTombstoneNoOpBatches;
    private final Counter indexWriterStateRecoveryCoverageFailures;

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

        messagesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
        bytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        bytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));

        // about flush
        logFlushCount = new SimpleCounter();
        meter(MetricNames.LOG_FLUSH_RATE, new MeterView(logFlushCount));
        logFlushLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.LOG_FLUSH_LATENCY_MS, logFlushLatencyHistogram);

        // about pre-write buffer.
        kvFlushCount = new SimpleCounter();
        meter(MetricNames.KV_FLUSH_RATE, new MeterView(kvFlushCount));
        kvFlushLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.KV_FLUSH_LATENCY_MS, kvFlushLatencyHistogram);
        kvTruncateAsDuplicatedCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_DUPLICATED_RATE,
                new MeterView(kvTruncateAsDuplicatedCount));
        kvTruncateAsErrorCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_ERROR_RATE,
                new MeterView(kvTruncateAsErrorCount));

        // replica metrics
        isrExpands = new SimpleCounter();
        meter(MetricNames.ISR_EXPANDS_RATE, new MeterView(isrExpands));
        isrShrinks = new SimpleCounter();
        meter(MetricNames.ISR_SHRINKS_RATE, new MeterView(isrShrinks));
        failedIsrUpdates = new SimpleCounter();
        meter(MetricNames.FAILED_ISR_UPDATES_RATE, new MeterView(failedIsrUpdates));

        // index push metrics
        indexPushRequests = new ThreadSafeSimpleCounter();
        meter(MetricNames.INDEX_PUSH_REQUESTS_RATE, new MeterView(indexPushRequests));
        indexPushErrors = new ThreadSafeSimpleCounter();
        meter(MetricNames.INDEX_PUSH_ERRORS_RATE, new MeterView(indexPushErrors));
        indexPushLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.INDEX_PUSH_LATENCY_MS, indexPushLatencyHistogram);
        partitionTombstoneApplyDrops = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.PARTITION_TOMBSTONE_APPLY_DROPS_RATE,
                new MeterView(partitionTombstoneApplyDrops));
        indexPushStaleV1Batches = new ThreadSafeSimpleCounter();
        meter(MetricNames.INDEX_PUSH_STALE_V1_BATCHES_RATE, new MeterView(indexPushStaleV1Batches));
        indexSourceRemoteReadBytes = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_SOURCE_REMOTE_READ_BYTES_RATE,
                new MeterView(indexSourceRemoteReadBytes));
        indexSourceRemoteReadFailures = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_SOURCE_REMOTE_READ_FAILURES_RATE,
                new MeterView(indexSourceRemoteReadFailures));
        indexPushRecordTooLargeFailures = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_PUSH_RECORD_TOO_LARGE_FAILURES_RATE,
                new MeterView(indexPushRecordTooLargeFailures));
        indexPushTombstoneNoOpBatches = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_PUSH_TOMBSTONE_NO_OP_BATCHES_RATE,
                new MeterView(indexPushTombstoneNoOpBatches));
        indexWriterStateRecoveryCoverageFailures = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_WRITER_STATE_RECOVERY_COVERAGE_FAILURES_RATE,
                new MeterView(indexWriterStateRecoveryCoverageFailures));

        // Register server-level RocksDB aggregated metrics
        registerServerRocksDBMetrics();
    }

    /**
     * Register server-level RocksDB aggregated metrics. These metrics aggregate memory usage from
     * all tables.
     */
    private void registerServerRocksDBMetrics() {
        // Total memory usage across all RocksDB instances in this server.
        gauge(
                MetricNames.ROCKSDB_MEMORY_USAGE_TOTAL,
                () ->
                        metricGroupByTable.values().stream()
                                .flatMap(TableMetricGroup::allRocksDBStatistics)
                                .mapToLong(RocksDBStatistics::getTotalMemoryUsage)
                                .sum());
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

    public Counter messageIn() {
        return messagesIn;
    }

    public Counter bytesIn() {
        return bytesIn;
    }

    public Counter bytesOut() {
        return bytesOut;
    }

    public Counter logFlushCount() {
        return logFlushCount;
    }

    public Histogram logFlushLatencyHistogram() {
        return logFlushLatencyHistogram;
    }

    public Counter kvFlushCount() {
        return kvFlushCount;
    }

    public Histogram kvFlushLatencyHistogram() {
        return kvFlushLatencyHistogram;
    }

    public Counter kvTruncateAsDuplicatedCount() {
        return kvTruncateAsDuplicatedCount;
    }

    public Counter kvTruncateAsErrorCount() {
        return kvTruncateAsErrorCount;
    }

    public Counter isrShrinks() {
        return isrShrinks;
    }

    public Counter isrExpands() {
        return isrExpands;
    }

    public Counter failedIsrUpdates() {
        return failedIsrUpdates;
    }

    public Counter indexPushRequests() {
        return indexPushRequests;
    }

    public Counter indexPushErrors() {
        return indexPushErrors;
    }

    public Histogram indexPushLatencyHistogram() {
        return indexPushLatencyHistogram;
    }

    public Counter partitionTombstoneApplyDrops() {
        return partitionTombstoneApplyDrops;
    }

    public Counter indexPushStaleV1Batches() {
        return indexPushStaleV1Batches;
    }

    public Counter indexSourceRemoteReadBytes() {
        return indexSourceRemoteReadBytes;
    }

    public Counter indexSourceRemoteReadFailures() {
        return indexSourceRemoteReadFailures;
    }

    public Counter indexPushRecordTooLargeFailures() {
        return indexPushRecordTooLargeFailures;
    }

    public Counter indexPushTombstoneNoOpBatches() {
        return indexPushTombstoneNoOpBatches;
    }

    public Counter indexWriterStateRecoveryCoverageFailures() {
        return indexWriterStateRecoveryCoverageFailures;
    }

    public void registerIndexPushGauges(
            LongSupplier pendingBytesSupplier,
            IntSupplier inFlightRequestsSupplier,
            LongSupplier oldestInFlightAgeMsSupplier) {
        gauge(MetricNames.INDEX_PUSH_PENDING_BYTES, pendingBytesSupplier::getAsLong);
        gauge(MetricNames.INDEX_PUSH_IN_FLIGHT_REQUESTS, inFlightRequestsSupplier::getAsInt);
        gauge(
                MetricNames.INDEX_PUSH_OLDEST_IN_FLIGHT_AGE_MS,
                oldestInFlightAgeMsSupplier::getAsLong);
    }

    public void registerIndexWriterStateGauges(
            LongSupplier entryCountSupplier, LongSupplier snapshotBytesSupplier) {
        gauge(MetricNames.INDEX_WRITER_STATE_ENTRIES, entryCountSupplier::getAsLong);
        gauge(MetricNames.INDEX_WRITER_STATE_SNAPSHOT_BYTES, snapshotBytesSupplier::getAsLong);
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addTableBucketMetricGroup(
            PhysicalTablePath physicalTablePath, TableBucket bucket, boolean isKvTable) {
        TablePath tablePath = physicalTablePath.getTablePath();
        TableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath,
                        table -> new TableMetricGroup(registry, tablePath, isKvTable, this));
        return tableMetricGroup.addBucketMetricGroup(physicalTablePath.getPartitionName(), bucket);
    }

    public void removeTableBucketMetricGroup(TablePath tablePath, TableBucket bucket) {
        // get the metric group of the table
        TableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        // if get the table metric group
        if (tableMetricGroup != null) {
            // remove the bucket metric group
            tableMetricGroup.removeBucketMetricGroup(bucket);
            // if no any bucket groups remain in the physical table metrics group,
            // close and remove the physical table metric group
            if (tableMetricGroup.bucketGroupsCount() == 0) {
                tableMetricGroup.close();
                metricGroupByTable.remove(tablePath);
            }
        }
    }
}
