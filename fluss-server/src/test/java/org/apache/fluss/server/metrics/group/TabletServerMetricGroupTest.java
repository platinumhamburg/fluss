/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class TabletServerMetricGroupTest {

    @Test
    void testIndexReplicationGaugeOwnershipCanBeReplacedAndCleared() {
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "gauge-owner", "rack", "host", 1);
        AtomicLong firstPending = new AtomicLong(33L);
        AtomicLong firstNoProgress = new AtomicLong(304L);
        AtomicLong firstFailedSources = new AtomicLong(30L);
        AtomicLong secondPending = new AtomicLong(44L);
        AtomicLong secondNoProgress = new AtomicLong(405L);
        AtomicLong secondFailedSources = new AtomicLong(40L);

        TabletServerMetricGroup.GaugeRegistration first =
                metrics.registerIndexReplicationGauges(
                        firstPending::get,
                        firstNoProgress::get,
                        firstFailedSources::get);
        assertReplicationGauges(metrics, 33L, 304L, 30L);

        TabletServerMetricGroup.GaugeRegistration second =
                metrics.registerIndexReplicationGauges(
                        secondPending::get,
                        secondNoProgress::get,
                        secondFailedSources::get);
        assertReplicationGauges(metrics, 44L, 405L, 40L);
        assertThat(metrics.getMetrics().keySet())
                .filteredOn(MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT::equals)
                .hasSize(1);

        first.close();
        assertReplicationGauges(metrics, 44L, 405L, 40L);

        second.close();
        assertReplicationGauges(metrics, 0L, 0L, 0L);
    }

    private static void assertReplicationGauges(
            TabletServerMetricGroup metrics,
            long expectedPending,
            long expectedNoProgress,
            long expectedFailedSources) {
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_PENDING_BYTES))
                .isEqualTo(expectedPending);
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_MAX_NO_PROGRESS_TIME_MS))
                .isEqualTo(expectedNoProgress);
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT))
                .isEqualTo(expectedFailedSources);
    }

    private static long metricValue(TabletServerMetricGroup metrics, String name) {
        Gauge<?> gauge = (Gauge<?>) metrics.getMetrics().get(name);
        assertThat(gauge).as("registered gauge %s", name).isNotNull();
        return ((Number) gauge.getValue()).longValue();
    }
}
