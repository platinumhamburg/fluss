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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class TabletServerMetricGroupTest {

    @Test
    void testWriterStateGaugeOwnershipCanBeReplacedAndCleared() {
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "gauge-owner", "rack", "host", 1);
        AtomicLong firstEntries = new AtomicLong(11L);
        AtomicLong firstBytes = new AtomicLong(101L);
        AtomicLong secondEntries = new AtomicLong(22L);
        AtomicLong secondBytes = new AtomicLong(202L);
        AtomicLong firstPending = new AtomicLong(33L);
        AtomicInteger firstInFlight = new AtomicInteger(3);
        AtomicLong firstAge = new AtomicLong(303L);
        AtomicLong firstFailedSources = new AtomicLong(30L);
        AtomicLong secondPending = new AtomicLong(44L);
        AtomicInteger secondInFlight = new AtomicInteger(4);
        AtomicLong secondAge = new AtomicLong(404L);
        AtomicLong secondFailedSources = new AtomicLong(40L);

        TabletServerMetricGroup.GaugeRegistration firstWriterState =
                metrics.registerIndexWriterStateGauges(firstEntries::get, firstBytes::get);
        TabletServerMetricGroup.GaugeRegistration firstPush =
                metrics.registerIndexPushGauges(
                        firstPending::get,
                        firstInFlight::get,
                        firstAge::get,
                        firstFailedSources::get);
        assertWriterStateGauges(metrics, 11L, 101L);
        assertPushGauges(metrics, 33L, 3L, 303L, 30L);

        TabletServerMetricGroup.GaugeRegistration secondWriterState =
                metrics.registerIndexWriterStateGauges(secondEntries::get, secondBytes::get);
        TabletServerMetricGroup.GaugeRegistration secondPush =
                metrics.registerIndexPushGauges(
                        secondPending::get,
                        secondInFlight::get,
                        secondAge::get,
                        secondFailedSources::get);
        assertWriterStateGauges(metrics, 22L, 202L);
        assertPushGauges(metrics, 44L, 4L, 404L, 40L);
        assertThat(metrics.getMetrics().keySet())
                .filteredOn("indexReplicationFailedSourceBucketCount"::equals)
                .hasSize(1);

        firstWriterState.close();
        firstPush.close();
        assertWriterStateGauges(metrics, 22L, 202L);
        assertPushGauges(metrics, 44L, 4L, 404L, 40L);

        secondWriterState.close();
        secondPush.close();
        assertWriterStateGauges(metrics, 0L, 0L);
        assertPushGauges(metrics, 0L, 0L, 0L, 0L);
    }

    private static void assertPushGauges(
            TabletServerMetricGroup metrics,
            long expectedPending,
            long expectedInFlight,
            long expectedAge,
            long expectedFailedSources) {
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_PENDING_BYTES))
                .isEqualTo(expectedPending);
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_IN_FLIGHT_REQUESTS))
                .isEqualTo(expectedInFlight);
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_OLDEST_IN_FLIGHT_AGE_MS))
                .isEqualTo(expectedAge);
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT))
                .isEqualTo(expectedFailedSources);
    }

    private static void assertWriterStateGauges(
            TabletServerMetricGroup metrics, long expectedEntries, long expectedBytes) {
        assertThat(metricValue(metrics, MetricNames.INDEX_WRITER_STATE_ENTRIES))
                .isEqualTo(expectedEntries);
        assertThat(metricValue(metrics, MetricNames.INDEX_WRITER_STATE_SNAPSHOT_BYTES))
                .isEqualTo(expectedBytes);
    }

    private static long metricValue(TabletServerMetricGroup metrics, String name) {
        Gauge<?> gauge = (Gauge<?>) metrics.getMetrics().get(name);
        assertThat(gauge).as("registered gauge %s", name).isNotNull();
        return ((Number) gauge.getValue()).longValue();
    }
}
