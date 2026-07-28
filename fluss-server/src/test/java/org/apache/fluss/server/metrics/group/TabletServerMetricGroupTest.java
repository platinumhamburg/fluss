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
    void testIndexGaugeOwnershipCanBeReplacedAndCleared() {
        TabletServerMetricGroup metrics =
                new TabletServerMetricGroup(
                        NOPMetricRegistry.INSTANCE, "gauge-owner", "rack", "host", 1);
        AtomicLong firstEntries = new AtomicLong(11L);
        AtomicLong secondEntries = new AtomicLong(22L);
        AtomicLong firstPending = new AtomicLong(33L);
        AtomicInteger firstInFlight = new AtomicInteger(3);
        AtomicLong firstAge = new AtomicLong(303L);
        AtomicLong firstNoProgress = new AtomicLong(304L);
        AtomicLong firstFailedSources = new AtomicLong(30L);
        AtomicLong secondPending = new AtomicLong(44L);
        AtomicInteger secondInFlight = new AtomicInteger(4);
        AtomicLong secondAge = new AtomicLong(404L);
        AtomicLong secondNoProgress = new AtomicLong(405L);
        AtomicLong secondFailedSources = new AtomicLong(40L);

        TabletServerMetricGroup.GaugeRegistration firstWriterState =
                metrics.registerIndexWriterStateGauge(firstEntries::get);
        TabletServerMetricGroup.GaugeRegistration firstPush =
                metrics.registerIndexPushGauges(
                        firstPending::get,
                        firstInFlight::get,
                        firstAge::get,
                        firstNoProgress::get,
                        firstFailedSources::get);
        assertWriterStateGauge(metrics, 11L);
        assertPushGauges(metrics, 33L, 3L, 303L, 304L, 30L);

        TabletServerMetricGroup.GaugeRegistration secondWriterState =
                metrics.registerIndexWriterStateGauge(secondEntries::get);
        TabletServerMetricGroup.GaugeRegistration secondPush =
                metrics.registerIndexPushGauges(
                        secondPending::get,
                        secondInFlight::get,
                        secondAge::get,
                        secondNoProgress::get,
                        secondFailedSources::get);
        assertWriterStateGauge(metrics, 22L);
        assertPushGauges(metrics, 44L, 4L, 404L, 405L, 40L);
        assertThat(metrics.getMetrics().keySet())
                .filteredOn(MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT::equals)
                .hasSize(1);

        firstWriterState.close();
        firstPush.close();
        assertWriterStateGauge(metrics, 22L);
        assertPushGauges(metrics, 44L, 4L, 404L, 405L, 40L);

        secondWriterState.close();
        secondPush.close();
        assertWriterStateGauge(metrics, 0L);
        assertPushGauges(metrics, 0L, 0L, 0L, 0L, 0L);
    }

    private static void assertPushGauges(
            TabletServerMetricGroup metrics,
            long expectedPending,
            long expectedInFlight,
            long expectedAge,
            long expectedNoProgress,
            long expectedFailedSources) {
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_PENDING_BYTES))
                .isEqualTo(expectedPending);
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_IN_FLIGHT_REQUESTS))
                .isEqualTo(expectedInFlight);
        assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_OLDEST_IN_FLIGHT_AGE_MS))
                .isEqualTo(expectedAge);
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_MAX_NO_PROGRESS_TIME_MS))
                .isEqualTo(expectedNoProgress);
        assertThat(metricValue(metrics, MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT))
                .isEqualTo(expectedFailedSources);
    }

    private static void assertWriterStateGauge(
            TabletServerMetricGroup metrics, long expectedEntries) {
        assertThat(metricValue(metrics, MetricNames.INDEX_WRITER_STATE_ENTRIES))
                .isEqualTo(expectedEntries);
    }

    private static long metricValue(TabletServerMetricGroup metrics, String name) {
        Gauge<?> gauge = (Gauge<?>) metrics.getMetrics().get(name);
        assertThat(gauge).as("registered gauge %s", name).isNotNull();
        return ((Number) gauge.getValue()).longValue();
    }
}
