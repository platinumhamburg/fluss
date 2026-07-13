/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.jmh;

import org.apache.fluss.record.WriterKey;
import org.apache.fluss.server.log.WriterStateManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexWriterStateBenchmarkTest {

    @Test
    void testV1ManagersRetainTargetLocalWriterKeys() throws Exception {
        IndexWriterStateBenchmark.TopologyState state = topology("V1_FENCED", 64, 2);
        try {
            state.setup();
            WriterStateManager[] managers = managers(state);
            Map<WriterKey, ?> first = fencedWriters(managers[0]);
            Map<WriterKey, ?> second = fencedWriters(managers[1]);

            for (int writer = 0; writer < state.configuredSourceWriters; writer++) {
                WriterKey expected = new WriterKey(0L, writer);
                WriterKey firstKey = retainedKey(first, expected);
                WriterKey secondKey = retainedKey(second, expected);
                assertThat(firstKey)
                        .as("writer %s must be materialized independently per target", writer)
                        .isNotSameAs(secondKey);
            }
        } finally {
            state.tearDown();
        }
    }

    @Test
    void testBenchmarkDoesNotCacheWriterKeysOutsideMeasuredManagers() {
        assertThat(
                        Arrays.stream(
                                        IndexWriterStateBenchmark.TopologyState.class
                                                .getSuperclass()
                                                .getDeclaredFields())
                                .map(Field::getType))
                .doesNotContain(WriterKey[].class);
    }

    @Test
    void testFreshTraversalCoversEveryManagerAndWriter() throws Exception {
        IndexWriterStateBenchmark.TopologyState state = topology("V1_FENCED", 64, 16);
        try {
            state.setup();
            state.assertAndResetFreshTraversalCoverage();
        } finally {
            state.tearDown();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"V0_COMPACT", "V1_FENCED"})
    void testSnapshotReloadRetainsEveryConfiguredWriter(String protocol) throws Exception {
        IndexWriterStateBenchmark benchmark = new IndexWriterStateBenchmark();
        IndexWriterStateBenchmark.TopologyState state = topology(protocol, 64, 16);
        try {
            state.setup();
            for (int bucket = 0; bucket < state.configuredTargetBuckets; bucket++) {
                assertThat(benchmark.snapshotReload(state))
                        .as("protocol=%s bucket=%s", protocol, bucket)
                        .isEqualTo(state.configuredSourceWriters);
            }
        } finally {
            state.tearDown();
        }
    }

    private static IndexWriterStateBenchmark.TopologyState topology(
            String protocol, int sourceWriters, int targetBuckets) {
        IndexWriterStateBenchmark.TopologyState state =
                new IndexWriterStateBenchmark.TopologyState();
        state.protocolName = protocol;
        state.configuredSourceWriters = sourceWriters;
        state.configuredTargetBuckets = targetBuckets;
        return state;
    }

    private static WriterStateManager[] managers(IndexWriterStateBenchmark.TopologyState state)
            throws Exception {
        Field field = state.getClass().getSuperclass().getDeclaredField("managers");
        field.setAccessible(true);
        return (WriterStateManager[]) field.get(state);
    }

    @SuppressWarnings("unchecked")
    private static Map<WriterKey, ?> fencedWriters(WriterStateManager manager) throws Exception {
        Field field = WriterStateManager.class.getDeclaredField("fencedWriters");
        field.setAccessible(true);
        return (Map<WriterKey, ?>) field.get(manager);
    }

    private static WriterKey retainedKey(Map<WriterKey, ?> writers, WriterKey expected) {
        return writers.keySet().stream()
                .filter(expected::equals)
                .findFirst()
                .orElseThrow(AssertionError::new);
    }
}
