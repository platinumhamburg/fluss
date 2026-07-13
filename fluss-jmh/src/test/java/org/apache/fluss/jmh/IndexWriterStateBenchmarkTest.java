/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.jmh;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class IndexWriterStateBenchmarkTest {

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
}
