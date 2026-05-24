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

package org.apache.fluss.server.zk;

import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ZooKeeperClient}'s {@link PartitionTombstone} CRUD methods (Plan 3 Task 4 of
 * the Global Secondary Index design).
 */
class ZooKeeperClientPartitionTombstoneTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    @BeforeAll
    static void beforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zkClient.close();
    }

    @Test
    void testGetReturnsEmptyForUnknownTable() throws Exception {
        PartitionTombstone result = zkClient.getPartitionTombstone(TablePath.of("db", "t"));
        assertThat(result).isEqualTo(PartitionTombstone.EMPTY);
    }

    @Test
    void testRoundTripPersistsAndReadsBack() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        Set<Long> explicit = new HashSet<>();
        explicit.add(7L);
        PartitionTombstone original = new PartitionTombstone(5L, explicit, 3L);
        zkClient.setOrCreatePartitionTombstone(tp, original);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(original);
    }

    @Test
    void testOverwriteAdvancesPersistedValue() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        PartitionTombstone v1 = new PartitionTombstone(0L, Collections.emptySet(), 1L);
        PartitionTombstone v2 = new PartitionTombstone(5L, Collections.emptySet(), 2L);
        zkClient.setOrCreatePartitionTombstone(tp, v1);
        zkClient.setOrCreatePartitionTombstone(tp, v2);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(v2);
    }

    @Test
    void testDeleteRemovesTombstoneNode() throws Exception {
        TablePath tp = TablePath.of("db", "t");
        zkClient.setOrCreatePartitionTombstone(
                tp, new PartitionTombstone(5L, Collections.emptySet(), 1L));
        zkClient.deletePartitionTombstone(tp);
        assertThat(zkClient.getPartitionTombstone(tp)).isEqualTo(PartitionTombstone.EMPTY);
    }
}
