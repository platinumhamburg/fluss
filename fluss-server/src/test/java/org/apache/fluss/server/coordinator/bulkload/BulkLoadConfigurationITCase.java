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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Positive retention durations below one millisecond remain valid server configuration. */
class BulkLoadConfigurationITCase {
    @ParameterizedTest
    @ValueSource(longs = {1, 500_000})
    void testPositiveSubmillisecondRetentionStartsCoordinator(long nanos) throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(0)
                        .setRacks(new String[0])
                        .setClusterConf(
                                new Configuration()
                                        .set(
                                                ConfigOptions.COORDINATOR_BULKLOAD_RESULT_RETENTION,
                                                Duration.ofNanos(nanos)))
                        .build();
        try {
            cluster.start();
            assertThat(
                            cluster.newCoordinatorClient()
                                    .listDatabases(new ListDatabasesRequest())
                                    .get())
                    .isNotNull();
        } finally {
            cluster.close();
        }
    }
}
