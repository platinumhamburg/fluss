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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.client.lookup.LookupClient.LOOKUP_CALLBACK_THREAD_PREFIX;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_INFO;
import static org.apache.fluss.record.TestData.IDX_NAME_TABLE_PATH;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_INFO;
import static org.apache.fluss.record.TestData.INDEXED_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the lookup callback executor in {@link LookupClient}, which offloads async callbacks
 * from Netty IO threads to prevent deadlocks in multi-hop lookups (e.g., {@link
 * SecondaryIndexLookuper}).
 */
class SecondaryIndexLookuperTest {

    @Test
    void testLookupCallbackExecutorIsCreatedAndAccessible() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(INDEXED_TABLE_PATH, INDEXED_TABLE_INFO);
        tableInfos.put(IDX_NAME_TABLE_PATH, IDX_NAME_TABLE_INFO);
        TestingMetadataUpdater metadataUpdater = new TestingMetadataUpdater(tableInfos);
        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);

        try {
            ExecutorService executor = lookupClient.getLookupCallbackExecutor();
            assertThat(executor).isNotNull();
            assertThat(executor.isShutdown()).isFalse();
        } finally {
            lookupClient.close(Duration.ofSeconds(5));
        }
    }

    @Test
    void testCallbackExecutorRunsOnDedicatedThread() throws Exception {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(INDEXED_TABLE_PATH, INDEXED_TABLE_INFO);
        tableInfos.put(IDX_NAME_TABLE_PATH, IDX_NAME_TABLE_INFO);
        TestingMetadataUpdater metadataUpdater = new TestingMetadataUpdater(tableInfos);
        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);

        try {
            AtomicReference<String> threadName = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            // Simulate the pattern used in SecondaryIndexLookuper:
            // first-hop completes, then second-hop is dispatched via thenComposeAsync
            CompletableFuture.completedFuture("first-hop-done")
                    .thenComposeAsync(
                            result -> {
                                threadName.set(Thread.currentThread().getName());
                                latch.countDown();
                                return CompletableFuture.completedFuture(null);
                            },
                            lookupClient.getLookupCallbackExecutor());

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(threadName.get()).startsWith(LOOKUP_CALLBACK_THREAD_PREFIX);
        } finally {
            lookupClient.close(Duration.ofSeconds(5));
        }
    }

    @Test
    void testCallbackExecutorIsShutdownOnClose() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(INDEXED_TABLE_PATH, INDEXED_TABLE_INFO);
        tableInfos.put(IDX_NAME_TABLE_PATH, IDX_NAME_TABLE_INFO);
        TestingMetadataUpdater metadataUpdater = new TestingMetadataUpdater(tableInfos);
        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);

        ExecutorService executor = lookupClient.getLookupCallbackExecutor();
        assertThat(executor.isShutdown()).isFalse();

        lookupClient.close(Duration.ofSeconds(5));
        assertThat(executor.isShutdown()).isTrue();
    }
}
