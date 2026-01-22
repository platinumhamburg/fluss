/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.producer;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerSnapshot;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link ProducerSnapshotManager}. */
class ProducerSnapshotManagerTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(new Configuration())
                    .build();

    @TempDir Path tempDir;

    private ProducerSnapshotManager manager;
    private ZooKeeperClient zkClient;

    @BeforeEach
    void setUp() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, tempDir.toString());
        conf.set(ConfigOptions.COORDINATOR_PRODUCER_SNAPSHOT_TTL, java.time.Duration.ofSeconds(5));
        conf.set(
                ConfigOptions.COORDINATOR_PRODUCER_SNAPSHOT_CLEANUP_INTERVAL,
                java.time.Duration.ofSeconds(1));

        manager = new ProducerSnapshotManager(conf, zkClient);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    @Test
    void testSnapshotLifecycle() throws Exception {
        // Tests: register, get, idempotency, delete
        String producerId = "test-producer-lifecycle";

        // 1. Register with multiple tables and partitions
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);
        offsets.put(new TableBucket(1L, 1), 200L);
        offsets.put(new TableBucket(2L, 0), 300L);
        offsets.put(new TableBucket(3L, 100L, 0), 400L); // partitioned table

        boolean created = manager.registerSnapshot(producerId, offsets, null);
        assertThat(created).isTrue();

        // 2. Verify metadata
        Optional<ProducerSnapshot> snapshot = manager.getSnapshotMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getTableOffsets()).hasSize(3); // 3 tables

        // 3. Verify offsets retrieval
        Map<TableBucket, Long> retrieved = manager.getOffsets(producerId);
        assertThat(retrieved).hasSize(4);
        assertThat(retrieved.get(new TableBucket(1L, 0))).isEqualTo(100L);
        assertThat(retrieved.get(new TableBucket(3L, 100L, 0))).isEqualTo(400L);

        // 4. Idempotency: second registration returns false, original preserved
        Map<TableBucket, Long> newOffsets = new HashMap<>();
        newOffsets.put(new TableBucket(1L, 0), 999L);
        assertThat(manager.registerSnapshot(producerId, newOffsets, null)).isFalse();
        assertThat(manager.getOffsets(producerId).get(new TableBucket(1L, 0))).isEqualTo(100L);

        // 5. Delete
        manager.deleteSnapshot(producerId);
        assertThat(manager.getSnapshotMetadata(producerId)).isEmpty();
    }

    @Test
    void testExpirationAndReregistration() throws Exception {
        String producerId = "test-producer-expiration";
        Map<TableBucket, Long> offsets1 = new HashMap<>();
        offsets1.put(new TableBucket(1L, 0), 100L);

        // Register with short TTL
        manager.registerSnapshot(producerId, offsets1, 100L);
        assertThat(manager.getSnapshotMetadata(producerId)).isPresent();

        // Wait for expiration
        Thread.sleep(200);

        // Cleanup should remove it
        int cleaned = manager.cleanupExpiredSnapshots();
        assertThat(cleaned).isEqualTo(1);
        assertThat(manager.getSnapshotMetadata(producerId)).isEmpty();

        // Re-registration should succeed with new offsets
        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(1L, 0), 200L);
        assertThat(manager.registerSnapshot(producerId, offsets2, null)).isTrue();
        assertThat(manager.getOffsets(producerId).get(new TableBucket(1L, 0))).isEqualTo(200L);
    }

    @Test
    void testConcurrentRegistrationAtomicity() throws Exception {
        // Verifies that concurrent registrations are atomic: exactly one succeeds
        String producerId = "test-producer-concurrent";
        int numThreads = 10;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger createdCount = new AtomicInteger(0);
        AtomicInteger alreadyExistsCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    Map<TableBucket, Long> offsets = new HashMap<>();
                                    offsets.put(new TableBucket(1L, 0), (long) (threadId * 100));

                                    boolean created =
                                            manager.registerSnapshot(producerId, offsets, null);
                                    if (created) {
                                        createdCount.incrementAndGet();
                                    } else {
                                        alreadyExistsCount.incrementAndGet();
                                    }
                                } catch (Exception e) {
                                    // Ignore
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await();

        // Exactly one should succeed
        assertThat(createdCount.get()).isEqualTo(1);
        assertThat(alreadyExistsCount.get()).isEqualTo(numThreads - 1);
        assertThat(manager.getSnapshotMetadata(producerId)).isPresent();
    }
}
