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

package org.apache.fluss.server.tablet;

import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.tablet.PartitionChangeWatcher.PartitionChangeCallback;
import org.apache.fluss.server.tablet.PartitionChangeWatcher.PartitionInfo;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionsZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionChangeWatcher}. */
class PartitionChangeWatcherTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";
    private static final TablePath TABLE_PATH = TablePath.of(DATABASE, TABLE);
    private static final long TABLE_ID = 1001L;

    private static ZooKeeperClient zkClient;
    private PartitionChangeWatcher watcher;

    @BeforeAll
    static void beforeAll() throws Exception {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setUp() throws Exception {
        // Create the partitions directory for the table
        String partitionsPath = PartitionsZNode.path(TABLE_PATH);
        zkClient.getCuratorClient().create().creatingParentsIfNeeded().forPath(partitionsPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (watcher != null) {
            watcher.close();
            watcher = null;
        }
        // Clean up ZK nodes
        try {
            zkClient.getCuratorClient()
                    .delete()
                    .deletingChildrenIfNeeded()
                    .forPath(PartitionsZNode.path(TABLE_PATH));
        } catch (Exception e) {
            // Ignore if already deleted
        }
    }

    @Test
    void testStartAndGetCurrentPartitions() throws Exception {
        // Create some partitions before starting the watcher
        createPartition("p1", 1L);
        createPartition("p2", 2L);

        List<PartitionInfo> createdPartitions = new ArrayList<>();
        List<PartitionInfo> deletedPartitions = new ArrayList<>();

        watcher =
                new PartitionChangeWatcher(
                        zkClient,
                        TABLE_PATH,
                        new TestCallback(createdPartitions, deletedPartitions),
                        null);

        List<PartitionInfo> currentPartitions = watcher.startAndGetCurrentPartitions();

        // Should return the existing partitions
        assertThat(currentPartitions).hasSize(2);
        assertThat(currentPartitions)
                .extracting(PartitionInfo::getPartitionName)
                .containsExactlyInAnyOrder("p1", "p2");
    }

    @Test
    void testPartitionCreatedEvent() throws Exception {
        List<PartitionInfo> createdPartitions = new ArrayList<>();
        List<PartitionInfo> deletedPartitions = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        watcher =
                new PartitionChangeWatcher(
                        zkClient,
                        TABLE_PATH,
                        new TestCallback(createdPartitions, deletedPartitions, latch, null),
                        null);

        watcher.startAndGetCurrentPartitions();

        // Create a new partition after starting the watcher
        createPartition("p_new", 100L);

        // Wait for the event
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(createdPartitions).hasSize(1);
        assertThat(createdPartitions.get(0).getPartitionName()).isEqualTo("p_new");
        assertThat(createdPartitions.get(0).getPartitionId()).isEqualTo(100L);
    }

    @Test
    void testPartitionDeletedEvent() throws Exception {
        // Create a partition first
        createPartition("p_to_delete", 200L);

        List<PartitionInfo> createdPartitions = new ArrayList<>();
        List<PartitionInfo> deletedPartitions = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        watcher =
                new PartitionChangeWatcher(
                        zkClient,
                        TABLE_PATH,
                        new TestCallback(createdPartitions, deletedPartitions, null, latch),
                        null);

        watcher.startAndGetCurrentPartitions();

        // Delete the partition
        deletePartition("p_to_delete");

        // Wait for the event
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(deletedPartitions).hasSize(1);
        assertThat(deletedPartitions.get(0).getPartitionName()).isEqualTo("p_to_delete");
        assertThat(deletedPartitions.get(0).getPartitionId()).isEqualTo(200L);
    }

    @Test
    void testMultiplePartitionEvents() throws Exception {
        List<PartitionInfo> createdPartitions = new ArrayList<>();
        List<PartitionInfo> deletedPartitions = new ArrayList<>();
        CountDownLatch createLatch = new CountDownLatch(3);

        watcher =
                new PartitionChangeWatcher(
                        zkClient,
                        TABLE_PATH,
                        new TestCallback(createdPartitions, deletedPartitions, createLatch, null),
                        null);

        watcher.startAndGetCurrentPartitions();

        // Create multiple partitions
        createPartition("p1", 1L);
        createPartition("p2", 2L);
        createPartition("p3", 3L);

        // Wait for all events
        assertThat(createLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(createdPartitions).hasSize(3);
        assertThat(createdPartitions)
                .extracting(PartitionInfo::getPartitionName)
                .containsExactlyInAnyOrder("p1", "p2", "p3");
    }

    @Test
    void testCloseStopsEvents() throws Exception {
        List<PartitionInfo> createdPartitions = new ArrayList<>();
        List<PartitionInfo> deletedPartitions = new ArrayList<>();
        AtomicReference<Exception> error = new AtomicReference<>();

        watcher =
                new PartitionChangeWatcher(
                        zkClient,
                        TABLE_PATH,
                        new TestCallback(createdPartitions, deletedPartitions),
                        null);

        watcher.startAndGetCurrentPartitions();
        watcher.close();

        // Create a partition after closing - should not trigger callback
        createPartition("p_after_close", 999L);

        // Wait a bit to ensure no events are received
        Thread.sleep(500);

        assertThat(createdPartitions).isEmpty();
    }

    private void createPartition(String partitionName, long partitionId) throws Exception {
        String path = PartitionZNode.path(TABLE_PATH, partitionName);
        TablePartition partition = new TablePartition(TABLE_ID, partitionId);
        byte[] data = PartitionZNode.encode(partition);
        zkClient.getCuratorClient().create().forPath(path, data);
    }

    private void deletePartition(String partitionName) throws Exception {
        String path = PartitionZNode.path(TABLE_PATH, partitionName);
        zkClient.getCuratorClient().delete().forPath(path);
    }

    private static class TestCallback implements PartitionChangeCallback {
        private final List<PartitionInfo> createdPartitions;
        private final List<PartitionInfo> deletedPartitions;
        private final CountDownLatch createLatch;
        private final CountDownLatch deleteLatch;

        TestCallback(List<PartitionInfo> createdPartitions, List<PartitionInfo> deletedPartitions) {
            this(createdPartitions, deletedPartitions, null, null);
        }

        TestCallback(
                List<PartitionInfo> createdPartitions,
                List<PartitionInfo> deletedPartitions,
                CountDownLatch createLatch,
                CountDownLatch deleteLatch) {
            this.createdPartitions = createdPartitions;
            this.deletedPartitions = deletedPartitions;
            this.createLatch = createLatch;
            this.deleteLatch = deleteLatch;
        }

        @Override
        public void onPartitionCreated(PartitionInfo partition) {
            createdPartitions.add(partition);
            if (createLatch != null) {
                createLatch.countDown();
            }
        }

        @Override
        public void onPartitionDeleted(PartitionInfo partition) {
            deletedPartitions.add(partition);
            if (deleteLatch != null) {
                deleteLatch.countDown();
            }
        }
    }
}
