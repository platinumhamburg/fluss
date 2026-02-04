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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionsZNode;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watches partition changes for a specific table using ZooKeeper's CuratorCache.
 *
 * <p>This class provides reliable partition change notifications by directly watching ZooKeeper,
 * rather than relying on push-based metadata updates which may fail silently.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Watches a specific table's partition directory in ZooKeeper
 *   <li>Provides initial partition list when starting the watch
 *   <li>Delivers partition create/delete events via callback
 *   <li>Handles ZooKeeper reconnection automatically (via Curator)
 * </ul>
 *
 * <p>Thread Safety: This class is thread-safe. The callback may be invoked from Curator's event
 * thread or the provided executor.
 */
@ThreadSafe
public class PartitionChangeWatcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionChangeWatcher.class);

    private final TablePath tablePath;
    private final PartitionChangeCallback callback;
    private final CuratorCache curatorCache;
    private final ExecutorService callbackExecutor;
    private final CountDownLatch initializedLatch = new CountDownLatch(1);

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean started = false;

    @GuardedBy("lock")
    private boolean closed = false;

    /**
     * Creates a new PartitionChangeWatcher.
     *
     * @param zkClient the ZooKeeper client
     * @param tablePath the table to watch
     * @param callback the callback for partition changes
     * @param callbackExecutor optional executor for callback invocation (null for synchronous)
     */
    public PartitionChangeWatcher(
            ZooKeeperClient zkClient,
            TablePath tablePath,
            PartitionChangeCallback callback,
            ExecutorService callbackExecutor) {
        this.tablePath = tablePath;
        this.callback = callback;
        this.callbackExecutor = callbackExecutor;

        // Watch the partitions directory for this table
        String partitionsPath = PartitionsZNode.path(tablePath);
        this.curatorCache = CuratorCache.build(zkClient.getCuratorClient(), partitionsPath);

        // Add listener for partition changes
        this.curatorCache.listenable().addListener(new PartitionCacheListener());

        // Add listener for initialization completion
        this.curatorCache
                .listenable()
                .addListener(
                        CuratorCacheListener.builder()
                                .forInitialized(initializedLatch::countDown)
                                .build());

        LOG.debug("Created PartitionChangeWatcher for table {}", tablePath);
    }

    /**
     * Starts watching and returns the current partition list.
     *
     * <p>This method is idempotent - calling it multiple times has no effect after the first call.
     *
     * @return list of current partitions at the time of starting
     */
    public List<PartitionInfo> startAndGetCurrentPartitions() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "PartitionChangeWatcher for " + tablePath + " is already closed");
            }
            if (started) {
                LOG.warn(
                        "PartitionChangeWatcher for {} already started, returning current partitions",
                        tablePath);
                return getCurrentPartitionsFromCache();
            }

            started = true;
            curatorCache.start();
            LOG.info("Started PartitionChangeWatcher for table {}", tablePath);

            // Wait for CuratorCache to complete initial data load
            // Use proper initialization callback instead of arbitrary sleep
            try {
                if (!initializedLatch.await(30, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Timeout waiting for CuratorCache initialization for table {}",
                            tablePath);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for CuratorCache initialization", e);
            }

            return getCurrentPartitionsFromCache();
        }
    }

    /** Returns the current partitions from the cache. */
    private List<PartitionInfo> getCurrentPartitionsFromCache() {
        List<PartitionInfo> partitions = new ArrayList<>();
        String partitionsPath = PartitionsZNode.path(tablePath);

        curatorCache.stream()
                .filter(data -> !data.getPath().equals(partitionsPath)) // Exclude root node
                .forEach(
                        data -> {
                            PartitionInfo info = parsePartitionData(data);
                            if (info != null) {
                                partitions.add(info);
                            }
                        });

        LOG.debug(
                "Retrieved {} current partitions for table {} from cache",
                partitions.size(),
                tablePath);
        return partitions;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            curatorCache.close();
            LOG.info("Closed PartitionChangeWatcher for table {}", tablePath);
        }
    }

    /** Parses partition data from a CuratorCache ChildData. */
    private PartitionInfo parsePartitionData(ChildData data) {
        if (data == null || data.getData() == null) {
            return null;
        }

        PhysicalTablePath physicalTablePath = PartitionZNode.parsePath(data.getPath());
        if (physicalTablePath == null || physicalTablePath.getPartitionName() == null) {
            return null;
        }

        try {
            TablePartition partition = PartitionZNode.decode(data.getData());
            return new PartitionInfo(
                    partition.getTableId(),
                    partition.getPartitionId(),
                    physicalTablePath.getPartitionName());
        } catch (Exception e) {
            LOG.warn("Failed to parse partition data from path {}", data.getPath(), e);
            return null;
        }
    }

    /** Listener for CuratorCache events. */
    private class PartitionCacheListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            synchronized (lock) {
                if (closed) {
                    return;
                }
            }

            switch (type) {
                case NODE_CREATED:
                    handleNodeCreated(newData);
                    break;
                case NODE_DELETED:
                    handleNodeDeleted(oldData);
                    break;
                case NODE_CHANGED:
                    // Partition data changes are not expected, ignore
                    break;
            }
        }

        private void handleNodeCreated(ChildData newData) {
            if (newData == null) {
                return;
            }

            // Skip the root partitions directory node
            String partitionsPath = PartitionsZNode.path(tablePath);
            if (newData.getPath().equals(partitionsPath)) {
                return;
            }

            PartitionInfo info = parsePartitionData(newData);
            if (info == null) {
                LOG.debug("Ignoring non-partition node: {}", newData.getPath());
                return;
            }

            LOG.info(
                    "Partition created: table={}, partition={}, partitionId={}",
                    tablePath,
                    info.getPartitionName(),
                    info.getPartitionId());

            invokeCallback(() -> callback.onPartitionCreated(info));
        }

        private void handleNodeDeleted(ChildData oldData) {
            if (oldData == null) {
                return;
            }

            // Skip the root partitions directory node
            String partitionsPath = PartitionsZNode.path(tablePath);
            if (oldData.getPath().equals(partitionsPath)) {
                LOG.warn("Partitions directory deleted for table {}", tablePath);
                return;
            }

            PartitionInfo info = parsePartitionData(oldData);
            if (info == null) {
                LOG.debug("Ignoring non-partition node deletion: {}", oldData.getPath());
                return;
            }

            LOG.info(
                    "Partition deleted: table={}, partition={}, partitionId={}",
                    tablePath,
                    info.getPartitionName(),
                    info.getPartitionId());

            invokeCallback(() -> callback.onPartitionDeleted(info));
        }

        private void invokeCallback(Runnable action) {
            if (callbackExecutor != null) {
                callbackExecutor.execute(
                        () -> {
                            try {
                                action.run();
                            } catch (Exception e) {
                                LOG.error(
                                        "Error in partition change callback for table {}",
                                        tablePath,
                                        e);
                            }
                        });
            } else {
                try {
                    action.run();
                } catch (Exception e) {
                    LOG.error("Error in partition change callback for table {}", tablePath, e);
                }
            }
        }
    }

    /** Callback interface for partition change events. */
    public interface PartitionChangeCallback {
        /**
         * Called when a new partition is created.
         *
         * @param partition the created partition info
         */
        void onPartitionCreated(PartitionInfo partition);

        /**
         * Called when a partition is deleted.
         *
         * @param partition the deleted partition info
         */
        void onPartitionDeleted(PartitionInfo partition);
    }

    /** Information about a partition. */
    public static class PartitionInfo {
        private final long tableId;
        private final long partitionId;
        private final String partitionName;

        public PartitionInfo(long tableId, long partitionId, String partitionName) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.partitionName = partitionName;
        }

        public long getTableId() {
            return tableId;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public String getPartitionName() {
            return partitionName;
        }

        @Override
        public String toString() {
            return "PartitionInfo{"
                    + "tableId="
                    + tableId
                    + ", partitionId="
                    + partitionId
                    + ", partitionName='"
                    + partitionName
                    + '\''
                    + '}';
        }
    }
}
