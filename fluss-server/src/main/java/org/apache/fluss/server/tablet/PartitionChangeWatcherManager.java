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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.tablet.PartitionChangeWatcher.PartitionChangeCallback;
import org.apache.fluss.server.tablet.PartitionChangeWatcher.PartitionInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * Manages partition change watchers for multiple tables.
 *
 * <p>This manager provides a shared infrastructure for watching partition changes across multiple
 * tables. It ensures that:
 *
 * <ul>
 *   <li>Each table has at most one CuratorCache (shared among all subscribers)
 *   <li>Multiple subscribers can register for the same table
 *   <li>Resources are properly cleaned up when no longer needed
 * </ul>
 *
 * <p>Thread Safety: This class is thread-safe.
 */
@ThreadSafe
public class PartitionChangeWatcherManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionChangeWatcherManager.class);

    private final ZooKeeperClient zkClient;
    private final ExecutorService callbackExecutor;

    /** Map from table path to watch context. */
    private final Map<TablePath, WatchContext> watchContexts = MapUtils.newConcurrentHashMap();

    private volatile boolean closed = false;

    /**
     * Creates a new PartitionChangeWatcherManager.
     *
     * @param zkClient the ZooKeeper client
     * @param callbackExecutor executor for callback invocation
     */
    public PartitionChangeWatcherManager(
            ZooKeeperClient zkClient, ExecutorService callbackExecutor) {
        this.zkClient = zkClient;
        this.callbackExecutor = callbackExecutor;
    }

    /**
     * Subscribes to partition changes for a table.
     *
     * <p>If this is the first subscriber for the table, a new watcher will be created. Otherwise,
     * the existing watcher will be reused.
     *
     * @param tablePath the table to watch
     * @param callback the callback for partition changes
     * @return a handle containing current partitions and a way to unsubscribe
     */
    public SubscriptionHandle subscribe(TablePath tablePath, PartitionChangeCallback callback) {
        if (closed) {
            throw new IllegalStateException("PartitionChangeWatcherManager is closed");
        }

        WatchContext context =
                watchContexts.compute(
                        tablePath,
                        (path, existing) -> {
                            if (existing != null) {
                                existing.addSubscriber(callback);
                                return existing;
                            } else {
                                return createNewContext(path, callback);
                            }
                        });

        List<PartitionInfo> currentPartitions = context.getCurrentPartitions();

        LOG.info(
                "Subscribed to partition changes for table {}, current partitions: {}",
                tablePath,
                currentPartitions.size());

        return new SubscriptionHandle(tablePath, callback, currentPartitions);
    }

    /**
     * Unsubscribes from partition changes.
     *
     * @param handle the subscription handle returned by subscribe()
     */
    public void unsubscribe(SubscriptionHandle handle) {
        if (handle == null) {
            return;
        }

        watchContexts.computeIfPresent(
                handle.tablePath,
                (path, context) -> {
                    context.removeSubscriber(handle.callback);
                    if (context.hasNoSubscribers()) {
                        context.close();
                        LOG.info(
                                "Closed watcher for table {} (no more subscribers)",
                                handle.tablePath);
                        return null; // Remove from map
                    }
                    return context;
                });
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        LOG.info(
                "Closing PartitionChangeWatcherManager with {} active watchers",
                watchContexts.size());

        for (WatchContext context : watchContexts.values()) {
            context.close();
        }
        watchContexts.clear();

        LOG.info("PartitionChangeWatcherManager closed");
    }

    private WatchContext createNewContext(
            TablePath tablePath, PartitionChangeCallback firstSubscriber) {
        WatchContext context = new WatchContext(tablePath);
        context.addSubscriber(firstSubscriber);
        context.start();
        return context;
    }

    /** Context for watching a single table's partitions. */
    private class WatchContext {
        private final TablePath tablePath;
        private final List<PartitionChangeCallback> subscribers = new CopyOnWriteArrayList<>();
        private final PartitionChangeWatcher watcher;
        // Use CopyOnWriteArrayList for thread-safe access from callback and subscriber threads
        private volatile List<PartitionInfo> currentPartitions;

        WatchContext(TablePath tablePath) {
            this.tablePath = tablePath;
            this.watcher =
                    new PartitionChangeWatcher(
                            zkClient, tablePath, new MultiplexingCallback(), callbackExecutor);
        }

        void addSubscriber(PartitionChangeCallback callback) {
            subscribers.add(callback);
        }

        void removeSubscriber(PartitionChangeCallback callback) {
            subscribers.remove(callback);
        }

        boolean hasNoSubscribers() {
            return subscribers.isEmpty();
        }

        void start() {
            // Convert to CopyOnWriteArrayList for thread-safe modifications
            this.currentPartitions =
                    new CopyOnWriteArrayList<>(watcher.startAndGetCurrentPartitions());
        }

        List<PartitionInfo> getCurrentPartitions() {
            return currentPartitions != null
                    ? new ArrayList<>(currentPartitions)
                    : new ArrayList<>();
        }

        void close() {
            watcher.close();
        }

        /** Callback that multiplexes events to all subscribers. */
        private class MultiplexingCallback implements PartitionChangeCallback {
            @Override
            public void onPartitionCreated(PartitionInfo partition) {
                // Update current partitions list (thread-safe via CopyOnWriteArrayList)
                if (currentPartitions != null) {
                    currentPartitions.add(partition);
                }
                // Notify all subscribers
                for (PartitionChangeCallback subscriber : subscribers) {
                    try {
                        subscriber.onPartitionCreated(partition);
                    } catch (Exception e) {
                        LOG.error(
                                "Error notifying subscriber of partition creation for table {}",
                                tablePath,
                                e);
                    }
                }
            }

            @Override
            public void onPartitionDeleted(PartitionInfo partition) {
                // Update current partitions list (thread-safe via CopyOnWriteArrayList)
                if (currentPartitions != null) {
                    currentPartitions.removeIf(
                            p -> p.getPartitionId() == partition.getPartitionId());
                }
                // Notify all subscribers
                for (PartitionChangeCallback subscriber : subscribers) {
                    try {
                        subscriber.onPartitionDeleted(partition);
                    } catch (Exception e) {
                        LOG.error(
                                "Error notifying subscriber of partition deletion for table {}",
                                tablePath,
                                e);
                    }
                }
            }
        }
    }

    /** Handle returned by subscribe() for managing the subscription. */
    public class SubscriptionHandle {
        private final TablePath tablePath;
        private final PartitionChangeCallback callback;
        private final List<PartitionInfo> currentPartitions;

        SubscriptionHandle(
                TablePath tablePath,
                PartitionChangeCallback callback,
                List<PartitionInfo> currentPartitions) {
            this.tablePath = tablePath;
            this.callback = callback;
            this.currentPartitions = currentPartitions;
        }

        /** Returns the current partitions at the time of subscription. */
        public List<PartitionInfo> getCurrentPartitions() {
            return currentPartitions;
        }

        /** Unsubscribes from partition changes. */
        public void unsubscribe() {
            PartitionChangeWatcherManager.this.unsubscribe(this);
        }
    }
}
