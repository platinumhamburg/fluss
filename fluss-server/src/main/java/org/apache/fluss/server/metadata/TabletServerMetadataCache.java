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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** The implement of {@link ServerMetadataCache} for {@link TabletServer}. */
public class TabletServerMetadataCache implements ServerMetadataCache {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerMetadataCache.class);

    private final Lock metadataLock = new ReentrantLock();

    /**
     * This is cache state. every Cluster instance is immutable, and updates (performed under a
     * lock) replace the value with a completely new one. this means reads (which are not under any
     * lock) need to grab the value of this ONCE and retain that read copy for the duration of their
     * operation.
     *
     * <p>multiple reads of this value risk getting different snapshots.
     */
    @GuardedBy("bucketMetadataLock")
    private volatile ServerMetadataSnapshot serverMetadataSnapshot;

    private final MetadataManager metadataManager;

    private Map<TablePath, List<PartitionListener>> tablePartitionListener =
            MapUtils.newConcurrentHashMap();

    public TabletServerMetadataCache(MetadataManager metadataManager) {
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
        this.metadataManager = metadataManager;
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Set<TabletServerInfo> tabletServerInfoList =
                serverMetadataSnapshot.getAliveTabletServerInfos();
        for (TabletServerInfo tabletServer : tabletServerInfoList) {
            if (tabletServer.getId() == serverId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServersById(serverId, listenerName);
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServers(listenerName);
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        return serverMetadataSnapshot.getCoordinatorServer(listenerName);
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        return serverMetadataSnapshot.getAliveTabletServerInfos();
    }

    public Optional<TablePath> getTablePath(long tableId) {
        return serverMetadataSnapshot.getTablePath(tableId);
    }

    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return serverMetadataSnapshot.getPhysicalTablePath(partitionId);
    }

    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        // Only get data from cache, do not access ZK.
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        if (!tableIdOpt.isPresent()) {
            LOG.debug("Table {} not found in metadata cache", tablePath);
            return Optional.empty();
        }

        long tableId = tableIdOpt.getAsLong();

        // Try to get table info from ZK only if we have the table ID in cache
        try {
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            List<BucketMetadata> bucketMetadataList =
                    new ArrayList<>(snapshot.getBucketMetadataForTable(tableId).values());
            LOG.debug(
                    "Retrieved table metadata for {}: {} buckets",
                    tablePath,
                    bucketMetadataList.size());
            return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
        } catch (Exception e) {
            // If table doesn't exist in ZK but exists in cache, return empty
            // This maintains backward compatibility while fixing the semantic issue
            LOG.warn(
                    "Failed to retrieve table metadata from ZK for table {} (cached table ID: {}): {}",
                    tablePath,
                    tableId,
                    e.getMessage());
            return Optional.empty();
        }
    }

    public Tuple2<List<PartitionMetadata>, PartitionListenerHandle> listPartitionMetaAndWatch(
            TablePath tablePath,
            Consumer<PartitionEvent> eventConsumer,
            ExecutorService executorService) {
        return inLock(
                metadataLock,
                () -> {
                    ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
                    OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
                    if (!tableIdOpt.isPresent()) {
                        LOG.error(
                                "Failed to register partition listener: table {} not found in metadata cache",
                                tablePath);
                        throw new TableNotExistException(tablePath);
                    }
                    long tableId = tableIdOpt.getAsLong();
                    List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
                    for (Map.Entry<PhysicalTablePath, Long> entry :
                            snapshot.getPartitionIdByPath().entrySet()) {
                        PhysicalTablePath physicalTablePath = entry.getKey();
                        Long partitionId = entry.getValue();
                        if (!physicalTablePath.getTablePath().equals(tablePath)) {
                            continue;
                        }
                        partitionMetadataList.add(
                                new PartitionMetadata(
                                        tableId,
                                        physicalTablePath.getPartitionName(),
                                        partitionId,
                                        new ArrayList<>(
                                                snapshot.getBucketMetadataForPartition(partitionId)
                                                        .values())));
                    }
                    PartitionListener listener =
                            new PartitionListener(executorService, eventConsumer);
                    PartitionListenerHandle handle = new PartitionListenerHandle(listener);
                    tablePartitionListener
                            .computeIfAbsent(tablePath, tp -> new ArrayList<>())
                            .add(listener);
                    LOG.debug(
                            "Added partition listener for table {}, current partition count: {}",
                            tablePath,
                            partitionMetadataList.size());
                    return Tuple2.of(partitionMetadataList, handle);
                });
    }

    public Tuple2<List<PartitionMetadata>, PartitionListenerHandle> listPartitionMetaAndWatch(
            TablePath tablePath, Consumer<PartitionEvent> eventConsumer) {
        return listPartitionMetaAndWatch(tablePath, eventConsumer, null);
    }

    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        TablePath tablePath = partitionPath.getTablePath();
        String partitionName = partitionPath.getPartitionName();
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;

        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        Optional<Long> partitionIdOpt = snapshot.getPartitionId(partitionPath);
        if (tableIdOpt.isPresent() && partitionIdOpt.isPresent()) {
            long tableId = tableIdOpt.getAsLong();
            long partitionId = partitionIdOpt.get();
            return Optional.of(
                    new PartitionMetadata(
                            tableId,
                            partitionName,
                            partitionId,
                            new ArrayList<>(
                                    snapshot.getBucketMetadataForPartition(partitionId).values())));
        } else {

            return Optional.empty();
        }
    }

    public boolean contains(TableBucket tableBucket) {
        return serverMetadataSnapshot.contains(tableBucket);
    }

    public void updateClusterMetadata(ClusterMetadata clusterMetadata) {
        inLock(
                metadataLock,
                () -> {
                    // 1. update coordinator server.
                    ServerInfo coordinatorServer = clusterMetadata.getCoordinatorServer();

                    // 2. Update the alive table servers. We always use the new alive table servers
                    // to replace the old alive table servers.
                    Map<Integer, ServerInfo> newAliveTableServers = new HashMap<>();
                    Set<ServerInfo> aliveTabletServers = clusterMetadata.getAliveTabletServers();
                    for (ServerInfo tabletServer : aliveTabletServers) {
                        newAliveTableServers.put(tabletServer.id(), tabletServer);
                    }

                    // 3. update table metadata. Always partial update.
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getTableIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(serverMetadataSnapshot.getBucketMetadataMapForTables());

                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();
                        if (tableId == DELETED_TABLE_ID) {
                            Long removedTableId = tableIdByPath.remove(tablePath);
                            if (removedTableId != null) {
                                bucketMetadataMapForTables.remove(removedTableId);
                            }
                        } else if (tablePath == DELETED_TABLE_PATH) {
                            Optional<TablePath> removedTablePath =
                                    serverMetadataSnapshot.getTablePath(tableId);
                            removedTablePath.ifPresent(tableIdByPath::remove);
                            bucketMetadataMapForTables.remove(tableId);
                        } else {
                            tableIdByPath.put(tablePath, tableId);
                            tableMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMapForTables
                                                            .computeIfAbsent(
                                                                    tableId, k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                        }
                    }

                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach(
                            ((tablePath, tableId) -> newPathByTableId.put(tableId, tablePath)));

                    // 4. update partition metadata. Always partial update.
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(
                                    serverMetadataSnapshot.getBucketMetadataMapForPartitions());

                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        TablePath tablePath = newPathByTableId.get(tableId);
                        String partitionName = partitionMetadata.getPartitionName();
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(tablePath, partitionName);
                        long partitionId = partitionMetadata.getPartitionId();
                        List<PartitionListener> listeners = tablePartitionListener.get(tablePath);
                        if (partitionId == DELETED_PARTITION_ID) {
                            Long removedPartitionId = partitionIdByPath.remove(physicalTablePath);
                            if (removedPartitionId != null) {
                                bucketMetadataMapForPartitions.remove(removedPartitionId);
                                PartitionMetadata oldPartitionMeta =
                                        new PartitionMetadata(
                                                partitionMetadata.getTableId(),
                                                partitionName,
                                                removedPartitionId,
                                                partitionMetadata.getBucketMetadataList());
                                LOG.info(
                                        "Partition deleted: table={}, partitionName={}, partitionId={}",
                                        tablePath,
                                        partitionName,
                                        removedPartitionId);
                                if (null != listeners) {
                                    int initialListenerCount = listeners.size();
                                    listeners.removeIf(
                                            listener ->
                                                    !listener.acceptEvent(
                                                            PartitionEvent.ofDeleted(
                                                                    oldPartitionMeta)));
                                    int removedListeners = initialListenerCount - listeners.size();
                                    if (removedListeners > 0) {
                                        LOG.debug(
                                                "Removed {} closed listeners for table {} after partition deletion",
                                                removedListeners,
                                                tablePath);
                                    }
                                }
                            }
                        } else if (partitionName.equals(DELETED_PARTITION_NAME)) {
                            Optional<PhysicalTablePath> removedPartitionPath =
                                    serverMetadataSnapshot.getPhysicalTablePath(partitionId);
                            removedPartitionPath.ifPresent(partitionIdByPath::remove);
                            bucketMetadataMapForPartitions.remove(partitionId);
                            PartitionMetadata oldPartitionMeta =
                                    new PartitionMetadata(
                                            tableId,
                                            removedPartitionPath
                                                    .map(PhysicalTablePath::getPartitionName)
                                                    .orElse(partitionName),
                                            partitionId,
                                            partitionMetadata.getBucketMetadataList());
                            LOG.info(
                                    "Partition deleted by id: table={}, partitionId={}, partitionName={}",
                                    tablePath,
                                    partitionId,
                                    removedPartitionPath
                                            .map(PhysicalTablePath::getPartitionName)
                                            .orElse("unknown"));
                            if (null != listeners) {
                                int initialListenerCount = listeners.size();
                                listeners.removeIf(
                                        listener ->
                                                !listener.acceptEvent(
                                                        PartitionEvent.ofDeleted(
                                                                oldPartitionMeta)));
                                int removedListeners = initialListenerCount - listeners.size();
                                if (removedListeners > 0) {
                                    LOG.debug(
                                            "Removed {} closed listeners for table {} after partition deletion by id",
                                            removedListeners,
                                            tablePath);
                                }
                            }
                        } else {
                            partitionIdByPath.put(physicalTablePath, partitionId);
                            partitionMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMapForPartitions
                                                            .computeIfAbsent(
                                                                    partitionId,
                                                                    k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                            LOG.info(
                                    "Partition created: table={}, partitionName={}, partitionId={}, buckets={}",
                                    tablePath,
                                    partitionName,
                                    partitionId,
                                    partitionMetadata.getBucketMetadataList().size());
                            if (null != listeners) {
                                int initialListenerCount = listeners.size();
                                listeners.removeIf(
                                        listener ->
                                                !listener.acceptEvent(
                                                        PartitionEvent.ofCreated(
                                                                partitionMetadata)));
                                int removedListeners = initialListenerCount - listeners.size();
                                if (removedListeners > 0) {
                                    LOG.debug(
                                            "Removed {} closed listeners for table {} after partition creation",
                                            removedListeners,
                                            tablePath);
                                }
                            }
                        }
                    }

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    newAliveTableServers,
                                    tableIdByPath,
                                    newPathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions);
                });
    }

    /**
     * Get all related index tables for the specified data table. This method is used by
     * ReplicaManager to obtain index metadata for data table replicas.
     *
     * @param tablePath the path of the data table
     * @return list of index table info, or empty list if no indexes
     */
    public List<TableInfo> getRelatedIndexTables(TablePath tablePath) {
        // Get the main table information from ZooKeeper
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        Schema schema = tableInfo.getSchema();
        List<Schema.Index> indexes = schema.getIndexes();

        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        List<TableInfo> indexTableInfoList = new ArrayList<>();

        // Get info for each index table
        for (Schema.Index index : indexes) {
            TablePath indexTablePath =
                    IndexTableUtils.generateIndexTablePath(tablePath, index.getIndexName());
            TableInfo indexTableInfo = metadataManager.getTable(indexTablePath);
            if (indexTableInfo != null) {
                indexTableInfoList.add(indexTableInfo);
            }
        }
        return indexTableInfoList;
    }

    /**
     * Get the data table information for the specified index table. This method is used by
     * ReplicaManager to obtain data table metadata for index table replicas.
     *
     * @param indexTablePath the path of the index table
     * @return data table info or null if not found or not an index table
     */
    public Optional<TableInfo> getMainTableForIndex(TablePath indexTablePath) {
        try {
            String mainTableName =
                    IndexTableUtils.extractMainTableName(indexTablePath.getTableName());
            if (mainTableName == null) {
                // Not an index table
                return Optional.empty();
            }

            TablePath dataTablePath =
                    new TablePath(indexTablePath.getDatabaseName(), mainTableName);
            return Optional.ofNullable(metadataManager.getTable(dataTablePath));
        } catch (Exception e) {
            LOG.warn("Failed to get data table for index {}: {}", indexTablePath, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Clear cached metadata for the specified table path from both main cache and index metadata
     * cache. This should be called when tables are dropped or metadata changes.
     *
     * @param tablePath the table path to clear from cache
     */
    public void clearTableMetadata(TablePath tablePath) {
        // Clear from main metadata cache would be handled by updateClusterMetadata
        // Here we just need to clear from any potential index metadata cache
        LOG.debug("Cleared metadata for table {}", tablePath);
    }

    @VisibleForTesting
    public void clearTableMetadata() {
        inLock(
                metadataLock,
                () -> {
                    ServerInfo coordinatorServer = serverMetadataSnapshot.getCoordinatorServer();
                    Map<Integer, ServerInfo> aliveTabletServers =
                            serverMetadataSnapshot.getAliveTabletServers();
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    aliveTabletServers,
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap());
                });
    }

    /**
     * Update a single table metadata to the local cache. This method is thread-safe and will merge
     * the new table metadata with existing cache.
     *
     * @param tableMetadata the table metadata to update
     */
    public void updateTableMetadata(TableMetadata tableMetadata) {
        inLock(
                metadataLock,
                () -> {
                    TableInfo tableInfo = tableMetadata.getTableInfo();
                    TablePath tablePath = tableInfo.getTablePath();
                    long tableId = tableInfo.getTableId();

                    // Get current snapshot
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;

                    // Create new maps based on current state
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());

                    // Update table mapping
                    tableIdByPath.put(tablePath, tableId);
                    pathByTableId.put(tableId, tablePath);

                    // Update bucket metadata for this table
                    Map<Integer, BucketMetadata> tableBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata : tableMetadata.getBucketMetadataList()) {
                        tableBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForTables.put(tableId, tableBucketMetadata);

                    // Copy other existing data
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Create new snapshot
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions);
                });
    }

    /**
     * Update a single partition metadata to the local cache. This method is thread-safe and will
     * merge the new partition metadata with existing cache.
     *
     * @param partitionMetadata the partition metadata to update
     */
    public void updatePartitionMetadata(PartitionMetadata partitionMetadata) {
        inLock(
                metadataLock,
                () -> {
                    long tableId = partitionMetadata.getTableId();
                    String partitionName = partitionMetadata.getPartitionName();
                    long partitionId = partitionMetadata.getPartitionId();

                    // Get current snapshot
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;

                    // Get table path from tableId
                    Optional<TablePath> tablePathOpt = currentSnapshot.getTablePath(tableId);
                    if (!tablePathOpt.isPresent()) {
                        // If table doesn't exist in cache, we can't update partition metadata
                        return;
                    }
                    TablePath tablePath = tablePathOpt.get();
                    PhysicalTablePath physicalTablePath =
                            PhysicalTablePath.of(tablePath, partitionName);

                    // Create new maps based on current state
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());

                    // Update partition mapping
                    partitionIdByPath.put(physicalTablePath, partitionId);

                    // Update bucket metadata for this partition
                    Map<Integer, BucketMetadata> partitionBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata :
                            partitionMetadata.getBucketMetadataList()) {
                        partitionBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForPartitions.put(partitionId, partitionBucketMetadata);

                    // Copy other existing data
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Create new snapshot
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions);
                });
    }

    /** Partition listener. */
    public static class PartitionListener {

        private boolean closed = false;

        private final ExecutorService executorService;

        private final Consumer<PartitionEvent> eventConsumer;

        public PartitionListener(
                @Nullable ExecutorService executorService, Consumer<PartitionEvent> eventConsumer) {
            this.executorService = executorService;
            this.eventConsumer = eventConsumer;
        }

        public boolean acceptEvent(PartitionEvent partitionEvent) {
            synchronized (this) {
                if (closed) {
                    LOG.debug(
                            "Partition listener is closed, ignoring event: {}",
                            partitionEvent.getType());
                    return false;
                }
                try {
                    if (null == executorService) {
                        this.eventConsumer.accept(partitionEvent);
                        LOG.debug(
                                "Processed partition event synchronously: type={}, partition={}",
                                partitionEvent.getType(),
                                partitionEvent.getPartitionMetadata().getPartitionName());
                    } else {
                        executorService.submit(
                                () -> {
                                    try {
                                        this.eventConsumer.accept(partitionEvent);
                                        LOG.debug(
                                                "Processed partition event asynchronously: type={}, partition={}",
                                                partitionEvent.getType(),
                                                partitionEvent
                                                        .getPartitionMetadata()
                                                        .getPartitionName());
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to process partition event asynchronously: type={}, partition={}, error={}",
                                                partitionEvent.getType(),
                                                partitionEvent
                                                        .getPartitionMetadata()
                                                        .getPartitionName(),
                                                e.getMessage(),
                                                e);
                                    }
                                });
                    }
                } catch (Exception e) {
                    LOG.error(
                            "Failed to process partition event: type={}, partition={}, error={}",
                            partitionEvent.getType(),
                            partitionEvent.getPartitionMetadata().getPartitionName(),
                            e.getMessage(),
                            e);
                    return false;
                }
            }
            return true;
        }

        public void close() {
            synchronized (this) {
                LOG.debug("Closing partition listener");
                this.closed = true;
            }
        }
    }

    /** Partition listener handle. */
    public static class PartitionListenerHandle {
        private final PartitionListener listener;

        PartitionListenerHandle(PartitionListener listener) {
            this.listener = listener;
        }

        public void stopWatch() {
            LOG.debug("Stopping partition listener watch");
            this.listener.close();
        }
    }

    /** Partition create or drop event type. */
    public enum PartitionEventType {
        CREATED,
        DELETED,
    }

    /** Partition create or drop event. */
    public static class PartitionEvent {
        private final PartitionEventType type;

        private final PartitionMetadata partitionMetadata;

        private PartitionEvent(PartitionEventType type, PartitionMetadata partitionMetadata) {
            this.type = type;
            this.partitionMetadata = partitionMetadata;
        }

        public static PartitionEvent ofCreated(PartitionMetadata partitionMetadata) {
            return new PartitionEvent(PartitionEventType.CREATED, partitionMetadata);
        }

        public static PartitionEvent ofDeleted(PartitionMetadata partitionMetadata) {
            return new PartitionEvent(PartitionEventType.DELETED, partitionMetadata);
        }

        public PartitionEventType getType() {
            return type;
        }

        public PartitionMetadata getPartitionMetadata() {
            return partitionMetadata;
        }
    }
}
