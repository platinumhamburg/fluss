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
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.zk.ZooKeeperClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
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

    private final ServerSchemaCache serverSchemaCache;

    /**
     * Cached partition tombstones keyed by main table id. Populated by metadata propagation from
     * the Coordinator (P3T6). Read on the apply path to filter writes targeting tombstoned source
     * partitions. Concurrent map provides lock-free reads with safe per-entry replacement on
     * update.
     */
    private final ConcurrentHashMap<Long, PartitionTombstone> partitionTombstones =
            new ConcurrentHashMap<>();

    /**
     * Optional ZooKeeper client used to rebuild {@link #partitionTombstones} from ZK on the first
     * cluster-metadata update after a TabletServer restart (P3T12). Wired by {@link
     * TabletServer#startServices()} via {@link #setZooKeeperClient(ZooKeeperClient)}. Stays {@code
     * null} in unit tests that exercise the cache without a real ZK client; in that case automatic
     * seeding is disabled and tests can call {@link #seedPartitionTombstonesFromZk(ZooKeeperClient,
     * Collection)} directly.
     */
    @Nullable private volatile ZooKeeperClient zkClientForTombstoneSeed;

    /** One-shot guard so the automatic ZK-seed only fires on the first non-empty metadata batch. */
    private final AtomicBoolean tombstonesSeededFromZk = new AtomicBoolean(false);

    /**
     * Optional callback fired after every successful {@link #updateClusterMetadata} so the
     * surrounding {@code ReplicaManager} can retry any {@link
     * org.apache.fluss.server.replica.Replica} whose {@code IndexPushScheduler} init was deferred
     * because the auto-derived Index Table was not yet visible in the cache (P3T14). Wired by
     * {@link org.apache.fluss.server.replica.ReplicaManager}; left {@code null} in unit-test
     * fixtures that exercise the cache without a ReplicaManager.
     */
    @Nullable private volatile Runnable replicaRetryHook;

    public TabletServerMetadataCache(MetadataManager metadataManager) {
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
        this.metadataManager = metadataManager;
        this.serverSchemaCache = new ServerSchemaCache(metadataManager);
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

    /**
     * Returns the cached {@code tableId} for the given {@link TablePath}, or empty if the cache
     * does not yet know about the table. Cache-only — does NOT consult ZK.
     */
    public OptionalLong getTableId(TablePath tablePath) {
        return serverMetadataSnapshot.getTableId(tablePath);
    }

    /**
     * Returns the cached {@code partitionId} for the given {@link PhysicalTablePath}, or empty if
     * the cache does not yet know about the partition. Cache-only — does NOT consult ZK.
     */
    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return serverMetadataSnapshot.getPartitionId(physicalTablePath);
    }

    /**
     * Returns the cached leader server id for {@code (tableId, bucketId)} of a non-partitioned
     * table, or empty if the cache does not yet know the leader (e.g. mid-election). Cache-only —
     * does NOT consult ZK. Used by the index-push pipeline to locate the Index Bucket leader.
     */
    public OptionalInt getBucketLeader(long tableId, int bucketId) {
        BucketMetadata bucketMetadata =
                serverMetadataSnapshot.getBucketMetadataForTable(tableId).get(bucketId);
        if (bucketMetadata == null) {
            return OptionalInt.empty();
        }
        return bucketMetadata.getLeaderId();
    }

    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        // Only get data from cache, do not access ZK.
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        if (!tableIdOpt.isPresent()) {
            return Optional.empty();
        }

        long tableId = tableIdOpt.getAsLong();

        // Try to get table info from ZK only if we have the table ID in cache
        try {
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            List<BucketMetadata> bucketMetadataList =
                    new ArrayList<>(snapshot.getBucketMetadataForTable(tableId).values());
            return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
        } catch (Exception e) {
            // If table doesn't exist in ZK but exists in cache, return empty
            // This maintains backward compatibility while fixing the semantic issue
            return Optional.empty();
        }
    }

    public SchemaGetter subscribeWithInitialSchema(
            TablePath tablePath, long tableId, int initialSchemaId, Schema initialSchema) {
        return serverSchemaCache.subscribeWithInitialSchema(
                tableId, tablePath, initialSchemaId, initialSchema);
    }

    public void updateLatestSchema(long tableId, SchemaInfo schemaInfo) {
        serverSchemaCache.updateLatestSchema(
                tableId, (short) schemaInfo.getSchemaId(), schemaInfo.getSchema());
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
                        // Update schema metadata.
                        // todo: apply schema id and schema info if needs
                        int schemaId = tableInfo.getSchemaId();
                        Schema schema = tableInfo.getSchema();
                        serverSchemaCache.updateLatestSchema(tableId, (short) schemaId, schema);

                        if (tableId == DELETED_TABLE_ID) {
                            Long removedTableId = tableIdByPath.remove(tablePath);
                            if (removedTableId != null) {
                                bucketMetadataMapForTables.remove(removedTableId);
                            }
                        } else if (tablePath == DELETED_TABLE_PATH) {
                            serverMetadataSnapshot
                                    .getTablePath(tableId)
                                    .ifPresent(tableIdByPath::remove);
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
                        if (partitionId == DELETED_PARTITION_ID) {
                            Long removedPartitionId = partitionIdByPath.remove(physicalTablePath);
                            if (removedPartitionId != null) {
                                bucketMetadataMapForPartitions.remove(removedPartitionId);
                            }
                        } else if (partitionName.equals(DELETED_PARTITION_NAME)) {
                            serverMetadataSnapshot
                                    .getPhysicalTablePath(partitionId)
                                    .ifPresent(partitionIdByPath::remove);
                            bucketMetadataMapForPartitions.remove(partitionId);
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

                    // 5. apply partition tombstones (P3T6). Each entry replaces the cached
                    // tombstone for the given main table id.
                    Map<Long, PartitionTombstone> tombstones =
                            clusterMetadata.getPartitionTombstones();
                    if (tombstones != null && !tombstones.isEmpty()) {
                        for (Map.Entry<Long, PartitionTombstone> e : tombstones.entrySet()) {
                            updatePartitionTombstone(e.getKey(), e.getValue());
                        }
                    }

                    // 6. (P3T12) on first non-empty metadata after a TabletServer restart, rebuild
                    // tombstones from ZK for the just-populated tables. The Coordinator only ships
                    // tombstones in {@code UpdateMetadataRequest} when it advances them; without
                    // this seed, a freshly-started TabletServer would not know about partitions
                    // that were dropped before it came up until the Coordinator next advances a
                    // tombstone. Idempotent: guarded by {@link #tombstonesSeededFromZk}.
                    ZooKeeperClient zk = zkClientForTombstoneSeed;
                    if (zk != null
                            && !clusterMetadata.getTableMetadataList().isEmpty()
                            && tombstonesSeededFromZk.compareAndSet(false, true)) {
                        List<TableInfo> tableInfos =
                                new ArrayList<>(clusterMetadata.getTableMetadataList().size());
                        for (TableMetadata tm : clusterMetadata.getTableMetadataList()) {
                            tableInfos.add(tm.getTableInfo());
                        }
                        seedPartitionTombstonesFromZk(zk, tableInfos);
                    }
                });

        // 7. (P3T14) Notify the surrounding ReplicaManager so any Replica whose IndexPushScheduler
        // init was deferred because its auto-derived Index Table had not yet propagated to this
        // cache can retry now that the cache has been refreshed. Fired OUTSIDE the metadataLock so
        // the retry callback (which may iterate replicas) does not contend with concurrent cache
        // updates. {@code null} when no ReplicaManager is wired (test fixtures that exercise the
        // cache in isolation).
        Runnable hook = replicaRetryHook;
        if (hook != null) {
            try {
                hook.run();
            } catch (Throwable t) {
                LOG.warn(
                        "Replica retry hook fired after updateClusterMetadata threw; ignoring so"
                                + " metadata propagation continues.",
                        t);
            }
        }
    }

    /**
     * Wires the {@link ReplicaManager}-supplied retry callback fired at the end of every {@link
     * #updateClusterMetadata}. Used to re-attempt {@code IndexPushScheduler} construction for
     * leader replicas whose auto-derived Index Table had not yet shown up in the cache (FIP V2 §3 +
     * P3T14). Idempotent — calling again replaces the previous hook.
     */
    public void setReplicaRetryHook(@Nullable Runnable hook) {
        this.replicaRetryHook = hook;
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

    @VisibleForTesting
    public ServerSchemaCache getServerSchemaCache() {
        return serverSchemaCache;
    }

    /**
     * Returns the cached {@link PartitionTombstone} for the given main table id, or {@link
     * PartitionTombstone#EMPTY} if the cache has not yet observed a tombstone for that table.
     * Cache-only — does NOT consult ZK.
     */
    public PartitionTombstone getPartitionTombstone(long mainTableId) {
        PartitionTombstone t = partitionTombstones.get(mainTableId);
        return t == null ? PartitionTombstone.EMPTY : t;
    }

    /**
     * Updates the cached tombstone for the given main table id. Called from the metadata
     * propagation receiver (P3T6).
     */
    public void updatePartitionTombstone(long mainTableId, PartitionTombstone tombstone) {
        partitionTombstones.put(mainTableId, checkNotNull(tombstone, "tombstone"));
    }

    /**
     * Wires a {@link ZooKeeperClient} that the cache can use to rebuild {@link
     * #partitionTombstones} from ZK on the first non-empty cluster-metadata update (P3T12). Called
     * by {@link TabletServer#startServices()}; tests that exercise the seed directly skip this
     * setter and pass a real ZK client to {@link #seedPartitionTombstonesFromZk(ZooKeeperClient,
     * Collection)} themselves.
     */
    public void setZooKeeperClient(ZooKeeperClient zkClient) {
        this.zkClientForTombstoneSeed = zkClient;
    }

    /**
     * Reseeds the in-memory partition-tombstone cache from ZK for the given tables (P3T12). On a
     * TabletServer restart, the Coordinator does not proactively re-broadcast historical tombstones
     * — it only ships them in {@code UpdateMetadataRequest} when it advances them. Without this
     * seed, newly-pushed PutKv records targeting an already-tombstoned partition could land before
     * the apply-path filter knows to drop them.
     *
     * <p>For each table:
     *
     * <ul>
     *   <li>Index Tables are skipped — only main tables carry tombstones.
     *   <li>Non-partitioned tables are skipped — partition tombstones only apply to partitioned
     *       tables.
     *   <li>{@link PartitionTombstone#EMPTY} (the no-op value returned by {@link
     *       ZooKeeperClient#getPartitionTombstone(TablePath)} when the znode is absent) is skipped
     *       to avoid clobbering any concurrently-applied non-empty cache entry.
     *   <li>Per-table failures are logged at WARN and seeding continues — startup must not crash on
     *       a single corrupted znode.
     * </ul>
     *
     * <p>Idempotent: calling multiple times is safe. Each call re-reads ZK and overwrites the
     * cached value, which is desired since {@link #updatePartitionTombstone(long,
     * PartitionTombstone)} is the canonical update primitive and ZK reflects the latest version.
     */
    public void seedPartitionTombstonesFromZk(
            ZooKeeperClient zkClient, Collection<TableInfo> tables) {
        checkNotNull(zkClient, "zkClient");
        checkNotNull(tables, "tables");
        int seededCount = 0;
        for (TableInfo tableInfo : tables) {
            if (tableInfo.isIndexTable()) {
                continue;
            }
            if (!tableInfo.isPartitioned()) {
                continue;
            }
            TablePath tablePath = tableInfo.getTablePath();
            try {
                PartitionTombstone tombstone = zkClient.getPartitionTombstone(tablePath);
                if (!PartitionTombstone.EMPTY.equals(tombstone)) {
                    updatePartitionTombstone(tableInfo.getTableId(), tombstone);
                    seededCount++;
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to seed partition tombstone from ZK for table {} (tableId={}); "
                                + "continuing startup.",
                        tablePath,
                        tableInfo.getTableId(),
                        e);
            }
        }
        if (seededCount > 0) {
            LOG.info(
                    "Seeded {} partition tombstone(s) from ZK on TabletServer startup.",
                    seededCount);
        }
    }
}
