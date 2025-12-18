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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.client.utils.ClientUtils;
import org.apache.fluss.client.utils.MetadataUtils;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.utils.ExceptionUtils.stripExecutionException;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * The updater to initialize and update client metadata.
 *
 * <p>This class manages metadata updates with batching and deduplication to avoid request storms.
 * Inspired by {@code AdjustIsrManager}, it uses:
 *
 * <ul>
 *   <li>Request deduplication: Multiple concurrent requests for the same resource are merged
 *   <li>Batch processing: All pending requests are sent together in one RPC call
 *   <li>Single in-flight request: Only one metadata request can be in-flight at a time
 *   <li>Asynchronous API: Provides CompletableFuture-based interface to avoid blocking
 * </ul>
 */
public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final int MAX_RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final ScheduledExecutorService scheduler;
    private final Set<Integer> unavailableTabletServerIds = new CopyOnWriteArraySet<>();

    /** The current cluster state, protected by read-write lock. */
    @GuardedBy("clusterRWLock")
    protected Cluster cluster;

    /** Read-write lock for cluster access. */
    protected final ReadWriteLock clusterRWLock = new ReentrantReadWriteLock();

    /**
     * Pending metadata update requests. Maps each atomic resource to the list of futures waiting
     * for it. Multiple requests for the same resource share the same update. Visible for testing.
     */
    @VisibleForTesting
    protected final Map<MetadataResourceKey, List<CompletableFuture<Void>>> pendingRequests =
            MapUtils.newConcurrentHashMap();

    /** Used to allow only one in-flight request at a time. */
    private final AtomicBoolean inflightRequest = new AtomicBoolean(false);

    public MetadataUpdater(Configuration conf, RpcClient rpcClient) {
        this(rpcClient, conf, initializeCluster(conf, rpcClient));
    }

    @VisibleForTesting
    public MetadataUpdater(RpcClient rpcClient, Configuration conf, Cluster cluster) {
        this.rpcClient = rpcClient;
        this.conf = conf;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "metadata-updater-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
        this.cluster = cluster;
    }

    // ========== Helper Methods ==========

    /** Await a CompletableFuture and convert exceptions appropriately for metadata operations. */
    private void awaitFuture(CompletableFuture<Void> future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            // Preserve PartitionNotExistException for callers to handle
            if (t instanceof PartitionNotExistException) {
                throw (PartitionNotExistException) t;
            }
            // For retriable exceptions, log warning but don't throw
            else if (t instanceof RetriableException || t instanceof TimeoutException) {
                LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
            }
            // For other exceptions, wrap in FlussRuntimeException
            else {
                throw new FlussRuntimeException("Failed to update metadata", t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while updating metadata", e);
        }
    }

    // ========== Cluster Metadata Access Methods ==========

    public Cluster getCluster() {
        return inReadLock(clusterRWLock, () -> cluster);
    }

    public @Nullable ServerNode getCoordinatorServer() {
        return getCluster().getCoordinatorServer();
    }

    public @Nullable ServerNode getRandomTabletServer() {
        return getCluster().getRandomTabletServer();
    }

    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return getCluster().getPartitionId(physicalTablePath);
    }

    public Long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
        return getCluster().getPartitionIdOrElseThrow(physicalTablePath);
    }

    public Optional<BucketLocation> getBucketLocation(TableBucket tableBucket) {
        return getCluster().getBucketLocation(tableBucket);
    }

    public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
        Integer serverNode = getCluster().leaderFor(tableBucket);
        if (serverNode == null) {
            for (int i = 0; i < MAX_RETRY_TIMES; i++) {
                // check if bucket is for a partition
                if (tableBucket.getPartitionId() != null) {
                    updateMetadata(
                            Collections.singleton(tablePath),
                            null,
                            Collections.singleton(tableBucket.getPartitionId()));
                } else {
                    updateMetadata(Collections.singleton(tablePath), null, null);
                }
                serverNode = getCluster().leaderFor(tableBucket);
                if (serverNode != null) {
                    break;
                }
            }

            if (serverNode == null) {
                throw new FlussRuntimeException(
                        "Leader not found after retry  "
                                + MAX_RETRY_TIMES
                                + " times for table bucket: "
                                + tableBucket);
            }
        }

        return serverNode;
    }

    public Set<PhysicalTablePath> getPhysicalTablePathByIds(
            @Nullable Collection<Long> tableId,
            @Nullable Collection<TablePartition> tablePartitions) {
        Set<PhysicalTablePath> physicalTablePaths = new HashSet<>();
        Cluster currentCluster = getCluster();

        if (tableId != null) {
            tableId.forEach(
                    id ->
                            currentCluster
                                    .getTablePath(id)
                                    .ifPresent(
                                            p -> physicalTablePaths.add(PhysicalTablePath.of(p))));
        }

        if (tablePartitions != null) {
            for (TablePartition tablePartition : tablePartitions) {
                currentCluster
                        .getTablePath(tablePartition.getTableId())
                        .ifPresent(
                                path -> {
                                    Optional<String> optPartition =
                                            currentCluster.getPartitionName(
                                                    tablePartition.getPartitionId());
                                    optPartition.ifPresent(
                                            p ->
                                                    physicalTablePaths.add(
                                                            PhysicalTablePath.of(path, p)));
                                });
            }
        }
        return physicalTablePaths;
    }

    // ========== RPC Client Creation Methods ==========

    public CoordinatorGateway newCoordinatorServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    public TabletServerGateway newRandomTabletServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getRandomTabletServer, rpcClient, TabletServerGateway.class);
    }

    public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
        @Nullable final ServerNode serverNode = getTabletServer(serverId);
        if (serverNode == null) {
            return null;
        } else {
            return GatewayClientProxy.createGatewayProxy(
                    () -> serverNode, rpcClient, TabletServerGateway.class);
        }
    }

    private @Nullable ServerNode getTabletServer(int id) {
        return getCluster().getTabletServer(id);
    }

    // ========== Metadata Update Methods (Synchronous) ==========

    /**
     * Check and update table metadata if needed. Only updates tables that are not present in cache.
     */
    public void checkAndUpdateTableMetadata(Set<TablePath> tablePaths) {
        awaitFuture(checkAndUpdateTableMetadataAsync(tablePaths));
    }

    /**
     * Check and update partition metadata. Returns whether partition exists after update.
     *
     * @throws PartitionNotExistException if partition does not exist after update
     */
    public boolean checkAndUpdatePartitionMetadata(PhysicalTablePath physicalTablePath)
            throws PartitionNotExistException {
        Cluster currentCluster = getCluster();
        if (!currentCluster.getPartitionId(physicalTablePath).isPresent()) {
            updateMetadata(null, Collections.singleton(physicalTablePath), null);
        }
        return getCluster().getPartitionId(physicalTablePath).isPresent();
    }

    /**
     * Check and update partition metadata by partition IDs.
     *
     * <p>Note: Assumes the partition IDs belong to the given table.
     */
    public void checkAndUpdatePartitionMetadata(
            TablePath tablePath, Collection<Long> partitionIds) {
        Cluster currentCluster = getCluster();
        Set<Long> needUpdatePartitionIds = new HashSet<>();
        for (Long partitionId : partitionIds) {
            if (!currentCluster.getPartitionName(partitionId).isPresent()) {
                needUpdatePartitionIds.add(partitionId);
            }
        }

        if (!needUpdatePartitionIds.isEmpty()) {
            updateMetadata(Collections.singleton(tablePath), null, needUpdatePartitionIds);
        }
    }

    /**
     * Check and update metadata for a table bucket. Updates either table or partition metadata
     * based on bucket type.
     */
    public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        } else {
            checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(tableBucket.getPartitionId()));
        }
    }

    /** Update table or partition metadata. Forces a metadata update regardless of cache state. */
    public void updateTableOrPartitionMetadata(TablePath tablePath, @Nullable Long partitionId) {
        Collection<Long> partitionIds =
                partitionId == null ? null : Collections.singleton(partitionId);
        updateMetadata(Collections.singleton(tablePath), null, partitionIds);
    }

    /** Update physical table metadata. Separates tables and partitions and updates accordingly. */
    public void updatePhysicalTableMetadata(Set<PhysicalTablePath> physicalTablePaths) {
        Set<TablePath> updateTablePaths = new HashSet<>();
        Set<PhysicalTablePath> updatePartitionPath = new HashSet<>();
        for (PhysicalTablePath physicalTablePath : physicalTablePaths) {
            if (physicalTablePath.getPartitionName() == null) {
                updateTablePaths.add(physicalTablePath.getTablePath());
            } else {
                updatePartitionPath.add(physicalTablePath);
            }
        }
        updateMetadata(updateTablePaths, updatePartitionPath, null);
    }

    /**
     * Update metadata for tables, partitions, and partition IDs. This is the main synchronous
     * update method.
     */
    @VisibleForTesting
    public void updateMetadata(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds)
            throws PartitionNotExistException {
        awaitFuture(updateMetadataAsync(tablePaths, tablePartitionNames, tablePartitionIds));
    }

    /** Invalidate bucket metadata for given physical table paths. */
    public void invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTablesToInvalid) {
        if (!physicalTablesToInvalid.isEmpty()) {
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster = cluster.invalidPhysicalTableBucketMeta(physicalTablesToInvalid);
                        return null;
                    });
        }
    }

    // ========== Metadata Update Methods (Asynchronous) ==========

    /**
     * Async version of checkAndUpdateTableMetadata. Only updates tables that are not present in
     * cache.
     */
    private CompletableFuture<Void> checkAndUpdateTableMetadataAsync(Set<TablePath> tablePaths) {
        Cluster currentCluster = getCluster();
        Set<TablePath> needUpdateTablePaths =
                tablePaths.stream()
                        .filter(tablePath -> !currentCluster.getTable(tablePath).isPresent())
                        .collect(Collectors.toSet());
        if (needUpdateTablePaths.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Submit each table as atomic resource
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (TablePath tablePath : needUpdateTablePaths) {
            futures.add(submitTableUpdateAsync(tablePath));
        }

        // Wait for all to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Async version that checks and updates partition metadata, returning the partition ID.
     *
     * <p>Uses request batching and deduplication to avoid metadata request storms.
     */
    public CompletableFuture<Long> checkAndUpdatePartitionMetadataAsync(
            PhysicalTablePath physicalTablePath) {
        // First check if partition already exists in current cluster (with read lock)
        Cluster currentCluster = getCluster();
        if (currentCluster.getPartitionId(physicalTablePath).isPresent()) {
            return CompletableFuture.completedFuture(
                    currentCluster.getPartitionIdOrElseThrow(physicalTablePath));
        }

        // Not in cache, need to fetch from server with batching
        return submitPartitionUpdateAsync(physicalTablePath)
                .thenApply(
                        v -> {
                            return inReadLock(
                                    clusterRWLock,
                                    () -> {
                                        if (cluster.getPartitionId(physicalTablePath).isPresent()) {
                                            return cluster.getPartitionIdOrElseThrow(
                                                    physicalTablePath);
                                        } else {
                                            throw new FlussRuntimeException(
                                                    new PartitionNotExistException(
                                                            "Partition does not exist after update: "
                                                                    + physicalTablePath));
                                        }
                                    });
                        });
    }

    // ========== Internal Batching and Request Management ==========

    /**
     * Submit a request to update metadata for a single table. Returns a future that completes when
     * the table metadata is updated.
     */
    private CompletableFuture<Void> submitTableUpdateAsync(TablePath tablePath) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        MetadataResourceKey key = MetadataResourceKey.forTable(tablePath);

        pendingRequests.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(future);

        maybePropagateMetadataUpdate();
        return future;
    }

    /**
     * Submit a request to update metadata for a single partition. Returns a future that completes
     * when the partition metadata is updated.
     */
    private CompletableFuture<Void> submitPartitionUpdateAsync(PhysicalTablePath partition) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        MetadataResourceKey key = MetadataResourceKey.forPartition(partition);

        pendingRequests.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(future);

        maybePropagateMetadataUpdate();
        return future;
    }

    /**
     * Submit a request to update metadata for a single partition ID. Returns a future that
     * completes when the partition metadata is updated.
     */
    private CompletableFuture<Void> submitPartitionIdUpdateAsync(Long partitionId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        MetadataResourceKey key = MetadataResourceKey.forPartitionId(partitionId);

        pendingRequests.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(future);

        maybePropagateMetadataUpdate();
        return future;
    }

    /** Trigger sending pending metadata requests if no request is currently in-flight. */
    private void maybePropagateMetadataUpdate() {
        // Send all pending items if there is not already a request in-flight
        if (!pendingRequests.isEmpty() && inflightRequest.compareAndSet(false, true)) {
            try {
                sendBatchedMetadataRequest();
            } catch (Throwable e) {
                // Handle any top-level exceptions
                LOG.error("Failed to send batched metadata request", e);
                clearInFlightRequest();
                // Retry in near future
                scheduler.schedule(this::maybePropagateMetadataUpdate, 50, TimeUnit.MILLISECONDS);
            }
        }
    }

    /** Aggregate all pending requests and send a single batched metadata request. */
    @VisibleForTesting
    protected void sendBatchedMetadataRequest() {
        // Take snapshot of pending requests
        Map<MetadataResourceKey, List<CompletableFuture<Void>>> snapshot =
                new HashMap<>(pendingRequests);

        if (snapshot.isEmpty()) {
            clearInFlightRequest();
            return;
        }

        // Aggregate all atomic resources by type
        Set<TablePath> allTablePaths = new HashSet<>();
        Set<PhysicalTablePath> allPartitions = new HashSet<>();
        Set<Long> allPartitionIds = new HashSet<>();

        for (MetadataResourceKey key : snapshot.keySet()) {
            switch (key.type) {
                case TABLE:
                    allTablePaths.add(key.asTablePath());
                    break;
                case PARTITION:
                    allPartitions.add(key.asPhysicalTablePath());
                    break;
                case PARTITION_ID:
                    allPartitionIds.add(key.asPartitionId());
                    break;
            }
        }

        LOG.debug(
                "Sending batched metadata request: tables={}, partitions={}, partitionIds={}",
                allTablePaths.size(),
                allPartitions.size(),
                allPartitionIds.size());

        // Get gateway to an available tablet server
        Cluster currentSnapshot = getCluster();
        ServerNode serverNode =
                MetadataUtils.getOneAvailableTabletServerNode(
                        currentSnapshot, unavailableTabletServerIds);

        if (serverNode == null) {
            LOG.info(
                    "No available tablet server to update metadata, trying to re-initialize cluster");
            handleNoAvailableServer(snapshot);
            return;
        }

        // Create gateway and send request
        AdminReadOnlyGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> serverNode, rpcClient, AdminReadOnlyGateway.class);

        MetadataRequest metadataRequest =
                ClientRpcMessageUtils.makeMetadataRequest(
                        allTablePaths.isEmpty() ? null : allTablePaths,
                        allPartitions.isEmpty() ? null : allPartitions,
                        allPartitionIds.isEmpty() ? null : allPartitionIds);

        gateway.metadata(metadataRequest)
                .whenComplete(
                        (response, exception) -> {
                            try {
                                if (exception != null) {
                                    handleMetadataRequestError(snapshot, exception);
                                    // Mark this server as potentially unavailable
                                    Throwable t = stripExecutionException(exception);
                                    if (t instanceof RetriableException
                                            || t instanceof TimeoutException) {
                                        unavailableTabletServerIds.add(serverNode.id());
                                        LOG.warn(
                                                "Marking tablet server {} as unavailable due to error",
                                                serverNode.id(),
                                                t);
                                    }
                                    // Retry in near future
                                    scheduler.schedule(
                                            this::maybePropagateMetadataUpdate,
                                            50,
                                            TimeUnit.MILLISECONDS);
                                } else {
                                    handleMetadataResponse(response, snapshot);
                                }
                            } finally {
                                clearInFlightRequest();
                            }
                            // Try to send more pending requests if any
                            maybePropagateMetadataUpdate();
                        });
    }

    /** Handle the case where no tablet server is available. */
    private void handleNoAvailableServer(
            Map<MetadataResourceKey, List<CompletableFuture<Void>>> snapshot) {
        try {
            // Try to re-initialize cluster from bootstrap servers
            Cluster newCluster = initializeCluster(conf, rpcClient);
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster = newCluster;
                        return null;
                    });
            // Clear unavailable server set since we have fresh cluster info
            unavailableTabletServerIds.clear();

            // Retry immediately with new cluster
            clearInFlightRequest();
            maybePropagateMetadataUpdate();
        } catch (Exception e) {
            // Re-initialization failed, fail all pending requests
            handleMetadataRequestError(snapshot, e);
            clearInFlightRequest();
            // Schedule retry
            scheduler.schedule(this::maybePropagateMetadataUpdate, 100, TimeUnit.MILLISECONDS);
        }
    }

    /** Handle successful metadata response. */
    private void handleMetadataResponse(
            MetadataResponse response,
            Map<MetadataResourceKey, List<CompletableFuture<Void>>> snapshot) {
        try {
            // Update cluster with write lock (single writer)
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster =
                                MetadataUtils.mergeMetadataResponseIntoCluster(
                                        cluster, response, false);
                        return null;
                    });

            // Complete all futures successfully
            for (Map.Entry<MetadataResourceKey, List<CompletableFuture<Void>>> entry :
                    snapshot.entrySet()) {
                // Remove from pending map
                pendingRequests.remove(entry.getKey());
                // Complete all futures for this resource
                for (CompletableFuture<Void> future : entry.getValue()) {
                    future.complete(null);
                }
            }

            // Clear unavailable servers as we successfully communicated
            Map<Integer, ServerNode> aliveServers = getCluster().getAliveTabletServers();
            unavailableTabletServerIds.removeIf(aliveServers::containsKey);

        } catch (Exception e) {
            LOG.error("Failed to process metadata response", e);
            handleMetadataRequestError(snapshot, e);
        }
    }

    /** Handle metadata request error. */
    private void handleMetadataRequestError(
            Map<MetadataResourceKey, List<CompletableFuture<Void>>> snapshot, Throwable error) {
        LOG.warn("Metadata request failed, will retry", error);

        Throwable cause = stripExecutionException(error);

        // For non-retriable errors, fail the requests immediately
        if (!(cause instanceof RetriableException) && !(cause instanceof TimeoutException)) {
            for (Map.Entry<MetadataResourceKey, List<CompletableFuture<Void>>> entry :
                    snapshot.entrySet()) {
                // Remove from pending map
                pendingRequests.remove(entry.getKey());
                // Fail all futures for this resource
                for (CompletableFuture<Void> future : entry.getValue()) {
                    // Pass the original exception without wrapping to preserve exception type
                    // (e.g., PartitionNotExistException needs to be caught by callers)
                    future.completeExceptionally(cause);
                }
            }
        }
        // For retriable errors, keep items in the map for retry
        // They will be retried by the scheduled task
    }

    /** Clear the in-flight request flag. */
    @VisibleForTesting
    protected void clearInFlightRequest() {
        if (!inflightRequest.compareAndSet(true, false)) {
            LOG.warn("Attempting to clear in-flight flag when no request is in-flight");
        }
    }

    /**
     * Async version that updates metadata for tables, partitions, and partition IDs.
     *
     * <p>This is the core async implementation that uses request batching and deduplication to
     * avoid metadata request storms.
     */
    public CompletableFuture<Void> updateMetadataAsync(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds) {
        // Collect all futures for atomic resources
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Submit each table as atomic resource
        if (tablePaths != null) {
            for (TablePath tablePath : tablePaths) {
                futures.add(submitTableUpdateAsync(tablePath));
            }
        }

        // Submit each partition as atomic resource
        if (tablePartitionNames != null) {
            for (PhysicalTablePath partition : tablePartitionNames) {
                futures.add(submitPartitionUpdateAsync(partition));
            }
        }

        // Submit each partition ID as atomic resource
        if (tablePartitionIds != null) {
            for (Long partitionId : tablePartitionIds) {
                futures.add(submitPartitionIdUpdateAsync(partitionId));
            }
        }

        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Wait for all to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    // ========== Cluster Initialization Methods ==========

    /**
     * Initialize Cluster. This step just to get the coordinator server address and alive tablet
     * servers according to the config {@link ConfigOptions#BOOTSTRAP_SERVERS}.
     */
    private static Cluster initializeCluster(Configuration conf, RpcClient rpcClient) {
        List<InetSocketAddress> inetSocketAddresses =
                ClientUtils.parseAndValidateAddresses(conf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        Cluster cluster = null;
        Exception lastException = null;
        for (InetSocketAddress address : inetSocketAddresses) {
            ServerNode serverNode = null;
            try {
                serverNode =
                        new ServerNode(
                                -1,
                                address.getHostString(),
                                address.getPort(),
                                ServerType.COORDINATOR);
                ServerNode finalServerNode = serverNode;
                AdminReadOnlyGateway adminReadOnlyGateway =
                        GatewayClientProxy.createGatewayProxy(
                                () -> finalServerNode, rpcClient, AdminReadOnlyGateway.class);
                if (inetSocketAddresses.size() == 1) {
                    // if there is only one bootstrap server, we can retry to connect to it.
                    cluster =
                            tryToInitializeClusterWithRetries(
                                    rpcClient, serverNode, adminReadOnlyGateway, MAX_RETRY_TIMES);
                } else {
                    cluster = tryToInitializeCluster(adminReadOnlyGateway);
                    break;
                }
            } catch (Exception e) {
                // We should dis-connected with the bootstrap server id to make sure the next
                // retry can rebuild the connection.
                if (serverNode != null) {
                    rpcClient.disconnect(serverNode.uid());
                }
                LOG.error(
                        "Failed to initialize fluss client connection to bootstrap server: {}",
                        address,
                        e);
                lastException = e;
            }
        }

        if (cluster == null && lastException != null) {
            String errorMsg =
                    "Failed to initialize fluss client connection to bootstrap servers: "
                            + inetSocketAddresses
                            + ". \nReason: "
                            + lastException.getMessage();
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg, lastException);
        }

        return cluster;
    }

    @VisibleForTesting
    static @Nullable Cluster tryToInitializeClusterWithRetries(
            RpcClient rpcClient,
            ServerNode serverNode,
            AdminReadOnlyGateway gateway,
            int maxRetryTimes)
            throws Exception {
        int retryCount = 0;
        while (retryCount <= maxRetryTimes) {
            try {
                return tryToInitializeCluster(gateway);
            } catch (Exception e) {
                Throwable cause = stripExecutionException(e);
                // in case of bootstrap is recovering, we should retry to connect.
                if (!(cause instanceof StaleMetadataException
                                || cause instanceof NetworkException
                                || cause instanceof TimeoutException)
                        || retryCount >= maxRetryTimes) {
                    throw e;
                }

                // We should dis-connected with the bootstrap server id to make sure the next
                // retry can rebuild the connection.
                rpcClient.disconnect(serverNode.uid());

                long delayMs = (long) (RETRY_INTERVAL_MS * Math.pow(2, retryCount));
                LOG.warn(
                        "Failed to connect to bootstrap server: {} (retry {}/{}). Retrying in {} ms.",
                        serverNode,
                        retryCount + 1,
                        maxRetryTimes,
                        delayMs,
                        e);

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry sleep", ex);
                }
                retryCount++;
            }
        }

        return null;
    }

    private static Cluster tryToInitializeCluster(AdminReadOnlyGateway adminReadOnlyGateway)
            throws Exception {
        return sendMetadataRequestAndRebuildCluster(adminReadOnlyGateway, Collections.emptySet());
    }

    /**
     * Key to identify and deduplicate atomic metadata resources. Each key represents exactly one
     * resource (table, partition, or partition ID).
     */
    @VisibleForTesting
    protected static class MetadataResourceKey {
        protected final ResourceType type;
        protected final Object resource;

        /** Types of atomic metadata resources. */
        enum ResourceType {
            TABLE, // Single table
            PARTITION, // Single partition
            PARTITION_ID // Single partition ID
        }

        private MetadataResourceKey(ResourceType type, Object resource) {
            this.type = type;
            this.resource = resource;
        }

        static MetadataResourceKey forTable(TablePath tablePath) {
            return new MetadataResourceKey(ResourceType.TABLE, tablePath);
        }

        static MetadataResourceKey forPartition(PhysicalTablePath partition) {
            return new MetadataResourceKey(ResourceType.PARTITION, partition);
        }

        static MetadataResourceKey forPartitionId(Long partitionId) {
            return new MetadataResourceKey(ResourceType.PARTITION_ID, partitionId);
        }

        TablePath asTablePath() {
            return (TablePath) resource;
        }

        PhysicalTablePath asPhysicalTablePath() {
            return (PhysicalTablePath) resource;
        }

        Long asPartitionId() {
            return (Long) resource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetadataResourceKey that = (MetadataResourceKey) o;
            return type == that.type && resource.equals(that.resource);
        }

        @Override
        public int hashCode() {
            return 31 * type.hashCode() + resource.hashCode();
        }

        @Override
        public String toString() {
            return type + ":" + resource;
        }
    }
}
