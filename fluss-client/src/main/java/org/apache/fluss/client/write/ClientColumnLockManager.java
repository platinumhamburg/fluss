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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AcquireColumnLockRequest;
import org.apache.fluss.rpc.messages.ReleaseColumnLockRequest;
import org.apache.fluss.rpc.messages.RenewColumnLockRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client-side column lock manager for write operations.
 *
 * <p>This class handles acquiring, renewing, and releasing column locks for tables or partitions.
 * It maintains a background thread that automatically renews locks before they expire.
 *
 * <p>Locks are table/partition-scoped, not bucket-scoped. A lock acquired for a table/partition
 * applies to the specified columns (or all columns if none specified) across all buckets in that
 * table/partition.
 */
@ThreadSafe
@Internal
public class ClientColumnLockManager {
    private static final Logger LOG = LoggerFactory.getLogger(ClientColumnLockManager.class);

    private static final String LOCK_RENEW_THREAD_NAME = "fluss-lock-renewer";
    // 30 seconds default TTL
    private static final long DEFAULT_LOCK_TTL_MS = 30000;
    // Check every 5 seconds
    private static final long RENEWAL_CHECK_INTERVAL_MS = 5000;
    // Sentinel value for non-partitioned tables
    private static final long NON_PARTITIONED_TABLE_PARTITION_ID = -1L;

    private final MetadataUpdater metadataUpdater;
    private final Configuration configuration;
    private final Map<LockKey, LockInfo> activeLocks;
    private final ScheduledExecutorService renewalExecutor;
    private volatile boolean closed = false;

    public ClientColumnLockManager(MetadataUpdater metadataUpdater, Configuration configuration) {
        this.metadataUpdater = metadataUpdater;
        this.configuration = configuration;
        this.activeLocks = MapUtils.newConcurrentHashMap();
        this.renewalExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, LOCK_RENEW_THREAD_NAME);
                            t.setDaemon(true);
                            return t;
                        });

        // Start the renewal task
        renewalExecutor.scheduleAtFixedRate(
                this::renewLocks,
                RENEWAL_CHECK_INTERVAL_MS,
                RENEWAL_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Lock key that uniquely identifies a column lock. Includes table/partition and column indexes
     * to support fine-grained column-level locking.
     *
     * <p>This is used by writers to identify and release their locks.
     */
    public static class LockKey {
        private final TablePartition tablePartition;
        private final String columnSetKey; // null means all columns

        LockKey(TablePartition tablePartition, @Nullable int[] columnIndexes) {
            this.tablePartition = tablePartition;
            this.columnSetKey = columnIndexes == null ? null : Arrays.toString(columnIndexes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LockKey lockKey = (LockKey) o;
            return tablePartition.equals(lockKey.tablePartition)
                    && Objects.equals(columnSetKey, lockKey.columnSetKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tablePartition, columnSetKey);
        }

        @Override
        public String toString() {
            return "LockKey{"
                    + "tablePartition="
                    + tablePartition
                    + ", columns="
                    + (columnSetKey == null ? "all" : columnSetKey)
                    + '}';
        }
    }

    /** Internal class to hold lock state information. */
    private static class LockInfo {
        private final TablePartition tablePartition;
        private final String ownerId;
        private final int schemaId;
        private final @Nullable int[] columnIndexes;
        private final long ttlMs;
        private final long acquiredTimeMs;
        private volatile long lastRenewTimeMs;
        // Store all TabletServer IDs that have been locked
        private final List<Integer> lockedServerIds;

        LockInfo(
                TablePartition tablePartition,
                List<Integer> lockedServerIds,
                String ownerId,
                int schemaId,
                @Nullable int[] columnIndexes,
                long ttlMs,
                long acquiredTimeMs) {
            this.tablePartition = tablePartition;
            this.lockedServerIds = lockedServerIds;
            this.ownerId = ownerId;
            this.schemaId = schemaId;
            this.columnIndexes = columnIndexes;
            this.ttlMs = ttlMs;
            this.acquiredTimeMs = acquiredTimeMs;
            this.lastRenewTimeMs = acquiredTimeMs;
        }

        public TablePartition getTablePartition() {
            return tablePartition;
        }

        public List<Integer> getLockedServerIds() {
            return lockedServerIds;
        }

        void updateRenewTime(long renewTimeMs) {
            this.lastRenewTimeMs = renewTimeMs;
        }

        boolean needsRenewal(long currentTimeMs) {
            return (currentTimeMs - lastRenewTimeMs) > (ttlMs / 2);
        }

        boolean isExpired(long currentTimeMs) {
            return (currentTimeMs - lastRenewTimeMs) > ttlMs;
        }

        @Override
        public String toString() {
            return "LockInfo{"
                    + "tablePartition="
                    + tablePartition
                    + ", lockedServers="
                    + lockedServerIds.size()
                    + ", ownerId='"
                    + ownerId
                    + '\''
                    + ", schemaId="
                    + schemaId
                    + ", columnIndexes="
                    + (columnIndexes == null ? "all" : columnIndexes.length)
                    + ", ttlMs="
                    + ttlMs
                    + ", acquiredTimeMs="
                    + acquiredTimeMs
                    + ", lastRenewTimeMs="
                    + lastRenewTimeMs
                    + '}';
        }
    }

    /**
     * Acquire a column lock for the specified table or partition.
     *
     * <p>Note: Column locks are table/partition-scoped, not bucket-scoped. The lock acquired
     * applies to all buckets in the specified table or partition.
     *
     * <p>This method acquires locks on ALL TabletServers in the cluster in sequential order
     * (ordered by server ID) to ensure global consistency.
     *
     * @param tableId the table ID
     * @param partitionId the partition ID, null for non-partitioned tables
     * @param ownerId the unique identifier of the lock owner
     * @param schemaId the schema id that the column indexes belong to
     * @param columnIndexes the column indexes to lock, null means lock all columns
     * @param ttlMs the Time-To-Live for the lock, null to use default
     * @return a CompletableFuture that completes with the LockKey when lock is acquired on all
     *     TabletServers
     */
    public CompletableFuture<LockKey> acquireLock(
            long tableId,
            @Nullable Long partitionId,
            String ownerId,
            int schemaId,
            @Nullable int[] columnIndexes,
            @Nullable Long ttlMs) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("ClientColumnLockManager has been closed"));
        }

        long effectiveTtl = ttlMs != null ? ttlMs : DEFAULT_LOCK_TTL_MS;

        // Create TablePartition (locks are table/partition-scoped)
        long effectivePartitionId =
                partitionId != null ? partitionId : NON_PARTITIONED_TABLE_PARTITION_ID;
        TablePartition tablePartition = new TablePartition(tableId, effectivePartitionId);
        LockKey lockKey = new LockKey(tablePartition, columnIndexes);

        // Get all alive TabletServers in the cluster
        Map<Integer, ServerNode> allTabletServers =
                metadataUpdater.getCluster().getAliveTabletServers();
        if (allTabletServers.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No alive TabletServers found in cluster"));
        }

        // Sort server IDs to ensure consistent ordering
        List<Integer> sortedServerIds = new ArrayList<>(allTabletServers.keySet());
        Collections.sort(sortedServerIds);

        LOG.info(
                "Acquiring column lock for table {} partition {} on {} TabletServers: {}",
                tableId,
                partitionId,
                sortedServerIds.size(),
                sortedServerIds);

        // Build RPC request for table/partition-level lock
        AcquireColumnLockRequest request = new AcquireColumnLockRequest();
        request.setTableId(tableId);
        if (partitionId != null) {
            request.setPartitionId(partitionId);
        }
        request.setOwnerId(ownerId);
        request.setSchemaId(schemaId);
        request.setTtlMs(effectiveTtl);

        if (columnIndexes != null && columnIndexes.length > 0) {
            for (int columnIndex : columnIndexes) {
                request.addColumnIndexe(columnIndex);
            }
        }

        // Acquire locks on all TabletServers sequentially
        CompletableFuture<Void> lockFuture = CompletableFuture.completedFuture(null);
        List<Integer> acquiredServerIds = new ArrayList<>();

        for (Integer serverId : sortedServerIds) {
            lockFuture =
                    lockFuture.thenCompose(
                            v -> {
                                TabletServerGateway gateway =
                                        metadataUpdater.newTabletServerClientForNode(serverId);
                                if (gateway == null) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalStateException(
                                                    "TabletServer gateway not found for server: "
                                                            + serverId));
                                }

                                return gateway.acquireColumnLock(request)
                                        .thenApply(
                                                response -> {
                                                    if (response.hasErrorCode()) {
                                                        Errors error =
                                                                Errors.forCode(
                                                                        response.getErrorCode());
                                                        String errorMessage =
                                                                response.hasErrorMessage()
                                                                        ? response.getErrorMessage()
                                                                        : String.format(
                                                                                "Failed to acquire column lock on server %d",
                                                                                serverId);
                                                        throw error.exception(errorMessage);
                                                    }
                                                    acquiredServerIds.add(serverId);
                                                    LOG.debug(
                                                            "Acquired column lock on TabletServer {}",
                                                            serverId);
                                                    return null;
                                                });
                            });
        }

        return lockFuture.thenApply(
                v -> {
                    long acquiredTime = System.currentTimeMillis();
                    LockInfo lockInfo =
                            new LockInfo(
                                    tablePartition,
                                    acquiredServerIds,
                                    ownerId,
                                    schemaId,
                                    columnIndexes,
                                    effectiveTtl,
                                    acquiredTime);

                    activeLocks.put(lockKey, lockInfo);
                    LOG.info(
                            "Acquired column lock on all TabletServers: {}, lockKey: {}",
                            lockInfo,
                            lockKey);
                    return lockKey;
                });
    }

    /**
     * Release a column lock identified by the lock key.
     *
     * <p>Note: Column locks are table/partition-scoped. This will release the lock on all
     * TabletServers for the specified columns.
     *
     * @param lockKey the lock key obtained from acquireLock
     * @param ownerId the owner ID of the lock
     * @return a CompletableFuture that completes when the lock is released on all TabletServers
     */
    public CompletableFuture<Void> releaseLock(LockKey lockKey, String ownerId) {
        LockInfo lockInfo = activeLocks.remove(lockKey);
        if (lockInfo == null) {
            LOG.warn("Attempted to release non-existent lock for lockKey: {}", lockKey);
            return CompletableFuture.completedFuture(null);
        }

        if (!lockInfo.ownerId.equals(ownerId)) {
            LOG.warn(
                    "Attempted to release lock with mismatched owner ID. Expected: {}, Got: {}",
                    lockInfo.ownerId,
                    ownerId);
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Lock owner ID mismatch"));
        }

        TablePartition tablePartition = lockInfo.getTablePartition();

        // Build RPC request for table/partition-level lock
        ReleaseColumnLockRequest request = new ReleaseColumnLockRequest();
        request.setTableId(tablePartition.getTableId());
        if (tablePartition.getPartitionId() != NON_PARTITIONED_TABLE_PARTITION_ID) {
            request.setPartitionId(tablePartition.getPartitionId());
        }
        request.setOwnerId(ownerId);

        // Release locks on all TabletServers that were locked
        List<CompletableFuture<Void>> releaseFutures = new ArrayList<>();
        for (Integer serverId : lockInfo.getLockedServerIds()) {
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
            if (gateway == null) {
                LOG.warn("TabletServer gateway not found for server: {}", serverId);
                continue;
            }

            CompletableFuture<Void> releaseFuture =
                    gateway.releaseColumnLock(request)
                            .handle(
                                    (response, error) -> {
                                        if (error != null) {
                                            LOG.warn(
                                                    "Failed to release lock on server {}: {}",
                                                    serverId,
                                                    error.getMessage());
                                        } else {
                                            LOG.debug(
                                                    "Released column lock on TabletServer {}",
                                                    serverId);
                                        }
                                        return null;
                                    });
            releaseFutures.add(releaseFuture);
        }

        // Wait for all release operations to complete
        return CompletableFuture.allOf(releaseFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            LOG.info("Released column lock on all TabletServers: {}", lockInfo);
                            return null;
                        });
    }

    /**
     * Renew all active locks that need renewal.
     *
     * <p>This method is called periodically by the background renewal thread.
     */
    private void renewLocks() {
        if (closed || activeLocks.isEmpty()) {
            return;
        }

        long currentTime = System.currentTimeMillis();

        for (Map.Entry<LockKey, LockInfo> entry : activeLocks.entrySet()) {
            LockInfo lockInfo = entry.getValue();

            if (lockInfo.needsRenewal(currentTime)) {
                renewLock(lockInfo)
                        .whenComplete(
                                (result, error) -> {
                                    if (error != null) {
                                        LOG.error("Failed to renew lock: {}", lockInfo, error);
                                        // Remove the lock if renewal fails
                                        activeLocks.remove(entry.getKey());
                                    } else {
                                        LOG.debug("Successfully renewed lock: {}", lockInfo);
                                    }
                                });
            } else if (lockInfo.isExpired(currentTime)) {
                LOG.warn("Lock has expired: {}", lockInfo);
                activeLocks.remove(entry.getKey());
            }
        }
    }

    /**
     * Renew a specific lock.
     *
     * <p>This method renews the lock on all TabletServers that were locked.
     *
     * @param lockInfo the lock info to renew
     * @return a CompletableFuture that completes when the lock is renewed on all TabletServers
     */
    private CompletableFuture<Void> renewLock(LockInfo lockInfo) {
        TablePartition tablePartition = lockInfo.getTablePartition();

        // Build RPC request for table/partition-level lock
        RenewColumnLockRequest request = new RenewColumnLockRequest();
        request.setTableId(tablePartition.getTableId());
        if (tablePartition.getPartitionId() != NON_PARTITIONED_TABLE_PARTITION_ID) {
            request.setPartitionId(tablePartition.getPartitionId());
        }
        request.setOwnerId(lockInfo.ownerId);

        // Renew locks on all TabletServers that were locked
        List<CompletableFuture<Void>> renewFutures = new ArrayList<>();
        for (Integer serverId : lockInfo.getLockedServerIds()) {
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
            if (gateway == null) {
                LOG.warn("TabletServer gateway not found for server {} during renewal", serverId);
                continue;
            }

            CompletableFuture<Void> renewFuture =
                    gateway.renewColumnLock(request)
                            .thenApply(
                                    response -> {
                                        LOG.debug(
                                                "Renewed column lock on TabletServer {}", serverId);
                                        return (Void) null;
                                    })
                            .exceptionally(
                                    error -> {
                                        LOG.warn(
                                                "Failed to renew lock on TabletServer {}: {}",
                                                serverId,
                                                error.getMessage());
                                        return (Void) null;
                                    });
            renewFutures.add(renewFuture);
        }

        // Wait for all renewal operations to complete
        return CompletableFuture.allOf(renewFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            long renewTime = System.currentTimeMillis();
                            lockInfo.updateRenewTime(renewTime);
                            LOG.debug("Renewed column lock on all TabletServers: {}", lockInfo);
                            return null;
                        });
    }

    /** Close the lock manager and release all locks. */
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        // Shutdown the renewal executor
        renewalExecutor.shutdown();
        try {
            if (!renewalExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                renewalExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            renewalExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Release all active locks
        for (Map.Entry<LockKey, LockInfo> entry : activeLocks.entrySet()) {
            try {
                LockKey lockKey = entry.getKey();
                LockInfo lockInfo = entry.getValue();
                releaseLock(lockKey, lockInfo.ownerId).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn("Failed to release lock during close: {}", entry.getValue(), e);
            }
        }

        activeLocks.clear();
        LOG.info("ClientColumnLockManager closed");
    }
}
