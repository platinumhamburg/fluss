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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerSnapshot;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manager for producer offset snapshots lifecycle.
 *
 * <p>This manager handles:
 *
 * <ul>
 *   <li>Registering new producer snapshots with atomic "check and register" semantics
 *   <li>Retrieving producer snapshot offsets
 *   <li>Deleting producer snapshots
 *   <li>Periodic cleanup of expired snapshots
 *   <li>Cleanup of orphan files in remote storage
 * </ul>
 *
 * <p>The manager uses a background scheduler to periodically clean up expired snapshots and orphan
 * files. The cleanup interval and snapshot TTL are configurable.
 *
 * @see ProducerSnapshotStore for low-level storage operations
 */
public class ProducerSnapshotManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSnapshotManager.class);

    private final ProducerSnapshotStore snapshotStore;
    private final long defaultTtlMs;
    private final long cleanupIntervalMs;
    private final ScheduledExecutorService cleanupScheduler;

    public ProducerSnapshotManager(Configuration conf, ZooKeeperClient zkClient) {
        this(
                new ProducerSnapshotStore(zkClient, conf.getString(ConfigOptions.REMOTE_DATA_DIR)),
                conf.get(ConfigOptions.COORDINATOR_PRODUCER_SNAPSHOT_TTL).toMillis(),
                conf.get(ConfigOptions.COORDINATOR_PRODUCER_SNAPSHOT_CLEANUP_INTERVAL).toMillis());
    }

    @VisibleForTesting
    ProducerSnapshotManager(
            ProducerSnapshotStore snapshotStore, long defaultTtlMs, long cleanupIntervalMs) {
        this.snapshotStore = snapshotStore;
        this.defaultTtlMs = defaultTtlMs;
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.cleanupScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("producer-snapshot-cleanup"));
    }

    /** Starts the background cleanup task. */
    public void start() {
        cleanupScheduler.scheduleAtFixedRate(
                this::runCleanup, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "Started producer snapshot manager with TTL={} ms, cleanup interval={} ms",
                defaultTtlMs,
                cleanupIntervalMs);
    }

    // ------------------------------------------------------------------------
    //  Public API
    // ------------------------------------------------------------------------

    /**
     * Registers a new producer offset snapshot with atomic "check and register" semantics.
     *
     * <p>This method leverages ZooKeeper's atomic create operation to ensure that concurrent
     * requests are handled correctly:
     *
     * <ul>
     *   <li>Atomicity: The check-and-create is a single atomic ZK operation
     *   <li>Idempotency: Concurrent requests with same producerId will have exactly one succeed
     *       with CREATED, others return ALREADY_EXISTS
     * </ul>
     *
     * <p>For expired snapshots, this method first cleans up the old snapshot, then creates a new
     * one. Note that there's a small window where concurrent requests might both try to clean up
     * the expired snapshot, but only one will succeed in creating the new one.
     *
     * @param producerId the producer ID (typically Flink job ID)
     * @param offsets map of TableBucket to offset for all tables
     * @param ttlMs TTL in milliseconds for the snapshot, or null to use default
     * @return true if a new snapshot was created (CREATED), false if snapshot already existed
     *     (ALREADY_EXISTS)
     * @throws Exception if the operation fails
     */
    public boolean registerSnapshot(String producerId, Map<TableBucket, Long> offsets, Long ttlMs)
            throws Exception {
        long currentTimeMs = System.currentTimeMillis();
        long effectiveTtlMs = ttlMs != null ? ttlMs : defaultTtlMs;
        long expirationTime = currentTimeMs + effectiveTtlMs;

        // First, try to atomically create the snapshot
        // This handles the common case where no snapshot exists
        boolean created = snapshotStore.tryStoreSnapshot(producerId, offsets, expirationTime);
        if (created) {
            return true;
        }

        // Snapshot already exists - check if it's expired
        Optional<ProducerSnapshot> existingSnapshot = snapshotStore.getSnapshotMetadata(producerId);
        if (existingSnapshot.isPresent() && !existingSnapshot.get().isExpired(currentTimeMs)) {
            // Valid snapshot exists, return ALREADY_EXISTS
            LOG.info(
                    "Producer snapshot already exists for producer {} (expires at {}), "
                            + "returning ALREADY_EXISTS",
                    producerId,
                    existingSnapshot.get().getExpirationTime());
            return false;
        }

        // Snapshot is expired (or was deleted between our check), clean it up and retry
        if (existingSnapshot.isPresent()) {
            LOG.info(
                    "Found expired snapshot for producer {}, cleaning up before creating new one",
                    producerId);
            try {
                snapshotStore.deleteSnapshot(producerId);
            } catch (Exception e) {
                LOG.warn("Failed to cleanup expired snapshot for producer {}", producerId, e);
                // Continue with retry anyway - the delete might have succeeded partially
                // or another concurrent request might have already cleaned it up
            }
        }

        // Retry creating the snapshot after cleanup
        // Use atomic operation again to handle concurrent cleanup+create race
        created = snapshotStore.tryStoreSnapshot(producerId, offsets, expirationTime);
        if (created) {
            return true;
        }

        // Another concurrent request created a snapshot after our cleanup
        // Check if it's valid (not expired)
        existingSnapshot = snapshotStore.getSnapshotMetadata(producerId);
        if (existingSnapshot.isPresent() && !existingSnapshot.get().isExpired(currentTimeMs)) {
            LOG.info(
                    "Another request created snapshot for producer {} during our retry, "
                            + "returning ALREADY_EXISTS",
                    producerId);
            return false;
        }

        // This is an edge case: the newly created snapshot is already expired
        // (shouldn't happen in practice unless TTL is very short)
        // Just return false to avoid infinite retry loop
        LOG.warn(
                "Failed to create snapshot for producer {} after retry, "
                        + "concurrent requests may have created and expired it",
                producerId);
        return false;
    }

    /**
     * Gets the snapshot metadata for a producer.
     *
     * @param producerId the producer ID
     * @return Optional containing the snapshot if exists
     * @throws Exception if the operation fails
     */
    public Optional<ProducerSnapshot> getSnapshotMetadata(String producerId) throws Exception {
        return snapshotStore.getSnapshotMetadata(producerId);
    }

    /**
     * Reads all offsets for a producer.
     *
     * @param producerId the producer ID
     * @return map of TableBucket to offset, or empty map if no snapshot exists
     * @throws Exception if the operation fails
     */
    public Map<TableBucket, Long> getOffsets(String producerId) throws Exception {
        return snapshotStore.readOffsets(producerId);
    }

    /**
     * Deletes a producer snapshot.
     *
     * @param producerId the producer ID
     * @throws Exception if the operation fails
     */
    public void deleteSnapshot(String producerId) throws Exception {
        snapshotStore.deleteSnapshot(producerId);
    }

    /**
     * Gets the default TTL in milliseconds.
     *
     * @return the default TTL
     */
    public long getDefaultTtlMs() {
        return defaultTtlMs;
    }

    // ------------------------------------------------------------------------
    //  Cleanup Operations
    // ------------------------------------------------------------------------

    /** Runs the cleanup task (expired snapshots and orphan files). */
    @VisibleForTesting
    void runCleanup() {
        try {
            int expiredCount = cleanupExpiredSnapshots();
            if (expiredCount > 0) {
                LOG.info("Producer snapshot cleanup: removed {} expired snapshots", expiredCount);
            }

            int orphanCount = cleanupOrphanFiles();
            if (orphanCount > 0) {
                LOG.info("Producer snapshot cleanup: removed {} orphan files", orphanCount);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup producer snapshots", e);
        }
    }

    /**
     * Cleans up expired producer snapshots.
     *
     * @return number of snapshots cleaned up
     * @throws Exception if the operation fails
     */
    @VisibleForTesting
    int cleanupExpiredSnapshots() throws Exception {
        List<String> producerIds = snapshotStore.listProducerIds();
        int cleanedCount = 0;
        long currentTime = System.currentTimeMillis();

        for (String producerId : producerIds) {
            try {
                Optional<ProducerSnapshot> optSnapshot =
                        snapshotStore.getSnapshotMetadata(producerId);
                if (optSnapshot.isPresent() && optSnapshot.get().isExpired(currentTime)) {
                    snapshotStore.deleteSnapshot(producerId);
                    cleanedCount++;
                    LOG.debug("Cleaned up expired snapshot for producer {}", producerId);
                }
            } catch (Exception e) {
                LOG.warn("Failed to cleanup snapshot for producer {}", producerId, e);
            }
        }

        return cleanedCount;
    }

    /**
     * Cleans up orphan files in remote storage that don't have corresponding ZK metadata.
     *
     * <p>Orphan files can occur when:
     *
     * <ul>
     *   <li>ZK metadata was deleted but remote file deletion failed
     *   <li>Process crashed after writing remote file but before creating ZK metadata
     *   <li>Network issues caused partial cleanup
     * </ul>
     *
     * @return number of orphan files cleaned up
     * @throws Exception if the operation fails
     */
    @VisibleForTesting
    int cleanupOrphanFiles() throws Exception {
        FsPath producersDir = snapshotStore.getProducersDirectory();
        FileSystem fileSystem = producersDir.getFileSystem();

        if (!fileSystem.exists(producersDir)) {
            LOG.debug("Producers directory does not exist, no orphan files to clean");
            return 0;
        }

        // Get all producer IDs from ZK (these are valid)
        Set<String> validProducerIds = new HashSet<>(snapshotStore.listProducerIds());

        // Collect all valid file paths from ZK metadata
        Set<String> validFilePaths = collectValidFilePaths(validProducerIds);

        int cleanedCount = 0;

        // List all producer directories in remote storage
        FileStatus[] producerDirs = fileSystem.listStatus(producersDir);
        if (producerDirs == null) {
            return 0;
        }

        for (FileStatus producerDirStatus : producerDirs) {
            if (!producerDirStatus.isDir()) {
                continue;
            }

            String producerId = producerDirStatus.getPath().getName();

            // Case 1: Producer ID not in ZK - entire directory is orphan
            if (!validProducerIds.contains(producerId)) {
                LOG.info(
                        "Found orphan producer directory {} (no ZK metadata), cleaning up",
                        producerId);
                cleanedCount +=
                        snapshotStore.deleteDirectoryRecursively(
                                fileSystem, producerDirStatus.getPath());
                continue;
            }

            // Case 2: Producer ID exists in ZK - check individual files
            cleanedCount +=
                    cleanupOrphanFilesForProducer(
                            fileSystem, producerDirStatus.getPath(), validFilePaths);
        }

        return cleanedCount;
    }

    private Set<String> collectValidFilePaths(Set<String> validProducerIds) {
        Set<String> validFilePaths = new HashSet<>();
        for (String producerId : validProducerIds) {
            try {
                Optional<ProducerSnapshot> optSnapshot =
                        snapshotStore.getSnapshotMetadata(producerId);
                if (optSnapshot.isPresent()) {
                    for (ProducerSnapshot.TableOffsetMetadata tableMetadata :
                            optSnapshot.get().getTableOffsets()) {
                        validFilePaths.add(tableMetadata.getOffsetsPath().toString());
                    }
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to get snapshot for producer {} during orphan cleanup, "
                                + "skipping its files",
                        producerId,
                        e);
            }
        }
        return validFilePaths;
    }

    private int cleanupOrphanFilesForProducer(
            FileSystem fileSystem, FsPath producerDir, Set<String> validFilePaths)
            throws IOException {
        int cleanedCount = 0;

        // List table directories under producer
        FileStatus[] tableDirs = fileSystem.listStatus(producerDir);
        if (tableDirs == null) {
            return 0;
        }

        for (FileStatus tableDirStatus : tableDirs) {
            if (!tableDirStatus.isDir()) {
                continue;
            }

            // List offset files under table directory
            FileStatus[] offsetFiles = fileSystem.listStatus(tableDirStatus.getPath());
            if (offsetFiles == null) {
                continue;
            }

            boolean hasValidFiles = false;
            for (FileStatus fileStatus : offsetFiles) {
                if (fileStatus.isDir()) {
                    continue;
                }

                String filePath = fileStatus.getPath().toString();
                if (!validFilePaths.contains(filePath)) {
                    // This file is not referenced by any ZK metadata - it's orphan
                    LOG.debug("Deleting orphan file: {}", filePath);
                    try {
                        fileSystem.delete(fileStatus.getPath(), false);
                        cleanedCount++;
                    } catch (IOException e) {
                        LOG.warn("Failed to delete orphan file: {}", filePath, e);
                    }
                } else {
                    hasValidFiles = true;
                }
            }

            // If table directory is now empty, delete it
            if (!hasValidFiles) {
                tryDeleteEmptyDirectory(fileSystem, tableDirStatus.getPath());
            }
        }

        // Check if producer directory is now empty
        tryDeleteEmptyDirectory(fileSystem, producerDir);

        return cleanedCount;
    }

    private void tryDeleteEmptyDirectory(FileSystem fileSystem, FsPath dir) {
        try {
            FileStatus[] remaining = fileSystem.listStatus(dir);
            if (remaining == null || remaining.length == 0) {
                fileSystem.delete(dir, false);
                LOG.debug("Deleted empty directory: {}", dir);
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete empty directory: {}", dir, e);
        }
    }

    @Override
    public void close() {
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Producer snapshot manager closed");
    }
}
