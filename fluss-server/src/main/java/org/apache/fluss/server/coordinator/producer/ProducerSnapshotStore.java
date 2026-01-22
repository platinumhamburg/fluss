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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerSnapshot;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Store for producer offset snapshots.
 *
 * <p>This class handles the low-level storage operations for producer snapshots:
 *
 * <ul>
 *   <li>Writing offset data to remote storage (OSS/S3/HDFS)
 *   <li>Reading offset data from remote storage
 *   <li>Registering/updating snapshot metadata in ZooKeeper
 *   <li>Deleting snapshots from both ZK and remote storage
 * </ul>
 *
 * <p>This class is designed to be stateless and thread-safe. All state management and lifecycle
 * operations should be handled by {@link ProducerSnapshotManager}.
 *
 * @see ProducerSnapshotManager for lifecycle management and cleanup scheduling
 */
public class ProducerSnapshotStore {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSnapshotStore.class);

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public ProducerSnapshotStore(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    // ------------------------------------------------------------------------
    //  Core CRUD Operations
    // ------------------------------------------------------------------------

    /**
     * Stores a new producer offset snapshot.
     *
     * @param producerId the producer ID
     * @param offsets map of TableBucket to offset
     * @param expirationTime the expiration timestamp in milliseconds
     * @throws Exception if the operation fails
     */
    public void storeSnapshot(
            String producerId, Map<TableBucket, Long> offsets, long expirationTime)
            throws Exception {
        // Group offsets by table ID
        Map<Long, Map<TableBucket, Long>> offsetsByTable = groupOffsetsByTable(offsets);

        // Write each table's offsets to remote storage and collect metadata
        List<ProducerSnapshot.TableOffsetMetadata> tableOffsetMetadatas = new ArrayList<>();
        for (Map.Entry<Long, Map<TableBucket, Long>> entry : offsetsByTable.entrySet()) {
            long tableId = entry.getKey();
            Map<TableBucket, Long> tableOffsets = entry.getValue();

            // Write offsets to remote file
            FsPath offsetsPath = storeOffsetsFile(producerId, tableId, tableOffsets);
            tableOffsetMetadatas.add(
                    new ProducerSnapshot.TableOffsetMetadata(tableId, offsetsPath));
        }

        // Create and register snapshot metadata in ZK
        ProducerSnapshot snapshot = new ProducerSnapshot(expirationTime, tableOffsetMetadatas);
        zkClient.registerProducerSnapshot(producerId, snapshot);

        LOG.info(
                "Stored producer snapshot for producer {} with {} tables, expires at {}",
                producerId,
                tableOffsetMetadatas.size(),
                expirationTime);
    }

    /**
     * Tries to atomically store a new producer offset snapshot.
     *
     * <p>This method first writes offset files to remote storage, then attempts to atomically
     * create the ZK metadata. If the ZK node already exists (another concurrent request succeeded
     * first), this method returns false and cleans up the remote files it created.
     *
     * <p>This ensures:
     *
     * <ul>
     *   <li>Atomicity: The check-and-create is a single atomic ZK operation
     *   <li>Idempotency: Concurrent requests with same producerId will have exactly one succeed
     * </ul>
     *
     * @param producerId the producer ID
     * @param offsets map of TableBucket to offset
     * @param expirationTime the expiration timestamp in milliseconds
     * @return true if the snapshot was created (CREATED), false if it already existed
     *     (ALREADY_EXISTS)
     * @throws Exception if the operation fails for reasons other than node already existing
     */
    public boolean tryStoreSnapshot(
            String producerId, Map<TableBucket, Long> offsets, long expirationTime)
            throws Exception {
        // Group offsets by table ID
        Map<Long, Map<TableBucket, Long>> offsetsByTable = groupOffsetsByTable(offsets);

        // Write each table's offsets to remote storage and collect metadata
        List<ProducerSnapshot.TableOffsetMetadata> tableOffsetMetadatas = new ArrayList<>();
        List<FsPath> createdFiles = new ArrayList<>();
        try {
            for (Map.Entry<Long, Map<TableBucket, Long>> entry : offsetsByTable.entrySet()) {
                long tableId = entry.getKey();
                Map<TableBucket, Long> tableOffsets = entry.getValue();

                // Write offsets to remote file
                FsPath offsetsPath = storeOffsetsFile(producerId, tableId, tableOffsets);
                createdFiles.add(offsetsPath);
                tableOffsetMetadatas.add(
                        new ProducerSnapshot.TableOffsetMetadata(tableId, offsetsPath));
            }

            // Try to atomically create snapshot metadata in ZK
            ProducerSnapshot snapshot = new ProducerSnapshot(expirationTime, tableOffsetMetadatas);
            boolean created = zkClient.tryRegisterProducerSnapshot(producerId, snapshot);

            if (!created) {
                // Another concurrent request already created the snapshot
                // Clean up the remote files we just created
                LOG.info(
                        "Producer snapshot already exists for producer {}, "
                                + "cleaning up {} remote files",
                        producerId,
                        createdFiles.size());
                for (FsPath filePath : createdFiles) {
                    deleteRemoteFile(filePath);
                }
                return false;
            }

            LOG.info(
                    "Stored producer snapshot for producer {} with {} tables, expires at {}",
                    producerId,
                    tableOffsetMetadatas.size(),
                    expirationTime);
            return true;
        } catch (Exception e) {
            // If any error occurs, clean up the remote files we created
            LOG.warn(
                    "Failed to store producer snapshot for producer {}, "
                            + "cleaning up {} remote files",
                    producerId,
                    createdFiles.size(),
                    e);
            for (FsPath filePath : createdFiles) {
                deleteRemoteFile(filePath);
            }
            throw e;
        }
    }

    /**
     * Gets the snapshot metadata for a producer.
     *
     * @param producerId the producer ID
     * @return Optional containing the snapshot if exists
     * @throws Exception if the operation fails
     */
    public Optional<ProducerSnapshot> getSnapshotMetadata(String producerId) throws Exception {
        return zkClient.getProducerSnapshot(producerId);
    }

    /**
     * Reads all offsets for a producer from remote storage.
     *
     * @param producerId the producer ID
     * @return map of TableBucket to offset, or empty map if no snapshot exists
     * @throws Exception if the operation fails
     */
    public Map<TableBucket, Long> readOffsets(String producerId) throws Exception {
        Optional<ProducerSnapshot> optSnapshot = zkClient.getProducerSnapshot(producerId);
        if (!optSnapshot.isPresent()) {
            return new HashMap<>();
        }

        ProducerSnapshot snapshot = optSnapshot.get();
        Map<TableBucket, Long> allOffsets = new HashMap<>();

        // Read offsets from each table's remote file
        for (ProducerSnapshot.TableOffsetMetadata tableMetadata : snapshot.getTableOffsets()) {
            Map<TableBucket, Long> tableOffsets = readOffsetsFile(tableMetadata.getOffsetsPath());
            allOffsets.putAll(tableOffsets);
        }

        return allOffsets;
    }

    /**
     * Deletes a producer snapshot (both ZK metadata and remote files).
     *
     * @param producerId the producer ID
     * @throws Exception if the operation fails
     */
    public void deleteSnapshot(String producerId) throws Exception {
        Optional<ProducerSnapshot> optSnapshot = zkClient.getProducerSnapshot(producerId);
        if (!optSnapshot.isPresent()) {
            LOG.debug("No snapshot found for producer {}, nothing to delete", producerId);
            return;
        }

        ProducerSnapshot snapshot = optSnapshot.get();

        // Delete remote offset files
        for (ProducerSnapshot.TableOffsetMetadata tableMetadata : snapshot.getTableOffsets()) {
            deleteRemoteFile(tableMetadata.getOffsetsPath());
        }

        // Delete ZK metadata
        zkClient.deleteProducerSnapshot(producerId);

        LOG.info("Deleted producer snapshot for producer {}", producerId);
    }

    /**
     * Lists all producer IDs that have registered snapshots.
     *
     * @return list of producer IDs
     * @throws Exception if the operation fails
     */
    public List<String> listProducerIds() throws Exception {
        return zkClient.listProducerIds();
    }

    // ------------------------------------------------------------------------
    //  Remote Storage Operations
    // ------------------------------------------------------------------------

    /**
     * Gets the base directory path for producer snapshots in remote storage.
     *
     * @return the producers directory path
     */
    public FsPath getProducersDirectory() {
        return new FsPath(remoteDataDir + "/producers");
    }

    /**
     * Deletes a remote file.
     *
     * @param filePath the file path to delete
     */
    public void deleteRemoteFile(FsPath filePath) {
        try {
            FileSystem fileSystem = filePath.getFileSystem();
            if (fileSystem.exists(filePath)) {
                fileSystem.delete(filePath, false);
                LOG.debug("Deleted remote file: {}", filePath);
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete remote file: {}", filePath, e);
        }
    }

    /**
     * Deletes a directory recursively.
     *
     * @param fileSystem the file system
     * @param path the directory path
     * @return number of files deleted
     */
    public int deleteDirectoryRecursively(FileSystem fileSystem, FsPath path) {
        int deletedCount = 0;
        try {
            FileStatus[] contents = fileSystem.listStatus(path);
            if (contents != null) {
                for (FileStatus status : contents) {
                    if (status.isDir()) {
                        deletedCount += deleteDirectoryRecursively(fileSystem, status.getPath());
                    } else {
                        if (fileSystem.delete(status.getPath(), false)) {
                            deletedCount++;
                        }
                    }
                }
            }
            // Delete the directory itself
            fileSystem.delete(path, false);
        } catch (IOException e) {
            LOG.warn("Failed to delete directory recursively: {}", path, e);
        }
        return deletedCount;
    }

    // ------------------------------------------------------------------------
    //  Private Helper Methods
    // ------------------------------------------------------------------------

    private Map<Long, Map<TableBucket, Long>> groupOffsetsByTable(Map<TableBucket, Long> offsets) {
        Map<Long, Map<TableBucket, Long>> result = new HashMap<>();
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            long tableId = bucket.getTableId();
            result.computeIfAbsent(tableId, k -> new HashMap<>()).put(bucket, entry.getValue());
        }
        return result;
    }

    private FsPath storeOffsetsFile(String producerId, long tableId, Map<TableBucket, Long> offsets)
            throws Exception {
        // Generate unique file path
        String fileName = UUID.randomUUID().toString() + ".offsets";
        FsPath offsetsPath =
                new FsPath(
                        remoteDataDir
                                + "/producers/"
                                + producerId
                                + "/"
                                + tableId
                                + "/"
                                + fileName);

        // Ensure parent directory exists
        FileSystem fileSystem = offsetsPath.getFileSystem();
        if (!fileSystem.exists(offsetsPath.getParent())) {
            fileSystem.mkdirs(offsetsPath.getParent());
        }

        // Serialize and write offsets
        TableBucketOffsets tableBucketOffsets = new TableBucketOffsets(tableId, offsets);
        byte[] jsonBytes = tableBucketOffsets.toJsonBytes();

        try (FSDataOutputStream outputStream =
                fileSystem.create(offsetsPath, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }

        LOG.debug(
                "Stored producer offsets for producer {} table {} at {}",
                producerId,
                tableId,
                offsetsPath);
        return offsetsPath;
    }

    private Map<TableBucket, Long> readOffsetsFile(FsPath offsetsPath) throws Exception {
        FileSystem fileSystem = offsetsPath.getFileSystem();
        if (!fileSystem.exists(offsetsPath)) {
            LOG.warn("Offsets file not found: {}", offsetsPath);
            return new HashMap<>();
        }

        try (FSDataInputStream inputStream = fileSystem.open(offsetsPath);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, true);
            TableBucketOffsets tableBucketOffsets =
                    TableBucketOffsets.fromJsonBytes(outputStream.toByteArray());
            return tableBucketOffsets.getOffsets();
        }
    }
}
