/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.metadata;

import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * An abstract implementation of {@link MetadataProvider} that provides common functionality for
 * fetching table and partition metadata from ZooKeeper.
 */
public abstract class ZkBasedMetadataProvider implements MetadataProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ZkBasedMetadataProvider.class);

    private final ZooKeeperClient zkClient;
    private final MetadataManager metadataManager;

    protected ZkBasedMetadataProvider(ZooKeeperClient zkClient, MetadataManager metadataManager) {
        this.zkClient = zkClient;
        this.metadataManager = metadataManager;
    }

    protected ZooKeeperClient getZkClient() {
        return zkClient;
    }

    @Override
    public abstract Optional<TablePath> getTablePathFromCache(long tableId);

    @Override
    public List<TableMetadata> getTablesMetadataFromZK(Collection<TablePath> tablePaths) {
        try {
            Map<TablePath, TableInfo> path2Info = metadataManager.getTables(tablePaths);
            Map<Long, TableInfo> id2Info =
                    path2Info.values().stream()
                            .collect(Collectors.toMap(TableInfo::getTableId, info -> info));
            // Fetch all table metadata from ZooKeeper
            Map<Long, List<BucketMetadata>> metadataForTables =
                    zkClient.getBucketMetadataForTables(id2Info.keySet());
            // we should iterate on the input tablePaths to guarantee the result contains all tables
            List<TableMetadata> result = new ArrayList<>();
            for (TablePath path : tablePaths) {
                TableInfo tableInfo = path2Info.get(path);
                if (tableInfo == null) {
                    throw new TableNotExistException("Table '" + path + "' does not exist.");
                }
                // the metadata maybe null when the table assignment not generated yet.
                List<BucketMetadata> metadata = metadataForTables.get(tableInfo.getTableId());
                TableMetadata tableMetadata = new TableMetadata(tableInfo, metadata);
                result.add(tableMetadata);
            }
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to fetch table metadata from ZooKeeper", e);
        }
    }

    @Override
    public List<PartitionMetadata> getPartitionsMetadataFromZK(
            Collection<PhysicalTablePath> partitionPaths) {
        try {
            Map<Long, String> partitionId2PartitionName = new HashMap<>();
            Map<Long, Long> partitionId2TableId = new HashMap<>();
            Map<PhysicalTablePath, TablePartition> partitionIds =
                    zkClient.getPartitionIds(partitionPaths);
            for (PhysicalTablePath partitionPath : partitionPaths) {
                TablePartition partition = partitionIds.get(partitionPath);
                if (partition == null) {
                    throw new PartitionNotExistException(
                            "Table partition '" + partitionPath + "' does not exist.");
                }
                partitionId2PartitionName.put(
                        partition.getPartitionId(), checkNotNull(partitionPath.getPartitionName()));
                partitionId2TableId.put(partition.getPartitionId(), partition.getTableId());
            }
            List<PartitionMetadata> result = new ArrayList<>();
            // Fetch all table metadata from ZooKeeper
            zkClient.getBucketMetadataForPartitions(partitionId2TableId.keySet())
                    .forEach(
                            (partitionId, bucketMetadataList) -> {
                                long tableId = partitionId2TableId.get(partitionId);
                                String partitionName = partitionId2PartitionName.get(partitionId);
                                PartitionMetadata partitionMetadata =
                                        new PartitionMetadata(
                                                tableId,
                                                partitionName,
                                                partitionId,
                                                bucketMetadataList);
                                result.add(partitionMetadata);
                            });
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to fetch partition metadata from ZooKeeper", e);
        }
    }

    @Override
    public List<PartitionMetadata> getPartitionsMetadataFromZK(long partitionId) {
        try {
            // Get partition assignment to obtain tableId and partitionName
            Optional<PartitionAssignment> partitionAssignmentOpt =
                    zkClient.getPartitionAssignment(partitionId);
            if (!partitionAssignmentOpt.isPresent()) {
                // Partition assignment not found, return empty list
                return new ArrayList<>();
            }

            PartitionAssignment partitionAssignment = partitionAssignmentOpt.get();
            long tableId = partitionAssignment.getTableId();

            // Get partition name - we need to query it from partition path in ZK
            // However, PartitionAssignment doesn't contain partition name, so we need another way
            // Fetch bucket metadata for this partition
            Map<Long, List<BucketMetadata>> bucketMetadataMap =
                    zkClient.getBucketMetadataForPartitions(
                            java.util.Collections.singleton(partitionId));

            List<BucketMetadata> bucketMetadataList = bucketMetadataMap.get(partitionId);
            if (bucketMetadataList == null) {
                // No bucket metadata found
                return new ArrayList<>();
            }

            // We still need partitionName. Let's get it from table and search all partitions
            // This is not ideal, but necessary since ZK structure doesn't allow direct
            // partitionId -> partitionName lookup
            Optional<TablePath> tablePathOpt = getTablePathFromCache(tableId);
            if (!tablePathOpt.isPresent()) {
                // Can't proceed without table path
                return new ArrayList<>();
            }

            TablePath tablePath = tablePathOpt.get();
            Map<String, Long> partitionNameAndIds = zkClient.getPartitionNameAndIds(tablePath);

            // Find the partition name for this partitionId
            String partitionName = null;
            for (Map.Entry<String, Long> entry : partitionNameAndIds.entrySet()) {
                if (entry.getValue().equals(partitionId)) {
                    partitionName = entry.getKey();
                    break;
                }
            }

            if (partitionName == null) {
                // Partition not found in table's partition list
                return new ArrayList<>();
            }

            PartitionMetadata partitionMetadata =
                    new PartitionMetadata(tableId, partitionName, partitionId, bucketMetadataList);
            List<PartitionMetadata> result = new ArrayList<>();
            result.add(partitionMetadata);
            return result;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(
                    "Failed to fetch partition metadata from ZooKeeper for partitionId: "
                            + partitionId,
                    e);
        }
    }

    @Override
    public BucketLeaderResult getBucketLeaderIdFromCache(TableBucket tableBucket) {
        try {
            // Directly query leader ID from cache without constructing BucketMetadata objects
            Optional<Integer> leaderIdOpt = getBucketLeaderIdFromCacheInternal(tableBucket);
            if (!leaderIdOpt.isPresent()) {
                // Bucket not found in cache - this is temporary unavailability
                return BucketLeaderResult.unavailable(
                        "Bucket " + tableBucket + " leader not found in cache");
            }

            // Bucket found in cache, return leader ID (may be -1 if no leader elected)
            return BucketLeaderResult.found(leaderIdOpt.get());
        } catch (Exception e) {
            LOG.debug(
                    "Error querying bucket {} leader from cache: {}", tableBucket, e.getMessage());
            return BucketLeaderResult.unavailable(
                    "Error querying bucket " + tableBucket + " from cache: " + e.getMessage());
        }
    }

    @Override
    public BucketLeaderResult getBucketLeaderIdFromZK(TableBucket tableBucket) {
        try {
            // Directly query leader ID from ZK without constructing BucketMetadata or other
            // intermediate objects
            Optional<Integer> leaderIdOpt =
                    zkClient.getLeaderAndIsr(tableBucket)
                            .map(leaderAndIsr -> leaderAndIsr.leader());

            if (!leaderIdOpt.isPresent()) {
                // Bucket not found in ZK - this means it doesn't exist (permanent failure)
                return BucketLeaderResult.notFound(
                        "Bucket " + tableBucket + " not found in ZooKeeper");
            }

            // Bucket found in ZK, extract leader ID (may be -1 if no leader elected)
            int leaderId = leaderIdOpt.get();
            return BucketLeaderResult.found(leaderId);
        } catch (Exception e) {
            // ZK query error - this is temporary unavailability
            LOG.warn(
                    "Error querying bucket {} leader from ZooKeeper: {}",
                    tableBucket,
                    e.getMessage());
            return BucketLeaderResult.unavailable(
                    "Error querying bucket " + tableBucket + " from ZooKeeper: " + e.getMessage());
        }
    }

    /**
     * Abstract method for efficiently querying bucket leader ID from cache. Subclasses must
     * implement this to provide direct access to cached leader information.
     *
     * @param tableBucket the table bucket to query
     * @return Optional containing the leader ID if found (may be -1 if no leader elected),
     *     Optional.empty() if bucket not found in cache
     */
    protected abstract Optional<Integer> getBucketLeaderIdFromCacheInternal(
            TableBucket tableBucket);

    /**
     * Get bucket metadata from cache. Handles both partitioned and non-partitioned tables.
     *
     * @param tableBucket the table bucket to query
     * @return the bucket metadata, or null if not found in cache
     */
    private BucketMetadata getBucketMetadataFromCache(TableBucket tableBucket) {
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId == null) {
            // Non-partitioned table
            return getBucketMetadataFromCacheForNonPartitionedTable(tableBucket);
        } else {
            // Partitioned table
            return getBucketMetadataFromCacheForPartitionedTable(tableBucket, partitionId);
        }
    }

    /**
     * Get bucket metadata from ZooKeeper. Handles both partitioned and non-partitioned tables.
     *
     * @param tableBucket the table bucket to query
     * @return the bucket metadata, or null if not found in ZK
     */
    private BucketMetadata getBucketMetadataFromZK(TableBucket tableBucket) {
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId == null) {
            // Non-partitioned table
            return getBucketMetadataFromZKForNonPartitionedTable(tableBucket);
        } else {
            // Partitioned table
            return getBucketMetadataFromZKForPartitionedTable(tableBucket, partitionId);
        }
    }

    private BucketMetadata getBucketMetadataFromCacheForNonPartitionedTable(
            TableBucket tableBucket) {
        Optional<TablePath> tablePathOpt = getTablePathFromCache(tableBucket.getTableId());
        if (!tablePathOpt.isPresent()) {
            return null;
        }

        Optional<TableMetadata> tableMetadataOpt = getTableMetadataFromCache(tablePathOpt.get());
        if (!tableMetadataOpt.isPresent()) {
            return null;
        }

        // Find the specific bucket
        for (BucketMetadata bucketMetadata : tableMetadataOpt.get().getBucketMetadataList()) {
            if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                return bucketMetadata;
            }
        }
        return null;
    }

    private BucketMetadata getBucketMetadataFromCacheForPartitionedTable(
            TableBucket tableBucket, long partitionId) {
        Optional<PhysicalTablePath> physicalPathOpt = getPhysicalTablePathFromCache(partitionId);
        if (!physicalPathOpt.isPresent()) {
            return null;
        }

        Optional<PartitionMetadata> partitionMetadataOpt =
                getPartitionMetadataFromCache(physicalPathOpt.get());
        if (!partitionMetadataOpt.isPresent()) {
            return null;
        }

        // Find the specific bucket
        for (BucketMetadata bucketMetadata : partitionMetadataOpt.get().getBucketMetadataList()) {
            if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                return bucketMetadata;
            }
        }
        return null;
    }

    private BucketMetadata getBucketMetadataFromZKForNonPartitionedTable(TableBucket tableBucket) {
        Optional<TablePath> tablePathOpt = getTablePathFromCache(tableBucket.getTableId());
        if (!tablePathOpt.isPresent()) {
            return null;
        }

        // Query from ZooKeeper and update cache
        List<TableMetadata> tableMetadataList =
                getTablesMetadataFromZK(java.util.Collections.singleton(tablePathOpt.get()));
        if (tableMetadataList.isEmpty()) {
            return null;
        }

        // Find the specific bucket
        for (BucketMetadata bucketMetadata : tableMetadataList.get(0).getBucketMetadataList()) {
            if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                return bucketMetadata;
            }
        }
        return null;
    }

    private BucketMetadata getBucketMetadataFromZKForPartitionedTable(
            TableBucket tableBucket, long partitionId) {
        // For ZK query, directly query bucket metadata by partitionId
        // without needing PhysicalTablePath from cache first.
        // This ensures we can handle partition bucket failover even when
        // the cache doesn't have the partitionId -> PhysicalTablePath mapping.
        List<PartitionMetadata> partitionMetadataList = getPartitionsMetadataFromZK(partitionId);
        if (partitionMetadataList.isEmpty()) {
            return null;
        }

        PartitionMetadata partitionMetadata = partitionMetadataList.get(0);
        // Find the specific bucket
        for (BucketMetadata bucketMetadata : partitionMetadata.getBucketMetadataList()) {
            if (bucketMetadata.getBucketId() == tableBucket.getBucket()) {
                return bucketMetadata;
            }
        }
        return null;
    }
}
