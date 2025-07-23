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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.StaleMetadataException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbPartitionMetadata;
import com.alibaba.fluss.rpc.messages.PbServerNode;
import com.alibaba.fluss.rpc.messages.PbTableMetadata;
import com.alibaba.fluss.rpc.messages.PbTablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Utils for metadata for client. */
public class MetadataUtils {

    private static final Random randOffset = new Random();

    /**
     * full update cluster, means we will rebuild the cluster by clearing all cached table in
     * cluster, and then send metadata request to request the input tables in tablePaths, after that
     * add those table into cluster.
     */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            AdminReadOnlyGateway gateway, Set<TablePath> tablePaths)
            throws ExecutionException, InterruptedException, TimeoutException {
        return sendMetadataRequestAndRebuildCluster(gateway, false, null, tablePaths, null, null);
    }

    /**
     * Partial update cluster, means we will rebuild the cluster by sending metadata request to
     * request the input tables/partitions in physicalTablePaths, after that add those
     * tables/partitions into cluster. The origin tables/partitions in cluster will not be cleared,
     * but will be updated.
     */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            Cluster cluster,
            RpcClient client,
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds)
            throws ExecutionException, InterruptedException, TimeoutException {
        AdminReadOnlyGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> getOneAvailableTabletServerNode(cluster),
                        client,
                        AdminReadOnlyGateway.class);
        return sendMetadataRequestAndRebuildCluster(
                gateway, true, cluster, tablePaths, tablePartitionNames, tablePartitionIds);
    }

    /** maybe partial update cluster. */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            AdminReadOnlyGateway gateway,
            boolean partialUpdate,
            Cluster originCluster,
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitions,
            @Nullable Collection<Long> tablePartitionIds)
            throws ExecutionException, InterruptedException, TimeoutException {
        MetadataRequest metadataRequest =
                ClientRpcMessageUtils.makeMetadataRequest(
                        tablePaths, tablePartitions, tablePartitionIds);
        return gateway.metadata(metadataRequest)
                .thenApply(
                        response -> {
                            // Update the alive table servers.
                            Map<Integer, ServerNode> newAliveTabletServers =
                                    getAliveTabletServers(response);
                            // when talking to the startup tablet
                            // server, it maybe receive empty metadata, we'll consider it as
                            // stale metadata and throw StaleMetadataException which will cause
                            // to retry later.
                            if (newAliveTabletServers.isEmpty()) {
                                throw new StaleMetadataException("Alive tablet server is empty.");
                            }
                            ServerNode coordinatorServer = getCoordinatorServer(response);

                            Map<TablePath, Long> newTablePathToTableId;
                            Map<TablePath, TableInfo> newTablePathToTableInfo;
                            Map<PhysicalTablePath, List<BucketLocation>> newBucketLocations;
                            Map<PhysicalTablePath, Long> newPartitionIdByPath;

                            NewTableMetadata newTableMetadata =
                                    getTableMetadataToUpdate(originCluster, response);

                            if (partialUpdate) {
                                // If partial update, we will clear the to be updated table out ot
                                // the origin cluster.
                                newTablePathToTableId =
                                        new HashMap<>(originCluster.getTableIdByPath());
                                newTablePathToTableInfo =
                                        new HashMap<>(originCluster.getTableInfoByPath());
                                newBucketLocations =
                                        new HashMap<>(originCluster.getBucketLocationsByPath());
                                newPartitionIdByPath =
                                        new HashMap<>(originCluster.getPartitionIdByPath());

                                newTablePathToTableId.putAll(newTableMetadata.tablePathToTableId);
                                newTablePathToTableInfo.putAll(
                                        newTableMetadata.tablePathToTableInfo);
                                newBucketLocations.putAll(newTableMetadata.bucketLocations);
                                newPartitionIdByPath.putAll(newTableMetadata.partitionIdByPath);

                            } else {
                                // If full update, we will clear all tables info out ot the origin
                                // cluster.
                                newTablePathToTableId = newTableMetadata.tablePathToTableId;
                                newTablePathToTableInfo = newTableMetadata.tablePathToTableInfo;
                                newBucketLocations = newTableMetadata.bucketLocations;
                                newPartitionIdByPath = newTableMetadata.partitionIdByPath;
                            }

                            // update the leader id to check whether the leader is alive or not.
                            newBucketLocations.forEach(
                                    (physicalTablePath, bucketLocations) ->
                                            bucketLocations.forEach(
                                                    bucketLocation -> {
                                                        Integer leader = bucketLocation.getLeader();
                                                        if (leader != null
                                                                && newAliveTabletServers.get(leader)
                                                                        == null) {
                                                            bucketLocation.setLeader(null);
                                                        }
                                                    }));

                            return new Cluster(
                                    newAliveTabletServers,
                                    coordinatorServer,
                                    newBucketLocations,
                                    newTablePathToTableId,
                                    newPartitionIdByPath,
                                    newTablePathToTableInfo);
                        })
                .get(30, TimeUnit.SECONDS); // TODO currently, we don't have timeout logic in
        // RpcClient, it will let the get() block forever. So we
        // time out here
    }

    private static NewTableMetadata getTableMetadataToUpdate(
            Cluster cluster, MetadataResponse metadataResponse) {
        Map<TablePath, Long> newTablePathToTableId = new HashMap<>();
        Map<TablePath, TableInfo> newTablePathToTableInfo = new HashMap<>();
        Map<PhysicalTablePath, List<BucketLocation>> newBucketLocations = new HashMap<>();
        Map<PhysicalTablePath, Long> newPartitionIdByPath = new HashMap<>();

        // iterate all table metadata
        List<PbTableMetadata> pbTableMetadataList = metadataResponse.getTableMetadatasList();
        pbTableMetadataList.forEach(
                pbTableMetadata -> {
                    // get table info for the table
                    long tableId = pbTableMetadata.getTableId();
                    PbTablePath protoTablePath = pbTableMetadata.getTablePath();
                    TablePath tablePath =
                            new TablePath(
                                    protoTablePath.getDatabaseName(),
                                    protoTablePath.getTableName());
                    newTablePathToTableId.put(tablePath, tableId);
                    TableDescriptor tableDescriptor =
                            TableDescriptor.fromJsonBytes(pbTableMetadata.getTableJson());
                    newTablePathToTableInfo.put(
                            tablePath,
                            TableInfo.of(
                                    tablePath,
                                    pbTableMetadata.getTableId(),
                                    pbTableMetadata.getSchemaId(),
                                    tableDescriptor,
                                    pbTableMetadata.getCreatedTime(),
                                    pbTableMetadata.getModifiedTime()));

                    // Get all buckets for the table.
                    List<PbBucketMetadata> pbBucketMetadataList =
                            pbTableMetadata.getBucketMetadatasList();
                    newBucketLocations.put(
                            PhysicalTablePath.of(tablePath),
                            toBucketLocations(
                                    tablePath, tableId, null, null, pbBucketMetadataList));
                });

        List<PbPartitionMetadata> pbPartitionMetadataList =
                metadataResponse.getPartitionMetadatasList();

        // iterate all partition metadata
        pbPartitionMetadataList.forEach(
                pbPartitionMetadata -> {
                    long tableId = pbPartitionMetadata.getTableId();
                    // the table path should be initialized at begin
                    TablePath tablePath = cluster.getTablePathOrElseThrow(tableId);
                    PhysicalTablePath physicalTablePath =
                            PhysicalTablePath.of(tablePath, pbPartitionMetadata.getPartitionName());
                    newPartitionIdByPath.put(
                            physicalTablePath, pbPartitionMetadata.getPartitionId());
                    newBucketLocations.put(
                            physicalTablePath,
                            toBucketLocations(
                                    tablePath,
                                    tableId,
                                    pbPartitionMetadata.getPartitionId(),
                                    pbPartitionMetadata.getPartitionName(),
                                    pbPartitionMetadata.getBucketMetadatasList()));
                });

        return new NewTableMetadata(
                newTablePathToTableId,
                newTablePathToTableInfo,
                newBucketLocations,
                newPartitionIdByPath);
    }

    private static final class NewTableMetadata {
        private final Map<TablePath, Long> tablePathToTableId;
        private final Map<TablePath, TableInfo> tablePathToTableInfo;
        private final Map<PhysicalTablePath, List<BucketLocation>> bucketLocations;
        private final Map<PhysicalTablePath, Long> partitionIdByPath;

        public NewTableMetadata(
                Map<TablePath, Long> tablePathToTableId,
                Map<TablePath, TableInfo> tablePathToTableInfo,
                Map<PhysicalTablePath, List<BucketLocation>> bucketLocations,
                Map<PhysicalTablePath, Long> partitionIdByPath) {
            this.tablePathToTableId = tablePathToTableId;
            this.tablePathToTableInfo = tablePathToTableInfo;
            this.bucketLocations = bucketLocations;
            this.partitionIdByPath = partitionIdByPath;
        }
    }

    public static ServerNode getOneAvailableTabletServerNode(Cluster cluster) {
        List<ServerNode> aliveTabletServers = cluster.getAliveTabletServerList();
        if (aliveTabletServers.isEmpty()) {
            throw new FlussRuntimeException("no alive tablet server in cluster");
        }
        // just pick one random server node
        int offset = randOffset.nextInt(aliveTabletServers.size());
        return aliveTabletServers.get(offset);
    }

    @Nullable
    private static ServerNode getCoordinatorServer(MetadataResponse response) {
        if (!response.hasCoordinatorServer()) {
            return null;
        } else {
            PbServerNode protoServerNode = response.getCoordinatorServer();
            return new ServerNode(
                    protoServerNode.getNodeId(),
                    protoServerNode.getHost(),
                    protoServerNode.getPort(),
                    ServerType.COORDINATOR);
        }
    }

    private static Map<Integer, ServerNode> getAliveTabletServers(MetadataResponse response) {
        Map<Integer, ServerNode> aliveTabletServers = new HashMap<>();
        response.getTabletServersList()
                .forEach(
                        serverNode -> {
                            int nodeId = serverNode.getNodeId();
                            aliveTabletServers.put(
                                    nodeId,
                                    new ServerNode(
                                            nodeId,
                                            serverNode.getHost(),
                                            serverNode.getPort(),
                                            ServerType.TABLET_SERVER,
                                            serverNode.hasRack() ? serverNode.getRack() : null));
                        });
        return aliveTabletServers;
    }

    private static List<BucketLocation> toBucketLocations(
            TablePath tablePath,
            long tableId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            List<PbBucketMetadata> pbBucketMetadataList) {
        List<BucketLocation> bucketLocations = new ArrayList<>();
        for (PbBucketMetadata pbBucketMetadata : pbBucketMetadataList) {
            int bucketId = pbBucketMetadata.getBucketId();
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            int[] replicas = new int[pbBucketMetadata.getReplicaIdsCount()];
            for (int i = 0; i < replicas.length; i++) {
                replicas[i] = pbBucketMetadata.getReplicaIdAt(i);
            }
            Integer leader = null;
            if (pbBucketMetadata.hasLeaderId()) {
                leader = pbBucketMetadata.getLeaderId();
            }
            PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);

            BucketLocation bucketLocation =
                    new BucketLocation(physicalTablePath, tableBucket, leader, replicas);
            bucketLocations.add(bucketLocation);
        }
        return bucketLocations;
    }
}
