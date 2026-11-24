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

package org.apache.fluss.client.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoRequest;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsRequest;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.ClientUtils.withSafeCoordinatorRetries;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.FutureUtils.completedExceptionally;

/**
 * Builds and registers BulkLoad bucket snapshots for supported primary-key tables.
 *
 * <p>Build workers require read and write access to the target snapshot directories and local space
 * for a RocksDB database under the JVM temporary directory. Each physical bucket within a
 * transaction must have at most one active writer across all workers. The SDK supports the plain KV
 * value layout and tables without row-level TTL.
 */
@PublicEvolving
@ThreadSafe
public final class BulkLoadClient {

    private static final int BUCKET_ALLOCATED = 0;
    private static final int BUCKET_COMMITTED = 1;

    private final ExecutorService metadataRefreshExecutor =
            Executors.newSingleThreadExecutor(
                    new ExecutorThreadFactory("fluss-bulkload-metadata-refresh"));
    private final AdminGateway gateway;

    /** Creates the client for a Fluss connection. */
    @Internal
    public BulkLoadClient(RpcClient rpcClient, MetadataUpdater metadataUpdater) {
        this.gateway =
                withSafeCoordinatorRetries(
                        GatewayClientProxy.createGatewayProxy(
                                metadataUpdater::getCoordinatorServer,
                                rpcClient,
                                AdminGateway.class),
                        metadataUpdater,
                        metadataRefreshExecutor);
    }

    /**
     * Begins a transaction for the specified targets and returns its frozen build context.
     *
     * <p>Use an empty partition list for a non-partitioned table. For a partitioned table, provide
     * the partition names explicitly. Persist the caller-supplied ID with the input selection and
     * reuse both when retrying Begin after an uncertain outcome.
     */
    public CompletableFuture<BulkLoadBuildContext> begin(
            TablePath tablePath, List<String> partitionNames, String bulkLoadId) {
        checkArgument(
                !checkNotNull(bulkLoadId, "bulkLoadId must not be null").isEmpty(),
                "bulkLoadId must not be empty");
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(bulkLoadId);
        setTablePath(request.setTablePath(), tablePath);
        partitionNames.forEach(request::addPartitionName);
        return gateway.beginBulkLoad(request)
                .thenApply(response -> new BulkLoadBuildContext(tablePath, response));
    }

    /** Returns transaction status and bucket progress for the original targets. */
    public CompletableFuture<BulkLoadInfo> getBulkLoadInfo(BulkLoadHandle handle) {
        GetBulkLoadInfoRequest request = new GetBulkLoadInfoRequest();
        handle.copyTo(request.setHandle());
        return gateway.getBulkLoadInfo(request).thenApply(BulkLoadInfo::new);
    }

    /**
     * Lists running transactions owning any selected physical target.
     *
     * <p>An empty partition list queries all current targets of the table.
     */
    public CompletableFuture<List<BulkLoadStatus>> listInProgressBulkLoads(
            TablePath tablePath, List<String> partitionNames) {
        ListInProgressBulkLoadsRequest request = new ListInProgressBulkLoadsRequest();
        setTablePath(request.setTablePath(), tablePath);
        partitionNames.forEach(request::addPartitionName);
        return gateway.listInProgressBulkLoads(request)
                .thenApply(
                        response ->
                                Collections.unmodifiableList(
                                        response.getTransactionsList().stream()
                                                .map(BulkLoadStatus::fromProto)
                                                .collect(Collectors.toList())));
    }

    /**
     * Finalizes the transaction after all buckets of its active targets have been committed.
     *
     * <p>Each bucket snapshot is published when it is registered with the coordinator.
     */
    public CompletableFuture<BulkLoadStatus> commit(BulkLoadHandle handle) {
        CommitBulkLoadRequest request = new CommitBulkLoadRequest();
        handle.copyTo(request.setHandle());
        return gateway.commitBulkLoad(request)
                .thenApply(response -> BulkLoadStatus.fromProto(response.getStatus()));
    }

    /** Stops the transaction while retaining any previously committed bucket snapshots. */
    public CompletableFuture<BulkLoadStatus> abort(
            BulkLoadHandle handle, @Nullable String abortMessage) {
        AbortBulkLoadRequest request = new AbortBulkLoadRequest();
        handle.copyTo(request.setHandle());
        if (abortMessage != null) {
            request.setAbortMessage(abortMessage);
        }
        return gateway.abortBulkLoad(request)
                .thenApply(response -> BulkLoadStatus.fromProto(response.getStatus()));
    }

    /**
     * Opens a bucket writer or returns empty once the allocated snapshot is committed.
     *
     * <p>If a previous attempt finished uploading the snapshot, this method registers that snapshot
     * with the coordinator before returning empty. The caller owns and must close any returned
     * writer.
     */
    public CompletableFuture<Optional<BulkLoadBucketWriter>> tryOpenBucketWriter(
            BulkLoadBuildContext.Target target, int bucketId) {
        checkArgument(
                bucketId >= 0 && bucketId < target.getBucketCount(),
                "Invalid bucket ID %s for target %s with %s buckets",
                bucketId,
                target.getPath(),
                target.getBucketCount());

        AllocateBulkLoadBucketSnapshotRequest request =
                new AllocateBulkLoadBucketSnapshotRequest().setBucketId(bucketId);
        target.getHandle().copyTo(request.setHandle());
        Long partitionId = target.getPartitionId();
        if (partitionId != null) {
            request.setPartitionId(partitionId);
        }

        CompletableFuture<Optional<BulkLoadBucketWriter>> result = new CompletableFuture<>();
        gateway.allocateBulkLoadBucketSnapshot(request)
                .<Optional<BulkLoadBucketWriter>>thenComposeAsync(
                        allocation -> {
                            if (result.isCancelled() || allocation.getState() == BUCKET_COMMITTED) {
                                return CompletableFuture.completedFuture(Optional.empty());
                            }
                            if (allocation.getState() != BUCKET_ALLOCATED) {
                                return completedExceptionally(
                                        new IllegalStateException(
                                                "Unknown BulkLoad bucket state: "
                                                        + allocation.getState()));
                            }
                            Supplier<CompletableFuture<Void>> register =
                                    () -> commitBucket(target, bucketId);
                            try {
                                if (BulkLoadBucketWriter.markerExists(
                                        target, bucketId, allocation.getSnapshotId())) {
                                    return register.get().thenApply(ignored -> Optional.empty());
                                }
                                return CompletableFuture.completedFuture(
                                        Optional.of(
                                                new BulkLoadBucketWriter(
                                                        target,
                                                        bucketId,
                                                        allocation,
                                                        Paths.get(
                                                                System.getProperty(
                                                                        "java.io.tmpdir")),
                                                        register)));
                            } catch (IOException e) {
                                return completedExceptionally(e);
                            }
                        })
                .whenComplete(
                        (writer, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                            } else if (!result.complete(writer)) {
                                writer.ifPresent(IOUtils::closeQuietly);
                            }
                        });
        return result;
    }

    /** Stops the connection-owned metadata refresh executor. */
    @Internal
    public void close() {
        metadataRefreshExecutor.shutdown();
    }

    private CompletableFuture<Void> commitBucket(BulkLoadBuildContext.Target target, int bucketId) {
        CommitBulkLoadBucketSnapshotRequest request =
                new CommitBulkLoadBucketSnapshotRequest().setBucketId(bucketId);
        target.getHandle().copyTo(request.setHandle());
        Long partitionId = target.getPartitionId();
        if (partitionId != null) {
            request.setPartitionId(partitionId);
        }
        return gateway.commitBulkLoadBucketSnapshot(request).thenApply(ignored -> null);
    }

    private static void setTablePath(PbTablePath requestPath, TablePath tablePath) {
        requestPath
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
    }
}
