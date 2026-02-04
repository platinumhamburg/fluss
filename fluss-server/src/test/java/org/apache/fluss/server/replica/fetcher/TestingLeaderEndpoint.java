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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchIndexLogResultForBucket;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchIndexRequest;
import org.apache.fluss.rpc.messages.FetchIndexResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.server.entity.DataBucketIndexFetchResult;
import org.apache.fluss.server.entity.FetchIndexReqInfo;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.index.FetchIndexParams;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getIndexReqMap;
import static org.apache.fluss.utils.function.ThrowingRunnable.unchecked;

/** The leader end point used for test, which replica manager in local. */
public class TestingLeaderEndpoint implements LeaderEndpoint {

    private final ReplicaManager replicaManager;
    private final ServerNode localNode;
    /** The max size for the fetch response. */
    private final int maxFetchSize;
    /** The max fetch size for a bucket in bytes. */
    private final int maxFetchSizeForBucket;

    public TestingLeaderEndpoint(
            Configuration conf, ReplicaManager replicaManager, ServerNode localNode) {
        this.replicaManager = replicaManager;
        this.localNode = localNode;
        this.maxFetchSize = (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES).getBytes();
        this.maxFetchSizeForBucket =
                (int) conf.get(ConfigOptions.LOG_REPLICA_FETCH_MAX_BYTES_FOR_BUCKET).getBytes();
    }

    @Override
    public int leaderServerId() {
        return localNode.id();
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogEndOffset(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLocalLogEndOffset());
    }

    @Override
    public CompletableFuture<Long> fetchLocalLogStartOffset(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLocalLogStartOffset());
    }

    @Override
    public CompletableFuture<Long> fetchLeaderEndOffsetSnapshot(TableBucket tableBucket) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        return CompletableFuture.completedFuture(replica.getLeaderEndOffsetSnapshot());
    }

    @Override
    public CompletableFuture<FetchData> fetchLog(FetchLogContext fetchLogContext) {
        CompletableFuture<FetchData> response = new CompletableFuture<>();
        FetchLogRequest fetchLogRequest = fetchLogContext.getFetchLogRequest();
        Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(fetchLogRequest);
        replicaManager.fetchLogRecords(
                new FetchParams(
                        fetchLogRequest.getFollowerServerId(), fetchLogRequest.getMaxBytes()),
                fetchLogData,
                null,
                result ->
                        response.complete(
                                new FetchData(new FetchLogResponse(), processResult(result))));
        return response;
    }

    @Override
    public CompletableFuture<FetchIndexData> fetchIndex(FetchIndexContext fetchIndexContext) {
        CompletableFuture<FetchIndexData> response = new CompletableFuture<>();
        FetchIndexRequest fetchIndexRequest = fetchIndexContext.getFetchIndexRequest();
        Map<TableBucket, Map<TableBucket, FetchIndexReqInfo>> fetchIndexReqMap =
                getIndexReqMap(fetchIndexRequest);
        replicaManager.fetchIndexRecords(
                new FetchIndexParams(
                        fetchIndexRequest.getMaxBytes(),
                        fetchIndexRequest.getMinAdvanceOffset(),
                        fetchIndexRequest.getMaxWaitMs()),
                fetchIndexReqMap,
                result ->
                        response.complete(
                                new FetchIndexData(
                                        new FetchIndexResponse(), processIndexResult(result))));
        return response;
    }

    @Override
    public Optional<FetchLogContext> buildFetchLogContext(
            Map<TableBucket, BucketFetchStatus> replicas) {
        return RemoteLeaderEndpoint.buildFetchLogContext(
                replicas, localNode.id(), maxFetchSize, maxFetchSizeForBucket, -1, -1);
    }

    @Override
    public void close() {
        // nothing to do now.
    }

    /** Convert FileLogRecords to MemoryLogRecords. */
    private Map<TableBucket, FetchLogResultForBucket> processResult(
            Map<TableBucket, FetchLogResultForBucket> fetchDataMap) {
        Map<TableBucket, FetchLogResultForBucket> result = new HashMap<>();
        fetchDataMap.forEach(
                (tb, value) -> {
                    LogRecords logRecords = value.recordsOrEmpty();
                    if (logRecords instanceof FileLogRecords) {
                        FileLogRecords fileRecords = (FileLogRecords) logRecords;
                        // convert FileLogRecords to MemoryLogRecords
                        ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());
                        unchecked(() -> fileRecords.readInto(buffer, 0)).run();
                        MemoryLogRecords memRecords = MemoryLogRecords.pointToByteBuffer(buffer);
                        result.put(
                                tb,
                                new FetchLogResultForBucket(
                                        tb, memRecords, value.getHighWatermark()));
                    } else {
                        result.put(tb, value);
                    }
                });

        return result;
    }

    /** Convert DataBucketIndexFetchResult to FetchIndexLogResultForBucket map. */
    private Map<DataIndexTableBucket, FetchIndexLogResultForBucket> processIndexResult(
            Map<TableBucket, DataBucketIndexFetchResult> fetchDataMap) {
        Map<DataIndexTableBucket, FetchIndexLogResultForBucket> result = new HashMap<>();

        for (Map.Entry<TableBucket, DataBucketIndexFetchResult> entry : fetchDataMap.entrySet()) {
            TableBucket dataBucket = entry.getKey();
            DataBucketIndexFetchResult dataBucketResult = entry.getValue();

            Map<TableBucket, FetchIndexLogResultForBucket> indexBucketResults =
                    dataBucketResult.getIndexBucketResults();

            for (Map.Entry<TableBucket, FetchIndexLogResultForBucket> indexEntry :
                    indexBucketResults.entrySet()) {
                TableBucket indexBucket = indexEntry.getKey();
                FetchIndexLogResultForBucket indexResult = indexEntry.getValue();

                // Create DataIndexTableBucket as the key
                DataIndexTableBucket dataIndexBucket =
                        new DataIndexTableBucket(dataBucket, indexBucket);

                // Convert FileLogRecords or BytesViewLogRecords to MemoryLogRecords if needed
                // This simulates network serialization/deserialization
                LogRecords logRecords = indexResult.recordsOrEmpty();
                if (logRecords instanceof FileLogRecords) {
                    FileLogRecords fileRecords = (FileLogRecords) logRecords;
                    // convert FileLogRecords to MemoryLogRecords
                    ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());
                    unchecked(() -> fileRecords.readInto(buffer, 0)).run();
                    MemoryLogRecords memRecords = MemoryLogRecords.pointToByteBuffer(buffer);

                    FetchIndexLogResultForBucket convertedResult =
                            new FetchIndexLogResultForBucket(
                                    memRecords,
                                    indexResult.getStartOffset(),
                                    indexResult.getEndOffset(),
                                    indexResult.isDataReady());
                    result.put(dataIndexBucket, convertedResult);
                } else if (logRecords instanceof BytesViewLogRecords) {
                    // Convert BytesViewLogRecords to MemoryLogRecords
                    // This simulates what happens during network transfer
                    // We need to properly copy the bytes to a contiguous buffer
                    BytesViewLogRecords bytesViewRecords = (BytesViewLogRecords) logRecords;
                    int size = bytesViewRecords.sizeInBytes();
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    // Use shaded Netty's ByteBuf to properly read all bytes into the buffer
                    org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf byteBuf =
                            bytesViewRecords.getBytesView().getByteBuf();

                    // Debug log
                    System.out.println(
                            "DEBUG TestingLeaderEndpoint: bytesViewRecords.size="
                                    + size
                                    + ", byteBuf.readableBytes="
                                    + byteBuf.readableBytes()
                                    + ", byteBuf.readerIndex="
                                    + byteBuf.readerIndex());

                    byteBuf.readBytes(buffer);
                    buffer.flip();

                    // Debug log after conversion
                    System.out.println(
                            "DEBUG TestingLeaderEndpoint: buffer.remaining="
                                    + buffer.remaining()
                                    + ", buffer.position="
                                    + buffer.position()
                                    + ", buffer.limit="
                                    + buffer.limit());

                    MemoryLogRecords memRecords = MemoryLogRecords.pointToByteBuffer(buffer);

                    // Debug log final MemoryLogRecords
                    System.out.println(
                            "DEBUG TestingLeaderEndpoint: memRecords.sizeInBytes="
                                    + memRecords.sizeInBytes()
                                    + ", memRecords.position="
                                    + memRecords.getPosition()
                                    + ", segment.size="
                                    + memRecords.getMemorySegment().size());

                    FetchIndexLogResultForBucket convertedResult =
                            new FetchIndexLogResultForBucket(
                                    memRecords,
                                    indexResult.getStartOffset(),
                                    indexResult.getEndOffset(),
                                    indexResult.isDataReady());
                    result.put(dataIndexBucket, convertedResult);
                } else {
                    result.put(dataIndexBucket, indexResult);
                }
            }
        }

        return result;
    }
}
