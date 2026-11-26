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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;

import java.util.function.BiConsumer;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.hasHistoricalLookup;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.hasHistoricalPut;

/** Extracts KV routing metadata without decoding record contents. */
final class RequestAccessProvider {
    private RequestAccessProvider() {}

    static boolean describe(RpcRequest request, BiConsumer<TableBucket, Boolean> consumer) {
        boolean earlyRelease = false;
        if (request instanceof FlussRequest) {
            Object message = ((FlussRequest) request).getMessage();
            if (message instanceof PutKvRequest && !hasHistoricalPut((PutKvRequest) message)) {
                PutKvRequest put = (PutKvRequest) message;
                put.getBucketsReqsList()
                        .forEach(
                                bucket ->
                                        consumer.accept(
                                                new TableBucket(
                                                        put.getTableId(),
                                                        bucket.hasPartitionId()
                                                                ? bucket.getPartitionId()
                                                                : null,
                                                        bucket.getBucketId()),
                                                true));
            } else if (message instanceof LookupRequest
                    && !hasHistoricalLookup((LookupRequest) message)) {
                LookupRequest lookup = (LookupRequest) message;
                earlyRelease = lookup.getBucketsReqsCount() == 1;
                lookup.getBucketsReqsList()
                        .forEach(
                                bucket ->
                                        consumer.accept(
                                                new TableBucket(
                                                        lookup.getTableId(),
                                                        bucket.hasPartitionId()
                                                                ? bucket.getPartitionId()
                                                                : null,
                                                        bucket.getBucketId()),
                                                lookup.hasInsertIfNotExists()
                                                        && lookup.isInsertIfNotExists()));
            } else if (message instanceof PrefixLookupRequest) {
                PrefixLookupRequest lookup = (PrefixLookupRequest) message;
                earlyRelease =
                        lookup.getBucketsReqsCount() == 1
                                && lookup.getBucketsReqAt(0).getKeysCount() == 1;
                lookup.getBucketsReqsList()
                        .forEach(
                                bucket ->
                                        consumer.accept(
                                                new TableBucket(
                                                        lookup.getTableId(),
                                                        bucket.hasPartitionId()
                                                                ? bucket.getPartitionId()
                                                                : null,
                                                        bucket.getBucketId()),
                                                false));
            }
        }
        return earlyRelease;
    }
}
