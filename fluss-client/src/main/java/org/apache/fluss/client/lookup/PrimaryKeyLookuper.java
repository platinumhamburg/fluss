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

package org.apache.fluss.client.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.TsValueDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionIdAsync;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** An implementation of {@link Lookuper} that lookups by primary key. */
@NotThreadSafe
class PrimaryKeyLookuper implements Lookuper {

    private final TableInfo tableInfo;

    private final MetadataUpdater metadataUpdater;

    private final LookupClient lookupClient;

    private final KeyEncoder primaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    /** Decode the lookup bytes to result row. */
    private final ValueDecoder kvValueDecoder;

    /** Decode the lookup bytes for values with timestamp encoding. */
    private final TsValueDecoder kvTsValueDecoder;

    /** Whether the values are encoded with timestamp (TsValueEncoder). */
    private final boolean shouldUseTsDecoding;

    public PrimaryKeyLookuper(
            TableInfo tableInfo, MetadataUpdater metadataUpdater, LookupClient lookupClient) {
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.tableInfo = tableInfo;
        this.numBuckets = tableInfo.getNumBuckets();
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;

        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(tableInfo.getPrimaryKeys());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        // the encoded primary key is the physical primary key
        this.primaryKeyEncoder =
                KeyEncoder.of(lookupRowType, tableInfo.getPhysicalPrimaryKeys(), lakeFormat);
        this.bucketKeyEncoder =
                tableInfo.isDefaultBucketKey()
                        ? primaryKeyEncoder
                        : KeyEncoder.of(lookupRowType, tableInfo.getBucketKeys(), lakeFormat);
        this.bucketingFunction = BucketingFunction.of(lakeFormat);

        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;

        // Determine if values are encoded with timestamp
        // Currently, only index tables use TsValueEncoder
        this.shouldUseTsDecoding =
                IndexTableUtils.isIndexTable(tableInfo.getTablePath().getTableName());
        RowDecoder rowDecoder =
                RowDecoder.create(
                        tableInfo.getTableConfig().getKvFormat(),
                        tableInfo.getRowType().getChildren().toArray(new DataType[0]));
        this.kvValueDecoder = new ValueDecoder(rowDecoder);
        this.kvTsValueDecoder = new TsValueDecoder(rowDecoder);
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        try {
            // encoding the key row using a compacted way consisted with how the key is encoded when
            // put
            // a row
            // Synchronize only the KeyEncoder usage to avoid blocking on network I/O
            final byte[] pkBytes;
            final byte[] bkBytes;
            synchronized (this) {
                pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
                bkBytes =
                        bucketKeyEncoder == primaryKeyEncoder
                                ? pkBytes
                                : bucketKeyEncoder.encodeKey(lookupKey);
            }

            // If partition getter is present, we need to get partition ID asynchronously
            if (partitionGetter != null) {
                // Use async version to avoid blocking Netty IO threads
                return getPartitionIdAsync(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater)
                        .thenCompose(
                                partitionId -> {
                                    int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
                                    TableBucket tableBucket =
                                            new TableBucket(
                                                    tableInfo.getTableId(), partitionId, bucketId);
                                    return lookupClient.lookup(tableBucket, pkBytes);
                                })
                        .thenApply(this::decodeValue)
                        .exceptionally(
                                e -> {
                                    // Handle PartitionNotExistException by returning empty result
                                    // Use ExceptionUtils.findThrowable to search through the
                                    // entire exception chain, as the exception may be wrapped in
                                    // multiple layers of CompletionException
                                    if (ExceptionUtils.findThrowable(
                                                    e, PartitionNotExistException.class)
                                            .isPresent()) {
                                        return new LookupResult(Collections.emptyList());
                                    }
                                    throw new CompletionException(e);
                                });
            } else {
                // No partition, directly perform lookup
                int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), null, bucketId);
                return lookupClient.lookup(tableBucket, pkBytes).thenApply(this::decodeValue);
            }
        } catch (Exception e) {
            return FutureUtils.failedCompletableFuture(e);
        }
    }

    private LookupResult decodeValue(byte[] valueBytes) {
        InternalRow row;
        if (valueBytes == null) {
            row = null;
        } else if (shouldUseTsDecoding) {
            // For values encoded with timestamp, use TsValueDecoder
            row = kvTsValueDecoder.decodeValue(valueBytes).row;
        } else {
            // For normal values, use ValueDecoder
            row = kvValueDecoder.decodeValue(valueBytes).row;
        }
        return new LookupResult(row);
    }
}
