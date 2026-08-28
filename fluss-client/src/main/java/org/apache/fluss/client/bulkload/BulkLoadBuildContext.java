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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.bulkload.protocol.BulkLoadTargetInfoSerde;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Immutable context for building the files of one BulkLoad transaction.
 *
 * <p>The context is returned by {@link BulkLoadClient#begin} and may be serialized for transport to
 * distributed builders. Its frozen {@link #getTableInfo() table information} defines the schema,
 * bucket count, bucket keys, formats, and remote data directory that every bucket writer must use.
 * It does not expose the internal RPC representation.
 */
@PublicEvolving
public final class BulkLoadBuildContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] serialized;
    private transient BulkLoadTargetInfo targetInfo;
    private transient BucketRouting bucketRouting;

    BulkLoadBuildContext(BulkLoadTargetInfo targetInfo) {
        this.targetInfo = checkNotNull(targetInfo, "BulkLoad target info must not be null.");
        this.serialized = BulkLoadTargetInfoSerde.toBytes(targetInfo);
    }

    /** Returns the transaction handle. */
    public BulkLoadHandle getHandle() {
        return targetInfo().getHandle();
    }

    /** Returns the table information frozen when the transaction began. */
    public TableInfo getTableInfo() {
        return targetInfo().getTableInfo();
    }

    /** Returns the bucket that must own the given full-schema row. */
    public synchronized int bucketOf(InternalRow row) {
        checkNotNull(row, "BulkLoad input row must not be null.");
        if (bucketRouting == null) {
            TableInfo tableInfo = targetInfo().getTableInfo();
            DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
            bucketRouting =
                    new BucketRouting(
                            KeyEncoder.ofBucketKeyEncoder(
                                    tableInfo.getRowType(), tableInfo.getBucketKeys(), lakeFormat),
                            BucketingFunction.of(lakeFormat),
                            tableInfo.getNumBuckets());
        }
        return bucketRouting.bucketOf(row);
    }

    BulkLoadTargetInfo targetInfo() {
        if (targetInfo == null) {
            targetInfo = BulkLoadTargetInfoSerde.fromBytes(serialized);
        }
        return targetInfo;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || (o instanceof BulkLoadBuildContext
                        && Arrays.equals(serialized, ((BulkLoadBuildContext) o).serialized));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serialized);
    }

    private static final class BucketRouting {
        private final KeyEncoder bucketKeyEncoder;
        private final BucketingFunction bucketingFunction;
        private final int numBuckets;

        private BucketRouting(
                KeyEncoder bucketKeyEncoder, BucketingFunction bucketingFunction, int numBuckets) {
            this.bucketKeyEncoder = bucketKeyEncoder;
            this.bucketingFunction = bucketingFunction;
            this.numBuckets = numBuckets;
        }

        private int bucketOf(InternalRow row) {
            return bucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), numBuckets);
        }
    }
}
