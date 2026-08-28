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

package org.apache.fluss.flink.sink.bulkload;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Computes the Fluss bucket id of a full-schema row for the BulkLoad sink.
 *
 * <p>The bucket-partitioned data edge uses this computation to route rows to per-bucket build
 * subtasks. The public bucket writer independently validates the assigned bucket at its API
 * boundary.
 *
 * <p>The encoding path is the one used by the regular Fluss sink: the bucket key bytes come from
 * {@link KeyEncoder#ofBucketKeyEncoder} and are reduced by {@link BucketingFunction#of} over the
 * number of buckets of the table.
 *
 * <p>Not thread-safe; instances are confined to a single task thread.
 */
final class BulkLoadBucketAssigner implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final List<String> bucketKeys;
    private final @Nullable DataLakeFormat lakeFormat;
    private final int numBuckets;

    private transient @Nullable BucketingFunction bucketingFunction;
    private transient @Nullable KeyEncoder bucketKeyEncoder;

    BulkLoadBucketAssigner(
            RowType rowType,
            List<String> bucketKeys,
            @Nullable DataLakeFormat lakeFormat,
            int numBuckets) {
        this.rowType = checkNotNull(rowType, "Row type must not be null.");
        checkNotNull(bucketKeys, "Bucket keys must not be null.");
        checkArgument(!bucketKeys.isEmpty(), "Bucket keys must not be empty.");
        this.bucketKeys = Collections.unmodifiableList(new ArrayList<>(bucketKeys));
        this.lakeFormat = lakeFormat;
        checkArgument(numBuckets > 0, "Number of buckets must be positive.");
        this.numBuckets = numBuckets;
    }

    /** Returns the bucket id of the given full-schema row. */
    int assign(InternalRow row) {
        if (bucketingFunction == null) {
            bucketingFunction = BucketingFunction.of(lakeFormat);
            bucketKeyEncoder = KeyEncoder.ofBucketKeyEncoder(rowType, bucketKeys, lakeFormat);
        }
        return bucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), numBuckets);
    }
}
