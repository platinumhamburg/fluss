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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.WriterKey;

import javax.annotation.Nullable;

import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Canonical encoding of a source {@link TableBucket} into an opaque {@link WriterKey}. */
public final class IndexWriterKey {
    private static final long PARTITIONED_MASK = Long.MIN_VALUE;
    private static final long BUCKET_MASK = Integer.MAX_VALUE;
    private static final long RESERVED_MASK = ~(PARTITIONED_MASK | BUCKET_MASK);

    private IndexWriterKey() {}

    public static WriterKey encode(TableBucket sourceBucket) {
        int bucketId = sourceBucket.getBucket();
        checkArgument(bucketId >= 0, "bucketId must be non-negative");
        Long partitionId = sourceBucket.getPartitionId();
        if (partitionId == null) {
            return new WriterKey(0L, bucketId);
        }
        checkArgument(partitionId >= 0L, "partitionId must be non-negative");
        return new WriterKey(partitionId, PARTITIONED_MASK | (long) bucketId);
    }

    public static SourceBucket decode(WriterKey writerKey) {
        long high = writerKey.high();
        long low = writerKey.low();
        checkArgument((low & RESERVED_MASK) == 0L, "WriterKey has reserved bits set");
        int bucketId = (int) (low & BUCKET_MASK);
        if ((low & PARTITIONED_MASK) == 0L) {
            checkArgument(high == 0L, "Unpartitioned WriterKey must have high=0");
            return new SourceBucket(null, bucketId);
        }
        checkArgument(high >= 0L, "partitionId must be non-negative");
        return new SourceBucket(high, bucketId);
    }

    /** Source bucket identity reconstructed from a canonical index writer key. */
    public static final class SourceBucket {
        private final @Nullable Long partitionId;
        private final int bucketId;

        private SourceBucket(@Nullable Long partitionId, int bucketId) {
            this.partitionId = partitionId;
            this.bucketId = bucketId;
        }

        public OptionalLong getPartitionId() {
            return partitionId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(partitionId);
        }

        public int getBucketId() {
            return bucketId;
        }
    }
}
