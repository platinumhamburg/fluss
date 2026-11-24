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
import org.apache.fluss.rpc.messages.PbBulkLoadTargetProgress;

import javax.annotation.Nullable;

/** Current bucket progress for one physical BulkLoad target. */
@PublicEvolving
public final class BulkLoadTargetProgress {

    private final @Nullable Long partitionId;
    private final int bucketCount;
    private final long allocatedBucketCount;
    private final long committedBucketCount;
    private final boolean withdrawn;

    BulkLoadTargetProgress(PbBulkLoadTargetProgress progress) {
        partitionId = progress.hasPartitionId() ? progress.getPartitionId() : null;
        bucketCount = progress.getBucketCount();
        allocatedBucketCount = progress.getAllocatedBucketCount();
        committedBucketCount = progress.getCommittedBucketCount();
        withdrawn = progress.isWithdrawn();
    }

    /** Returns the partition ID, or null for a non-partitioned table. */
    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    /** Returns the number of buckets in the target. */
    public int getBucketCount() {
        return bucketCount;
    }

    /** Returns the number of allocated buckets, including committed buckets. */
    public long getAllocatedBucketCount() {
        return allocatedBucketCount;
    }

    /** Returns the number of buckets whose snapshots have been committed. */
    public long getCommittedBucketCount() {
        return committedBucketCount;
    }

    /** Returns whether the physical target has been dropped and withdrawn from the transaction. */
    public boolean isWithdrawn() {
        return withdrawn;
    }
}
