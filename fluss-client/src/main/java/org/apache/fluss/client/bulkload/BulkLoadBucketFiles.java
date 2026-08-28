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
import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;

import java.io.Serializable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Opaque immutable description of the final files produced for one BulkLoad bucket.
 *
 * <p>Applications obtain this object from {@link BulkLoadBucketWriter#finish()} or {@link
 * BulkLoadBucketWriter#finishAtLogEndOffset(long)}, may serialize it between distributed build and
 * commit tasks, and pass the complete bucket collection to {@link BulkLoadClient#commit}.
 * Applications do not construct it or interpret its internal Snapshot file handle.
 */
@PublicEvolving
public final class BulkLoadBucketFiles implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bulkLoadId;
    private final int bucketId;
    private final BulkLoadFileHandle snapshotMetadata;

    /** Creates the immutable final metadata handles for one bucket. */
    BulkLoadBucketFiles(String bulkLoadId, int bucketId, BulkLoadFileHandle snapshotMetadata) {
        this.bulkLoadId = checkNotNull(bulkLoadId, "BulkLoad id must not be null.");
        checkArgument(bucketId >= 0, "BulkLoad bucket id must be non-negative.");
        this.snapshotMetadata =
                checkNotNull(snapshotMetadata, "BulkLoad snapshot metadata must not be null.");
        this.bucketId = bucketId;
    }

    String getBulkLoadId() {
        return bulkLoadId;
    }

    int getBucketId() {
        return bucketId;
    }

    BulkLoadFileHandle getSnapshotMetadata() {
        return snapshotMetadata;
    }
}
