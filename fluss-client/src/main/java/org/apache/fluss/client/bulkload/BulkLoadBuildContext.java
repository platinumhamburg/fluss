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
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.bulkload.protocol.BulkLoadMessageUtils;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbTableInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Frozen, serializable input contract for all targets of one BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadBuildContext implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] serialized;
    private transient List<Target> targets;

    /** Creates a build context from a successful Begin response. */
    @Internal
    public BulkLoadBuildContext(BeginBulkLoadResponse response) {
        serialized = response.toByteArray();
    }

    /** Returns the shared transaction handle. */
    public BulkLoadHandle getHandle() {
        return getTargets().get(0).getHandle();
    }

    /** Returns the frozen targets ordered by physical partition ID. */
    public synchronized List<Target> getTargets() {
        if (targets == null) {
            BeginBulkLoadResponse response = new BeginBulkLoadResponse();
            response.parseFrom(serialized);
            BulkLoadHandle handle = BulkLoadMessageUtils.toBulkLoadHandle(response.getHandle());
            List<Target> decoded = new ArrayList<>();
            for (PbBulkLoadTargetInfo info : response.getTargetInfosList()) {
                decoded.add(new Target(handle, info));
            }
            decoded.sort(
                    Comparator.comparingLong(
                            t -> t.getPartitionId() == null ? -1L : t.getPartitionId()));
            targets = Collections.unmodifiableList(decoded);
        }
        return targets;
    }

    /** Returns the frozen input contract for one physical target. */
    public Target getTarget(PhysicalTablePath path) {
        for (Target target : getTargets()) {
            if (target.getPath().equals(path)) {
                return target;
            }
        }
        throw new IllegalArgumentException(
                "Target does not belong to this BulkLoad transaction: " + path);
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof BulkLoadBuildContext
                        && Arrays.equals(serialized, ((BulkLoadBuildContext) other).serialized));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serialized);
    }

    /** Frozen table and snapshot information for one table or physical partition. */
    @PublicEvolving
    public static final class Target {
        private final BulkLoadHandle handle;
        private final PhysicalTablePath path;
        @Nullable private final Long partitionId;
        private final TableInfo tableInfo;
        private final long[] snapshotIds;
        private final KeyEncoder bucketKeyEncoder;
        private final BucketingFunction bucketingFunction;

        private Target(BulkLoadHandle handle, PbBulkLoadTargetInfo info) {
            this.handle = handle;
            path = BulkLoadMessageUtils.toPhysicalTablePath(info.getTarget());
            partitionId = info.hasPartitionId() ? info.getPartitionId() : null;
            PbTableInfo frozenTable = info.getTableInfo();
            tableInfo =
                    TableInfo.of(
                            path.getTablePath(),
                            frozenTable.getTableId(),
                            frozenTable.getSchemaId(),
                            TableDescriptor.fromJsonBytes(frozenTable.getTableJson()),
                            frozenTable.hasRemoteDataDir() ? frozenTable.getRemoteDataDir() : null,
                            frozenTable.getCreatedTime(),
                            frozenTable.getModifiedTime(),
                            frozenTable.hasBucketCountEpoch()
                                    ? frozenTable.getBucketCountEpoch()
                                    : 0L);
            snapshotIds = info.getSnapshotIds().clone();
            DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
            bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            tableInfo.getRowType(), tableInfo.getBucketKeys(), lakeFormat);
            bucketingFunction = BucketingFunction.of(lakeFormat);
        }

        /** Returns the shared transaction handle. */
        public BulkLoadHandle getHandle() {
            return handle;
        }

        /** Returns the physical table or partition path. */
        public PhysicalTablePath getPath() {
            return path;
        }

        /** Returns the partition ID, or null for a non-partitioned table. */
        @Nullable
        public Long getPartitionId() {
            return partitionId;
        }

        /** Returns the frozen table schema and bucket layout. */
        public TableInfo getTableInfo() {
            return tableInfo;
        }

        /** Returns the allocated snapshot ID for one bucket. */
        public long getSnapshotId(int bucketId) {
            return snapshotIds[bucketId];
        }

        /** Returns the owning bucket of a full-schema row within this target. */
        public synchronized int bucketOf(InternalRow row) {
            return bucketingFunction.bucketing(
                    bucketKeyEncoder.encodeKey(checkNotNull(row, "BulkLoad row must not be null.")),
                    tableInfo.getNumBuckets());
        }
    }
}
