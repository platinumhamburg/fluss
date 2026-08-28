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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbPhysicalTablePath;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Serializes and deserializes {@link BulkLoadTargetInfo} to and from its protobuf byte
 * representation.
 *
 * <p>{@link BulkLoadTargetInfo} is not {@link java.io.Serializable}; the byte form produced here is
 * used to ship the input-generation contract frozen by a BulkLoad Begin request across process
 * boundaries, for example when a Flink Begin operator broadcasts it to the Build and Commit
 * operators.
 *
 * <p>The class also exposes the underlying model-to-protobuf conversion tree for callers that
 * already work with the protobuf messages, such as the BulkLoad admin response parsing path.
 */
@Internal
public final class BulkLoadTargetInfoSerde {

    private BulkLoadTargetInfoSerde() {}

    /** Serializes the given target info into its protobuf byte representation. */
    public static byte[] toBytes(BulkLoadTargetInfo targetInfo) {
        return toPbBulkLoadTargetInfo(targetInfo).toByteArray();
    }

    /**
     * Deserializes a target info from its protobuf byte representation.
     *
     * @throws RuntimeException if the bytes cannot be decoded or fail validation
     */
    public static BulkLoadTargetInfo fromBytes(byte[] bytes) {
        checkNotNull(bytes, "BulkLoad target info bytes must not be null.");
        PbBulkLoadTargetInfo pbTargetInfo = new PbBulkLoadTargetInfo();
        pbTargetInfo.parseFrom(bytes);
        return toBulkLoadTargetInfo(pbTargetInfo);
    }

    /** Converts the given target info into its protobuf message representation. */
    public static PbBulkLoadTargetInfo toPbBulkLoadTargetInfo(BulkLoadTargetInfo targetInfo) {
        checkNotNull(targetInfo, "BulkLoad target info must not be null.");
        TableInfo tableInfo = targetInfo.getTableInfo();
        PbBulkLoadTargetInfo pbTargetInfo =
                new PbBulkLoadTargetInfo()
                        .setHandle(toPbBulkLoadHandle(targetInfo.getHandle()))
                        .setSchemaId(tableInfo.getSchemaId())
                        .setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                        .setCreatedTime(tableInfo.getCreatedTime())
                        .setModifiedTime(tableInfo.getModifiedTime())
                        .setSnapshotIds(snapshotIds(targetInfo));
        if (tableInfo.getRemoteDataDir() != null) {
            pbTargetInfo.setRemoteDataDir(tableInfo.getRemoteDataDir());
        }
        return pbTargetInfo;
    }

    /**
     * Converts the given protobuf message into a validated {@link BulkLoadTargetInfo}.
     *
     * @throws IllegalArgumentException if the message fails validation
     * @throws IllegalStateException if a required field of the message is not set
     */
    public static BulkLoadTargetInfo toBulkLoadTargetInfo(PbBulkLoadTargetInfo targetInfo) {
        checkNotNull(targetInfo, "BulkLoad protobuf target info must not be null.");
        BulkLoadHandle handle = toBulkLoadHandle(targetInfo.getHandle());
        int schemaId = targetInfo.getSchemaId();
        if (schemaId < 0) {
            throw new IllegalArgumentException("BulkLoad schema ID must be non-negative.");
        }
        TableInfo tableInfo =
                TableInfo.of(
                        handle.getTarget().getTablePath(),
                        handle.getTableId(),
                        schemaId,
                        TableDescriptor.fromJsonBytes(targetInfo.getTableJson()),
                        targetInfo.hasRemoteDataDir() ? targetInfo.getRemoteDataDir() : null,
                        targetInfo.getCreatedTime(),
                        targetInfo.getModifiedTime());

        return new BulkLoadTargetInfo(handle, tableInfo, targetInfo.getSnapshotIds());
    }

    /** Converts the given BulkLoad handle into its protobuf message representation. */
    public static PbBulkLoadHandle toPbBulkLoadHandle(BulkLoadHandle handle) {
        checkNotNull(handle, "BulkLoad handle must not be null.");
        PbBulkLoadHandle pbHandle =
                new PbBulkLoadHandle()
                        .setTarget(toPbPhysicalTablePath(handle.getTarget()))
                        .setTableId(handle.getTableId())
                        .setBulkLoadId(handle.getBulkLoadId());
        if (handle.getPartitionId() != null) {
            pbHandle.setPartitionId(handle.getPartitionId());
        }
        return pbHandle;
    }

    /** Converts the given protobuf message into a BulkLoad handle. */
    public static BulkLoadHandle toBulkLoadHandle(PbBulkLoadHandle handle) {
        checkNotNull(handle, "BulkLoad protobuf handle must not be null.");
        return new BulkLoadHandle(
                toPhysicalTablePath(handle.getTarget()),
                handle.getTableId(),
                handle.hasPartitionId() ? handle.getPartitionId() : null,
                handle.getBulkLoadId());
    }

    /** Converts the given physical table path into its protobuf message representation. */
    public static PbPhysicalTablePath toPbPhysicalTablePath(PhysicalTablePath target) {
        checkNotNull(target, "BulkLoad target must not be null.");
        PbPhysicalTablePath pbTarget =
                new PbPhysicalTablePath()
                        .setDatabaseName(target.getDatabaseName())
                        .setTableName(target.getTableName());
        if (target.getPartitionName() != null) {
            pbTarget.setPartitionName(target.getPartitionName());
        }
        return pbTarget;
    }

    /** Converts the given protobuf message into a physical table path. */
    public static PhysicalTablePath toPhysicalTablePath(PbPhysicalTablePath target) {
        checkNotNull(target, "BulkLoad protobuf target must not be null.");
        TablePath tablePath = TablePath.of(target.getDatabaseName(), target.getTableName());
        return PhysicalTablePath.of(
                tablePath, target.hasPartitionName() ? target.getPartitionName() : null);
    }

    private static long[] snapshotIds(BulkLoadTargetInfo targetInfo) {
        int numBuckets = targetInfo.getTableInfo().getNumBuckets();
        long[] snapshotIds = new long[numBuckets];
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            snapshotIds[bucketId] = targetInfo.getSnapshotId(bucketId);
        }
        return snapshotIds;
    }
}
