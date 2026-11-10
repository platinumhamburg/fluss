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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbPhysicalTablePath;
import org.apache.fluss.rpc.messages.PbTablePath;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Converts BulkLoad transaction identities to and from RPC messages. */
@Internal
public final class BulkLoadMessageUtils {
    private BulkLoadMessageUtils() {}

    /** Converts the given BulkLoad handle into its protobuf message representation. */
    public static PbBulkLoadHandle toPbBulkLoadHandle(BulkLoadHandle handle) {
        checkNotNull(handle, "BulkLoad handle must not be null.");
        return new PbBulkLoadHandle()
                .setTablePath(
                        new PbTablePath()
                                .setDatabaseName(handle.getTablePath().getDatabaseName())
                                .setTableName(handle.getTablePath().getTableName()))
                .setTableId(handle.getTableId())
                .setBulkLoadId(handle.getBulkLoadId());
    }

    /** Converts a protobuf transaction handle. */
    public static BulkLoadHandle toBulkLoadHandle(PbBulkLoadHandle handle) {
        return new BulkLoadHandle(
                TablePath.of(
                        handle.getTablePath().getDatabaseName(),
                        handle.getTablePath().getTableName()),
                handle.getTableId(),
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
}
