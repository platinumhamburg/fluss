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
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.BulkLoadTargetInfo;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The result of beginning or finding an existing BulkLoad transaction. */
@Internal
public final class BeginBulkLoadResult {

    private final boolean created;
    private final BulkLoadStatus status;
    private final @Nullable BulkLoadTargetInfo targetInfo;

    /** Creates a BulkLoad Begin result. */
    public BeginBulkLoadResult(
            boolean created, BulkLoadStatus status, @Nullable BulkLoadTargetInfo targetInfo) {
        this.status = checkNotNull(status, "BulkLoad status must not be null.");

        if (targetInfo != null) {
            checkArgument(
                    status.getState() == BulkLoadState.BEGUN,
                    "BulkLoad target info requires a begun transaction.");
        } else if (created) {
            checkArgument(
                    status.getState() == BulkLoadState.ABORTED,
                    "A created BulkLoad result without target info must be aborted.");
        }

        if (targetInfo != null) {
            checkArgument(
                    status.getHandle().equals(targetInfo.getHandle()),
                    "BulkLoad status and target info must identify the same handle.");
        }

        this.created = created;
        this.targetInfo = targetInfo;
    }

    /** Returns whether this request created the transaction. */
    public boolean isCreated() {
        return created;
    }

    /** Returns the persisted transaction status. */
    public BulkLoadStatus getStatus() {
        return status;
    }

    /** Returns the frozen input-generation contract for a fence-ready begun transaction. */
    @Nullable
    public BulkLoadTargetInfo getTargetInfo() {
        return targetInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BeginBulkLoadResult that = (BeginBulkLoadResult) o;
        return created == that.created
                && Objects.equals(status, that.status)
                && Objects.equals(targetInfo, that.targetInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, status, targetInfo);
    }

    @Override
    public String toString() {
        return "BeginBulkLoadResult{"
                + "created="
                + created
                + ", status="
                + status
                + ", targetInfo="
                + targetInfo
                + '}';
    }
}
