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
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Immutable result of beginning or recovering a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadBeginResult {

    private final BulkLoadStatus status;
    private final @Nullable BulkLoadBuildContext buildContext;

    BulkLoadBeginResult(BulkLoadStatus status, @Nullable BulkLoadBuildContext buildContext) {
        this.status = checkNotNull(status, "BulkLoad status must not be null.");
        if (buildContext != null) {
            checkArgument(
                    status.getState() == BulkLoadState.BEGUN,
                    "BulkLoad build context requires a begun transaction.");
            checkArgument(
                    status.getHandle().equals(buildContext.getHandle()),
                    "BulkLoad status and build context must identify the same handle.");
        }
        this.buildContext = buildContext;
    }

    /**
     * Returns whether callers must build and commit the transaction.
     *
     * <p>When this returns {@code false}, the recovered transaction has already reached the status
     * returned by {@link #getStatus()} and no bucket files should be built.
     */
    public boolean isBuildRequired() {
        return buildContext != null;
    }

    /**
     * Returns the context for building the transaction's bucket files.
     *
     * @throws IllegalStateException if {@link #isBuildRequired()} is {@code false}
     */
    public BulkLoadBuildContext getBuildContext() {
        checkState(buildContext != null, "This BulkLoad transaction does not require building.");
        return buildContext;
    }

    /** Returns the persisted transaction status reached by Begin or recovery. */
    public BulkLoadStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadBeginResult that = (BulkLoadBeginResult) o;
        return Objects.equals(status, that.status)
                && Objects.equals(buildContext, that.buildContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, buildContext);
    }

    @Override
    public String toString() {
        return "BulkLoadBeginResult{"
                + "status="
                + status
                + ", buildRequired="
                + isBuildRequired()
                + '}';
    }
}
