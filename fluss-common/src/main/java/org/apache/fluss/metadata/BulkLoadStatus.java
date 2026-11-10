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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable client-visible status of a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadStatus {

    private final BulkLoadHandle handle;
    private final BulkLoadState state;
    private final @Nullable BulkLoadAbortReason abortReason;
    private final @Nullable String abortMessage;

    /** Creates an immutable BulkLoad status. */
    public BulkLoadStatus(
            BulkLoadHandle handle,
            BulkLoadState state,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage) {
        this.handle = checkNotNull(handle, "BulkLoad handle must not be null.");
        this.state = checkNotNull(state, "BulkLoad state must not be null.");
        checkArgument(
                (state == BulkLoadState.ABORTED) == (abortReason != null),
                "BulkLoad abort reason must exist exactly in the ABORTED state.");
        checkArgument(
                abortMessage == null || state == BulkLoadState.ABORTED,
                "BulkLoad abort message is only valid in the ABORTED state.");
        this.abortReason = abortReason;
        this.abortMessage = abortMessage;
    }

    /** Returns the transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the transaction state. */
    public BulkLoadState getState() {
        return state;
    }

    /** Returns why the transaction was aborted, or {@code null} in a non-aborted state. */
    @Nullable
    public BulkLoadAbortReason getAbortReason() {
        return abortReason;
    }

    /** Returns the abort diagnostic, if one is available. */
    @Nullable
    public String getAbortMessage() {
        return abortMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadStatus that = (BulkLoadStatus) o;
        return Objects.equals(handle, that.handle)
                && state == that.state
                && abortReason == that.abortReason
                && Objects.equals(abortMessage, that.abortMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handle, state, abortReason, abortMessage);
    }

    @Override
    public String toString() {
        return "BulkLoadStatus{"
                + "handle="
                + handle
                + ", state="
                + state
                + ", abortReason="
                + abortReason
                + ", abortMessage='"
                + abortMessage
                + '\''
                + '}';
    }
}
