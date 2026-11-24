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
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;

import javax.annotation.Nullable;

import java.io.Serializable;

/** The persisted status of a BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private final BulkLoadHandle handle;
    private final State state;
    private final long createdTime;
    private final @Nullable Long finishedTime;
    private final @Nullable String abortMessage;

    private BulkLoadStatus(PbBulkLoadStatus status) {
        handle = BulkLoadHandle.fromProto(status.getHandle());
        state = State.fromCode(status.getState());
        createdTime = status.getCreatedTime();
        finishedTime = status.hasFinishedTime() ? status.getFinishedTime() : null;
        abortMessage = status.hasAbortMessage() ? status.getAbortMessage() : null;
    }

    /** Returns the transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the transaction state. */
    public State getState() {
        return state;
    }

    /** Returns when the transaction began, in milliseconds since the Unix epoch. */
    public long getCreatedTime() {
        return createdTime;
    }

    /** Returns when the transaction finished, in milliseconds since the Unix epoch. */
    @Nullable
    public Long getFinishedTime() {
        return finishedTime;
    }

    /** Returns the diagnostic supplied when the transaction was aborted, if present. */
    @Nullable
    public String getAbortMessage() {
        return abortMessage;
    }

    static BulkLoadStatus fromProto(PbBulkLoadStatus status) {
        return new BulkLoadStatus(status);
    }

    /** The coordinator's transaction state. */
    @PublicEvolving
    public enum State {
        RUNNING,
        COMMITTED,
        ABORTED;

        private static State fromCode(int code) {
            switch (code) {
                case 0:
                    return RUNNING;
                case 1:
                    return COMMITTED;
                case 2:
                    return ABORTED;
                default:
                    throw new IllegalArgumentException("Unknown BulkLoad state: " + code);
            }
        }
    }
}
