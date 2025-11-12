/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;

/**
 * Represents a pending write operation to IndexCache, which can be either hot data writing or cold
 * data loading.
 *
 * <p>PendingWrite operations are executed sequentially by PendingWriteQueue to ensure data
 * consistency and ordering.
 */
@Internal
public interface PendingWrite {

    /**
     * Executes the pending write operation.
     *
     * @throws Exception if an error occurs during execution
     */
    void execute() throws Exception;

    /**
     * Gets the start offset of the data range this operation will process.
     *
     * @return the start offset
     */
    long getStartOffset();

    /**
     * Gets the end offset of the data range this operation will process.
     *
     * @return the end offset (exclusive)
     */
    long getEndOffset();

    /**
     * Gets the type of this pending write operation.
     *
     * @return the operation type
     */
    WriteType getType();

    /** Type of write operation. */
    enum WriteType {
        /** Hot data write from real-time WAL. */
        HOT_DATA,
        /** Cold data load from historical WAL. */
        COLD_DATA
    }
}
