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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of index fetch request for each index table bucket. */
@Internal
public class FetchIndexLogResultForBucket {

    private final @Nullable LogRecords records;
    private final long startOffset;
    private final long endOffset;
    private final boolean isDataReady;
    protected final ApiError error;

    public FetchIndexLogResultForBucket(
            LogRecords records, long startOffset, long endOffset, boolean isDataReady) {
        this.records = checkNotNull(records, "records can not be null");
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.isDataReady = isDataReady;
        this.error = ApiError.NONE;
    }

    public FetchIndexLogResultForBucket(ApiError error) {
        this.records = null;
        this.startOffset = -1L;
        this.endOffset = -1L;
        // For error cases, isDataReady is not meaningful, use true as default
        this.isDataReady = true;
        this.error = checkNotNull(error, "error can not be null");
    }

    public @Nullable LogRecords records() {
        return records;
    }

    public LogRecords recordsOrEmpty() {
        if (records == null) {
            return MemoryLogRecords.EMPTY;
        } else {
            return records;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public boolean isDataReady() {
        return isDataReady;
    }

    public ApiError getError() {
        return error;
    }

    public boolean hasError() {
        return error != ApiError.NONE;
    }

    @Override
    public String toString() {
        return "FetchIndexLogResultForBucket{"
                + "startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + ", error="
                + error
                + '}';
    }
}
