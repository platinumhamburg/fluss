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
    private final FetchIndexStatus status;
    protected final ApiError error;

    /** Constructor for successful fetch with data. */
    public FetchIndexLogResultForBucket(
            LogRecords records, long startOffset, long endOffset, boolean isDataReady) {
        this.records = checkNotNull(records, "records can not be null");
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.isDataReady = isDataReady;
        this.status = FetchIndexStatus.SUCCESS;
        this.error = ApiError.NONE;
    }

    /** Constructor for error cases. */
    public FetchIndexLogResultForBucket(ApiError error) {
        this.records = null;
        this.startOffset = -1L;
        this.endOffset = -1L;
        // For error cases, isDataReady is not meaningful, use true as default
        this.isDataReady = true;
        this.status = FetchIndexStatus.ERROR;
        this.error = checkNotNull(error, "error can not be null");
    }

    /** Constructor for non-error status without data (e.g., NO_DATA_AVAILABLE, THROTTLED). */
    public FetchIndexLogResultForBucket(FetchIndexStatus status) {
        if (status == FetchIndexStatus.SUCCESS || status == FetchIndexStatus.ERROR) {
            throw new IllegalArgumentException(
                    "Use appropriate constructor for SUCCESS or ERROR status");
        }
        this.records = null;
        this.startOffset = -1L;
        this.endOffset = -1L;
        this.isDataReady = false;
        this.status = checkNotNull(status, "status can not be null");
        this.error = ApiError.NONE;
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

    public FetchIndexStatus getStatus() {
        return status;
    }

    public ApiError getError() {
        return error;
    }

    public boolean hasError() {
        return status == FetchIndexStatus.ERROR || error != ApiError.NONE;
    }

    /**
     * Check if this result indicates that the client should retry later. This includes cases where
     * no data is available, data is not ready, or the request was throttled.
     */
    public boolean shouldRetryLater() {
        return status == FetchIndexStatus.NO_DATA_AVAILABLE
                || status == FetchIndexStatus.DATA_NOT_READY
                || status == FetchIndexStatus.THROTTLED;
    }

    /**
     * Check if this result has actual data that can be processed.
     *
     * @return true if status is SUCCESS and records are available
     */
    public boolean hasData() {
        return status == FetchIndexStatus.SUCCESS && records != null;
    }

    @Override
    public String toString() {
        return "FetchIndexLogResultForBucket{"
                + "startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + ", status="
                + status
                + ", error="
                + error
                + '}';
    }
}
