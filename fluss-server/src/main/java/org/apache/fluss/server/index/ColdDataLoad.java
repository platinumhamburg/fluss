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
 * Represents a cold data load operation that reads historical WAL data and loads index data to
 * IndexCache.
 */
@Internal
public final class ColdDataLoad implements PendingWrite {

    /** Callback interface for cold data load completion. */
    public interface CompletionCallback {
        /**
         * Called when cold data loading completes successfully.
         *
         * @param startOffset the start offset of the loaded range
         * @param endOffset the end offset of the loaded range
         */
        void onComplete(long startOffset, long endOffset);

        /**
         * Called when cold data loading fails.
         *
         * @param startOffset the start offset of the failed range
         * @param endOffset the end offset of the failed range
         * @param cause the exception that caused the failure
         */
        void onFailure(long startOffset, long endOffset, Exception cause);
    }

    private final IndexCacheWriter indexCacheWriter;
    private final long startOffset;
    private final long endOffset;
    private final CompletionCallback callback;

    public ColdDataLoad(
            IndexCacheWriter indexCacheWriter,
            long startOffset,
            long endOffset,
            CompletionCallback callback) {
        this.indexCacheWriter = indexCacheWriter;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.callback = callback;
    }

    @Override
    public void execute() throws Exception {
        try {
            indexCacheWriter.loadColdDataToCache(startOffset, endOffset);
            if (callback != null) {
                callback.onComplete(startOffset, endOffset);
            }
        } catch (Exception e) {
            if (callback != null) {
                callback.onFailure(startOffset, endOffset, e);
            }
            throw e;
        }
    }

    @Override
    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public WriteType getType() {
        return WriteType.COLD_DATA;
    }

    @Override
    public String toString() {
        return String.format("ColdDataLoad[%d, %d)", startOffset, endOffset);
    }
}
