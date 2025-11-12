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
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.LogAppendInfo;

/**
 * Represents a hot data write operation that extracts index data from real-time WAL records and
 * writes to IndexCache.
 */
@Internal
public final class HotDataWrite implements PendingWrite {

    private final IndexCacheWriter indexCacheWriter;
    private final WalBuilder walBuilder;
    private final LogRecords walRecords;
    private final LogAppendInfo appendInfo;

    public HotDataWrite(
            IndexCacheWriter indexCacheWriter,
            WalBuilder walBuilder,
            LogRecords walRecords,
            LogAppendInfo appendInfo) {
        this.indexCacheWriter = indexCacheWriter;
        this.walBuilder = walBuilder;
        this.walRecords = walRecords;
        this.appendInfo = appendInfo;
    }

    @Override
    public void execute() throws Exception {
        try {
            indexCacheWriter.cacheIndexDataByHotData(walRecords, appendInfo);
        } finally {
            if (walBuilder != null) {
                walBuilder.deallocate();
            }
        }
    }

    @Override
    public long getStartOffset() {
        return appendInfo.firstOffset();
    }

    @Override
    public long getEndOffset() {
        return appendInfo.lastOffset() + 1;
    }

    @Override
    public WriteType getType() {
        return WriteType.HOT_DATA;
    }

    @Override
    public String toString() {
        return String.format(
                "HotDataWrite[%d, %d)", appendInfo.firstOffset(), appendInfo.lastOffset() + 1);
    }
}
