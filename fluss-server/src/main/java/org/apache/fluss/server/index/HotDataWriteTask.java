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
import org.apache.fluss.server.log.LogAppendInfo;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents a hot data write task that extracts index data from real-time WAL records and writes
 * to IndexDataProducer.
 *
 * <p>NOTE: The walRecords should be an independent copy with its own memory, not sharing memory
 * with any WalBuilder. The WalBuilder's lifecycle is managed by the caller (KvTablet) and will be
 * deallocated immediately after creating this task.
 */
@Internal
public final class HotDataWriteTask implements IndexWriteTask {

    private final IndexDataExtractor dataExtractor;
    private final LogRecords walRecords;
    private final LogAppendInfo appendInfo;

    public HotDataWriteTask(
            IndexDataExtractor dataExtractor, LogRecords walRecords, LogAppendInfo appendInfo) {
        this.dataExtractor = checkNotNull(dataExtractor, "dataExtractor cannot be null");
        this.walRecords = checkNotNull(walRecords, "walRecords cannot be null");
        this.appendInfo = checkNotNull(appendInfo, "appendInfo cannot be null");
    }

    @Override
    public void execute() throws Exception {
        dataExtractor.cacheIndexDataByHotData(walRecords, appendInfo);
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
    public TaskType getType() {
        return TaskType.HOT_DATA;
    }

    @Override
    public String toString() {
        return String.format(
                "HotDataWriteTask[%d, %d)", appendInfo.firstOffset(), appendInfo.lastOffset() + 1);
    }
}
