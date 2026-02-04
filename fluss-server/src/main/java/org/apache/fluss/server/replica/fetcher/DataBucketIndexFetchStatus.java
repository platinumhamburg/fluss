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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.index.IndexApplier;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

/**
 * This class represents a lightweight scheduling token for index fetch operations in index
 * replication. It serves as a fair scheduling unit without storing state information.
 *
 * <p>Key Design Changes:
 *
 * <ul>
 *   <li>Removed fetchOffset and indexCommitOffset fields to eliminate state duplication
 *   <li>IndexApplier.IndexApplyStatus is the single source of truth for all state information
 *   <li>This class only provides fair scheduling capability via FairDataIndexTableBucketStatusMap
 *   <li>All state queries should be directed to IndexApplier for consistency
 * </ul>
 *
 * <p>This represents a data bucket's index fetch scheduling status as being either:
 *
 * <ul>
 *   <li>Delayed, for example due to an error, where we subsequently back off a bit.
 *   <li>ReadyForFetch, this is the active state where the thread can schedule index fetching from
 *       the data bucket.
 * </ul>
 */
public class DataBucketIndexFetchStatus {
    private final DataIndexTableBucket dataIndexTableBucket;
    private final TablePath indexTablePath;
    private final @Nullable DelayedItem delayedItem;
    private final IndexApplier indexApplier;

    public DataBucketIndexFetchStatus(
            DataIndexTableBucket dataIndexTableBucket,
            TablePath indexTablePath,
            @Nullable DelayedItem delayedItem,
            IndexApplier indexApplier) {
        this.dataIndexTableBucket = dataIndexTableBucket;
        this.indexTablePath = indexTablePath;
        this.delayedItem = delayedItem;
        this.indexApplier = indexApplier;
    }

    public boolean isReadyForFetch() {
        return !isDelayed();
    }

    public boolean isDelayed() {
        return delayedItem != null && delayedItem.getDelay(TimeUnit.MILLISECONDS) > 0;
    }

    public DataIndexTableBucket getDataIndexTableBucket() {
        return dataIndexTableBucket;
    }

    public long indexTableId() {
        return dataIndexTableBucket.getIndexBucket().getTableId();
    }

    public TablePath indexTablePath() {
        return indexTablePath;
    }

    public long dataTableId() {
        return dataIndexTableBucket.getDataBucket().getTableId();
    }

    public IndexApplier indexApplier() {
        return indexApplier;
    }

    /**
     * Query the current index commit offset for the data bucket from IndexApplier.
     *
     * @return the current index commit offset, or -1 if no status exists
     */
    public long queryIndexCommitOffset() {
        TableBucket dataBucket = dataIndexTableBucket.getDataBucket();
        return indexApplier.getIndexCommitOffset(dataBucket);
    }

    /**
     * Query the IndexApplyStatus for the data bucket from IndexApplier.
     *
     * @return the IndexApplyStatus, or null if no status exists
     */
    public IndexApplier.IndexApplyStatus queryIndexApplyStatus() {
        TableBucket dataBucket = dataIndexTableBucket.getDataBucket();
        return indexApplier.getOrInitIndexApplyStatus(dataBucket);
    }

    /**
     * Query the current fetch offset from IndexApplyStatus. This is the end offset of the last
     * applied records in the data bucket address space.
     *
     * @return the current fetch offset, or 0 if no status exists
     */
    public long queryFetchOffset() {
        IndexApplier.IndexApplyStatus status = queryIndexApplyStatus();
        return status != null ? status.getLastApplyRecordsDataEndOffset() : 0L;
    }

    @Override
    public String toString() {
        return String.format(
                "DataBucketIndexFetchStatus(dataBucket=%s, indexBucket=%s, indexTablePath=%s, "
                        + "delay=%s ms, indexApplier=available)",
                dataIndexTableBucket.getDataBucket(),
                dataIndexTableBucket.getIndexBucket(),
                indexTablePath,
                delayedItem == null ? 0 : delayedItem.getDelayMs());
    }
}
