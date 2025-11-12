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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.index.IndexApplier;

import java.util.Objects;

/** Initial fetch state for index table replication. */
public class IndexInitialFetchStatus {
    private final long indexTableId;
    private final TablePath indexTablePath;
    private final int dataTableLeader;
    private final long initFetchOffset;
    private final long indexCommitOffset;
    private final IndexApplier indexApplier;

    public IndexInitialFetchStatus(
            long indexTableId,
            TablePath indexTablePath,
            int dataTableLeader,
            long initFetchOffset,
            long indexCommitOffset,
            IndexApplier indexApplier) {
        this.indexTableId = indexTableId;
        this.indexTablePath = indexTablePath;
        this.dataTableLeader = dataTableLeader;
        this.initFetchOffset = initFetchOffset;
        this.indexCommitOffset = indexCommitOffset;
        this.indexApplier = indexApplier;
    }

    public long indexTableId() {
        return indexTableId;
    }

    public TablePath indexTablePath() {
        return indexTablePath;
    }

    public int dataTableLeader() {
        return dataTableLeader;
    }

    public long initFetchOffset() {
        return initFetchOffset;
    }

    public long indexCommitOffset() {
        return indexCommitOffset;
    }

    public IndexApplier indexApplier() {
        return indexApplier;
    }

    /**
     * Query the current index commit offset for the specified data bucket from IndexApplier.
     * Returns the provided indexCommitOffset if no status exists in IndexApplier.
     *
     * @param dataBucket the data bucket to query for
     * @return the current index commit offset, or the initial indexCommitOffset if no status exists
     */
    public long queryIndexCommitOffset(TableBucket dataBucket) {
        long currentCommitOffset = indexApplier.getIndexCommitOffset(dataBucket);
        return currentCommitOffset != -1L ? currentCommitOffset : indexCommitOffset;
    }

    /**
     * Query the IndexApplyStatus for the specified data bucket from IndexApplier.
     *
     * @param dataBucket the data bucket to query for
     * @return the IndexApplyStatus, or null if no status exists
     */
    public IndexApplier.IndexApplyStatus queryIndexApplyStatus(TableBucket dataBucket) {
        return indexApplier.getOrInitIndexApplyStatus(dataBucket);
    }

    @Override
    public String toString() {
        return "IndexInitialFetchStatus{"
                + "indexTableId="
                + indexTableId
                + ", indexTablePath="
                + indexTablePath
                + ", dataTableLeader="
                + dataTableLeader
                + ", initFetchOffset="
                + initFetchOffset
                + ", indexCommitOffset="
                + indexCommitOffset
                + ", indexApplier=available"
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IndexInitialFetchStatus)) {
            return false;
        }
        IndexInitialFetchStatus that = (IndexInitialFetchStatus) object;
        return indexTableId == that.indexTableId
                && indexTablePath.equals(that.indexTablePath)
                && dataTableLeader == that.dataTableLeader
                && initFetchOffset == that.initFetchOffset
                && indexCommitOffset == that.indexCommitOffset
                && Objects.equals(indexApplier, that.indexApplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                indexTableId,
                indexTablePath,
                dataTableLeader,
                initFetchOffset,
                indexCommitOffset,
                indexApplier);
    }
}
