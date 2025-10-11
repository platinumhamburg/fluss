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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class to identify a table partition.
 *
 * @since 0.1
 */
public class TablePartitionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    // will be null if the bucket doesn't belong to a partition
    private final @Nullable Long partitionId;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    public TablePartitionId(long tableId, @Nullable Long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public long getTableId() {
        return tableId;
    }

    /**
     * Create a TablePartitionId from a TableBucket.
     *
     * @param tableBucket the TableBucket
     * @return the TablePartitionId
     */
    public static TablePartitionId from(TableBucket tableBucket) {
        return new TablePartitionId(tableBucket.getTableId(), tableBucket.getPartitionId());
    }

    /**
     * Create a TablePartitionId.
     *
     * @param tableId the table id
     * @param partitionId the partition id
     * @return the TablePartitionId
     */
    public static TablePartitionId of(long tableId, @Nullable Long partitionId) {
        return new TablePartitionId(tableId, partitionId);
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePartitionId that = (TablePartitionId) o;
        return tableId == that.tableId && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = Objects.hash(tableId, partitionId);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TableBucket{tableId=");
        builder.append(tableId);
        if (partitionId != null) {
            builder.append(", partitionId=").append(partitionId);
        }
        builder.append('}');
        return builder.toString();
    }
}
