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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.List;

/**
 * The result of a cleanup orphan metadata operation.
 *
 * @since 0.9
 */
@PublicEvolving
public final class CleanupOrphanMetadataResult {

    private final int orphanTableCount;
    private final int orphanPartitionCount;
    private final List<Long> cleanedTableIds;
    private final List<Long> cleanedPartitionIds;

    public CleanupOrphanMetadataResult(
            int orphanTableCount,
            int orphanPartitionCount,
            List<Long> cleanedTableIds,
            List<Long> cleanedPartitionIds) {
        this.orphanTableCount = orphanTableCount;
        this.orphanPartitionCount = orphanPartitionCount;
        this.cleanedTableIds = cleanedTableIds;
        this.cleanedPartitionIds = cleanedPartitionIds;
    }

    public int getOrphanTableCount() {
        return orphanTableCount;
    }

    public int getOrphanPartitionCount() {
        return orphanPartitionCount;
    }

    public List<Long> getCleanedTableIds() {
        return cleanedTableIds;
    }

    public List<Long> getCleanedPartitionIds() {
        return cleanedPartitionIds;
    }

    @Override
    public String toString() {
        return "CleanupOrphanMetadataResult{"
                + "orphanTableCount="
                + orphanTableCount
                + ", orphanPartitionCount="
                + orphanPartitionCount
                + ", cleanedTableIds="
                + cleanedTableIds
                + ", cleanedPartitionIds="
                + cleanedPartitionIds
                + '}';
    }
}
