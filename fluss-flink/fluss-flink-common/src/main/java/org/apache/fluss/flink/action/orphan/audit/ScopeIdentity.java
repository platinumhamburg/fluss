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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Low-cardinality ownership identity attached to cleanup tasks and statistics. */
@Internal
public final class ScopeIdentity implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ScopeIdentity GLOBAL =
            new ScopeIdentity(ScopeKind.GLOBAL, "", "", null, null, null);

    private final ScopeKind kind;
    private final String database;
    private final String table;
    private final @Nullable Long tableId;
    private final @Nullable Long partitionId;
    private final @Nullable Integer bucketId;

    private ScopeIdentity(
            ScopeKind kind,
            String database,
            String table,
            @Nullable Long tableId,
            @Nullable Long partitionId,
            @Nullable Integer bucketId) {
        this.kind = kind;
        this.database = database;
        this.table = table;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.bucketId = bucketId;
    }

    public static ScopeIdentity global() {
        return GLOBAL;
    }

    public static ScopeIdentity table(String database, String table, long tableId) {
        return new ScopeIdentity(ScopeKind.TABLE, database, table, tableId, null, null);
    }

    public ScopeIdentity withPartitionAndBucket(
            @Nullable Long partitionId, @Nullable Integer bucketId) {
        return new ScopeIdentity(kind, database, table, tableId, partitionId, bucketId);
    }

    public ScopeIdentity tableKey() {
        if (kind == ScopeKind.GLOBAL) {
            return GLOBAL;
        }
        return new ScopeIdentity(kind, database, table, tableId, null, null);
    }

    public ScopeKind kind() {
        return kind;
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    @Nullable
    public Long tableId() {
        return tableId;
    }

    @Nullable
    public Long partitionId() {
        return partitionId;
    }

    @Nullable
    public Integer bucketId() {
        return bucketId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ScopeIdentity)) {
            return false;
        }
        ScopeIdentity that = (ScopeIdentity) obj;
        return kind == that.kind
                && database.equals(that.database)
                && table.equals(that.table)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(bucketId, that.bucketId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, database, table, tableId, partitionId, bucketId);
    }
}
