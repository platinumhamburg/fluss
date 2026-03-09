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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

/** Mutation type of a KV write record. */
@PublicEvolving
public enum KvMutationType {
    /** Insert or update a value through the table's merge semantics. */
    UPSERT,

    /** Delete an existing value. */
    DELETE,

    /** Retract a previously applied aggregation contribution. */
    RETRACT;

    /** Returns whether this mutation must carry row data. */
    public boolean requiresRowData() {
        return this != DELETE;
    }

    /** Resolves the normalized mutation type from legacy nullable-row semantics. */
    public static KvMutationType fromRow(@Nullable BinaryRow row) {
        return row == null ? DELETE : UPSERT;
    }

    /**
     * Resolves the normalized mutation type from legacy nullable-row plus retract-flag semantics.
     *
     * @throws IllegalArgumentException if isRetract is true but row is null, since RETRACT requires
     *     row data to compute inverse aggregation
     */
    public static KvMutationType fromRowAndRetract(@Nullable BinaryRow row, boolean isRetract) {
        if (isRetract) {
            if (row == null) {
                throw new IllegalArgumentException(
                        "RETRACT requires row data to compute inverse aggregation, but row is null");
            }
            return RETRACT;
        }
        return fromRow(row);
    }
}
