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

import java.nio.ByteBuffer;

/**
 * A kv record is a tuple consisting of a key, and a nullable {@link BinaryRow}.
 *
 * @since 0.1
 */
@PublicEvolving
public interface KvRecord {

    /**
     * Get the key of the kv record.
     *
     * @return the key of the kv record.
     */
    ByteBuffer getKey();

    /**
     * Get the row of the kv record. If the row is null, it means the record is a delete record.
     *
     * @return the row of the kv record, which is BinaryRow.
     */
    @Nullable
    BinaryRow getRow();

    /**
     * Returns the normalized mutation type of this KV record.
     *
     * <p>The current wire format still represents delete as {@code row == null} and retract as a
     * per-record flag. This accessor centralizes that legacy decoding so callers no longer have to
     * duplicate the same branching logic.
     */
    default KvMutationType getMutationType() {
        return KvMutationType.fromRowAndRetract(getRow(), isRetract());
    }

    /**
     * Returns whether this record is a retract record. A retract record reverses a previously
     * aggregated value in merge-engine tables.
     *
     * @return true if this record is a retract record, false otherwise.
     */
    boolean isRetract();

    /**
     * Get the size in bytes of this record.
     *
     * @return the size of the record in bytes
     */
    int getSizeInBytes();
}
