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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * Callback invoked from {@code KvTablet} during apply, before the WAL append, for each KV record.
 * Used by the Global Secondary Index push pipeline to extract index mutations.
 *
 * <p>Default no-op when no indexes are present.
 */
@Internal
@FunctionalInterface
public interface IndexedRowEmitter {

    /**
     * Emit one record application to the index path.
     *
     * @param key raw key bytes (from {@code KvPreWriteBuffer.Key.get()})
     * @param oldRow pre-image row, or {@code null} for inserts
     * @param newRow post-image row, or {@code null} for deletes
     * @param sourceOffset WAL offset assigned to this record on the data side
     */
    void emit(
            byte[] key,
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            long sourceOffset);

    /** No-op emitter — used when the table has no secondary indexes. */
    IndexedRowEmitter NO_OP = (key, oldRow, newRow, sourceOffset) -> {};
}
