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

package org.apache.fluss.utils;

import java.util.Collections;
import java.util.Set;

/** Utility constants and helpers shared by the Global Secondary Index Table layer. */
public final class IndexTableUtils {

    /**
     * Single system column appended to partitioned Index Tables. Carries the source main
     * table's {@code partitionId} (Big-Endian int64) so that partition tombstone cleanup can
     * filter dead rows without consulting external state.
     */
    public static final String PARTITION_ID_SYSTEM_COLUMN = "__partition_id";

    /**
     * Reserved system column names that user-defined schemas must not declare on a main table
     * (otherwise Index Table derivation would collide). Index Tables themselves may add these
     * columns at derive time; user-facing schemas must keep their column namespace clean.
     */
    public static final Set<String> RESERVED_INDEX_SYSTEM_COLUMNS =
            Collections.singleton(PARTITION_ID_SYSTEM_COLUMN);

    private IndexTableUtils() {}
}
