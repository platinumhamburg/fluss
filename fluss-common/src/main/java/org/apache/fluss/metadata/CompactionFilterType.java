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

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Type of compaction filter applied to KV storage.
 *
 * <p>Compaction filters can be used to customize the behavior of RocksDB compaction, such as
 * removing expired entries (TTL) or filtering entries based on custom logic.
 *
 * @since 0.9
 */
@PublicEvolving
public enum CompactionFilterType {
    /** No compaction filter. */
    NONE,

    /** TTL-based compaction filter that removes entries older than a configured retention time. */
    TTL

    // TODO: Future support for PARTITION compaction filter
    // The PARTITION compaction filter will encode partition ID as a prefix in the value,
    // allowing removal of data from deleted partitions during compaction.
    // This is useful for managing partitioned tables where some partitions are dropped.
}
