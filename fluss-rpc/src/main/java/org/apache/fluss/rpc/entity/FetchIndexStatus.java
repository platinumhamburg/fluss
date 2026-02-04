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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;

/**
 * Status of index fetch operation for a bucket.
 *
 * <p>This enum provides explicit status information for index fetch results, replacing the
 * anti-pattern of using exceptions (like FetchIndexEarlyFireException) for normal control flow.
 *
 * @since 0.9
 */
@Internal
public enum FetchIndexStatus {

    /**
     * Index data was successfully fetched and is available in the result. This is the normal
     * success case where records are returned.
     */
    SUCCESS,

    /**
     * No new index data is currently available for this bucket. This is a normal condition that
     * occurs when:
     *
     * <ul>
     *   <li>The data table has no new writes since the last fetch
     *   <li>The delayed fetch operation timed out without new data arriving
     *   <li>The fetch was completed early due to other buckets having data
     * </ul>
     *
     * <p>The client should back off and retry later. This replaces the FetchIndexEarlyFireException
     * for the "no data available" case.
     */
    NO_DATA_AVAILABLE,

    /**
     * Index data exists but is not yet ready (e.g., being loaded from cold storage). The client
     * should retry after a short delay. This is different from NO_DATA_AVAILABLE because data
     * exists but is temporarily inaccessible.
     */
    DATA_NOT_READY,

    /**
     * The fetch was throttled because the maximum bytes limit was reached. Other buckets in the
     * same request may have consumed the byte budget. The client should retry to fetch this
     * bucket's data.
     *
     * <p>This replaces the FetchIndexEarlyFireException for the "byte limit exceeded" case.
     */
    THROTTLED,

    /**
     * An error occurred during the fetch operation. When this status is set, the error field in
     * FetchIndexLogResultForBucket will contain the specific error details.
     */
    ERROR
}
