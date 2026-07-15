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

/** Stable low-cardinality reasons used for skip aggregation. */
@Internal
public enum SkipReasonCode {
    KEEP_ACTIVE(SkipCategory.EXPECTED_SKIP, false, false),
    NEWER_THAN_CUTOFF(SkipCategory.EXPECTED_SKIP, false, false),
    NO_REMOTE_MANIFEST(SkipCategory.EXPECTED_SKIP, false, false),
    EMPTY_KV_ACTIVE_SET(SkipCategory.SAFETY_SKIP, true, false),
    UNKNOWN_FILE_TYPE(SkipCategory.SAFETY_SKIP, false, true),
    OUT_OF_SCOPE_ROOT(SkipCategory.OUT_OF_SCOPE, false, true),
    PARTITION_NOT_EXIST(SkipCategory.EXPECTED_SKIP, false, false),
    TABLE_NOT_EXIST(SkipCategory.EXPECTED_SKIP, false, false),
    RPC_ERROR(SkipCategory.DEGRADED_SKIP, true, true),
    METADATA_READ_FAILED(SkipCategory.DEGRADED_SKIP, true, true),
    DIRECTORY_LIST_FAILED(SkipCategory.DEGRADED_SKIP, true, true),
    UNSUPPORTED_API(SkipCategory.UNSUPPORTED, false, true);

    private final SkipCategory category;
    private final boolean retryable;
    private final boolean actionRequired;

    SkipReasonCode(SkipCategory category, boolean retryable, boolean actionRequired) {
        this.category = category;
        this.retryable = retryable;
        this.actionRequired = actionRequired;
    }

    public SkipCategory category() {
        return category;
    }

    public boolean retryable() {
        return retryable;
    }

    public boolean actionRequired() {
        return actionRequired;
    }
}
