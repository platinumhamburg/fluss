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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;

/** Enumeration of all file-level rule identifiers used in orphan cleanup audit logs. */
@Internal
public enum RuleId {
    LOG_SEGMENT("log-segment", CleanupObjectType.LOG_SEGMENT),
    LOG_MANIFEST("log-manifest", CleanupObjectType.LOG_MANIFEST),
    KV_SNAPSHOT_FILE("kv-snapshot-file", CleanupObjectType.KV_SNAPSHOT_FILE),
    KV_SHARED_SST("kv-shared-sst", CleanupObjectType.KV_SHARED_SST),
    UNKNOWN("unknown", CleanupObjectType.UNKNOWN);

    private final String auditTag;
    private final CleanupObjectType objectType;

    RuleId(String auditTag, CleanupObjectType objectType) {
        this.auditTag = auditTag;
        this.objectType = objectType;
    }

    public CleanupObjectType objectType() {
        return objectType;
    }

    @Override
    public String toString() {
        return auditTag;
    }
}
