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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import java.util.EnumMap;
import java.util.Map;

/** Marker task carrying the final stage-1 scope statistics through the existing task stream. */
@Internal
public final class ScopeSummaryTask implements CleanTask {

    private static final long serialVersionUID = 1L;

    private final CleanupStats stats;

    private ScopeSummaryTask(CleanupStats stats) {
        this.stats = stats;
    }

    public static ScopeSummaryTask from(ScopePlanStats plan) {
        EnumMap<SkipReasonCode, Long> skipped = new EnumMap<>(SkipReasonCode.class);
        putPositive(
                skipped, SkipReasonCode.NO_REMOTE_MANIFEST, plan.skippedNoRemoteManifestCount());
        putPositive(
                skipped, SkipReasonCode.EMPTY_KV_ACTIVE_SET, plan.skippedEmptyKvActiveSetCount());
        putPositive(skipped, SkipReasonCode.OUT_OF_SCOPE_ROOT, plan.skippedOutOfScopeRootCount());
        return new ScopeSummaryTask(CleanupStats.scope(plan, skipped));
    }

    public CleanupStats stats() {
        return stats;
    }

    @Override
    public ScopeIdentity scope() {
        return ScopeIdentity.global();
    }

    private static void putPositive(
            Map<SkipReasonCode, Long> target, SkipReasonCode reason, long count) {
        if (count > 0L) {
            target.put(reason, count);
        }
    }
}
