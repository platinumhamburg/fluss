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

import javax.annotation.Nullable;

import java.io.Serializable;

/** Tagged union carrying stage-1 plans and stage-2 cleanup statistics into final aggregation. */
@Internal
public final class CleanupReportInput implements Serializable {

    private static final long serialVersionUID = 1L;

    private final @Nullable TablePlanStats plan;
    private final @Nullable CleanStats stats;

    private CleanupReportInput(@Nullable TablePlanStats plan, @Nullable CleanStats stats) {
        this.plan = plan;
        this.stats = stats;
    }

    public static CleanupReportInput plan(TablePlanStats plan) {
        return new CleanupReportInput(plan, null);
    }

    public static CleanupReportInput stats(CleanStats stats) {
        return new CleanupReportInput(null, stats);
    }

    @Nullable
    public TablePlanStats plan() {
        return plan;
    }

    @Nullable
    public CleanStats stats() {
        return stats;
    }
}
