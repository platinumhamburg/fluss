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
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Builds and executes the 3-stage Flink Batch DAG for orphan files cleanup.
 *
 * <pre>
 * Stage 1: ScopeEnumerator (p=1)   — coordinator RPCs, emits CleanTask
 * Stage 2: ScanAndClean (p=N)      — FS scan + rate-limited delete, emits CleanStats
 * Stage 3: StatsAggregate (p=1)    — merge stats, emits final CleanupReport
 * </pre>
 */
@Internal
public final class OrphanFilesCleanJob {

    private OrphanFilesCleanJob() {}

    /**
     * Builds the DAG, executes it in batch mode, and returns the final aggregated cleanup
     * statistics.
     *
     * @param env the Flink execution environment (caller configures classpath, etc.)
     * @param config parsed orphan cleanup configuration
     * @param parallelism the parallelism for Stage 2 (ScanAndClean); null uses env default
     * @return the final cleanup statistics
     */
    public static CleanupReport execute(
            StreamExecutionEnvironment env,
            OrphanCleanConfig config,
            Integer parallelism,
            String runId)
            throws Exception {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Stage 1: ScopeEnumerator (parallelism=1)
        DataStream<Integer> trigger =
                env.fromCollection(Collections.singletonList(1), TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<CleanTask> tasks =
                trigger.process(new ScopeEnumeratorFunction(config, runId))
                        .returns(TypeInformation.of(new TypeHint<CleanTask>() {}))
                        .setParallelism(1)
                        .setMaxParallelism(1)
                        .name("ScopeEnumerator");
        DataStream<TablePlanStats> tablePlans =
                tasks.getSideOutput(ScopeEnumeratorFunction.TABLE_PLAN_STATS);

        // Stage 2: ScanAndClean (parallelism=N)
        SingleOutputStreamOperator<CleanStats> stats =
                tasks.rebalance()
                        .process(
                                new ScanAndCleanFunction(
                                        config.remoteFsOpRateLimitPerSecond(),
                                        config.extraConfigs(),
                                        runId,
                                        config.dryRun(),
                                        config.progressLogInterval()))
                        .returns(TypeInformation.of(new TypeHint<CleanStats>() {}))
                        .name("ScanAndClean");
        if (parallelism != null) {
            stats = stats.setParallelism(parallelism);
        }

        int scanParallelism = stats.getParallelism();
        SingleOutputStreamOperator<CleanStats> statsWithProgress =
                stats.forward()
                        .transform(
                                "ScanProgress",
                                TypeInformation.of(new TypeHint<CleanStats>() {}),
                                new ScanProgressOperator(runId, config.dryRun()))
                        .setParallelism(scanParallelism);

        DataStream<CleanupReportInput> reportInputs =
                statsWithProgress
                        .map(CleanupReportInput::stats)
                        .returns(TypeInformation.of(new TypeHint<CleanupReportInput>() {}))
                        .name("CleanupStatsReportInput")
                        .union(
                                tablePlans
                                        .map(CleanupReportInput::plan)
                                        .returns(
                                                TypeInformation.of(
                                                        new TypeHint<CleanupReportInput>() {}))
                                        .name("TablePlanReportInput"));

        // Stage 3: StatsAggregate (parallelism=1)
        SingleOutputStreamOperator<CleanupReport> result =
                reportInputs
                        .transform(
                                "StatsAggregate",
                                TypeInformation.of(new TypeHint<CleanupReport>() {}),
                                new StatsAggregateOperator(runId, config.dryRun()))
                        .setParallelism(1)
                        .setMaxParallelism(1);

        // Execute and collect the single result
        List<CleanupReport> collected = collectResults(result);
        if (collected.isEmpty()) {
            return CleanupReport.aggregate(
                    Collections.emptyList(), Collections.emptyList(), config.dryRun());
        }
        return collected.get(0);
    }

    @SuppressWarnings("deprecation")
    private static List<CleanupReport> collectResults(DataStream<CleanupReport> result)
            throws Exception {
        Iterator<CleanupReport> iterator = result.executeAndCollect("OrphanFilesClean");
        List<CleanupReport> results = new java.util.ArrayList<CleanupReport>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }
}
