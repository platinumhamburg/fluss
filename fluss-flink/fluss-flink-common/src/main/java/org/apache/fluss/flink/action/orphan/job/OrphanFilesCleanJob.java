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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.ExceptionUtils;

import java.util.Collections;

/**
 * Builds and executes the 3-stage Flink Batch DAG for orphan files cleanup.
 *
 * <pre>
 * Stage 1: ScopeEnumerator (p=1)   — coordinator RPCs, emits CleanTask
 * Stage 2: ScanAndClean (p=N)      — FS scan + rate-limited delete, emits CleanupStats
 * Stage 3: StatsAggregate (p=1)    — merge stats, emits final CleanupSummary
 * </pre>
 */
@Internal
public final class OrphanFilesCleanJob {

    private OrphanFilesCleanJob() {}

    /**
     * Builds the DAG, executes it in batch mode, and waits for the cleanup job to complete.
     *
     * <p>The terminal summary is emitted by {@link StatsAggregateOperator} through the audit logger
     * and reporters. The execution path deliberately uses a regular discarding sink rather than
     * {@code executeAndCollect} API, which installs a REST result collector in the JobManager.
     *
     * @param env the Flink execution environment (caller configures classpath, etc.)
     * @param config parsed orphan cleanup configuration
     * @param parallelism the parallelism for Stage 2 (ScanAndClean); null uses env default
     */
    public static void execute(
            StreamExecutionEnvironment env, OrphanCleanConfig config, Integer parallelism)
            throws Exception {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        buildExecutablePipeline(env, config, parallelism);
        waitForCompletion(env.executeAsync("OrphanFilesClean"));
    }

    /** Builds the executable cleanup DAG with the standard terminal discarding sink. */
    static void buildExecutablePipeline(
            StreamExecutionEnvironment env, OrphanCleanConfig config, Integer parallelism) {
        buildPipeline(env, config, parallelism)
                .sinkTo(new DiscardingSink<CleanupSummary>())
                .name("end")
                .setParallelism(1);
    }

    static DataStream<CleanupSummary> buildPipeline(
            StreamExecutionEnvironment env, OrphanCleanConfig config, Integer parallelism) {
        // Stage 1: ScopeEnumerator (parallelism=1)
        DataStream<Integer> trigger =
                env.fromCollection(Collections.singletonList(1), TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<CleanTask> tasks =
                trigger.process(new ScopeEnumeratorFunction(config))
                        .returns(TypeInformation.of(new TypeHint<CleanTask>() {}))
                        .setParallelism(1)
                        .setMaxParallelism(1)
                        .name("ScopeEnumerator");

        // Stage 2: ScanAndClean (parallelism=N)
        SingleOutputStreamOperator<CleanupStats> stats =
                tasks.rebalance()
                        .process(
                                new ScanAndCleanFunction(
                                        config.remoteFsOpRateLimitPerSecond(),
                                        config.extraConfigs(),
                                        config.auditReporterSpec(),
                                        config.dryRun()))
                        .returns(TypeInformation.of(new TypeHint<CleanupStats>() {}))
                        .name("ScanAndClean");
        if (parallelism != null) {
            stats = stats.setParallelism(parallelism);
        }

        // Stage 3: StatsAggregate (parallelism=1)
        SingleOutputStreamOperator<CleanupSummary> result =
                stats.transform(
                                "StatsAggregate",
                                TypeInformation.of(new TypeHint<CleanupSummary>() {}),
                                new StatsAggregateOperator(
                                        config.dryRun(), config.auditReporterSpec()))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return result;
    }

    private static void waitForCompletion(JobClient jobClient) throws Exception {
        try {
            jobClient.getJobExecutionResult().get();
        } catch (Throwable failure) {
            ExceptionUtils.rethrowException(ExceptionUtils.stripExecutionException(failure));
        }
    }
}
