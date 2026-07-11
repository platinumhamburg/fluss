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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.time.Duration;

/**
 * Stage 3 of the orphan files cleanup job. Runs at parallelism=1 to aggregate per-subtask {@link
 * CleanStats} records.
 *
 * <p>Implemented as a custom operator (not ProcessFunction) because {@code ProcessOperator} does
 * not implement {@link BoundedOneInput} — the {@code endInput()} callback would never fire.
 *
 * <p>Scalar counters are accumulated into longs and the final summary is emitted in {@link
 * #endInput()}.
 */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanupReport>
        implements OneInputStreamOperator<CleanupReportInput, CleanupReport>, BoundedOneInput {

    private static final long serialVersionUID = 3L;

    private final String runId;
    private final boolean dryRun;
    private final Duration postRunWait;
    private final InterruptibleSleeper sleeper;

    private transient CleanupReport.Accumulator accumulator;

    public StatsAggregateOperator(String runId, boolean dryRun, Duration postRunWait) {
        this(runId, dryRun, postRunWait, Thread::sleep);
    }

    StatsAggregateOperator(
            String runId, boolean dryRun, Duration postRunWait, InterruptibleSleeper sleeper) {
        this.runId = runId;
        this.dryRun = dryRun;
        this.postRunWait = postRunWait;
        this.sleeper = sleeper;
    }

    @Override
    public void open() throws Exception {
        super.open();
        accumulator = CleanupReport.accumulator(dryRun);
    }

    @Override
    public void processElement(StreamRecord<CleanupReportInput> element) {
        CleanupReportInput input = element.getValue();
        if (input.plan() != null) {
            accumulator.addPlan(input.plan());
        }
        if (input.stats() != null) {
            accumulator.addStats(input.stats());
        }
    }

    @Override
    public void endInput() throws Exception {
        AuditLogger audit = new AuditLogger();
        CleanupReport report = accumulator.build();

        audit.logReport(runId, "aggregate", "summary", report, dryRun);

        if (!postRunWait.isZero()) {
            long waitMillis = postRunWait.toMillis();
            audit.logRetentionWaitStart(runId, waitMillis);
            try {
                sleeper.sleep(waitMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            audit.logRetentionWaitEnd(runId, waitMillis);
        }

        output.collect(new StreamRecord<>(report));
    }
}

@FunctionalInterface
interface InterruptibleSleeper extends Serializable {
    void sleep(long millis) throws InterruptedException;
}
