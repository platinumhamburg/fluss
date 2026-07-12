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
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/** Per-scan-subtask pass-through operator that emits bounded cumulative audit statistics. */
@Internal
public final class ScanProgressOperator extends AbstractStreamOperator<CleanStats>
        implements OneInputStreamOperator<CleanStats, CleanStats>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final String runId;
    private final boolean dryRun;
    private final SerializableLongSupplier clock;

    private transient AuditLogger audit;
    private transient CleanupCounters counters;
    private transient long tasksCompleted;
    private transient long startMillis;
    private transient int subtask;
    private transient int parallelism;
    private transient int attempt;

    public ScanProgressOperator(String runId, boolean dryRun) {
        this(runId, dryRun, System::currentTimeMillis);
    }

    ScanProgressOperator(String runId, boolean dryRun, SerializableLongSupplier clock) {
        this.runId = runId;
        this.dryRun = dryRun;
        this.clock = clock;
    }

    @Override
    public void open() throws Exception {
        super.open();
        audit = new AuditLogger();
        counters = CleanupCounters.empty();
        tasksCompleted = 0L;
        startMillis = clock.getAsLong();
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        attempt = RuntimeContextAdapter.getAttemptNumber(getRuntimeContext());
    }

    @Override
    public void processElement(StreamRecord<CleanStats> element) {
        counters = counters.add(element.getValue().counters());
        tasksCompleted++;
        output.collect(element);
    }

    @Override
    public void endInput() {
        audit.logScanSubtaskSummary(
                runId,
                dryRun,
                subtask,
                parallelism,
                attempt,
                tasksCompleted,
                counters,
                clock.getAsLong() - startMillis);
    }
}

@FunctionalInterface
interface SerializableLongSupplier extends Serializable {
    long getAsLong();
}
