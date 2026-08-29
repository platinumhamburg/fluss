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

package org.apache.fluss.flink.sink.bulkload;

import org.apache.fluss.client.bulkload.BulkLoadBucketWriter;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.flink.row.FlinkAsFlussRow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.File;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Builds exactly one Fluss bucket in each subtask.
 *
 * <p>The topology fixes this operator's parallelism to the target bucket count and routes bucket
 * {@code b} directly to subtask {@code b}. Every task owns one RocksDB instance, including an empty
 * bucket, and the public writer revalidates that each row belongs to the task's bucket.
 */
final class BulkLoadBuildOperator extends AbstractStreamOperator<BulkLoadCommittable>
        implements TwoInputStreamOperator<RowData, BulkLoadBuildContext, BulkLoadCommittable>,
                BoundedMultiInput,
                InputSelectable {

    private static final long serialVersionUID = 1L;

    private transient boolean contextInputEnded;
    private transient int bucketId;
    private transient @Nullable BulkLoadBuildContext buildContext;
    private transient @Nullable BulkLoadBucketWriter bucketWriter;
    private transient @Nullable FlinkAsFlussRow rowWrapper;

    @Override
    public void open() throws Exception {
        super.open();
        bucketId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public InputSelection nextSelection() {
        return contextInputEnded ? InputSelection.FIRST : InputSelection.SECOND;
    }

    @Override
    public void processElement2(StreamRecord<BulkLoadBuildContext> element) {
        checkState(
                buildContext == null,
                "The BulkLoad build operator expects exactly one build context.");
        BulkLoadBuildContext context = element.getValue();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        checkState(
                parallelism == context.getTableInfo().getNumBuckets(),
                "BulkLoad build parallelism %s differs from the target bucket count %s.",
                parallelism,
                context.getTableInfo().getNumBuckets());
        buildContext = context;
        rowWrapper = new FlinkAsFlussRow();
        bucketWriter = new BulkLoadBucketWriter(context, bucketId, bucketWorkDirectory());
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) {
        checkNotNull(
                        bucketWriter,
                        "The BulkLoad build operator received data before the build context.")
                .add(
                        checkNotNull(rowWrapper, "The BulkLoad row wrapper is not initialized.")
                                .replace(element.getValue()));
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (inputId == 2) {
            checkNotNull(
                    buildContext,
                    "The BulkLoad context input ended before a build context arrived.");
            contextInputEnded = true;
            return;
        }
        checkState(inputId == 1, "Unknown input id %s.", inputId);
        BulkLoadBuildContext context =
                checkNotNull(buildContext, "BulkLoad data ended before the build context arrived.");
        BulkLoadBucketWriter writer =
                checkNotNull(bucketWriter, "BulkLoad bucket writer is not initialized.");
        output.collect(new StreamRecord<>(new BulkLoadCommittable(context, writer.finish())));
        writer.close();
        bucketWriter = null;
    }

    @Override
    public void close() throws Exception {
        try {
            BulkLoadBucketWriter writer = bucketWriter;
            bucketWriter = null;
            if (writer != null) {
                writer.close();
            }
        } finally {
            super.close();
        }
    }

    private File bucketWorkDirectory() {
        String[] tmpDirs =
                getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories();
        checkState(
                tmpDirs.length > 0,
                "The TaskManager exposes no temporary directories for BulkLoad input building.");
        return new File(tmpDirs[Math.floorMod(bucketId, tmpDirs.length)]);
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {}

    @Override
    public void processWatermark2(Watermark mark) throws Exception {}

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}
}
