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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Assembles the DataStream topology of the BulkLoad sink for a batch {@code INSERT INTO} a
 * primary-key table.
 *
 * <p>The topology wires the three operators of the primary-key bulk-load protocol:
 *
 * <pre>
 *   [trigger: one element]                    [table rows]
 *          |                                       |
 *   BulkLoadBegin (p=1)                  partitionCustom by bucket id
 *          |  (broadcast)                           |
 *          +------------------&gt; BulkLoadBuild (p=numBuckets)
 *                                      |
 *                              BulkLoadCommit (p=1)
 * </pre>
 *
 * <ul>
 *   <li>{@link BulkLoadBeginOperator} runs at parallelism one on a single trigger element and
 *       broadcasts exactly one frozen build context to every Build subtask;
 *   <li>the data edge is partitioned by the Fluss bucket id: the key selector computes the bucket
 *       full-schema row once and routes bucket {@code b} directly to subtask {@code b};
 *   <li>{@link BulkLoadBuildOperator} runs at the bucket count, owns one RocksDB, and emits one
 *       committable for its bucket;
 *   <li>{@link BulkLoadCommitSink} runs at parallelism one and hands the complete bucket set to
 *       Flink's standard committer, which drives the transaction to readiness.
 * </ul>
 *
 * <p>Only Flink API present in both the Flink 1.20 compile baseline and the Flink 2.2 runtime of
 * the shaded connector is used ({@code fromElements}, {@code broadcast}, {@code connect} + {@code
 * transform}, {@code partitionCustom}, {@code sinkTo}, and {@code SupportsCommitter}).
 *
 * <p>Eligibility is <b>not</b> re-checked here; the table sink validates it first. This class only
 * translates already-validated parameters into the topology.
 */
@Internal
public final class BulkLoadSinkTopology {

    private BulkLoadSinkTopology() {}

    /**
     * Wires the BulkLoad sink topology onto the given sink input stream and returns the sink.
     *
     * @param dataStream the sink input rows; for a static-partition INSERT the partition columns
     *     carry the static partition constants materialized by the planner
     * @param tablePath the target table
     * @param flussConfig the Fluss client configuration
     * @param tableRowType the full table schema (Flink row type)
     * @param partitionKeys the partition keys of the target table, empty when not partitioned
     * @param bucketKeys the effective bucket keys of the target table
     * @param numBuckets the number of buckets of the target table
     * @param lakeFormat the data lake format of the target table, or null
     * @param staticPartition the static partition spec of the statement, or null for
     *     non-partitioned targets
     * @param buildTimeout the build deadline passed to BulkLoad Begin, or null for the server
     *     default
     * @param awaitTimeout the client-side upper bound for awaiting transaction readiness
     */
    public static DataStreamSink<?> apply(
            DataStream<RowData> dataStream,
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            List<String> partitionKeys,
            List<String> bucketKeys,
            int numBuckets,
            @Nullable DataLakeFormat lakeFormat,
            @Nullable Map<String, String> staticPartition,
            @Nullable Duration buildTimeout,
            Duration awaitTimeout) {
        checkNotNull(dataStream, "Sink input stream must not be null.");
        checkNotNull(tablePath, "Target table path must not be null.");
        checkNotNull(flussConfig, "Fluss configuration must not be null.");
        checkNotNull(tableRowType, "Table row type must not be null.");
        checkNotNull(partitionKeys, "Partition keys must not be null.");
        checkNotNull(bucketKeys, "Bucket keys must not be null.");
        checkNotNull(awaitTimeout, "BulkLoad await timeout must not be null.");
        Map<String, String> staticPartitionValues =
                staticPartition == null ? Collections.emptyMap() : staticPartition;
        PhysicalTablePath target = physicalTarget(tablePath, partitionKeys, staticPartitionValues);

        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
        // Exactly one trigger element: fromElements is a non-parallel source, and the begin
        // operator contract requires exactly one trigger per operator instance anyway.
        DataStream<BulkLoadBuildContext> buildContextStream =
                env.fromElements(0L)
                        .transform(
                                "BulkLoadBegin(" + target + ")",
                                TypeInformation.of(BulkLoadBuildContext.class),
                                new BulkLoadBeginOperator(
                                        flussConfig, target, buildTimeout, awaitTimeout))
                        .setParallelism(1);

        org.apache.fluss.types.RowType flussRowType = FlinkConversions.toFlussRowType(tableRowType);
        DataStream<RowData> routed =
                dataStream.partitionCustom(
                        new BucketPartitioner(),
                        new BucketIdKeySelector(flussRowType, bucketKeys, lakeFormat, numBuckets));

        DataStream<BulkLoadCommittable> committables =
                routed.connect(buildContextStream.broadcast())
                        .transform(
                                "BulkLoadBuild(" + target + ")",
                                TypeInformation.of(BulkLoadCommittable.class),
                                new BulkLoadBuildOperator())
                        .setParallelism(numBuckets);

        return committables
                .sinkTo(new BulkLoadCommitSink(flussConfig, awaitTimeout))
                .name("BulkLoadCommit(" + target + ")")
                .setParallelism(1);
    }

    /**
     * Resolves the physical BulkLoad target: the table itself for a non-partitioned target, or the
     * single static partition named with the Fluss partition name rule (the values joined by '$' in
     * partition key definition order).
     */
    private static PhysicalTablePath physicalTarget(
            TablePath tablePath, List<String> partitionKeys, Map<String, String> staticPartition) {
        if (partitionKeys.isEmpty()) {
            return PhysicalTablePath.of(tablePath);
        }
        List<String> partitionValues = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            String value = staticPartition.get(partitionKey);
            checkArgument(
                    value != null,
                    "The static partition spec %s does not contain a value for partition key"
                            + " '%s'.",
                    staticPartition,
                    partitionKey);
            partitionValues.add(value);
        }
        return PhysicalTablePath.of(
                tablePath,
                new ResolvedPartitionSpec(partitionKeys, partitionValues).getPartitionName());
    }

    /**
     * The key selector of the bucket-partitioned data edge: computes the Fluss bucket id of the
     * full-schema sink input row.
     *
     * <p>The selector is serializable: the Fluss row type and bucket keys are plain serializable
     * values, while the row wrapper and bucket assigner are transient and lazily initialized on the
     * first row.
     */
    static final class BucketIdKeySelector implements KeySelector<RowData, Integer> {

        private static final long serialVersionUID = 1L;

        private final org.apache.fluss.types.RowType flussRowType;
        private final List<String> bucketKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final int numBuckets;

        private transient @Nullable BulkLoadBucketAssigner bucketAssigner;
        private transient @Nullable FlinkAsFlussRow rowWrapper;

        BucketIdKeySelector(
                org.apache.fluss.types.RowType flussRowType,
                List<String> bucketKeys,
                @Nullable DataLakeFormat lakeFormat,
                int numBuckets) {
            this.flussRowType = checkNotNull(flussRowType, "Fluss row type must not be null.");
            checkNotNull(bucketKeys, "Bucket keys must not be null.");
            checkArgument(!bucketKeys.isEmpty(), "Bucket keys must not be empty.");
            this.bucketKeys = Collections.unmodifiableList(new ArrayList<>(bucketKeys));
            this.lakeFormat = lakeFormat;
            checkArgument(numBuckets > 0, "Number of buckets must be positive.");
            this.numBuckets = numBuckets;
        }

        @Override
        public Integer getKey(RowData row) {
            BulkLoadBucketAssigner assigner = bucketAssigner;
            if (assigner == null) {
                assigner =
                        new BulkLoadBucketAssigner(
                                flussRowType, bucketKeys, lakeFormat, numBuckets);
                bucketAssigner = assigner;
                rowWrapper = new FlinkAsFlussRow();
            }
            return assigner.assign(checkNotNull(rowWrapper).replace(row));
        }
    }

    private static final class BucketPartitioner implements Partitioner<Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public int partition(Integer bucketId, int numPartitions) {
            checkArgument(
                    bucketId >= 0 && bucketId < numPartitions,
                    "BulkLoad bucket %s is out of range [0, %s).",
                    bucketId,
                    numPartitions);
            return bucketId;
        }
    }
}
