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

package org.apache.fluss.flink.sink;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DataStatisticsOperatorFactory;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecord;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecordChannelComputer;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecordTypeInformation;
import org.apache.fluss.flink.sink.undo.OffsetReportContext;
import org.apache.fluss.flink.sink.undo.UndoRecoveryOperatorFactory;
import org.apache.fluss.flink.sink.writer.AppendSinkWriter;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.flink.sink.writer.UpsertSinkWriter;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static org.apache.fluss.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;

/**
 * Flink sink for Fluss.
 *
 * <p>State management for undo recovery is handled by {@code UndoRecoveryOperator} using Union List
 * State, so this sink does not implement StatefulSink or SupportsWriterState.
 */
class FlinkSink<InputT> extends SinkAdapter<InputT> {

    private static final long serialVersionUID = 1L;

    protected final SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> builder;
    private final TablePath tablePath;

    /**
     * Whether undo recovery is enabled for this sink.
     *
     * <p>When true, an UndoRecoveryOperator will be added before the sink to manage state using
     * Union List State for correct redistribution during scale up/down scenarios.
     *
     * <p>This is typically enabled for aggregation tables that require undo recovery for
     * exactly-once semantics.
     */
    private final boolean enableUndoRecovery;

    FlinkSink(
            SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> builder,
            TablePath tablePath,
            boolean enableUndoRecovery) {
        this.builder = builder;
        this.tablePath = tablePath;
        this.enableUndoRecovery = enableUndoRecovery;
    }

    @Override
    protected SinkWriter<InputT> createWriter(
            MailboxExecutor mailboxExecutor, SinkWriterMetricGroup metricGroup) {
        FlinkSinkWriter<InputT> flinkSinkWriter = builder.createWriter(mailboxExecutor);
        flinkSinkWriter.initialize(metricGroup);
        return flinkSinkWriter;
    }

    /**
     * {@link FlinkSink} serves as the SQL connector. Since we uses {@link DataStreamSinkProvider}
     * (rather than {@link SinkV2Provider}), it does not automatically recognize or invoke the
     * {@link SupportsPreWriteTopology} interface. Therefore, the pre-write topology must be added
     * manually here.
     *
     * <p>In contrast, {@link FlussSink} is used directly as a DataStream connector, Flink's runtime
     * explicitly checks for the {@link SupportsPreWriteTopology} interface and automatically
     * incorporates the pre-write topology if present. To support this path, the {@link
     * SupportsPreWriteTopology} implementation resides in {@link FlussSink}.
     */
    public DataStreamSink<InputT> apply(DataStream<InputT> input) {
        DataStream<InputT> stream = builder.addPreWriteTopology(input);

        // Add UndoRecoveryOperator for aggregation tables
        stream = addUndoRecoveryOperatorIfNeeded(stream);

        return stream.sinkTo(this)
                .name("Sink(" + tablePath + ")")
                .setParallelism(stream.getParallelism());
    }

    /**
     * Adds UndoRecoveryOperator to the stream if undo recovery is enabled for aggregation tables.
     *
     * <p>This method is shared between {@link FlinkSink#apply} and {@link
     * FlussSink#addPreWriteTopology} to avoid code duplication.
     *
     * @param stream the input stream after pre-write topology
     * @return the stream with UndoRecoveryOperator added if needed, otherwise the original stream
     */
    protected DataStream<InputT> addUndoRecoveryOperatorIfNeeded(DataStream<InputT> stream) {
        if (enableUndoRecovery && builder instanceof UpsertSinkWriterBuilder) {
            @SuppressWarnings("unchecked")
            UpsertSinkWriterBuilder<InputT> upsertBuilder =
                    (UpsertSinkWriterBuilder<InputT>) builder;

            // Create UndoRecoveryOperatorFactory with required parameters
            UndoRecoveryOperatorFactory<InputT> operatorFactory =
                    new UndoRecoveryOperatorFactory<>(
                            tablePath,
                            upsertBuilder.getFlussConfig(),
                            toFlussRowType(upsertBuilder.getTableRowType()),
                            upsertBuilder.getTargetColumnIndexes(),
                            upsertBuilder.getNumBucket(),
                            upsertBuilder.isPartitioned(),
                            upsertBuilder.getProducerId());

            // Add UndoRecoveryOperator to the stream
            stream =
                    stream.transform(
                                    "UndoRecovery(" + tablePath + ")",
                                    stream.getType(),
                                    operatorFactory)
                            .setParallelism(stream.getParallelism());

            // Pass the OffsetReportContext to the builder for the writer to use
            upsertBuilder.setOffsetReportContext(operatorFactory.getOffsetReportContext());
        }
        return stream;
    }

    @Internal
    interface SinkWriterBuilder<W extends FlinkSinkWriter<InputT>, InputT> extends Serializable {
        W createWriter(MailboxExecutor mailboxExecutor);

        DataStream<InputT> addPreWriteTopology(DataStream<InputT> input);
    }

    @Internal
    static class AppendSinkWriterBuilder<InputT>
            implements SinkWriterBuilder<AppendSinkWriter<InputT>, InputT> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final DistributionMode distributionMode;
        private final FlussSerializationSchema<InputT> flussSerializationSchema;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                DistributionMode distributionMode,
                FlussSerializationSchema<InputT> flussSerializationSchema) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.distributionMode = distributionMode;
            this.flussSerializationSchema = flussSerializationSchema;
        }

        @Override
        public AppendSinkWriter<InputT> createWriter(MailboxExecutor mailboxExecutor) {
            return new AppendSinkWriter<>(
                    tablePath,
                    flussConfig,
                    tableRowType,
                    mailboxExecutor,
                    flussSerializationSchema);
        }

        @Override
        public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
            switch (distributionMode) {
                case NONE:
                    return input;
                case AUTO:
                    // if bucket keys are not empty, use BUCKET as default. Otherwise, use NONE.
                    return bucketKeys.isEmpty() ? input : bucketShuffle(input);
                case BUCKET:
                    if (!bucketKeys.isEmpty()) {
                        return bucketShuffle(input);
                    }
                    throw new UnsupportedOperationException(
                            "BUCKET mode is only supported for log tables with bucket keys");
                case PARTITION_DYNAMIC:
                    if (partitionKeys.isEmpty()) {
                        throw new UnsupportedOperationException(
                                "PARTITION_DYNAMIC is only supported for partitioned tables");
                    }

                    TypeInformation<StatisticsOrRecord<InputT>> statisticsOrRecordTypeInformation =
                            new StatisticsOrRecordTypeInformation<>(input.getType());
                    SingleOutputStreamOperator<StatisticsOrRecord<InputT>> shuffleStream =
                            input.transform(
                                            "Collect Statistics",
                                            statisticsOrRecordTypeInformation,
                                            new DataStatisticsOperatorFactory<>(
                                                    toFlussRowType(tableRowType),
                                                    partitionKeys,
                                                    flussSerializationSchema))
                                    // Set the parallelism same as input operator to encourage
                                    // chaining
                                    .setParallelism(input.getParallelism());

                    return partition(
                                    shuffleStream,
                                    new StatisticsOrRecordChannelComputer<>(
                                            toFlussRowType(tableRowType),
                                            bucketKeys,
                                            partitionKeys,
                                            numBucket,
                                            lakeFormat,
                                            flussSerializationSchema),
                                    input.getParallelism())
                            .flatMap(
                                    (FlatMapFunction<StatisticsOrRecord<InputT>, InputT>)
                                            (statisticsOrRecord, out) -> {
                                                if (statisticsOrRecord.isRecord()) {
                                                    out.collect(statisticsOrRecord.record());
                                                }
                                            })
                            .name("Strip Statistics")
                            .setParallelism(input.getParallelism())
                            // we remove the slot sharing group here make all operators can be
                            // co-located in the same TaskManager slot
                            .returns(input.getType());

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported distribution mode: " + distributionMode);
            }
        }

        private DataStream<InputT> bucketShuffle(DataStream<InputT> input) {
            return partition(
                    input,
                    new FlinkRowDataChannelComputer<>(
                            toFlussRowType(tableRowType),
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            numBucket,
                            flussSerializationSchema),
                    input.getParallelism());
        }
    }

    @Internal
    static class UpsertSinkWriterBuilder<InputT>
            implements SinkWriterBuilder<UpsertSinkWriter<InputT>, InputT> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final @Nullable int[] targetColumnIndexes;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final DistributionMode distributionMode;
        private final FlussSerializationSchema<InputT> flussSerializationSchema;

        /**
         * Optional producer ID for undo recovery. If null, defaults to the Flink job ID.
         *
         * <p>This is used by UndoRecoveryOperator to register and retrieve producer offsets for
         * pre-checkpoint failure recovery.
         */
        @Nullable private final String producerId;

        /**
         * Optional context for reporting offsets to the upstream UndoRecoveryOperator.
         *
         * <p>This is set by FlinkSink.apply() or FlussSink.addPreWriteTopology() when
         * UndoRecoveryOperator is added to the pipeline. The context is then passed to the
         * UpsertSinkWriter during creation.
         *
         * <p>Note: This field is NOT transient because the OffsetReportContextHolder is
         * serializable and needs to survive job serialization to be passed to the TaskManager.
         */
        @Nullable private OffsetReportContext offsetReportContext;

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                DistributionMode distributionMode,
                FlussSerializationSchema<InputT> flussSerializationSchema) {
            this(
                    tablePath,
                    flussConfig,
                    tableRowType,
                    targetColumnIndexes,
                    numBucket,
                    bucketKeys,
                    partitionKeys,
                    lakeFormat,
                    distributionMode,
                    flussSerializationSchema,
                    null);
        }

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                DistributionMode distributionMode,
                FlussSerializationSchema<InputT> flussSerializationSchema,
                @Nullable String producerId) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.distributionMode = distributionMode;
            this.flussSerializationSchema = flussSerializationSchema;
            this.producerId = producerId;
        }

        @Override
        public UpsertSinkWriter<InputT> createWriter(MailboxExecutor mailboxExecutor) {
            return new UpsertSinkWriter<>(
                    tablePath,
                    flussConfig,
                    tableRowType,
                    targetColumnIndexes,
                    mailboxExecutor,
                    flussSerializationSchema,
                    offsetReportContext);
        }

        @Override
        public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
            switch (distributionMode) {
                case NONE:
                    return input;
                case AUTO:
                case BUCKET:
                    return partition(
                            input,
                            new FlinkRowDataChannelComputer<>(
                                    toFlussRowType(tableRowType),
                                    bucketKeys,
                                    partitionKeys,
                                    lakeFormat,
                                    numBucket,
                                    flussSerializationSchema),
                            input.getParallelism());
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported distribution mode: %s for primary key table",
                                    distributionMode));
            }
        }

        // ==================== Getters for UndoRecoveryOperator integration ====================

        Configuration getFlussConfig() {
            return flussConfig;
        }

        RowType getTableRowType() {
            return tableRowType;
        }

        @Nullable
        int[] getTargetColumnIndexes() {
            return targetColumnIndexes;
        }

        int getNumBucket() {
            return numBucket;
        }

        boolean isPartitioned() {
            return !partitionKeys.isEmpty();
        }

        @Nullable
        String getProducerId() {
            return producerId;
        }

        void setOffsetReportContext(@Nullable OffsetReportContext offsetReportContext) {
            this.offsetReportContext = offsetReportContext;
        }
    }
}
