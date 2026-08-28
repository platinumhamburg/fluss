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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.sink.bulkload.BulkLoadSinkTopology;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.flink.utils.PushdownUtils;
import org.apache.fluss.flink.utils.PushdownUtils.FieldEqual;
import org.apache.fluss.flink.utils.PushdownUtils.ValueConversion;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.utils.PushdownUtils.extractFieldEquals;

/** A Flink {@link DynamicTableSink}. */
public class FlinkTableSink
        implements DynamicTableSink,
                SupportsPartitioning,
                SupportsDeletePushDown,
                SupportsRowLevelDelete,
                SupportsRowLevelUpdate {

    private static final String BULK_LOAD_SUPPORTED_FLINK_VERSION = "2.2";

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final RowType tableRowType;
    private final int[] primaryKeyIndexes;
    private final List<String> partitionKeys;
    private final boolean streaming;
    @Nullable private final MergeEngineType mergeEngineType;
    private final boolean sinkIgnoreDelete;
    private final DeleteBehavior tableDeleteBehavior;
    private final int numBucket;
    private final List<String> bucketKeys;
    private final DistributionMode distributionMode;
    private final @Nullable DataLakeFormat lakeFormat;
    @Nullable private final String producerId;
    private final boolean bulkLoadEnabled;
    @Nullable private final Duration bulkLoadBuildTimeout;
    private final Duration bulkLoadAwaitTimeout;
    private final boolean hasAutoIncrementColumn;

    private boolean appliedUpdates = false;
    @Nullable private GenericRow deleteRow;
    /**
     * The static partition spec captured by {@link #applyStaticPartition}; only consumed by the
     * BulkLoad path, the regular path keeps ignoring it.
     */
    @Nullable private Map<String, String> staticPartition;

    public FlinkTableSink(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            int[] primaryKeyIndexes,
            List<String> partitionKeys,
            boolean streaming,
            @Nullable MergeEngineType mergeEngineType,
            @Nullable DataLakeFormat lakeFormat,
            boolean sinkIgnoreDelete,
            DeleteBehavior tableDeleteBehavior,
            int numBucket,
            List<String> bucketKeys,
            DistributionMode distributionMode,
            @Nullable String producerId,
            boolean bulkLoadEnabled,
            @Nullable Duration bulkLoadBuildTimeout,
            Duration bulkLoadAwaitTimeout,
            boolean hasAutoIncrementColumn) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.partitionKeys = partitionKeys;
        this.streaming = streaming;
        this.mergeEngineType = mergeEngineType;
        this.sinkIgnoreDelete = sinkIgnoreDelete;
        this.tableDeleteBehavior = tableDeleteBehavior;
        this.numBucket = numBucket;
        this.bucketKeys = bucketKeys;
        this.distributionMode = distributionMode;
        this.lakeFormat = lakeFormat;
        this.producerId = producerId;
        this.bulkLoadEnabled = bulkLoadEnabled;
        this.bulkLoadBuildTimeout = bulkLoadBuildTimeout;
        this.bulkLoadAwaitTimeout = bulkLoadAwaitTimeout;
        this.hasAutoIncrementColumn = hasAutoIncrementColumn;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (primaryKeyIndexes.length > 0 || (streaming && sinkIgnoreDelete)) {
            // Primary-key tables can accept row-level changes in batch mode. In streaming mode,
            // ignore-delete sinks can also accept and drop DELETE messages.
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                // optimize out the update_before messages
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (bulkLoadEnabled) {
            // The BulkLoad path only supports full-column batch INSERT INTO statements;
            // reject the unsupported statement shapes right at the fork, before the provider
            // is constructed. Batch UPDATE statements reach the sink via applyRowLevelUpdate.
            if (appliedUpdates) {
                throw bulkLoadRejection(
                        "only supports full-column batch INSERT INTO statements; "
                                + "UPDATE statements are not supported.");
            }
            Optional<int[][]> targetColumns = context.getTargetColumns();
            // when no columns specified in insert into, the length of target columns is 0,
            // which is a full-column insert rather than a partial-column one, see FLINK-36000
            if (targetColumns.isPresent()
                    && targetColumns.get().length != 0
                    && targetColumns.get().length != tableRowType.getFieldCount()) {
                throw bulkLoadRejection(
                        String.format(
                                "only supports full-column batch INSERT INTO statements; "
                                        + "partial-column INSERT statements (the statement "
                                        + "targets %d of the %d table columns) are not supported.",
                                targetColumns.get().length, tableRowType.getFieldCount()));
            }
            // The BulkLoad path replaces the regular sink entirely. The eligibility check runs
            // inside consumeDataStream, which is invoked during job translation: the statement
            // still fails fast at compile time, and the execution environment configuration
            // needed for the speculative execution check is available there.
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    validateBulkLoadEligibility(dataStream);
                    return BulkLoadSinkTopology.apply(
                            dataStream,
                            tablePath,
                            flussConfig,
                            tableRowType,
                            partitionKeys,
                            bucketKeys,
                            numBucket,
                            lakeFormat,
                            staticPartition,
                            bulkLoadBuildTimeout,
                            bulkLoadAwaitTimeout);
                }
            };
        }

        int[] targetColumnIndexes = null;
        // skip applying partial-updates for UPDATE command as the Context#targetColumns
        // is not correct, see FLINK-36736
        if (!appliedUpdates
                && context.getTargetColumns().isPresent()
                // when no columns specified in insert into, the length of target columns
                // is 0, when no column specified, it's not partial update
                // see FLINK-36000
                && context.getTargetColumns().get().length != 0) {
            // is partial update, check whether partial update is supported or not
            if (context.getTargetColumns().get().length != tableRowType.getFieldCount()) {
                if (primaryKeyIndexes.length == 0) {
                    throw new ValidationException(
                            "Fluss table sink does not support partial updates for table without primary key. Please make sure the "
                                    + "number of specified columns in INSERT INTO matches columns of the Fluss table.");
                }
                if (mergeEngineType != null && mergeEngineType != MergeEngineType.AGGREGATION) {
                    throw new ValidationException(
                            String.format(
                                    "Table %s uses the '%s' merge engine which does not support partial updates. Please make sure the "
                                            + "number of specified columns in INSERT INTO matches columns of the Fluss table.",
                                    tablePath, mergeEngineType));
                }
                int[][] targetColumns = context.getTargetColumns().get();
                targetColumnIndexes = new int[targetColumns.length];
                for (int i = 0; i < targetColumns.length; i++) {
                    int[] column = targetColumns[i];
                    if (column.length != 1) {
                        throw new ValidationException(
                                "Fluss sink table doesn't support partial updates for nested columns.");
                    }
                    targetColumnIndexes[i] = column[0];
                }
                // check the target column contains the primary key columns
                for (int primaryKeyIndex : primaryKeyIndexes) {
                    if (Arrays.stream(targetColumnIndexes)
                            .noneMatch(targetColumIndex -> targetColumIndex == primaryKeyIndex)) {
                        throw new ValidationException(
                                String.format(
                                        "Fluss table sink does not support partial updates without fully specifying the primary key columns. "
                                                + "The insert columns are %s, but the primary key columns are %s. "
                                                + "Please make sure the specified columns in INSERT INTO contains "
                                                + "the primary key columns.",
                                        columns(targetColumnIndexes), columns(primaryKeyIndexes)));
                    }
                }
            }
            // else, it's full update, ignore the given target columns as we don't care the order
        }

        FlinkSink<RowData> flinkSink = getFlinkSink(targetColumnIndexes);
        // Use DataStreamSinkProvider rather than SinkV2Provider because later won't set default uid
        // for transforms added by addPreWriteTopology.
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return flinkSink.apply(dataStream);
            }
        };
    }

    private FlinkSink<RowData> getFlinkSink(int[] targetColumnIndexes) {
        // Enable undo recovery for aggregation tables
        boolean enableUndoRecovery = mergeEngineType == MergeEngineType.AGGREGATION;

        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter, RowData> flinkSinkWriterBuilder =
                (primaryKeyIndexes.length > 0)
                        ? new FlinkSink.UpsertSinkWriterBuilder<>(
                                tablePath,
                                flussConfig,
                                tableRowType,
                                targetColumnIndexes,
                                numBucket,
                                bucketKeys,
                                partitionKeys,
                                lakeFormat,
                                distributionMode,
                                new RowDataSerializationSchema(false, sinkIgnoreDelete),
                                enableUndoRecovery,
                                producerId)
                        : new FlinkSink.AppendSinkWriterBuilder<>(
                                tablePath,
                                flussConfig,
                                tableRowType,
                                numBucket,
                                bucketKeys,
                                partitionKeys,
                                lakeFormat,
                                distributionMode,
                                new RowDataSerializationSchema(true, sinkIgnoreDelete));

        return new FlinkSink<>(flinkSinkWriterBuilder, tablePath);
    }

    /**
     * Builds the {@link ValidationException} rejecting an unsupported BulkLoad statement, in the
     * same message style as the other BulkLoad rejections.
     */
    private static ValidationException bulkLoadRejection(String reason) {
        return new ValidationException(
                String.format(
                        "The option '%s' is enabled, but the BulkLoad sink %s",
                        FlinkConnectorOptions.SINK_BULK_LOAD_ENABLED.key(), reason));
    }

    private void validateBulkLoadEligibility(DataStream<RowData> dataStream) {
        if (streaming) {
            throw bulkLoadRejection(
                    "requires batch execution mode; the current job runs in streaming mode.");
        }
        if (primaryKeyIndexes.length == 0) {
            throw bulkLoadRejection("only supports primary key tables; the target table has none.");
        }
        if (mergeEngineType != null) {
            throw bulkLoadRejection(
                    String.format(
                            "only supports primary key tables with the default merge engine; "
                                    + "the target table uses the '%s' merge engine.",
                            mergeEngineType));
        }
        if (hasAutoIncrementColumn) {
            throw bulkLoadRejection("does not support tables with an auto-increment column.");
        }
        Map<String, String> partitionSpec =
                staticPartition == null ? Collections.emptyMap() : staticPartition;
        List<String> missingPartitionKeys = new ArrayList<>();
        for (String partitionKey : partitionKeys) {
            if (!partitionSpec.containsKey(partitionKey)) {
                missingPartitionKeys.add(partitionKey);
            }
        }
        if (!missingPartitionKeys.isEmpty()) {
            throw bulkLoadRejection(
                    String.format(
                            "requires a complete static partition spec for partitioned tables, "
                                    + "but the values of partition keys %s are missing from the "
                                    + "static partition spec.",
                            missingPartitionKeys));
        }
        String flinkVersion = EnvironmentInformation.getVersion();
        String[] versionTokens = flinkVersion.split("[.\\-]");
        if (versionTokens.length < 2
                || !BULK_LOAD_SUPPORTED_FLINK_VERSION.equals(
                        versionTokens[0] + "." + versionTokens[1])) {
            throw bulkLoadRejection(
                    String.format(
                            "requires Flink version %s, but the current Flink version is %s.",
                            BULK_LOAD_SUPPORTED_FLINK_VERSION, flinkVersion));
        }
        if (dataStream
                .getExecutionEnvironment()
                .getConfiguration()
                .get(BatchExecutionOptions.SPECULATIVE_ENABLED)) {
            throw bulkLoadRejection(
                    "does not support speculative execution. Please disable "
                            + "'execution.batch.speculative.enabled' when using BulkLoad.");
        }
    }

    private List<String> columns(int[] columnIndexes) {
        List<String> columns = new ArrayList<>();
        for (int columnIndex : columnIndexes) {
            columns.add(tableRowType.getFieldNames().get(columnIndex));
        }
        return columns;
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink sink =
                new FlinkTableSink(
                        tablePath,
                        flussConfig,
                        tableRowType,
                        primaryKeyIndexes,
                        partitionKeys,
                        streaming,
                        mergeEngineType,
                        lakeFormat,
                        sinkIgnoreDelete,
                        tableDeleteBehavior,
                        numBucket,
                        bucketKeys,
                        distributionMode,
                        producerId,
                        bulkLoadEnabled,
                        bulkLoadBuildTimeout,
                        bulkLoadAwaitTimeout,
                        hasAutoIncrementColumn);
        sink.appliedUpdates = appliedUpdates;
        sink.deleteRow = deleteRow;
        sink.staticPartition = staticPartition;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "FlussTableSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // Only consumed by the BulkLoad path; the regular path keeps ignoring it.
        this.staticPartition = new LinkedHashMap<>(partition);
    }

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
        validateDeletable();
        if (filters.size() != primaryKeyIndexes.length) {
            // only supports delete on primary key
            return false;
        }

        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        Map<Integer, LogicalType> primaryKeyTypes = getPrimaryKeyTypes();
        List<FieldEqual> fieldEquals =
                extractFieldEquals(
                        filters,
                        primaryKeyTypes,
                        acceptedFilters,
                        remainingFilters,
                        ValueConversion.FLUSS_INTERNAL_VALUE);
        if (!remainingFilters.isEmpty()) {
            // only supports delete on primary key
            return false;
        }

        HashSet<Integer> visitedPkFields = new HashSet<>();
        GenericRow deleteRow = new GenericRow(tableRowType.getFieldCount());
        for (FieldEqual fieldEqual : fieldEquals) {
            deleteRow.setField(fieldEqual.fieldIndex, fieldEqual.equalValue);
            visitedPkFields.add(fieldEqual.fieldIndex);
        }

        // if not all primary key fields are in condition, we can't push down
        if (!visitedPkFields.equals(primaryKeyTypes.keySet())) {
            return false;
        }

        this.deleteRow = deleteRow;
        return true;
    }

    @Override
    public Optional<Long> executeDeletion() {
        if (deleteRow != null) {
            PushdownUtils.deleteSingleRow(deleteRow, tablePath, flussConfig);
            // return empty to indicate the number of deleted rows is unknown
            return Optional.empty();
        }
        throw new IllegalStateException(
                "Failed to execute DELETE statement as no deletion pushdown, this should never happen.");
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        validateDeletable();
        return new RowLevelDeleteInfo() {};
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns,
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        validateUpdatable();
        Set<String> primaryKeys = getPrimaryKeyNames();
        updatedColumns.forEach(
                column -> {
                    if (primaryKeys.contains(column.getName())) {
                        String errMsg =
                                String.format(
                                        "Updates to primary keys are not supported, primaryKeys (%s), updatedColumns (%s)",
                                        primaryKeys,
                                        updatedColumns.stream()
                                                .map(Column::getName)
                                                .collect(Collectors.toList()));
                        throw new UnsupportedOperationException(errMsg);
                    }
                });

        appliedUpdates = true;
        return new RowLevelUpdateInfo() {
            @Override
            public Optional<List<Column>> requiredColumns() {
                // TODO: return primary-key columns to support partial-updates after
                //  FLINK-36735 is resolved.
                return Optional.empty();
            }

            @Override
            public RowLevelUpdateMode getRowLevelUpdateMode() {
                return RowLevelUpdateMode.UPDATED_ROWS;
            }
        };
    }

    private void validateUpdatable() {
        if (primaryKeyIndexes.length == 0) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s is a Log Table. Log Table doesn't support DELETE and UPDATE statements.",
                            tablePath));
        }
        if (mergeEngineType != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s uses the '%s' merge engine which does not support DELETE or UPDATE statements.",
                            tablePath, mergeEngineType));
        }
    }

    private void validateDeletable() {
        validateUpdatable();
        if (tableDeleteBehavior == DeleteBehavior.DISABLE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table %s has delete behavior set to 'disable' which does not support DELETE statements.",
                            tablePath));
        }
    }

    private Map<Integer, LogicalType> getPrimaryKeyTypes() {
        Map<Integer, LogicalType> pkTypes = new HashMap<>();
        for (int index : primaryKeyIndexes) {
            pkTypes.put(index, tableRowType.getTypeAt(index));
        }
        return pkTypes;
    }

    private Set<String> getPrimaryKeyNames() {
        Set<String> pkNames = new HashSet<>();
        for (int index : primaryKeyIndexes) {
            pkNames.add(tableRowType.getFieldNames().get(index));
        }
        return pkNames;
    }

    @VisibleForTesting
    public List<String> getBucketKeys() {
        return bucketKeys;
    }
}
