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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    // the log format of the table
    private final LogFormat logFormat;
    // the schema of the date read form server or remote. (which is projected in the server side)
    private final RowType dataRowType;
    // the static schemaId of the table, should support dynamic schema evolution in the future
    private final int schemaId;
    // the Arrow vector schema root of the table, should be null if not ARROW log format
    @Nullable private final VectorSchemaRoot vectorSchemaRoot;
    // the Arrow memory buffer allocator for the table, should be null if not ARROW log format
    @Nullable private final BufferAllocator bufferAllocator;
    // the final selected fields of the read data
    private final FieldGetter[] selectedFieldGetters;
    // whether the projection is push downed to the server side and the returned data is pruned.
    private final boolean projectionPushDowned;
    // whether the recordBatchFilter is push downed to the server side and the returned data is
    // filtered.
    private final boolean filterPushDowned;

    /**
     * Creates a LogRecordReadContext for the given table information and projection information.
     */
    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo, boolean readFromRemote, @Nullable Projection projection) {
        return createReadContext(tableInfo, readFromRemote, projection, false);
    }

    /**
     * Creates a LogRecordReadContext for the given table information, projection information and
     * filter information.
     */
    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            @Nullable Projection projection,
            boolean hasRecordBatchFilter) {
        RowType rowType = tableInfo.getRowType();
        LogFormat logFormat = tableInfo.getTableConfig().getLogFormat();
        // only for arrow log format, the projection can be push downed to the server side
        boolean projectionPushDowned = projection != null && logFormat == LogFormat.ARROW;
        // recordBatchFilter can be push downed to the server side when hasRecordBatchFilter is true
        boolean filterPushDowned = hasRecordBatchFilter && !readFromRemote;
        int schemaId = tableInfo.getSchemaId();
        if (projection == null) {
            // set a default dummy projection to simplify code
            projection = Projection.of(IntStream.range(0, rowType.getFieldCount()).toArray());
        }

        if (logFormat == LogFormat.ARROW) {
            if (readFromRemote) {
                // currently, for remote read, arrow log doesn't support projection pushdown,
                // so set the rowType as is.
                int[] selectedFields = projection.getProjection();
                return createArrowReadContext(
                        rowType, schemaId, selectedFields, false, filterPushDowned);
            } else {
                // arrow data that returned from server has been projected (in order)
                RowType projectedRowType = projection.projectInOrder(rowType);
                // need to reorder the fields for final output
                int[] selectedFields = projection.getReorderingIndexes();
                return createArrowReadContext(
                        projectedRowType,
                        schemaId,
                        selectedFields,
                        projectionPushDowned,
                        filterPushDowned);
            }
        } else if (logFormat == LogFormat.INDEXED) {
            int[] selectedFields = projection.getProjection();
            return createIndexedReadContext(rowType, schemaId, selectedFields, filterPushDowned);
        } else {
            throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    private static LogRecordReadContext createArrowReadContext(
            RowType dataRowType, int schemaId, int[] selectedFields, boolean projectionPushDowned) {
        return createArrowReadContext(
                dataRowType, schemaId, selectedFields, projectionPushDowned, false);
    }

    private static LogRecordReadContext createArrowReadContext(
            RowType dataRowType,
            int schemaId,
            int[] selectedFields,
            boolean projectionPushDowned,
            boolean filterPushDowned) {
        // TODO: use a more reasonable memory limit
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot vectorRoot =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(dataRowType), allocator);
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(dataRowType, selectedFields);
        return new LogRecordReadContext(
                LogFormat.ARROW,
                dataRowType,
                schemaId,
                vectorRoot,
                allocator,
                fieldGetters,
                projectionPushDowned,
                filterPushDowned);
    }

    /**
     * Creates a testing purpose LogRecordReadContext for ARROW log format, that underlying Arrow
     * resources are not reused.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     */
    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(RowType rowType, int schemaId) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createArrowReadContext(rowType, schemaId, selectedFields, false);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     */
    public static LogRecordReadContext createIndexedReadContext(RowType rowType, int schemaId) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createIndexedReadContext(rowType, schemaId, selectedFields);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     */
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, int[] selectedFields) {
        return createIndexedReadContext(rowType, schemaId, selectedFields, false);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     * @param filterPushDowned whether the recordBatchFilter is push downed
     */
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, int[] selectedFields, boolean filterPushDowned) {
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(rowType, selectedFields);
        // for INDEXED log format, the projection is NEVER push downed to the server side
        return new LogRecordReadContext(
                LogFormat.INDEXED,
                rowType,
                schemaId,
                null,
                null,
                fieldGetters,
                false,
                filterPushDowned);
    }

    private LogRecordReadContext(
            LogFormat logFormat,
            RowType dataRowType,
            int schemaId,
            VectorSchemaRoot vectorSchemaRoot,
            BufferAllocator bufferAllocator,
            FieldGetter[] selectedFieldGetters,
            boolean projectionPushDowned,
            boolean filterPushDowned) {
        this.logFormat = logFormat;
        this.dataRowType = dataRowType;
        this.schemaId = schemaId;
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.bufferAllocator = bufferAllocator;
        this.selectedFieldGetters = selectedFieldGetters;
        this.projectionPushDowned = projectionPushDowned;
        this.filterPushDowned = filterPushDowned;
    }

    @Override
    public LogFormat getLogFormat() {
        return logFormat;
    }

    @Override
    public RowType getRowType(int schemaId) {
        checkArgument(
                schemaId == this.schemaId,
                "The schemaId (%s) in the record batch is not the same as the context (%s).",
                schemaId,
                this.schemaId);
        return dataRowType;
    }

    /** Get the selected field getters for the read data. */
    public FieldGetter[] getSelectedFieldGetters() {
        return selectedFieldGetters;
    }

    /** Whether the projection is push downed to the server side and the returned data is pruned. */
    public boolean isProjectionPushDowned() {
        return projectionPushDowned;
    }

    /**
     * Whether the recordBatchFilter is push downed to the server side and the returned data is
     * filtered.
     */
    public boolean isFilterPushDowned() {
        return filterPushDowned;
    }

    /** Get the schema ID of the table. */
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(int schemaId) {
        checkArgument(
                schemaId == this.schemaId,
                "The schemaId (%s) in the record batch is not the same as the context (%s).",
                schemaId,
                this.schemaId);
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Only Arrow log format provides vector schema root.");
        }
        checkNotNull(vectorSchemaRoot, "The vector schema root is not available.");
        return vectorSchemaRoot;
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException("Only Arrow log format provides buffer allocator.");
        }
        checkNotNull(bufferAllocator, "The buffer allocator is not available.");
        return bufferAllocator;
    }

    public void close() {
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
        }
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
    }

    private static FieldGetter[] buildProjectedFieldGetters(RowType rowType, int[] selectedFields) {
        List<DataType> dataTypeList = rowType.getChildren();
        FieldGetter[] fieldGetters = new FieldGetter[selectedFields.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldGetters[i] =
                    InternalRow.createFieldGetter(
                            dataTypeList.get(selectedFields[i]), selectedFields[i]);
        }
        return fieldGetters;
    }
}
