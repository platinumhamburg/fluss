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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;

import java.util.BitSet;
import java.util.List;

/**
 * Utility class for processing aggregated fields from old and new rows.
 *
 * <p>This class provides reusable methods for aggregating fields with different strategies (full
 * aggregation or partial aggregation with target columns). It handles schema evolution by matching
 * fields using column IDs.
 *
 * <p>Note: This class processes aggregation logic and delegates encoding to {@link RowEncoder}. The
 * class is designed to be stateless and thread-safe, with all methods being static.
 */
public final class AggregateFieldsProcessor {

    // A FieldGetter that always returns null, used for non-existent columns in schema evolution
    private static final InternalRow.FieldGetter NULL_FIELD_GETTER = row -> null;

    // Private constructor to prevent instantiation
    private AggregateFieldsProcessor() {}

    /**
     * Aggregate all fields from old and new rows (full aggregation).
     *
     * <p>This method handles schema evolution by matching fields using column IDs. When both
     * schemas are identical, uses a fast path that avoids column ID lookup.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param oldContext context for the old schema
     * @param newContext context for the new schema
     * @param encoder the row encoder to encode results
     */
    public static void aggregateAllFields(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newContext,
            RowEncoder encoder) {
        if (newContext == oldContext) {
            aggregateAllFieldsWithSameSchema(oldRow, newRow, newContext, encoder);
        } else {
            aggregateAllFieldsWithDifferentSchema(oldRow, newRow, oldContext, newContext, encoder);
        }
    }

    /**
     * Aggregate and encode a single field.
     *
     * @param oldFieldGetter getter for the old field
     * @param newFieldGetter getter for the new field
     * @param oldRow the old row
     * @param newRow the new row
     * @param aggregator the aggregator for this field
     * @param targetIdx the target index to encode
     * @param encoder the row encoder
     */
    private static void aggregateAndEncode(
            InternalRow.FieldGetter oldFieldGetter,
            InternalRow.FieldGetter newFieldGetter,
            BinaryRow oldRow,
            BinaryRow newRow,
            FieldAggregator aggregator,
            int targetIdx,
            RowEncoder encoder) {
        Object accumulator = oldFieldGetter.getFieldOrNull(oldRow);
        Object inputField = newFieldGetter.getFieldOrNull(newRow);
        Object mergedField = aggregator.agg(accumulator, inputField);
        encoder.encodeField(targetIdx, mergedField);
    }

    /**
     * Copy and encode a field value from old row.
     *
     * @param fieldGetter getter for the field
     * @param oldRow the old row
     * @param targetIdx the target index to encode
     * @param encoder the row encoder
     */
    private static void copyOldValueAndEncode(
            InternalRow.FieldGetter fieldGetter,
            BinaryRow oldRow,
            int targetIdx,
            RowEncoder encoder) {
        encoder.encodeField(targetIdx, fieldGetter.getFieldOrNull(oldRow));
    }

    /**
     * Aggregate target fields from old and new rows (partial aggregation).
     *
     * <p>For target columns, aggregate with the aggregation function. For non-target columns, keep
     * the old value unchanged. For new columns that don't exist in old schema, copy from newRow.
     *
     * <p>When both schemas are identical, uses a fast path that avoids column ID lookup.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param oldContext context for the old schema
     * @param newContext context for the new schema
     * @param targetColumnIdBitSet BitSet marking target columns by column ID
     * @param encoder the row encoder to encode results
     */
    public static void aggregateTargetFields(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newContext,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        if (newContext == oldContext) {
            aggregateTargetFieldsWithSameSchema(
                    oldRow, newRow, newContext, targetColumnIdBitSet, encoder);
        } else {
            aggregateTargetFieldsWithDifferentSchema(
                    oldRow, newRow, oldContext, newContext, targetColumnIdBitSet, encoder);
        }
    }

    /**
     * Aggregate all fields when old and new schemas are identical.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     */
    private static void aggregateAllFieldsWithSameSchema(
            BinaryRow oldRow, BinaryRow newRow, AggregationContext context, RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        int fieldCount = context.getFieldCount();

        for (int idx = 0; idx < fieldCount; idx++) {
            aggregateAndEncode(
                    fieldGetters[idx],
                    fieldGetters[idx],
                    oldRow,
                    newRow,
                    aggregators[idx],
                    idx,
                    encoder);
        }
    }

    /**
     * Aggregate all fields when old and new schemas differ (schema evolution).
     *
     * <p>Slow path: requires column ID matching to find corresponding fields between schemas.
     */
    private static void aggregateAllFieldsWithDifferentSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newContext,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] newFieldGetters = newContext.getFieldGetters();
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        FieldAggregator[] newAggregators = newContext.getAggregators();
        List<Schema.Column> newColumns = newContext.getSchema().getColumns();

        for (int newIdx = 0; newIdx < newColumns.size(); newIdx++) {
            Schema.Column newColumn = newColumns.get(newIdx);
            int columnId = newColumn.getColumnId();

            // Find the corresponding field in old schema using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);

            // Use old field getter if column exists in old schema, otherwise use null getter
            InternalRow.FieldGetter oldFieldGetter =
                    (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;

            aggregateAndEncode(
                    oldFieldGetter,
                    newFieldGetters[newIdx],
                    oldRow,
                    newRow,
                    newAggregators[newIdx],
                    newIdx,
                    encoder);
        }
    }

    /**
     * Aggregate target fields when old and new schemas are identical.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     */
    private static void aggregateTargetFieldsWithSameSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext context,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();
        FieldAggregator[] aggregators = context.getAggregators();
        List<Schema.Column> columns = context.getSchema().getColumns();
        int fieldCount = context.getFieldCount();

        for (int idx = 0; idx < fieldCount; idx++) {
            int columnId = columns.get(idx).getColumnId();

            if (targetColumnIdBitSet.get(columnId)) {
                // Target column: aggregate and encode
                aggregateAndEncode(
                        fieldGetters[idx],
                        fieldGetters[idx],
                        oldRow,
                        newRow,
                        aggregators[idx],
                        idx,
                        encoder);
            } else {
                // Non-target column: encode old value
                copyOldValueAndEncode(fieldGetters[idx], oldRow, idx, encoder);
            }
        }
    }

    /**
     * Aggregate target fields when old and new schemas differ (schema evolution).
     *
     * <p>Slow path: requires column ID matching to find corresponding fields between schemas.
     */
    private static void aggregateTargetFieldsWithDifferentSchema(
            BinaryRow oldRow,
            BinaryRow newRow,
            AggregationContext oldContext,
            AggregationContext newContext,
            BitSet targetColumnIdBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        InternalRow.FieldGetter[] newFieldGetters = newContext.getFieldGetters();
        FieldAggregator[] newAggregators = newContext.getAggregators();
        List<Schema.Column> newColumns = newContext.getSchema().getColumns();

        for (int newIdx = 0; newIdx < newColumns.size(); newIdx++) {
            Schema.Column newColumn = newColumns.get(newIdx);
            int columnId = newColumn.getColumnId();

            // Find the corresponding field in old schema using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);

            if (targetColumnIdBitSet.get(columnId)) {
                // Target column: aggregate and encode
                InternalRow.FieldGetter oldFieldGetter =
                        (oldIdx != null) ? oldFieldGetters[oldIdx] : NULL_FIELD_GETTER;
                aggregateAndEncode(
                        oldFieldGetter,
                        newFieldGetters[newIdx],
                        oldRow,
                        newRow,
                        newAggregators[newIdx],
                        newIdx,
                        encoder);
            } else if (oldIdx == null) {
                // New column that doesn't exist in old schema: encode from newRow
                encoder.encodeField(newIdx, newFieldGetters[newIdx].getFieldOrNull(newRow));
            } else {
                // Non-target column that exists in old schema: encode old value
                copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, newIdx, encoder);
            }
        }
    }

    /**
     * Encode a partial delete when old and new schemas are identical.
     *
     * <p>Set target columns (except primary key) to null, keep other columns unchanged.
     *
     * <p>Fast path: field positions match directly, no column ID lookup needed.
     *
     * @param oldRow the old row to partially delete
     * @param context the aggregation context for encoding
     * @param targetPosBitSet BitSet marking target column positions
     * @param pkPosBitSet BitSet marking primary key positions
     * @param encoder the row encoder to encode results
     */
    public static void encodePartialDeleteWithSameSchema(
            BinaryRow oldRow,
            AggregationContext context,
            BitSet targetPosBitSet,
            BitSet pkPosBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] fieldGetters = context.getFieldGetters();

        for (int i = 0; i < oldRow.getFieldCount(); i++) {
            if (targetPosBitSet.get(i) && !pkPosBitSet.get(i)) {
                // Target column (not primary key): set to null
                encoder.encodeField(i, null);
            } else {
                // Non-target column or primary key: keep old value
                copyOldValueAndEncode(fieldGetters[i], oldRow, i, encoder);
            }
        }
    }

    /**
     * Encode a partial delete when old and new schemas differ (schema evolution).
     *
     * <p>Set target columns (except primary key) to null, keep other columns unchanged. For new
     * columns that don't exist in old schema, set to null.
     *
     * <p>Slow path: requires column ID matching to find corresponding fields between schemas.
     *
     * @param oldRow the old row to partially delete
     * @param oldContext context for the old schema
     * @param targetContext context for the target schema
     * @param targetColumnIdBitSet BitSet marking target columns by column ID
     * @param pkPosBitSet BitSet marking primary key positions in target schema
     * @param encoder the row encoder to encode results
     */
    public static void encodePartialDeleteWithDifferentSchema(
            BinaryRow oldRow,
            AggregationContext oldContext,
            AggregationContext targetContext,
            BitSet targetColumnIdBitSet,
            BitSet pkPosBitSet,
            RowEncoder encoder) {
        InternalRow.FieldGetter[] oldFieldGetters = oldContext.getFieldGetters();
        List<Schema.Column> targetColumns = targetContext.getSchema().getColumns();

        for (int targetIdx = 0; targetIdx < targetColumns.size(); targetIdx++) {
            Schema.Column targetColumn = targetColumns.get(targetIdx);
            int columnId = targetColumn.getColumnId();

            // Find corresponding field in old schema using column ID
            Integer oldIdx = oldContext.getFieldIndex(columnId);

            // Check if this is a target column using columnId (not position)
            boolean isTargetColumn = targetColumnIdBitSet.get(columnId);
            boolean isPrimaryKey = pkPosBitSet.get(targetIdx);

            if (isTargetColumn && !isPrimaryKey) {
                // Target column (not primary key): set to null
                encoder.encodeField(targetIdx, null);
            } else if (oldIdx != null) {
                // Column exists in old schema: copy value from old row
                copyOldValueAndEncode(oldFieldGetters[oldIdx], oldRow, targetIdx, encoder);
            } else {
                // New column that doesn't exist in old schema: set to null
                encoder.encodeField(targetIdx, null);
            }
        }
    }

    /**
     * Check if there are any non-null fields in non-target columns.
     *
     * @param row the row to check
     * @param targetPosBitSet BitSet marking target column positions
     * @return true if at least one non-target column has a non-null value
     */
    public static boolean hasNonTargetNonNullField(BinaryRow row, BitSet targetPosBitSet) {
        for (int pos = 0; pos < row.getFieldCount(); pos++) {
            if (!targetPosBitSet.get(pos) && !row.isNullAt(pos)) {
                return true;
            }
        }
        return false;
    }
}
