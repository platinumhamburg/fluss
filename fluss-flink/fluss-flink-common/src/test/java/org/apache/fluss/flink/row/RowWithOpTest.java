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

package org.apache.fluss.flink.row;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RowWithOp}. */
public class RowWithOpTest {

    @Test
    void testConstructor_withValidInputs() {
        InternalRow row = new GenericRow(2);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp.getRow()).isSameAs(row);
        assertThat(rowWithOp.getOperationType()).isEqualTo(OperationType.APPEND);
    }

    @Test
    void testConstructor_withNullRow_shouldThrowException() {
        assertThatThrownBy(() -> new RowWithOp(null, OperationType.APPEND))
                .isInstanceOf(NullPointerException.class);

        InternalRow row = new GenericRow(1);
        assertThatThrownBy(() -> new RowWithOp(row, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEquals_withSameInstance() {
        InternalRow row = new GenericRow(1);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp).isEqualTo(rowWithOp);
    }

    @Test
    void testEquals_withEqualObjects() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1).isEqualTo(rowWithOp2);
        assertThat(rowWithOp2).isEqualTo(rowWithOp1);
    }

    @Test
    void testEquals_withDifferentRows() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(2); // Different field count to ensure they're different

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1).isNotEqualTo(rowWithOp2);
    }

    @Test
    void testEquals_withDifferentOpTypes() {
        InternalRow row = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row, OperationType.DELETE);

        assertThat(rowWithOp1).isNotEqualTo(rowWithOp2);
    }

    @Test
    void testEquals_withNull() {
        InternalRow row = new GenericRow(1);
        RowWithOp rowWithOp = new RowWithOp(row, OperationType.APPEND);

        assertThat(rowWithOp).isNotEqualTo(null);
    }

    @Test
    void testHashCode_withEqualObjects() {
        GenericRow row1 = new GenericRow(1);
        GenericRow row2 = new GenericRow(1);

        RowWithOp rowWithOp1 = new RowWithOp(row1, OperationType.APPEND);
        RowWithOp rowWithOp2 = new RowWithOp(row2, OperationType.APPEND);

        assertThat(rowWithOp1.hashCode()).isEqualTo(rowWithOp2.hashCode());
    }

    // --- RowWithOp equality/hashCode with RETRACT ---

    @Test
    void testRowWithOpEqualityWithRetract() {
        GenericRow row1 = new GenericRow(2);
        GenericRow row2 = new GenericRow(2);
        RowWithOp a = new RowWithOp(row1, OperationType.RETRACT);
        RowWithOp b = new RowWithOp(row2, OperationType.RETRACT);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testRowWithOpRetractNotEqualToDelete() {
        InternalRow row = new GenericRow(2);
        RowWithOp retractOp = new RowWithOp(row, OperationType.RETRACT);
        RowWithOp deleteOp = new RowWithOp(row, OperationType.DELETE);
        assertThat(retractOp).isNotEqualTo(deleteOp);
    }

    // --- RowDataSerializationSchema: retract-compatible mapping ---

    @Test
    void testUpdateBeforeMapsToRetractWhenRetractCompatible() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, true, true);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.RETRACT);
    }

    @Test
    void testDeleteStillMapsToDeleteWhenRetractCompatible() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, true, true);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.DELETE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void testUpdateBeforeMapsToDeleteWhenNotRetractCompatible() throws Exception {
        // Non-aggregation table: isAggregationTable=false, schemaSupportsRetract=false
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, false, false);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void testUpdateBeforeFailsFastForAggregationTableWithoutRetract() throws Exception {
        // Aggregation table but schema does NOT support retract: should fail fast
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, true, false);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        assertThatThrownBy(() -> schema.serialize(rowData))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support retract");
    }

    @Test
    void testExistingTwoArgConstructorPreservesDeleteBehavior() throws Exception {
        // The two-arg constructor should default isAggregationTable=false
        RowDataSerializationSchema schema = new RowDataSerializationSchema(false, false);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void testNonAggregationTableKeepsCurrentBehavior() throws Exception {
        // Non-aggregation: isAggregationTable=false, schemaSupportsRetract=false
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, false, false);
        schema.open(new TestInitializationContext());

        // INSERT -> UPSERT
        GenericRowData insert = new GenericRowData(2);
        insert.setField(0, 1);
        insert.setField(1, 2);
        insert.setRowKind(RowKind.INSERT);
        assertThat(schema.serialize(insert).getOperationType()).isEqualTo(OperationType.UPSERT);

        // UPDATE_AFTER -> UPSERT
        GenericRowData updateAfter = new GenericRowData(2);
        updateAfter.setField(0, 1);
        updateAfter.setField(1, 2);
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        assertThat(schema.serialize(updateAfter).getOperationType())
                .isEqualTo(OperationType.UPSERT);

        // DELETE -> DELETE
        GenericRowData delete = new GenericRowData(2);
        delete.setField(0, 1);
        delete.setField(1, 2);
        delete.setRowKind(RowKind.DELETE);
        assertThat(schema.serialize(delete).getOperationType()).isEqualTo(OperationType.DELETE);

        // UPDATE_BEFORE -> DELETE (not RETRACT)
        GenericRowData updateBefore = new GenericRowData(2);
        updateBefore.setField(0, 1);
        updateBefore.setField(1, 2);
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        assertThat(schema.serialize(updateBefore).getOperationType())
                .isEqualTo(OperationType.DELETE);
    }

    @Test
    void testInsertAndUpdateAfterStillMapToUpsertWhenAggregationTable() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(false, false, true, true);
        schema.open(new TestInitializationContext());

        // INSERT -> UPSERT even when isAggregationTable=true
        GenericRowData insert = new GenericRowData(2);
        insert.setField(0, 1);
        insert.setField(1, 2);
        insert.setRowKind(RowKind.INSERT);
        assertThat(schema.serialize(insert).getOperationType()).isEqualTo(OperationType.UPSERT);

        // UPDATE_AFTER -> UPSERT even when isAggregationTable=true
        GenericRowData updateAfter = new GenericRowData(2);
        updateAfter.setField(0, 1);
        updateAfter.setField(1, 2);
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        assertThat(schema.serialize(updateAfter).getOperationType())
                .isEqualTo(OperationType.UPSERT);
    }

    @Test
    void testIgnoreDeleteOverridesAggregationTable() throws Exception {
        // When ignoreDelete=true, UPDATE_BEFORE should be IGNORE even if aggregation table
        RowDataSerializationSchema schema = new RowDataSerializationSchema(false, true, true, true);
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.IGNORE);
    }

    // --- FlinkTableSink.getChangelogMode() tests ---

    @Test
    void testGetChangelogModeIncludesUpdateBeforeForRetractCapableAggregationTable() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, /* schemaSupportsRetract= */ true);
        ChangelogMode requestedMode = allKindsChangelogMode();

        ChangelogMode result = sink.getChangelogMode(requestedMode);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isTrue();
    }

    @Test
    void testGetChangelogModeExcludesUpdateBeforeForNonRetractAggregationTable() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, /* schemaSupportsRetract= */ false);
        ChangelogMode requestedMode = allKindsChangelogMode();

        assertThatThrownBy(() -> sink.getChangelogMode(requestedMode))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("cannot consume UPDATE changelog safely");
    }

    @Test
    void testGetChangelogModeExcludesUpdateBeforeForNonAggregationTable() {
        FlinkTableSink sink = createPkTableSink(null, /* schemaSupportsRetract= */ false);
        ChangelogMode requestedMode = allKindsChangelogMode();

        ChangelogMode result = sink.getChangelogMode(requestedMode);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
    }

    @Test
    void testGetChangelogModeAdmitsUpdateBeforeWhenSinkIgnoreDelete() {
        // sink.ignore-delete must not bypass aggregation update safety checks.
        FlinkTableSink sink =
                createPkTableSink(
                        MergeEngineType.AGGREGATION,
                        /* schemaSupportsRetract= */ false,
                        /* sinkIgnoreDelete= */ true);
        ChangelogMode requestedMode = allKindsChangelogMode();

        assertThatThrownBy(() -> sink.getChangelogMode(requestedMode))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("cannot consume UPDATE changelog safely");
    }

    /** Creates a PK table FlinkTableSink for changelog mode testing. */
    private static FlinkTableSink createPkTableSink(
            MergeEngineType mergeEngineType, boolean schemaSupportsRetract) {
        return createPkTableSink(mergeEngineType, schemaSupportsRetract, false);
    }

    private static FlinkTableSink createPkTableSink(
            MergeEngineType mergeEngineType,
            boolean schemaSupportsRetract,
            boolean sinkIgnoreDelete) {
        return new FlinkTableSink(
                TablePath.of("test_db", "test_table"),
                new Configuration(),
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new org.apache.flink.table.types.logical.IntType(),
                            new org.apache.flink.table.types.logical.IntType()
                        },
                        new String[] {"id", "value"}),
                new int[] {0}, // primary key on first column
                Collections.emptyList(),
                true, // streaming
                mergeEngineType,
                null, // lakeFormat
                sinkIgnoreDelete,
                DeleteBehavior.IGNORE,
                1, // numBucket
                new ArrayList<>(),
                DistributionMode.BUCKET,
                null, // producerId
                schemaSupportsRetract ? Collections.emptySet() : Collections.singleton("value"));
    }

    /** Builds a ChangelogMode containing all four RowKinds. */
    private static ChangelogMode allKindsChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    /**
     * Minimal InitializationContext for unit tests that don't need schema evolution or statistics.
     */
    private static class TestInitializationContext
            implements FlussSerializationSchema.InitializationContext {

        private static final RowType FLUSS_ROW_TYPE =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {new IntType(), new IntType()},
                        new String[] {"a", "b"});

        private static final org.apache.flink.table.types.logical.RowType FLINK_ROW_TYPE =
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.IntType(),
                        new org.apache.flink.table.types.logical.IntType());

        @Override
        public RowType getRowSchema() {
            return FLUSS_ROW_TYPE;
        }

        @Override
        public org.apache.flink.table.types.logical.RowType getInputRowSchema() {
            return FLINK_ROW_TYPE;
        }

        @Override
        public boolean isStatisticEnabled() {
            return false;
        }
    }
}
