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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link UpsertSinkWriter} undo recovery mechanism. */
public class UpsertSinkWriterUndoRecoveryITCase extends FlinkTestBase {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .column("value", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(2, "id").build();

    private static final String TEST_TABLE_NAME = "test_undo_recovery";
    private TablePath tablePath;
    private org.apache.flink.table.types.logical.RowType flinkRowType;

    @BeforeEach
    public void setup() throws Exception {
        tablePath = TablePath.of(DEFAULT_DB, TEST_TABLE_NAME);
        createTable(tablePath, TABLE_DESCRIPTOR);

        flinkRowType =
                org.apache.flink.table.types.logical.RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), new VarCharType(Integer.MAX_VALUE), new IntType()
                        },
                        new String[] {"id", "name", "value"});
    }

    @Test
    public void testUndoRecoveryAfterCheckpoint() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create and initialize the first writer instance
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        // Initialize state (no previous state)
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write some data
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(
                createRowData(3L, "Charlie", 300, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Simulate checkpoint 1
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);

        // Get the checkpointed state
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);
        assertThat(checkpoint1State).isNotEmpty();

        // Write more data after checkpoint (these should be undone)
        writer1.write(
                createRowData(1L, "Alice-Updated", 150, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writer1.write(createRowData(4L, "David", 400, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Close the first writer
        writer1.close();

        // Verify data before recovery (includes post-checkpoint writes)
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(4);

        // Simulate recovery: create a new writer with checkpointed state
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        // Initialize with recovered state
        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify data after recovery (post-checkpoint writes should be undone)
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(3);

        // Verify the data is correct (reverted to checkpoint state)
        Map<Long, InternalRow> dataMap = new HashMap<>();
        for (InternalRow row : dataAfterRecovery) {
            dataMap.put(row.getLong(0), row);
        }

        assertThat(dataMap).containsKey(1L);
        assertThat(dataMap.get(1L).getString(1).toString()).isEqualTo("Alice");
        assertThat(dataMap.get(1L).getInt(2)).isEqualTo(100);

        assertThat(dataMap).containsKey(2L);
        assertThat(dataMap.get(2L).getString(1).toString()).isEqualTo("Bob");

        assertThat(dataMap).containsKey(3L);
        assertThat(dataMap.get(3L).getString(1).toString()).isEqualTo("Charlie");

        // ID 4 should not exist (was inserted after checkpoint)
        assertThat(dataMap).doesNotContainKey(4L);

        writer2.close();
    }

    @Test
    public void testNoUndoForFreshStart() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create writer with no previous state
        UpsertSinkWriter<RowData> writer =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx = new TestFunctionInitializationContext();
        writer.initializeState(initCtx);
        writer.initialize(initContext.metricGroup());

        // Write some data
        writer.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer.flush(false);

        // Verify data is written
        List<InternalRow> data = scanAllData();
        assertThat(data).hasSize(2);

        writer.close();
    }

    @Test
    public void testUndoWithDeleteOperations() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create and initialize writer
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write initial data
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(
                createRowData(3L, "Charlie", 300, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Checkpoint
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);

        // Delete a record after checkpoint
        writer1.write(createRowData(2L, "Bob", 200, RowKind.DELETE), new MockSinkWriterContext());
        writer1.flush(false);

        writer1.close();

        // Verify Bob is deleted
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(2);

        // Recover from checkpoint
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify Bob is restored (delete was undone)
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(3);

        writer2.close();
    }

    private UpsertSinkWriter<RowData> createUpsertSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            int subtaskIndex,
            int parallelism)
            throws Exception {
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, true);
        serializationSchema.open(new SerializerInitContextImpl(toFlussRowType(flinkRowType)));

        return new UpsertSinkWriter<>(
                tablePath,
                flussConfig,
                flinkRowType,
                null, // no partial update
                mailboxExecutor,
                serializationSchema,
                subtaskIndex,
                parallelism,
                "test-lock-owner-" + System.currentTimeMillis());
    }

    private RowData createRowData(long id, String name, int value, RowKind rowKind) {
        GenericRowData rowData = GenericRowData.of(id, StringData.fromString(name), value);
        rowData.setRowKind(rowKind);
        return rowData;
    }

    private List<InternalRow> scanAllData() throws Exception {
        Table table = conn.getTable(tablePath);
        List<InternalRow> results = new ArrayList<>();

        // For primary key table, we need to scan the KV state, not the log
        // We use BatchScanner to read current data from each bucket
        int numBuckets = TABLE_DESCRIPTOR.getTableDistribution().get().getBucketCount().get();
        long tableId = table.getTableInfo().getTableId();

        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, null, bucketId);
            try (org.apache.fluss.client.table.scanner.batch.BatchScanner batchScanner =
                    table.newScan().createBatchScanner(tableBucket)) {
                org.apache.fluss.utils.CloseableIterator<InternalRow> iterator;
                while ((iterator = batchScanner.pollBatch(java.time.Duration.ofSeconds(1)))
                        != null) {
                    try {
                        while (iterator.hasNext()) {
                            results.add(iterator.next());
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }
        }

        return results;
    }

    private Map<TableBucket, Long> getStateFromWriter(TestFunctionInitializationContext context) {
        List<WriterState> states = context.getWriterStates();
        Map<TableBucket, Long> result = new HashMap<>();
        for (WriterState state : states) {
            result.putAll(state.getBucketOffsets());
        }
        return result;
    }

    static class MockSinkWriterContext extends FlinkSinkWriterTest.MockSinkWriterContext {}

    static class TestFunctionInitializationContext implements FunctionInitializationContext {
        private final Map<TableBucket, Long> restoredState;
        private final long checkpointId;
        private final List<WriterState> writerStates;

        TestFunctionInitializationContext() {
            this(new HashMap<>(), -1L);
        }

        TestFunctionInitializationContext(Map<TableBucket, Long> restoredState, long checkpointId) {
            this.restoredState = restoredState;
            this.checkpointId = checkpointId;
            this.writerStates = new ArrayList<>();
            if (!restoredState.isEmpty()) {
                writerStates.add(new WriterState(checkpointId, restoredState));
            }
        }

        @Override
        public boolean isRestored() {
            return !restoredState.isEmpty();
        }

        @Override
        public org.apache.flink.api.common.state.OperatorStateStore getOperatorStateStore() {
            return new TestOperatorStateStore(writerStates);
        }

        @Override
        public org.apache.flink.api.common.state.KeyedStateStore getKeyedStateStore() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.OptionalLong getRestoredCheckpointId() {
            return checkpointId >= 0
                    ? java.util.OptionalLong.of(checkpointId)
                    : java.util.OptionalLong.empty();
        }

        public List<WriterState> getWriterStates() {
            return writerStates;
        }
    }

    static class TestFunctionSnapshotContext implements FunctionSnapshotContext {
        private final long checkpointId;

        TestFunctionSnapshotContext(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        @Override
        public long getCheckpointId() {
            return checkpointId;
        }

        @Override
        public long getCheckpointTimestamp() {
            return System.currentTimeMillis();
        }
    }

    static class TestOperatorStateStore
            implements org.apache.flink.api.common.state.OperatorStateStore {
        private final List<WriterState> writerStates;

        TestOperatorStateStore(List<WriterState> writerStates) {
            this.writerStates = writerStates;
        }

        @Override
        public <S> org.apache.flink.api.common.state.ListState<S> getUnionListState(
                org.apache.flink.api.common.state.ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            return new TestUnionListState<>((List<S>) writerStates);
        }

        @Override
        public <S> org.apache.flink.api.common.state.ListState<S> getListState(
                org.apache.flink.api.common.state.ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public <K, V> org.apache.flink.api.common.state.BroadcastState<K, V> getBroadcastState(
                org.apache.flink.api.common.state.MapStateDescriptor<K, V> stateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.Set<String> getRegisteredStateNames() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.Set<String> getRegisteredBroadcastStateNames() {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    static class TestUnionListState<S> implements org.apache.flink.api.common.state.ListState<S> {
        private final List<S> states;

        TestUnionListState(List<S> states) {
            this.states = states;
        }

        @Override
        public void update(List<S> values) throws Exception {
            states.clear();
            states.addAll(values);
        }

        @Override
        public void addAll(List<S> values) throws Exception {
            states.addAll(values);
        }

        @Override
        public Iterable<S> get() throws Exception {
            return states;
        }

        @Override
        public void add(S value) throws Exception {
            states.add(value);
        }

        @Override
        public void clear() {
            states.clear();
        }
    }
}
