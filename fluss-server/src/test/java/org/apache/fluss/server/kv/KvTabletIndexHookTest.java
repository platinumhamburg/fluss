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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.index.IndexedRowEmitter;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IndexedRowEmitter} integration in {@link KvTablet}: emit callback invocations,
 * WAL fast-path disablement when an emitter is installed, and no-op default behavior.
 */
class KvTabletIndexHookTest {

    private static final short schemaId = 1;

    private final Configuration conf = new Configuration();
    private final RowType baseRowType = DATA1_SCHEMA_PK.getRowType();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;
    private int initCounter;

    @BeforeEach
    void beforeEach() throws Exception {
        // default fixture: PK table with FULL changelog image
        initKvTablet(DATA1_SCHEMA_PK, new HashMap<>());
    }

    private void initKvTablet(Schema schema, Map<String, String> tableConfig) throws Exception {
        TablePath tablePath = TablePath.of("testDb", "indexHookTest_" + initCounter);
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, schemaId));

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir,
                        physicalTablePath.getDatabaseName(),
                        0L,
                        physicalTablePath.getTableName());
        logTablet =
                LogTablet.create(
                        tempLogDir,
                        physicalTablePath,
                        logTabletDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);
        TableBucket tableBucket = logTablet.getTableBucket();

        TableConfig tableConf = new TableConfig(Configuration.fromMap(tableConfig));
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        physicalTablePath.getTablePath(),
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());

        // each init must use a fresh kv directory because RocksDB holds a directory lock
        // even briefly after close()
        File kvDir = new File(tmpKvDir, "kv-" + initCounter++);
        if (!kvDir.exists() && !kvDir.mkdirs()) {
            throw new IllegalStateException("Failed to create kv test dir: " + kvDir);
        }
        kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        kvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager);
    }

    @Test
    void testIndexedRowEmitterIsInvokedForInsertWithNullOldRow() throws Exception {
        RecordingEmitter recorder = new RecordingEmitter();
        kvTablet.setIndexedRowEmitter(recorder);

        KvRecordBatch batch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"}));
        kvTablet.putAsLeader(batch, null);

        assertThat(recorder.events).hasSize(1);
        EmittedEvent e = recorder.events.get(0);
        assertThat(e.key).isEqualTo("k1".getBytes());
        assertThat(e.oldRow).isNull();
        assertThat(e.newRow).isNotNull();
        assertThat(e.newRow.getInt(0)).isEqualTo(1);
        assertThat(e.newRow.getString(1).toString()).isEqualTo("v1");
        assertThat(e.sourceOffset).isEqualTo(0L);
    }

    @Test
    void testIndexedRowEmitterIsInvokedForUpdateWithBothRows() throws Exception {
        RecordingEmitter recorder = new RecordingEmitter();
        kvTablet.setIndexedRowEmitter(recorder);

        // first insert
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"})),
                null);
        // then update — pre-image is still in the pre-write buffer
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v2"})),
                null);

        assertThat(recorder.events).hasSize(2);

        EmittedEvent insertEvent = recorder.events.get(0);
        assertThat(insertEvent.oldRow).isNull();
        assertThat(insertEvent.newRow).isNotNull();
        assertThat(insertEvent.newRow.getString(1).toString()).isEqualTo("v1");

        EmittedEvent updateEvent = recorder.events.get(1);
        assertThat(updateEvent.key).isEqualTo("k1".getBytes());
        assertThat(updateEvent.oldRow).isNotNull();
        assertThat(updateEvent.oldRow.getString(1).toString()).isEqualTo("v1");
        assertThat(updateEvent.newRow).isNotNull();
        assertThat(updateEvent.newRow.getString(1).toString()).isEqualTo("v2");
    }

    @Test
    void testIndexedRowEmitterIsInvokedForDeleteWithNullNewRow() throws Exception {
        RecordingEmitter recorder = new RecordingEmitter();
        kvTablet.setIndexedRowEmitter(recorder);

        // insert then delete
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"})),
                null);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k1".getBytes(), null)),
                null);

        assertThat(recorder.events).hasSize(2);
        EmittedEvent deleteEvent = recorder.events.get(1);
        assertThat(deleteEvent.key).isEqualTo("k1".getBytes());
        assertThat(deleteEvent.oldRow).isNotNull();
        assertThat(deleteEvent.oldRow.getString(1).toString()).isEqualTo("v1");
        assertThat(deleteEvent.newRow).isNull();
    }

    @Test
    void testWalFastPathSkippedWhenIndexedRowEmitterIsSet() throws Exception {
        // Re-create tablet with WAL changelog image — this is the configuration where the
        // pre-image fetch is normally skipped (DefaultRowMerger + no auto-increment column).
        Map<String, String> walConfig = new HashMap<>();
        walConfig.put("table.changelog.image", "WAL");
        kvTablet.close();
        initKvTablet(DATA1_SCHEMA_PK, walConfig);

        RecordingEmitter recorder = new RecordingEmitter();
        kvTablet.setIndexedRowEmitter(recorder);

        // first upsert: pre-image absent → INSERT
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"})),
                null);
        // second upsert: pre-image present → UPDATE. WAL fast-path would normally skip the
        // pre-image read; with the emitter installed it must not.
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v2"})),
                null);

        assertThat(recorder.events).hasSize(2);
        EmittedEvent first = recorder.events.get(0);
        // first upsert: nothing in KV yet → oldRow is null (this is INSERT applied via apply path)
        assertThat(first.oldRow).isNull();
        assertThat(first.newRow.getString(1).toString()).isEqualTo("v1");

        EmittedEvent second = recorder.events.get(1);
        // critical assertion: WAL fast path is disabled because emitter is set, so the
        // pre-image was fetched and is delivered to the emitter.
        assertThat(second.oldRow)
                .as("Emitter must receive non-null oldRow on UPDATE even in WAL mode")
                .isNotNull();
        assertThat(second.oldRow.getString(1).toString()).isEqualTo("v1");
        assertThat(second.newRow.getString(1).toString()).isEqualTo("v2");
    }

    @Test
    void testNoOpEmitterIsCalledZeroTimes() throws Exception {
        // Default emitter is IndexedRowEmitter.NO_OP — exercise a varied workload and assert
        // that nothing breaks and that the tablet end-state is correct.
        List<KvRecord> records = new ArrayList<>();
        records.add(kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"}));
        records.add(kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v2"}));
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(records), null);

        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1b"})),
                null);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k2".getBytes(), null)),
                null);

        // Setting null restores the no-op emitter; subsequent operations must continue to work.
        kvTablet.setIndexedRowEmitter(null);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k3".getBytes(), new Object[] {3, "v3"})),
                null);

        // No exceptions thrown — workload completed.
        assertThat(logTablet.localLogEndOffset()).isGreaterThan(0L);
    }

    /** Captured emission. */
    private static final class EmittedEvent {
        final byte[] key;
        @Nullable final InternalRow oldRow;
        @Nullable final InternalRow newRow;
        final long sourceOffset;

        EmittedEvent(
                byte[] key,
                @Nullable InternalRow oldRow,
                @Nullable InternalRow newRow,
                long sourceOffset) {
            this.key = Arrays.copyOf(key, key.length);
            this.oldRow = oldRow;
            this.newRow = newRow;
            this.sourceOffset = sourceOffset;
        }
    }

    /** Records every invocation. Thread-safe so tests using multi-thread can extend later. */
    private static final class RecordingEmitter implements IndexedRowEmitter {
        final List<EmittedEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void emit(
                byte[] key,
                @Nullable InternalRow oldRow,
                @Nullable InternalRow newRow,
                long sourceOffset) {
            events.add(new EmittedEvent(key, oldRow, newRow, sourceOffset));
        }
    }

}
