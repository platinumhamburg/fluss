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
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ProjectionPushdownCache;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.KvEntry;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Value;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Test for {@link KvTablet}. */
class KvTabletTest {

    private static final short schemaId = 1;
    private final Configuration conf = new Configuration();
    private final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;
    private ExecutorService executor;

    @BeforeEach
    void beforeEach() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void afterEach() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void initLogTabletAndKvTablet(Schema schema, Map<String, String> tableConfig)
            throws Exception {
        initLogTabletAndKvTablet(TablePath.of("testDb", "t1"), schema, tableConfig);
    }

    private void initLogTabletAndKvTablet(
            TablePath tablePath, Schema schema, Map<String, String> tableConfig) throws Exception {
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, schemaId));
        logTablet = createLogTablet(tempLogDir, 0L, physicalTablePath);
        TableBucket tableBucket = logTablet.getTableBucket();
        kvTablet =
                createKvTablet(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        schemaGetter,
                        tableConfig);
    }

    private LogTablet createLogTablet(File tempLogDir, long tableId, PhysicalTablePath tablePath)
            throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, tablePath.getDatabaseName(), tableId, tablePath.getTableName());
        return LogTablet.create(
                tablePath,
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
    }

    private KvTablet createKvTablet(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File tmpKvDir,
            SchemaGetter schemaGetter,
            Map<String, String> tableConfig)
            throws Exception {
        RowMerger rowMerger =
                RowMerger.create(
                        new TableConfig(Configuration.fromMap(tableConfig)), KvFormat.COMPACTED);
        return KvTablet.create(
                tablePath,
                tableBucket,
                logTablet,
                tmpKvDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                new RootAllocator(Long.MAX_VALUE),
                new TestingMemorySegmentPool(10 * 1024),
                KvFormat.COMPACTED,
                rowMerger,
                DEFAULT_COMPRESSION,
                schemaGetter,
                null,
                -1L,
                SystemClock.getInstance());
    }

    @Test
    void testIndexTableRejectsDirectClientWrite() throws Exception {
        // Create an index table (identified by naming convention)
        String indexTableName = "__test_table__index__name_idx";
        TablePath indexTablePath = TablePath.of("testDb", indexTableName);
        PhysicalTablePath physicalIndexTablePath = PhysicalTablePath.of(indexTablePath);

        final Schema schema1 = DATA1_SCHEMA_PK;
        LogTablet indexLogTablet = createLogTablet(tempLogDir, 0L, physicalIndexTablePath);
        TableBucket indexTableBucket = indexLogTablet.getTableBucket();

        RowMerger rowMerger =
                RowMerger.create(new TableConfig(new Configuration()), KvFormat.COMPACTED);
        SchemaGetter schemaGetter = new TestingSchemaGetter(1, schema1);
        KvTablet indexKvTablet =
                KvTablet.create(
                        physicalIndexTablePath,
                        indexTableBucket,
                        indexLogTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        null);

        // Try to write to index table should be rejected
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1"})));
        assertThatThrownBy(() -> indexKvTablet.putAsLeader(kvRecordBatch, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Index table")
                .hasMessageContaining("does not support direct client writes");

        indexKvTablet.close();
    }

    @Test
    void testInvalidPartialUpdate1() throws Exception {
        final Schema schema1 = DATA2_SCHEMA;
        initLogTabletAndKvTablet(schema1, new HashMap<>());
        KvRecordTestUtils.KvRecordFactory data2kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema1.getRowType());
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, null})));
        // target columns don't contain primary key columns
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {1, 2}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "The target write columns [b, c] must contain the primary key columns [a].");
    }

    @Test
    void testInvalidPartialUpdate2() throws Exception {
        // the column not in target columns is not null
        final Schema schema2 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", new StringType(false))
                        .primaryKey("a")
                        .build();
        initLogTabletAndKvTablet(schema2, new HashMap<>());
        KvRecordTestUtils.KvRecordFactory data2kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema2.getRowType());
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, "str"})));
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {0, 1}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch, new int[] {0, 2}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessage(
                        "Partial Update requires all columns except primary key to be nullable, but column c is NOT NULL.");
    }

    @Test
    void testPartialUpdateAndDelete() throws Exception {
        initLogTabletAndKvTablet(DATA2_SCHEMA, new HashMap<>());
        RowType rowType = DATA2_SCHEMA.getRowType();
        KvRecordTestUtils.KvRecordFactory data2kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(rowType);

        // create one kv batch with only writing first one column
        KvRecordBatch kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        data2kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, null, null}),
                        data2kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, null, null}),
                        data2kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, null, null}));

        int[] targetColumns = new int[] {0};

        kvTablet.putAsLeader(kvRecordBatch, targetColumns);
        long endOffset = logTablet.localLogEndOffset();

        // expected cdc log records for putting order: batch1, batch2;
        List<MemoryLogRecords> expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                0,
                                Arrays.asList(
                                        // -- for batch 1
                                        ChangeType.INSERT,
                                        ChangeType.INSERT,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, null, null},
                                        // for k2: +I
                                        new Object[] {2, null, null},
                                        // for k2: -U, +U
                                        new Object[] {2, null, null},
                                        new Object[] {2, null, null})));

        LogRecords actualLogRecords = readLogRecords();

        checkEqual(actualLogRecords, expectedLogs, rowType);

        // create one kv batch with only writing second one column
        KvRecordBatch kvRecordBatch2 =
                kvRecordBatchFactory.ofRecords(
                        data2kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, "v11", null}),
                        data2kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, "v21", null}),
                        data2kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, "v23", null}));

        targetColumns = new int[] {0, 1};
        kvTablet.putAsLeader(kvRecordBatch2, targetColumns);

        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        // -- for k2
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, null, null},
                                        new Object[] {1, "v11", null},
                                        // for k2: -U, +U
                                        new Object[] {2, null, null},
                                        new Object[] {2, "v21", null},
                                        // for k2: -U, +U
                                        new Object[] {2, "v21", null},
                                        new Object[] {2, "v23", null})));
        actualLogRecords = readLogRecords(endOffset);
        checkEqual(actualLogRecords, expectedLogs, rowType);
        endOffset = logTablet.localLogEndOffset();

        // now, not use partial update, update all columns, it should still work
        KvRecordBatch kvRecordBatch3 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                data2kvRecordFactory.ofRecord(
                                        "k1".getBytes(), new Object[] {1, null, "c3"}),
                                data2kvRecordFactory.ofRecord(
                                        "k3".getBytes(), new Object[] {3, null, "v211"})));

        targetColumns = new int[] {0, 2};
        kvTablet.putAsLeader(kvRecordBatch3, targetColumns);

        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        // -- for k3
                                        ChangeType.INSERT),
                                Arrays.asList(
                                        // for k1
                                        new Object[] {1, "v11", null},
                                        new Object[] {1, "v11", "c3"},
                                        // for k3: +I
                                        new Object[] {3, null, "v211"})));
        actualLogRecords = readLogRecords(endOffset);

        checkEqual(actualLogRecords, expectedLogs, rowType);

        endOffset = logTablet.localLogEndOffset();

        // now, partial delete "k1", delete column 1
        targetColumns = new int[] {0, 1};
        kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord("k1".getBytes(), null)));
        kvTablet.putAsLeader(kvRecordBatch, targetColumns);

        // check cdc log, should produce -U, +U
        actualLogRecords = readLogRecords(endOffset);
        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        // -- for k1
                                        ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        new Object[] {1, "v11", "c3"},
                                        new Object[] {1, null, "c3"})));
        checkEqual(actualLogRecords, expectedLogs, rowType);
        // we can still get "k1" from pre-write buffer
        assertThat(kvTablet.getKvPreWriteBuffer().get(Key.of("k1".getBytes())))
                .isEqualTo(valueOf(compactedRow(rowType, new Object[] {1, null, "c3"})));

        endOffset = logTablet.localLogEndOffset();
        // now, partial delete "k1", delete column 2, should produce -D
        targetColumns = new int[] {0, 2};
        kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        Collections.singletonList(
                                data2kvRecordFactory.ofRecord("k1".getBytes(), null)));

        kvTablet.putAsLeader(kvRecordBatch, targetColumns);
        // check cdc log, should produce -D
        actualLogRecords = readLogRecords(endOffset);
        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Collections.singletonList(
                                        // -- for k1
                                        ChangeType.DELETE),
                                Collections.singletonList(new Object[] {1, null, "c3"})));
        checkEqual(actualLogRecords, expectedLogs, rowType);
        // now we can not get "k1" from pre-write buffer
        assertThat(kvTablet.getKvPreWriteBuffer().get(Key.of("k1".getBytes()))).isNotNull();

        // Test schema evolution.
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(DATA1_SCHEMA, 2));
        rowType = DATA1_SCHEMA.getRowType();

        kvRecordBatch =
                kvRecordBatchFactory.ofRecords(
                        data2kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, null, "c3"}),
                        data2kvRecordFactory.ofRecord("k2".getBytes(), null),
                        data2kvRecordFactory.ofRecord(
                                "k3".getBytes(), new Object[] {3, "31", "c4"}));
        endOffset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvRecordBatch, new int[] {0, 1, 2});
        // check cdc log, should produce +I and -D.
        actualLogRecords = readLogRecords(endOffset);
        expectedLogs =
                Collections.singletonList(
                        logRecords(
                                rowType,
                                endOffset,
                                Arrays.asList(
                                        ChangeType.INSERT,
                                        ChangeType.DELETE,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        new Object[] {1, null},
                                        new Object[] {2, "v23"},
                                        new Object[] {3, null},
                                        new Object[] {3, "31"}),
                                (short) 2));
        checkEqual(actualLogRecords, expectedLogs, rowType);
    }

    @Test
    void testPutWithMultiThread() throws Exception {
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, new HashMap<>());
        // create two kv batches
        KvRecordBatch kvRecordBatch1 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                                kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                                kvRecordFactory.ofRecord(
                                        "k2".getBytes(), new Object[] {2, "v23"})));

        KvRecordBatch kvRecordBatch2 =
                kvRecordBatchFactory.ofRecords(
                        Arrays.asList(
                                kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v22"}),
                                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v21"}),
                                kvRecordFactory.ofRecord("k1".getBytes(), null)));

        // expected cdc log records for putting order: batch1, batch2;
        List<MemoryLogRecords> expectedLogs1 =
                Arrays.asList(
                        logRecords(
                                0L,
                                Arrays.asList(
                                        // -- for batch 1
                                        ChangeType.INSERT,
                                        ChangeType.INSERT,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        // --- cdc log for batch 1
                                        // for k1
                                        new Object[] {1, "v11"},
                                        // for k2: +I
                                        new Object[] {2, "v21"},
                                        // for k2: -U, +U
                                        new Object[] {2, "v21"},
                                        new Object[] {2, "v23"})),
                        logRecords(
                                4L,
                                Arrays.asList(
                                        // -- for batch2
                                        // for k2
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        // for k1
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        ChangeType.DELETE),
                                Arrays.asList(
                                        // -- cdc log for batch 2
                                        // for k2: -U, +U
                                        new Object[] {2, "v23"},
                                        new Object[] {2, "v22"},
                                        // for k1:  -U, +U
                                        new Object[] {1, "v11"},
                                        new Object[] {1, "v21"},
                                        // for k1: -D
                                        new Object[] {1, "v21"})));
        // expected log last offset for each putting with putting order: batch1, batch2
        List<Long> expectLogLastOffset1 = Arrays.asList(3L, 8L);

        // expected cdc log records for putting order: batch2, batch1;
        List<MemoryLogRecords> expectedLogs2 =
                Arrays.asList(
                        logRecords(
                                0L,
                                Arrays.asList(
                                        ChangeType.INSERT, ChangeType.INSERT, ChangeType.DELETE),
                                Arrays.asList(
                                        // --- cdc log for batch 2
                                        // for k2
                                        new Object[] {2, "v22"},
                                        // for k1
                                        new Object[] {1, "v21"},
                                        new Object[] {1, "v21"})),
                        logRecords(
                                3L,
                                Arrays.asList(
                                        ChangeType.INSERT,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        // -- cdc log for batch 1
                                        // for k1
                                        new Object[] {1, "v11"},
                                        // for k2, -U, +U
                                        new Object[] {2, "v22"},
                                        new Object[] {2, "v21"},
                                        // for k2, -U, +U
                                        new Object[] {2, "v21"},
                                        new Object[] {2, "v23"})));
        // expected log last offset for each putting with putting order: batch2, batch1
        List<Long> expectLogLastOffset2 = Arrays.asList(2L, 7L);

        // start two thread to put records, one to put batch1, one to put batch2;
        // each thread will sleep for a random time;
        // so, the putting order can be batch1, batch2; or batch2, batch1
        Random random = new Random();
        List<Future<LogAppendInfo>> putFutures = new ArrayList<>();
        putFutures.add(
                executor.submit(
                        () -> {
                            Thread.sleep(random.nextInt(100));
                            return kvTablet.putAsLeader(kvRecordBatch1, null);
                        }));
        putFutures.add(
                executor.submit(
                        () -> {
                            Thread.sleep(random.nextInt(100));
                            return kvTablet.putAsLeader(kvRecordBatch2, null);
                        }));

        List<Long> actualLogEndOffset = new ArrayList<>();
        for (Future<LogAppendInfo> future : putFutures) {
            actualLogEndOffset.add(future.get().lastOffset());
        }
        Collections.sort(actualLogEndOffset);

        LogRecords actualLogRecords = readLogRecords();
        // with putting order: batch1, batch2
        if (actualLogEndOffset.equals(expectLogLastOffset1)) {
            checkEqual(actualLogRecords, expectedLogs1);
        } else if (actualLogEndOffset.equals(expectLogLastOffset2)) {
            // with putting order: batch2, batch1
            checkEqual(actualLogRecords, expectedLogs2);
        } else {
            fail(
                    "The putting order is not batch1, batch2; or batch2, batch1. The actual log end offset is: "
                            + actualLogEndOffset
                            + ". The expected log end offset is: "
                            + expectLogLastOffset1
                            + " or "
                            + expectLogLastOffset2);
        }
    }

    @Test
    void testPutAsLeaderWithOutOfOrderSequenceException() throws Exception {
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, new HashMap<>());
        long writeId = 100L;
        List<KvRecord> kvData1 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v23"}));
        KvRecordBatch kvRecordBatch1 = kvRecordBatchFactory.ofRecords(kvData1, writeId, 0);

        List<KvRecord> kvData2 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v22"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v21"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), null));
        KvRecordBatch kvRecordBatch2 = kvRecordBatchFactory.ofRecords(kvData2, writeId, 3);
        kvTablet.putAsLeader(kvRecordBatch1, null);

        KvEntry kv1 =
                KvEntry.of(
                        Key.of("k1".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {1, "v11"})),
                        0);
        KvEntry kv2 =
                KvEntry.of(
                        Key.of("k2".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {2, "v21"})),
                        1);
        KvEntry kv3 =
                KvEntry.of(
                        Key.of("k2".getBytes()),
                        valueOf(compactedRow(baseRowType, new Object[] {2, "v23"})),
                        3,
                        kv2);
        List<KvEntry> expectedEntries = Arrays.asList(kv1, kv2, kv3);
        Map<Key, KvEntry> expectedMap = new HashMap<>();
        for (KvEntry kvEntry : expectedEntries) {
            expectedMap.put(kvEntry.getKey(), kvEntry);
        }
        assertThat(kvTablet.getKvPreWriteBuffer().getAllKvEntries()).isEqualTo(expectedEntries);
        assertThat(kvTablet.getKvPreWriteBuffer().getKvEntryMap()).isEqualTo(expectedMap);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);

        // the second batch will be ignored.
        assertThatThrownBy(() -> kvTablet.putAsLeader(kvRecordBatch2, null))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 100 at offset 8 in table-bucket "
                                + "TableBucket{tableId=0, bucket=587113} : 3 (incoming batch seq.), 0 (current batch seq.)");
        assertThat(kvTablet.getKvPreWriteBuffer().getAllKvEntries()).isEqualTo(expectedEntries);
        assertThat(kvTablet.getKvPreWriteBuffer().getKvEntryMap()).isEqualTo(expectedMap);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFirstRowMergeEngine(boolean doProjection) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.merge-engine", "first_row");
        String tableName =
                "test_first_row_merge_engine_" + (doProjection ? "projection" : "no_projection");
        TablePath tablePath = TablePath.of("testDb", tableName);
        initLogTabletAndKvTablet(tablePath, DATA1_SCHEMA_PK, config);
        RowType rowType = DATA1_SCHEMA_PK.getRowType();
        ProjectionPushdownCache projectionCache = new ProjectionPushdownCache();
        FileLogProjection logProjection = null;
        if (doProjection) {
            logProjection = new FileLogProjection(projectionCache);
            logProjection.setCurrentProjection(
                    0L, schemaGetter, DEFAULT_COMPRESSION, new int[] {0});
        }

        RowType readLogRowType = doProjection ? rowType.project(new int[] {0}) : rowType;

        List<KvRecord> kvData1 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v23"}));
        KvRecordBatch kvRecordBatch1 = kvRecordBatchFactory.ofRecords(kvData1);
        kvTablet.putAsLeader(kvRecordBatch1, null);
        long endOffset = logTablet.localLogEndOffset();
        LogRecords actualLogRecords = readLogRecords(logTablet, 0L, logProjection);
        MemoryLogRecords expectedLogs =
                logRecords(
                        readLogRowType,
                        0,
                        Arrays.asList(ChangeType.INSERT, ChangeType.INSERT),
                        doProjection
                                ? Arrays.asList(new Object[] {1}, new Object[] {2})
                                : Arrays.asList(new Object[] {1, "v11"}, new Object[] {2, "v21"}));
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // append some new batches which will not generate only cdc logs, but the endOffset will be
        // updated.
        int emptyBatchCount = 3;
        long offsetBefore = endOffset;
        for (int i = 0; i < emptyBatchCount; i++) {
            List<KvRecord> kvData2 =
                    Arrays.asList(
                            kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v111"}),
                            kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v222"}),
                            kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v233"}));
            KvRecordBatch kvRecordBatch2 = kvRecordBatchFactory.ofRecords(kvData2);
            kvTablet.putAsLeader(kvRecordBatch2, null);
            endOffset = logTablet.localLogEndOffset();
            assertThat(endOffset).isEqualTo(offsetBefore + i + 1);

            // the empty batch will be read if no projection,
            // the empty batch will not be read if has projection
            if (!doProjection) {
                MemoryLogRecords emptyLogs =
                        logRecords(
                                readLogRowType,
                                offsetBefore + i,
                                Collections.emptyList(),
                                Collections.emptyList());
                MultiBytesView bytesView =
                        MultiBytesView.builder()
                                .addBytes(
                                        expectedLogs.getMemorySegment(),
                                        0,
                                        expectedLogs.sizeInBytes())
                                .addBytes(emptyLogs.getMemorySegment(), 0, emptyLogs.sizeInBytes())
                                .build();
                expectedLogs = MemoryLogRecords.pointToBytesView(bytesView);
            }
            actualLogRecords = readLogRecords(logTablet, 0, logProjection);
            assertThatLogRecords(actualLogRecords)
                    .withSchema(readLogRowType)
                    .assertCheckSum(!doProjection)
                    .isEqualTo(expectedLogs);
        }

        List<KvRecord> kvData3 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v22"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v21"}),
                        kvRecordFactory.ofRecord("k1".getBytes(), null),
                        kvRecordFactory.ofRecord("k3".getBytes(), new Object[] {3, "v31"}));
        KvRecordBatch kvRecordBatch3 = kvRecordBatchFactory.ofRecords(kvData3);
        kvTablet.putAsLeader(kvRecordBatch3, null);

        expectedLogs =
                logRecords(
                        readLogRowType,
                        endOffset,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(
                                doProjection ? new Object[] {3} : new Object[] {3, "v31"}));
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // Test schema evolution.
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(DATA2_SCHEMA, 2));
        readLogRowType = doProjection ? DATA2_ROW_TYPE.project(new int[] {0}) : DATA2_ROW_TYPE;
        if (doProjection) {
            logProjection = new FileLogProjection(projectionCache);
            logProjection.setCurrentProjection(
                    0L, schemaGetter, DEFAULT_COMPRESSION, new int[] {0});
        }

        // schema evolution case 1 :insert with data with schema 2
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryOfSchema2 =
                KvRecordTestUtils.KvRecordBatchFactory.of(2);
        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(DATA2_ROW_TYPE);

        KvRecordBatch kvData4 =
                batchFactoryOfSchema2.ofRecords(
                        recordFactory.ofRecord("k3".getBytes(), new Object[] {3, "v41", "c1"}),
                        recordFactory.ofRecord("k4".getBytes(), new Object[] {4, "v41", "c2"}));
        endOffset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvData4, null);
        expectedLogs =
                logRecords(
                        readLogRowType,
                        endOffset,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(
                                doProjection ? new Object[] {4} : new Object[] {4, "v41", "c2"}),
                        (short) 2);
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // schema evolution case 2 :insert with data with old schema 1.
        KvRecordBatch kvData5 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k4".getBytes(), new Object[] {4, "v51"}),
                        kvRecordFactory.ofRecord("k5".getBytes(), new Object[] {5, "v51"}));
        endOffset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvData5, null);
        expectedLogs =
                logRecords(
                        readLogRowType,
                        endOffset,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(
                                doProjection ? new Object[] {5} : new Object[] {5, "v51", null}),
                        (short) 2);
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testVersionRowMergeEngine(boolean doProjection) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.INT())
                        .withComment("b is second column")
                        .primaryKey("a")
                        .build();

        Schema newSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .withComment("c is third column")
                        .build();
        Map<String, String> config = new HashMap<>();
        config.put("table.merge-engine", "versioned");
        config.put("table.merge-engine.versioned.ver-column", "b");
        String tableName =
                "test_versioned_row_merge_engine_"
                        + (doProjection ? "projection" : "no_projection");
        TablePath tablePath = TablePath.of("testDb", tableName);
        initLogTabletAndKvTablet(tablePath, schema, config);
        RowType rowType = schema.getRowType();
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(rowType);

        ProjectionPushdownCache projectionCache = new ProjectionPushdownCache();
        FileLogProjection logProjection = null;
        if (doProjection) {
            logProjection = new FileLogProjection(projectionCache);
            logProjection.setCurrentProjection(
                    0L, schemaGetter, DEFAULT_COMPRESSION, new int[] {0});
        }
        RowType readLogRowType = doProjection ? rowType.project(new int[] {0}) : rowType;

        List<KvRecord> kvData1 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 1000}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, 1000}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, 1001}));
        KvRecordBatch kvRecordBatch1 = kvRecordBatchFactory.ofRecords(kvData1);
        kvTablet.putAsLeader(kvRecordBatch1, null);

        long endOffset = logTablet.localLogEndOffset();
        LogRecords actualLogRecords = readLogRecords(logTablet, 0L, logProjection);
        MemoryLogRecords expectedLogs =
                logRecords(
                        readLogRowType,
                        0,
                        Arrays.asList(
                                ChangeType.INSERT,
                                ChangeType.INSERT,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        doProjection
                                ? Arrays.asList(
                                        new Object[] {1},
                                        new Object[] {2},
                                        new Object[] {2},
                                        new Object[] {2})
                                : Arrays.asList(
                                        new Object[] {1, 1000},
                                        new Object[] {2, 1000},
                                        new Object[] {2, 1000},
                                        new Object[] {2, 1001}));
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // append some new batches which will not generate only cdc logs, but the endOffset will be
        // updated.
        int emptyBatchCount = 3;
        long offsetBefore = endOffset;
        for (int i = 0; i < emptyBatchCount; i++) {
            List<KvRecord> kvData2 =
                    Arrays.asList(
                            kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 999}),
                            kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, 998}));
            KvRecordBatch kvRecordBatch2 = kvRecordBatchFactory.ofRecords(kvData2);
            kvTablet.putAsLeader(kvRecordBatch2, null);
            endOffset = logTablet.localLogEndOffset();
            assertThat(endOffset).isEqualTo(offsetBefore + i + 1);

            // the empty batch will be read if no projection,
            // the empty batch will not be read if has projection
            if (!doProjection) {
                MemoryLogRecords emptyLogs =
                        logRecords(
                                readLogRowType,
                                offsetBefore + i,
                                Collections.emptyList(),
                                Collections.emptyList());
                MultiBytesView bytesView =
                        MultiBytesView.builder()
                                .addBytes(
                                        expectedLogs.getMemorySegment(),
                                        0,
                                        expectedLogs.sizeInBytes())
                                .addBytes(emptyLogs.getMemorySegment(), 0, emptyLogs.sizeInBytes())
                                .build();
                expectedLogs = MemoryLogRecords.pointToBytesView(bytesView);
            }
            actualLogRecords = readLogRecords(logTablet, 0, logProjection);
            assertThatLogRecords(actualLogRecords)
                    .withSchema(readLogRowType)
                    .assertCheckSum(!doProjection)
                    .isEqualTo(expectedLogs);
        }

        List<KvRecord> kvData3 =
                Arrays.asList(
                        kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 1000}), // not update
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 1001}), // -U , +U
                        kvRecordFactory.ofRecord("k1".getBytes(), null), // not update
                        kvRecordFactory.ofRecord("k3".getBytes(), new Object[] {3, 1000})); // +I
        KvRecordBatch kvRecordBatch3 = kvRecordBatchFactory.ofRecords(kvData3);
        kvTablet.putAsLeader(kvRecordBatch3, null);

        expectedLogs =
                logRecords(
                        readLogRowType,
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.INSERT),
                        doProjection
                                ? Arrays.asList(
                                        new Object[] {1}, new Object[] {1}, new Object[] {3})
                                : Arrays.asList(
                                        new Object[] {1, 1000},
                                        new Object[] {1, 1001},
                                        new Object[] {3, 1000}));
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(readLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // Test schema evolution.
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, 2));
        RowType newSchemaLogRowType =
                doProjection
                        ? newSchema.getRowType().project(new int[] {0})
                        : newSchema.getRowType();
        if (doProjection) {
            logProjection = new FileLogProjection(projectionCache);
            logProjection.setCurrentProjection(
                    0L, schemaGetter, DEFAULT_COMPRESSION, new int[] {0});
        }

        // schema evolution case 1 :insert with data with new schema
        KvRecordTestUtils.KvRecordBatchFactory batchFactoryOfBroadenSchema =
                KvRecordTestUtils.KvRecordBatchFactory.of(2);
        KvRecordTestUtils.KvRecordFactory recordFactoryOfBroadenSchema =
                KvRecordTestUtils.KvRecordFactory.of(newSchema.getRowType());
        KvRecordBatch kvRecordBatch4 =
                batchFactoryOfBroadenSchema.ofRecords(
                        recordFactoryOfBroadenSchema.ofRecord(
                                "k1".getBytes(), new Object[] {1, 1002, "a"}));
        endOffset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvRecordBatch4, null);
        expectedLogs =
                logRecords(
                        newSchemaLogRowType,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        doProjection
                                ? Arrays.asList(new Object[] {1}, new Object[] {1})
                                : Arrays.asList(
                                        new Object[] {1, 1001, null}, new Object[] {1, 1002, "a"}),
                        (short) 2);
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(newSchemaLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);

        // schema evolution case 1 :insert with data with old schema
        KvRecordBatch kvRecordBatch5 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 1003}));
        endOffset = logTablet.localLogEndOffset();
        kvTablet.putAsLeader(kvRecordBatch5, null);
        expectedLogs =
                logRecords(
                        newSchemaLogRowType,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        doProjection
                                ? Arrays.asList(new Object[] {1}, new Object[] {1})
                                : Arrays.asList(
                                        new Object[] {1, 1002, "a"}, new Object[] {1, 1003, null}),
                        (short) 2);
        actualLogRecords = readLogRecords(logTablet, endOffset, logProjection);
        assertThatLogRecords(actualLogRecords)
                .withSchema(newSchemaLogRowType)
                .assertCheckSum(!doProjection)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testAppendDuplicatedKvBatch() throws Exception {
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, new HashMap<>());
        long writeId = 100L;
        List<KvRecord> kvData1 =
                Arrays.asList(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v11"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v21"}),
                        kvRecordFactory.ofRecord("k2".getBytes(), new Object[] {2, "v23"}));
        KvRecordBatch kvRecordBatch = kvRecordBatchFactory.ofRecords(kvData1, writeId, 0);
        kvTablet.putAsLeader(kvRecordBatch, null);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);

        // append a duplicated kv batch (with same writerId and batchSequenceId).
        // This situation occurs when the client sends a KV batch of data to server, the server
        // successfully ack the cdc log, but a network error occurs while returning the success
        // response to client. The client will re-send this batch.
        kvTablet.putAsLeader(kvRecordBatch, null);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);

        // append a duplicated kv batch again.
        kvTablet.putAsLeader(kvRecordBatch, null);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(3);

        // append a new batch, the max LSN should be updated.
        KvRecordBatch kvRecordBatch2 = kvRecordBatchFactory.ofRecords(kvData1, writeId, 1);
        kvTablet.putAsLeader(kvRecordBatch2, null);
        assertThat(kvTablet.getKvPreWriteBuffer().getMaxLSN()).isEqualTo(9);
    }

    private LogRecords readLogRecords() throws Exception {
        return readLogRecords(0L);
    }

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return readLogRecords(logTablet, startOffset);
    }

    private LogRecords readLogRecords(LogTablet logTablet, long startOffset) throws Exception {
        return readLogRecords(logTablet, startOffset, null);
    }

    private LogRecords readLogRecords(
            LogTablet logTablet, long startOffset, @Nullable FileLogProjection projection)
            throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, projection)
                .getRecords();
    }

    private MemoryLogRecords logRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> values) throws Exception {
        return logRecords(baseRowType, baseOffset, changeTypes, values);
    }

    private MemoryLogRecords logRecords(
            RowType rowType, long baseOffset, List<ChangeType> changeTypes, List<Object[]> values)
            throws Exception {
        return logRecords(rowType, baseOffset, changeTypes, values, DEFAULT_SCHEMA_ID);
    }

    private MemoryLogRecords logRecords(
            RowType rowType,
            long baseOffset,
            List<ChangeType> changeTypes,
            List<Object[]> values,
            short schemaId)
            throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                schemaId,
                baseOffset,
                -1L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changeTypes,
                values,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
    }

    private void checkEqual(
            LogRecords actaulLogRecords, List<MemoryLogRecords> expectedLogs, RowType rowType) {
        LogTestBase.assertLogRecordsListEquals(expectedLogs, actaulLogRecords, rowType);
    }

    private void checkEqual(LogRecords actaulLogRecords, List<MemoryLogRecords> expectedLogs) {
        LogTestBase.assertLogRecordsListEquals(expectedLogs, actaulLogRecords, baseRowType);
    }

    private Value valueOf(BinaryRow row) {
        return Value.of(ValueEncoder.encodeValue(schemaId, row));
    }

    /**
     * Test for TTL functionality with TsValueEncoder in index tables. This test verifies that: 1.
     * Data is encoded with timestamps using TsValueEncoder 2. Manual compaction triggers TTL
     * evaluation 3. Expired data is removed after compaction 4. Non-expired data remains after
     * compaction
     */
    @Test
    void testIndexTableTTLWithCompaction() throws Exception {
        // Initialize index table with 10-second TTL
        long ttlMillis = 10_000L; // 10 seconds

        // Create a ManualClock for precise time control
        ManualClock manualClock = new ManualClock(System.currentTimeMillis());

        // Reuse index table schema and path from TestData
        Schema indexSchema = TestData.IDX_NAME_SCHEMA;
        RowType indexRowType = TestData.IDX_NAME_ROW_TYPE;
        PhysicalTablePath indexTablePath = TestData.IDX_NAME_PHYSICAL_TABLE_PATH;

        logTablet = createLogTablet(tempLogDir, 0L, indexTablePath);
        TableBucket tableBucket = logTablet.getTableBucket();

        // Create index KvTablet with TTL enabled and ManualClock
        RowMerger rowMerger =
                RowMerger.create(
                        new TableConfig(Configuration.fromMap(Collections.emptyMap())),
                        KvFormat.COMPACTED);
        SchemaGetter schemaGetter = new TestingSchemaGetter(1, indexSchema);
        kvTablet =
                KvTablet.create(
                        indexTablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        null,
                        ttlMillis,
                        manualClock);

        // Prepare test data with timestamps
        // Use the clock's current time to determine expiration
        // For expired data, use a timestamp that is older than TTL
        // For valid data, use a very recent timestamp (just 1 second ago)
        long currentTime = manualClock.milliseconds();
        long expiredTimestamp = currentTime - ttlMillis - 5000L; // 15 seconds ago (expired)
        long validTimestamp = currentTime - 1000L; // 1 second ago (valid)

        // Create rows with schema [name(STRING), id(INT), __offset(BIGINT)]
        BinaryRow expiredRow1 = compactedRow(indexRowType, new Object[] {"name1", 1, 100L});
        BinaryRow expiredRow2 = compactedRow(indexRowType, new Object[] {"name2", 2, 200L});
        BinaryRow validRow1 = compactedRow(indexRowType, new Object[] {"name3", 3, 300L});
        BinaryRow validRow2 = compactedRow(indexRowType, new Object[] {"name4", 4, 400L});

        // Encode with TsValueEncoder: [timestamp(8)][schemaId(2)][row bytes]
        byte[] key1 = new byte[] {0, 0, 0, 1}; // key for id=1
        byte[] key2 = new byte[] {0, 0, 0, 2}; // key for id=2
        byte[] key3 = new byte[] {0, 0, 0, 3}; // key for id=3
        byte[] key4 = new byte[] {0, 0, 0, 4}; // key for id=4

        byte[] expiredValue1 =
                org.apache.fluss.row.encode.TsValueEncoder.encodeValue(
                        expiredTimestamp, schemaId, expiredRow1);
        byte[] expiredValue2 =
                org.apache.fluss.row.encode.TsValueEncoder.encodeValue(
                        expiredTimestamp, schemaId, expiredRow2);
        byte[] validValue1 =
                org.apache.fluss.row.encode.TsValueEncoder.encodeValue(
                        validTimestamp, schemaId, validRow1);
        byte[] validValue2 =
                org.apache.fluss.row.encode.TsValueEncoder.encodeValue(
                        validTimestamp, schemaId, validRow2);

        // Write data directly to RocksDB
        KvBatchWriter batchWriter = kvTablet.createKvBatchWriter();
        batchWriter.put(key1, expiredValue1);
        batchWriter.put(key2, expiredValue2);
        batchWriter.put(key3, validValue1);
        batchWriter.put(key4, validValue2);
        batchWriter.flush();
        batchWriter.close();

        // Get RocksDBKv for direct access
        RocksDBKv rocksDBKv = kvTablet.getRocksDBKv();

        // Verify all data exists before compaction
        byte[] readValue1 = rocksDBKv.get(key1);
        byte[] readValue2 = rocksDBKv.get(key2);
        byte[] readValue3 = rocksDBKv.get(key3);
        byte[] readValue4 = rocksDBKv.get(key4);
        assertThat(readValue1).isNotNull();
        assertThat(readValue2).isNotNull();
        assertThat(readValue3).isNotNull();
        assertThat(readValue4).isNotNull();

        // Trigger manual compaction to apply TTL filtering
        // The expiredTimestamp is already 15 seconds old, which is beyond the 10-second TTL
        kvTablet.compact();

        // Wait longer for compaction to complete
        Thread.sleep(1000);

        // Verify expired data is removed
        assertThat(rocksDBKv.get(key1))
                .as("Expired row 1 should be removed after compaction")
                .isNull();
        assertThat(rocksDBKv.get(key2))
                .as("Expired row 2 should be removed after compaction")
                .isNull();

        // Verify valid data still exists
        assertThat(rocksDBKv.get(key3))
                .as("Valid row 1 should still exist after compaction")
                .isNotNull();
        assertThat(rocksDBKv.get(key4))
                .as("Valid row 2 should still exist after compaction")
                .isNotNull();

        // Verify the content of valid rows
        byte[] validReadValue1 = rocksDBKv.get(key3);
        byte[] validReadValue2 = rocksDBKv.get(key4);
        assertThat(validReadValue1).isEqualTo(validValue1);
        assertThat(validReadValue2).isEqualTo(validValue2);
    }
}
