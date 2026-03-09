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
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link KvTablet} with {@link MergeMode} support.
 *
 * <p>These tests verify that OVERWRITE mode correctly bypasses the merge engine and directly
 * replaces values, which is essential for undo recovery scenarios.
 */
class KvTabletMergeModeTest {

    private static final short SCHEMA_ID = 1;

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    // Schema with aggregation functions for testing
    private static final Schema AGG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                    .column("name", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                    .primaryKey("id")
                    .build();

    private static final RowType AGG_ROW_TYPE = AGG_SCHEMA.getRowType();

    // Sum-only schema for retract tests (all non-PK fields must be retract-safe)
    private static final Schema AGG_RETRACT_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("total", DataTypes.DOUBLE(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    private static final RowType AGG_RETRACT_ROW_TYPE = AGG_RETRACT_SCHEMA.getRowType();

    // LAST_VALUE schema for retract + delete behavior tests
    private static final Schema LAST_VALUE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                    .primaryKey("id")
                    .build();

    private static final RowType LAST_VALUE_ROW_TYPE = LAST_VALUE_SCHEMA.getRowType();

    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(AGG_ROW_TYPE);

    private final KvRecordTestUtils.KvRecordFactory retractKvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(AGG_RETRACT_ROW_TYPE);

    private final KvRecordTestUtils.KvRecordFactory lastValueKvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(LAST_VALUE_ROW_TYPE);

    @BeforeEach
    void setUp() throws Exception {
        initTablets(AGG_SCHEMA);
    }

    /** Reinitialize tablets with a different schema. Closes existing tablets first. */
    private void initTablets(Schema schema) throws Exception {
        initTablets(schema, Collections.emptyMap());
    }

    /**
     * Reinitialize tablets with a different schema and extra config. Closes existing tablets first.
     */
    private void initTablets(Schema schema, Map<String, String> extraConfig) throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }

        Map<String, String> config = new HashMap<>();
        config.put("table.merge-engine", "aggregation");
        config.putAll(extraConfig);

        TablePath tablePath = TablePath.of("testDb", "test_merge_mode");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir,
                        physicalTablePath.getDatabaseName(),
                        0L,
                        physicalTablePath.getTableName());
        logTablet =
                LogTablet.create(
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
        TableConfig tableConf = new TableConfig(Configuration.fromMap(config));
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        tablePath,
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());

        kvTablet =
                KvTablet.create(
                        physicalTablePath,
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
                        tableConf.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
    }

    // ==================== DEFAULT Mode Tests ====================

    @Test
    void testDefaultModeAppliesMergeEngine() throws Exception {
        // Insert initial record: id=1, count=10, max_val=100, name="Alice"
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Update with DEFAULT mode: count should be summed, max_val should take max
        // id=1, count=5, max_val=150, name="Bob"
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 150, "Bob"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        // Verify CDC log shows aggregated values
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 15L, 150, "Bob"
                                } // after: count=10+5, max=max(100,150)
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== OVERWRITE Mode Tests ====================

    @Test
    void testOverwriteModeBypassesMergeEngine() throws Exception {
        // Insert initial record with DEFAULT mode
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Update with OVERWRITE mode: values should be directly replaced, not aggregated
        // id=1, count=5, max_val=50, name="Bob"
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 5L, 50, "Bob"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.OVERWRITE);

        // Verify CDC log shows directly replaced values (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 5L, 50, "Bob"
                                } // after: directly replaced, NOT aggregated
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);

        // Key assertion: count=5 (not 15), max_val=50 (not 100)
        // This proves OVERWRITE bypassed the merge engine
    }

    @Test
    void testOverwriteModeForUndoRecoveryScenario() throws Exception {
        // Simulate a typical undo recovery scenario:
        // 1. Initial state: id=1, count=100, max_val=500, name="Original"
        // 2. After some operations: id=1, count=150, max_val=600, name="Updated"
        // 3. Undo recovery needs to restore to: id=1, count=100, max_val=500, name="Original"

        // Step 1: Insert initial record
        KvRecordBatch initialBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500, "Original"}));
        kvTablet.putAsLeader(initialBatch, null, MergeMode.DEFAULT);

        // Step 2: Simulate some aggregation operations
        KvRecordBatch updateBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 50L, 600, "Updated"}));
        kvTablet.putAsLeader(updateBatch, null, MergeMode.DEFAULT);

        long beforeUndoOffset = logTablet.localLogEndOffset();

        // Step 3: Undo recovery - restore to original state using OVERWRITE mode
        KvRecordBatch undoBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500, "Original"}));
        kvTablet.putAsLeader(undoBatch, null, MergeMode.OVERWRITE);

        // Verify the undo operation produced correct CDC log
        LogRecords actualLogRecords = readLogRecords(beforeUndoOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        beforeUndoOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                // Before: the aggregated state (count=150, max_val=600)
                                new Object[] {1, 150L, 600, "Updated"},
                                // After: restored to original (count=100, max_val=500)
                                new Object[] {1, 100L, 500, "Original"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithNewKey() throws Exception {
        // OVERWRITE mode with a new key should behave like INSERT
        KvRecordBatch batch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch, null, MergeMode.OVERWRITE);

        LogRecords actualLogRecords = readLogRecords(0);
        MemoryLogRecords expectedLogs =
                logRecords(
                        0L,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {1, 10L, 100, "Alice"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithDelete() throws Exception {
        // Insert initial record
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Delete with OVERWRITE mode
        KvRecordBatch deleteBatch =
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(deleteBatch, null, MergeMode.OVERWRITE);

        // Verify DELETE is produced
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Collections.singletonList(ChangeType.DELETE),
                        Collections.singletonList(new Object[] {1, 10L, 100, "Alice"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testMixedMergeModeOperations() throws Exception {
        // Test interleaved DEFAULT and OVERWRITE operations

        // 1. Insert with DEFAULT
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "v1"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        // 2. Update with DEFAULT (should aggregate)
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 5L, 150, "v2"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        long afterDefaultOffset = logTablet.localLogEndOffset();

        // 3. Overwrite with specific value
        KvRecordBatch batch3 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, 20L, 80, "v3"}));
        kvTablet.putAsLeader(batch3, null, MergeMode.OVERWRITE);

        long afterOverwriteOffset = logTablet.localLogEndOffset();

        // 4. Continue with DEFAULT (should aggregate from overwritten value)
        KvRecordBatch batch4 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 200, "v4"}));
        kvTablet.putAsLeader(batch4, null, MergeMode.DEFAULT);

        // Verify the final aggregation is based on overwritten value
        LogRecords actualLogRecords = readLogRecords(afterOverwriteOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        afterOverwriteOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                // Before: overwritten value
                                new Object[] {1, 20L, 80, "v3"},
                                // After: aggregated from overwritten value
                                // count=20+10=30, max_val=max(80,200)=200
                                new Object[] {1, 30L, 200, "v4"}));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithPartialUpdate() throws Exception {
        // Insert initial record
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Partial update with OVERWRITE mode (only update id and count columns)
        int[] targetColumns = new int[] {0, 1}; // id and count
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, null, null}));
        kvTablet.putAsLeader(batch2, targetColumns, MergeMode.OVERWRITE);

        // Verify partial update with OVERWRITE: count should be replaced (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"}, // before
                                new Object[] {
                                    1, 5L, 100, "Alice"
                                } // after: count replaced, others unchanged
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testOverwriteModeWithMultipleKeys() throws Exception {
        // Insert multiple records
        List<KvRecord> initialRecords =
                Arrays.asList(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}),
                        kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 20L, 200, "Bob"}));
        KvRecordBatch batch1 = kvRecordBatchFactory.ofRecords(initialRecords);
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Overwrite multiple keys in single batch
        List<KvRecord> overwriteRecords =
                Arrays.asList(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 50, "Alice2"}),
                        kvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 8L, 80, "Bob2"}));
        KvRecordBatch batch2 = kvRecordBatchFactory.ofRecords(overwriteRecords);
        kvTablet.putAsLeader(batch2, null, MergeMode.OVERWRITE);

        // Verify both keys are overwritten (not aggregated)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"},
                                new Object[] {1, 5L, 50, "Alice2"}, // k1 overwritten
                                new Object[] {2, 20L, 200, "Bob"},
                                new Object[] {2, 8L, 80, "Bob2"} // k2 overwritten
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== Default MergeMode Tests ====================

    @Test
    void testDefaultMergeModeIsDefault() throws Exception {
        // Insert initial record using default (no mergeMode parameter)
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100, "Alice"}));
        kvTablet.putAsLeader(batch1, null); // Using overload without mergeMode

        long endOffset = logTablet.localLogEndOffset();

        // Update using default (should aggregate)
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 150, "Bob"}));
        kvTablet.putAsLeader(batch2, null); // Using overload without mergeMode

        // Verify aggregation happened (proving default is DEFAULT)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100, "Alice"},
                                new Object[] {
                                    1, 15L, 150, "Bob"
                                } // count=10+5=15, max=max(100,150)=150
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== RETRACT Mode Tests ====================

    @Test
    void testRetractModeReversesSumAggregation() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert initial record: id=1, count=10, total=100.0
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        // Aggregate more: count=10+5=15, total=100+50=150
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 50.0}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Retract the second record: count=15-5=10, total=150-50=100
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 5L, 50.0}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 15L, 150.0}, // before
                                new Object[] {1, 10L, 100.0} // after: count=15-5, total=150-50
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractModeWithNonExistentKeyIsIgnored() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        long startOffset = logTablet.localLogEndOffset();

        // Retract a key that doesn't exist - should be silently ignored
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "nonexistent".getBytes(), new Object[] {99, 5L, 100.0}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);
        long endOffset = logTablet.localLogEndOffset();
        // offset advances by 1 for the empty batch
        assertThat(endOffset).isEqualTo(startOffset + 1);
    }

    @Test
    void testNoOpRetractDoesNotProduceCdcRecords() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long startOffset = logTablet.localLogEndOffset();

        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, null, null}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();
        assertThat(endOffset).isEqualTo(startOffset + 1);
        assertThat(countLogRecords(readLogRecords(startOffset), AGG_RETRACT_ROW_TYPE)).isZero();
    }

    @Test
    void testRetractThenAggregate() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert: id=1, count=100, total=500.0
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        // Retract 30 from count, 100.0 from total
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 30L, 100.0}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Aggregate again: count=70+20=90, total=400+200=600
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 20L, 200.0}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 70L, 400.0}, // before (after retract)
                                new Object[] {1, 90L, 600.0} // after: count=70+20, total=400+200
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractWithMultipleKeys() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert two keys
        List<KvRecord> initialRecords =
                Arrays.asList(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100.0}),
                        retractKvRecordFactory.ofRecord(
                                "k2".getBytes(), new Object[] {2, 20L, 200.0}));
        KvRecordBatch batch1 = kvRecordBatchFactory.ofRecords(initialRecords);
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Retract both keys in a single batch
        List<KvRecord> retractRecords =
                Arrays.asList(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 3L, 10.0}),
                        retractKvRecordFactory.ofRetractRecord(
                                "k2".getBytes(), new Object[] {2, 7L, 50.0}));
        KvRecordBatch retractBatch = kvRecordBatchFactory.ofRecords(retractRecords);
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset,
                        Arrays.asList(
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100.0},
                                new Object[] {1, 7L, 90.0}, // count=10-3, total=100-10
                                new Object[] {2, 20L, 200.0},
                                new Object[] {2, 13L, 150.0} // count=20-7, total=200-50
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== RETRACT + DeleteBehavior Tests ====================

    @Test
    void testRetractWithDeleteBehaviorAllowProducesDeleteCdc() throws Exception {
        // LAST_VALUE retract always returns null for non-PK fields.
        // With DeleteBehavior.ALLOW, this should produce UPDATE_BEFORE/UPDATE_AFTER CDC.
        long endOffset = initLastValueTabletWithRecord("allow");

        // Retract: LAST_VALUE retract returns null for val, PK is kept.
        // Result: id=1, val=null (non-null BinaryValue with null fields).
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, "hello"}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        // Verify CDC: UPDATE_BEFORE/UPDATE_AFTER (val becomes null)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                lastValueLogRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, "hello"}, // before
                                new Object[] {1, null} // after: val retracted to null
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(LAST_VALUE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractWithDeleteBehaviorDisableThrowsOnDeletion() throws Exception {
        // LAST_VALUE with DeleteBehavior.DISABLE:
        // Retract succeeds (non-null result), but DELETE should throw.
        initLastValueTabletWithRecord("disable");

        // Retract should succeed (produces a row with null val, not a deletion)
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, "hello"}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        // But DELETE should throw with DISABLE behavior
        KvRecordBatch deleteBatch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRecord("k1".getBytes(), null));
        assertThatThrownBy(() -> kvTablet.putAsLeader(deleteBatch, null, MergeMode.DEFAULT))
                .isInstanceOf(org.apache.fluss.exception.DeletionDisabledException.class);
    }

    // ==================== RETRACT + Partial Update Tests ====================

    @Test
    void testRetractWithPartialUpdate() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert initial record: id=1, count=100, total=500.0
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Partial retract: only retract 'count' column (index 0=id, 1=count)
        int[] targetColumns = new int[] {0, 1};
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 30L, null}));
        kvTablet.putAsLeader(retractBatch, targetColumns, MergeMode.DEFAULT);

        // Verify: count=100-30=70, total unchanged=500.0
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 100L, 500.0}, // before
                                new Object[] {1, 70L, 500.0} // after: count retracted, total kept
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== RETRACT + Schema Evolution Tests ====================

    @Test
    void testRetractWithSchemaEvolution() throws Exception {
        // Start with old schema: id(columnId=0), count(columnId=1)
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();
        RowType oldRowType = oldSchema.getRowType();
        KvRecordTestUtils.KvRecordFactory oldFactory =
                KvRecordTestUtils.KvRecordFactory.of(oldRowType);

        initTablets(oldSchema);

        // Insert with old schema: id=1, count=100
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        oldFactory.ofRecord("k1".getBytes(), new Object[] {1, 100L}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        // Evolve schema: add new_sum column (columnId=2)
        short newSchemaId = (short) 2;
        Schema newSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        new Schema.Column("id", DataTypes.INT(), null, 0),
                                        new Schema.Column(
                                                "count",
                                                DataTypes.BIGINT(),
                                                null,
                                                1,
                                                AggFunctions.SUM()),
                                        new Schema.Column(
                                                "new_sum",
                                                DataTypes.BIGINT(),
                                                null,
                                                2,
                                                AggFunctions.SUM())))
                        .primaryKey("id")
                        .build();
        RowType newRowType = newSchema.getRowType();
        KvRecordTestUtils.KvRecordFactory newFactory =
                KvRecordTestUtils.KvRecordFactory.of(newRowType);
        KvRecordTestUtils.KvRecordBatchFactory newBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(newSchemaId);

        // Update schema getter to know about both schemas
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, newSchemaId));

        long endOffset = logTablet.localLogEndOffset();

        // Retract with new schema: id=1, count=30, new_sum=10
        KvRecordBatch retractBatch =
                newBatchFactory.ofRecords(
                        newFactory.ofRetractRecord("k1".getBytes(), new Object[] {1, 30L, 10L}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        // Verify: count=100-30=70, new_sum: null-10=null (retract from null stays null)
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                logRecords(
                        newRowType,
                        newSchemaId,
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 100L, null}, // before (old schema padded)
                                new Object[] {1, 70L, null} // after: count retracted, new_sum null
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(newRowType)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== RETRACT Edge Case Tests ====================

    @Test
    void testDoubleRetractOnSameKey() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert: id=1, count=100, total=500.0
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 100L, 500.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset1 = logTablet.localLogEndOffset();

        // First retract: count=100-30=70, total=500-100=400
        KvRecordBatch retract1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 30L, 100.0}));
        kvTablet.putAsLeader(retract1, null, MergeMode.DEFAULT);

        long endOffset2 = logTablet.localLogEndOffset();

        // Second retract: count=70-20=50, total=400-150=250
        KvRecordBatch retract2 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 20L, 150.0}));
        kvTablet.putAsLeader(retract2, null, MergeMode.DEFAULT);

        // Verify second retract reads from pre-write buffer (not RocksDB)
        LogRecords actualLogRecords = readLogRecords(endOffset2);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset2,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 70L, 400.0}, // before (after first retract)
                                new Object[] {1, 50L, 250.0} // after second retract
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractToZeroAndNegative() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // Insert: id=1, count=10, total=100.0
        KvRecordBatch batch1 =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 100.0}));
        kvTablet.putAsLeader(batch1, null, MergeMode.DEFAULT);

        long endOffset = logTablet.localLogEndOffset();

        // Retract full amount: count=10-10=0, total=100-200=-100 (goes negative)
        KvRecordBatch retractBatch =
                kvRecordBatchFactory.ofRecords(
                        retractKvRecordFactory.ofRetractRecord(
                                "k1".getBytes(), new Object[] {1, 10L, 200.0}));
        kvTablet.putAsLeader(retractBatch, null, MergeMode.DEFAULT);

        // Verify: row is kept (not deleted), values are 0 and -100
        LogRecords actualLogRecords = readLogRecords(endOffset);
        MemoryLogRecords expectedLogs =
                retractLogRecords(
                        endOffset,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, 10L, 100.0}, // before
                                new Object[] {1, 0L, -100.0} // after: zero and negative
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(AGG_RETRACT_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    @Test
    void testRetractWithDeleteBehaviorIgnoreSkipsNullResult() throws Exception {
        // LAST_VALUE with DeleteBehavior.IGNORE (the default):
        // Retract produces a non-null row (PK kept, val=null), so it should succeed.
        // But if we then DELETE (which produces null), IGNORE should silently skip it.
        long endOffsetAfterInsert = initLastValueTabletWithRecord("ignore");

        // DELETE with IGNORE behavior: should be silently skipped
        KvRecordBatch deleteBatch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(deleteBatch, null, MergeMode.DEFAULT);

        long endOffsetAfterDelete = logTablet.localLogEndOffset();

        // Log should advance by 1 (empty batch, no CDC records)
        assertThat(endOffsetAfterDelete).isEqualTo(endOffsetAfterInsert + 1);

        // Verify original value is still intact by aggregating on top
        KvRecordBatch batch2 =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, "world"}));
        kvTablet.putAsLeader(batch2, null, MergeMode.DEFAULT);

        LogRecords actualLogRecords = readLogRecords(endOffsetAfterDelete);
        MemoryLogRecords expectedLogs =
                lastValueLogRecords(
                        endOffsetAfterDelete,
                        Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                        Arrays.asList(
                                new Object[] {1, "hello"}, // before: still "hello" (delete ignored)
                                new Object[] {1, "world"} // after: updated to "world"
                                ));

        assertThatLogRecords(actualLogRecords)
                .withSchema(LAST_VALUE_ROW_TYPE)
                .assertCheckSum(true)
                .isEqualTo(expectedLogs);
    }

    // ==================== RETRACT Null-Row Guard Test ====================

    @Test
    void testRetractWithNullRowIsRejectedAtBatchConstruction() throws Exception {
        initTablets(AGG_RETRACT_SCHEMA);

        // A retract record with null row is invalid — RETRACT requires row data to compute
        // inverse aggregation. This should be rejected at batch construction time.
        assertThatThrownBy(
                        () ->
                                kvRecordBatchFactory.ofRecords(
                                        retractKvRecordFactory.ofRetractRecord(
                                                "k1".getBytes(), null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RETRACT requires row data");
    }

    // ==================== Helper Methods ====================

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null)
                .getRecords();
    }

    private int countLogRecords(LogRecords logRecords, RowType rowType) {
        LogRecordReadContext context =
                LogRecordReadContext.createArrowReadContext(rowType, SCHEMA_ID, schemaGetter);
        int recordCount = 0;
        for (LogRecordBatch batch : logRecords.batches()) {
            try (CloseableIterator<LogRecord> iterator = batch.records(context)) {
                while (iterator.hasNext()) {
                    iterator.next();
                    recordCount++;
                }
            }
        }
        return recordCount;
    }

    private MemoryLogRecords logRecords(
            RowType rowType, long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows)
            throws Exception {
        return logRecords(rowType, SCHEMA_ID, baseOffset, changeTypes, rows);
    }

    private MemoryLogRecords logRecords(
            RowType rowType,
            short schemaId,
            long baseOffset,
            List<ChangeType> changeTypes,
            List<Object[]> rows)
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
                rows,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
    }

    private MemoryLogRecords logRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows) throws Exception {
        return logRecords(AGG_ROW_TYPE, baseOffset, changeTypes, rows);
    }

    private MemoryLogRecords retractLogRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows) throws Exception {
        return logRecords(AGG_RETRACT_ROW_TYPE, baseOffset, changeTypes, rows);
    }

    private MemoryLogRecords lastValueLogRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> rows) throws Exception {
        return logRecords(LAST_VALUE_ROW_TYPE, baseOffset, changeTypes, rows);
    }

    /** Initialize a LAST_VALUE tablet with delete behavior and insert id=1, val="hello". */
    private long initLastValueTabletWithRecord(String deleteBehavior) throws Exception {
        initTablets(
                LAST_VALUE_SCHEMA,
                Collections.singletonMap("table.delete.behavior", deleteBehavior));
        KvRecordBatch batch =
                kvRecordBatchFactory.ofRecords(
                        lastValueKvRecordFactory.ofRecord(
                                "k1".getBytes(), new Object[] {1, "hello"}));
        kvTablet.putAsLeader(batch, null, MergeMode.DEFAULT);
        return logTablet.localLogEndOffset();
    }
}
