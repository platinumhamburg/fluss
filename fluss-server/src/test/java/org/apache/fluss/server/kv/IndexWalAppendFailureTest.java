/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchReader;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTabletTestHelper;
import org.apache.fluss.server.log.LogTabletTestHelper.FaultPhase;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexWalAppendFailureTest extends ReplicaTestBase {

    private static final short SCHEMA_ID = 1;
    private static final WriterKey WRITER_KEY = new WriterKey(7L, Long.MIN_VALUE | 3L);

    @Test
    void testKnownBuildFailureTruncatesAndCanRetry() throws Exception {
        Fixture fixture = createFixture(8100L, "known_build_failure");
        KvRecordBatch mutation = mutation(WRITER_KEY, 100L, "value");
        fixture.kv.setBeforeWalBuild(
                () -> {
                    throw new TestBuildException();
                });

        assertThatThrownBy(() -> fixture.put(mutation)).isInstanceOf(TestBuildException.class);
        assertThat(fixture.kv.getKvPreWriteBuffer().getAllKvEntries()).isEmpty();
        assertThat(fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount())
                .isEqualTo(1L);
        assertThat(fixture.log.localLogEndOffset()).isZero();

        fixture.kv.setBeforeWalBuild(null);
        fixture.put(mutation);
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
        assertWriterState(fixture.log, 100L);
    }

    @Test
    void testBeforeLocalAppendRestartConverges() throws Exception {
        assertRestartConverges(FaultPhase.BEFORE_LOCAL_APPEND, 0L);
    }

    @Test
    void testAfterLocalAppendRestartConverges() throws Exception {
        assertRestartConverges(FaultPhase.AFTER_LOCAL_APPEND, 1L);
    }

    @Test
    void testAfterWriterStatePublicationRestartConverges() throws Exception {
        assertRestartConverges(FaultPhase.AFTER_WRITER_STATE_UPDATE, 1L);
    }

    private void assertRestartConverges(FaultPhase phase, long expectedWalEnd) throws Exception {
        Fixture fixture = createFixture(8200L + phase.ordinal(), "phase_" + phase.name());
        KvRecordBatch mutation = mutation(WRITER_KEY, 100L, "value");
        long errorTruncationsBefore =
                fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount();
        LogTabletTestHelper.failOnceAt(fixture.log, phase, new TestAppendException());

        assertThatThrownBy(() -> fixture.put(mutation))
                .isInstanceOf(UncertainWalAppendException.class)
                .hasCauseInstanceOf(TestAppendException.class);
        assertThat(fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount())
                .isEqualTo(errorTruncationsBefore);
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(expectedWalEnd);

        restartFromDurableWal(fixture);
        LogAppendInfo retried = fixture.put(mutation);
        assertThat(retried.duplicated()).isEqualTo(expectedWalEnd == 1L);
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
        fixture.log.updateHighWatermark(1L);
        fixture.kv.flush(1L, NOPErrorHandler.INSTANCE);

        assertThat(fixture.kv.multiGet(Collections.singletonList(fixture.expectedKey)))
                .containsExactly(fixture.expectedValue);
        assertWriterState(fixture.log, 100L);
    }

    private static void assertWriterState(LogTablet logTablet, long sequence) {
        assertThat(
                        logTablet
                                .writerStateManager()
                                .lastFencedEntry(WRITER_KEY)
                                .orElseThrow(AssertionError::new))
                .satisfies(
                        state -> {
                            assertThat(state.lastSequence()).isEqualTo(sequence);
                            assertThat(state.dominatingTargetWalOffset()).isZero();
                        });
    }

    private void restartFromDurableWal(Fixture fixture) throws Exception {
        File dataDir = fixture.log.getDataDir();
        File logDir = fixture.log.getLogDir();
        org.apache.fluss.metadata.LogFormat logFormat = fixture.log.getLogFormat();
        fixture.kv.close();
        fixture.log.flush(true);
        fixture.log.close();
        fixture.log =
                LogTablet.create(
                        dataDir,
                        fixture.path,
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L,
                        new FlussScheduler(1),
                        logFormat,
                        1,
                        true,
                        SystemClock.getInstance(),
                        false,
                        KvIdempotenceProtocol.V1_FENCED);
        fixture.log.updateHighWatermark(fixture.log.localLogEndOffset());
        fixture.kv = createRecoveredKv(fixture);

        try (RemoteLogFetcher fetcher =
                new RemoteLogFetcher(
                        remoteLogManager, fixture.log.getTableBucket(), fixture.log.getLogDir())) {
            new KvRecoverHelper(
                            fixture.kv,
                            fixture.log,
                            0L,
                            0L,
                            null,
                            new KvRecoverHelper.KvRecoverContext(
                                    fixture.path.getTablePath(), zkClient, Integer.MAX_VALUE),
                            KvFormat.COMPACTED,
                            logFormat,
                            fixture.schemaGetter,
                            fetcher,
                            fixture.kv.getValueEncoder())
                    .recover();
        }
    }

    private KvTablet createRecoveredKv(Fixture fixture) throws Exception {
        TableConfig tableConfig = new TableConfig(new Configuration());
        RowMerger rowMerger =
                RowMerger.create(tableConfig, KvFormat.COMPACTED, fixture.schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        fixture.schemaGetter,
                        fixture.path.getTablePath(),
                        tableConfig,
                        new TestingSequenceGeneratorFactory());
        return KvTablet.create(
                fixture.path,
                fixture.log.getTableBucket(),
                fixture.log,
                new File(tempDir, "recovered-kv-" + fixture.log.getTableBucket().getTableId()),
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                new RootAllocator(Long.MAX_VALUE),
                new TestingMemorySegmentPool(10 * 1024),
                KvFormat.COMPACTED,
                rowMerger,
                DEFAULT_COMPRESSION,
                fixture.schemaGetter,
                tableConfig.getChangelogImage(),
                tableConfig.getKvFormatVersion().orElse(KV_FORMAT_VERSION_2),
                KvManager.getDefaultRateLimiter(),
                autoIncrementManager,
                null,
                null);
    }

    private Fixture createFixture(long tableId, String tableName) throws Exception {
        TablePath path = TablePath.of("test_db", tableName);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TestData.DATA1_SCHEMA_PK)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION, 1)
                        .build();
        zkClient.registerTable(
                path, TableRegistration.newTable(tableId, DEFAULT_REMOTE_DATA_DIR, descriptor));
        zkClient.registerFirstSchema(path, TestData.DATA1_SCHEMA_PK);
        long now = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(
                        path, tableId, SCHEMA_ID, descriptor, DEFAULT_REMOTE_DATA_DIR, now, now);
        TableBucket bucket = new TableBucket(tableId, 0);
        PhysicalTablePath physicalPath = PhysicalTablePath.of(path);
        Replica replica = makeKvReplica(physicalPath, bucket, tableInfo);
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        physicalPath,
                        bucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                INITIAL_LEADER_EPOCH,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                INITIAL_LEADER_EPOCH)));
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA_PK, SCHEMA_ID));
        BinaryRow expectedRow =
                compactedRow(TestData.DATA1_SCHEMA_PK.getRowType(), new Object[] {1, "value"});
        byte[] expectedKey =
                new CompactedKeyEncoder(TestData.DATA1_SCHEMA_PK.getRowType(), new int[] {0})
                        .encodeKey(expectedRow);
        return new Fixture(
                physicalPath,
                replica.getLogTablet(),
                replica.getKvTablet(),
                schemaGetter,
                expectedKey,
                ValueEncoder.encodeValue(SCHEMA_ID, expectedRow));
    }

    private static KvRecordBatch mutation(WriterKey key, long sequence, String value)
            throws Exception {
        FencedKvRecordBatchBuilder builder =
                FencedKvRecordBatchBuilder.builder(
                        SCHEMA_ID, 1024, new UnmanagedPagedOutputView(128), KvFormat.COMPACTED);
        BinaryRow row =
                compactedRow(TestData.DATA1_SCHEMA_PK.getRowType(), new Object[] {1, value});
        builder.append(
                new CompactedKeyEncoder(TestData.DATA1_SCHEMA_PK.getRowType(), new int[] {0})
                        .encodeKey(row),
                row);
        builder.setWriterState(key, sequence);
        return KvRecordBatchReader.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
    }

    private final class Fixture {
        private final PhysicalTablePath path;
        private LogTablet log;
        private KvTablet kv;
        private final TestingSchemaGetter schemaGetter;
        private final byte[] expectedKey;
        private final byte[] expectedValue;

        private Fixture(
                PhysicalTablePath path,
                LogTablet log,
                KvTablet kv,
                TestingSchemaGetter schemaGetter,
                byte[] expectedKey,
                byte[] expectedValue) {
            this.path = path;
            this.log = log;
            this.kv = kv;
            this.schemaGetter = schemaGetter;
            this.expectedKey = expectedKey;
            this.expectedValue = expectedValue;
        }

        private LogAppendInfo put(KvRecordBatch records) throws Exception {
            return kv.putAsLeader(records, null, org.apache.fluss.rpc.protocol.MergeMode.OVERWRITE);
        }
    }

    private static final class TestAppendException extends Exception {}

    private static final class TestBuildException extends RuntimeException {}
}
