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
import org.apache.fluss.exception.NotLeaderOrFollowerException;
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
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
        long errorTruncationsBefore =
                fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount();
        fixture.kv.setBeforeWalBuild(
                () -> {
                    throw new TestBuildException();
                });

        assertThatThrownBy(() -> fixture.putDirect(mutation))
                .isInstanceOf(TestBuildException.class);
        assertThat(fixture.kv.getKvPreWriteBuffer().getAllKvEntries()).isEmpty();
        assertThat(fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount())
                .isEqualTo(errorTruncationsBefore + 1L);
        assertThat(fixture.log.localLogEndOffset()).isZero();

        fixture.kv.setBeforeWalBuild(null);
        fixture.putDirect(mutation);
        assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
        assertWriterState(fixture.log, 100L);
    }

    @ParameterizedTest(name = "phase={0}, wal={1}")
    @MethodSource("restartCases")
    void testReplicaFailStopAndRestartConvergence(FaultPhase phase, WalOutcome walOutcome)
            throws Exception {
        Fixture fixture =
                createFixture(
                        8200L + phase.ordinal() * 10L + walOutcome.ordinal(),
                        "phase_" + phase.name() + "_" + walOutcome.name());
        KvRecordBatch mutation = mutation(WRITER_KEY, 100L, "value");
        AtomicInteger fatalErrors = installCountingFatalHandler(fixture.replica);
        long errorTruncationsBefore =
                fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount();
        LogTabletTestHelper.failOnceAt(fixture.log, phase, new TestAppendException());

        try {
            assertThatThrownBy(() -> fixture.putThroughReplica(mutation))
                    .isInstanceOf(UncertainWalAppendException.class)
                    .hasCauseInstanceOf(TestAppendException.class);
            assertThat(fixture.kv.getKvPreWriteBuffer().getTruncateAsErrorCount().getCount())
                    .isEqualTo(errorTruncationsBefore);
            assertThat(fixture.log.localLogEndOffset())
                    .isEqualTo(phase == FaultPhase.BEFORE_LOCAL_APPEND ? 0L : 1L);
            assertThat(fatalErrors).hasValue(1);
            assertThat(isReplicaOnline(fixture.replica)).isFalse();
            assertThat(fixture.replica.isLeader()).isFalse();
            assertThatThrownBy(() -> fixture.putThroughReplica(mutation))
                    .isInstanceOf(NotLeaderOrFollowerException.class);
            assertThatThrownBy(() -> fixture.putDirect(mutation))
                    .isInstanceOf(UncertainWalAppendException.class);
            assertThat(fixture.log.localLogEndOffset())
                    .isEqualTo(phase == FaultPhase.BEFORE_LOCAL_APPEND ? 0L : 1L);
            assertThat(fatalErrors).hasValue(1);

            restartFromDurableWal(fixture, walOutcome);
            LogAppendInfo retried = fixture.putDirect(mutation);
            assertThat(retried.duplicated()).isEqualTo(walOutcome == WalOutcome.RETAINED);
            assertThat(fixture.log.localLogEndOffset()).isEqualTo(1L);
            fixture.log.updateHighWatermark(1L);
            fixture.kv.flush(1L, NOPErrorHandler.INSTANCE);
            reopenDurableState(fixture);

            assertThat(fixture.kv.limitScan(Integer.MAX_VALUE))
                    .containsExactly(fixture.expectedValue);
            assertWriterState(fixture.log, 100L);
        } finally {
            fixture.closeReplacements();
        }
    }

    private static Stream<Arguments> restartCases() {
        return Stream.of(
                Arguments.of(FaultPhase.BEFORE_LOCAL_APPEND, WalOutcome.NO_WAL),
                Arguments.of(FaultPhase.AFTER_LOCAL_APPEND, WalOutcome.RETAINED),
                Arguments.of(FaultPhase.AFTER_LOCAL_APPEND, WalOutcome.LOST),
                Arguments.of(FaultPhase.AFTER_WRITER_STATE_UPDATE, WalOutcome.RETAINED),
                Arguments.of(FaultPhase.AFTER_WRITER_STATE_UPDATE, WalOutcome.LOST));
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

    private void restartFromDurableWal(Fixture fixture, WalOutcome walOutcome) throws Exception {
        fixture.dataDir = fixture.log.getDataDir();
        fixture.logDir = fixture.log.getLogDir();
        fixture.logFormat = fixture.log.getLogFormat();
        fixture.kv.close();
        if (walOutcome == WalOutcome.LOST) {
            LogTabletTestHelper.truncateTo(fixture.log, 0L);
        } else {
            fixture.log.flush(true);
        }
        fixture.log.close();
        fixture.replacementScheduler = new FlussScheduler(1);
        fixture.replacementScheduler.startup();
        fixture.log = createReplacementLog(fixture);
        fixture.log.updateHighWatermark(fixture.log.localLogEndOffset());
        fixture.recoveredKvDir =
                new File(tempDir, "recovered-kv-" + fixture.log.getTableBucket().getTableId());
        fixture.replacementAllocator = new RootAllocator(Long.MAX_VALUE);
        fixture.kv = createReplacementKv(fixture);

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
                            fixture.logFormat,
                            fixture.schemaGetter,
                            fetcher,
                            fixture.kv.getValueEncoder())
                    .recover();
        }
    }

    private void reopenDurableState(Fixture fixture) throws Exception {
        fixture.log.flush(true);
        fixture.kv.close();
        fixture.replacementAllocator.close();
        fixture.replacementAllocator = null;
        fixture.log.close();

        fixture.log = createReplacementLog(fixture);
        fixture.log.updateHighWatermark(fixture.log.localLogEndOffset());
        fixture.replacementAllocator = new RootAllocator(Long.MAX_VALUE);
        fixture.kv = createReplacementKv(fixture);
    }

    private LogTablet createReplacementLog(Fixture fixture) throws Exception {
        return LogTablet.create(
                fixture.dataDir,
                fixture.path,
                fixture.logDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0L,
                fixture.replacementScheduler,
                fixture.logFormat,
                1,
                true,
                SystemClock.getInstance(),
                false,
                KvIdempotenceProtocol.V1_FENCED);
    }

    private KvTablet createReplacementKv(Fixture fixture) throws Exception {
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
                fixture.recoveredKvDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                fixture.replacementAllocator,
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

    private static AtomicInteger installCountingFatalHandler(Replica replica) throws Exception {
        AtomicInteger fatalErrors = new AtomicInteger();
        Field field = Replica.class.getDeclaredField("fatalErrorHandler");
        field.setAccessible(true);
        field.set(replica, (FatalErrorHandler) ignored -> fatalErrors.incrementAndGet());
        return fatalErrors;
    }

    private static boolean isReplicaOnline(Replica replica) throws Exception {
        Field field = Replica.class.getDeclaredField("online");
        field.setAccessible(true);
        return ((AtomicBoolean) field.get(replica)).get();
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
        return new Fixture(
                physicalPath,
                replica,
                replica.getLogTablet(),
                replica.getKvTablet(),
                schemaGetter,
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
        private final Replica replica;
        private LogTablet log;
        private KvTablet kv;
        private final TestingSchemaGetter schemaGetter;
        private final byte[] expectedValue;
        private File dataDir;
        private File logDir;
        private File recoveredKvDir;
        private org.apache.fluss.metadata.LogFormat logFormat;
        private FlussScheduler replacementScheduler;
        private RootAllocator replacementAllocator;

        private Fixture(
                PhysicalTablePath path,
                Replica replica,
                LogTablet log,
                KvTablet kv,
                TestingSchemaGetter schemaGetter,
                byte[] expectedValue) {
            this.path = path;
            this.replica = replica;
            this.log = log;
            this.kv = kv;
            this.schemaGetter = schemaGetter;
            this.expectedValue = expectedValue;
        }

        private LogAppendInfo putDirect(KvRecordBatch records) throws Exception {
            return kv.putAsLeader(records, null, org.apache.fluss.rpc.protocol.MergeMode.OVERWRITE);
        }

        private LogAppendInfo putThroughReplica(KvRecordBatch records) throws Exception {
            return replica.putRecordsToLeader(
                    records, null, org.apache.fluss.rpc.protocol.MergeMode.OVERWRITE, -1);
        }

        private void closeReplacements() throws Exception {
            if (replacementScheduler == null) {
                return;
            }
            try {
                if (kv != null) {
                    kv.close();
                }
            } finally {
                try {
                    if (log != null) {
                        log.close();
                    }
                } finally {
                    try {
                        replacementScheduler.shutdown();
                    } finally {
                        if (replacementAllocator != null) {
                            replacementAllocator.close();
                        }
                    }
                }
            }
        }
    }

    private enum WalOutcome {
        NO_WAL,
        RETAINED,
        LOST
    }

    private static final class TestAppendException extends Exception {}

    private static final class TestBuildException extends RuntimeException {}
}
