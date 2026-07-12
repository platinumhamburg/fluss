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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsCompactedBuilder;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.index.IndexTableDescriptorFactory;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogTieringTask} max upload segments per task limit. */
class RemoteLogMaxUploadSegmentsTest extends RemoteLogTestBase {

    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE, MemorySize.parse("10b"));
        // Use default value (5) for REMOTE_LOG_TASK_MAX_UPLOAD_SEGMENTS.
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMaxUploadSegmentsPerTaskLimit(boolean partitionTable) throws Exception {
        // Default maxUploadSegmentsPerTask is 5, so with 10 segments (9 candidates),
        // only 5 should be uploaded per task execution.
        TableBucket tb = makeTableBucket(partitionTable);
        makeLogTableAsLeader(tb, partitionTable);
        addMultiSegmentsToLogTablet(replicaManager.getReplicaOrException(tb).getLogTablet(), 10);
        // 10 segments total, 9 candidates (1 active segment excluded).

        // First tiering task execution - should upload only 5 segments.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        List<RemoteLogSegment> manifestSegments = remoteLog.allRemoteLogSegments();
        assertThat(manifestSegments).hasSize(5);
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(50L);

        // Second tiering task execution - should upload the remaining 4 segments.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        manifestSegments = remoteLog.allRemoteLogSegments();
        assertThat(manifestSegments).hasSize(9);
        assertThat(remoteLog.getRemoteLogEndOffset()).hasValue(90L);
        // Verify remote storage has all 9 segment files.
        assertThat(listRemoteLogFiles(tb))
                .isEqualTo(
                        manifestSegments.stream()
                                .map(s -> s.remoteLogSegmentId().toString())
                                .collect(Collectors.toSet()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testV1TieringRequiresExactEndWriterSnapshot(boolean retry) throws Exception {
        TableBucket tb = makeIndexTableAsLeader(9910L);
        LogTablet log = replicaManager.getReplicaOrException(tb).getLogTablet();
        addFencedSegments(log, 3);
        long localStartBefore = log.localLogStartOffset();
        long firstClosedEnd = log.getSegments().get(1).getBaseOffset();
        assertThat(FlussPaths.writerSnapshotFile(log.getLogDir(), firstClosedEnd).delete()).isTrue();

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        if (retry) {
            remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        }

        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLog.getRemoteLogEndOffset()).isEmpty();
        assertThat(log.localLogStartOffset()).isEqualTo(localStartBefore);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testV1TieringFailureDoesNotPublishPartialSegments(boolean manifestFailure)
            throws Exception {
        TableBucket tb = makeIndexTableAsLeader(manifestFailure ? 9911L : 9912L);
        LogTablet log = replicaManager.getReplicaOrException(tb).getLogTablet();
        addFencedSegments(log, 4);
        long localStartBefore = log.localLogStartOffset();
        if (manifestFailure) {
            remoteLogStorage.writeManifestFail.set(true);
        } else {
            remoteLogStorage.copySegmentFailAfterNCopies.set(1);
        }

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);
        assertThat(remoteLog.allRemoteLogSegments()).isEmpty();
        assertThat(remoteLog.getRemoteLogEndOffset()).isEmpty();
        assertThat(log.localLogStartOffset()).isEqualTo(localStartBefore);
    }

    private TableBucket makeTableBucket(boolean partitionTable) {
        if (partitionTable) {
            return new TableBucket(DATA1_TABLE_ID, 0L, 0);
        } else {
            return new TableBucket(DATA1_TABLE_ID, 0);
        }
    }

    private TableBucket makeIndexTableAsLeader(long tableId) throws Exception {
        TablePath mainPath = TablePath.of("test_db", "tiering_main_" + tableId);
        TablePath indexPath = TablePath.of("test_db", "tiering_index_" + tableId);
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("indexed", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index(
                                "idx",
                                IndexType.SECONDARY,
                                java.util.Collections.singletonList("indexed"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        TableDescriptor mainDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(1, "id").build();
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(
                        mainDescriptor, tableId - 1, mainPath.toString(), "idx");
        zkClient.registerTable(
                indexPath,
                TableRegistration.newTable(tableId, DEFAULT_REMOTE_DATA_DIR, indexDescriptor));
        zkClient.registerFirstSchema(indexPath, indexDescriptor.getSchema());
        TableBucket tb = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tb, indexPath, INITIAL_LEADER_EPOCH, false);
        return tb;
    }

    private static void addFencedSegments(LogTablet log, int segmentCount) throws Exception {
        WriterKey writerKey = new WriterKey(0L, 0L);
        for (int i = 0; i < segmentCount; i++) {
            MemoryLogRecordsCompactedBuilder builder =
                    MemoryLogRecordsCompactedBuilder.fencedBuilder(
                            1,
                            1024,
                            new UnmanagedPagedOutputView(128),
                            false);
            builder.setFencedWriterState(writerKey, 100L + i * 100L);
            builder.close();
            log.appendAsLeader(
                    MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer()));
            if (i != segmentCount - 1) {
                log.roll(Optional.empty());
            }
        }
        log.updateHighWatermark(log.localLogEndOffset());
    }
}
