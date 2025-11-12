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

package org.apache.fluss.server.log.state;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultStateChangeLogs;
import org.apache.fluss.record.DefaultStateChangeLogsBuilder;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.StateChangeLogs;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.StateDefs.DATA_BUCKET_OFFSET_OF_INDEX;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link BucketStateManager} recovery from cold start. */
class BucketStateManagerRecoveryITCase {

    private @TempDir File tempDir;
    private File logDir;
    private Configuration conf;
    private FlussScheduler scheduler;

    @BeforeEach
    void setup() throws Exception {
        logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        conf = new Configuration();
        // Set a small segment size to easily trigger segment roll
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, new MemorySize(1024));
        scheduler = new FlussScheduler(1);
        scheduler.startup();
    }

    @AfterEach
    void teardown() throws Exception {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void testBucketStateManagerRecoveryFromColdStart() throws Exception {
        // Step 1: Create LogTablet and write data with state change logs
        LogTablet logTablet =
                LogTablet.create(
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L,
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);

        // Write first batch with state change logs
        DefaultStateChangeLogsBuilder stateBuilder1 = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(DATA1_TABLE_ID, 0);
        TableBucket key2 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket key3 = new TableBucket(DATA1_TABLE_ID, 2);
        TableBucket key4 = new TableBucket(DATA1_TABLE_ID, 3);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        stateBuilder1.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key3, 300L);
        DefaultStateChangeLogs stateLogs1 = stateBuilder1.build();

        MemoryLogRecords records1 = createMemoryLogRecordsWithStateChangeLogs(0L, stateLogs1);

        logTablet.appendAsLeader(records1);

        // Update high watermark to commit the state
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // Get BucketStateManager and verify state was applied and committed
        BucketStateManager stateManager1 = logTablet.bucketStateManager();

        // Step 2: Trigger segment roll and snapshot
        logTablet.roll(Optional.empty());

        // Verify snapshot was created
        assertThat(stateManager1.latestSnapshotOffset()).isPresent();
        long firstSnapshotOffset = stateManager1.latestSnapshotOffset().get();

        // Verify state is accessible after commit
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(100L);
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(200L);
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true).getValue())
                .isEqualTo(300L);

        // Write second batch with more state changes
        DefaultStateChangeLogsBuilder stateBuilder2 = new DefaultStateChangeLogsBuilder();
        stateBuilder2.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key1, 1001L);
        stateBuilder2.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key4, 400L);
        DefaultStateChangeLogs stateLogs2 = stateBuilder2.build();

        MemoryLogRecords records2 =
                createMemoryLogRecordsWithStateChangeLogs(
                        logTablet.localLogEndOffset(), stateLogs2);
        logTablet.appendAsLeader(records2);

        // Update high watermark to commit the state and trigger another roll
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        logTablet.roll(Optional.empty());

        // Verify second snapshot was created
        assertThat(stateManager1.latestSnapshotOffset()).isPresent();
        long secondSnapshotOffset = stateManager1.latestSnapshotOffset().get();
        assertThat(secondSnapshotOffset).isGreaterThan(firstSnapshotOffset);

        // Verify updated state
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true).getValue())
                .isEqualTo(1001L);
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true).getValue())
                .isEqualTo(200L);
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true).getValue())
                .isEqualTo(300L);
        assertThat(stateManager1.getState(DATA_BUCKET_OFFSET_OF_INDEX, key4, true).getValue())
                .isEqualTo(400L);

        // Step 3: Close LogTablet (simulating shutdown)
        logTablet.flush(true);
        logTablet.close();

        // Step 4: Recreate LogTablet from disk (simulating cold start)
        LogTablet recoveredLogTablet =
                LogTablet.create(
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0L,
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);

        // Step 5: Verify BucketStateManager recovered state correctly
        BucketStateManager recoveredStateManager = recoveredLogTablet.bucketStateManager();

        // Verify the latest snapshot offset was restored
        assertThat(recoveredStateManager.latestSnapshotOffset()).isPresent();
        assertThat(recoveredStateManager.latestSnapshotOffset().get())
                .isEqualTo(secondSnapshotOffset);

        // Verify all state was recovered correctly
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key1, true)
                                .getValue())
                .isEqualTo(1001L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key2, true)
                                .getValue())
                .isEqualTo(200L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key3, true)
                                .getValue())
                .isEqualTo(300L);
        assertThat(
                        recoveredStateManager
                                .getState(DATA_BUCKET_OFFSET_OF_INDEX, key4, true)
                                .getValue())
                .isEqualTo(400L);

        // Verify that the committed offset was restored
        assertThat(recoveredStateManager.lastCommittedOffset()).isEqualTo(secondSnapshotOffset);

        // Cleanup
        recoveredLogTablet.close();
    }

    private MemoryLogRecords createMemoryLogRecordsWithStateChangeLogs(
            long baseOffset, StateChangeLogs stateChangeLogs) throws Exception {
        // Create a simple log record with state change logs attached
        List<ChangeType> changeTypes = new ArrayList<>();
        changeTypes.add(ChangeType.APPEND_ONLY);

        List<InternalRow> rows = new ArrayList<>();
        rows.add(row(1, "test-data"));

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            1L,
                            DEFAULT_SCHEMA_ID,
                            Integer.MAX_VALUE,
                            DATA1_ROW_TYPE,
                            DEFAULT_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            LOG_MAGIC_VALUE_V3,
                            DEFAULT_SCHEMA_ID,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)),
                            stateChangeLogs);

            for (int i = 0; i < changeTypes.size(); i++) {
                builder.append(changeTypes.get(i), rows.get(i));
            }
            builder.setWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE);
            builder.close();

            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
            memoryLogRecords.ensureValid(LOG_MAGIC_VALUE_V3);
            return memoryLogRecords;
        }
    }
}
