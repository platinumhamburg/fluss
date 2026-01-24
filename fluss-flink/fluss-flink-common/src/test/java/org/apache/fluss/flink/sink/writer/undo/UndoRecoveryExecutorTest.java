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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link UndoRecoveryExecutor}.
 *
 * <p>Tests verify: (1) streaming execution with futures, (2) multi-bucket recovery, (3) exception
 * propagation.
 */
class UndoRecoveryExecutorTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    private static final List<String> PRIMARY_KEY_COLUMNS = List.of("f0");
    private static final long TABLE_ID = 1L;

    private KeyEncoder keyEncoder;
    private MockUpsertWriter mockWriter;
    private MockLogScanner mockScanner;
    private UndoComputer undoComputer;
    private UndoRecoveryExecutor executor;

    @BeforeEach
    void setUp() {
        keyEncoder = KeyEncoder.of(ROW_TYPE, PRIMARY_KEY_COLUMNS, null);
        mockWriter = new MockUpsertWriter();
        mockScanner = new MockLogScanner();
        undoComputer = new UndoComputer(keyEncoder, mockWriter);
        executor = new UndoRecoveryExecutor(mockScanner, mockWriter, undoComputer);
    }

    /**
     * Test multi-bucket recovery with mixed ChangeTypes and key deduplication.
     *
     * <p>Validates: Requirements 3.3 - All futures complete after execute.
     */
    @Test
    void testMultiBucketRecoveryWithDeduplication() throws Exception {
        TableBucket bucket0 = new TableBucket(TABLE_ID, 0);
        TableBucket bucket1 = new TableBucket(TABLE_ID, 1);

        BucketRecoveryContext ctx0 = new BucketRecoveryContext(bucket0, 0L);
        ctx0.setTargetOffset(4L);

        BucketRecoveryContext ctx1 = new BucketRecoveryContext(bucket1, 0L);
        ctx1.setTargetOffset(3L);

        // Bucket0: INSERT(key=1), UPDATE_BEFORE(key=1, dup), INSERT(key=2), DELETE(key=3)
        mockScanner.setRecordsForBucket(
                bucket0,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.UPDATE_BEFORE, row(1, "b", 200)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(2, "c", 300)),
                        new ScanRecord(3L, 0L, ChangeType.DELETE, row(3, "d", 400))));

        // Bucket1: DELETE(key=10), UPDATE_AFTER(key=11, skip), INSERT(key=12)
        mockScanner.setRecordsForBucket(
                bucket1,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.DELETE, row(10, "e", 500)),
                        new ScanRecord(1L, 0L, ChangeType.UPDATE_AFTER, row(11, "f", 600)),
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row(12, "g", 700))));

        executor.execute(Arrays.asList(ctx0, ctx1));

        // Bucket0: 3 unique keys (key=1 deduplicated)
        // - key=1: INSERT → delete
        // - key=2: INSERT → delete
        // - key=3: DELETE → upsert
        assertThat(ctx0.getProcessedKeys()).hasSize(3);

        // Bucket1: 2 unique keys (UPDATE_AFTER skipped)
        // - key=10: DELETE → upsert
        // - key=12: INSERT → delete
        assertThat(ctx1.getProcessedKeys()).hasSize(2);

        // Total: 3 deletes (key=1,2,12), 2 upserts (key=3,10)
        assertThat(mockWriter.deleteCount).isEqualTo(3);
        assertThat(mockWriter.upsertCount).isEqualTo(2);
        assertThat(mockWriter.flushCalled).isTrue();

        assertThat(ctx0.isComplete()).isTrue();
        assertThat(ctx1.isComplete()).isTrue();
    }

    /**
     * Test that recovery is skipped when checkpoint offset >= target offset.
     *
     * <p>Validates: No unnecessary work when no recovery needed.
     */
    @Test
    void testNoRecoveryNeededSkipsExecution() throws Exception {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        // Checkpoint offset (5) >= target offset (5) → no recovery needed
        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 5L);
        ctx.setTargetOffset(5L);

        executor.execute(Collections.singletonList(ctx));

        assertThat(mockWriter.deleteCount).isEqualTo(0);
        assertThat(mockWriter.upsertCount).isEqualTo(0);
        assertThat(mockWriter.flushCalled).isFalse();
    }

    /**
     * Test exception propagation from writer failures.
     *
     * <p>Validates: Requirements 7.1, 7.2 - Exception propagation.
     */
    @Test
    void testExceptionPropagationFromWriter() {
        TableBucket bucket = new TableBucket(TABLE_ID, 0);

        BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, 0L);
        ctx.setTargetOffset(2L);

        mockScanner.setRecordsForBucket(
                bucket,
                Arrays.asList(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row(1, "a", 100)),
                        new ScanRecord(1L, 0L, ChangeType.INSERT, row(2, "b", 200))));

        mockWriter.shouldFail = true;

        assertThatThrownBy(() -> executor.execute(Collections.singletonList(ctx)))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated write failure");
    }
}
