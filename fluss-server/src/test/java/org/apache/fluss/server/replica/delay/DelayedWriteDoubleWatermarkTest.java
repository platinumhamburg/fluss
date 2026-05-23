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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.replica.delay.DelayedWrite.DelayedBucketStatus;
import org.apache.fluss.server.replica.delay.DelayedWrite.DelayedWriteMetadata;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests covering the double-watermark gating in {@link DelayedWrite#tryComplete()}: completion
 * requires BOTH HW and the optional {@code requiredIndexOffset} to be met. A {@code
 * requiredIndexOffset} of {@link DelayedBucketStatus#NO_INDEX_OFFSET_REQUIRED} preserves the legacy
 * HW-only semantics.
 */
final class DelayedWriteDoubleWatermarkTest extends ReplicaTestBase {

    private static final int DELAY_MS = 10_000;
    private static final long REQUIRED_HW = 10L;

    @Test
    void testCompletesWhenBothHwAndIndexOffsetMet() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        AtomicReference<List<ProduceLogResultForBucket>> callbackResult = new AtomicReference<>();
        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        REQUIRED_HW,
                        /* requiredIndexOffset */ 7L,
                        DELAY_MS,
                        callbackResult::set);

        // HW met but index offset not yet met: must NOT complete.
        replica.getLogTablet().updateHighWatermark(REQUIRED_HW);
        assertThat(delayedWrite.tryComplete()).isFalse();
        assertThat(callbackResult.get()).isNull();

        // Advance index offset to the requirement: tryComplete must now succeed.
        replica.advanceIndexPushedOffset(7L);
        assertThat(delayedWrite.tryComplete()).isTrue();
        assertThat(callbackResult.get()).hasSize(1);
        assertThat(callbackResult.get().get(0).getErrorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void testStallsWhenHwMetButIndexOffsetNotMet() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        AtomicReference<List<ProduceLogResultForBucket>> callbackResult = new AtomicReference<>();
        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        REQUIRED_HW,
                        /* requiredIndexOffset */ 5L,
                        DELAY_MS,
                        callbackResult::set);

        // HW satisfied, index offset still at the sentinel (-1L): stall.
        replica.getLogTablet().updateHighWatermark(REQUIRED_HW);
        assertThat(replica.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(delayedWrite.tryComplete()).isFalse();
        assertThat(callbackResult.get()).isNull();

        // Calling tryComplete again must remain false until the index offset advances —
        // a stall on the index watermark must not flip acksPending behind the scenes.
        assertThat(delayedWrite.tryComplete()).isFalse();
        assertThat(callbackResult.get()).isNull();
    }

    @Test
    void testCompletesImmediatelyWhenRequiredIndexOffsetIsMinusOne() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        AtomicReference<List<ProduceLogResultForBucket>> callbackResult = new AtomicReference<>();
        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        REQUIRED_HW,
                        DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED,
                        DELAY_MS,
                        callbackResult::set);

        // Legacy single-watermark behavior: completing on HW alone, no index check.
        assertThat(replica.getIndexPushedOffset()).isEqualTo(-1L);
        replica.getLogTablet().updateHighWatermark(REQUIRED_HW);
        assertThat(delayedWrite.tryComplete()).isTrue();
        assertThat(callbackResult.get()).hasSize(1);
        assertThat(callbackResult.get().get(0).getErrorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void testNoLongerStallsAfterIndexOffsetAdvances() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        AtomicReference<List<ProduceLogResultForBucket>> callbackResult = new AtomicReference<>();
        DelayedWrite<?> delayedWrite =
                createDelayedWrite(
                        replica,
                        tb,
                        REQUIRED_HW,
                        /* requiredIndexOffset */ 9L,
                        DELAY_MS,
                        callbackResult::set);

        // HW satisfied; index offset still lagging.
        replica.getLogTablet().updateHighWatermark(REQUIRED_HW);
        replica.advanceIndexPushedOffset(8L);
        assertThat(delayedWrite.tryComplete()).isFalse();
        assertThat(callbackResult.get()).isNull();

        // Advance index offset to required value; second tryComplete must succeed.
        replica.advanceIndexPushedOffset(9L);
        assertThat(delayedWrite.tryComplete()).isTrue();
        assertThat(callbackResult.get()).hasSize(1);
    }

    @Test
    void testErrorPathCompletesEvenWithIndexLag() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        Replica replica = replicaManager.getReplicaOrException(tb);

        // Use a TableBucket that does NOT exist in the replicaManager so that
        // checkEnoughReplicasReachOffset throws and bubbles back as an error in the result tuple.
        TableBucket bogusBucket = new TableBucket(DATA1_TABLE_ID, 999);

        AtomicReference<List<ProduceLogResultForBucket>> callbackResult = new AtomicReference<>();
        ProduceLogResultForBucket appendResult = new ProduceLogResultForBucket(bogusBucket, 0L, 0L);
        Map<TableBucket, DelayedBucketStatus<ProduceLogResultForBucket>> bucketStatusMap =
                Collections.singletonMap(
                        bogusBucket,
                        new DelayedBucketStatus<>(
                                REQUIRED_HW, /* requiredIndexOffset */ 100L, appendResult));
        DelayedWrite<ProduceLogResultForBucket> delayedWrite =
                new DelayedWrite<>(
                        DELAY_MS,
                        new DelayedWriteMetadata<>(-1, bucketStatusMap),
                        replicaManager,
                        callbackResult::set,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

        // Replica lookup throws -> error path -> completes immediately despite index lag.
        // Note: replica is irrelevant here, so index-offset state cannot have advanced.
        assertThat(replica.getIndexPushedOffset()).isEqualTo(-1L);
        assertThat(delayedWrite.tryComplete()).isTrue();
        assertThat(callbackResult.get()).hasSize(1);
        assertThat(callbackResult.get().get(0).getErrorCode()).isNotEqualTo(Errors.NONE.code());
    }

    private DelayedWrite<ProduceLogResultForBucket> createDelayedWrite(
            Replica replica,
            TableBucket tb,
            long requiredOffset,
            long requiredIndexOffset,
            int delayMs,
            Consumer<List<ProduceLogResultForBucket>> callback)
            throws Exception {
        LogAppendInfo appendInfo =
                replica.getLogTablet().appendAsLeader(genMemoryLogRecordsByObject(DATA1));
        ProduceLogResultForBucket appendResult =
                new ProduceLogResultForBucket(
                        tb, appendInfo.firstOffset(), appendInfo.lastOffset() + 1);
        Map<TableBucket, DelayedBucketStatus<ProduceLogResultForBucket>> bucketStatusMap =
                Collections.singletonMap(
                        tb,
                        new DelayedBucketStatus<>(
                                requiredOffset, requiredIndexOffset, appendResult));

        return new DelayedWrite<>(
                delayMs,
                new DelayedWriteMetadata<>(-1, bucketStatusMap),
                replicaManager,
                callback,
                TestingMetricGroups.TABLET_SERVER_METRICS);
    }
}
