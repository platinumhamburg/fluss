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

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.record.LogRecordBatch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KvRecoverHelperTest {

    @Test
    void testRecoveryBatchMustStartAtExpectedOffset() {
        LogRecordBatch batch = batch(5L, 6L);

        assertThatThrownBy(() -> KvRecoverHelper.validateRecoveryBatch(batch, 4L, 10L))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("expected offset 4")
                .hasMessageContaining("starts at 5");
        verify(batch).ensureValid();
    }

    @Test
    void testRemoteRecoveryBatchCannotCrossLocalWalBoundary() {
        LogRecordBatch batch = batch(4L, 11L);

        assertThatThrownBy(() -> KvRecoverHelper.validateRecoveryBatch(batch, 4L, 10L))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("crosses local WAL start offset 10");
    }

    @Test
    void testRecoveryBatchMustAdvanceOffset() {
        LogRecordBatch batch = batch(4L, 4L);

        assertThatThrownBy(() -> KvRecoverHelper.validateRecoveryBatch(batch, 4L, Long.MAX_VALUE))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("does not advance");
    }

    @Test
    void testRecoveryValidatesChecksumBeforeOffsets() {
        LogRecordBatch batch = batch(4L, 5L);
        doThrow(new CorruptMessageException("bad crc")).when(batch).ensureValid();

        assertThatThrownBy(() -> KvRecoverHelper.validateRecoveryBatch(batch, 4L, Long.MAX_VALUE))
                .isInstanceOf(CorruptMessageException.class)
                .hasMessageContaining("bad crc");
    }

    @Test
    void testEmptyRemoteFetchCannotSpinWithoutProgress() {
        assertThatThrownBy(() -> KvRecoverHelper.requireRemoteFetchProgress(4L, 4L, 10L))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("made no progress")
                .hasMessageContaining("offset 4");
    }

    @Test
    void testEmptyLocalFetchIsAllowedOnlyAfterReachingTarget() {
        assertThatThrownBy(() -> KvRecoverHelper.requireLocalFetchProgress(4L, 4L, 10L))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("Local WAL fetch made no progress")
                .hasMessageContaining("offset 4")
                .hasMessageContaining("target offset 10");

        assertThatCode(() -> KvRecoverHelper.requireLocalFetchProgress(10L, 10L, 10L))
                .doesNotThrowAnyException();
    }

    private static LogRecordBatch batch(long baseOffset, long nextOffset) {
        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(baseOffset);
        when(batch.nextLogOffset()).thenReturn(nextOffset);
        return batch;
    }
}
