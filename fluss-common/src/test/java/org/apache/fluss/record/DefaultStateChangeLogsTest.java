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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.StateDefs.DATA_BUCKET_OFFSET_OF_INDEX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultStateChangeLogs}. */
class DefaultStateChangeLogsTest {

    @Test
    void testEmptyStateLogs() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        DefaultStateChangeLogs logs = builder.build();

        assertThat(logs.sizeInBytes()).isGreaterThan(0);
        Iterator<StateChangeLog> iterator = logs.iters();
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testSingleStateLog() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        DefaultStateChangeLogs logs = builder.build();

        assertThat(logs.sizeInBytes()).isGreaterThan(0);

        Iterator<StateChangeLog> iterator = logs.iters();
        assertThat(iterator.hasNext()).isTrue();

        StateChangeLog log = iterator.next();
        assertThat(log.getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(log.getStateDef()).isEqualTo(DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(log.getKey()).isEqualTo(key1);
        assertThat(log.getValue()).isEqualTo(100L);

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testMultipleStateLogs() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        TableBucket key3 = new TableBucket(1L, 2);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        builder.addLog(ChangeType.DELETE, StateDefs.DATA_BUCKET_OFFSET_OF_INDEX, key3, null);
        DefaultStateChangeLogs logs = builder.build();

        List<StateChangeLog> logList = new ArrayList<>();
        logs.iters().forEachRemaining(logList::add);

        assertThat(logList).hasSize(3);

        // Check first log
        assertThat(logList.get(0).getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(logList.get(0).getStateDef()).isEqualTo(DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logList.get(0).getKey()).isEqualTo(key1);
        assertThat(logList.get(0).getValue()).isEqualTo(100L);

        // Check second log
        assertThat(logList.get(1).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logList.get(1).getStateDef()).isEqualTo(DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logList.get(1).getKey()).isEqualTo(key2);
        assertThat(logList.get(1).getValue()).isEqualTo(200L);

        // Check third log
        assertThat(logList.get(2).getChangeType()).isEqualTo(ChangeType.DELETE);
        assertThat(logList.get(2).getStateDef()).isEqualTo(StateDefs.DATA_BUCKET_OFFSET_OF_INDEX);
        assertThat(logList.get(2).getKey()).isEqualTo(key3);
        assertThat(logList.get(2).getValue()).isNull();
    }

    @Test
    void testZeroCopyPointTo() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        DefaultStateChangeLogs logs = builder.build();

        // Test that multiple iters() calls work correctly with zero-copy
        List<StateChangeLog> logList1 = new ArrayList<>();
        logs.iters().forEachRemaining(logList1::add);

        List<StateChangeLog> logList2 = new ArrayList<>();
        logs.iters().forEachRemaining(logList2::add);

        assertThat(logList1).hasSize(1);
        assertThat(logList2).hasSize(1);
        assertThat(logList1.get(0).getKey()).isEqualTo(key1);
        assertThat(logList2.get(0).getKey()).isEqualTo(key1);
    }

    @Test
    void testNullValues() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        builder.addLog(null, null, null, null);
        DefaultStateChangeLogs logs = builder.build();

        Iterator<StateChangeLog> iterator = logs.iters();
        assertThat(iterator.hasNext()).isTrue();

        StateChangeLog log = iterator.next();
        assertThat(log.getChangeType()).isNull();
        assertThat(log.getStateDef()).isNull();
        assertThat(log.getKey()).isNull();
        assertThat(log.getValue()).isNull();
    }

    @Test
    void testBuilderReset() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);

        int sizeBeforeReset = builder.sizeInBytes();
        assertThat(builder.getRowCount()).isEqualTo(1);

        builder.reset();

        assertThat(builder.getRowCount()).isEqualTo(0);
        assertThat(builder.sizeInBytes()).isLessThan(sizeBeforeReset);

        TableBucket key2 = new TableBucket(1L, 1);
        builder.addLog(ChangeType.DELETE, DATA_BUCKET_OFFSET_OF_INDEX, key2, null);
        DefaultStateChangeLogs logs = builder.build();

        List<StateChangeLog> logList = new ArrayList<>();
        logs.iters().forEachRemaining(logList::add);

        assertThat(logList).hasSize(1);
        assertThat(logList.get(0).getChangeType()).isEqualTo(ChangeType.DELETE);
    }

    @Test
    void testAllChangeTypes() {
        DefaultStateChangeLogsBuilder builder = new DefaultStateChangeLogsBuilder();
        TableBucket key1 = new TableBucket(1L, 0);
        TableBucket key2 = new TableBucket(1L, 1);
        TableBucket key3 = new TableBucket(1L, 2);
        TableBucket key4 = new TableBucket(1L, 3);
        TableBucket key5 = new TableBucket(1L, 4);
        builder.addLog(ChangeType.APPEND_ONLY, DATA_BUCKET_OFFSET_OF_INDEX, key1, 100L);
        builder.addLog(ChangeType.INSERT, DATA_BUCKET_OFFSET_OF_INDEX, key2, 200L);
        builder.addLog(ChangeType.UPDATE_BEFORE, DATA_BUCKET_OFFSET_OF_INDEX, key3, 300L);
        builder.addLog(ChangeType.UPDATE_AFTER, DATA_BUCKET_OFFSET_OF_INDEX, key4, 400L);
        builder.addLog(ChangeType.DELETE, DATA_BUCKET_OFFSET_OF_INDEX, key5, null);

        DefaultStateChangeLogs logs = builder.build();

        List<StateChangeLog> logList = new ArrayList<>();
        logs.iters().forEachRemaining(logList::add);

        assertThat(logList).hasSize(5);
        assertThat(logList.get(0).getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(logList.get(1).getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(logList.get(2).getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
        assertThat(logList.get(3).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(logList.get(4).getChangeType()).isEqualTo(ChangeType.DELETE);
    }

    @Test
    void testUnsupportedVersion() {
        // Create a memory segment with an unsupported version
        byte[] data = new byte[10];
        MemorySegment segment = MemorySegment.wrap(data);

        // Write an unsupported version (e.g., version 99)
        segment.put(0, (byte) 99);
        // Write row count
        segment.putInt(1, 0);

        DefaultStateChangeLogs logs = new DefaultStateChangeLogs();

        // Should throw IllegalArgumentException for unsupported version
        try {
            logs.pointTo(segment, 0, 10);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Unsupported state change logs version: 99");
        }
    }
}
