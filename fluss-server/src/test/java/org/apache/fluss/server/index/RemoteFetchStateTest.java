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

package org.apache.fluss.server.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RemoteFetchState}. */
class RemoteFetchStateTest {

    @Test
    void testInitialState() {
        RemoteFetchState state = new RemoteFetchState();

        assertThat(state.getCommitOffset()).isEqualTo(0L);
        assertThat(state.getLeaderServerId()).isEqualTo(-1);
    }

    @Test
    void testUpdateCommitOffset() {
        RemoteFetchState state = new RemoteFetchState();

        state.updateCommitOffset(100L, 1);

        assertThat(state.getCommitOffset()).isEqualTo(100L);
        assertThat(state.getLeaderServerId()).isEqualTo(1);
    }

    @Test
    void testMultipleCommitOffsetUpdates() {
        RemoteFetchState state = new RemoteFetchState();

        state.updateCommitOffset(10L, 1);
        assertThat(state.getCommitOffset()).isEqualTo(10L);
        assertThat(state.getLeaderServerId()).isEqualTo(1);

        state.updateCommitOffset(20L, 2);
        assertThat(state.getCommitOffset()).isEqualTo(20L);
        assertThat(state.getLeaderServerId()).isEqualTo(2);

        state.updateCommitOffset(30L, 1);
        assertThat(state.getCommitOffset()).isEqualTo(30L);
        assertThat(state.getLeaderServerId()).isEqualTo(1);
    }

    @Test
    void testCommitOffsetCanDecrease() {
        RemoteFetchState state = new RemoteFetchState();

        state.updateCommitOffset(100L, 1);
        assertThat(state.getCommitOffset()).isEqualTo(100L);

        state.updateCommitOffset(50L, 2);
        assertThat(state.getCommitOffset()).isEqualTo(50L);
    }

    @Test
    void testRecordFetchOffset() {
        RemoteFetchState state = new RemoteFetchState();

        state.recordFetchOffset(500L);

        String diagnostic = state.getDiagnosticString(1000L, 0);
        assertThat(diagnostic).contains("lastFetchOffset=500");
    }

    @Test
    void testMultipleFetchOffsetRecordings() {
        RemoteFetchState state = new RemoteFetchState();

        state.recordFetchOffset(100L);
        state.recordFetchOffset(200L);
        state.recordFetchOffset(300L);

        String diagnostic = state.getDiagnosticString(1000L, 0);
        assertThat(diagnostic).contains("lastFetchOffset=300");
    }

    @Test
    void testLargeOffsetValues() {
        RemoteFetchState state = new RemoteFetchState();

        long largeOffset = Long.MAX_VALUE - 1;
        state.updateCommitOffset(largeOffset, Integer.MAX_VALUE);
        state.recordFetchOffset(Long.MAX_VALUE);

        assertThat(state.getCommitOffset()).isEqualTo(largeOffset);
        assertThat(state.getLeaderServerId()).isEqualTo(Integer.MAX_VALUE);
    }
}
