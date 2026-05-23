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

package org.apache.fluss.server.replica;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IndexPushedOffsetState}. */
class IndexPushedOffsetStateTest {

    private IndexPushedOffsetState state;

    @BeforeEach
    void setUp() {
        state = new IndexPushedOffsetState();
    }

    @Test
    void testInitialOffsetIsMinusOne() {
        assertThat(state.get()).isEqualTo(-1L);
    }

    @Test
    void testAdvanceIsMonotonic() {
        state.advance(5L);
        assertThat(state.get()).isEqualTo(5L);

        // stale advance is silently dropped.
        state.advance(3L);
        assertThat(state.get()).isEqualTo(5L);

        // equal advance is also dropped (strict increase required).
        state.advance(5L);
        assertThat(state.get()).isEqualTo(5L);

        state.advance(10L);
        assertThat(state.get()).isEqualTo(10L);
    }

    @Test
    void testAdvanceFiresCallbackOnStrictIncrease() {
        AtomicInteger calls = new AtomicInteger();
        state.setOnAdvanced(calls::incrementAndGet);

        state.advance(5L);
        state.advance(5L); // no strict increase, no callback.
        state.advance(3L); // stale, no callback.
        state.advance(7L);

        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void testSeedDoesNotFireCallback() {
        AtomicInteger calls = new AtomicInteger();
        state.setOnAdvanced(calls::incrementAndGet);

        state.seed(5L);
        assertThat(calls.get()).isZero();
        assertThat(state.get()).isEqualTo(5L);

        // seed is also monotonic.
        state.seed(3L);
        assertThat(state.get()).isEqualTo(5L);

        state.seed(10L);
        assertThat(state.get()).isEqualTo(10L);
        assertThat(calls.get()).isZero();
    }

    @Test
    void testNullCallbackNormalizesToNoOp() {
        state.setOnAdvanced(null);
        // should not throw.
        state.advance(5L);
        assertThat(state.get()).isEqualTo(5L);
    }
}
