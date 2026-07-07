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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueEquality}. */
class ValueEqualityTest {

    @Test
    void testByteArraysCompareByContent() {
        assertThat(ValueEquality.contentEquals(new byte[] {1, 2, 3}, new byte[] {1, 2, 3}))
                .isTrue();
        assertThat(ValueEquality.contentEquals(new byte[] {1, 2, 3}, new byte[] {1, 2, 4}))
                .isFalse();
    }

    @Test
    void testNonByteArraysUseObjectEquality() {
        assertThat(ValueEquality.contentEquals("alice", "alice")).isTrue();
        assertThat(ValueEquality.contentEquals("alice", "bob")).isFalse();
        assertThat(ValueEquality.contentEquals(null, null)).isTrue();
        assertThat(ValueEquality.contentEquals(null, "alice")).isFalse();
    }
}
