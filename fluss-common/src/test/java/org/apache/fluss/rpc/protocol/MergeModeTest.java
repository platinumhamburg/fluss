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

package org.apache.fluss.rpc.protocol;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MergeMode}. */
class MergeModeTest {

    @Test
    void testFromValueRetractThrowsException() {
        // Proto value 2 is not a valid MergeMode (retract is now per-record, not a MergeMode)
        assertThatThrownBy(() -> MergeMode.fromValue(2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MergeMode value: 2");
        assertThatThrownBy(() -> MergeMode.fromProtoValue(2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MergeMode value: 2");
    }

    @Test
    void testExistingModesUnchanged() {
        assertThat(MergeMode.DEFAULT.getValue()).isEqualTo(0);
        assertThat(MergeMode.OVERWRITE.getValue()).isEqualTo(1);
        assertThat(MergeMode.fromValue(0)).isEqualTo(MergeMode.DEFAULT);
        assertThat(MergeMode.fromValue(1)).isEqualTo(MergeMode.OVERWRITE);
    }

    @Test
    void testFromValueUnknownThrowsException() {
        assertThatThrownBy(() -> MergeMode.fromValue(99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MergeMode value: 99");
        assertThatThrownBy(() -> MergeMode.fromValue(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MergeMode value: -1");
    }

    @Test
    void testFromProtoValueUnknownThrowsException() {
        assertThatThrownBy(() -> MergeMode.fromProtoValue(99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MergeMode.fromProtoValue(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testValuesSize() {
        assertThat(MergeMode.values()).hasSize(2);
    }
}
