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

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KvMutationType}. */
class KvMutationTypeTest {

    @Test
    void testRequiresRowData() {
        assertThat(KvMutationType.UPSERT.requiresRowData()).isTrue();
        assertThat(KvMutationType.RETRACT.requiresRowData()).isTrue();
        assertThat(KvMutationType.DELETE.requiresRowData()).isFalse();
    }

    @Test
    void testFromRow() {
        assertThat(KvMutationType.fromRow((BinaryRow) null)).isEqualTo(KvMutationType.DELETE);
        assertThat(KvMutationType.fromRow(TestInternalRowGenerator.genCompactedRowForAllType()))
                .isEqualTo(KvMutationType.UPSERT);
    }

    @Test
    void testFromRowAndRetract() {
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();

        assertThat(KvMutationType.fromRowAndRetract(row, false)).isEqualTo(KvMutationType.UPSERT);
        assertThat(KvMutationType.fromRowAndRetract(null, false)).isEqualTo(KvMutationType.DELETE);
        assertThat(KvMutationType.fromRowAndRetract(row, true)).isEqualTo(KvMutationType.RETRACT);
        assertThatThrownBy(() -> KvMutationType.fromRowAndRetract(null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RETRACT requires row data");
    }
}
