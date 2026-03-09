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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.KvMutationType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WriteRecord}. */
class WriteRecordTest {

    @Test
    void testKvWriteSemanticsAccessors() {
        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        byte[] key =
                new CompactedKeyEncoder(DATA1_ROW_TYPE, DATA1_SCHEMA_PK.getPrimaryKeyIndexes())
                        .encodeKey(row);
        WriteRecord writeRecord =
                WriteRecord.forUpsert(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null);

        assertThat(writeRecord.isKvWrite()).isTrue();
        assertThat(writeRecord.getKvMutationType()).isEqualTo(KvMutationType.UPSERT);
        assertThat(writeRecord.requireKvMutationType()).isEqualTo(KvMutationType.UPSERT);
    }

    @Test
    void testAppendOnlyWriteDoesNotExposeKvMutationSemantics() {
        CompactedRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        WriteRecord writeRecord =
                WriteRecord.forCompactedAppend(
                        DATA1_TABLE_INFO_PK, PhysicalTablePath.of(DATA1_TABLE_PATH_PK), row, null);

        assertThat(writeRecord.isKvWrite()).isFalse();
        assertThat(writeRecord.getKvMutationType()).isNull();
        assertThatThrownBy(writeRecord::requireKvMutationType)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kv mutation type is only available for kv write records");
    }
}
