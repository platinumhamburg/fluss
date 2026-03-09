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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.DefaultKvRecordBatch.HAS_RECORD_FLAGS_MASK;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultKvRecordBatch}. */
class DefaultKvRecordBatchTest extends KvTestBase {

    @Test
    void writeAndReadBatch() throws Exception {
        int recordNumber = 100;
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        List<byte[]> keys = new ArrayList<>();
        List<CompactedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = new byte[] {(byte) i, (byte) i};
            CompactedRow row =
                    i % 2 == 1 ? null : TestInternalRowGenerator.genCompactedRowForAllType();
            builder.append(key, row);
            keys.add(key);
            rows.add(row);
        }

        KvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(builder.build());
        kvRecords.ensureValid();

        // verify the header info
        assertThat(kvRecords.getRecordCount()).isEqualTo(recordNumber);
        assertThat(kvRecords.magic()).isEqualTo(magic);
        assertThat(kvRecords.isValid()).isTrue();
        assertThat(kvRecords.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        for (KvRecord record :
                kvRecords.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA)))) {
            assertThat(keyToBytes(record)).isEqualTo(keys.get(i));
            assertThat(record.getRow()).isEqualTo(rows.get(i));
            assertThat(record.getMutationType())
                    .isEqualTo(rows.get(i) == null ? KvMutationType.DELETE : KvMutationType.UPSERT);
            i++;
        }

        builder.close();
    }

    @Test
    void testNewFormatBatchHasRecordFlagsAttribute() throws Exception {
        // New format (useRecordFlags=true) should set bit 0 of attributes
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED,
                        KvRecordBatchEncoding.WITH_RECORD_FLAGS);
        byte[] key = new byte[] {1, 2};
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();
        builder.append(key, row);
        // Append a retract record
        byte[] key2 = new byte[] {3, 4};
        builder.append(key2, row, true);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        assertThat(batch.hasRecordFlags()).isTrue();
        assertThat(batch.attributes() & HAS_RECORD_FLAGS_MASK).isEqualTo((byte) 1);
        assertThat(batch.getRecordCount()).isEqualTo(2);

        // Verify per-record retract flags
        KvRecordBatch.ReadContext ctx =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        List<KvRecord> records = new ArrayList<>();
        for (KvRecord r : batch.records(ctx)) {
            records.add(r);
        }
        assertThat(records.get(0).isRetract()).isFalse();
        assertThat(records.get(0).getMutationType()).isEqualTo(KvMutationType.UPSERT);
        assertThat(records.get(1).isRetract()).isTrue();
        assertThat(records.get(1).getMutationType()).isEqualTo(KvMutationType.RETRACT);

        builder.close();
    }

    @Test
    void testLegacyFormatBatchNoRecordFlags() throws Exception {
        // Legacy format should NOT set bit 0 of attributes
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED,
                        KvRecordBatchEncoding.LEGACY);
        byte[] key = new byte[] {1, 2};
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();
        builder.append(key, row);
        // Even if isRetract=true is passed, legacy format ignores it
        byte[] key2 = new byte[] {3, 4};
        builder.append(key2, row, true);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        assertThat(batch.hasRecordFlags()).isFalse();
        assertThat(batch.attributes() & HAS_RECORD_FLAGS_MASK).isEqualTo((byte) 0);
        assertThat(batch.getRecordCount()).isEqualTo(2);

        // All records should have isRetract=false in legacy format
        KvRecordBatch.ReadContext ctx =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        List<KvRecord> records = new ArrayList<>();
        for (KvRecord r : batch.records(ctx)) {
            records.add(r);
        }
        assertThat(records.get(0).isRetract()).isFalse();
        assertThat(records.get(0).getMutationType()).isEqualTo(KvMutationType.UPSERT);
        assertThat(records.get(1).isRetract()).isFalse();
        assertThat(records.get(1).getMutationType()).isEqualTo(KvMutationType.UPSERT);

        builder.close();
    }

    @Test
    void testLegacyNullRowIsNormalizedAsDelete() throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED,
                        KvRecordBatchEncoding.LEGACY);
        byte[] key = new byte[] {1, 2};
        builder.append(key, null);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        KvRecord record =
                batch.records(
                                KvRecordReadContext.createReadContext(
                                        KvFormat.COMPACTED,
                                        new TestingSchemaGetter(1, DATA1_SCHEMA)))
                        .iterator()
                        .next();
        assertThat(record.getRow()).isNull();
        assertThat(record.isRetract()).isFalse();
        assertThat(record.getMutationType()).isEqualTo(KvMutationType.DELETE);

        builder.close();
    }
}
