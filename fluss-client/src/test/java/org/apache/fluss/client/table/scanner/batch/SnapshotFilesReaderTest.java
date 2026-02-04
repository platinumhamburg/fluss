/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TtlCompactionFilterConfig;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotFilesReaderTest {

    @Test
    void testSnapshotFilesReaderDecodesTimestampPrefixedValue(@TempDir Path rocksDbDir)
            throws Exception {
        RocksDB.loadLibrary();

        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("ttl_ms", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(1, schema);

        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema.getRowType());
        KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1", 10_000L});

        TtlCompactionFilterConfig ttlConfig = new TtlCompactionFilterConfig("ttl_ms", 1000L);
        byte[] valueBytes =
                ValueEncoder.encodeValueWithLongPrefix(
                        10_000L, (short) 1, record.getRow(), ttlConfig);

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, rocksDbDir.toString())) {
            db.put("k1".getBytes(), valueBytes);
        }

        try (SnapshotFilesReader reader =
                new SnapshotFilesReader(
                        KvFormat.COMPACTED, rocksDbDir, null, 1, schema, schemaGetter, ttlConfig)) {
            assertThat(reader.hasNext()).isTrue();
            InternalRow row = reader.next();
            InternalRow.FieldGetter aGetter =
                    InternalRow.createFieldGetter(schema.getRowType().getTypeAt(0), 0);
            InternalRow.FieldGetter bGetter =
                    InternalRow.createFieldGetter(schema.getRowType().getTypeAt(1), 1);
            InternalRow.FieldGetter ttlGetter =
                    InternalRow.createFieldGetter(schema.getRowType().getTypeAt(2), 2);
            assertThat(aGetter.getFieldOrNull(row)).isEqualTo(1);
            assertThat(String.valueOf(bGetter.getFieldOrNull(row))).isEqualTo("v1");
            assertThat(ttlGetter.getFieldOrNull(row)).isEqualTo(10_000L);
            assertThat(reader.hasNext()).isFalse();
        }
    }
}
