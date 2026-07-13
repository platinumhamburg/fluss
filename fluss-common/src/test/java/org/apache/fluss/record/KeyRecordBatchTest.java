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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KeyRecordBatch}. */
class KeyRecordBatchTest extends KvTestBase {

    @Test
    void testSingleKeyField() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();

        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(schema.getRowType(), new int[] {0});
        byte[] key1 = keyEncoder.encodeKey(row(1));
        byte[] key2 = keyEncoder.encodeKey(row(2));

        KeyRecordBatch batch =
                new KeyRecordBatch(
                        Arrays.asList(key1, key2), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        KvRecordBatch.ReadContext context = createContext(schema);
        Iterator<KvRecord> iterator = batch.records(context).iterator();

        BinaryRow row1 = iterator.next().getRow();
        assertThat(row1.getInt(0)).isEqualTo(1);
        assertThat(row1.isNullAt(1)).isTrue();
        assertThat(row1.isNullAt(2)).isTrue();

        BinaryRow row2 = iterator.next().getRow();
        assertThat(row2.getInt(0)).isEqualTo(2);
        assertThat(row2.isNullAt(1)).isTrue();
        assertThat(row2.isNullAt(2)).isTrue();

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testMultipleKeyFields() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.DOUBLE())
                        .primaryKey("a", "b", "c")
                        .build();

        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(schema.getRowType(), new int[] {0, 1, 2});
        byte[] key1 = keyEncoder.encodeKey(row(100, "key1", 1000L));
        byte[] key2 = keyEncoder.encodeKey(row(200, "key2", 2000L));

        KeyRecordBatch batch =
                new KeyRecordBatch(
                        Arrays.asList(key1, key2), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        KvRecordBatch.ReadContext context = createContext(schema);
        Iterator<KvRecord> iterator = batch.records(context).iterator();

        BinaryRow row1 = iterator.next().getRow();
        assertThat(row1.getInt(0)).isEqualTo(100);
        assertThat(row1.getString(1).toString()).isEqualTo("key1");
        assertThat(row1.getLong(2)).isEqualTo(1000L);
        assertThat(row1.isNullAt(3)).isTrue();

        BinaryRow row2 = iterator.next().getRow();
        assertThat(row2.getInt(0)).isEqualTo(200);
        assertThat(row2.getString(1).toString()).isEqualTo("key2");
        assertThat(row2.getLong(2)).isEqualTo(2000L);
        assertThat(row2.isNullAt(3)).isTrue();

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testNonSequentialKeyFields() {
        // Key at end: pk=(c), fields=(a, b, c)
        Schema schema1 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("c")
                        .build();
        CompactedKeyEncoder encoder1 = new CompactedKeyEncoder(schema1.getRowType(), new int[] {2});
        byte[] key1 = encoder1.encodeKey(row(schema1.getRowType(), null, null, 1000L));
        KeyRecordBatch batch1 =
                new KeyRecordBatch(Arrays.asList(key1), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        BinaryRow row1 = batch1.records(createContext(schema1)).iterator().next().getRow();
        assertThat(row1.isNullAt(0)).isTrue();
        assertThat(row1.isNullAt(1)).isTrue();
        assertThat(row1.getLong(2)).isEqualTo(1000L);

        // Non-consecutive: pk=(b, d), fields=(a, b, c, d, e)
        Schema schema2 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.DOUBLE())
                        .column("e", DataTypes.BOOLEAN())
                        .primaryKey("b", "d")
                        .build();
        CompactedKeyEncoder encoder2 =
                new CompactedKeyEncoder(schema2.getRowType(), new int[] {1, 3});
        byte[] key2 = encoder2.encodeKey(row(schema2.getRowType(), null, "key1", null, 1.5, null));
        KeyRecordBatch batch2 =
                new KeyRecordBatch(Arrays.asList(key2), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        BinaryRow row2 = batch2.records(createContext(schema2)).iterator().next().getRow();
        assertThat(row2.isNullAt(0)).isTrue();
        assertThat(row2.getString(1).toString()).isEqualTo("key1");
        assertThat(row2.isNullAt(2)).isTrue();
        assertThat(row2.getDouble(3)).isEqualTo(1.5);
        assertThat(row2.isNullAt(4)).isTrue();

        // Complex ordering: pk=(d, a, c), fields=(a, b, c, d, e, f)
        Schema schema3 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.DOUBLE())
                        .column("e", DataTypes.BOOLEAN())
                        .column("f", DataTypes.FLOAT())
                        .primaryKey("d", "a", "c")
                        .build();
        CompactedKeyEncoder encoder3 =
                new CompactedKeyEncoder(schema3.getRowType(), new int[] {3, 0, 2});
        byte[] key3 = encoder3.encodeKey(row(100, null, 1000L, 1.5, null, null));
        KeyRecordBatch batch3 =
                new KeyRecordBatch(Arrays.asList(key3), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        BinaryRow row3 = batch3.records(createContext(schema3)).iterator().next().getRow();
        assertThat(row3.getInt(0)).isEqualTo(100);
        assertThat(row3.isNullAt(1)).isTrue();
        assertThat(row3.getLong(2)).isEqualTo(1000L);
        assertThat(row3.getDouble(3)).isEqualTo(1.5);
        assertThat(row3.isNullAt(4)).isTrue();
        assertThat(row3.isNullAt(5)).isTrue();
    }

    @Test
    void testEmptyBatch() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        KeyRecordBatch batch =
                new KeyRecordBatch(Arrays.asList(), KvFormat.COMPACTED, DEFAULT_SCHEMA_ID);
        assertThat(batch.getRecordCount()).isEqualTo(0);
        assertThat(batch.records(createContext(schema)).iterator().hasNext()).isFalse();
    }

    @Test
    void testVersionThreeDecodesVersionTwoKeyEncoding() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id", "name")
                        .build();
        CompactedKeyEncoder keyEncoder =
                CompactedKeyEncoder.createKeyEncoder(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames());
        byte[] key = keyEncoder.encodeKey(row(1, "Alice", "ignored"));

        KeyRecordBatch batch =
                new KeyRecordBatch(
                        Collections.singletonList(key),
                        KvFormat.COMPACTED,
                        (short) ConfigOptions.KV_FORMAT_VERSION_3,
                        DEFAULT_SCHEMA_ID,
                        false,
                        null);
        BinaryRow result = batch.records(createContext(schema)).iterator().next().getRow();

        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getString(1).toString()).isEqualTo("Alice");
        assertThat(result.isNullAt(2)).isTrue();
    }

    @Test
    void testCreatePreservesLegacyKeysWhenKvFormatVersionIsMissing() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id", "name")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .build();
        long now = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("db", "missing_version_keys"),
                        1L,
                        DEFAULT_SCHEMA_ID,
                        descriptor,
                        null,
                        now,
                        now);
        Configuration legacyConfig = new Configuration();
        legacyConfig.set(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        legacyConfig.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, 1);
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        schema.getRowType(),
                        schema.getPrimaryKeyColumnNames(),
                        new TableConfig(legacyConfig),
                        false);
        byte[] key = keyEncoder.encodeKey(row(1, "Alice", "ignored"));

        KeyRecordBatch batch = KeyRecordBatch.create(Collections.singletonList(key), tableInfo);
        BinaryRow result = batch.records(createContext(schema)).iterator().next().getRow();

        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getString(1).toString()).isEqualTo("Alice");
        assertThat(result.isNullAt(2)).isTrue();
    }

    private KvRecordBatch.ReadContext createContext(Schema schema) {
        return KvRecordReadContext.createReadContext(
                KvFormat.COMPACTED, new TestingSchemaGetter(DEFAULT_SCHEMA_ID, schema));
    }
}
