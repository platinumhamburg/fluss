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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class LookuperDecodingTest {

    @Test
    void testKvTtlEncodedValueCanBeDecodedInLookupPath() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("ttl_ms", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a")
                        .property(
                                ConfigOptions.TABLE_KV_COMPACTION_FILTER_TYPE,
                                org.apache.fluss.metadata.CompactionFilterType.TTL)
                        .property(ConfigOptions.TABLE_KV_COMPACTION_FILTER_TTL_COLUMN, "ttl_ms")
                        .property(ConfigOptions.TABLE_KV_COMPACTION_FILTER_TTL_RETENTION_MS, 1000L)
                        .build();

        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "__ttl_table__index__ttl"),
                        1L,
                        1,
                        descriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema.getRowType());
        KvRecord record =
                kvRecordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1", 10_000L});

        byte[] encoded =
                ValueEncoder.encodeValueWithLongPrefix(
                        10_000L, (short) 1, record.getRow(), tableInfo.getCompactionFilterConfig());

        TestLookuper lookuper =
                new TestLookuper(
                        tableInfo,
                        mock(MetadataUpdater.class),
                        mock(LookupClient.class),
                        mock(SchemaGetter.class));

        LookupResult result = lookuper.decodeValues(Collections.singletonList(encoded));
        assertThat(result.getRowList()).hasSize(1);

        InternalRow row = result.getRowList().get(0);
        InternalRow.FieldGetter aGetter =
                InternalRow.createFieldGetter(schema.getRowType().getTypeAt(0), 0);
        InternalRow.FieldGetter bGetter =
                InternalRow.createFieldGetter(schema.getRowType().getTypeAt(1), 1);
        InternalRow.FieldGetter ttlGetter =
                InternalRow.createFieldGetter(schema.getRowType().getTypeAt(2), 2);

        assertThat(aGetter.getFieldOrNull(row)).isEqualTo(1);
        assertThat(String.valueOf(bGetter.getFieldOrNull(row))).isEqualTo("v1");
        assertThat(ttlGetter.getFieldOrNull(row)).isEqualTo(10_000L);
    }

    private static final class TestLookuper extends AbstractLookuper {

        private TestLookuper(
                TableInfo tableInfo,
                MetadataUpdater metadataUpdater,
                LookupClient lookupClient,
                SchemaGetter schemaGetter) {
            super(tableInfo, metadataUpdater, lookupClient, schemaGetter);
        }

        @Override
        public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
            // This is a test implementation, not used in this test
            throw new UnsupportedOperationException(
                    "lookup(InternalRow) is not supported in TestLookuper");
        }

        LookupResult decodeValues(List<byte[]> values) {
            CompletableFuture<LookupResult> future = new CompletableFuture<>();
            handleLookupResponse(values, future);
            return future.join();
        }
    }
}
