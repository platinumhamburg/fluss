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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TtlCompactionFilterConfig;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LimitBatchScannerTest {

    @Test
    void testLimitScanCanDecodeKvTtlTimestampPrefixedValues() throws Exception {
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
                        // Enable KV TTL so that value bytes are timestamp-prefixed.
                        .property("table.kv.compaction-filter.type", "TTL")
                        .property("table.kv.compaction-filter.ttl.column", "ttl_ms")
                        .property("table.kv.compaction-filter.ttl.retention-ms", "1000")
                        .build();

        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "__ttl_table__index__ttl"),
                        1L,
                        1,
                        descriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(1, schema);

        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(schema.getRowType());
        KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "v1", 10_000L});

        TtlCompactionFilterConfig ttlConfig = new TtlCompactionFilterConfig("ttl_ms", 1000L);
        byte[] recordsBytes =
                buildValueRecordBatchBytesWithValue(
                        ValueEncoder.encodeValueWithLongPrefix(
                                10_000L, (short) 1, record.getRow(), ttlConfig));
        LimitScanResponse response = new LimitScanResponse().setRecords(recordsBytes);

        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.limitScan(org.mockito.ArgumentMatchers.any()))
                .thenReturn(CompletableFuture.completedFuture(response));

        MetadataUpdater metadataUpdater = mock(MetadataUpdater.class);
        when(metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket)).thenReturn(1);
        when(metadataUpdater.newTabletServerClientForNode(1)).thenReturn(gateway);

        LimitBatchScanner scanner =
                new LimitBatchScanner(
                        tableInfo, tableBucket, schemaGetter, metadataUpdater, null, 10);

        try (CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofSeconds(1))) {
            assertThat(it).isNotNull();
            assertThat(it.hasNext()).isTrue();
            InternalRow row = it.next();
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
    }

    private static byte[] buildValueRecordBatchBytesWithValue(byte[] valueBytes)
            throws IOException {
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(valueBytes);
        DefaultValueRecordBatch batch = builder.build();
        byte[] out = new byte[batch.sizeInBytes()];
        batch.getSegment().get(batch.getPosition(), out, 0, out.length);
        return out;
    }
}
