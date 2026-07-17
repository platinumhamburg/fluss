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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LimitBatchScanner}. */
class LimitBatchScannerTest {

    @Test
    void testVersion3KvValuesUseTableLayout() throws Exception {
        TableInfo tableInfo = version3TableInfo();
        SchemaGetter schemaGetter =
                new TestingSchemaGetter(tableInfo.getSchemaId(), tableInfo.getSchema());
        RecordingGateway gateway = new RecordingGateway(version3Response(tableInfo));
        TestMetadataUpdater metadataUpdater = new TestMetadataUpdater(gateway);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);

        List<InternalRow> rows = new ArrayList<>();
        try (LimitBatchScanner scanner =
                new LimitBatchScanner(
                        tableInfo, tableBucket, schemaGetter, metadataUpdater, null, 2)) {
            try (CloseableIterator<InternalRow> batch = scanner.pollBatch(Duration.ofSeconds(5))) {
                assertThat(batch).isNotNull();
                while (batch.hasNext()) {
                    rows.add(batch.next());
                }
            }
            assertThat(scanner.pollBatch(Duration.ofSeconds(5))).isNull();
        }

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(1).getString(1).toString()).isEqualTo("b");
        assertThat(gateway.requests).hasSize(1);
        LimitScanRequest request = gateway.requests.get(0);
        assertThat(request.getTableId()).isEqualTo(DATA1_TABLE_ID_PK);
        assertThat(request.getBucketId()).isZero();
        assertThat(request.getLimit()).isEqualTo(2);
        assertThat(request.hasPartitionId()).isFalse();
    }

    private static TableInfo version3TableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(3, "a")
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, KV_FORMAT_VERSION_3)
                        .build();
        return TableInfo.of(DATA1_TABLE_PATH_PK, DATA1_TABLE_ID_PK, 1, descriptor, null, 1L, 1L);
    }

    private static LimitScanResponse version3Response(TableInfo tableInfo) throws Exception {
        ValueEncoder encoder =
                ValueEncoder.forKvFormatVersion(KV_FORMAT_VERSION_3, row -> row.getInt(0));
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(encoder.encodeValue(value(tableInfo, 1, "a")));
        builder.append(encoder.encodeValue(value(tableInfo, 2, "b")));
        DefaultValueRecordBatch batch = builder.build();
        byte[] bytes = new byte[batch.sizeInBytes()];
        batch.getSegment().get(batch.getPosition(), bytes, 0, bytes.length);
        return new LimitScanResponse().setRecords(bytes);
    }

    private static BinaryValue value(TableInfo tableInfo, int id, String text) {
        BinaryRow row =
                compactedRow(tableInfo.getRowType(), new Object[] {Integer.valueOf(id), text});
        return new BinaryValue((short) tableInfo.getSchemaId(), row);
    }

    private static final class RecordingGateway extends TestingTabletGatewayService {
        private final LimitScanResponse response;
        private final List<LimitScanRequest> requests = new ArrayList<>();

        private RecordingGateway(LimitScanResponse response) {
            this.response = response;
        }

        @Override
        public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
            requests.add(request);
            return CompletableFuture.completedFuture(response);
        }
    }

    private static final class TestMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway gateway;

        private TestMetadataUpdater(TabletServerGateway gateway) {
            super(null, new Configuration(), Cluster.empty());
            this.gateway = gateway;
        }

        @Override
        public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
            return 1;
        }

        @Override
        public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
            return gateway;
        }
    }
}
