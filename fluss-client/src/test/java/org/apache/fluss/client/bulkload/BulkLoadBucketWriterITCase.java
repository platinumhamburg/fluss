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

package org.apache.fluss.client.bulkload;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FileUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for the public BulkLoad bucket writer. */
final class BulkLoadBucketWriterITCase {

    private static final Duration TIMEOUT = Duration.ofMinutes(5);
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testKeepsLastRowForDuplicatePrimaryKey() throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder().setNumOfTabletServers(3).build();
        try {
            cluster.start();
            try (Connection connection =
                            ConnectionFactory.createConnection(cluster.getClientConfig());
                    FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                TablePath tablePath = TablePath.of("bulkload_writer", "last_row_wins");
                admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                admin.createTable(
                                tablePath,
                                TableDescriptor.builder()
                                        .schema(SCHEMA)
                                        .distributedBy(1, "id")
                                        .build(),
                                false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                TableInfo tableInfo =
                        admin.getTableInfo(tablePath)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                cluster.waitUntilTableReady(tableInfo.getTableId());

                BulkLoadClient client = connection.getBulkLoadClient();
                BulkLoadBeginResult result =
                        client.begin(
                                PhysicalTablePath.of(tablePath),
                                UUID.randomUUID().toString(),
                                null,
                                TIMEOUT);
                BulkLoadBuildContext context = result.getBuildContext();
                InternalRow first = row(tableInfo.getRowType(), 1, "z");
                InternalRow last = row(tableInfo.getRowType(), 1, "a");
                Path workDirectory = Files.createTempDirectory("fluss-bulkload-last-row-wins-");
                try {
                    BulkLoadBucketFiles bucketFiles;
                    try (BulkLoadBucketWriter writer =
                            new BulkLoadBucketWriter(context, 0, workDirectory.toFile())) {
                        writer.add(first);
                        writer.add(last);
                        bucketFiles = writer.finish();
                    }
                    assertThat(
                                    client.commit(
                                                    context,
                                                    Collections.singletonList(bucketFiles),
                                                    TIMEOUT)
                                            .getState())
                            .isEqualTo(BulkLoadState.COMMITTED);

                    try (Table table = connection.getTable(tablePath)) {
                        InternalRow actual =
                                table.newLookup()
                                        .createLookuper()
                                        .lookup(row(1))
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                        .getSingletonRow();
                        assertThatRow(actual).withSchema(tableInfo.getRowType()).isEqualTo(last);
                    }
                } finally {
                    FileUtils.deleteDirectoryQuietly(workDirectory.toFile());
                }
            }
        } finally {
            cluster.close();
        }
    }
}
