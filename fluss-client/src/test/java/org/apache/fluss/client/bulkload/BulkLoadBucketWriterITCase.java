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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            Path workDirectory = Files.createTempDirectory("fluss-bulkload-last-row-wins-");
            Configuration clientConfiguration = new Configuration(cluster.getClientConfig());
            clientConfiguration.set(
                    ConfigOptions.CLIENT_BULK_LOAD_LOCAL_DIRS,
                    Collections.singletonList(workDirectory.toString()));
            try (Connection connection = ConnectionFactory.createConnection(clientConfiguration);
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
                BulkLoadBuildContext context =
                        client.begin(PhysicalTablePath.of(tablePath), null, TIMEOUT);
                InternalRow first = row(tableInfo.getRowType(), 1, "z");
                InternalRow last = row(tableInfo.getRowType(), 1, "a");
                BulkLoadBucketFiles bucketFiles;
                BulkLoadBucketWriter writer;
                try (BulkLoadBuildSession build = client.openBuildSession(context)) {
                    writer = build.createBucketWriter(PhysicalTablePath.of(tablePath), 0);
                    writer.add(first);
                    writer.add(last);
                    bucketFiles = writer.finish();
                }
                assertThatThrownBy(() -> writer.add(first))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("already closed");
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
        } finally {
            cluster.close();
        }
    }

    @ParameterizedTest
    @EnumSource(ChangelogImage.class)
    void testSharedPartitionTransactionAndSerializedBuilders(
            ChangelogImage image, @TempDir Path workDirectory) throws Exception {
        FlussClusterExtension cluster =
                FlussClusterExtension.builder().setNumOfTabletServers(3).build();
        try {
            cluster.start();
            Configuration configuration = new Configuration(cluster.getClientConfig());
            configuration.set(
                    ConfigOptions.CLIENT_BULK_LOAD_LOCAL_DIRS,
                    Collections.singletonList(workDirectory.toString()));
            Path unrelated = Files.createFile(workDirectory.resolve("unrelated"));
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("payload", DataTypes.STRING())
                            .column("pt", DataTypes.STRING())
                            .primaryKey("id", "pt")
                            .build();
            try (Connection connection = ConnectionFactory.createConnection(configuration);
                    FlussAdmin admin = (FlussAdmin) connection.getAdmin()) {
                TablePath path = TablePath.of("bulkload_sdk", "shared_partitions");
                admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, false).get();
                admin.createTable(
                                path,
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .distributedBy(2, "id")
                                        .partitionedBy("pt")
                                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, image)
                                        .build(),
                                false)
                        .get();
                for (String partition : Arrays.asList("p1", "p2")) {
                    if (partition.equals("p2")) {
                        admin.alterTable(
                                        path,
                                        Collections.singletonList(TableChange.modifyBucketCount(3)),
                                        false)
                                .get();
                    }
                    admin.createPartition(
                                    path,
                                    new PartitionSpec(Collections.singletonMap("pt", partition)),
                                    false)
                            .get();
                }
                BulkLoadClient client = connection.getBulkLoadClient();
                PhysicalTablePath p1 = PhysicalTablePath.of(path, "p1");
                PhysicalTablePath p2 = PhysicalTablePath.of(path, "p2");
                BulkLoadBuildContext begun = client.begin(Arrays.asList(p2, p1), null, TIMEOUT);
                BulkLoadBuildContext context =
                        InstantiationUtils.deserializeObject(
                                InstantiationUtils.serializeObject(begun),
                                getClass().getClassLoader());
                assertThat(context).isEqualTo(begun);
                assertThat(context.getTarget(p1).getTableInfo().getNumBuckets()).isEqualTo(2);
                assertThat(context.getTarget(p2).getTableInfo().getNumBuckets()).isEqualTo(3);
                assertThat(client.getInProgressBulkLoad(p1).get().getHandle())
                        .isEqualTo(context.getHandle());
                assertThat(client.getInProgressBulkLoad(p2).get().getHandle())
                        .isEqualTo(context.getHandle());
                List<BulkLoadBucketFiles> results = new ArrayList<>();
                BulkLoadBuildSession build = client.openBuildSession(context);
                try (BulkLoadBuildSession ignored = build) {
                    // An unfinished writer is owned and closed by its session as well.
                    build.createBucketWriter(p1, 0);
                    for (BulkLoadBuildContext.Target target : context.getTargets()) {
                        InternalRow input =
                                row(
                                        schema.getRowType(),
                                        1,
                                        target.getPath().getPartitionName(),
                                        target.getPath().getPartitionName());
                        for (int bucket = 0;
                                bucket < target.getTableInfo().getNumBuckets();
                                bucket++) {
                            try (BulkLoadBucketWriter writer =
                                    build.createBucketWriter(target.getPath(), bucket)) {
                                if (target.bucketOf(input) == bucket) {
                                    writer.add(input);
                                } else {
                                    assertThatThrownBy(() -> writer.add(input))
                                            .isInstanceOf(IllegalArgumentException.class)
                                            .hasMessageContaining("this writer owns bucket");
                                }
                                results.add(writer.finish());
                            }
                        }
                    }
                }
                try (Stream<Path> children = Files.list(workDirectory)) {
                    assertThat(children).containsExactly(unrelated);
                }
                assertThatThrownBy(() -> build.createBucketWriter(p1, 0))
                        .isInstanceOf(IllegalStateException.class);
                // Builder completion order must not affect the shared input digest.
                Collections.reverse(results);
                assertThatThrownBy(
                                () ->
                                        BulkLoadClient.inputDigest(
                                                context, results.subList(1, results.size())))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("every target bucket");
                List<BulkLoadBucketFiles> duplicate = new ArrayList<>(results);
                duplicate.add(results.get(0));
                assertThatThrownBy(() -> BulkLoadClient.inputDigest(context, duplicate))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("duplicates");
                assertThat(client.commit(context, results, TIMEOUT).getState())
                        .isEqualTo(BulkLoadState.COMMITTED);
                try (Table table = connection.getTable(path)) {
                    for (String partition : Arrays.asList("p1", "p2")) {
                        InternalRow actual =
                                table.newLookup()
                                        .createLookuper()
                                        .lookup(row(1, partition))
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                        .getSingletonRow();
                        assertThatRow(actual)
                                .withSchema(schema.getRowType())
                                .isEqualTo(row(schema.getRowType(), 1, partition, partition));
                    }
                }
                assertThat(client.getInProgressBulkLoad(p1)).isEmpty();
                assertThat(client.getInProgressBulkLoad(p2)).isEmpty();
            }
        } finally {
            cluster.close();
        }
    }
}
