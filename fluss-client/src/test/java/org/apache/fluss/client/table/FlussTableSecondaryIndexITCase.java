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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for secondary index lookup on {@link FlussTable}, covering non-partitioned and
 * partitioned scenarios. Migrated from V1 secondary index tests to V2 API patterns.
 */
class FlussTableSecondaryIndexITCase extends ClientToServerITCaseBase {

    private static final String DB = "test_db_sec_idx";

    /** Bounded poll deadline for index visibility checks. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void testSecondaryIndexLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sec_idx_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, "Alice", "alice@example.com"));
            upsertWriter.upsert(row(2, "Bob", "bob@example.com"));
            upsertWriter.upsert(row(3, "Charlie", "charlie@example.com"));
            upsertWriter.upsert(row(4, "Diana", "diana@example.com"));
            upsertWriter.upsert(row(5, "Eve", "eve@example.com"));
            upsertWriter.flush();

            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_email");

            // Wait until name index for "Alice" is visible.
            waitUntil(
                    () -> {
                        LookupResult r = nameLookuper.lookup(row("Alice")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_name entry 'Alice' to become visible");

            // Verify full row returned by name lookup for "Alice".
            LookupResult aliceResult = nameLookuper.lookup(row("Alice")).get();
            assertThat(aliceResult.getRowList()).hasSize(1);
            InternalRow aliceRow = aliceResult.getRowList().get(0);
            assertThat(aliceRow.getInt(0)).isEqualTo(1);
            assertThat(aliceRow.getString(1).toString()).isEqualTo("Alice");
            assertThat(aliceRow.getString(2).toString()).isEqualTo("alice@example.com");

            // Verify email index for "bob@example.com" returns correct row.
            waitUntil(
                    () -> {
                        LookupResult r = emailLookuper.lookup(row("bob@example.com")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_email entry 'bob@example.com' to become visible");

            LookupResult bobResult = emailLookuper.lookup(row("bob@example.com")).get();
            assertThat(bobResult.getRowList()).hasSize(1);
            InternalRow bobRow = bobResult.getRowList().get(0);
            assertThat(bobRow.getInt(0)).isEqualTo(2);
            assertThat(bobRow.getString(1).toString()).isEqualTo("Bob");
            assertThat(bobRow.getString(2).toString()).isEqualTo("bob@example.com");

            // Verify non-existent value returns empty.
            LookupResult notFoundResult = nameLookuper.lookup(row("NonExistent")).get();
            assertThat(notFoundResult.getRowList()).isEmpty();
        }
    }

    @Test
    void testPartitionedTableSecondaryIndexLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_partitioned_sec_idx_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("year", DataTypes.STRING())
                        .primaryKey("id", "year")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "id")
                        .partitionedBy("year")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .build();

        createTable(tablePath, descriptor, true);

        // Wait for non-partitioned index tables to have their replicas ready.
        TablePath nameIdxPath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName(tablePath.getTableName(), "idx_name"));
        TablePath emailIdxPath =
                TablePath.of(
                        DB, IndexTableUtils.indexTableName(tablePath.getTableName(), "idx_email"));
        long nameIdxTableId = admin.getTableInfo(nameIdxPath).get().getTableId();
        long emailIdxTableId = admin.getTableInfo(emailIdxPath).get().getTableId();
        waitAllReplicasReady(nameIdxTableId, 1);
        waitAllReplicasReady(emailIdxTableId, 1);

        // Manually create partitions "2023" and "2024".
        admin.createPartition(tablePath, newPartitionSpec("year", "2023"), true).get();
        admin.createPartition(tablePath, newPartitionSpec("year", "2024"), true).get();

        // Wait for both partitions to be registered.
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(tablePath, 2);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            // Write cross-partition data.
            upsertWriter.upsert(row(1, "Alice", "alice@example.com", "2023"));
            upsertWriter.upsert(row(2, "Bob", "bob@example.com", "2023"));
            upsertWriter.upsert(row(3, "Charlie", "charlie@example.com", "2024"));
            upsertWriter.upsert(row(4, "Diana", "diana@example.com", "2024"));
            upsertWriter.flush();

            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_email");

            // Wait for name index to become visible.
            waitUntil(
                    () -> {
                        LookupResult r = nameLookuper.lookup(row("Alice")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_name entry 'Alice' (partition 2023) to become visible");

            // Verify name lookup returns data from correct partition (2023).
            LookupResult aliceResult = nameLookuper.lookup(row("Alice")).get();
            assertThat(aliceResult.getRowList()).hasSize(1);
            InternalRow aliceRow = aliceResult.getRowList().get(0);
            assertThat(aliceRow.getInt(0)).isEqualTo(1);
            assertThat(aliceRow.getString(1).toString()).isEqualTo("Alice");
            assertThat(aliceRow.getString(3).toString()).isEqualTo("2023");

            // Verify email lookup returns data from correct partition (2024).
            waitUntil(
                    () -> {
                        LookupResult r = emailLookuper.lookup(row("charlie@example.com")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_email entry 'charlie@example.com' (partition 2024) to become visible");

            LookupResult charlieResult = emailLookuper.lookup(row("charlie@example.com")).get();
            assertThat(charlieResult.getRowList()).hasSize(1);
            InternalRow charlieRow = charlieResult.getRowList().get(0);
            assertThat(charlieRow.getInt(0)).isEqualTo(3);
            assertThat(charlieRow.getString(2).toString()).isEqualTo("charlie@example.com");
            assertThat(charlieRow.getString(3).toString()).isEqualTo("2024");
        }
    }

    @Test
    void testIndexColumnNormalWriteAndLookup() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_idx_col_write_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, "Frank", "frank@example.com"));
            upsertWriter.upsert(row(2, "Grace", "grace@example.com"));
            upsertWriter.upsert(row(3, "Hank", "hank@example.com"));
            upsertWriter.flush();

            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_email");

            // Wait for name index to become visible.
            waitUntil(
                    () -> {
                        LookupResult r = nameLookuper.lookup(row("Frank")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_name entry 'Frank' to become visible");

            // Verify name lookup works.
            LookupResult frankResult = nameLookuper.lookup(row("Frank")).get();
            assertThat(frankResult.getRowList()).hasSize(1);
            InternalRow frankRow = frankResult.getRowList().get(0);
            assertThat(frankRow.getInt(0)).isEqualTo(1);
            assertThat(frankRow.getString(1).toString()).isEqualTo("Frank");
            assertThat(frankRow.getString(2).toString()).isEqualTo("frank@example.com");

            // Verify email lookup works.
            waitUntil(
                    () -> {
                        LookupResult r = emailLookuper.lookup(row("grace@example.com")).get();
                        return !r.getRowList().isEmpty();
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_email entry 'grace@example.com' to become visible");

            LookupResult graceResult = emailLookuper.lookup(row("grace@example.com")).get();
            assertThat(graceResult.getRowList()).hasSize(1);
            InternalRow graceRow = graceResult.getRowList().get(0);
            assertThat(graceRow.getInt(0)).isEqualTo(2);
            assertThat(graceRow.getString(1).toString()).isEqualTo("Grace");
            assertThat(graceRow.getString(2).toString()).isEqualTo("grace@example.com");

            // Verify non-existent value returns empty.
            LookupResult notFoundResult = nameLookuper.lookup(row("NonExistent")).get();
            assertThat(notFoundResult.getRowList()).isEmpty();

            LookupResult emailNotFoundResult =
                    emailLookuper.lookup(row("unknown@example.com")).get();
            assertThat(emailNotFoundResult.getRowList()).isEmpty();
        }
    }

    @Test
    void testSecondaryIndexMassiveDataTest() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_massive_data");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        int totalRows = 500;
        List<Object[]> testData = generateRandomTestData(totalRows);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            int batchSize = 1000;
            for (int i = 0; i < testData.size(); i += batchSize) {
                int endIdx = Math.min(i + batchSize, testData.size());
                for (int j = i; j < endIdx; j++) {
                    Object[] rowData = testData.get(j);
                    upsertWriter.upsert(row(rowData[0], rowData[1], rowData[2]));
                }
                upsertWriter.flush();
            }

            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");
            Lookuper emailLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_email");

            // generateRandomTestData yields a unique id, name and email for every row, so each
            // secondary-index lookup must resolve to exactly one row. Verifying *all* rows (not a
            // sample) makes this a total-count check: a single missing or duplicated index entry
            // fails the run.
            List<InternalRow> nameKeys = new ArrayList<>(testData.size());
            List<InternalRow> emailKeys = new ArrayList<>(testData.size());
            for (Object[] rowData : testData) {
                nameKeys.add(row(rowData[1]));
                emailKeys.add(row(rowData[2]));
            }
            List<LookupResult> nameResults =
                    waitForAllIndexEntries(nameLookuper, nameKeys, "all name index entries");
            List<LookupResult> emailResults =
                    waitForAllIndexEntries(emailLookuper, emailKeys, "all email index entries");

            for (int i = 0; i < testData.size(); i++) {
                Object[] rowData = testData.get(i);
                int id = (Integer) rowData[0];
                String nameVal = (String) rowData[1];
                String emailVal = (String) rowData[2];

                List<InternalRow> nameRows = nameResults.get(i).getRowList();
                assertThat(nameRows).hasSize(1);
                assertThat(nameRows.get(0).getInt(0)).isEqualTo(id);
                assertThat(nameRows.get(0).getString(1).toString()).isEqualTo(nameVal);
                assertThat(nameRows.get(0).getString(2).toString()).isEqualTo(emailVal);

                List<InternalRow> emailRows = emailResults.get(i).getRowList();
                assertThat(emailRows).hasSize(1);
                assertThat(emailRows.get(0).getInt(0)).isEqualTo(id);
                assertThat(emailRows.get(0).getString(1).toString()).isEqualTo(nameVal);
                assertThat(emailRows.get(0).getString(2).toString()).isEqualTo(emailVal);
            }

            // Negative anchor: values that were never written must resolve to no index entry, so a
            // bug returning rows for absent keys would be caught.
            assertThat(
                            nameLookuper
                                    .lookup(row("NoSuchName_42"))
                                    .get(INDEX_VISIBILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                    .getRowList())
                    .isEmpty();
            List<InternalRow> missingEmail =
                    emailLookuper
                            .lookup(row("missing.person.0@nowhere.invalid"))
                            .get(INDEX_VISIBILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                            .getRowList();
            assertThat(missingEmail).isEmpty();
        }
    }

    @Test
    void testSecondaryIndexLookupWithIntIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_int_index_lookup");

        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("product_id", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_product_id",
                                IndexType.SECONDARY,
                                Arrays.asList("product_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 1; i <= 15; i++) {
                upsertWriter.upsert(row(i, i, i * 100));
            }
            upsertWriter.flush();

            Lookuper indexLookuper =
                    ((FlussTable) table).getSecondaryIndexLookuper("idx_product_id");

            // Wait for all entries to become visible
            for (int i = 1; i <= 15; i++) {
                final int productId = i;
                waitUntil(
                        () -> !indexLookuper.lookup(row(productId)).get().getRowList().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "index lookup for product_id=" + productId);
            }

            // Verify specific lookup
            LookupResult result = indexLookuper.lookup(row(1)).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow resultRow = result.getRowList().get(0);
            assertThat(resultRow.getInt(0)).isEqualTo(1);
            assertThat(resultRow.getInt(1)).isEqualTo(1);
            assertThat(resultRow.getInt(2)).isEqualTo(100);
        }
    }

    @Test
    void testSecondaryIndexLookupWithBigintIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_bigint_index_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("order_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_order_id",
                                IndexType.SECONDARY,
                                Arrays.asList("order_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();
        createTable(tablePath, descriptor, true);

        long orderId = 9_000_000_000L; // beyond int range, exercises true BIGINT handling
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, orderId, 100));
            writer.flush();

            Lookuper indexLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_order_id");
            waitUntil(
                    () -> !indexLookuper.lookup(row(orderId)).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "index lookup for order_id=" + orderId);

            LookupResult result = indexLookuper.lookup(row(orderId)).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow resultRow = result.getRowList().get(0);
            assertThat(resultRow.getInt(0)).isEqualTo(1);
            assertThat(resultRow.getLong(1)).isEqualTo(orderId);
            assertThat(resultRow.getInt(2)).isEqualTo(100);
        }
    }

    @Test
    void testSecondaryIndexLookupWithDecimalIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_decimal_index_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("price", DataTypes.DECIMAL(10, 2))
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_price",
                                IndexType.SECONDARY,
                                Arrays.asList("price"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();
        createTable(tablePath, descriptor, true);

        BigDecimal priceValue = new BigDecimal("123.45");
        Decimal price = Decimal.fromBigDecimal(priceValue, 10, 2);
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, price, 100));
            writer.flush();

            Lookuper indexLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_price");
            waitUntil(
                    () ->
                            !indexLookuper
                                    .lookup(row(Decimal.fromBigDecimal(priceValue, 10, 2)))
                                    .get()
                                    .getRowList()
                                    .isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "index lookup for price=" + priceValue);

            LookupResult result =
                    indexLookuper.lookup(row(Decimal.fromBigDecimal(priceValue, 10, 2))).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow resultRow = result.getRowList().get(0);
            assertThat(resultRow.getInt(0)).isEqualTo(1);
            assertThat(resultRow.getDecimal(1, 10, 2).toBigDecimal())
                    .isEqualByComparingTo(priceValue);
            assertThat(resultRow.getInt(2)).isEqualTo(100);
        }
    }

    @Test
    void testSecondaryIndexLookupWithTimestampIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_timestamp_index_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_event_time",
                                IndexType.SECONDARY,
                                Arrays.asList("event_time"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();
        createTable(tablePath, descriptor, true);

        TimestampNtz eventTime = TimestampNtz.fromMillis(1_700_000_000_000L);
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, eventTime, 100));
            writer.flush();

            Lookuper indexLookuper =
                    ((FlussTable) table).getSecondaryIndexLookuper("idx_event_time");
            waitUntil(
                    () -> !indexLookuper.lookup(row(eventTime)).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "index lookup for event_time=" + eventTime);

            LookupResult result = indexLookuper.lookup(row(eventTime)).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow resultRow = result.getRowList().get(0);
            assertThat(resultRow.getInt(0)).isEqualTo(1);
            assertThat(resultRow.getTimestampNtz(1, 6)).isEqualTo(eventTime);
            assertThat(resultRow.getInt(2)).isEqualTo(100);
        }
    }

    @Test
    void testSecondaryIndexLookupWithBooleanIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_boolean_index_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("active", DataTypes.BOOLEAN())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_active",
                                IndexType.SECONDARY,
                                Arrays.asList("active"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "fact_id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, true, 100));
            writer.upsert(row(2, false, 200));
            writer.upsert(row(3, true, 300));
            writer.flush();

            Lookuper indexLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_active");
            // A boolean index is low-cardinality: lookup(true) must return both true rows.
            waitUntil(
                    () -> indexLookuper.lookup(row(true)).get().getRowList().size() == 2,
                    INDEX_VISIBILITY_TIMEOUT,
                    "index lookup for active=true returns both rows");

            List<Integer> trueFactIds = new ArrayList<>();
            for (InternalRow r : indexLookuper.lookup(row(true)).get().getRowList()) {
                assertThat(r.getBoolean(1)).isTrue();
                trueFactIds.add(r.getInt(0));
            }
            assertThat(trueFactIds).containsExactlyInAnyOrder(1, 3);

            LookupResult falseResult = indexLookuper.lookup(row(false)).get();
            assertThat(falseResult.getRowList()).hasSize(1);
            assertThat(falseResult.getRowList().get(0).getInt(0)).isEqualTo(2);
        }
    }

    @Test
    void testPartialUpdateOmittingNotNullIndexColumnIsRejected() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_pu_notnull_idx_rejected");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(false)) // NOT NULL index column
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Arrays.asList("user_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            // A partial update that omits the NOT NULL index column must be rejected at writer
            // construction: index columns are NOT exempted from the partial-update nullable rule.
            assertThatThrownBy(() -> table.newUpsert().partialUpdate("id", "note").createWriter())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Partial Update requires all columns except primary key to be nullable")
                    .hasMessageContaining("user_id");
        }
    }

    @Test
    void testPartialUpdateOmittingNullableIndexColumnIsAccepted() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_pu_nullable_idx_accepted");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(true)) // nullable index column
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index(
                                "idx_user",
                                IndexType.SECONDARY,
                                Arrays.asList("user_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            // A nullable index column may be omitted from a partial update (sparse index): writer
            // construction must succeed.
            UpsertWriter writer = table.newUpsert().partialUpdate("id", "note").createWriter();
            assertThat(writer).isNotNull();
        }
    }

    @Test
    void testSecondaryIndexLookupWithoutDistributedBy() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_no_dist_by_index");

        Schema schema =
                Schema.newBuilder()
                        .column("fact_id", DataTypes.INT())
                        .column("product_id", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("fact_id")
                        .index(
                                "idx_product_id",
                                IndexType.SECONDARY,
                                Arrays.asList("product_id"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor = TableDescriptor.builder().schema(schema).build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 1; i <= 15; i++) {
                upsertWriter.upsert(row(i, i, i * 100));
            }
            upsertWriter.flush();

            // Verify PK lookup on main table works
            Lookuper pkLookuper = table.newLookup().createLookuper();
            for (int i = 1; i <= 3; i++) {
                LookupResult pkResult = pkLookuper.lookup(row(i)).get();
                assertThat(pkResult.getRowList()).hasSize(1);
            }

            // Verify secondary index lookup
            Lookuper indexLookuper =
                    ((FlussTable) table).getSecondaryIndexLookuper("idx_product_id");

            for (int i = 1; i <= 15; i++) {
                final int pid = i;
                waitUntil(
                        () -> !indexLookuper.lookup(row(pid)).get().getRowList().isEmpty(),
                        INDEX_VISIBILITY_TIMEOUT,
                        "secondary index lookup for product_id=" + pid);
                LookupResult r = indexLookuper.lookup(row(pid)).get();
                assertThat(r.getRowList())
                        .as("Secondary index lookup for product_id=" + pid)
                        .hasSize(1);
            }
        }
    }

    @Test
    void testSparseIndexWithNullIndexColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sparse_null");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            // Write rows with NULL index columns — sparse index skips them
            upsertWriter.upsert(row(1, null, "test1@example.com"));
            upsertWriter.upsert(row(2, "TestName", null));
            upsertWriter.upsert(row(3, null, null));
            // Also write a normal row for contrast
            upsertWriter.upsert(row(4, "ValidName", "valid@example.com"));
            upsertWriter.flush();

            // Verify PK lookup still works for all rows
            Lookuper pkLookuper = table.newLookup().createLookuper();
            assertThat(pkLookuper.lookup(row(1)).get().getRowList()).hasSize(1);
            assertThat(pkLookuper.lookup(row(2)).get().getRowList()).hasSize(1);
            assertThat(pkLookuper.lookup(row(3)).get().getRowList()).hasSize(1);

            // Verify the valid row IS indexed
            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");
            waitUntil(
                    () -> !nameLookuper.lookup(row("ValidName")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "name index for ValidName");
            LookupResult validResult = nameLookuper.lookup(row("ValidName")).get();
            assertThat(validResult.getRowList()).hasSize(1);
            assertThat(validResult.getRowList().get(0).getInt(0)).isEqualTo(4);
        }
    }

    @Test
    void testSecondaryIndexLookupOnBytesColumn() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_bytes_sec_idx_lookup");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BYTES())
                        .primaryKey("id")
                        .index(
                                "idx_payload",
                                IndexType.SECONDARY,
                                Arrays.asList("payload"),
                                IndexVisibility.SYNC,
                                1)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        createTable(tablePath, descriptor, true);

        byte[] writtenPayload = new byte[] {1, 2, 3};
        byte[] lookupPayload = new byte[] {1, 2, 3};
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, writtenPayload));
            upsertWriter.flush();

            Lookuper lookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_payload");
            waitUntil(
                    () -> !lookuper.lookup(row(lookupPayload)).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "wait for idx_payload bytes entry");

            LookupResult result = lookuper.lookup(row(lookupPayload)).get();
            assertThat(result.getRowList()).hasSize(1);
            InternalRow row = result.getRowList().get(0);
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getBytes(1)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void testSparseIndexUpdateToNull() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_sparse_update_null");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            // Write a row with non-NULL index column
            upsertWriter.upsert(row(1, "Alice", "alice@example.com"));
            upsertWriter.flush();

            Lookuper nameLookuper = ((FlussTable) table).getSecondaryIndexLookuper("idx_name");

            // Verify index lookup works
            waitUntil(
                    () -> !nameLookuper.lookup(row("Alice")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "Alice should be findable");
            assertThat(nameLookuper.lookup(row("Alice")).get().getRowList()).hasSize(1);

            // Update the row to have NULL index column
            upsertWriter.upsert(row(1, null, "alice_updated@example.com"));
            upsertWriter.flush();

            // Verify main table has updated value
            Lookuper pkLookuper = table.newLookup().createLookuper();
            waitUntil(
                    () -> {
                        InternalRow r = pkLookuper.lookup(row(1)).get().getRowList().get(0);
                        return r.isNullAt(1);
                    },
                    INDEX_VISIBILITY_TIMEOUT,
                    "main table should reflect NULL name");
            InternalRow result = pkLookuper.lookup(row(1)).get().getRowList().get(0);
            assertThat(result.isNullAt(1)).isTrue();
            assertThat(result.getString(2).toString()).isEqualTo("alice_updated@example.com");

            // Verify old index entry is removed (lookup by "Alice" → empty)
            waitUntil(
                    () -> nameLookuper.lookup(row("Alice")).get().getRowList().isEmpty(),
                    INDEX_VISIBILITY_TIMEOUT,
                    "Alice index entry should be deleted");
            assertThat(nameLookuper.lookup(row("Alice")).get().getRowList()).isEmpty();
        }
    }

    @Test
    void testUpsertWithoutIndexNormalWrite() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_no_index_null");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();

            upsertWriter.upsert(row(1, null, "value1"));
            upsertWriter.upsert(row(2, "name2", null));
            upsertWriter.upsert(row(3, null, null));
            upsertWriter.flush();

            Lookuper lookuper = table.newLookup().createLookuper();

            InternalRow r1 = lookuper.lookup(row(1)).get().getRowList().get(0);
            assertThat(r1.getInt(0)).isEqualTo(1);
            assertThat(r1.isNullAt(1)).isTrue();
            assertThat(r1.getString(2).toString()).isEqualTo("value1");

            InternalRow r2 = lookuper.lookup(row(2)).get().getRowList().get(0);
            assertThat(r2.getInt(0)).isEqualTo(2);
            assertThat(r2.getString(1).toString()).isEqualTo("name2");
            assertThat(r2.isNullAt(2)).isTrue();

            InternalRow r3 = lookuper.lookup(row(3)).get().getRowList().get(0);
            assertThat(r3.getInt(0)).isEqualTo(3);
            assertThat(r3.isNullAt(1)).isTrue();
            assertThat(r3.isNullAt(2)).isTrue();
        }
    }

    @Test
    void testIndexColumnRemainsNullableForSparseIndex() throws Exception {
        TablePath tablePath = TablePath.of(DB, "test_nullable_idx_col");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .index(
                                "idx_name",
                                IndexType.SECONDARY,
                                Arrays.asList("name"),
                                IndexVisibility.SYNC,
                                3)
                        .index(
                                "idx_email",
                                IndexType.SECONDARY,
                                Arrays.asList("email"),
                                IndexVisibility.SYNC,
                                3)
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        createTable(tablePath, descriptor, true);

        // Verify schema nullability via admin API
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        Schema serverSchema = tableInfo.getSchema();
        RowType rowType = serverSchema.getRowType();

        assertThat(rowType.getField("name").getType().isNullable())
                .as("Index column 'name' should remain nullable for sparse index")
                .isTrue();
        assertThat(rowType.getField("email").getType().isNullable())
                .as("Index column 'email' should remain nullable for sparse index")
                .isTrue();
        assertThat(rowType.getField("id").getType().isNullable())
                .as("Primary key 'id' should be NOT NULL")
                .isFalse();
    }

    private static List<LookupResult> waitForAllIndexEntries(
            Lookuper lookuper, List<InternalRow> keys, String description) {
        AtomicReference<List<LookupResult>> successfulResults = new AtomicReference<>();
        waitUntil(
                () -> {
                    List<CompletableFuture<LookupResult>> futures = new ArrayList<>(keys.size());
                    for (InternalRow key : keys) {
                        futures.add(lookuper.lookup(key));
                    }
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .get(INDEX_VISIBILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                    List<LookupResult> results = new ArrayList<>(futures.size());
                    for (CompletableFuture<LookupResult> future : futures) {
                        LookupResult result = future.join();
                        if (result.getRowList().isEmpty()) {
                            return false;
                        }
                        results.add(result);
                    }
                    successfulResults.set(results);
                    return true;
                },
                INDEX_VISIBILITY_TIMEOUT,
                description + " should become visible");
        return successfulResults.get();
    }

    private List<Object[]> generateRandomTestData(int rowCount) {
        List<Object[]> testData = new ArrayList<>(rowCount);
        String[] firstNames = {
            "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
            "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina"
        };
        String[] lastNames = {
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson"
        };
        String[] domains = {"example.com", "test.org", "sample.net", "demo.io", "mock.edu"};

        for (int i = 0; i < rowCount; i++) {
            int id = i + 1;
            String firstName = firstNames[i % firstNames.length];
            String lastName = lastNames[(i / firstNames.length) % lastNames.length];
            String name = firstName + lastName + "_" + (i / (firstNames.length * lastNames.length));
            String domain = domains[i % domains.length];
            String email =
                    firstName.toLowerCase()
                            + "."
                            + lastName.toLowerCase()
                            + "."
                            + id
                            + "@"
                            + domain;
            testData.add(new Object[] {id, name, email});
        }
        return testData;
    }
}
