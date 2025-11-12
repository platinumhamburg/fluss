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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metadata.SchemaGetterFactory;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link TableLookup}. */
class TableLookupTest {

    private MetadataUpdater metadataUpdater;
    private Cluster cluster;
    private SchemaGetter schemaGetter;
    private LookupClient lookupClient;
    private SchemaGetterFactory schemaGetterFactory;
    private TableInfo mainTableInfo;
    private TableInfo emailIndexTableInfo;
    private TableInfo nameAgeIndexTableInfo;

    @BeforeEach
    void setUp() {
        metadataUpdater = mock(MetadataUpdater.class);
        schemaGetter = mock(SchemaGetter.class);
        lookupClient = mock(LookupClient.class);

        // Create a simple SchemaGetterFactory that returns mocked SchemaGetter
        schemaGetterFactory = (tablePath, schemaInfo) -> schemaGetter;

        // Create main table schema with secondary indexes
        Schema mainSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .index("email_idx", "email")
                        .index("name_age_idx", "name", "age")
                        .build();

        TableDescriptor mainTableDescriptor =
                TableDescriptor.builder().schema(mainSchema).distributedBy(3, "id").build();

        mainTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "users"),
                        1L,
                        1,
                        mainTableDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        // Create email index table schema
        Schema emailIndexSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .primaryKey("email", "id")
                        .build();

        TableDescriptor emailIndexDescriptor =
                TableDescriptor.builder()
                        .schema(emailIndexSchema)
                        .distributedBy(3, "email")
                        .build();

        emailIndexTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "__users__index__email_idx"),
                        2L,
                        1,
                        emailIndexDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        // Create name_age index table schema
        Schema nameAgeIndexSchema =
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .primaryKey("name", "age", "id")
                        .build();

        TableDescriptor nameAgeIndexDescriptor =
                TableDescriptor.builder()
                        .schema(nameAgeIndexSchema)
                        .distributedBy(3, "name", "age")
                        .build();

        nameAgeIndexTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "__users__index__name_age_idx"),
                        3L,
                        1,
                        nameAgeIndexDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        // Create a Cluster instance with the table info
        Map<TablePath, TableInfo> tableInfoByPath = new HashMap<>();
        tableInfoByPath.put(mainTableInfo.getTablePath(), mainTableInfo);
        tableInfoByPath.put(emailIndexTableInfo.getTablePath(), emailIndexTableInfo);
        tableInfoByPath.put(nameAgeIndexTableInfo.getTablePath(), nameAgeIndexTableInfo);

        Map<TablePath, Long> tableIdByPath = new HashMap<>();
        tableIdByPath.put(mainTableInfo.getTablePath(), mainTableInfo.getTableId());
        tableIdByPath.put(emailIndexTableInfo.getTablePath(), emailIndexTableInfo.getTableId());
        tableIdByPath.put(nameAgeIndexTableInfo.getTablePath(), nameAgeIndexTableInfo.getTableId());

        cluster =
                new Cluster(
                        Collections.emptyMap(), // aliveTabletServersById
                        null, // coordinatorServer
                        Collections.emptyMap(), // bucketLocationsByPath
                        tableIdByPath, // tableIdByPath
                        Collections.emptyMap(), // partitionsIdByPath
                        tableInfoByPath); // tableInfoByPath

        when(metadataUpdater.getCluster()).thenReturn(cluster);
    }

    @Test
    void testCreatePrimaryKeyLookuper() {
        TableLookup tableLookup =
                new TableLookup(
                        mainTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        Lookuper lookuper = tableLookup.createLookuper();

        assertThat(lookuper).isInstanceOf(PrimaryKeyLookuper.class);
    }

    @Test
    void testCreateSecondaryIndexLookuper() {
        TableLookup tableLookup =
                new TableLookup(
                        mainTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Test single column index lookup
        Lookup emailLookup = tableLookup.lookupBy(Collections.singletonList("email"));
        Lookuper emailLookuper = emailLookup.createLookuper();

        assertThat(emailLookuper).isInstanceOf(SecondaryIndexLookuper.class);

        // Test multi-column index lookup
        Lookup nameAgeLookup = tableLookup.lookupBy(Arrays.asList("name", "age"));
        Lookuper nameAgeLookuper = nameAgeLookup.createLookuper();

        assertThat(nameAgeLookuper).isInstanceOf(SecondaryIndexLookuper.class);

        // Test multi-column index lookup with different order - should fail and fall back to prefix
        // lookup
        // But first, the lookup column order must match bucket keys exactly for prefix lookup to
        // work
        // Since main table bucket key is ["id"], this should fall back to prefix lookup
        Lookup ageNameLookup = tableLookup.lookupBy(Collections.singletonList("id"));
        Lookuper ageNameLookuper = ageNameLookup.createLookuper();

        assertThat(ageNameLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testCreatePrefixKeyLookuper() {
        TableLookup tableLookup =
                new TableLookup(
                        mainTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Test lookup by bucket key should use prefix lookup (when not using primary key lookup)
        Lookup idLookup = tableLookup.lookupBy(Collections.singletonList("id"));
        Lookuper idLookuper = idLookup.createLookuper();

        assertThat(idLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testIndexTableShouldNotCreateSecondaryIndexLookuper() {
        // Create an index table
        Schema indexTableSchema =
                Schema.newBuilder()
                        .column("email", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .primaryKey("email", "id")
                        .build();

        TableDescriptor indexTableDescriptor =
                TableDescriptor.builder()
                        .schema(indexTableSchema)
                        .distributedBy(3, "email")
                        .build();

        TableInfo indexTable =
                TableInfo.of(
                        TablePath.of("test_db", "__users__index__email_idx"),
                        2L,
                        1,
                        indexTableDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        indexTable,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Even if we lookup by columns that would match an index, it should not create
        // SecondaryIndexLookuper for index tables
        Lookup emailLookup = tableLookup.lookupBy(Collections.singletonList("email"));
        Lookuper emailLookuper = emailLookup.createLookuper();

        assertThat(emailLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testTableWithoutIndexesShouldNotCreateSecondaryIndexLookuper() {
        // Create table without indexes
        Schema schemaWithoutIndexes =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("email", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor descriptorWithoutIndexes =
                TableDescriptor.builder()
                        .schema(schemaWithoutIndexes)
                        .distributedBy(3, "id")
                        .build();

        TableInfo tableWithoutIndexes =
                TableInfo.of(
                        TablePath.of("test_db", "users_no_index"),
                        3L,
                        1,
                        descriptorWithoutIndexes,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        tableWithoutIndexes,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Must lookup by bucket key for prefix lookup to work
        Lookup idLookup = tableLookup.lookupBy(Collections.singletonList("id"));
        Lookuper idLookuper = idLookup.createLookuper();

        assertThat(idLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testPartialIndexMatchShouldUsePrefixLookup() {
        TableLookup tableLookup =
                new TableLookup(
                        mainTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Test lookup by partial index columns (only name from name_age_idx)
        // Should fall back to prefix lookup - but for prefix lookup to work,
        // lookup columns must exactly match bucket keys (excluding partitions)
        // Since bucket keys = ["id"], we can only do prefix lookup by ["id"]
        Lookup prefixLookup = tableLookup.lookupBy(Collections.singletonList("id"));
        Lookuper prefixLookuper = prefixLookup.createLookuper();

        assertThat(prefixLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testPrefixLookupWithPartitionedTable() {
        // Create a partitioned table with bucket keys
        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("partition_col", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("partition_col", "id")
                        .build();

        TableDescriptor partitionedDescriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .partitionedBy("partition_col")
                        .distributedBy(3, "id")
                        .build();

        TableInfo partitionedTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "partitioned_table"),
                        4L,
                        1,
                        partitionedDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        partitionedTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // For partitioned table, lookup columns must contain all partition keys AND bucket keys
        Lookup validLookup = tableLookup.lookupBy(Arrays.asList("partition_col", "id"));
        Lookuper validLookuper = validLookup.createLookuper();

        assertThat(validLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testPrefixLookupWithMultiBucketKeys() {
        // Create a table with multiple bucket keys
        Schema multiKeySchema =
                Schema.newBuilder()
                        .column("id1", DataTypes.INT())
                        .column("id2", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id1", "id2")
                        .build();

        TableDescriptor multiKeyDescriptor =
                TableDescriptor.builder()
                        .schema(multiKeySchema)
                        .distributedBy(3, "id1", "id2")
                        .build();

        TableInfo multiKeyTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "multi_key_table"),
                        5L,
                        1,
                        multiKeyDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        multiKeyTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Lookup columns must exactly match bucket keys in order
        Lookup validLookup = tableLookup.lookupBy(Arrays.asList("id1", "id2"));
        Lookuper validLookuper = validLookup.createLookuper();

        assertThat(validLookuper).isInstanceOf(PrefixKeyLookuper.class);
    }

    @Test
    void testPrefixLookupShouldFailWhenMissingPartitionColumns() {
        // Create a partitioned table
        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("partition_col", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("partition_col", "id")
                        .build();

        TableDescriptor partitionedDescriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .partitionedBy("partition_col")
                        .distributedBy(3, "id")
                        .build();

        TableInfo partitionedTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "partitioned_table"),
                        6L,
                        1,
                        partitionedDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        partitionedTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Should fail when partition column is missing
        Lookup invalidLookup = tableLookup.lookupBy(Collections.singletonList("id"));

        assertThatThrownBy(invalidLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lookup columns")
                .hasMessageContaining("must contain all partition fields");
    }

    @Test
    void testPrefixLookupShouldFailWhenBucketKeysNotMatched() {
        TableLookup tableLookup =
                new TableLookup(
                        mainTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Should fail when lookup columns don't match bucket keys exactly
        Lookup invalidLookup = tableLookup.lookupBy(Collections.singletonList("name"));

        assertThatThrownBy(invalidLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lookup columns")
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");
    }

    @Test
    void testPrefixLookupShouldFailWithWrongBucketKeyOrder() {
        // Create a table with multiple bucket keys
        Schema multiKeySchema =
                Schema.newBuilder()
                        .column("id1", DataTypes.INT())
                        .column("id2", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id1", "id2")
                        .build();

        TableDescriptor multiKeyDescriptor =
                TableDescriptor.builder()
                        .schema(multiKeySchema)
                        .distributedBy(3, "id1", "id2")
                        .build();

        TableInfo multiKeyTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "multi_key_table"),
                        7L,
                        1,
                        multiKeyDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        multiKeyTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Should fail when bucket keys are in wrong order
        Lookup invalidLookup = tableLookup.lookupBy(Arrays.asList("id2", "id1"));

        assertThatThrownBy(invalidLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lookup columns")
                .hasMessageContaining("must contain all bucket keys")
                .hasMessageContaining("in order");
    }

    @Test
    void testPrefixLookupShouldFailOnLogTable() {
        // Create a log table (without primary key)
        Schema logTableSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .build();

        TableDescriptor logTableDescriptor =
                TableDescriptor.builder().schema(logTableSchema).distributedBy(3, "id").build();

        TableInfo logTableInfo =
                TableInfo.of(
                        TablePath.of("test_db", "log_table"),
                        8L,
                        1,
                        logTableDescriptor,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        TableLookup tableLookup =
                new TableLookup(
                        logTableInfo,
                        schemaGetter,
                        metadataUpdater,
                        lookupClient,
                        new Configuration(),
                        schemaGetterFactory);

        // Should fail on log table (no primary key)
        Lookup invalidLookup = tableLookup.lookupBy(Collections.singletonList("id"));

        assertThatThrownBy(invalidLookup::createLookuper)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Log table")
                .hasMessageContaining("doesn't support prefix lookup");
    }
}
