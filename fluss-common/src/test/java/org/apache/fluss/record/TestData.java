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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;

/** utils to create test data. */
public final class TestData {
    public static final short DEFAULT_SCHEMA_ID = 1;
    public static final long BASE_OFFSET = 0L;
    public static final byte DEFAULT_MAGIC = CURRENT_LOG_MAGIC_VALUE;
    // ---------------------------- data1 and related table info begin ---------------------------
    public static final List<Object[]> DATA1 =
            Arrays.asList(
                    new Object[] {1, "a"},
                    new Object[] {2, "b"},
                    new Object[] {3, "c"},
                    new Object[] {4, "d"},
                    new Object[] {5, "e"},
                    new Object[] {6, "f"},
                    new Object[] {7, "g"},
                    new Object[] {8, "h"},
                    new Object[] {9, "i"},
                    new Object[] {10, "j"});
    public static final List<Object[]> ANOTHER_DATA1 =
            Arrays.asList(
                    new Object[] {1, "a1"},
                    new Object[] {2, "b1"},
                    new Object[] {3, "c1"},
                    new Object[] {4, "d1"},
                    new Object[] {5, "e1"},
                    new Object[] {6, "f1"},
                    new Object[] {7, "g1"},
                    new Object[] {8, "h1"},
                    new Object[] {9, "i1"},
                    new Object[] {10, "j1"});

    public static final RowType DATA1_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()), new DataField("b", DataTypes.STRING()));

    // for log table
    public static final long DATA1_TABLE_ID = 150001L;
    public static final Schema DATA1_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .build();
    public static final TablePath DATA1_TABLE_PATH =
            TablePath.of("test_db_1", "test_non_pk_table_1");

    public static final TableDescriptor DATA1_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(3).build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(DATA1_TABLE_PATH);

    private static final long currentMillis = System.currentTimeMillis();
    public static final TableInfo DATA1_TABLE_INFO =
            TableInfo.of(
                    DATA1_TABLE_PATH,
                    DATA1_TABLE_ID,
                    1,
                    DATA1_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // for log table / partition table
    public static final TableDescriptor DATA1_PARTITIONED_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DATA1_SCHEMA)
                    .distributedBy(3)
                    .partitionedBy("b")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
                    .build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PA_2024 =
            PhysicalTablePath.of(DATA1_TABLE_PATH, "2024");

    // for pk table
    public static final RowType DATA1_KEY_TYPE = DataTypes.ROW(new DataField("a", DataTypes.INT()));
    public static final Schema DATA1_SCHEMA_PK =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .primaryKey("a")
                    .build();
    public static final TablePath DATA1_TABLE_PATH_PK =
            TablePath.of("test_db_1", "test_pk_table_1");
    public static final long DATA1_TABLE_ID_PK = 150003L;

    public static final TableDescriptor DATA1_TABLE_DESCRIPTOR_PK =
            TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(3, "a").build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PK =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK);
    public static final TableInfo DATA1_TABLE_INFO_PK =
            TableInfo.of(
                    DATA1_TABLE_PATH_PK,
                    DATA1_TABLE_ID_PK,
                    1,
                    DATA1_TABLE_DESCRIPTOR_PK,
                    currentMillis,
                    currentMillis);

    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PK_PA_2024 =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "2024");

    public static final List<Tuple2<Object[], Object[]>> DATA_1_WITH_KEY_AND_VALUE =
            Arrays.asList(
                    Tuple2.of(new Object[] {1}, new Object[] {1, "a"}),
                    Tuple2.of(new Object[] {2}, new Object[] {2, "b"}),
                    Tuple2.of(new Object[] {3}, new Object[] {3, "c"}),
                    Tuple2.of(new Object[] {1}, new Object[] {1, "a1"}),
                    Tuple2.of(new Object[] {2}, new Object[] {2, "b1"}),
                    Tuple2.of(new Object[] {3}, null));

    public static final List<Tuple2<ChangeType, Object[]>> EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK =
            Arrays.asList(
                    Tuple2.of(ChangeType.INSERT, new Object[] {1, "a"}),
                    Tuple2.of(ChangeType.INSERT, new Object[] {2, "b"}),
                    Tuple2.of(ChangeType.INSERT, new Object[] {3, "c"}),
                    Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {1, "a"}),
                    Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {1, "a1"}),
                    Tuple2.of(ChangeType.UPDATE_BEFORE, new Object[] {2, "b"}),
                    Tuple2.of(ChangeType.UPDATE_AFTER, new Object[] {2, "b1"}),
                    Tuple2.of(ChangeType.DELETE, new Object[] {3, "c"}));

    // ---------------------------- data1 table info end ------------------------------

    // ------------------- data2 and related table info begin ----------------------
    public static final List<Object[]> DATA2 =
            Arrays.asList(
                    new Object[] {1, "a", "hello"},
                    new Object[] {2, "b", "hi"},
                    new Object[] {3, "c", "nihao"},
                    new Object[] {4, "d", "hello world"},
                    new Object[] {5, "e", "hi world"},
                    new Object[] {6, "f", "nihao world"},
                    new Object[] {7, "g", "hello world2"},
                    new Object[] {8, "h", "hi world2"},
                    new Object[] {9, "i", "nihao world2"},
                    new Object[] {10, "j", "hello world3"});

    public static final RowType DATA2_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("c", DataTypes.STRING()));
    public static final Schema DATA2_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.STRING())
                    .withComment("c is adding column")
                    .primaryKey("a")
                    .build();
    public static final TablePath DATA2_TABLE_PATH = TablePath.of("test_db_2", "test_table_2");
    public static final PhysicalTablePath DATA2_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(DATA2_TABLE_PATH);
    public static final long DATA2_TABLE_ID = 150002L;
    public static final TableDescriptor DATA2_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(DATA2_SCHEMA).distributedBy(3, "a").build();
    public static final TableInfo DATA2_TABLE_INFO =
            TableInfo.of(
                    DATA2_TABLE_PATH,
                    DATA2_TABLE_ID,
                    1,
                    DATA2_TABLE_DESCRIPTOR,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
    // -------------------------------- data2 info end ------------------------------------

    // ------------------- data3 and related table info begin ----------------------
    public static final Schema DATA3_SCHEMA_PK =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.BIGINT())
                    .withComment("b is second column")
                    .primaryKey("a")
                    .build();
    // ---------------------------- data3 table info end ------------------------------

    // ------------------- indexed table and related table info begin ----------------------
    public static final List<Object[]> INDEXED_DATA =
            Arrays.asList(
                    new Object[] {1, "Alice", "alice@example.com"},
                    new Object[] {2, "Bob", "bob@example.com"},
                    new Object[] {3, "Charlie", "charlie@example.com"},
                    new Object[] {4, "Diana", "diana@example.com"},
                    new Object[] {5, "Eve", "eve@example.com"});

    // Index table bucket count constant
    public static final int INDEX_TABLE_BUCKET_COUNT = 3;

    public static final RowType INDEXED_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()));

    public static final RowType INDEXED_KEY_TYPE =
            DataTypes.ROW(new DataField("id", DataTypes.INT()));

    public static final Schema INDEXED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .withComment("id is the primary key")
                    .column("name", DataTypes.STRING())
                    .withComment("name with index")
                    .column("email", DataTypes.STRING())
                    .withComment("email with index")
                    .primaryKey("id")
                    .index("idx_name", "name")
                    .index("idx_email", "email")
                    .build();

    public static final TablePath INDEXED_TABLE_PATH = TablePath.of("test_db", "indexed_table");
    public static final long INDEXED_TABLE_ID = 200001L;

    public static final TableDescriptor INDEXED_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(INDEXED_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                    .build();
    public static final PhysicalTablePath INDEXED_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(INDEXED_TABLE_PATH);
    public static final TableInfo INDEXED_TABLE_INFO =
            TableInfo.of(
                    INDEXED_TABLE_PATH,
                    INDEXED_TABLE_ID,
                    1,
                    INDEXED_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    public static final List<Tuple2<Object[], Object[]>> INDEXED_DATA_WITH_KEY_AND_VALUE =
            Arrays.asList(
                    Tuple2.of(new Object[] {1}, new Object[] {1, "Alice", "alice@example.com"}),
                    Tuple2.of(new Object[] {2}, new Object[] {2, "Bob", "bob@example.com"}),
                    Tuple2.of(new Object[] {3}, new Object[] {3, "Charlie", "charlie@example.com"}),
                    Tuple2.of(new Object[] {4}, new Object[] {4, "Diana", "diana@example.com"}),
                    Tuple2.of(new Object[] {5}, new Object[] {5, "Eve", "eve@example.com"}));

    // ------------------- index table definitions for INDEXED_SCHEMA begin ----------------------
    /** Schema definitions for index tables derived from INDEXED_SCHEMA. */

    // idx_name index table schema and related data
    public static final RowType IDX_NAME_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("name", DataTypes.STRING()),
                    new DataField("id", DataTypes.INT()),
                    new DataField("__offset", DataTypes.BIGINT()));

    public static final List<String> IDX_NAME_BUCKET_KEYS = Arrays.asList("name");
    public static final List<String> IDX_NAME_PRIMARY_KEYS = Arrays.asList("name", "id");

    public static final Schema IDX_NAME_SCHEMA =
            Schema.newBuilder()
                    .column("name", DataTypes.STRING())
                    .withComment("name with index")
                    .column("id", DataTypes.INT())
                    .withComment("id is the primary key")
                    .column("__offset", DataTypes.BIGINT())
                    .primaryKeyNamed("pk_idx_name", IDX_NAME_PRIMARY_KEYS)
                    .build();

    public static final TablePath IDX_NAME_TABLE_PATH =
            TablePath.of("test_db", "__indexed_table__index__idx_name");
    public static final long IDX_NAME_TABLE_ID = 200002L;

    public static final TableDescriptor IDX_NAME_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(IDX_NAME_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, IDX_NAME_BUCKET_KEYS)
                    .build();
    public static final PhysicalTablePath IDX_NAME_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(IDX_NAME_TABLE_PATH);
    public static final TableInfo IDX_NAME_TABLE_INFO =
            TableInfo.of(
                    IDX_NAME_TABLE_PATH,
                    IDX_NAME_TABLE_ID,
                    1,
                    IDX_NAME_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // idx_email index table schema and related data
    public static final RowType IDX_EMAIL_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("email", DataTypes.STRING()),
                    new DataField("id", DataTypes.INT()),
                    new DataField("__offset", DataTypes.BIGINT()));

    public static final List<String> IDX_EMAIL_BUCKET_KEYS = Arrays.asList("email");
    public static final List<String> IDX_EMAIL_PRIMARY_KEYS = Arrays.asList("email", "id");

    public static final Schema IDX_EMAIL_SCHEMA =
            Schema.newBuilder()
                    .column("email", DataTypes.STRING())
                    .withComment("email with index")
                    .column("id", DataTypes.INT())
                    .withComment("id is the primary key")
                    .column("__offset", DataTypes.BIGINT())
                    .primaryKeyNamed("pk_idx_email", IDX_EMAIL_PRIMARY_KEYS)
                    .build();

    public static final TablePath IDX_EMAIL_TABLE_PATH =
            TablePath.of("test_db", "__indexed_table__index__idx_email");
    public static final long IDX_EMAIL_TABLE_ID = 200003L;

    public static final TableDescriptor IDX_EMAIL_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(IDX_EMAIL_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, IDX_EMAIL_BUCKET_KEYS)
                    .build();
    public static final PhysicalTablePath IDX_EMAIL_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(IDX_EMAIL_TABLE_PATH);
    public static final TableInfo IDX_EMAIL_TABLE_INFO =
            TableInfo.of(
                    IDX_EMAIL_TABLE_PATH,
                    IDX_EMAIL_TABLE_ID,
                    1,
                    IDX_EMAIL_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // Test data for index tables based on INDEXED_DATA
    public static final List<Object[]> IDX_NAME_DATA =
            Arrays.asList(
                    new Object[] {"Alice", 1, 0L},
                    new Object[] {"Bob", 2, 1L},
                    new Object[] {"Charlie", 3, 2L},
                    new Object[] {"Diana", 4, 3L},
                    new Object[] {"Eve", 5, 4L});

    public static final List<Object[]> IDX_EMAIL_DATA =
            Arrays.asList(
                    new Object[] {"alice@example.com", 1, 0L},
                    new Object[] {"bob@example.com", 2, 1L},
                    new Object[] {"charlie@example.com", 3, 2L},
                    new Object[] {"diana@example.com", 4, 3L},
                    new Object[] {"eve@example.com", 5, 4L});

    public static final List<Tuple2<Object[], Object[]>> IDX_NAME_DATA_WITH_KEY_AND_VALUE =
            Arrays.asList(
                    Tuple2.of(new Object[] {"Alice", 1}, new Object[] {"Alice", 1, 0L}),
                    Tuple2.of(new Object[] {"Bob", 2}, new Object[] {"Bob", 2, 1L}),
                    Tuple2.of(new Object[] {"Charlie", 3}, new Object[] {"Charlie", 3, 2L}),
                    Tuple2.of(new Object[] {"Diana", 4}, new Object[] {"Diana", 4, 3L}),
                    Tuple2.of(new Object[] {"Eve", 5}, new Object[] {"Eve", 5, 4L}));

    public static final List<Tuple2<Object[], Object[]>> IDX_EMAIL_DATA_WITH_KEY_AND_VALUE =
            Arrays.asList(
                    Tuple2.of(
                            new Object[] {"alice@example.com", 1},
                            new Object[] {"alice@example.com", 1, 0L}),
                    Tuple2.of(
                            new Object[] {"bob@example.com", 2},
                            new Object[] {"bob@example.com", 2, 1L}),
                    Tuple2.of(
                            new Object[] {"charlie@example.com", 3},
                            new Object[] {"charlie@example.com", 3, 2L}),
                    Tuple2.of(
                            new Object[] {"diana@example.com", 4},
                            new Object[] {"diana@example.com", 4, 3L}),
                    Tuple2.of(
                            new Object[] {"eve@example.com", 5},
                            new Object[] {"eve@example.com", 5, 4L}));

    // ------------------- index table definitions end ----------------------

    // ------------------- index cache test data begin ----------------------
    /** Test data specifically for index cache operations with diverse offset patterns. */
    public static final List<Object[]> INDEX_CACHE_DATA =
            Arrays.asList(
                    new Object[] {100, "test1", "test1@example.com"},
                    new Object[] {101, "test2", "test2@example.com"},
                    new Object[] {102, "test3", "test3@example.com"},
                    new Object[] {110, "test4", "test4@example.com"}, // Gap to test sparse offsets
                    new Object[] {111, "test5", "test5@example.com"},
                    new Object[] {120, "test6", "test6@example.com"}, // Another gap
                    new Object[] {121, "test7", "test7@example.com"},
                    new Object[] {122, "test8", "test8@example.com"},
                    new Object[] {130, "test9", "test9@example.com"}, // Final gap
                    new Object[] {131, "test10", "test10@example.com"});

    /** Large dataset for performance and batch operations testing. */
    public static final List<Object[]> INDEX_CACHE_COLD_WAL_DATA = generateLargeIndexData(1000);

    /** Helper method to generate large test datasets compatible with INDEXED_SCHEMA. */
    private static List<Object[]> generateLargeIndexData(int size) {
        List<Object[]> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(new Object[] {i, "name_" + i, "user" + i + "@example.com"});
        }
        return data;
    }

    /** Test offsets for sparse range testing. */
    public static final long[] INDEX_CACHE_SPARSE_OFFSETS = {
        0L, 5L, 10L, 15L, 25L, 30L, 40L, 45L, 50L, 100L
    };

    /** Test offsets for continuous range testing. */
    public static final long[] INDEX_CACHE_CONTINUOUS_OFFSETS = {
        0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L
    };

    /** Common page sizes for testing memory management. */
    public static final int[] INDEX_CACHE_TEST_PAGE_SIZES = {1024, 4096, 8192, 16384, 32768, 65536};

    /** Default test page size for most tests. */
    public static final int INDEX_CACHE_DEFAULT_TEST_PAGE_SIZE = 4096;

    // ---------------------------- index cache test data end ------------------------------

    // ------------------- partitioned indexed table and related table info begin
    // ----------------------
    /**
     * Test data for partitioned table with global secondary index. Note: While the main table is
     * partitioned, the associated index tables are non-partitioned and adapt to partition changes
     * through TabletServerMetadataCache.
     */
    public static final List<Object[]> PARTITIONED_INDEXED_DATA =
            Arrays.asList(
                    new Object[] {1, "Alice", "alice@example.com", "2023"},
                    new Object[] {2, "Bob", "bob@example.com", "2023"},
                    new Object[] {3, "Charlie", "charlie@example.com", "2024"},
                    new Object[] {4, "Diana", "diana@example.com", "2024"},
                    new Object[] {5, "Eve", "eve@example.com", "2025"},
                    new Object[] {6, "Frank", "frank@example.com", "2025"});

    public static final RowType PARTITIONED_INDEXED_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()),
                    new DataField("year", DataTypes.STRING()));

    public static final RowType PARTITIONED_INDEXED_KEY_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("year", DataTypes.STRING()));

    public static final Schema PARTITIONED_INDEXED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .withComment("id is part of the primary key")
                    .column("name", DataTypes.STRING())
                    .withComment("name with index")
                    .column("email", DataTypes.STRING())
                    .withComment("email with index")
                    .column("year", DataTypes.STRING())
                    .withComment("year is partition key")
                    .primaryKey("id", "year")
                    .index("idx_name", "name")
                    .index("idx_email", "email")
                    .build();

    public static final TablePath PARTITIONED_INDEXED_TABLE_PATH =
            TablePath.of("test_db", "partitioned_indexed_table");
    public static final long PARTITIONED_INDEXED_TABLE_ID = 300001L;

    public static final TableDescriptor PARTITIONED_INDEXED_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(PARTITIONED_INDEXED_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                    .partitionedBy("year")
                    .property("table.auto-partition.enabled", "true")
                    .build();

    public static final PhysicalTablePath PARTITIONED_INDEXED_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(PARTITIONED_INDEXED_TABLE_PATH);
    public static final TableInfo PARTITIONED_INDEXED_TABLE_INFO =
            TableInfo.of(
                    PARTITIONED_INDEXED_TABLE_PATH,
                    PARTITIONED_INDEXED_TABLE_ID,
                    1,
                    PARTITIONED_INDEXED_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // Index table paths for partitioned main table
    // Note: Index tables are always non-partitioned, even when main table is partitioned
    public static final TablePath PARTITIONED_IDX_NAME_TABLE_PATH =
            TablePath.of("test_db", "__partitioned_indexed_table__index__idx_name");
    public static final TablePath PARTITIONED_IDX_EMAIL_TABLE_PATH =
            TablePath.of("test_db", "__partitioned_indexed_table__index__idx_email");

    // ------------------- partitioned indexed table info end ----------------------
    // ---------------------------- indexed table info end ------------------------------

    // ------------------- indexed table with complex types begin ----------------------
    /**
     * Schema with complex types (MAP) to test that TableCacheWriter only creates FieldGetters for
     * index columns, not for complex types that are not supported by InternalRow.createFieldGetter.
     */
    public static final RowType INDEXED_WITH_MAP_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()),
                    new DataField(
                            "metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

    public static final Schema INDEXED_WITH_MAP_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .withComment("id is the primary key")
                    .column("name", DataTypes.STRING())
                    .withComment("name with index")
                    .column("email", DataTypes.STRING())
                    .withComment("email with index")
                    .column("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .withComment("metadata is a MAP column without index")
                    .primaryKey("id")
                    .index("idx_name", "name")
                    .index("idx_email", "email")
                    .build();

    public static final TablePath INDEXED_WITH_MAP_TABLE_PATH =
            TablePath.of("test_db", "indexed_table_with_map");
    public static final long INDEXED_WITH_MAP_TABLE_ID = 200002L;

    public static final TableDescriptor INDEXED_WITH_MAP_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(INDEXED_WITH_MAP_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, "id")
                    .build();

    public static final PhysicalTablePath INDEXED_WITH_MAP_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(INDEXED_WITH_MAP_TABLE_PATH);
    public static final TableInfo INDEXED_WITH_MAP_TABLE_INFO =
            TableInfo.of(
                    INDEXED_WITH_MAP_TABLE_PATH,
                    INDEXED_WITH_MAP_TABLE_ID,
                    1,
                    INDEXED_WITH_MAP_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // Index table info for idx_name on INDEXED_WITH_MAP_SCHEMA
    public static final Schema IDX_NAME_WITH_MAP_SCHEMA =
            Schema.newBuilder()
                    .column("name", DataTypes.STRING())
                    .withComment("indexed column name")
                    .column("id", DataTypes.INT())
                    .withComment("main table primary key")
                    .column("__offset", DataTypes.BIGINT())
                    .withComment("log offset for lookup")
                    .primaryKey("name", "id")
                    .build();

    public static final TablePath IDX_NAME_WITH_MAP_TABLE_PATH =
            TablePath.of("test_db", "__indexed_table_with_map__index__idx_name");
    public static final long IDX_NAME_WITH_MAP_TABLE_ID = 200012L;

    public static final TableDescriptor IDX_NAME_WITH_MAP_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(IDX_NAME_WITH_MAP_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, "name")
                    .build();

    public static final TableInfo IDX_NAME_WITH_MAP_TABLE_INFO =
            TableInfo.of(
                    IDX_NAME_WITH_MAP_TABLE_PATH,
                    IDX_NAME_WITH_MAP_TABLE_ID,
                    1,
                    IDX_NAME_WITH_MAP_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);

    // Index table info for idx_email on INDEXED_WITH_MAP_SCHEMA
    public static final Schema IDX_EMAIL_WITH_MAP_SCHEMA =
            Schema.newBuilder()
                    .column("email", DataTypes.STRING())
                    .withComment("indexed column email")
                    .column("id", DataTypes.INT())
                    .withComment("main table primary key")
                    .column("__offset", DataTypes.BIGINT())
                    .withComment("log offset for lookup")
                    .primaryKey("email", "id")
                    .build();

    public static final TablePath IDX_EMAIL_WITH_MAP_TABLE_PATH =
            TablePath.of("test_db", "__indexed_table_with_map__index__idx_email");
    public static final long IDX_EMAIL_WITH_MAP_TABLE_ID = 200022L;

    public static final TableDescriptor IDX_EMAIL_WITH_MAP_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(IDX_EMAIL_WITH_MAP_SCHEMA)
                    .distributedBy(INDEX_TABLE_BUCKET_COUNT, "email")
                    .build();

    public static final TableInfo IDX_EMAIL_WITH_MAP_TABLE_INFO =
            TableInfo.of(
                    IDX_EMAIL_WITH_MAP_TABLE_PATH,
                    IDX_EMAIL_WITH_MAP_TABLE_ID,
                    1,
                    IDX_EMAIL_WITH_MAP_TABLE_DESCRIPTOR,
                    currentMillis,
                    currentMillis);
    // ------------------- indexed table with complex types end ----------------------
}
