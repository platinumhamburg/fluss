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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.lookup.LookupType;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupNormalizer}. */
class LookupNormalizerTest {

    // Schema: id(0) INT, name(1) VARCHAR(200), email(2) VARCHAR(200), age(3) INT
    // PK = (id), bucket_key = (id)
    private static final RowType TABLE_SCHEMA =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("id", new IntType()),
                            new RowType.RowField("name", new VarCharType(200)),
                            new RowType.RowField("email", new VarCharType(200)),
                            new RowType.RowField("age", new IntType())));

    private static final int[] PRIMARY_KEYS = new int[] {0}; // id
    private static final int[] BUCKET_KEYS = new int[] {0}; // id
    private static final int[] PARTITION_KEYS = new int[] {};

    @Test
    void testLookupKeyMatchesPrimaryKey() {
        // Lookup on PK column (id) -> should produce LOOKUP type
        int[][] lookupKeyIndexes = new int[][] {{0}}; // id
        int[][] secondaryIndexes = new int[][] {{1}}; // name index exists but PK wins

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.LOOKUP);
    }

    @Test
    void testLookupKeyMatchesSecondaryIndex() {
        // Lookup on name (index 1) -> should match secondary index on name
        int[][] lookupKeyIndexes = new int[][] {{1}}; // name
        int[][] secondaryIndexes = new int[][] {{1}}; // secondary index on name

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
    }

    @Test
    void testLookupKeyMatchesMultiColumnSecondaryIndex() {
        // Lookup on (name, age) -> should match composite secondary index on (name, age)
        int[][] lookupKeyIndexes = new int[][] {{1}, {3}}; // name, age
        int[][] secondaryIndexes = new int[][] {{1, 3}}; // composite index on (name, age)

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
    }

    @Test
    void testPrimaryKeyTakesPriorityOverSecondaryIndex() {
        // Lookup on PK (id) when a secondary index also matches id -> PK wins
        int[][] lookupKeyIndexes = new int[][] {{0}}; // id
        int[][] secondaryIndexes = new int[][] {{0}}; // secondary index on id too

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.LOOKUP);
    }

    @Test
    void testLookupKeyMatchesNoIndex() {
        // Lookup on email (index 2) -> no PK match, no secondary index match -> error
        int[][] lookupKeyIndexes = new int[][] {{2}}; // email
        int[][] secondaryIndexes = new int[][] {{1}}; // secondary index on name only

        assertThatThrownBy(
                        () ->
                                LookupNormalizer.validateAndCreateLookupNormalizer(
                                        lookupKeyIndexes,
                                        PRIMARY_KEYS,
                                        BUCKET_KEYS,
                                        PARTITION_KEYS,
                                        TABLE_SCHEMA,
                                        null,
                                        secondaryIndexes))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("secondary index");
    }

    @Test
    void testNullSecondaryIndexesFallsThrough() {
        // Lookup on name (index 1) with null secondary indexes -> no prefix support -> error
        int[][] lookupKeyIndexes = new int[][] {{1}}; // name

        assertThatThrownBy(
                        () ->
                                LookupNormalizer.validateAndCreateLookupNormalizer(
                                        lookupKeyIndexes,
                                        PRIMARY_KEYS,
                                        BUCKET_KEYS,
                                        PARTITION_KEYS,
                                        TABLE_SCHEMA,
                                        null,
                                        null))
                .isInstanceOf(TableException.class);
    }

    @Test
    void testFirstMatchingIndexWins() {
        // Multiple secondary indexes, lookup on email (index 2)
        // First index (name) doesn't match, second index (email) matches
        int[][] lookupKeyIndexes = new int[][] {{2}}; // email
        int[][] secondaryIndexes = new int[][] {{1}, {2}}; // name index, email index

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
    }

    @Test
    void testCreateSecondaryIndexLookupNormalizer() {
        // Test the factory method directly
        int[] lookupKeys = new int[] {1}; // name
        LookupNormalizer normalizer =
                LookupNormalizer.createSecondaryIndexLookupNormalizer(lookupKeys, TABLE_SCHEMA);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(1);
    }

    @Test
    void testSecondaryIndexMatchIsOrderInsensitive() {
        // Index defined as (name, age) = [1, 3], but lookup keys provided as (age, name) = [3, 1]
        int[][] lookupKeyIndexes = new int[][] {{3}, {1}}; // age, name (reversed order)
        int[][] secondaryIndexes = new int[][] {{1, 3}}; // index on (name, age)

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
    }

    @Test
    void testSixParamOverloadPassesNullSecondaryIndexes() {
        // The 6-param overload should behave the same as 7-param with null secondary indexes
        // Lookup on PK (id) -> should work with both overloads
        int[][] lookupKeyIndexes = new int[][] {{0}}; // id

        LookupNormalizer fromSixParam =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null);

        LookupNormalizer fromSevenParam =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        null);

        assertThat(fromSixParam.getLookupType()).isEqualTo(fromSevenParam.getLookupType());
        assertThat(fromSixParam.getLookupKeyIndexes())
                .containsExactly(fromSevenParam.getLookupKeyIndexes());
    }

    @Test
    void testSupersetMatchRoutesToSecondaryIndex() {
        // Lookup on (name, age) = [1, 3], secondary index on (name) = [1]
        // Superset: lookup keys contain all index columns -> SI lookup with remaining filter
        int[][] lookupKeyIndexes = new int[][] {{1}, {3}}; // name, age
        int[][] secondaryIndexes = new int[][] {{1}}; // index on name only

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.SECONDARY_INDEX_LOOKUP);
        // normalized key should only contain the index column (name)
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(1);
    }

    @Test
    void testSupersetMatchNormalizesKeyToIndexColumns() {
        // Lookup key row = [nameValue, ageValue] (in Flink lookup key order)
        // After normalization, only the index key (name) should remain
        int[][] lookupKeyIndexes = new int[][] {{1}, {3}}; // name, age
        int[][] secondaryIndexes = new int[][] {{1}}; // index on name

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        // Simulate a lookup key row: [name="alice", age=25]
        GenericRowData lookupKeyRow = new GenericRowData(2);
        lookupKeyRow.setField(0, StringData.fromString("alice"));
        lookupKeyRow.setField(1, 25);

        RowData normalized = normalizer.normalizeLookupKey(lookupKeyRow);
        assertThat(normalized.getArity()).isEqualTo(1);
        assertThat(normalized.getString(0).toString()).isEqualTo("alice");
    }

    @Test
    void testSupersetMatchProducesRemainingFilter() {
        // The extra key (age) should become a remaining filter condition
        int[][] lookupKeyIndexes = new int[][] {{1}, {3}}; // name, age
        int[][] secondaryIndexes = new int[][] {{1}}; // index on name

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        // Lookup key row: [name="alice", age=25]
        GenericRowData lookupKeyRow = new GenericRowData(2);
        lookupKeyRow.setField(0, StringData.fromString("alice"));
        lookupKeyRow.setField(1, 25);

        LookupNormalizer.RemainingFilter filter = normalizer.createRemainingFilter(lookupKeyRow);
        assertThat(filter).isNotNull();

        // Result row matching schema: id(0), name(1), email(2), age(3)
        GenericRowData matchingResult = new GenericRowData(4);
        matchingResult.setField(0, 1);
        matchingResult.setField(1, StringData.fromString("alice"));
        matchingResult.setField(2, StringData.fromString("a@x.com"));
        matchingResult.setField(3, 25); // age matches
        assertThat(filter.isMatch(matchingResult)).isTrue();

        // Non-matching result (age != 25)
        GenericRowData nonMatchingResult = new GenericRowData(4);
        nonMatchingResult.setField(0, 2);
        nonMatchingResult.setField(1, StringData.fromString("alice"));
        nonMatchingResult.setField(2, StringData.fromString("a2@x.com"));
        nonMatchingResult.setField(3, 30); // age doesn't match
        assertThat(filter.isMatch(nonMatchingResult)).isFalse();
    }

    @Test
    void testSupersetMatchRemainingFilterHandlesNull() {
        // When the remaining condition value is null, only null results should match
        int[][] lookupKeyIndexes = new int[][] {{1}, {3}}; // name, age
        int[][] secondaryIndexes = new int[][] {{1}}; // index on name

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        // Lookup key row: [name="alice", age=null]
        GenericRowData lookupKeyRow = new GenericRowData(2);
        lookupKeyRow.setField(0, StringData.fromString("alice"));
        lookupKeyRow.setField(1, null);

        LookupNormalizer.RemainingFilter filter = normalizer.createRemainingFilter(lookupKeyRow);
        assertThat(filter).isNotNull();

        // Result with null age -> should match (null == null)
        GenericRowData nullAgeResult = new GenericRowData(4);
        nullAgeResult.setField(0, 1);
        nullAgeResult.setField(1, StringData.fromString("alice"));
        nullAgeResult.setField(2, StringData.fromString("a@x.com"));
        nullAgeResult.setField(3, null);
        assertThat(filter.isMatch(nullAgeResult)).isTrue();

        // Result with non-null age -> should not match (null != 25)
        GenericRowData nonNullAgeResult = new GenericRowData(4);
        nonNullAgeResult.setField(0, 1);
        nonNullAgeResult.setField(1, StringData.fromString("alice"));
        nonNullAgeResult.setField(2, StringData.fromString("a@x.com"));
        nonNullAgeResult.setField(3, 25);
        assertThat(filter.isMatch(nonNullAgeResult)).isFalse();
    }

    @Test
    void testRemainingFilterComparesBytesByContent() {
        RowType schema =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("id", new IntType()),
                                new RowType.RowField("name", new VarCharType(200)),
                                new RowType.RowField("payload", new VarBinaryType(20))));
        int[][] lookupKeyIndexes = new int[][] {{1}, {2}};
        int[][] secondaryIndexes = new int[][] {{1}};

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        new int[] {0},
                        new int[] {0},
                        new int[] {},
                        schema,
                        null,
                        secondaryIndexes);

        GenericRowData lookupKeyRow = new GenericRowData(2);
        lookupKeyRow.setField(0, StringData.fromString("alice"));
        lookupKeyRow.setField(1, new byte[] {1, 2, 3});
        LookupNormalizer.RemainingFilter filter = normalizer.createRemainingFilter(lookupKeyRow);
        assertThat(filter).isNotNull();

        GenericRowData result = new GenericRowData(3);
        result.setField(0, 1);
        result.setField(1, StringData.fromString("alice"));
        result.setField(2, new byte[] {1, 2, 3});

        assertThat(filter.isMatch(result)).isTrue();
    }

    @Test
    void testExactMatchProducesNoRemainingFilter() {
        // When lookup keys exactly match the index (no superset), no remaining filter
        int[][] lookupKeyIndexes = new int[][] {{1}}; // name
        int[][] secondaryIndexes = new int[][] {{1}}; // index on name

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        PRIMARY_KEYS,
                        BUCKET_KEYS,
                        PARTITION_KEYS,
                        TABLE_SCHEMA,
                        null,
                        secondaryIndexes);

        GenericRowData lookupKeyRow = new GenericRowData(1);
        lookupKeyRow.setField(0, StringData.fromString("alice"));

        assertThat(normalizer.createRemainingFilter(lookupKeyRow)).isNull();
    }
}
