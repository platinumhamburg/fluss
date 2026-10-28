/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.columnar.ColumnarMap;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.AggregateRowMerger;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldCollectAgg;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for collection aggregation semantics and encoding in both KV formats. */
class FieldCollectionAggregatorTest {

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectOnFirstWrite(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue input = value(schema, format, GenericArray.of(3, 1, 3, null, null));
        BinaryValue expected = value(schema, format, GenericArray.of(3, 1, null));

        BinaryValue result = merger.merge(null, input);
        assertThat(result.row).isEqualTo(expected.row);
        assertThat(merger.merge(result, value(schema, format, null)).row).isEqualTo(expected.row);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectAfterNull(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue result =
                merger.merge(
                        value(schema, format, null),
                        value(schema, format, GenericArray.of(3, 1, 3, null, null)));
        assertThat(result.row).isEqualTo(value(schema, format, GenericArray.of(3, 1, null)).row);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectKeepsFirstOccurrenceOrder(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericArray.of(3, 1)),
                                value(schema, format, GenericArray.of(1, 2, 3, null)));
        assertThat(result.row).isEqualTo(value(schema, format, GenericArray.of(3, 1, 2, null)).row);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectWithRepeatedInput(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue old = value(schema, format, GenericArray.of(3, 1, null));
        BinaryValue result =
                merger.merge(old, value(schema, format, GenericArray.of(null, 1, 3, 1)));
        assertThat(result.row).isEqualTo(old.row);

        result =
                merger.merge(
                        value(schema, format, GenericArray.of(3, 3, null, null)),
                        value(schema, format, GenericArray.of(3, null)));
        assertThat(result.row).isEqualTo(value(schema, format, GenericArray.of(3, null)).row);
    }

    @ParameterizedTest
    @MethodSource("collectModes")
    void testCollectWithEmptyArrays(KvFormat format, boolean distinct) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(distinct));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue empty = value(schema, format, GenericArray.of());
        BinaryValue input = value(schema, format, GenericArray.of(3, 3, null));
        BinaryValue expected =
                value(
                        schema,
                        format,
                        distinct ? GenericArray.of(3, null) : GenericArray.of(3, 3, null));

        assertThat(merger.merge(null, empty).row).isEqualTo(empty.row);
        assertThat(merger.merge(value(schema, format, null), empty).row).isEqualTo(empty.row);
        assertThat(merger.merge(empty, empty).row).isEqualTo(empty.row);
        assertThat(merger.merge(empty, input).row).isEqualTo(expected.row);
        assertThat(merger.merge(input, empty).row).isEqualTo(expected.row);
    }

    static Stream<Arguments> collectModes() {
        return Stream.of(KvFormat.values())
                .flatMap(
                        format ->
                                Stream.of(false, true)
                                        .map(distinct -> Arguments.of(format, distinct)));
    }

    @ParameterizedTest
    @MethodSource("complexElements")
    void testCollectComplexElements(
            KvFormat format, boolean distinct, DataType elementType, Object first, Object second) {
        Schema schema = schema(DataTypes.ARRAY(elementType), AggFunctions.COLLECT(distinct));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericArray.of(first, null)),
                                value(schema, format, GenericArray.of(first, second, null)));
        GenericArray expected =
                distinct
                        ? GenericArray.of(first, null, second)
                        : GenericArray.of(first, null, first, second, null);
        assertThat(result.row).isEqualTo(value(schema, format, expected).row);
        if (distinct) {
            BinaryValue repeated =
                    merger(schema, format)
                            .merge(
                                    value(schema, format, expected),
                                    value(
                                            schema,
                                            format,
                                            GenericArray.of(second, first, null, second)));
            assertThat(repeated.row).isEqualTo(value(schema, format, expected).row);
        }
    }

    static Stream<Arguments> complexElements() {
        Stream.Builder<Arguments> cases = Stream.builder();
        for (KvFormat format : KvFormat.values()) {
            for (boolean distinct : new boolean[] {false, true}) {
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.STRING(),
                                BinaryString.fromString("b"),
                                BinaryString.fromString("a")));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.BYTES(),
                                new byte[] {2, 3},
                                new byte[] {1, 4}));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.ARRAY(DataTypes.INT()),
                                GenericArray.of(2, null),
                                GenericArray.of(1, 3)));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())),
                                GenericRow.of(2),
                                GenericRow.of(1)));
            }
        }
        return cases.build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReversedCollect(boolean distinct) {
        FieldCollectAgg aggregator =
                new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), distinct);
        InternalArray result =
                (InternalArray)
                        aggregator.aggReversed(GenericArray.of(3, 4), GenericArray.of(1, 2));
        assertThat(result.size()).isEqualTo(4);
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.getInt(i)).isEqualTo(i + 1);
        }
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapBinaryKeys(KvFormat format, DataType keyType) {
        Schema schema =
                schema(DataTypes.MAP(keyType, DataTypes.STRING()), AggFunctions.MERGE_MAP());
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue result =
                merger.merge(
                        value(
                                schema,
                                format,
                                GenericMap.of(
                                        new byte[] {1, 2}, BinaryString.fromString("old"),
                                        new byte[] {3, 4}, BinaryString.fromString("kept"))),
                        value(
                                schema,
                                format,
                                GenericMap.of(
                                        new byte[] {1, 2}, BinaryString.fromString("new"),
                                        new byte[] {5, 6}, BinaryString.fromString("added"))));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(3);
        assertThat(binaryMapEntries(map))
                .containsOnly(
                        entry("[1, 2]", "new"), entry("[3, 4]", "kept"), entry("[5, 6]", "added"));

        result =
                merger.merge(result, value(schema, format, GenericMap.of(new byte[] {1, 2}, null)));
        assertThat(result.row.getMap(1).size()).isEqualTo(3);
        assertThat(binaryMapEntries(result.row.getMap(1)))
                .containsOnly(
                        entry("[1, 2]", null), entry("[3, 4]", "kept"), entry("[5, 6]", "added"));
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapKeepsLastStoredValue(KvFormat format, DataType keyType) {
        Schema schema = schema(DataTypes.MAP(keyType, DataTypes.BYTES()), AggFunctions.MERGE_MAP());
        InternalMap old =
                new ColumnarMap(
                        GenericArray.of(new byte[] {1, 2}, new byte[] {1, 2}),
                        GenericArray.of(new byte[] {1}, new byte[] {2}));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, old),
                                value(
                                        schema,
                                        format,
                                        GenericMap.of(new byte[] {3, 4}, new byte[] {3})));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(2);
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            entries.put(
                    Arrays.toString(map.keyArray().getBytes(i)),
                    Arrays.toString(map.valueArray().getBytes(i)));
        }
        assertThat(entries).containsOnly(entry("[1, 2]", "[2]"), entry("[3, 4]", "[3]"));
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapNullValueOverridesStoredBytes(KvFormat format, DataType valueType) {
        Schema schema = schema(DataTypes.MAP(DataTypes.INT(), valueType), AggFunctions.MERGE_MAP());
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericMap.of(1, new byte[] {1, 2})),
                                value(schema, format, GenericMap.of(1, null)));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.keyArray().getInt(0)).isEqualTo(1);
        assertThat(map.valueArray().isNullAt(0)).isTrue();
    }

    static Stream<Arguments> binaryTypes() {
        return Stream.of(KvFormat.values())
                .flatMap(
                        format ->
                                Stream.of(DataTypes.BYTES(), DataTypes.BINARY(2))
                                        .map(type -> Arguments.of(format, type)));
    }

    private static Map<String, String> binaryMapEntries(InternalMap map) {
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            entries.put(
                    Arrays.toString(map.keyArray().getBytes(i)),
                    map.valueArray().isNullAt(i) ? null : map.valueArray().getString(i).toString());
        }
        return entries;
    }

    private static Schema schema(DataType type, AggFunction function) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", type, function)
                .primaryKey("id")
                .build();
    }

    private static AggregateRowMerger merger(Schema schema, KvFormat format) {
        AggregateRowMerger merger =
                new AggregateRowMerger(
                        new TableConfig(new Configuration()),
                        format,
                        new TestingSchemaGetter(new SchemaInfo(schema, (short) 1)));
        merger.configureTargetColumns(null, (short) 1, schema);
        return merger;
    }

    private static BinaryValue value(Schema schema, KvFormat format, Object field) {
        RowEncoder encoder = RowEncoder.create(format, schema.getRowType());
        encoder.startNewRow();
        encoder.encodeField(0, 1);
        encoder.encodeField(1, field);
        return new BinaryValue((short) 1, encoder.finishRow());
    }
}
