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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.DateTimeUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllTypes;
import static com.alibaba.fluss.row.indexed.IndexedRowTest.genRecordForAllTypes;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter}. */
class FlussRowToFlinkRowConverterTest {

    @Test
    void testConverter() throws Exception {
        RowType rowType = createAllRowType();
        FlussRowToFlinkRowConverter flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(rowType);

        IndexedRow row = new IndexedRow(rowType.getChildren().toArray(new DataType[0]));
        try (IndexedRowWriter writer = genRecordForAllTypes(createAllTypes())) {
            row.pointTo(writer.segment(), 0, writer.position());

            ScanRecord scanRecord = new ScanRecord(0, 1L, ChangeType.UPDATE_BEFORE, row);

            RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(scanRecord);
            assertThat(flinkRow.getArity()).isEqualTo(rowType.getFieldCount());
            assertThat(flinkRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

            // now, check the field values
            assertThat(flinkRow.getBoolean(0)).isTrue();
            assertThat(flinkRow.getByte(1)).isEqualTo((byte) 2);
            assertThat(flinkRow.getShort(2)).isEqualTo((short) 10);
            assertThat(flinkRow.getInt(3)).isEqualTo(100);
            assertThat(flinkRow.getLong(4))
                    .isEqualTo(new BigInteger("12345678901234567890").longValue());
            assertThat(flinkRow.getFloat(5)).isEqualTo(13.2f);
            assertThat(flinkRow.getDouble(6)).isEqualTo(15.21);

            assertThat(DateTimeUtils.toLocalDate(flinkRow.getInt(7)))
                    .isEqualTo(LocalDate.of(2023, 10, 25));
            assertThat(DateTimeUtils.toLocalTime(flinkRow.getInt(8)))
                    .isEqualTo(LocalTime.of(9, 30, 0, 0));

            assertThat(flinkRow.getBinary(9)).isEqualTo("1234567890".getBytes());
            assertThat(flinkRow.getBinary(10)).isEqualTo("20".getBytes());

            assertThat(flinkRow.getString(11).toString()).isEqualTo("1");
            assertThat(flinkRow.getString(12).toString()).isEqualTo("hello");

            assertThat(flinkRow.getDecimal(13, 5, 2).toBigDecimal())
                    .isEqualTo(BigDecimal.valueOf(9, 2));
            assertThat(flinkRow.getDecimal(14, 20, 0).toBigDecimal()).isEqualTo(new BigDecimal(10));

            assertThat(flinkRow.getTimestamp(15, 1).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");
            assertThat(flinkRow.getTimestamp(16, 5).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");

            assertThat(flinkRow.getTimestamp(17, 1).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");
            assertThat(flinkRow.getTimestamp(18, 5).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");

            assertThat(flinkRow.getArray(19).size()).isEqualTo(3);
            assertThat(
                            convertToJavaArray(
                                    flinkRow.getArray(19),
                                    new org.apache.flink.table.types.logical.IntType()))
                    .isEqualTo(new Object[] {1, 2, 3});

            Map<Object, Object> javaMap =
                    convertToJavaMap(
                            flinkRow.getMap(20),
                            new org.apache.flink.table.types.logical.IntType(),
                            new org.apache.flink.table.types.logical.VarCharType());
            assertThat(javaMap.get(0)).isEqualTo(null);
            assertThat(javaMap.get(1).toString()).isEqualTo("1");
            assertThat(javaMap.get(2).toString()).isEqualTo("2");

            assertThat(flinkRow.getRow(21, 3).getInt(0)).isEqualTo(123);
            assertThat(flinkRow.getRow(21, 3).getRow(1, 1).getInt(0)).isEqualTo(20);
            assertThat(flinkRow.getRow(21, 3).getString(2).toString()).isEqualTo("Test");
        }
    }

    private static Object[] convertToJavaArray(
            ArrayData array, org.apache.flink.table.types.logical.LogicalType dataType) {
        Object[] javaArray = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
            ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(dataType);
            Object value = keyGetter.getElementOrNull(array, i);
            javaArray[i] = value;
        }
        return javaArray;
    }

    private static Map<Object, Object> convertToJavaMap(
            MapData map,
            org.apache.flink.table.types.logical.LogicalType keyType,
            org.apache.flink.table.types.logical.LogicalType valueType) {
        Map<Object, Object> javaMap = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
            ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
            Object key = keyGetter.getElementOrNull(map.keyArray(), i);
            Object value = valueGetter.getElementOrNull(map.valueArray(), i);
            javaMap.put(key, value);
        }
        return javaMap;
    }
}
