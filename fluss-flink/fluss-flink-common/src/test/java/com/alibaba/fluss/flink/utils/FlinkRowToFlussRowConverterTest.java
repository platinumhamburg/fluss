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

import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.TypeUtils;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static com.alibaba.fluss.row.indexed.IndexedRowTest.assertAllTypeEquals;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.flink.utils.FlinkRowToFlussRowConverter}. */
public class FlinkRowToFlussRowConverterTest {

    @Test
    void testConverter() throws Exception {
        RowType flussRowType = createAllRowType();

        // test indexed row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(toFlinkRowType(flussRowType))) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(22);
            assertAllTypeEquals(internalRow);
        }

        // test compacted row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(
                        toFlinkRowType(flussRowType), KvFormat.COMPACTED)) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(22);
            assertAllTypeEquals(internalRow);
        }
    }

    private static RowData genRowDataForAllType() {
        GenericRowData genericRowData = new GenericRowData(22);
        genericRowData.setField(0, true);
        genericRowData.setField(1, (byte) 2);
        genericRowData.setField(2, Short.parseShort("10"));
        genericRowData.setField(3, 100);
        genericRowData.setField(4, new BigInteger("12345678901234567890").longValue());
        genericRowData.setField(5, Float.parseFloat("13.2"));
        genericRowData.setField(6, Double.parseDouble("15.21"));
        genericRowData.setField(7, TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        genericRowData.setField(8, TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        genericRowData.setField(9, "1234567890".getBytes());
        genericRowData.setField(10, "20".getBytes());
        genericRowData.setField(11, StringData.fromBytes("1".getBytes()));
        genericRowData.setField(12, StringData.fromString("hello"));
        genericRowData.setField(13, DecimalData.fromUnscaledLong(9, 5, 2));
        genericRowData.setField(14, DecimalData.fromBigDecimal(new BigDecimal(10), 20, 0));
        genericRowData.setField(15, TimestampData.fromEpochMillis(1698235273182L, 0));
        genericRowData.setField(16, TimestampData.fromEpochMillis(1698235273182L, 0));
        genericRowData.setField(
                17,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2023-10-25T12:01:13.182")));
        genericRowData.setField(
                18,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2023-10-25T12:01:13.182")));

        // array
        GenericArrayData arrayData = new GenericArrayData(new Integer[] {1, 2, 3});
        genericRowData.setField(19, arrayData);

        // map
        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(0, null);
        javaMap.put(1, StringData.fromString("1"));
        javaMap.put(2, StringData.fromString("2"));
        GenericMapData mapData = new GenericMapData(javaMap);
        genericRowData.setField(20, mapData);

        // row
        genericRowData.setField(
                21, GenericRowData.of(123, GenericRowData.of(20), StringData.fromString("Test")));
        return genericRowData;
    }
}
