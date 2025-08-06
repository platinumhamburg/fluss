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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.row.serializer.InternalArraySerializer.convertToJavaArray;
import static com.alibaba.fluss.row.serializer.InternalMapSerializer.convertToJavaMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.offset;

/** Test for {@link FlinkAsFlussRow}. */
class FlinkAsFlussRowTest {

    private FlinkAsFlussRow row;

    @BeforeEach
    public void setUp() {
        // Create a sample RowData instance
        RowData flinkRow =
                GenericRowData.of(
                        true,
                        (byte) 1,
                        (short) 10,
                        100,
                        1000L,
                        10.5f,
                        10.5,
                        StringData.fromString("test"),
                        StringData.fromString("test"),
                        DecimalData.fromUnscaledLong(12345L, 10, 2),
                        TimestampData.fromInstant(Instant.ofEpochMilli(1672531200000L)),
                        TimestampData.fromEpochMillis(1672531200000L, 3),
                        new byte[] {1, 2, 3},
                        null,
                        new GenericArrayData(new Integer[] {1, 2, 3}),
                        new GenericMapData(
                                new HashMap<Integer, StringData>() {
                                    {
                                        put(0, null);
                                        put(5, StringData.fromString("5"));
                                        put(6, StringData.fromString("6"));
                                        put(666, StringData.fromString("666"));
                                    }
                                }),
                        GenericRowData.of(
                                StringData.fromString("a"),
                                GenericRowData.of(1, StringData.fromString("nested")),
                                42));
        row = new FlinkAsFlussRow().replace(flinkRow);
    }

    @Test
    public void testGetFieldCount() {
        assertThat(17).isEqualTo(row.getFieldCount());
    }

    @Test
    public void testIsNullAt() {
        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.isNullAt(13)).isTrue();
    }

    @Test
    public void testGetBoolean() {
        assertThat(row.getBoolean(0)).isTrue();
    }

    @Test
    public void testGetByte() {
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
    }

    @Test
    public void testGetShort() {
        assertThat(row.getShort(2)).isEqualTo((short) 10);
    }

    @Test
    public void testGetInt() {
        assertThat(row.getInt(3)).isEqualTo(100);
    }

    @Test
    public void testGetLong() {
        assertThat(row.getLong(4)).isEqualTo(1000L);
    }

    @Test
    public void testGetFloat() {
        assertThat(row.getFloat(5)).isCloseTo(10.5f, offset(0.01f));
    }

    @Test
    public void testGetDouble() {
        assertThat(row.getDouble(6)).isCloseTo(10.5, offset(0.01));
    }

    @Test
    public void testGetChar() {
        BinaryString binaryString = row.getChar(7, 4);
        assertThat(binaryString.toString()).isEqualTo("test");
    }

    @Test
    public void testGetString() {
        BinaryString binaryString = row.getString(8);
        assertThat(binaryString.toString()).isEqualTo("test");
    }

    @Test
    public void testGetDecimalCompact() {
        Decimal decimal = row.getDecimal(9, 10, 2);
        assertThat(decimal.toUnscaledLong()).isEqualTo(12345L);
        assertThat(decimal.precision()).isEqualTo(10);
        assertThat(decimal.scale()).isEqualTo(2);
    }

    @Test
    public void testGetTimestampNtz() {
        TimestampNtz timestampNtz = row.getTimestampNtz(10, 3);
        assertThat(timestampNtz.getMillisecond()).isEqualTo(1672531200000L);
        assertThat(timestampNtz.getNanoOfMillisecond()).isEqualTo(0);
    }

    @Test
    public void testGetTimestampLtz() {
        TimestampLtz timestampLtz = row.getTimestampLtz(11, 9);
        assertThat(timestampLtz.getEpochMillisecond()).isEqualTo(1672531200000L);
        assertThat(timestampLtz.getNanoOfMillisecond()).isEqualTo(3);
    }

    @Test
    public void testGetBinary() {
        byte[] binary = row.getBinary(12, 3);
        assertThat(binary).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testGetBytes() {
        byte[] bytes = row.getBytes(12);
        assertThat(bytes).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testGetArray() {
        InternalArray array = row.getArray(14);
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(1)).isEqualTo(2);
        assertThat(array.getInt(2)).isEqualTo(3);

        Object[] javaArray = convertToJavaArray(array, DataTypes.INT());
        assertThat(javaArray).isEqualTo(new Object[] {1, 2, 3});
    }

    @Test
    public void testGetMap() {
        InternalMap map = row.getMap(15);
        assertThat(map.size()).isEqualTo(4);
        Map<Object, Object> javaMap = convertToJavaMap(map, DataTypes.INT(), DataTypes.STRING());
        assertThat(javaMap.get(0)).isEqualTo(null);
        assertThat(javaMap.get(5).toString()).isEqualTo("5");
        assertThat(javaMap.get(6).toString()).isEqualTo("6");
        assertThat(javaMap.get(666).toString()).isEqualTo("666");
    }

    @Test
    public void testGetRow() {
        InternalRow nestedRow = row.getRow(16, 2);
        assertThat(nestedRow.getString(0)).isEqualTo(BinaryString.fromString("a"));
        assertThat(nestedRow.getRow(1, 2).getInt(0)).isEqualTo(1);
        assertThat(nestedRow.getRow(1, 2).getString(1))
                .isEqualTo(BinaryString.fromString("nested"));
        assertThat(nestedRow.getInt(2)).isEqualTo(42);
    }
}
