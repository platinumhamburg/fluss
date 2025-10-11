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

package org.apache.fluss.utils;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.util.Arrays;

/** Utility class to convert objects into strings in vice-versa. */
public class StringUtils {

    public static final String EMPTY_STRING = "";

    /**
     * Checks if the string is null, empty, or contains only whitespace characters. A whitespace
     * character is defined via {@link Character#isWhitespace(char)}.
     *
     * @param str The string to check
     * @return True, if the string is null or blank, false otherwise.
     */
    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts the given object into a string representation by calling {@link Object#toString()}
     * and formatting (possibly nested) arrays and {@code null}.
     *
     * <p>See {@link Arrays#deepToString(Object[])} for more information about the used format.
     */
    public static String arrayAwareToString(Object o) {
        final String arrayString = Arrays.deepToString(new Object[] {o});
        return arrayString.substring(1, arrayString.length() - 1);
    }

    public static String internalRowDebugString(RowType rowType, InternalRow row) {
        StringBuilder sb = new StringBuilder();
        sb.append("Row[");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String valueString;
            if (row.isNullAt(i)) {
                valueString = "null";
            } else {
                DataType type = rowType.getTypeAt(i);
                switch (type.getTypeRoot()) {
                    case CHAR:
                        CharType charType = (CharType) rowType.getTypeAt(i);
                        valueString = "'" + row.getChar(i, charType.getLength()) + "'";
                        break;
                    case STRING:
                        valueString = "'" + row.getString(i).toString() + "'";
                        break;
                    case BOOLEAN:
                        valueString = String.valueOf(row.getBoolean(i));
                        break;
                    case BINARY:
                        int binaryLength =
                                ((org.apache.fluss.types.BinaryType) rowType.getTypeAt(i))
                                        .getLength();
                        valueString = "@binary[" + row.getBinary(i, binaryLength).length + "]";
                        break;
                    case BYTES:
                        valueString = "@bytes[" + row.getBytes(i).length + "]";
                        break;
                    case DECIMAL:
                        DecimalType decimalType = (DecimalType) rowType.getTypeAt(i);
                        valueString =
                                row.getDecimal(
                                                i,
                                                decimalType.getPrecision(),
                                                decimalType.getScale())
                                        .toString();
                        break;
                    case TINYINT:
                        valueString = String.valueOf(row.getByte(i));
                        break;
                    case SMALLINT:
                        valueString = String.valueOf(row.getShort(i));
                        break;
                    case INTEGER:
                        valueString = String.valueOf(row.getInt(i));
                        break;
                    case BIGINT:
                        valueString = String.valueOf(row.getLong(i));
                        break;
                    case FLOAT:
                        valueString = String.valueOf(row.getFloat(i));
                        break;
                    case DOUBLE:
                        valueString = String.valueOf(row.getDouble(i));
                        break;
                    case DATE:
                        valueString = String.valueOf(row.getInt(i));
                        break;
                    case TIME_WITHOUT_TIME_ZONE:
                        valueString = String.valueOf(row.getInt(i));
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        TimestampType timestampType = (TimestampType) rowType.getTypeAt(i);
                        valueString =
                                String.valueOf(
                                        row.getTimestampNtz(i, timestampType.getPrecision()));
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        LocalZonedTimestampType localZonedTimestampType =
                                (LocalZonedTimestampType) rowType.getTypeAt(i);
                        valueString =
                                String.valueOf(
                                        row.getTimestampLtz(
                                                i, localZonedTimestampType.getPrecision()));
                        break;
                    case ARRAY:
                        valueString = "@array";
                        break;
                    case MAP:
                        valueString = "@map";
                        break;
                    case ROW:
                        valueString = "@row";
                        break;
                    default:
                        valueString = "@unknown_type";
                        break;
                }
            }
            sb.append(rowType.getFieldNames().get(i)).append(": ").append(valueString);
        }
        sb.append("]");
        return sb.toString();
    }
}
