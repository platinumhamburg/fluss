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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.InputView;
import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericArray;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalArray;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.serializer.DataInputDeserializer;
import com.alibaba.fluss.row.serializer.DataOutputSerializer;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;

import java.io.IOException;

/**
 * Serializer for {@link LogRecordBatchStatistics} to support statistics storage in V2 format.
 *
 * <p>The serialization format is as follows:
 *
 * <ul>
 *   <li>Version (1 byte): Format version for future compatibility
 *   <li>Field Count (2 bytes): Number of fields in the statistics
 *   <li>For each field:
 *       <ul>
 *         <li>Null Count (8 bytes): Number of null values for this field
 *         <li>Min Value Exists (1 byte): Whether min value exists (0 or 1)
 *         <li>Min Value (variable): Serialized min value if exists
 *         <li>Max Value Exists (1 byte): Whether max value exists (0 or 1)
 *         <li>Max Value (variable): Serialized max value if exists
 *       </ul>
 * </ul>
 */
public class LogRecordBatchStatisticsSerializer {

    private static final byte STATISTICS_VERSION = 1;

    /**
     * Serializes the given statistics to a byte array.
     *
     * @param statistics The statistics to serialize
     * @param rowType The row type for the statistics
     * @return The serialized statistics as a byte array
     * @throws IOException If serialization fails
     */
    public static byte[] serialize(LogRecordBatchStatistics statistics, RowType rowType)
            throws IOException {
        if (statistics == null) {
            return new byte[0];
        }

        DataOutputSerializer serializer =
                new DataOutputSerializer(estimateSerializedSize(statistics, rowType));

        // Write version
        serializer.writeByte(STATISTICS_VERSION);

        // Write field count
        int fieldCount = rowType.getFieldCount();
        serializer.writeShort(fieldCount);

        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        InternalArray nullCounts = statistics.getNullCounts();

        // Write statistics for each field
        for (int i = 0; i < fieldCount; i++) {
            DataType fieldType = rowType.getTypeAt(i);

            // Write null count
            long nullCount = nullCounts != null ? nullCounts.getLong(i) : 0L;
            serializer.writeLong(nullCount);

            // Write min value
            if (minValues != null && !minValues.isNullAt(i)) {
                serializer.writeBoolean(true);
                writeValue(serializer, minValues, i, fieldType);
            } else {
                serializer.writeBoolean(false);
            }

            // Write max value
            if (maxValues != null && !maxValues.isNullAt(i)) {
                serializer.writeBoolean(true);
                writeValue(serializer, maxValues, i, fieldType);
            } else {
                serializer.writeBoolean(false);
            }
        }
        return serializer.getCopyOfBuffer();
    }

    /**
     * Deserializes statistics from a byte array.
     *
     * @param data The serialized statistics data
     * @param rowType The row type for the statistics
     * @return The deserialized statistics, or null if data is empty
     * @throws IOException If deserialization fails
     */
    public static LogRecordBatchStatistics deserialize(byte[] data, RowType rowType)
            throws IOException {
        if (data == null || data.length == 0) {
            return null;
        }

        InputView inputView = new DataInputDeserializer(data);

        // Read version
        byte version = inputView.readByte();
        if (version != STATISTICS_VERSION) {
            throw new IOException("Unsupported statistics version: " + version);
        }

        // Read field count
        int fieldCount = inputView.readShort();
        if (fieldCount != rowType.getFieldCount()) {
            throw new IOException(
                    "Field count mismatch: expected "
                            + rowType.getFieldCount()
                            + ", got "
                            + fieldCount);
        }

        // Read statistics for each field
        Object[] minValues = new Object[fieldCount];
        Object[] maxValues = new Object[fieldCount];
        long[] nullCounts = new long[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataType fieldType = rowType.getTypeAt(i);

            // Read null count
            nullCounts[i] = inputView.readLong();

            // Read min value
            if (inputView.readBoolean()) {
                minValues[i] = readValue(inputView, fieldType);
            } else {
                minValues[i] = null;
            }

            // Read max value
            if (inputView.readBoolean()) {
                maxValues[i] = readValue(inputView, fieldType);
            } else {
                maxValues[i] = null;
            }
        }

        return new DefaultLogRecordBatchStatistics(
                GenericRow.of(minValues), GenericRow.of(maxValues), new GenericArray(nullCounts));
    }

    private static int estimateSerializedSize(
            LogRecordBatchStatistics statistics, RowType rowType) {
        // Conservative estimate: 1KB should be enough for most cases
        return 1024;
    }

    private static void writeValue(
            OutputView outputView, InternalRow row, int fieldIndex, DataType fieldType)
            throws IOException {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                outputView.writeBoolean(row.getBoolean(fieldIndex));
                break;
            case TINYINT:
                outputView.writeByte(row.getByte(fieldIndex));
                break;
            case SMALLINT:
                outputView.writeShort(row.getShort(fieldIndex));
                break;
            case INTEGER:
                outputView.writeInt(row.getInt(fieldIndex));
                break;
            case BIGINT:
                outputView.writeLong(row.getLong(fieldIndex));
                break;
            case FLOAT:
                outputView.writeFloat(row.getFloat(fieldIndex));
                break;
            case DOUBLE:
                outputView.writeDouble(row.getDouble(fieldIndex));
                break;
            case STRING:
                BinaryString binaryString = row.getString(fieldIndex);
                outputView.writeUTF(binaryString.toString());
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                Decimal decimal =
                        row.getDecimal(
                                fieldIndex, decimalType.getPrecision(), decimalType.getScale());
                if (Decimal.isCompact(decimalType.getPrecision())) {
                    outputView.writeLong(decimal.toUnscaledLong());
                } else {
                    byte[] bytes = decimal.toUnscaledBytes();
                    outputView.writeInt(bytes.length);
                    outputView.write(bytes);
                }
                break;
            case DATE:
                outputView.writeInt(row.getInt(fieldIndex));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                outputView.writeInt(row.getInt(fieldIndex));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                TimestampNtz timestampNtz =
                        row.getTimestampNtz(fieldIndex, timestampType.getPrecision());
                if (TimestampNtz.isCompact(timestampType.getPrecision())) {
                    outputView.writeLong(timestampNtz.getMillisecond());
                } else {
                    outputView.writeLong(timestampNtz.getMillisecond());
                    outputView.writeInt(timestampNtz.getNanoOfMillisecond());
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) fieldType;
                TimestampLtz timestampLtz =
                        row.getTimestampLtz(fieldIndex, localZonedTimestampType.getPrecision());
                if (TimestampLtz.isCompact(localZonedTimestampType.getPrecision())) {
                    outputView.writeLong(timestampLtz.getEpochMillisecond());
                } else {
                    outputView.writeLong(timestampLtz.getEpochMillisecond());
                    outputView.writeInt(timestampLtz.getNanoOfMillisecond());
                }
                break;
            default:
                throw new IOException("Unsupported field type for statistics: " + fieldType);
        }
    }

    private static Object readValue(InputView inputView, DataType fieldType) throws IOException {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return inputView.readBoolean();
            case TINYINT:
                return inputView.readByte();
            case SMALLINT:
                return inputView.readShort();
            case INTEGER:
                return inputView.readInt();
            case BIGINT:
                return inputView.readLong();
            case FLOAT:
                return inputView.readFloat();
            case DOUBLE:
                return inputView.readDouble();
            case STRING:
                return BinaryString.fromString(inputView.readUTF());
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                if (Decimal.isCompact(decimalType.getPrecision())) {
                    long unscaledLong = inputView.readLong();
                    return Decimal.fromUnscaledLong(
                            unscaledLong, decimalType.getPrecision(), decimalType.getScale());
                } else {
                    int length = inputView.readInt();
                    byte[] bytes = new byte[length];
                    inputView.readFully(bytes);
                    return Decimal.fromUnscaledBytes(
                            bytes, decimalType.getPrecision(), decimalType.getScale());
                }
            case DATE:
                return inputView.readInt();
            case TIME_WITHOUT_TIME_ZONE:
                return inputView.readInt();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                if (TimestampNtz.isCompact(timestampType.getPrecision())) {
                    long millisecond = inputView.readLong();
                    return TimestampNtz.fromMillis(millisecond);
                } else {
                    long millisecond = inputView.readLong();
                    int nanoOfMillisecond = inputView.readInt();
                    return TimestampNtz.fromMillis(millisecond, nanoOfMillisecond);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) fieldType;
                if (TimestampLtz.isCompact(localZonedTimestampType.getPrecision())) {
                    long epochMillisecond = inputView.readLong();
                    return TimestampLtz.fromEpochMillis(epochMillisecond);
                } else {
                    long epochMillisecond = inputView.readLong();
                    int nanoOfMillisecond = inputView.readInt();
                    return TimestampLtz.fromEpochMillis(epochMillisecond, nanoOfMillisecond);
                }
            default:
                throw new IOException("Unsupported field type for statistics: " + fieldType);
        }
    }
}
