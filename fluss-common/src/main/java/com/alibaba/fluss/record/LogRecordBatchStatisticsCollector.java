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

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericArray;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;

import java.util.Arrays;

/**
 * Collector for {@link LogRecordBatchStatistics} that accumulates statistics during record batch
 * construction.
 */
public class LogRecordBatchStatisticsCollector {

    private final RowType rowType;
    private final int fieldCount;
    private final DataType[] fieldTypes;

    // Statistics arrays
    private final Object[] minValues;
    private final Object[] maxValues;
    private final long[] nullCounts;

    // Flags to track if values have been set
    private final boolean[] minSet;
    private final boolean[] maxSet;

    private long totalRowCount = 0;

    public LogRecordBatchStatisticsCollector(RowType rowType) {
        this.rowType = rowType;
        this.fieldCount = rowType.getFieldCount();
        this.fieldTypes = new DataType[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            fieldTypes[i] = rowType.getTypeAt(i);
        }

        this.minValues = new Object[fieldCount];
        this.maxValues = new Object[fieldCount];
        this.nullCounts = new long[fieldCount];
        this.minSet = new boolean[fieldCount];
        this.maxSet = new boolean[fieldCount];

        // Initialize arrays
        Arrays.fill(minSet, false);
        Arrays.fill(maxSet, false);
        Arrays.fill(nullCounts, 0L);
    }

    /**
     * Process a row and update statistics.
     *
     * @param row The row to process
     */
    public void processRow(InternalRow row) {
        totalRowCount++;

        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                nullCounts[i]++;
            } else {
                updateMinMax(i, row);
            }
        }
    }

    /**
     * Get the collected statistics.
     *
     * @return The collected statistics, or null if no rows were processed
     */
    public LogRecordBatchStatistics getStatistics() {
        if (totalRowCount == 0) {
            return null;
        }

        // Create min/max rows with null for unset values
        Object[] finalMinValues = new Object[fieldCount];
        Object[] finalMaxValues = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            finalMinValues[i] = minSet[i] ? minValues[i] : null;
            finalMaxValues[i] = maxSet[i] ? maxValues[i] : null;
        }

        return new DefaultLogRecordBatchStatistics(
                GenericRow.of(finalMinValues),
                GenericRow.of(finalMaxValues),
                new GenericArray(nullCounts));
    }

    /**
     * Get the row type used by this collector.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    /** Reset the collector to collect new statistics. */
    public void reset() {
        totalRowCount = 0;
        Arrays.fill(minSet, false);
        Arrays.fill(maxSet, false);
        Arrays.fill(nullCounts, 0L);
        Arrays.fill(minValues, null);
        Arrays.fill(maxValues, null);
    }

    private void updateMinMax(int fieldIndex, InternalRow row) {
        DataType fieldType = fieldTypes[fieldIndex];

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                boolean boolValue = row.getBoolean(fieldIndex);
                updateBooleanMinMax(fieldIndex, boolValue);
                break;
            case TINYINT:
                byte byteValue = row.getByte(fieldIndex);
                updateByteMinMax(fieldIndex, byteValue);
                break;
            case SMALLINT:
                short shortValue = row.getShort(fieldIndex);
                updateShortMinMax(fieldIndex, shortValue);
                break;
            case INTEGER:
                int intValue = row.getInt(fieldIndex);
                updateIntMinMax(fieldIndex, intValue);
                break;
            case BIGINT:
                long longValue = row.getLong(fieldIndex);
                updateLongMinMax(fieldIndex, longValue);
                break;
            case FLOAT:
                float floatValue = row.getFloat(fieldIndex);
                updateFloatMinMax(fieldIndex, floatValue);
                break;
            case DOUBLE:
                double doubleValue = row.getDouble(fieldIndex);
                updateDoubleMinMax(fieldIndex, doubleValue);
                break;
            case STRING:
                BinaryString stringValue = row.getString(fieldIndex);
                updateStringMinMax(fieldIndex, stringValue);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                Decimal decimalValue =
                        row.getDecimal(
                                fieldIndex, decimalType.getPrecision(), decimalType.getScale());
                updateDecimalMinMax(fieldIndex, decimalValue);
                break;
            case DATE:
                int dateValue = row.getInt(fieldIndex);
                updateDateMinMax(fieldIndex, dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                int timeValue = row.getInt(fieldIndex);
                updateTimeMinMax(fieldIndex, timeValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                TimestampNtz timestampNtzValue =
                        row.getTimestampNtz(fieldIndex, timestampType.getPrecision());
                updateTimestampNtzMinMax(fieldIndex, timestampNtzValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) fieldType;
                TimestampLtz timestampLtzValue =
                        row.getTimestampLtz(fieldIndex, localZonedTimestampType.getPrecision());
                updateTimestampLtzMinMax(fieldIndex, timestampLtzValue);
                break;
            default:
                // For unsupported types, don't collect min/max
                break;
        }
    }

    private void updateBooleanMinMax(int fieldIndex, boolean value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            boolean currentMin = (Boolean) minValues[fieldIndex];
            if (!value && currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            boolean currentMax = (Boolean) maxValues[fieldIndex];
            if (value && !currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateByteMinMax(int fieldIndex, byte value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            byte currentMin = (Byte) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            byte currentMax = (Byte) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateShortMinMax(int fieldIndex, short value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            short currentMin = (Short) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            short currentMax = (Short) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateIntMinMax(int fieldIndex, int value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            int currentMin = (Integer) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            int currentMax = (Integer) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateLongMinMax(int fieldIndex, long value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            long currentMin = (Long) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            long currentMax = (Long) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateFloatMinMax(int fieldIndex, float value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            float currentMin = (Float) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            float currentMax = (Float) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateDoubleMinMax(int fieldIndex, double value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            double currentMin = (Double) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            double currentMax = (Double) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateStringMinMax(int fieldIndex, BinaryString value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value.copy();
            minSet[fieldIndex] = true;
        } else {
            BinaryString currentMin = (BinaryString) minValues[fieldIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[fieldIndex] = value.copy();
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value.copy();
            maxSet[fieldIndex] = true;
        } else {
            BinaryString currentMax = (BinaryString) maxValues[fieldIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[fieldIndex] = value.copy();
            }
        }
    }

    private void updateDecimalMinMax(int fieldIndex, Decimal value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            Decimal currentMin = (Decimal) minValues[fieldIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            Decimal currentMax = (Decimal) maxValues[fieldIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateDateMinMax(int fieldIndex, int value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            int currentMin = (Integer) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            int currentMax = (Integer) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateTimeMinMax(int fieldIndex, int value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            int currentMin = (Integer) minValues[fieldIndex];
            if (value < currentMin) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            int currentMax = (Integer) maxValues[fieldIndex];
            if (value > currentMax) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateTimestampNtzMinMax(int fieldIndex, TimestampNtz value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            TimestampNtz currentMin = (TimestampNtz) minValues[fieldIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            TimestampNtz currentMax = (TimestampNtz) maxValues[fieldIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[fieldIndex] = value;
            }
        }
    }

    private void updateTimestampLtzMinMax(int fieldIndex, TimestampLtz value) {
        if (!minSet[fieldIndex]) {
            minValues[fieldIndex] = value;
            minSet[fieldIndex] = true;
        } else {
            TimestampLtz currentMin = (TimestampLtz) minValues[fieldIndex];
            if (value.compareTo(currentMin) < 0) {
                minValues[fieldIndex] = value;
            }
        }

        if (!maxSet[fieldIndex]) {
            maxValues[fieldIndex] = value;
            maxSet[fieldIndex] = true;
        } else {
            TimestampLtz currentMax = (TimestampLtz) maxValues[fieldIndex];
            if (value.compareTo(currentMax) > 0) {
                maxValues[fieldIndex] = value;
            }
        }
    }
}
