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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.metadata.TablePath.validatePrefix;

/** Utils for partition. */
public class PartitionUtils {

    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.CHAR,
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.BINARY,
                    DataTypeRoot.BYTES,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static void validatePartitionSpec(
            TablePath tablePath,
            List<String> partitionKeys,
            PartitionSpec partitionSpec,
            boolean isCreate) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            throw new InvalidPartitionException(
                    String.format(
                            "PartitionSpec size is not equal to partition keys size for partitioned table %s.",
                            tablePath));
        }

        List<String> reOrderedPartitionValues = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new InvalidPartitionException(
                        String.format(
                                "PartitionSpec %s does not contain partition key '%s' for partitioned table %s.",
                                partitionSpec, partitionKey, tablePath));
            } else {
                reOrderedPartitionValues.add(partitionSpecMap.get(partitionKey));
            }
        }

        validatePartitionValues(reOrderedPartitionValues, isCreate);
    }

    @VisibleForTesting
    static void validatePartitionValues(List<String> partitionValues, boolean isCreate) {
        for (String value : partitionValues) {
            String invalidNameError = detectInvalidName(value);
            if (invalidNameError != null || (isCreate && validatePrefix(value) != null)) {
                throw new InvalidPartitionException(
                        "The partition value "
                                + value
                                + " is invalid: "
                                + (invalidNameError != null
                                        ? invalidNameError
                                        : validatePrefix(value)));
            }
        }
    }

    /**
     * Validates that the partition value for the auto-partition key conforms to the expected time
     * format based on the configured time unit.
     *
     * <p>This validation should only be called for tables with indexes. Tables with indexes must
     * have auto-partition enabled, and we need to ensure partition values match the expected time
     * format for proper TTL calculation on index tables.
     *
     * <p>When auto-partition is enabled, the partition value for the time-based partition key must
     * match the expected format:
     *
     * <ul>
     *   <li>HOUR: "yyyyMMddHH" (e.g., "2025012108")
     *   <li>DAY: "yyyyMMdd" (e.g., "20250121")
     *   <li>MONTH: "yyyyMM" (e.g., "202501")
     *   <li>QUARTER: "yyyyQ#" (e.g., "2025Q1")
     *   <li>YEAR: "yyyy" (e.g., "2025")
     * </ul>
     *
     * @param partitionKeys the list of partition keys
     * @param partitionSpec the partition specification to validate
     * @param autoPartitionStrategy the auto-partition strategy containing time unit and key
     *     configuration
     * @throws InvalidPartitionException if the partition value does not match the expected time
     *     format
     */
    public static void validateAutoPartitionTimeFormat(
            List<String> partitionKeys,
            PartitionSpec partitionSpec,
            AutoPartitionStrategy autoPartitionStrategy) {
        if (!autoPartitionStrategy.isAutoPartitionEnabled()) {
            return;
        }

        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        if (timeUnit == null) {
            return;
        }

        // Determine which partition key is the auto-partition key
        String autoPartitionKey = autoPartitionStrategy.key();
        if (autoPartitionKey == null && partitionKeys.size() == 1) {
            // For single-column partitioned tables, use that column as the time partition key
            autoPartitionKey = partitionKeys.get(0);
        }

        if (autoPartitionKey == null) {
            return;
        }

        // Get the partition value for the auto-partition key
        String partitionValue = partitionSpec.getSpecMap().get(autoPartitionKey);
        if (partitionValue == null) {
            return;
        }

        // Validate the partition value format
        String errorMessage = validateTimePartitionFormat(partitionValue, timeUnit);
        if (errorMessage != null) {
            throw new InvalidPartitionException(
                    String.format(
                            "Invalid partition value '%s' for auto-partition key '%s'. %s",
                            partitionValue, autoPartitionKey, errorMessage));
        }
    }

    /**
     * Validates that a partition value matches the expected time format for the given time unit.
     *
     * @param partitionValue the partition value to validate
     * @param timeUnit the time unit defining the expected format
     * @return null if valid, or an error message describing the validation failure
     */
    @VisibleForTesting
    static String validateTimePartitionFormat(
            String partitionValue, AutoPartitionTimeUnit timeUnit) {
        if (partitionValue == null || partitionValue.isEmpty()) {
            return "Partition value cannot be null or empty.";
        }

        try {
            switch (timeUnit) {
                case HOUR:
                    return validateHourFormat(partitionValue);
                case DAY:
                    return validateDayFormat(partitionValue);
                case MONTH:
                    return validateMonthFormat(partitionValue);
                case QUARTER:
                    return validateQuarterFormat(partitionValue);
                case YEAR:
                    return validateYearFormat(partitionValue);
                default:
                    return "Unknown time unit: " + timeUnit;
            }
        } catch (Exception e) {
            return "Failed to parse partition value: " + e.getMessage();
        }
    }

    private static String validateHourFormat(String value) {
        // Expected format: yyyyMMddHH (e.g., "2025012108")
        if (value.length() != 10) {
            return String.format(
                    "Expected format '%s' (e.g., '2025012108'), but got '%s' with length %d.",
                    HOUR_FORMAT, value, value.length());
        }
        if (!value.matches("\\d{10}")) {
            return String.format(
                    "Expected format '%s' with all digits, but got '%s'.", HOUR_FORMAT, value);
        }
        // Validate date/time components
        int year = Integer.parseInt(value.substring(0, 4));
        int month = Integer.parseInt(value.substring(4, 6));
        int day = Integer.parseInt(value.substring(6, 8));
        int hour = Integer.parseInt(value.substring(8, 10));
        return validateDateTimeComponents(year, month, day, hour, HOUR_FORMAT, value);
    }

    private static String validateDayFormat(String value) {
        // Expected format: yyyyMMdd (e.g., "20250121")
        if (value.length() != 8) {
            return String.format(
                    "Expected format '%s' (e.g., '20250121'), but got '%s' with length %d.",
                    DAY_FORMAT, value, value.length());
        }
        if (!value.matches("\\d{8}")) {
            return String.format(
                    "Expected format '%s' with all digits, but got '%s'.", DAY_FORMAT, value);
        }
        // Validate date components
        int year = Integer.parseInt(value.substring(0, 4));
        int month = Integer.parseInt(value.substring(4, 6));
        int day = Integer.parseInt(value.substring(6, 8));
        return validateDateTimeComponents(year, month, day, -1, DAY_FORMAT, value);
    }

    private static String validateMonthFormat(String value) {
        // Expected format: yyyyMM (e.g., "202501")
        if (value.length() != 6) {
            return String.format(
                    "Expected format '%s' (e.g., '202501'), but got '%s' with length %d.",
                    MONTH_FORMAT, value, value.length());
        }
        if (!value.matches("\\d{6}")) {
            return String.format(
                    "Expected format '%s' with all digits, but got '%s'.", MONTH_FORMAT, value);
        }
        // Validate year and month
        int year = Integer.parseInt(value.substring(0, 4));
        int month = Integer.parseInt(value.substring(4, 6));
        if (year < 1970 || year > 9999) {
            return String.format("Invalid year %d in partition value '%s'.", year, value);
        }
        if (month < 1 || month > 12) {
            return String.format("Invalid month %d in partition value '%s'.", month, value);
        }
        return null;
    }

    private static String validateQuarterFormat(String value) {
        // Expected format: yyyyQ# (e.g., "2025Q1")
        if (value.length() != 6) {
            return String.format(
                    "Expected format '%s' (e.g., '2025Q1'), but got '%s' with length %d.",
                    QUARTER_FORMAT, value, value.length());
        }
        if (!value.matches("\\d{4}Q[1-4]")) {
            return String.format(
                    "Expected format '%s' (e.g., '2025Q1'), but got '%s'.", QUARTER_FORMAT, value);
        }
        // Validate year
        int year = Integer.parseInt(value.substring(0, 4));
        if (year < 1970 || year > 9999) {
            return String.format("Invalid year %d in partition value '%s'.", year, value);
        }
        return null;
    }

    private static String validateYearFormat(String value) {
        // Expected format: yyyy (e.g., "2025")
        if (value.length() != 4) {
            return String.format(
                    "Expected format '%s' (e.g., '2025'), but got '%s' with length %d.",
                    YEAR_FORMAT, value, value.length());
        }
        if (!value.matches("\\d{4}")) {
            return String.format(
                    "Expected format '%s' with all digits, but got '%s'.", YEAR_FORMAT, value);
        }
        // Validate year range
        int year = Integer.parseInt(value);
        if (year < 1970 || year > 9999) {
            return String.format("Invalid year %d in partition value '%s'.", year, value);
        }
        return null;
    }

    private static String validateDateTimeComponents(
            int year, int month, int day, int hour, String format, String value) {
        if (year < 1970 || year > 9999) {
            return String.format("Invalid year %d in partition value '%s'.", year, value);
        }
        if (month < 1 || month > 12) {
            return String.format("Invalid month %d in partition value '%s'.", month, value);
        }
        // Get max days for the month (considering leap year)
        int maxDays = getMaxDaysInMonth(year, month);
        if (day < 1 || day > maxDays) {
            return String.format(
                    "Invalid day %d for month %d in partition value '%s'.", day, month, value);
        }
        if (hour >= 0 && hour > 23) {
            return String.format("Invalid hour %d in partition value '%s'.", hour, value);
        }
        return null;
    }

    private static int getMaxDaysInMonth(int year, int month) {
        switch (month) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                return 31;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            case 2:
                // Leap year check
                boolean isLeapYear = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
                return isLeapYear ? 29 : 28;
            default:
                // Should never reach here as month is validated before calling this method
                throw new IllegalArgumentException("Invalid month: " + month);
        }
    }

    /**
     * Generate {@link ResolvedPartitionSpec} for auto partition in server. When we auto creating a
     * partition, we need to first generate a {@link ResolvedPartitionSpec}.
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param partitionKeys the partition keys
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the resolved partition spec
     */
    public static ResolvedPartitionSpec generateAutoPartition(
            List<String> partitionKeys,
            ZonedDateTime current,
            int offset,
            AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec = generateAutoPartitionTime(current, offset, timeUnit);

        return ResolvedPartitionSpec.fromPartitionName(partitionKeys, autoPartitionFieldSpec);
    }

    public static String generateAutoPartitionTime(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec;
        switch (timeUnit) {
            case YEAR:
                autoPartitionFieldSpec = getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
                break;
            case QUARTER:
                autoPartitionFieldSpec =
                        getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
                break;
            case MONTH:
                autoPartitionFieldSpec = getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
                break;
            case DAY:
                autoPartitionFieldSpec = getFormattedTime(current.plusDays(offset), DAY_FORMAT);
                break;
            case HOUR:
                autoPartitionFieldSpec = getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
        return autoPartitionFieldSpec;
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }

    public static String convertValueOfType(Object value, DataTypeRoot type) {
        String stringPartitionKey = "";
        switch (type) {
            case CHAR:
            case STRING:
                stringPartitionKey = ((BinaryString) value).toString();
                break;
            case BOOLEAN:
                Boolean booleanValue = (Boolean) value;
                stringPartitionKey = booleanValue.toString();
                break;
            case BINARY:
            case BYTES:
                byte[] bytesValue = (byte[]) value;
                stringPartitionKey = PartitionNameConverters.hexString(bytesValue);
                break;
            case TINYINT:
                Byte tinyIntValue = (Byte) value;
                stringPartitionKey = tinyIntValue.toString();
                break;
            case SMALLINT:
                Short smallIntValue = (Short) value;
                stringPartitionKey = smallIntValue.toString();
                break;
            case INTEGER:
                Integer intValue = (Integer) value;
                stringPartitionKey = intValue.toString();
                break;
            case BIGINT:
                Long bigIntValue = (Long) value;
                stringPartitionKey = bigIntValue.toString();
                break;
            case DATE:
                Integer dateValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.dayToString(dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                Integer timeValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.milliToString(timeValue);
                break;
            case FLOAT:
                Float floatValue = (Float) value;
                stringPartitionKey = PartitionNameConverters.reformatFloat(floatValue);
                break;
            case DOUBLE:
                Double doubleValue = (Double) value;
                stringPartitionKey = PartitionNameConverters.reformatDouble(doubleValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz timeStampLTZValue = (TimestampLtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampLTZValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz timeStampNTZValue = (TimestampNtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampNTZValue);
                break;
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
        return stringPartitionKey;
    }
}
