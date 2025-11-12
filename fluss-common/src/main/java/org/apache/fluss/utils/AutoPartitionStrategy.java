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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.TimeZone;

/** A class wrapping the strategy for auto partition. */
public class AutoPartitionStrategy {

    private final boolean autoPartitionEnable;
    private final String key;
    private final AutoPartitionTimeUnit timeUnit;
    private final int numPreCreate;
    private final int numToRetain;
    private final TimeZone timeZone;

    // Pre-computed runtime fields for extracting partition timestamps from data rows
    private final InternalRow.FieldGetter timePartitionFieldGetter;
    private final DataType timePartitionColumnType;

    private AutoPartitionStrategy(
            boolean autoPartitionEnable,
            String key,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            int numPreCreate,
            int numToRetain,
            TimeZone timeZone,
            @Nullable InternalRow.FieldGetter timePartitionFieldGetter,
            @Nullable DataType timePartitionColumnType) {
        this.autoPartitionEnable = autoPartitionEnable;
        this.key = key;
        this.timeUnit = autoPartitionTimeUnit;
        this.numPreCreate = numPreCreate;
        this.numToRetain = numToRetain;
        this.timeZone = timeZone;
        this.timePartitionFieldGetter = timePartitionFieldGetter;
        this.timePartitionColumnType = timePartitionColumnType;
    }

    public static AutoPartitionStrategy from(Map<String, String> options) {
        return from(Configuration.fromMap(options));
    }

    public static AutoPartitionStrategy from(Configuration conf) {
        return from(conf, null, null, null);
    }

    /**
     * Creates an AutoPartitionStrategy from configuration with partition keys. For single-column
     * partitioned tables, the key will be set to that partition column automatically.
     *
     * @param conf the configuration
     * @param partitionKeys the list of partition keys, can be null if not available
     * @return the auto partition strategy
     */
    public static AutoPartitionStrategy from(
            Configuration conf, @Nullable java.util.List<String> partitionKeys) {
        return from(conf, partitionKeys, null, null);
    }

    /**
     * Creates an AutoPartitionStrategy from configuration with partition keys and schema
     * information. For single-column partitioned tables, the key will be set to that partition
     * column automatically. If schema information is provided, the FieldGetter and DataType will be
     * pre-computed.
     *
     * @param conf the configuration
     * @param partitionKeys the list of partition keys, can be null if not available
     * @param rowType the row type of the table, can be null if not available
     * @param columnNames the list of column names, can be null if not available
     * @return the auto partition strategy
     */
    public static AutoPartitionStrategy from(
            Configuration conf,
            @Nullable java.util.List<String> partitionKeys,
            @Nullable org.apache.fluss.types.RowType rowType,
            @Nullable java.util.List<String> columnNames) {
        String configuredKey = conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_KEY);

        // Determine the effective key based on partition keys
        String effectiveKey;
        if (partitionKeys != null && partitionKeys.size() == 1) {
            // For single-column partitioned tables, use that column as the time partition key
            effectiveKey = partitionKeys.get(0);
        } else {
            // For multi-column partitioned tables or when partition keys are unknown,
            // use the configured key
            effectiveKey = configuredKey;
        }

        // Pre-compute runtime fields if schema information is available
        InternalRow.FieldGetter fieldGetter = null;
        DataType columnType = null;
        if (effectiveKey != null
                && !effectiveKey.isEmpty()
                && rowType != null
                && columnNames != null) {
            int columnIndex = columnNames.indexOf(effectiveKey);
            if (columnIndex >= 0) {
                columnType = rowType.getTypeAt(columnIndex);
                fieldGetter = InternalRow.createFieldGetter(columnType, columnIndex);
            }
        }

        return new AutoPartitionStrategy(
                conf.getBoolean(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED),
                effectiveKey,
                conf.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE),
                conf.getInt(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION),
                TimeZone.getTimeZone(conf.getString(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE)),
                fieldGetter,
                columnType);
    }

    public boolean isAutoPartitionEnabled() {
        return autoPartitionEnable;
    }

    public String key() {
        return key;
    }

    public AutoPartitionTimeUnit timeUnit() {
        return timeUnit;
    }

    public int numPreCreate() {
        return numPreCreate;
    }

    public int numToRetain() {
        return numToRetain;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    /**
     * Returns the pre-computed field getter for the time partition column, or null if not
     * available.
     */
    @Nullable
    public InternalRow.FieldGetter getTimePartitionFieldGetter() {
        return timePartitionFieldGetter;
    }

    /**
     * Returns the pre-computed data type of the time partition column, or null if not available.
     */
    @Nullable
    public DataType getTimePartitionColumnType() {
        return timePartitionColumnType;
    }
}
