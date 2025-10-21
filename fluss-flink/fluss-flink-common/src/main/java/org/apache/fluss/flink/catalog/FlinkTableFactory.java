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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.lake.LakeTableFactory;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.StringUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.config.FlussConfigUtils.CLIENT_PREFIX;
import static org.apache.fluss.flink.catalog.FlinkCatalog.LAKE_TABLE_SPLITTER;
import static org.apache.fluss.flink.utils.DataLakeUtils.getDatalakeFormat;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getBucketKeyIndexes;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getBucketKeys;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkOption;

/** Factory to create table source and table sink for Fluss. */
public class FlinkTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private volatile LakeTableFactory lakeTableFactory;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // check whether should read from datalake
        ObjectIdentifier tableIdentifier = context.getObjectIdentifier();
        String tableName = tableIdentifier.getObjectName();
        if (tableName.contains(LAKE_TABLE_SPLITTER)) {
            tableName = tableName.substring(0, tableName.indexOf(LAKE_TABLE_SPLITTER));
            lakeTableFactory = mayInitLakeTableFactory();
            return lakeTableFactory.createDynamicTableSource(context, tableName);
        }

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        Optional<DataLakeFormat> datalakeFormat = getDatalakeFormat(tableOptions);
        List<String> prefixesToSkip = new ArrayList<>(Arrays.asList("table.", "client."));
        datalakeFormat.ifPresent(dataLakeFormat -> prefixesToSkip.add(dataLakeFormat + "."));
        helper.validateExcept(prefixesToSkip.toArray(new String[0]));

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        RowType tableOutputType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        FlinkConnectorOptionsUtils.validateTableSourceOptions(tableOptions);

        ZoneId timeZone =
                FlinkConnectorOptionsUtils.getLocalTimeZone(
                        context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE));
        final FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                FlinkConnectorOptionsUtils.getStartupOptions(tableOptions, timeZone);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
        int[] primaryKeyIndexes = resolvedSchema.getPrimaryKeyIndexes();
        int[] partitionKeyIndexes =
                resolvedCatalogTable.getPartitionKeys().stream()
                        .mapToInt(tableOutputType::getFieldIndex)
                        .toArray();
        int[] bucketKeyIndexes = getBucketKeyIndexes(tableOptions, tableOutputType);

        // options for lookup
        LookupCache cache = null;
        LookupOptions.LookupCacheType lookupCacheType = tableOptions.get(LookupOptions.CACHE_TYPE);
        if (lookupCacheType.equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        } else if (lookupCacheType.equals(LookupOptions.LookupCacheType.FULL)) {
            // currently, flink framework only support InputFormatProvider
            // as ScanRuntimeProviders for Full caching lookup join, so in here, we just throw
            // unsupported exception
            throw new UnsupportedOperationException("Full lookup caching is not supported yet.");
        }

        // other option values
        long partitionDiscoveryIntervalMs =
                tableOptions
                        .get(FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL)
                        .toMillis();

        // Parse secondary indexes from table options
        int[][] secondaryIndexes = parseSecondaryIndexes(tableOptions, tableOutputType);

        return new FlinkTableSource(
                toFlussTablePath(context.getObjectIdentifier()),
                toFlussClientConfig(
                        context.getCatalogTable().getOptions(), context.getConfiguration()),
                tableOutputType,
                primaryKeyIndexes,
                bucketKeyIndexes,
                partitionKeyIndexes,
                isStreamingMode,
                startupOptions,
                tableOptions.get(LookupOptions.MAX_RETRIES),
                tableOptions.get(FlinkConnectorOptions.LOOKUP_ASYNC),
                cache,
                partitionDiscoveryIntervalMs,
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_DATALAKE_ENABLED)),
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_MERGE_ENGINE)),
                context.getCatalogTable().getOptions(),
                secondaryIndexes);
    }

    /**
     * Parse secondary indexes from table options. Returns array of column indexes for each index.
     */
    private static int[][] parseSecondaryIndexes(
            ReadableConfig tableOptions,
            org.apache.flink.table.types.logical.RowType tableOutputType) {
        String configValue =
                tableOptions
                        .getOptional(toFlinkOption(ConfigOptions.TABLE_INDEXES_COLUMNS))
                        .orElse("");

        if (StringUtils.isNullOrWhitespaceOnly(configValue)) {
            return null;
        }

        java.util.List<int[]> indexList = new java.util.ArrayList<>();
        String[] indexConfigs = configValue.split(";");

        for (String indexConfig : indexConfigs) {
            indexConfig = indexConfig.trim();
            if (StringUtils.isNullOrWhitespaceOnly(indexConfig)) {
                continue;
            }

            String[] columns = indexConfig.split(",");
            java.util.List<String> columnList =
                    java.util.Arrays.stream(columns)
                            .map(String::trim)
                            .filter(col -> !StringUtils.isNullOrWhitespaceOnly(col))
                            .collect(java.util.stream.Collectors.toList());

            if (columnList.isEmpty()) {
                continue;
            }

            // Convert column names to column indexes
            int[] columnIndexes = new int[columnList.size()];
            for (int i = 0; i < columnList.size(); i++) {
                int fieldIndex = tableOutputType.getFieldIndex(columnList.get(i));
                if (fieldIndex < 0) {
                    throw new IllegalArgumentException(
                            "Column '" + columnList.get(i) + "' not found in table schema");
                }
                columnIndexes[i] = fieldIndex;
            }

            indexList.add(columnIndexes);
        }

        return indexList.isEmpty() ? null : indexList.toArray(new int[0][]);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        Optional<DataLakeFormat> datalakeFormat = getDatalakeFormat(tableOptions);
        if (datalakeFormat.isPresent()) {
            helper.validateExcept("table.", "client.", datalakeFormat.get() + ".");
        } else {
            helper.validateExcept("table.", "client.");
        }

        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        ResolvedCatalogTable resolvedCatalogTable = context.getCatalogTable();
        List<String> partitionKeys = resolvedCatalogTable.getPartitionKeys();

        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();

        return new FlinkTableSink(
                toFlussTablePath(context.getObjectIdentifier()),
                toFlussClientConfig(
                        context.getCatalogTable().getOptions(), context.getConfiguration()),
                rowType,
                context.getPrimaryKeyIndexes(),
                partitionKeys,
                isStreamingMode,
                tableOptions.get(toFlinkOption(ConfigOptions.TABLE_MERGE_ENGINE)),
                tableOptions.get(toFlinkOption(TABLE_DATALAKE_FORMAT)),
                tableOptions.get(FlinkConnectorOptions.SINK_IGNORE_DELETE),
                tableOptions.get(FlinkConnectorOptions.BUCKET_NUMBER),
                getBucketKeys(tableOptions),
                tableOptions.get(FlinkConnectorOptions.SINK_BUCKET_SHUFFLE));
    }

    @Override
    public String factoryIdentifier() {
        return FlinkCatalogFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Collections.singletonList(FlinkConnectorOptions.BOOTSTRAP_SERVERS));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options =
                new HashSet<>(
                        Arrays.asList(
                                FlinkConnectorOptions.BUCKET_KEY,
                                FlinkConnectorOptions.BUCKET_NUMBER,
                                FlinkConnectorOptions.SCAN_STARTUP_MODE,
                                FlinkConnectorOptions.SCAN_STARTUP_TIMESTAMP,
                                FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL,
                                FlinkConnectorOptions.LOOKUP_ASYNC,
                                FlinkConnectorOptions.SINK_IGNORE_DELETE,
                                FlinkConnectorOptions.SINK_BUCKET_SHUFFLE,
                                LookupOptions.MAX_RETRIES,
                                LookupOptions.CACHE_TYPE,
                                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                                LookupOptions.PARTIAL_CACHE_MAX_ROWS));
        // forward all fluss table and client options
        options.addAll(FlinkConnectorOptions.TABLE_OPTIONS);
        options.addAll(FlinkConnectorOptions.CLIENT_OPTIONS);
        return options;
    }

    private static Configuration toFlussClientConfig(
            Map<String, String> tableOptions, ReadableConfig flinkConfig) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                tableOptions.get(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key()));

        // forward all client configs
        tableOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(CLIENT_PREFIX)) {
                        flussConfig.setString(key, value);
                    }
                });

        // pass flink io tmp dir to fluss client.
        flussConfig.setString(
                ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR,
                new File(flinkConfig.get(CoreOptions.TMP_DIRS), "/fluss").getAbsolutePath());
        return flussConfig;
    }

    private static TablePath toFlussTablePath(ObjectIdentifier tablePath) {
        return TablePath.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    private LakeTableFactory mayInitLakeTableFactory() {
        if (lakeTableFactory == null) {
            synchronized (this) {
                if (lakeTableFactory == null) {
                    lakeTableFactory = new LakeTableFactory();
                }
            }
        }
        return lakeTableFactory;
    }
}
