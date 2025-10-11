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

package org.apache.fluss.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.types.DataTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Utilities for index table metadata operations. */
public final class IndexTableUtils {

    private IndexTableUtils() {
        // Utility class, no instantiation
    }

    /**
     * Checks if a table name represents an index table based on naming convention.
     *
     * <p>Index table names follow the pattern: "__" + mainTableName + "__" + "index" + "__" +
     * indexName
     *
     * @param tableName the table name to check
     * @return true if the table name represents an index table, false otherwise
     */
    public static boolean isIndexTable(String tableName) {
        return tableName.startsWith("__") && tableName.contains("__index__");
    }

    /**
     * Extracts the main table name from an index table name.
     *
     * <p>Index table names follow the pattern: "__" + mainTableName + "__" + "index" + "__" +
     * indexName
     *
     * @param indexTableName the index table name
     * @return the main table name, or null if the table name is not a valid index table name
     */
    public static String extractMainTableName(String indexTableName) {
        if (!isIndexTable(indexTableName)) {
            return null;
        }
        // Remove the "__" prefix and find "__index__" suffix
        String withoutPrefix = indexTableName.substring(2);
        int indexSuffix = withoutPrefix.indexOf("__index__");
        if (indexSuffix == -1) {
            return null;
        }
        return withoutPrefix.substring(0, indexSuffix);
    }

    /**
     * Generates the table path for the data table based on the index table path.
     *
     * <p>The data table name is the same as the main table name.
     *
     * @param indexTablePath the path of the index table
     * @return the table path for the data table
     */
    public static TablePath generateDataTablePath(TablePath indexTablePath) {
        String mainTableName = extractMainTableName(indexTablePath.getTableName());
        return TablePath.of(indexTablePath.getDatabaseName(), mainTableName);
    }

    /**
     * Extracts the index name from an index table name.
     *
     * <p>Index table names follow the pattern: "__" + mainTableName + "__" + "index" + "__" +
     * indexName
     *
     * @param indexTableName the index table name
     * @return the index name, or null if the table name is not a valid index table name
     */
    public static String extractIndexName(String indexTableName) {
        if (!isIndexTable(indexTableName)) {
            return null;
        }
        // Find "__index__" and extract everything after it
        int indexPos = indexTableName.indexOf("__index__");
        if (indexPos == -1) {
            return null;
        }
        return indexTableName.substring(indexPos + "__index__".length());
    }

    /**
     * Generates the table path for an index table based on the main table path and index name.
     *
     * <p>The index table name follows the pattern: "__" + mainTableName + "__" + "index" + "__" +
     * indexName
     *
     * @param mainTablePath the path of the main table
     * @param indexName the name of the index
     * @return the table path for the index table
     */
    public static TablePath generateIndexTablePath(TablePath mainTablePath, String indexName) {
        String indexTableName =
                "__" + mainTablePath.getTableName() + "__" + "index" + "__" + indexName;
        return new TablePath(mainTablePath.getDatabaseName(), indexTableName);
    }

    /**
     * Creates a table descriptor for an index table based on the main table descriptor and index
     * definition.
     *
     * <p>The index table schema includes:
     *
     * <ul>
     *   <li>All index columns
     *   <li>All primary key columns from the main table
     *   <li>A system internal column __offset (BIGINT) to store the logOffset in DataBucket
     * </ul>
     *
     * <p>The primary key of the index table consists of index columns + primary key columns to
     * ensure uniqueness.
     *
     * @param mainTableDescriptor the descriptor of the main table
     * @param index the index definition
     * @param defaultBucketNumber the default bucket number to use if not specified
     * @return the table descriptor for the index table
     */
    public static TableDescriptor createIndexTableDescriptor(
            TableDescriptor mainTableDescriptor, Schema.Index index, int defaultBucketNumber) {
        Schema mainSchema = mainTableDescriptor.getSchema();
        List<String> mainPrimaryKeyColumns = mainSchema.getPrimaryKeyColumnNames();
        List<String> indexColumns = index.getColumnNames();

        // Index table should include only necessary columns in the correct order:
        // 1. index columns first
        // 2. primary key columns (excluding already included index columns)
        List<String> orderedColumnNames = new ArrayList<>();
        Set<String> addedColumns = new HashSet<>();

        // Add index columns first
        for (String indexColumn : indexColumns) {
            if (!addedColumns.contains(indexColumn)) {
                orderedColumnNames.add(indexColumn);
                addedColumns.add(indexColumn);
            }
        }

        // Add primary key columns (excluding duplicates)
        for (String pkColumn : mainPrimaryKeyColumns) {
            if (!addedColumns.contains(pkColumn)) {
                orderedColumnNames.add(pkColumn);
                addedColumns.add(pkColumn);
            }
        }

        // create columns for index table in the correct order
        Map<String, Schema.Column> columnMap = new HashMap<>();
        for (Schema.Column column : mainSchema.getColumns()) {
            columnMap.put(column.getName(), column);
        }

        List<Schema.Column> indexTableColumns = new ArrayList<>();
        for (String columnName : orderedColumnNames) {
            indexTableColumns.add(columnMap.get(columnName));
        }

        // add system internal column __offset to store the logOffset in DataBucket
        indexTableColumns.add(new Schema.Column("__offset", DataTypes.BIGINT()));

        // create schema for index table
        // primary key of index table = index columns + primary key columns (to ensure uniqueness)
        List<String> indexTablePrimaryKey = new ArrayList<>(indexColumns);
        for (String pkColumn : mainPrimaryKeyColumns) {
            if (!indexTablePrimaryKey.contains(pkColumn)) {
                indexTablePrimaryKey.add(pkColumn);
            }
        }

        Schema indexTableSchema =
                Schema.newBuilder()
                        .fromColumns(indexTableColumns)
                        .primaryKeyNamed("pk_" + index.getIndexName(), indexTablePrimaryKey)
                        .build();

        // use index bucket count if configured, otherwise use default value
        int indexBucketCount =
                Optional.ofNullable(
                                mainTableDescriptor
                                        .getProperties()
                                        .get(ConfigOptions.TABLE_INDEX_BUCKET_NUM.key()))
                        .map(Integer::parseInt)
                        .orElse(ConfigOptions.TABLE_INDEX_BUCKET_NUM.defaultValue());

        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(indexTableSchema)
                        .kvFormat(KvFormat.INDEXED)
                        .logFormat(LogFormat.INDEXED)
                        .property(
                                ConfigOptions.TABLE_REPLICATION_FACTOR.key(),
                                String.valueOf(indexBucketCount))
                        .distributedBy(indexBucketCount, indexColumns);

        return builder.build();
    }
}
