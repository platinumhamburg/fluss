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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/** API for configuring and creating {@link Lookuper}. */
public class TableLookup implements Lookup {

    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;
    private final Connection connection;

    @Nullable private final List<String> lookupColumnNames;

    private final boolean insertIfNotExists;

    public TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            Connection connection) {
        this(tableInfo, schemaGetter, metadataUpdater, lookupClient, connection, null, false);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            Connection connection,
            @Nullable List<String> lookupColumnNames,
            boolean insertIfNotExists) {
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.connection = connection;
        this.lookupColumnNames = lookupColumnNames;
        this.insertIfNotExists = insertIfNotExists;
    }

    @Override
    public Lookup enableInsertIfNotExists() {
        return new TableLookup(
                tableInfo,
                schemaGetter,
                metadataUpdater,
                lookupClient,
                connection,
                lookupColumnNames,
                true);
    }

    @Override
    public Lookup lookupBy(List<String> lookupColumnNames) {
        return new TableLookup(
                tableInfo,
                schemaGetter,
                metadataUpdater,
                lookupClient,
                connection,
                lookupColumnNames,
                insertIfNotExists);
    }

    @Override
    public Lookuper createLookuper() {
        if (lookupColumnNames == null) {
            if (insertIfNotExists) {
                // get all non-nullable columns that are not primary key columns and auto increment
                // columns, if there is any, throw exception, as we cannot fill values for those
                // columns when doing insert if not exists.
                List<String> notNullColumnNames =
                        tableInfo.getSchema().getColumns().stream()
                                .filter(column -> !column.getDataType().isNullable())
                                .map(Schema.Column::getName)
                                .filter(name -> !tableInfo.getPrimaryKeys().contains(name))
                                .filter(
                                        name ->
                                                !tableInfo
                                                        .getSchema()
                                                        .getAutoIncrementColumnNames()
                                                        .contains(name))
                                .collect(Collectors.toList());
                if (!notNullColumnNames.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Lookup with insertIfNotExists enabled cannot be created for table '"
                                    + tableInfo.getTablePath()
                                    + "', because it contains non-nullable columns that are not primary key columns or auto increment columns: "
                                    + notNullColumnNames
                                    + ". ");
                }
            }
            return new PrimaryKeyLookuper(
                    tableInfo, schemaGetter, metadataUpdater, lookupClient, insertIfNotExists);
        } else {
            // throw exception if the insertIfNotExists is enabled for prefix key lookup, as
            // currently we only support insertIfNotExists for primary key lookup.
            if (insertIfNotExists) {
                throw new IllegalArgumentException(
                        "insertIfNotExists cannot be enabled for prefix key lookup, as currently we only support insertIfNotExists for primary key lookup.");
            }
            // Check if this is a secondary index lookup
            Schema.Index matchedIndex = findMatchingSecondaryIndex(lookupColumnNames);
            if (matchedIndex != null && isValidForSecondaryIndexLookup()) {
                // Create SecondaryIndexLookuper for secondary index lookup
                TablePath indexTablePath =
                        TablePath.forIndexTable(
                                tableInfo.getTablePath(), matchedIndex.getIndexName());
                // Ensure metadata cache is updated before getting index table info
                metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(indexTablePath));
                TableInfo indexTableInfo;
                try {
                    indexTableInfo =
                            connection
                                    .getAdmin()
                                    .getTableInfo(indexTablePath)
                                    .get(30, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    throw new FlussRuntimeException(
                            "Timed out getting index table info for "
                                    + indexTablePath
                                    + " while creating secondary index lookuper for "
                                    + tableInfo.getTablePath(),
                            e);
                } catch (ExecutionException e) {
                    throw new FlussRuntimeException(
                            "Failed to get index table info for "
                                    + indexTablePath
                                    + " while creating secondary index lookuper for "
                                    + tableInfo.getTablePath(),
                            e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FlussRuntimeException(
                            "Interrupted while getting index table info for " + indexTablePath, e);
                }

                // Create SchemaGetter for index table
                SchemaGetter indexTableSchemaGetter =
                        new ClientSchemaGetter(
                                indexTablePath,
                                indexTableInfo.getSchemaInfo(),
                                connection.getAdmin());

                return new SecondaryIndexLookuper(
                        tableInfo,
                        indexTableInfo,
                        schemaGetter,
                        indexTableSchemaGetter,
                        metadataUpdater,
                        lookupClient,
                        lookupColumnNames);
            } else {
                if (lookupKeysArePrimaryKeys()) {
                    throw new IllegalArgumentException(
                            "Can not perform prefix lookup on table '"
                                    + tableInfo.getTablePath()
                                    + "', because the lookup columns "
                                    + lookupColumnNames
                                    + " equals the physical primary keys "
                                    + tableInfo.getPrimaryKeys()
                                    + ". Please use primary key lookup (Lookuper without lookupBy) instead.");
                }
                // Use PrefixKeyLookuper for prefix lookup
                return new PrefixKeyLookuper(
                        tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames);
            }
        }
    }

    /**
     * Checks if the current table is valid for secondary index lookup.
     *
     * @return true if the table is not an index table and has indexes defined
     */
    private boolean isValidForSecondaryIndexLookup() {
        // Check if current table is not an index table
        if (tableInfo.isIndexTable()) {
            return false;
        }

        // Check if current table has indexes
        return !tableInfo.getSchema().getIndexes().isEmpty();
    }

    /**
     * Finds a matching secondary index for the given lookup column names.
     *
     * @param lookupColumnNames the column names to lookup by
     * @return the matching index, or null if no match found
     */
    @Nullable
    private Schema.Index findMatchingSecondaryIndex(List<String> lookupColumnNames) {
        for (Schema.Index index : tableInfo.getSchema().getIndexes()) {
            // Require exact column order for composite index correctness
            if (index.getColumnNames().equals(lookupColumnNames)) {
                return index;
            }
        }

        return null;
    }

    private boolean lookupKeysArePrimaryKeys() {
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        return lookupColumnNames != null
                && lookupColumnNames.size() == primaryKeys.size()
                && new HashSet<>(lookupColumnNames).containsAll(primaryKeys);
    }

    @Override
    public <T> TypedLookuper<T> createTypedLookuper(Class<T> pojoClass) {
        return new TypedLookuperImpl<>(createLookuper(), tableInfo, lookupColumnNames, pojoClass);
    }
}
