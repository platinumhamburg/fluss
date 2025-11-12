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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metadata.SchemaGetterFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** API for configuring and creating {@link Lookuper}. */
public class TableLookup implements Lookup {

    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;
    private final SchemaGetterFactory schemaGetterFactory;

    @Nullable private final List<String> lookupColumnNames;

    private final Configuration conf;

    public TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            Configuration conf,
            SchemaGetterFactory schemaGetterFactory) {
        this(
                tableInfo,
                schemaGetter,
                metadataUpdater,
                lookupClient,
                conf,
                schemaGetterFactory,
                null);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            Configuration conf,
            SchemaGetterFactory schemaGetterFactory,
            @Nullable List<String> lookupColumnNames) {
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.conf = conf;
        this.schemaGetterFactory = schemaGetterFactory;
        this.lookupColumnNames = lookupColumnNames;
    }

    @Override
    public Lookup lookupBy(List<String> lookupColumnNames) {
        return new TableLookup(
                tableInfo,
                schemaGetter,
                metadataUpdater,
                lookupClient,
                conf,
                schemaGetterFactory,
                lookupColumnNames);
    }

    @Override
    public Lookuper createLookuper() {
        if (lookupColumnNames == null) {
            return new PrimaryKeyLookuper(tableInfo, schemaGetter, metadataUpdater, lookupClient);
        } else {
            // Check if this is a secondary index lookup
            Schema.Index matchedIndex = findMatchingSecondaryIndex(lookupColumnNames);
            if (matchedIndex != null && isValidForSecondaryIndexLookup()) {
                // Create SecondaryIndexLookuper for secondary index lookup
                TablePath indexTablePath =
                        IndexTableUtils.generateIndexTablePath(
                                tableInfo.getTablePath(), matchedIndex.getIndexName());
                // Ensure metadata cache is updated before getting index table info
                metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(indexTablePath));
                TableInfo indexTableInfo =
                        metadataUpdater
                                .getCluster()
                                .getTable(indexTablePath)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Index table not found: "
                                                                + indexTablePath));

                // Create a separate SchemaGetter for the index table to ensure correct schema
                // resolution
                // Index table has its own schema and schema id, different from the main table
                // Use the factory to create SchemaGetter, which encapsulates Admin dependency
                SchemaGetter indexTableSchemaGetter =
                        schemaGetterFactory.create(indexTablePath, indexTableInfo.getSchemaInfo());

                return new SecondaryIndexLookuper(
                        tableInfo,
                        indexTableInfo,
                        schemaGetter,
                        indexTableSchemaGetter,
                        metadataUpdater,
                        lookupClient,
                        lookupColumnNames,
                        lookupClient.getLookuperMetricGroup(),
                        conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE));
            } else {
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
        if (IndexTableUtils.isIndexTable(tableInfo.getTablePath().getTableName())) {
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
        Set<String> lookupColumnsSet = new HashSet<>(lookupColumnNames);

        for (Schema.Index index : tableInfo.getSchema().getIndexes()) {
            Set<String> indexColumnsSet = new HashSet<>(index.getColumnNames());
            // Check if lookup columns exactly match index columns
            if (lookupColumnsSet.equals(indexColumnsSet)) {
                return index;
            }
        }

        return null;
    }
}
