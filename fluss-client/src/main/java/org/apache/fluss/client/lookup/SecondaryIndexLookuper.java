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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link Lookuper} that lookups by secondary index. This is a composite
 * lookuper that first queries the index table to get primary keys, then queries the main table
 * using those primary keys.
 */
@VisibleForTesting
public class SecondaryIndexLookuper implements Lookuper {

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexLookuper.class);

    /** Lookuper for querying the index table. */
    private final PrefixKeyLookuper indexTableLookuper;

    /** Lookuper for querying the main table. */
    private final PrimaryKeyLookuper mainTableLookuper;

    /** Projection to extract primary key from index table results. */
    private final ProjectedRow primaryKeyProjection;

    public SecondaryIndexLookuper(
            TableInfo mainTableInfo,
            TableInfo indexTableInfo,
            Schema.Index index,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames) {

        // Create index table lookuper to query index table using secondary index keys
        this.indexTableLookuper =
                new PrefixKeyLookuper(
                        indexTableInfo, metadataUpdater, lookupClient, lookupColumnNames);

        // Create main table lookuper to query main table using primary keys
        this.mainTableLookuper =
                new PrimaryKeyLookuper(mainTableInfo, metadataUpdater, lookupClient);

        // Create projection to extract primary key columns from index table results
        List<String> mainTablePrimaryKeys = mainTableInfo.getPrimaryKeys();
        int[] primaryKeyIndexes = new int[mainTablePrimaryKeys.size()];
        List<String> indexTableColumns = indexTableInfo.getRowType().getFieldNames();

        for (int i = 0; i < mainTablePrimaryKeys.size(); i++) {
            primaryKeyIndexes[i] = indexTableColumns.indexOf(mainTablePrimaryKeys.get(i));
        }

        this.primaryKeyProjection = ProjectedRow.from(primaryKeyIndexes);

        LOG.debug(
                "Created SecondaryIndexLookuper for main table {} via index table {}, lookup columns: {}",
                mainTableInfo.getTablePath(),
                indexTableInfo.getTablePath(),
                lookupColumnNames);
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        long startTime = System.currentTimeMillis();
        LOG.debug("Starting secondary index lookup with key: {}", lookupKey);

        // Step 1: Query index table to get primary keys
        return indexTableLookuper
                .lookup(lookupKey)
                .thenCompose(
                        indexResult -> {
                            List<InternalRow> indexRows = indexResult.getRowList();
                            LOG.debug("Index table lookup returned {} records", indexRows.size());

                            if (indexRows.isEmpty()) {
                                // No matching records found in index table
                                LOG.debug(
                                        "No matching records found in index table for key: {}",
                                        lookupKey);
                                return CompletableFuture.completedFuture(
                                        new LookupResult(new ArrayList<>()));
                            }

                            // Step 2: Extract primary keys from index table results and query main
                            // table
                            LOG.debug(
                                    "Querying main table with {} primary keys extracted from index",
                                    indexRows.size());
                            List<CompletableFuture<LookupResult>> mainTableLookups =
                                    new ArrayList<>();

                            for (InternalRow indexRow : indexRows) {
                                // Extract primary key from index table row
                                InternalRow primaryKey = primaryKeyProjection.replaceRow(indexRow);

                                // Query main table using primary key
                                CompletableFuture<LookupResult> mainLookup =
                                        mainTableLookuper
                                                .lookup(primaryKey)
                                                .exceptionally(
                                                        throwable -> {
                                                            // Unwrap the throwable to check the
                                                            // root cause
                                                            Throwable cause = throwable.getCause();
                                                            while (cause != null
                                                                    && cause.getCause() != null) {
                                                                cause = cause.getCause();
                                                            }
                                                            // If it's
                                                            // UnknownTableOrBucketException,
                                                            // convert to empty result and log
                                                            // warning
                                                            if (cause
                                                                    instanceof
                                                                    UnknownTableOrBucketException) {
                                                                LOG.warn(
                                                                        "Primary key lookup failed due to unknown table/bucket, possibly due to concurrent table operations. Key: {}, Error: {}",
                                                                        primaryKey,
                                                                        cause.getMessage());
                                                                return new LookupResult(
                                                                        (InternalRow) null);
                                                            } else {
                                                                LOG.error(
                                                                        "Unexpected error during main table lookup for primary key: {}",
                                                                        primaryKey,
                                                                        throwable);
                                                                // For other exceptions, rethrow
                                                                throw new RuntimeException(
                                                                        throwable);
                                                            }
                                                        });
                                mainTableLookups.add(mainLookup);
                            }

                            // Wait for all main table lookups to complete
                            return CompletableFuture.allOf(
                                            mainTableLookups.toArray(new CompletableFuture[0]))
                                    .thenApply(
                                            v -> {
                                                List<InternalRow> result = new ArrayList<>();
                                                int nullResults = 0;
                                                for (CompletableFuture<LookupResult> future :
                                                        mainTableLookups) {
                                                    LookupResult lookupResult = future.join();
                                                    InternalRow row =
                                                            lookupResult.getSingletonRow();
                                                    if (row != null) {
                                                        result.add(row);
                                                    } else {
                                                        nullResults++;
                                                    }
                                                }

                                                long duration =
                                                        System.currentTimeMillis() - startTime;
                                                LOG.debug(
                                                        "Secondary index lookup completed: {} index records -> {} main table results ({} null results) in {} ms",
                                                        indexRows.size(),
                                                        result.size(),
                                                        nullResults,
                                                        duration);

                                                if (nullResults > 0) {
                                                    LOG.debug(
                                                            "Found {} null results during main table lookup, indicating potential data inconsistency between index and main table",
                                                            nullResults);
                                                }

                                                return new LookupResult(result);
                                            });
                        });
    }
}
