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
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Lists;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.InternalRowUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link Lookuper} that lookups by secondary index. This is a composite
 * lookuper that first queries the index table to get primary keys, then queries the main table
 * using those primary keys.
 *
 * <p>Thread Safety: This class is designed to be thread-safe for concurrent lookup operations by
 * using thread-local lookuper instances, because {@link PrefixKeyLookuper} and {@link
 * PrimaryKeyLookuper} are not thread-safe. For each lookup, a new {@link ProjectedRow} instance is
 * created to avoid mutable state sharing between concurrent operations.
 */
@VisibleForTesting
public class SecondaryIndexLookuper implements Lookuper {

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexLookuper.class);

    /** Default batch size for main table lookups to control lookup pressure. */
    private static final int DEFAULT_MAIN_TABLE_LOOKUP_BATCH_SIZE = 512;

    /** Thread-local lookuper for querying the index table. */
    private final ThreadLocal<PrefixKeyLookuper> indexTableLookuper;

    /**
     * Thread-local PrimaryKeyLookuper instance for main table lookups. This avoids sharing
     * non-thread-safe encoders across threads.
     */
    private final ThreadLocal<PrimaryKeyLookuper> mainTableLookuper;

    /** Primary key indexes for creating ProjectedRow instances. */
    private final int[] primaryKeyIndexes;

    private final RowType primaryKeyRowType;

    private final RowType lookupRowType;

    public SecondaryIndexLookuper(
            TableInfo mainTableInfo,
            TableInfo indexTableInfo,
            SchemaGetter mainTableSchemaGetter,
            SchemaGetter indexTableSchemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames) {

        // Create index table lookuper to query index table using secondary index keys
        // Use indexTableSchemaGetter for index table to ensure correct schema resolution
        this.indexTableLookuper =
                ThreadLocal.withInitial(
                        () ->
                                new PrefixKeyLookuper(
                                        indexTableInfo,
                                        indexTableSchemaGetter,
                                        metadataUpdater,
                                        lookupClient,
                                        lookupColumnNames));

        // Create a single shared PrimaryKeyLookuper instance for main table lookups.
        // This is thread-safe because lookup() only reads immutable state and performs async ops.
        this.mainTableLookuper =
                ThreadLocal.withInitial(
                        () ->
                                new PrimaryKeyLookuper(
                                        mainTableInfo,
                                        mainTableSchemaGetter,
                                        metadataUpdater,
                                        lookupClient));

        // Calculate primary key indexes for creating ProjectedRow instances
        List<String> mainTablePrimaryKeys = mainTableInfo.getPrimaryKeys();
        this.primaryKeyIndexes = new int[mainTablePrimaryKeys.size()];
        List<String> indexTableColumns = indexTableInfo.getRowType().getFieldNames();

        for (int i = 0; i < mainTablePrimaryKeys.size(); i++) {
            this.primaryKeyIndexes[i] = indexTableColumns.indexOf(mainTablePrimaryKeys.get(i));
        }

        this.primaryKeyRowType = mainTableInfo.getRowType().project(mainTablePrimaryKeys);
        this.lookupRowType = indexTableInfo.getRowType().project(lookupColumnNames);

        LOG.trace(
                "Created SecondaryIndexLookuper for main table {} via index table {}, lookup columns: {}, batch size: {}",
                mainTableInfo.getTablePath(),
                indexTableInfo.getTablePath(),
                lookupColumnNames,
                DEFAULT_MAIN_TABLE_LOOKUP_BATCH_SIZE);
    }

    /**
     * Creates a new ProjectedRow instance for extracting primary keys from index table rows. A new
     * instance is created for each lookup operation to ensure thread safety, as ProjectedRow has
     * mutable state (the underlying InternalRow reference).
     *
     * @return a new ProjectedRow instance configured with primary key indexes
     */
    private ProjectedRow createPrimaryKeyProjection() {
        return ProjectedRow.from(primaryKeyIndexes);
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        long startTime = System.currentTimeMillis();
        LOG.trace(
                "Starting secondary index lookup with key: {}",
                InternalRowUtils.toDebugString(lookupRowType, lookupKey));

        // Step 1: Query index table to get primary keys
        return indexTableLookuper
                .get()
                .lookup(lookupKey)
                .thenCompose(
                        indexResult ->
                                handleIndexTableLookupResult(lookupKey, indexResult, startTime))
                .exceptionally(throwable -> handleLookupException(throwable, lookupKey, startTime));
    }

    /**
     * Handle the result of index table lookup and proceed to query main table.
     *
     * @param indexResult the result from index table lookup
     * @param startTime the start time of the lookup operation
     * @return a CompletableFuture containing the final lookup result
     */
    private CompletableFuture<LookupResult> handleIndexTableLookupResult(
            InternalRow lookupKey, LookupResult indexResult, long startTime) {
        List<InternalRow> indexRows = indexResult.getRowList();
        LOG.trace("Index table lookup returned {} records", indexRows.size());
        if (indexRows.isEmpty()) {
            // No matching records found in index table
            LOG.trace("No matching records found in index table");
            return CompletableFuture.completedFuture(new LookupResult(new ArrayList<>()));
        }

        // Step 2: Extract primary keys from index table results and query main table
        LOG.debug(
                "Querying main table with {} primary keys extracted from index", indexRows.size());

        return lookupMainTableByPrimaryKeys(lookupKey, indexRows, startTime);
    }

    /**
     * Lookup main table using primary keys extracted from index table results. Uses batch
     * processing to control lookup pressure on main table.
     *
     * @param indexRows the rows from index table containing primary keys
     * @param startTime the start time of the lookup operation
     * @return a CompletableFuture containing the final lookup result
     */
    private CompletableFuture<LookupResult> lookupMainTableByPrimaryKeys(
            InternalRow lookupKey, List<InternalRow> indexRows, long startTime) {

        LOG.trace(
                "Processing {} index rows in batches of {}",
                indexRows.size(),
                DEFAULT_MAIN_TABLE_LOOKUP_BATCH_SIZE);

        // Split index rows into batches
        List<List<InternalRow>> batches =
                Lists.partition(indexRows, DEFAULT_MAIN_TABLE_LOOKUP_BATCH_SIZE);
        List<InternalRow> allResults = new ArrayList<>();

        // Process batches sequentially to control lookup pressure
        CompletableFuture<Void> batchProcessing = CompletableFuture.completedFuture(null);

        for (List<InternalRow> batch : batches) {
            batchProcessing =
                    batchProcessing.thenCompose(
                            v -> processMainTableLookupBatch(lookupKey, batch, allResults));
        }

        // After all batches are processed, return the final result
        return batchProcessing.thenApply(
                v -> {
                    long duration = System.currentTimeMillis() - startTime;
                    LOG.debug(
                            "Secondary index lookup completed: {} index records -> {} main table results in {} ms using {} batches",
                            indexRows.size(),
                            allResults.size(),
                            duration,
                            batches.size());

                    return new LookupResult(allResults);
                });
    }

    /**
     * Process a single batch of index rows and collect results.
     *
     * @param batch the batch of index rows to process
     * @param allResults the collection to add results to
     * @return a CompletableFuture that completes when the batch is processed
     */
    private CompletableFuture<Void> processMainTableLookupBatch(
            InternalRow lookupKey, List<InternalRow> batch, List<InternalRow> allResults) {
        List<CompletableFuture<LookupResult>> batchLookups = new ArrayList<>();

        // Create a new ProjectedRow for this batch to ensure thread safety.
        // ProjectedRow is lightweight (just an int[] and a reference), so creating
        // a new instance per batch has negligible overhead.
        ProjectedRow primaryKeyProjection = createPrimaryKeyProjection();

        for (InternalRow indexRow : batch) {
            // Extract primary key from index table row
            InternalRow primaryKey = primaryKeyProjection.replaceRow(indexRow);

            // Query main table using primary key with exception handling
            CompletableFuture<LookupResult> mainLookup =
                    mainTableLookuper
                            .get()
                            .lookup(primaryKey)
                            .exceptionally(
                                    throwable ->
                                            handleMainTableLookupException(
                                                    throwable, lookupKey, primaryKey));
            batchLookups.add(mainLookup);
        }

        // Wait for all lookups in this batch to complete
        return CompletableFuture.allOf(batchLookups.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            int nullResults = 0;
                            for (CompletableFuture<LookupResult> future : batchLookups) {
                                LookupResult lookupResult = future.join();
                                InternalRow row = lookupResult.getSingletonRow();
                                if (row != null) {
                                    synchronized (allResults) {
                                        allResults.add(row);
                                    }
                                } else {
                                    nullResults++;
                                }
                            }

                            if (nullResults > 0) {
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace(
                                            "Found {} null results in batch of size {}, indicating potential data inconsistency",
                                            nullResults,
                                            batch.size());
                                }
                            }

                            return null; // Void return type
                        });
    }

    /**
     * Handle exceptions that occur during main table lookup.
     *
     * @param throwable the exception that occurred
     * @param primaryKey the primary key being looked up
     * @return a LookupResult with null row for recoverable exceptions
     * @throws RuntimeException for unrecoverable exceptions
     */
    private LookupResult handleMainTableLookupException(
            Throwable throwable, InternalRow lookupKey, InternalRow primaryKey) {
        String errorMessage =
                String.format(
                        "Error happened during main table lookup for sk:%s, pk:%s",
                        InternalRowUtils.toDebugString(lookupRowType, lookupKey),
                        InternalRowUtils.toDebugString(primaryKeyRowType, primaryKey));
        LOG.error(errorMessage, throwable);
        if (ExceptionUtils.findThrowable(throwable, UnknownTableOrBucketException.class)
                .isPresent()) {
            return new LookupResult((InternalRow) null);
        } else if (ExceptionUtils.findThrowable(throwable, IllegalArgumentException.class)
                .isPresent()) {
            return new LookupResult((InternalRow) null);
        } else {
            throw new RuntimeException(errorMessage, throwable);
        }
    }

    /**
     * Handle exceptions that occur during the overall lookup operation.
     *
     * @param throwable the exception that occurred
     * @param lookupKey the original lookup key
     * @param startTime the start time of the lookup operation
     * @throws RuntimeException always throws after logging
     */
    private LookupResult handleLookupException(
            Throwable throwable, InternalRow lookupKey, long startTime) {
        LOG.error(
                "Secondary index lookup failed for key: {}",
                InternalRowUtils.toDebugString(lookupRowType, lookupKey),
                throwable);
        throw new RuntimeException("Secondary index lookup failed", throwable);
    }
}
