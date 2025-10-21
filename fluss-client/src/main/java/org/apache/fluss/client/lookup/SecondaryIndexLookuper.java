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
import org.apache.fluss.client.metrics.LookuperMetricGroup;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Lists;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.StringUtils;

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

    /** Metric group for reporting secondary index lookup metrics. */
    private final LookuperMetricGroup lookuperMetricGroup;

    /** Batch size for main table lookups to control lookup pressure. */
    private final int mainTableLookupBatchSize;

    private final RowType primaryKeyRowType;

    public SecondaryIndexLookuper(
            TableInfo mainTableInfo,
            TableInfo indexTableInfo,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            List<String> lookupColumnNames,
            LookuperMetricGroup lookuperMetricGroup,
            int mainTableLookupBatchSize) {

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
        this.lookuperMetricGroup = lookuperMetricGroup;
        this.mainTableLookupBatchSize = Math.max(1, mainTableLookupBatchSize);

        this.primaryKeyRowType = mainTableInfo.getRowType();

        LOG.trace(
                "Created SecondaryIndexLookuper for main table {} via index table {}, lookup columns: {}, batch size: {}",
                mainTableInfo.getTablePath(),
                indexTableInfo.getTablePath(),
                lookupColumnNames,
                this.mainTableLookupBatchSize);
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Starting secondary index lookup with key: {}", lookupKey);

        // Increment request count metric
        lookuperMetricGroup.secondaryIndexRequestCount().inc();

        // Step 1: Query index table to get primary keys
        return indexTableLookuper
                .lookup(lookupKey)
                .thenCompose(indexResult -> handleIndexTableLookupResult(indexResult, startTime))
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
            LookupResult indexResult, long startTime) {
        List<InternalRow> indexRows = indexResult.getRowList();
        LOG.trace("Index table lookup returned {} records", indexRows.size());
        lookuperMetricGroup.updateSecondaryIndexLookupPkMappingCount(indexRows.size());
        long duration = System.currentTimeMillis() - startTime;
        lookuperMetricGroup.updateSecondaryIndexLookupLatency(duration);
        if (indexRows.isEmpty()) {
            // No matching records found in index table
            LOG.trace("No matching records found in index table");
            return CompletableFuture.completedFuture(new LookupResult(new ArrayList<>()));
        }

        // Step 2: Extract primary keys from index table results and query main table
        LOG.debug(
                "Querying main table with {} primary keys extracted from index", indexRows.size());

        return lookupMainTableByPrimaryKeys(indexRows, startTime);
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
            List<InternalRow> indexRows, long startTime) {

        LOG.trace(
                "Processing {} index rows in batches of {}",
                indexRows.size(),
                mainTableLookupBatchSize);

        // Split index rows into batches
        List<List<InternalRow>> batches = Lists.partition(indexRows, mainTableLookupBatchSize);
        List<InternalRow> allResults = new ArrayList<>();

        // Process batches sequentially to control lookup pressure
        CompletableFuture<Void> batchProcessing = CompletableFuture.completedFuture(null);

        for (List<InternalRow> batch : batches) {
            batchProcessing =
                    batchProcessing.thenCompose(
                            v -> processMainTableLookupBatch(batch, allResults));
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

                    // Update latency metric
                    lookuperMetricGroup.updateSecondaryIndexLookupLatency(duration);

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
            List<InternalRow> batch, List<InternalRow> allResults) {
        List<CompletableFuture<LookupResult>> batchLookups = new ArrayList<>();

        for (InternalRow indexRow : batch) {
            // Extract primary key from index table row
            InternalRow primaryKey = primaryKeyProjection.replaceRow(indexRow);

            // Query main table using primary key with exception handling
            CompletableFuture<LookupResult> mainLookup =
                    mainTableLookuper
                            .lookup(primaryKey)
                            .exceptionally(
                                    throwable ->
                                            handleMainTableLookupException(throwable, primaryKey));
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
            Throwable throwable, InternalRow primaryKey) {
        // Unwrap the throwable to check the root cause
        Throwable cause = throwable.getCause();
        while (cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }

        // If it's UnknownTableOrBucketException, convert to empty result and log warning
        if (cause instanceof UnknownTableOrBucketException) {
            LOG.warn(
                    "Primary key lookup failed due to unknown table/bucket, possibly due to concurrent table operations. Key: {}, Error: {}",
                    primaryKey,
                    cause.getMessage());
            return new LookupResult((InternalRow) null);
        } else if (cause instanceof IllegalArgumentException) {
            LOG.error(
                    "Primary key lookup failed due to invalid arguments, possibly due to corrupt index record. PrimaryKey: {}, Error: {}",
                    StringUtils.internalRowDebugString(primaryKeyRowType, primaryKey),
                    cause);
            throw new RuntimeException(cause);
        } else {
            LOG.error(
                    "Unexpected error during main table lookup for primary key: {}",
                    primaryKey,
                    throwable);
            // For other exceptions, rethrow
            throw new RuntimeException(throwable);
        }
    }

    /**
     * Handle exceptions that occur during the overall lookup operation.
     *
     * @param throwable the exception that occurred
     * @param lookupKey the original lookup key
     * @param startTime the start time of the lookup operation
     * @throws RuntimeException always throws after logging and updating metrics
     */
    private LookupResult handleLookupException(
            Throwable throwable, InternalRow lookupKey, long startTime) {
        // Increment error count for any uncaught exceptions
        lookuperMetricGroup.secondaryIndexErrorCount().inc();
        long duration = System.currentTimeMillis() - startTime;
        lookuperMetricGroup.updateSecondaryIndexLookupLatency(duration);
        LOG.error("Secondary index lookup failed for key: {}", lookupKey, throwable);
        throw new RuntimeException("Secondary index lookup failed", throwable);
    }
}
