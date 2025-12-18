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
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Lists;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
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

    /**
     * ThreadLocal storage for PrimaryKeyLookuper instances. Each thread gets its own instance to
     * ensure thread safety during async callback execution.
     */
    private final ThreadLocal<PrimaryKeyLookuper> mainTableLookuperThreadLocal;

    /**
     * ThreadLocal storage for ProjectedRow instances. Each thread gets its own instance to ensure
     * thread safety during async callback execution. ProjectedRow has mutable state (the underlying
     * InternalRow), so we need to create separate instances per thread.
     */
    private final ThreadLocal<ProjectedRow> primaryKeyProjectionThreadLocal;

    /** Parameters needed to create lookuper and projection instances. */
    private final TableInfo mainTableInfo;

    private final SchemaGetter mainTableSchemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;

    /** Metric group for reporting secondary index lookup metrics. */
    private final LookuperMetricGroup lookuperMetricGroup;

    /** Batch size for main table lookups to control lookup pressure. */
    private final int mainTableLookupBatchSize;

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
            List<String> lookupColumnNames,
            LookuperMetricGroup lookuperMetricGroup,
            int mainTableLookupBatchSize) {

        // Store parameters for creating lookuper and projection instances
        this.mainTableInfo = mainTableInfo;
        this.mainTableSchemaGetter = mainTableSchemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;

        // Create index table lookuper to query index table using secondary index keys
        // Use indexTableSchemaGetter for index table to ensure correct schema resolution
        this.indexTableLookuper =
                new PrefixKeyLookuper(
                        indexTableInfo,
                        indexTableSchemaGetter,
                        metadataUpdater,
                        lookupClient,
                        lookupColumnNames);

        // Initialize ThreadLocal for main table lookuper to ensure thread safety
        // Use mainTableSchemaGetter for main table to ensure correct schema resolution
        this.mainTableLookuperThreadLocal =
                ThreadLocal.withInitial(
                        () -> {
                            LOG.trace(
                                    "Creating PrimaryKeyLookuper instance for thread: {}",
                                    Thread.currentThread().getName());
                            return new PrimaryKeyLookuper(
                                    this.mainTableInfo,
                                    this.mainTableSchemaGetter,
                                    this.metadataUpdater,
                                    this.lookupClient);
                        });

        // Calculate primary key indexes for creating ProjectedRow instances
        List<String> mainTablePrimaryKeys = mainTableInfo.getPrimaryKeys();
        this.primaryKeyIndexes = new int[mainTablePrimaryKeys.size()];
        List<String> indexTableColumns = indexTableInfo.getRowType().getFieldNames();

        for (int i = 0; i < mainTablePrimaryKeys.size(); i++) {
            this.primaryKeyIndexes[i] = indexTableColumns.indexOf(mainTablePrimaryKeys.get(i));
        }

        // Initialize ThreadLocal for ProjectedRow to ensure thread safety
        // ProjectedRow has mutable state, so each thread needs its own instance
        this.primaryKeyProjectionThreadLocal =
                ThreadLocal.withInitial(
                        () -> {
                            LOG.trace(
                                    "Creating ProjectedRow instance for thread: {}",
                                    Thread.currentThread().getName());
                            return ProjectedRow.from(primaryKeyIndexes);
                        });

        this.lookuperMetricGroup = lookuperMetricGroup;
        this.mainTableLookupBatchSize = Math.max(1, mainTableLookupBatchSize);

        this.primaryKeyRowType = mainTableInfo.getRowType().project(mainTablePrimaryKeys);
        this.lookupRowType = indexTableInfo.getRowType().project(lookupColumnNames);

        LOG.trace(
                "Created SecondaryIndexLookuper for main table {} via index table {}, lookup columns: {}, batch size: {}",
                mainTableInfo.getTablePath(),
                indexTableInfo.getTablePath(),
                lookupColumnNames,
                this.mainTableLookupBatchSize);
    }

    /**
     * Get the PrimaryKeyLookuper instance for the current thread. Creates a new instance if one
     * doesn't exist for this thread.
     *
     * @return the PrimaryKeyLookuper instance for the current thread
     */
    private PrimaryKeyLookuper getMainTableLookuper() {
        return mainTableLookuperThreadLocal.get();
    }

    /**
     * Get the ProjectedRow instance for the current thread. Creates a new instance if one doesn't
     * exist for this thread.
     *
     * @return the ProjectedRow instance for the current thread
     */
    private ProjectedRow getPrimaryKeyProjection() {
        return primaryKeyProjectionThreadLocal.get();
    }

    /**
     * Close this lookuper and release all resources. This method should be called when the lookuper
     * is no longer needed to prevent memory leaks from ThreadLocal storage.
     */
    @Override
    public void close() {
        LOG.trace("Closing SecondaryIndexLookuper and cleaning up ThreadLocal resources");
        mainTableLookuperThreadLocal.remove();
        primaryKeyProjectionThreadLocal.remove();
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        long startTime = System.currentTimeMillis();
        LOG.trace(
                "Starting secondary index lookup with key: {}",
                StringUtils.internalRowDebugString(lookupRowType, lookupKey));

        // Increment request count metric
        lookuperMetricGroup.secondaryIndexRequestCount().inc();

        // Step 1: Query index table to get primary keys
        return indexTableLookuper
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
                mainTableLookupBatchSize);

        // Split index rows into batches
        List<List<InternalRow>> batches = Lists.partition(indexRows, mainTableLookupBatchSize);
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
            InternalRow lookupKey, List<InternalRow> batch, List<InternalRow> allResults) {
        List<CompletableFuture<LookupResult>> batchLookups = new ArrayList<>();

        for (InternalRow indexRow : batch) {
            // Extract primary key from index table row
            // Use thread-local projection instance for thread safety
            InternalRow primaryKey = getPrimaryKeyProjection().replaceRow(indexRow);

            // Query main table using primary key with exception handling
            // Use thread-local lookuper instance for thread safety
            CompletableFuture<LookupResult> mainLookup =
                    getMainTableLookuper()
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
                                LOG.warn(
                                        "Found {} null results in batch of size {}, indicating potential data inconsistency, lookup key: {}",
                                        nullResults,
                                        batch.size(),
                                        StringUtils.internalRowDebugString(
                                                lookupRowType, lookupKey));
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
                        StringUtils.internalRowDebugString(lookupRowType, lookupKey),
                        StringUtils.internalRowDebugString(primaryKeyRowType, primaryKey));

        // Handle recoverable exceptions by returning empty result
        if (ExceptionUtils.findThrowable(throwable, PartitionNotExistException.class).isPresent()) {
            // This can happen when main table partition has been deleted (TTL)
            // but index table still has references due to different retention mechanisms
            LOG.warn(
                    "Partition not found during main table lookup for sk:{}, pk:{}. "
                            + "This may indicate index-main table retention mismatch.",
                    StringUtils.internalRowDebugString(lookupRowType, lookupKey),
                    StringUtils.internalRowDebugString(primaryKeyRowType, primaryKey));
            return new LookupResult((InternalRow) null);
        } else {
            LOG.error(errorMessage, throwable);
            throw new RuntimeException(errorMessage, throwable);
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
        LOG.error(
                "Secondary index lookup failed for key: {}",
                StringUtils.internalRowDebugString(lookupRowType, lookupKey),
                throwable);
        throw new RuntimeException("Secondary index lookup failed", throwable);
    }
}
