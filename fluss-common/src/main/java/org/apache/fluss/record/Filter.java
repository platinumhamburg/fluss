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

package org.apache.fluss.record;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.shaded.guava32.com.google.common.cache.Cache;
import org.apache.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;
import org.apache.fluss.utils.SchemaUtil;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Schema-aware filter that supports statistics-based filtering with schema evolution support.
 *
 * <p>This filter applies predicates to batch statistics to determine whether entire batches can be
 * filtered out based on their statistical properties. It supports filtering across different schema
 * versions by using {@link SchemaGetter} to dynamically obtain schemas and {@link
 * SchemaUtil#getIndexMapping} for column mapping between different schema versions.
 *
 * <h3>Schema Evolution Support</h3>
 *
 * <p>When the filter's schema ID matches the statistics' schema ID, filtering is performed directly
 * using field indexes. When schemas differ, the filter uses {@link
 * PredicateBuilder#transformFieldMapping} to create an adapted predicate with remapped field
 * indexes:
 *
 * <ul>
 *   <li>If all predicate columns can be mapped to the statistics schema, the predicate is adapted
 *       with remapped field indexes and cached for future use
 *   <li>If any predicate column doesn't exist in the statistics schema (e.g., new column added),
 *       the batch is included without filtering (safe fallback)
 * </ul>
 */
@Internal
public class Filter {

    private final Predicate predicate;
    private final int schemaId;
    @Nullable private final SchemaGetter schemaGetter;

    /**
     * Cache for adapted predicates per statistics schema ID. Uses Optional to allow caching null
     * results (schemas that cannot be adapted). Uses Guava Cache with expiration to prevent memory
     * leaks.
     */
    private final Cache<Integer, Optional<Predicate>> adaptedPredicateCache;

    /**
     * Creates a schema-aware filter without schema evolution support.
     *
     * <p>This constructor creates a filter that only works with statistics of the same schema. When
     * statistics have a different schema ID, filtering will be skipped (returns true).
     *
     * @param predicate the underlying predicate to apply
     * @param schemaId the schema ID associated with the predicate
     */
    public Filter(Predicate predicate, int schemaId) {
        this(predicate, schemaId, null);
    }

    /**
     * Creates a schema-aware filter with schema evolution support.
     *
     * <p>The schema associated with the predicate will be dynamically retrieved from the
     * schemaGetter using the provided schemaId.
     *
     * @param predicate the underlying predicate to apply
     * @param schemaId the schema ID associated with the predicate
     * @param schemaGetter the schema getter for retrieving schemas by ID (nullable)
     */
    public Filter(Predicate predicate, int schemaId, @Nullable SchemaGetter schemaGetter) {
        this.predicate = predicate;
        this.schemaId = schemaId;
        this.schemaGetter = schemaGetter;
        // Use bounded cache with expiration to prevent memory leaks
        this.adaptedPredicateCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
    }

    /**
     * Test method that accepts LogRecordBatchStatistics directly. This method provides the main
     * schema-aware filtering logic with schema evolution support.
     *
     * <p>When the statistics schema matches the filter's schema, filtering is performed directly.
     * When schemas differ, the filter uses {@link PredicateBuilder#transformFieldMapping} to create
     * an adapted predicate using column ID mapping from SchemaUtil.
     *
     * @param rowCount the number of rows in the batch
     * @param statistics the schema-aware statistics
     * @return true to include the batch, false to filter it out
     */
    public boolean test(long rowCount, LogRecordBatchStatistics statistics) {
        if (statistics == null) {
            // No statistics available, cannot filter
            return true;
        }

        // Fast path: schema ID matches
        int statisticsSchemaId = statistics.getSchemaId();
        if (statisticsSchemaId == schemaId) {
            return predicate.test(
                    rowCount,
                    statistics.getMinValues(),
                    statistics.getMaxValues(),
                    statistics.getNullCounts());
        }

        // Schema evolution path: get or create adapted predicate
        Predicate adaptedPredicate = getOrCreateAdaptedPredicate(statisticsSchemaId);
        if (adaptedPredicate == null) {
            // Cannot adapt predicate (e.g., required column doesn't exist in statistics schema)
            return true;
        }

        // Apply the adapted predicate
        return adaptedPredicate.test(
                rowCount,
                statistics.getMinValues(),
                statistics.getMaxValues(),
                statistics.getNullCounts());
    }

    /**
     * Get or create an adapted predicate for the given statistics schema ID using Guava Cache's
     * get() method with a loader function.
     *
     * @param statisticsSchemaId the schema ID of the statistics
     * @return the adapted predicate, or null if adaptation is not possible
     */
    @Nullable
    private Predicate getOrCreateAdaptedPredicate(int statisticsSchemaId) {
        try {
            return adaptedPredicateCache
                    .get(
                            statisticsSchemaId,
                            () -> Optional.ofNullable(createAdaptedPredicate(statisticsSchemaId)))
                    .orElse(null);
        } catch (ExecutionException e) {
            // Should not happen as createAdaptedPredicate doesn't throw checked exceptions
            return null;
        }
    }

    /**
     * Create an adapted predicate for the given statistics schema ID.
     *
     * @param statisticsSchemaId the schema ID of the statistics
     * @return the adapted predicate, or null if adaptation is not possible
     */
    @Nullable
    private Predicate createAdaptedPredicate(int statisticsSchemaId) {
        if (schemaGetter == null) {
            // No schema getter, cannot perform schema evolution
            return null;
        }

        // Get schemas dynamically from SchemaGetter
        Schema filterSchema;
        Schema statsSchema;
        try {
            filterSchema = schemaGetter.getSchema(schemaId);
            statsSchema = schemaGetter.getSchema(statisticsSchemaId);
        } catch (Exception e) {
            // Cannot get schema, skip filtering for safety
            return null;
        }

        if (filterSchema == null || statsSchema == null) {
            return null;
        }

        // Compute the index mapping from filter schema to statistics schema
        // indexMapping[filterIdx] = statsIdx
        int[] indexMapping = SchemaUtil.getIndexMapping(statsSchema, filterSchema);

        // Use PredicateBuilder's transformFieldMapping to create the adapted predicate
        return PredicateBuilder.transformFieldMapping(predicate, indexMapping).orElse(null);
    }

    /**
     * Get the underlying predicate.
     *
     * @return the underlying predicate
     */
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * Get the expected schema ID.
     *
     * @return the expected schema ID
     */
    public int getSchemaId() {
        return schemaId;
    }

    /**
     * Create a schema-aware wrapper for an existing predicate with schema evolution support.
     *
     * @param predicate the predicate to wrap
     * @param schemaId the schema ID associated with the predicate
     * @param schemaGetter the schema getter for retrieving schemas by ID
     * @return schema-aware predicate wrapper
     */
    public static Filter wrap(Predicate predicate, int schemaId, SchemaGetter schemaGetter) {
        if (predicate instanceof Filter) {
            return (Filter) predicate;
        }
        return new Filter(predicate, schemaId, schemaGetter);
    }

    @Override
    public String toString() {
        return String.format("Filter{schemaId=%d, predicate=%s}", schemaId, predicate);
    }
}
