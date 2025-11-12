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

package org.apache.fluss.server.metadata;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Interface for providing metadata information about tables and partitions from various sources.
 * This provider supports retrieving metadata from both local cache and ZooKeeper, allowing for
 * flexible metadata access patterns with different consistency and performance characteristics.
 */
public interface MetadataProvider {

    /**
     * Retrieves table metadata from local cache.
     *
     * @param tablePath the path identifying the table
     * @return an Optional containing the table metadata if found in cache, empty otherwise
     */
    Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath);

    /**
     * Retrieves partition metadata from local cache.
     *
     * @param physicalTablePath the physical path identifying the table partition
     * @return an Optional containing the partition metadata if found in cache, empty otherwise
     */
    Optional<PartitionMetadata> getPartitionMetadataFromCache(PhysicalTablePath physicalTablePath);

    /**
     * Retrieves the physical table path from local cache using partition ID.
     *
     * @param partitionId the partition identifier
     * @return an Optional containing the physical table path if found in cache, empty otherwise
     */
    Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId);

    /**
     * Retrieves the table path from local cache using table ID.
     *
     * @param tableId the table identifier
     * @return an Optional containing the table path if found in cache, empty otherwise
     */
    Optional<TablePath> getTablePathFromCache(long tableId);

    /**
     * Retrieves a batch of table metadata from ZooKeeper.
     *
     * @param tablePaths the path identifying the table
     * @return a CompletableFuture containing the table metadata from ZooKeeper
     */
    List<TableMetadata> getTablesMetadataFromZK(Collection<TablePath> tablePaths);

    /**
     * Retrieves a batch of partition metadata from ZooKeeper.
     *
     * @param partitionPaths the physical path identifying the table partition
     * @return an Optional containing the partition metadata if found in cache, empty otherwise
     */
    List<PartitionMetadata> getPartitionsMetadataFromZK(
            Collection<PhysicalTablePath> partitionPaths);

    /**
     * Retrieves partition metadata from ZooKeeper by partition ID. This method is useful when the
     * PhysicalTablePath is not available in the cache, especially during partition bucket failover
     * scenarios.
     *
     * @param partitionId the partition identifier
     * @return a list containing the partition metadata if found in ZooKeeper, empty list otherwise
     */
    List<PartitionMetadata> getPartitionsMetadataFromZK(long partitionId);

    /**
     * Queries only the leader server ID for a given table bucket from local cache.
     *
     * @param tableBucket the table bucket to query
     * @return a BucketLeaderResult indicating whether the leader was found, the bucket doesn't
     *     exist, or the information is temporarily unavailable in cache
     */
    BucketLeaderResult getBucketLeaderIdFromCache(TableBucket tableBucket);

    /**
     * Queries only the leader server ID for a given table bucket directly from ZooKeeper.
     *
     * @param tableBucket the table bucket to query
     * @return a BucketLeaderResult indicating whether the leader was found, the bucket doesn't
     *     exist, or there was an error querying ZooKeeper
     */
    BucketLeaderResult getBucketLeaderIdFromZK(TableBucket tableBucket);

    /**
     * Result of querying a bucket's leader information. This type distinguishes between three
     * distinct states:
     *
     * <ul>
     *   <li><b>FOUND</b>: Leader was successfully found (leaderId is valid, may be -1 if no leader
     *       elected yet)
     *   <li><b>NOT_FOUND</b>: The bucket doesn't exist in the metadata store (permanent failure -
     *       table/partition was deleted)
     *   <li><b>UNAVAILABLE</b>: Leader information is temporarily unavailable (cache miss or
     *       transient ZooKeeper error)
     * </ul>
     */
    class BucketLeaderResult {
        private final Status status;
        private final int leaderId;
        @Nullable private final String reason;

        private BucketLeaderResult(Status status, int leaderId, @Nullable String reason) {
            this.status = status;
            this.leaderId = leaderId;
            this.reason = reason;
        }

        /** Create a FOUND result with the given leader ID. */
        public static BucketLeaderResult found(int leaderId) {
            return new BucketLeaderResult(Status.FOUND, leaderId, null);
        }

        /** Create a NOT_FOUND result indicating the bucket doesn't exist. */
        public static BucketLeaderResult notFound(String reason) {
            return new BucketLeaderResult(Status.NOT_FOUND, -1, reason);
        }

        /** Create an UNAVAILABLE result indicating temporary unavailability. */
        public static BucketLeaderResult unavailable(String reason) {
            return new BucketLeaderResult(Status.UNAVAILABLE, -1, reason);
        }

        public Status getStatus() {
            return status;
        }

        public boolean isFound() {
            return status == Status.FOUND;
        }

        public boolean isNotFound() {
            return status == Status.NOT_FOUND;
        }

        public boolean isUnavailable() {
            return status == Status.UNAVAILABLE;
        }

        /**
         * Get the leader ID. Only valid when status is FOUND.
         *
         * @return the leader server ID, or -1 if no leader is currently elected
         */
        public int getLeaderId() {
            if (status != Status.FOUND) {
                throw new IllegalStateException(
                        "Cannot get leaderId when status is " + status + ". Reason: " + reason);
            }
            return leaderId;
        }

        /**
         * Get the reason for NOT_FOUND or UNAVAILABLE status.
         *
         * @return the reason string, or null for FOUND status
         */
        @Nullable
        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            if (status == Status.FOUND) {
                return "BucketLeaderResult{status=FOUND, leaderId=" + leaderId + "}";
            } else {
                return "BucketLeaderResult{status=" + status + ", reason='" + reason + "'}";
            }
        }

        /** Status of the bucket leader query. */
        public enum Status {
            /** Leader information was successfully retrieved. */
            FOUND,
            /** The bucket does not exist (table/partition was deleted). */
            NOT_FOUND,
            /** Leader information is temporarily unavailable (cache miss or transient error). */
            UNAVAILABLE
        }
    }
}
