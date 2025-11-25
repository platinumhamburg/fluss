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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;

/**
 * Helper class that handles all index table related operations including creation, deletion of
 * index tables.
 */
public class IndexTableHelper {

    private static final Logger LOG = LoggerFactory.getLogger(IndexTableHelper.class);

    private final MetadataManager metadataManager;
    private final ServerMetadataCache metadataCache;
    private final int defaultBucketNumber;
    private final int indexAutoCreateMaxRetryAttempts;

    public IndexTableHelper(
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache,
            Configuration conf) {
        this.metadataManager = metadataManager;
        this.metadataCache = metadataCache;
        this.defaultBucketNumber = conf.get(ConfigOptions.TABLE_INDEX_BUCKET_NUM);
        this.indexAutoCreateMaxRetryAttempts =
                conf.get(ConfigOptions.INDEX_AUTO_CREATE_MAX_RETRY_ATTEMPTS);
    }

    /**
     * Creates index tables for the given main table.
     *
     * @param mainTablePath the path of the main table
     * @param mainTableDescriptor the descriptor of the main table
     */
    public void createIndexTables(TablePath mainTablePath, TableDescriptor mainTableDescriptor) {
        Schema mainSchema = mainTableDescriptor.getSchema();
        List<Schema.Index> indexes = mainSchema.getIndexes();

        Set<TablePath> createdIndexTables = new HashSet<>();
        try {
            for (Schema.Index index : indexes) {
                TablePath indexTablePath =
                        IndexTableUtils.generateIndexTablePath(mainTablePath, index.getIndexName());
                TableDescriptor indexTableDescriptor =
                        IndexTableUtils.createIndexTableDescriptor(
                                mainTableDescriptor, index, defaultBucketNumber);

                int retryAttempts = 0;
                Exception lastException = null;
                boolean created = false;

                while (retryAttempts < indexAutoCreateMaxRetryAttempts && !created) {
                    try {
                        long indexTableId =
                                createSingleIndexTable(indexTablePath, indexTableDescriptor);
                        // Only track as created if it was actually created (not skipped)
                        if (indexTableId != -1) {
                            createdIndexTables.add(indexTablePath);
                        }
                        created = true;
                    } catch (Exception e) {
                        lastException = e;
                        retryAttempts++;
                        if (retryAttempts < indexAutoCreateMaxRetryAttempts) {
                            try {
                                // exponential backoff
                                Thread.sleep(100L * retryAttempts);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(
                                        "Interrupted while retrying index table creation", ie);
                            }
                        }
                    }
                }

                if (!created) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to create index table %s after %d attempts",
                                    indexTablePath, indexAutoCreateMaxRetryAttempts),
                            lastException);
                }
            }
        } catch (Exception e) {
            // rollback created index tables
            for (TablePath indexTablePath : createdIndexTables) {
                try {
                    metadataManager.dropTable(indexTablePath, true);
                } catch (Exception rollbackException) {
                    // log rollback failure but continue with other rollbacks
                    LOG.error(
                            "Failed to rollback index table creation for table: {}",
                            indexTablePath,
                            rollbackException);
                }
            }
            throw e;
        }
    }

    /**
     * Creates a single index table.
     *
     * @param indexTablePath the path of the index table
     * @param indexTableDescriptor the descriptor of the index table
     * @return the table id, or -1 if the table already exists and was skipped
     */
    private long createSingleIndexTable(
            TablePath indexTablePath, TableDescriptor indexTableDescriptor) {
        int bucketCount = indexTableDescriptor.getTableDistribution().get().getBucketCount().get();
        int replicaFactor = indexTableDescriptor.getReplicationFactor();
        TabletServerInfo[] servers = metadataCache.getLiveServers();
        TableAssignment indexTableAssignment =
                generateAssignment(bucketCount, replicaFactor, servers);

        // create the index table with ignoreIfExists=true to avoid failure when index table
        // already exists (mark as index table to allow __offset system column)
        long tableId =
                metadataManager.createTable(
                        indexTablePath, indexTableDescriptor, indexTableAssignment, true, true);

        if (tableId == -1) {
            LOG.info(
                    "Index table {} already exists, skipping creation for index {}",
                    indexTablePath,
                    indexTableDescriptor.getSchema().getIndexes().get(0).getIndexName());
        }

        return tableId;
    }

    /**
     * Drops all index tables for the given main table.
     *
     * @param mainTablePath the path of the main table
     * @param ignoreIfNotExists whether to ignore if the table doesn't exist
     */
    public void dropIndexTables(TablePath mainTablePath, boolean ignoreIfNotExists) {
        try {
            // get the main table information to check if it has indexes
            TableInfo tableInfo = metadataManager.getTable(mainTablePath);
            if (tableInfo == null) {
                if (!ignoreIfNotExists) {
                    throw new InvalidTableException(
                            String.format("Table %s does not exist", mainTablePath));
                }
                return;
            }

            TableDescriptor mainTableDescriptor = tableInfo.toTableDescriptor();
            Schema mainSchema = mainTableDescriptor.getSchema();
            List<Schema.Index> indexes = mainSchema.getIndexes();

            // if no indexes, nothing to drop
            if (indexes.isEmpty()) {
                return;
            }

            // drop each index table
            for (Schema.Index index : indexes) {
                TablePath indexTablePath =
                        IndexTableUtils.generateIndexTablePath(mainTablePath, index.getIndexName());
                try {
                    metadataManager.dropTable(indexTablePath, ignoreIfNotExists);
                } catch (Exception e) {
                    if (!ignoreIfNotExists) {
                        throw new RuntimeException(
                                String.format("Failed to drop index table %s", indexTablePath), e);
                    }
                    // if ignoreIfNotExists is true, we continue with other index tables
                }
            }
        } catch (InvalidTableException e) {
            if (!ignoreIfNotExists) {
                throw e;
            }
            // if ignoreIfNotExists is true and table doesn't exist, it's fine
        }
    }
}
