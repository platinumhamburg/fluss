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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.entity.DatabasePropertyChanges;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.utils.TableDescriptorValidation;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableDeletion;
import org.apache.fluss.server.zk.data.TableMetadataRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.function.RunnableWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.server.utils.TableDescriptorValidation.validateAlterTableProperties;
import static org.apache.fluss.utils.concurrent.Executors.directExecutor;

/** A manager for metadata. */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);
    private static final int PARTITION_TOMBSTONE_CAS_RETRY_LIMIT = 3;
    private static final long TABLE_DELETION_RETRY_DELAY_MILLIS = 1000L;

    private final ZooKeeperClient zookeeperClient;
    private final int maxPartitionNum;
    private final int maxBucketNum;
    private final LakeCatalogDynamicLoader lakeCatalogDynamicLoader;
    private final Executor tableDeletionExecutor;
    private final Consumer<Runnable> tableDeletionRetryScheduler;
    // Coordinator epochs fence different leaders. This lock serializes read-modify-write metadata
    // mutations issued by the current leader, including automatic partition management and
    // watcher repairs, so a deleted path cannot be recreated between validation and commit.
    private final Object metadataMutationLock = new Object();

    public static final Set<String> SENSITIVE_TABLE_OPTIONS = new HashSet<>();

    static {
        SENSITIVE_TABLE_OPTIONS.add("password");
        SENSITIVE_TABLE_OPTIONS.add("secret");
        SENSITIVE_TABLE_OPTIONS.add("key");
    }

    /**
     * Creates a new metadata manager.
     *
     * @param zookeeperClient the zookeeper client
     * @param conf the cluster configuration
     */
    public MetadataManager(
            ZooKeeperClient zookeeperClient,
            Configuration conf,
            LakeCatalogDynamicLoader lakeCatalogDynamicLoader) {
        this(zookeeperClient, conf, lakeCatalogDynamicLoader, directExecutor());
    }

    MetadataManager(
            ZooKeeperClient zookeeperClient,
            Configuration conf,
            LakeCatalogDynamicLoader lakeCatalogDynamicLoader,
            Executor tableDeletionExecutor) {
        this(
                zookeeperClient,
                conf,
                lakeCatalogDynamicLoader,
                tableDeletionExecutor,
                runnable ->
                        FutureUtils.runAfterDelay(
                                runnable,
                                TABLE_DELETION_RETRY_DELAY_MILLIS,
                                TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    MetadataManager(
            ZooKeeperClient zookeeperClient,
            Configuration conf,
            LakeCatalogDynamicLoader lakeCatalogDynamicLoader,
            Executor tableDeletionExecutor,
            Consumer<Runnable> tableDeletionRetryScheduler) {
        this.zookeeperClient = zookeeperClient;
        this.maxPartitionNum = conf.get(ConfigOptions.MAX_PARTITION_NUM);
        this.maxBucketNum = conf.get(ConfigOptions.MAX_BUCKET_NUM);
        this.lakeCatalogDynamicLoader = lakeCatalogDynamicLoader;
        this.tableDeletionExecutor = tableDeletionExecutor;
        this.tableDeletionRetryScheduler = tableDeletionRetryScheduler;
    }

    /** Validates the table descriptor. */
    public void validateTableDescriptor(TableDescriptor tableDescriptor) {
        TableDescriptorValidation.validateTableDescriptor(
                tableDescriptor,
                maxBucketNum,
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat());
    }

    public void createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        synchronized (metadataMutationLock) {
            createDatabaseInternal(databaseName, databaseDescriptor, ignoreIfExists);
        }
    }

    private void createDatabaseInternal(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(
                    "Database " + databaseName + " already exists.");
        }

        DatabaseRegistration databaseRegistration = DatabaseRegistration.of(databaseDescriptor);
        try {
            zookeeperClient.registerDatabase(databaseName, databaseRegistration);
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                if (ignoreIfExists) {
                    return;
                }
                throw new DatabaseAlreadyExistException(
                        "Database " + databaseName + " already exists.");
            } else {
                throw new FlussRuntimeException("Failed to create database: " + databaseName, e);
            }
        }
    }

    public void alterDatabaseProperties(
            String databaseName,
            DatabasePropertyChanges databasePropertyChanges,
            boolean ignoreIfNotExists) {
        synchronized (metadataMutationLock) {
            alterDatabasePropertiesInternal(
                    databaseName, databasePropertyChanges, ignoreIfNotExists);
        }
    }

    private void alterDatabasePropertiesInternal(
            String databaseName,
            DatabasePropertyChanges databasePropertyChanges,
            boolean ignoreIfNotExists) {
        try {
            // Check if database exists
            if (!databaseExists(databaseName)) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new DatabaseNotExistException("Database " + databaseName + " not exists.");
            }

            DatabaseRegistration databaseRegistration = getDatabaseRegistration(databaseName);
            DatabaseDescriptor currentDescriptor = databaseRegistration.toDatabaseDescriptor();

            // Create updated descriptor
            DatabaseDescriptor newDescriptor =
                    getUpdatedDatabaseDescriptor(currentDescriptor, databasePropertyChanges);

            if (newDescriptor != null) {
                // Update the database in ZooKeeper
                DatabaseRegistration updatedRegistration =
                        databaseRegistration.newProperties(newDescriptor);
                zookeeperClient.updateDatabase(databaseName, updatedRegistration);
                LOG.info("Successfully altered database properties for database: {}", databaseName);
            } else {
                LOG.info(
                        "No properties changed when alter database {}, skip update.", databaseName);
            }
        } catch (Exception e) {
            if (e instanceof DatabaseNotExistException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw (DatabaseNotExistException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlussRuntimeException("Failed to alter database: " + databaseName, e);
            }
        }
    }

    @Nullable
    private DatabaseDescriptor getUpdatedDatabaseDescriptor(
            DatabaseDescriptor currentDescriptor, DatabasePropertyChanges changes) {
        Map<String, String> newCustomProperties =
                new HashMap<>(currentDescriptor.getCustomProperties());
        // set properties
        newCustomProperties.putAll(changes.customPropertiesToSet);
        // reset properties
        newCustomProperties.keySet().removeAll(changes.customPropertiesToReset);

        if (newCustomProperties.equals(currentDescriptor.getCustomProperties())
                && changes.commentToSet == null) {
            return null;
        }

        String newComment;
        if (changes.commentToSet != null) {
            // If comment is set to empty string, it means to reset the comment
            if (changes.commentToSet.isEmpty()) {
                newComment = null;
            } else {
                newComment = changes.commentToSet;
            }
        } else {
            newComment = currentDescriptor.getComment().orElse(null);
        }

        return DatabaseDescriptor.builder()
                .customProperties(newCustomProperties)
                .comment(newComment)
                .build();
    }

    public DatabaseInfo getDatabase(String databaseName) throws DatabaseNotExistException {
        DatabaseRegistration databaseReg = getDatabaseRegistration(databaseName);
        return new DatabaseInfo(
                databaseName,
                databaseReg.toDatabaseDescriptor(),
                databaseReg.createdTime,
                databaseReg.modifiedTime);
    }

    public DatabaseRegistration getDatabaseRegistration(String databaseName) {
        Optional<DatabaseRegistration> optionalDB;
        try {
            optionalDB = zookeeperClient.getDatabase(databaseName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Fail to get database '%s'.", databaseName), e);
        }

        if (!optionalDB.isPresent()) {
            throw new DatabaseNotExistException("Database '" + databaseName + "' does not exist.");
        }
        return optionalDB.get();
    }

    public boolean databaseExists(String databaseName) {
        return uncheck(
                () -> zookeeperClient.databaseExists(databaseName),
                "Fail to check database exists or not");
    }

    public List<String> listDatabases() {
        return uncheck(zookeeperClient::listDatabases, "Fail to list database");
    }

    public List<DatabaseSummary> listDatabaseSummaries(Collection<String> databaseNames) {
        return uncheck(
                () -> zookeeperClient.listDatabaseSummaries(databaseNames),
                "Fail to get database summaries for " + databaseNames);
    }

    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException("Database " + databaseName + " does not exist.");
        }
        return uncheck(
                () -> zookeeperClient.listTables(databaseName),
                "Fail to list tables for database:" + databaseName);
    }

    /**
     * List the partitions of the given table.
     *
     * @return a map from partition name to partition registration.
     */
    public Map<String, PartitionRegistration> listPartitions(TablePath tablePath)
            throws TableNotExistException, TableNotPartitionedException {
        return listPartitions(tablePath, null);
    }

    /**
     * List the partitions of the given table and partitionSpec.
     *
     * @return a map from partition name to partition registration.
     */
    public Map<String, PartitionRegistration> listPartitions(
            TablePath tablePath, ResolvedPartitionSpec partitionFilter)
            throws TableNotExistException, TableNotPartitionedException, InvalidPartitionException {
        TableInfo tableInfo = getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        try {
            if (partitionFilter == null) {
                return zookeeperClient.getPartitionRegistrations(tablePath);
            } else {

                return zookeeperClient.getPartitionRegistrations(
                        tablePath, tableInfo.getPartitionKeys(), partitionFilter);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to list partitions for table: %s, partitionSpec: %s.",
                            tablePath, partitionFilter),
                    e);
        }
    }

    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        synchronized (metadataMutationLock) {
            dropDatabaseInternal(name, ignoreIfNotExists, cascade);
        }
    }

    private void dropDatabaseInternal(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException("Database " + name + " does not exist.");
        }
        if (!cascade && !listTables(name).isEmpty()) {
            throw new DatabaseNotEmptyException("Database " + name + " is not empty.");
        }

        uncheck(() -> zookeeperClient.deleteDatabase(name), "Fail to drop database: " + name);
    }

    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        synchronized (metadataMutationLock) {
            dropTableInternal(tablePath, ignoreIfNotExists);
        }
    }

    private void dropTableInternal(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }

        // in here, we just delete the table node in zookeeper, which will then trigger
        // the physical deletion in tablet servers and assignments in zk
        uncheck(() -> zookeeperClient.deleteTable(tablePath), "Fail to drop table: " + tablePath);
    }

    public void deleteTables(List<TableDeletion> tableDeletions, int coordinatorZkVersion) {
        List<TableDeletion> deletions = new ArrayList<>(tableDeletions);
        synchronized (metadataMutationLock) {
            uncheck(
                    () -> zookeeperClient.markTablesForDeletion(deletions, coordinatorZkVersion),
                    "Fail to mark tables for deletion: " + deletions);
        }
        for (int i = 0; i < deletions.size(); i++) {
            TableDeletion tableDeletion = deletions.get(i);
            try {
                zookeeperClient.completeTableDeletion(
                        tableDeletion.getTablePath(), coordinatorZkVersion);
            } catch (Exception e) {
                List<TableDeletion> remaining =
                        new ArrayList<>(deletions.subList(i, deletions.size()));
                if (isRetryableTableDeletionFailure(e)) {
                    scheduleTableDeletionRetry(remaining, coordinatorZkVersion);
                }
                throw new FlussRuntimeException(
                        "Fail to complete table deletion: " + tableDeletion, e);
            }
        }
    }

    private void scheduleTableDeletionRetry(
            List<TableDeletion> tableDeletions, int coordinatorZkVersion) {
        tableDeletionRetryScheduler.accept(
                () -> {
                    try {
                        tableDeletionExecutor.execute(
                                () -> retryTableDeletions(tableDeletions, coordinatorZkVersion));
                    } catch (RejectedExecutionException e) {
                        LOG.debug(
                                "Table deletion retry was discarded because the Coordinator is stopping: {}.",
                                tableDeletions,
                                e);
                    }
                });
    }

    private void retryTableDeletions(List<TableDeletion> tableDeletions, int coordinatorZkVersion) {
        for (int i = 0; i < tableDeletions.size(); i++) {
            TableDeletion tableDeletion = tableDeletions.get(i);
            try {
                zookeeperClient.completeTableDeletion(
                        tableDeletion.getTablePath(), coordinatorZkVersion);
            } catch (Exception e) {
                if (!isRetryableTableDeletionFailure(e)) {
                    LOG.warn(
                            "Stopping table deletion retry for {} after a non-retryable failure.",
                            tableDeletions,
                            e);
                    return;
                }
                List<TableDeletion> remaining =
                        new ArrayList<>(tableDeletions.subList(i, tableDeletions.size()));
                LOG.warn(
                        "Failed to complete marked table deletion {}; retrying remaining deletions {}.",
                        tableDeletion,
                        remaining,
                        e);
                scheduleTableDeletionRetry(remaining, coordinatorZkVersion);
                return;
            }
        }
    }

    private static boolean isCoordinatorEpochConflict(Throwable throwable) {
        return ExceptionUtils.findThrowable(throwable, KeeperException.BadVersionException.class)
                .isPresent();
    }

    private static boolean isRetryableTableDeletionFailure(Throwable throwable) {
        return !isCoordinatorEpochConflict(throwable)
                && !ExceptionUtils.findThrowable(throwable, IllegalStateException.class).isPresent()
                && !ExceptionUtils.findThrowable(throwable, IllegalArgumentException.class)
                        .isPresent();
    }

    public void resumeTableDeletions(int coordinatorZkVersion) {
        uncheck(
                () -> zookeeperClient.resumeTableDeletions(coordinatorZkVersion),
                "Fail to resume incomplete table deletions");
    }

    public void completeDeleteTable(long tableId) {
        // final step for delete a table.
        // delete bucket assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this table are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deleteTableAssignment(tableId),
                String.format("Delete tablet assignment meta fail for table %s.", tableId));
    }

    public void completeDeletePartition(long partitionId) {
        // final step for delete a partition.
        // delete partition assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this partition are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deletePartitionAssignment(partitionId),
                String.format("Delete tablet assignment meta fail for partition %s.", partitionId));
    }

    /**
     * Creates the necessary metadata of the given table in zookeeper and return the table id.
     * Returns -1 if the table already exists and ignoreIfExists is true.
     *
     * @param tablePath the table path
     * @param tableToCreate the table descriptor describing the table to create
     * @param tableAssignment the table assignment, will be null when the table is partitioned table
     * @param ignoreIfExists whether to ignore if the table already exists
     * @return the table id
     */
    public long createTable(
            TablePath tablePath,
            TableDescriptor tableToCreate,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        synchronized (metadataMutationLock) {
            return createTableInternal(tablePath, tableToCreate, tableAssignment, ignoreIfExists);
        }
    }

    private long createTableInternal(
            TablePath tablePath,
            TableDescriptor tableToCreate,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(
                    "Database " + tablePath.getDatabaseName() + " does not exist.");
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return -1;
            } else {
                throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
            }
        }

        // register schema to zk
        // first register a schema to the zk, if then register the table
        // to zk fails, there's no harm to register a new schema to zk again
        try {
            zookeeperClient.registerFirstSchema(tablePath, tableToCreate.getSchema());
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Fail to register schema when creating table " + tablePath, e);
        }

        // register the table, we have registered the schema whose path have contained the node for
        // the table, then we won't need to create the node to store the table
        return uncheck(
                () -> {
                    // generate a table id
                    long tableId = zookeeperClient.getTableIdAndIncrement();
                    if (tableAssignment != null) {
                        // register table assignment
                        zookeeperClient.registerTableAssignment(tableId, tableAssignment);
                    }
                    // register the table
                    zookeeperClient.registerTable(
                            tablePath,
                            TableRegistration.newTable(
                                    tableId,
                                    zookeeperClient.getDefaultRemoteDataDir(),
                                    tableToCreate),
                            false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }

    public long allocateTableId() {
        return uncheck(zookeeperClient::getTableIdAndIncrement, "Fail to allocate table id");
    }

    /** Persists a group of fully prepared tables in one epoch-fenced ZooKeeper transaction. */
    void createTablesAtomically(
            List<TableCreation> tableCreations,
            boolean ignoreIfFirstTableExists,
            int coordinatorZkVersion) {
        synchronized (metadataMutationLock) {
            createTablesAtomicallyInternal(
                    tableCreations, ignoreIfFirstTableExists, coordinatorZkVersion);
        }
    }

    private void createTablesAtomicallyInternal(
            List<TableCreation> tableCreations,
            boolean ignoreIfFirstTableExists,
            int coordinatorZkVersion) {
        if (tableCreations.isEmpty()) {
            throw new IllegalArgumentException("At least one table is required.");
        }

        TablePath firstTablePath = tableCreations.get(0).getTablePath();
        if (!databaseExists(firstTablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(
                    "Database " + firstTablePath.getDatabaseName() + " does not exist.");
        }
        if (tableExists(firstTablePath)) {
            if (ignoreIfFirstTableExists) {
                return;
            }
            throw new TableAlreadyExistException("Table " + firstTablePath + " already exists.");
        }
        for (int i = 1; i < tableCreations.size(); i++) {
            TablePath tablePath = tableCreations.get(i).getTablePath();
            if (!databaseExists(tablePath.getDatabaseName())) {
                throw new DatabaseNotExistException(
                        "Database " + tablePath.getDatabaseName() + " does not exist.");
            }
            if (tableExists(tablePath)) {
                throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
            }
        }

        List<TableMetadataRegistration> registrations =
                tableCreations.stream()
                        .map(
                                tableCreation ->
                                        new TableMetadataRegistration(
                                                tableCreation.getTablePath(),
                                                TableRegistration.newTable(
                                                        tableCreation.getTableId(),
                                                        zookeeperClient.getDefaultRemoteDataDir(),
                                                        tableCreation.getTableDescriptor()),
                                                tableCreation.getTableDescriptor().getSchema(),
                                                tableCreation.getTableAssignment()))
                        .collect(Collectors.toList());
        try {
            zookeeperClient.registerTablesAtomically(registrations, coordinatorZkVersion);
        } catch (KeeperException.NodeExistsException e) {
            throw new TableAlreadyExistException(
                    "One of the tables in the atomic creation already exists.", e);
        } catch (Exception e) {
            throw new FlussRuntimeException("Fail to atomically create tables.", e);
        }
    }

    public void alterTableSchema(
            TablePath tablePath,
            List<TableChange> schemaChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal,
            int coordinatorZkVersion)
            throws TableNotExistException, TableNotPartitionedException {
        synchronized (metadataMutationLock) {
            alterTableSchemaInternal(
                    tablePath,
                    schemaChanges,
                    ignoreIfNotExists,
                    flussPrincipal,
                    coordinatorZkVersion);
        }
    }

    private void alterTableSchemaInternal(
            TablePath tablePath,
            List<TableChange> schemaChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal,
            int coordinatorZkVersion)
            throws TableNotExistException, TableNotPartitionedException {
        try {

            TableInfo table = getTable(tablePath);
            TableDescriptor tableDescriptor = table.toTableDescriptor();

            // validate the table column changes
            if (!schemaChanges.isEmpty()) {
                validateIndexedTableSchemaEvolution(table);
                Schema newSchema =
                        SchemaUpdate.applySchemaChanges(table.getSchema(), schemaChanges);
                LakeCatalog.Context lakeCatalogContext =
                        new CoordinatorService.DefaultLakeCatalogContext(
                                false,
                                flussPrincipal,
                                tableDescriptor,
                                TableDescriptor.builder(tableDescriptor).schema(newSchema).build());
                // Lake First: sync to Lake before updating Fluss schema
                syncSchemaChangesToLake(tablePath, table, schemaChanges, lakeCatalogContext);

                // Update Fluss schema (ZK) after Lake sync succeeds
                if (!newSchema.equals(table.getSchema())) {
                    zookeeperClient.registerSchema(
                            tablePath,
                            table.getTableId(),
                            newSchema,
                            table.getSchemaId() + 1,
                            coordinatorZkVersion);
                } else {
                    LOG.info(
                            "Skipping schema evolution for table {} because the column(s) to add {} already exist.",
                            tablePath,
                            schemaChanges);
                }
            }
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw (TableNotExistException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlussRuntimeException("Failed to alter table schema: " + tablePath, e);
            }
        }
    }

    private static void validateIndexedTableSchemaEvolution(TableInfo table) {
        if (table.isIndexTable()) {
            throw new InvalidAlterTableException(
                    "Schema evolution is not supported for internal secondary index tables.");
        }
        if (!table.getSchema().getIndexes().isEmpty()) {
            throw new InvalidAlterTableException(
                    "Schema evolution is not supported for tables with secondary indexes.");
        }
    }

    private void syncSchemaChangesToLake(
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableChange> schemaChanges,
            LakeCatalog.Context lakeCatalogContext) {
        if (!isDataLakeEnabled(tableInfo.toTableDescriptor())) {
            return;
        }

        LakeCatalog lakeCatalog =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeCatalog();
        if (lakeCatalog == null) {
            throw new InvalidAlterTableException(
                    "Cannot alter schema for datalake enabled table "
                            + tablePath
                            + ", because the Fluss cluster doesn't enable datalake tables.");
        }

        try {
            lakeCatalog.alterTable(tablePath, schemaChanges, lakeCatalogContext);
        } catch (TableNotExistException e) {
            throw new FlussRuntimeException(
                    "Lake table doesn't exist for lake-enabled table "
                            + tablePath
                            + ", which shouldn't happen. Please check if the lake table was deleted manually.",
                    e);
        }
    }

    public void alterTableProperties(
            TablePath tablePath,
            List<TableChange> tableChanges,
            TablePropertyChanges tablePropertyChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal,
            int coordinatorZkVersion) {
        synchronized (metadataMutationLock) {
            alterTablePropertiesInternal(
                    tablePath,
                    tableChanges,
                    tablePropertyChanges,
                    ignoreIfNotExists,
                    flussPrincipal,
                    coordinatorZkVersion);
        }
    }

    private void alterTablePropertiesInternal(
            TablePath tablePath,
            List<TableChange> tableChanges,
            TablePropertyChanges tablePropertyChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal,
            int coordinatorZkVersion) {
        try {
            // it throws TableNotExistException if the table or database not exists
            TableRegistration tableReg = getTableRegistration(tablePath);
            SchemaInfo schemaInfo = getLatestSchema(tablePath);
            // we can't use MetadataManager#getTable here, because it will add the default
            // lake options to the table properties, which may cause the validation failure
            TableInfo tableInfo = tableReg.toTableInfo(tablePath, schemaInfo);

            // validate the changes
            validateAlterTableProperties(tableInfo, tablePropertyChanges.tableKeysToChange());

            TableDescriptor tableDescriptor = tableInfo.toTableDescriptor();
            TableDescriptor newDescriptor =
                    getUpdatedTableDescriptor(tableDescriptor, tablePropertyChanges);

            if (newDescriptor != null) {
                // is to enable datalake for the table
                if (isDataLakeEnabled(newDescriptor) && !isDataLakeEnabled(tableDescriptor)) {
                    // The table was created before cluster-level datalake was enabled.
                    // Backfill `table.datalake.format` before enabling datalake on the table
                    // so the updated table metadata stays consistent with the cluster setting.
                    if (!tableInfo.getTableConfig().getDataLakeFormat().isPresent()) {
                        DataLakeFormat dataLakeFormat =
                                lakeCatalogDynamicLoader
                                        .getLakeCatalogContainer()
                                        .getDataLakeFormat();
                        if (dataLakeFormat == null) {
                            throw new InvalidAlterTableException(
                                    "Cannot alter table "
                                            + tablePath
                                            + " in data lake, because the Fluss cluster doesn't enable datalake tables.");
                        }
                        newDescriptor = newDescriptor.withDataLakeFormat(dataLakeFormat);
                    }
                }

                // reuse the same validate logic with the createTable() method
                validateTableDescriptor(newDescriptor);
                // pre alter table properties, e.g. create lake table in lake storage if it's to
                // enable datalake for the table
                preAlterTableProperties(
                        tablePath, tableDescriptor, newDescriptor, tableChanges, flussPrincipal);
                // update the table to zk
                TableRegistration updatedTableRegistration =
                        tableReg.newProperties(
                                newDescriptor.getProperties(), newDescriptor.getCustomProperties());
                zookeeperClient.updateTable(
                        tablePath,
                        tableReg.tableId,
                        updatedTableRegistration,
                        coordinatorZkVersion);
            } else {
                LOG.info(
                        "No properties changed when alter table {}, skip update table.", tablePath);
            }
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw (TableNotExistException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlussRuntimeException(
                        "Failed to alter table properties: " + tablePath, e);
            }
        }
    }

    private void preAlterTableProperties(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            TableDescriptor newDescriptor,
            List<TableChange> tableChanges,
            FlussPrincipal flussPrincipal) {
        LakeCatalog.Context lakeCatalogContext =
                new CoordinatorService.DefaultLakeCatalogContext(
                        false, flussPrincipal, tableDescriptor, newDescriptor);
        LakeCatalog lakeCatalog =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeCatalog();

        if (isDataLakeEnabled(newDescriptor)) {
            if (lakeCatalog == null) {
                throw new InvalidAlterTableException(
                        "Cannot alter table "
                                + tablePath
                                + " in data lake, because the Fluss cluster doesn't enable datalake tables.");
            }

            // to enable lake table
            if (!isDataLakeEnabled(tableDescriptor)) {
                // before create table in fluss, we may create in lake
                try {
                    lakeCatalog.createTable(tablePath, newDescriptor, lakeCatalogContext);
                } catch (TableAlreadyExistException e) {
                    throw new LakeTableAlreadyExistException(e.getMessage(), e);
                }
            }
        }

        // We should always alter lake table even though datalake is disabled.
        // Otherwise, if user alter the fluss table when datalake is disabled, then enable datalake
        // again, the lake table will mismatch.
        // Only sync to lake if this table has ever opted into datalake (key present regardless of
        // value).
        if (lakeCatalog != null
                && tableDescriptor
                        .getProperties()
                        .containsKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key())) {
            try {
                lakeCatalog.alterTable(tablePath, tableChanges, lakeCatalogContext);
            } catch (TableNotExistException e) {
                // only throw TableNotExistException if datalake is enabled
                if (isDataLakeEnabled(newDescriptor)) {
                    throw new FlussRuntimeException(
                            "Lake table doesn't exist for lake-enabled table "
                                    + tablePath
                                    + ", which shouldn't be happened. Please check if the lake table was deleted manually.",
                            e);
                }
            }
        }
    }

    /**
     * Get a new TableDescriptor with updated properties.
     *
     * @param tableDescriptor the current table descriptor.
     * @param tablePropertyChanges the changes for the table properties
     * @return the updated TableDescriptor, or null if no properties updated.
     */
    private @Nullable TableDescriptor getUpdatedTableDescriptor(
            TableDescriptor tableDescriptor, TablePropertyChanges tablePropertyChanges) {
        Map<String, String> newProperties = new HashMap<>(tableDescriptor.getProperties());
        Map<String, String> newCustomProperties =
                new HashMap<>(tableDescriptor.getCustomProperties());

        // set properties
        newProperties.putAll(tablePropertyChanges.tablePropertiesToSet);
        newCustomProperties.putAll(tablePropertyChanges.customPropertiesToSet);

        // reset properties
        for (String key : tablePropertyChanges.tablePropertiesToReset) {
            newProperties.remove(key);
        }

        for (String key : tablePropertyChanges.customPropertiesToReset) {
            newCustomProperties.remove(key);
        }

        // no properties change happen
        if (newProperties.equals(tableDescriptor.getProperties())
                && newCustomProperties.equals(tableDescriptor.getCustomProperties())) {
            return null;
        } else {
            return tableDescriptor.withProperties(newProperties, newCustomProperties);
        }
    }

    private boolean isDataLakeEnabled(TableDescriptor tableDescriptor) {
        String dataLakeEnabledValue =
                tableDescriptor.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        return Boolean.parseBoolean(dataLakeEnabledValue);
    }

    public void removeSensitiveTableOptions(Map<String, String> tableLakeOptions) {
        if (tableLakeOptions == null || tableLakeOptions.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<String, String>> iterator = tableLakeOptions.entrySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next().getKey().toLowerCase();
            if (SENSITIVE_TABLE_OPTIONS.stream().anyMatch(key::contains)) {
                iterator.remove();
            }
        }
    }

    public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get table '%s'.", tablePath), e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        TableRegistration tableReg = optionalTable.get();
        SchemaInfo schemaInfo = getLatestSchema(tablePath);
        Map<String, String> defaultTableLakeOptions =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getDefaultTableLakeOptions();
        // Create a copy to avoid ConcurrentModificationException when multiple threads
        // call getTable() concurrently, as defaultTableLakeOptions is a shared instance
        Map<String, String> tableLakeOptions =
                defaultTableLakeOptions != null ? new HashMap<>(defaultTableLakeOptions) : null;
        removeSensitiveTableOptions(tableLakeOptions);
        return tableReg.toTableInfo(tablePath, schemaInfo, tableLakeOptions);
    }

    public Map<TablePath, TableInfo> getTables(Collection<TablePath> tablePaths)
            throws TableNotExistException {
        Map<TablePath, TableInfo> result = new HashMap<>();
        try {
            Map<TablePath, TableRegistration> tablePath2TableRegistrations =
                    zookeeperClient.getTables(tablePaths);
            // currently, we don't support schema evolution, so all schemas are version 1
            Map<TablePath, SchemaInfo> tablePath2SchemaInfos =
                    zookeeperClient.getLatestSchemas(tablePaths);
            for (TablePath tablePath : tablePaths) {
                if (!tablePath2TableRegistrations.containsKey(tablePath)) {
                    throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
                }
                if (!tablePath2SchemaInfos.containsKey(tablePath)) {
                    throw new SchemaNotExistException(
                            "Schema for '" + tablePath + "' does not exist.");
                }
                TableRegistration tableReg = tablePath2TableRegistrations.get(tablePath);
                SchemaInfo schemaInfo = tablePath2SchemaInfos.get(tablePath);

                result.put(
                        tablePath,
                        tableReg.toTableInfo(
                                tablePath,
                                schemaInfo,
                                lakeCatalogDynamicLoader
                                        .getLakeCatalogContainer()
                                        .getDefaultTableLakeOptions()));
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get tables '%s'.", tablePaths), e);
        }
        return result;
    }

    public TableRegistration getTableRegistration(TablePath tablePath) {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        return optionalTable.get();
    }

    public SchemaInfo getLatestSchema(TablePath tablePath) throws SchemaNotExistException {
        final int currentSchemaId;
        try {
            currentSchemaId = zookeeperClient.getCurrentSchemaId(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to get latest schema id of table " + tablePath, e);
        }
        return getSchemaById(tablePath, currentSchemaId);
    }

    public SchemaInfo getSchemaById(TablePath tablePath, int schemaId)
            throws SchemaNotExistException {
        Optional<SchemaInfo> optionalSchema;
        try {
            optionalSchema = zookeeperClient.getSchemaById(tablePath, schemaId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Fail to get schema of %s for table %s", schemaId, tablePath), e);
        }
        if (optionalSchema.isPresent()) {
            return optionalSchema.get();
        } else {
            throw new SchemaNotExistException(
                    "Schema for table "
                            + tablePath
                            + " with schema id "
                            + schemaId
                            + " does not exist.");
        }
    }

    public boolean tableExists(TablePath tablePath) {
        // check the path of the table exists
        return uncheck(
                () -> zookeeperClient.tableExist(tablePath),
                String.format("Fail to check the table %s exist or not.", tablePath));
    }

    public long initWriterId() {
        return uncheck(
                zookeeperClient::getWriterIdAndIncrement, "Fail to get writer id from zookeeper");
    }

    public Set<String> getPartitions(TablePath tablePath) {
        return uncheck(
                () -> zookeeperClient.getPartitions(tablePath),
                "Fail to get partitions from zookeeper for table " + tablePath);
    }

    public void createPartition(
            TablePath tablePath,
            long tableId,
            PartitionAssignment partitionAssignment,
            ResolvedPartitionSpec partition,
            boolean ignoreIfExists,
            int coordinatorZkVersion) {
        synchronized (metadataMutationLock) {
            createPartitionInternal(
                    tablePath,
                    tableId,
                    partitionAssignment,
                    partition,
                    ignoreIfExists,
                    coordinatorZkVersion);
        }
    }

    private void createPartitionInternal(
            TablePath tablePath,
            long tableId,
            PartitionAssignment partitionAssignment,
            ResolvedPartitionSpec partition,
            boolean ignoreIfExists,
            int coordinatorZkVersion) {
        String partitionName = partition.getPartitionName();
        Optional<PartitionRegistration> optionalPartitionRegistration =
                getOptionalPartitionRegistration(tablePath, partitionName);
        if (optionalPartitionRegistration.isPresent()) {
            if (ignoreIfExists) {
                return;
            }
            throw new PartitionAlreadyExistsException(
                    String.format(
                            "Partition '%s' already exists for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        final int partitionNumber;
        try {
            partitionNumber = zookeeperClient.getPartitionNumber(tablePath);
            if (partitionNumber + 1 > maxPartitionNum) {
                throw new TooManyPartitionsException(
                        String.format(
                                "Exceed the maximum number of partitions for table %s, only allow %s partitions.",
                                tablePath, maxPartitionNum));
            }
        } catch (TooManyPartitionsException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Get the number of partition from zookeeper failed for table %s",
                            tablePath),
                    e);
        }

        try {
            int bucketCount = partitionAssignment.getBucketAssignments().size();
            // currently, every partition has the same bucket count
            int totalBuckets = bucketCount * (partitionNumber + 1);
            if (totalBuckets > maxBucketNum) {
                throw new TooManyBucketsException(
                        String.format(
                                "Adding partition '%s' would result in %d total buckets for table %s, exceeding the maximum of %d buckets.",
                                partition.getPartitionName(),
                                totalBuckets,
                                tablePath,
                                maxBucketNum));
            }
        } catch (TooManyBucketsException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to check total bucket count for table %s", tablePath), e);
        }

        try {
            long partitionId = zookeeperClient.getPartitionIdAndIncrement();
            validatePartitionIdAboveTombstoneFloor(tablePath, partitionId);
            // register partition assignments and partition metadata to zk in transaction
            zookeeperClient.registerPartitionAssignmentAndMetadata(
                    partitionId,
                    partitionName,
                    partitionAssignment,
                    zookeeperClient.getDefaultRemoteDataDir(),
                    tablePath,
                    tableId,
                    coordinatorZkVersion);
            LOG.info(
                    "Register partition {} to zookeeper for table [{}].", partitionName, tablePath);
        } catch (KeeperException.NodeExistsException nodeExistsException) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(
                        String.format(
                                "Partition '%s' already exists for table %s",
                                partition.getPartitionQualifiedName(), tablePath));
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Register partition to zookeeper failed to create partition %s for table [%s]",
                            partitionName, tablePath),
                    e);
        }
    }

    private void validatePartitionIdAboveTombstoneFloor(TablePath tablePath, long partitionId) {
        try {
            PartitionTombstone tombstone = zookeeperClient.getPartitionTombstone(tablePath);
            PartitionTombstoneAdvancer.validateNewPartitionId(tombstone, partitionId);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to validate partition id against tombstone floor for table "
                            + tablePath,
                    e);
        }
    }

    public void dropPartition(
            TablePath tablePath,
            long expectedTableId,
            ResolvedPartitionSpec partition,
            boolean ignoreIfNotExists,
            int coordinatorZkVersion) {
        synchronized (metadataMutationLock) {
            dropPartitionInternal(
                    tablePath, expectedTableId, partition, ignoreIfNotExists, coordinatorZkVersion);
        }
    }

    private void dropPartitionInternal(
            TablePath tablePath,
            long expectedTableId,
            ResolvedPartitionSpec partition,
            boolean ignoreIfNotExists,
            int coordinatorZkVersion) {
        String partitionName = partition.getPartitionName();
        Optional<PartitionRegistration> optionalPartitionRegistration =
                getOptionalPartitionRegistration(tablePath, partitionName);
        if (!optionalPartitionRegistration.isPresent()) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(
                    String.format(
                            "Partition '%s' does not exist for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        try {
            if (hasSecondaryIndexes(tablePath)) {
                dropPartitionAndPersistTombstone(
                        tablePath,
                        expectedTableId,
                        partitionName,
                        optionalPartitionRegistration.get(),
                        coordinatorZkVersion);
            } else {
                zookeeperClient.deletePartition(
                        tablePath,
                        partitionName,
                        expectedTableId,
                        optionalPartitionRegistration.get().getPartitionId(),
                        coordinatorZkVersion);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to delete partition '%s' from zookeeper for table %s.",
                            partitionName, tablePath),
                    e);
        }
    }

    private boolean hasSecondaryIndexes(TablePath tablePath) {
        return !getLatestSchema(tablePath).getSchema().getIndexes().isEmpty();
    }

    public PartitionTombstone advancePartitionTombstone(
            TablePath tablePath,
            long expectedTableId,
            long partitionId,
            Collection<Long> alivePartitionIdsAfterDrop,
            int coordinatorZkVersion)
            throws Exception {
        synchronized (metadataMutationLock) {
            return PartitionTombstoneAdvancer.advanceAndPersist(
                    zookeeperClient,
                    tablePath,
                    expectedTableId,
                    partitionId,
                    alivePartitionIdsAfterDrop,
                    coordinatorZkVersion);
        }
    }

    private void dropPartitionAndPersistTombstone(
            TablePath tablePath,
            long expectedTableId,
            String partitionName,
            PartitionRegistration partitionRegistration,
            int coordinatorZkVersion)
            throws Exception {
        long partitionId = partitionRegistration.getPartitionId();
        Exception lastConflict = null;
        for (int attempt = 0; attempt < PARTITION_TOMBSTONE_CAS_RETRY_LIMIT; attempt++) {
            Tuple2<PartitionTombstone, Optional<Integer>> current =
                    zookeeperClient.getPartitionTombstoneWithVersion(tablePath);
            Set<Long> alivePartitionIdsAfterDrop =
                    loadAlivePartitionIdsAfterDrop(tablePath, partitionId);
            PartitionTombstone updated =
                    PartitionTombstoneAdvancer.dropPartition(
                            current.f0, partitionId, alivePartitionIdsAfterDrop);
            try {
                zookeeperClient.deletePartitionAndSetTombstone(
                        tablePath,
                        partitionName,
                        expectedTableId,
                        partitionId,
                        updated,
                        current.f1,
                        coordinatorZkVersion);
                return;
            } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
                lastConflict = e;
                LOG.warn(
                        "Retrying atomic partition drop for table {} partition {} after tombstone version conflict.",
                        tablePath,
                        partitionName,
                        e);
            }
        }
        throw new FlussRuntimeException(
                String.format(
                        "Failed to atomically drop partition '%s' for table %s after %s retries.",
                        partitionName, tablePath, PARTITION_TOMBSTONE_CAS_RETRY_LIMIT),
                lastConflict);
    }

    @Nullable
    private Set<Long> loadAlivePartitionIdsAfterDrop(TablePath tablePath, long droppedPartitionId) {
        try {
            Set<Long> alivePartitionIds = new HashSet<>();
            for (PartitionRegistration registration :
                    zookeeperClient.getPartitionRegistrations(tablePath).values()) {
                long partitionId = registration.getPartitionId();
                if (partitionId != droppedPartitionId) {
                    alivePartitionIds.add(partitionId);
                }
            }
            return alivePartitionIds;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to load alive partition ids for table {} while dropping partition {}, "
                            + "falling back to conservative tombstone advancement.",
                    tablePath,
                    droppedPartitionId,
                    e);
            return null;
        }
    }

    private Optional<PartitionRegistration> getOptionalPartitionRegistration(
            TablePath tablePath, String partitionName) {
        try {
            return zookeeperClient.getPartition(tablePath, partitionName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to get partition '%s' of table %s from zookeeper.",
                            tablePath, partitionName),
                    e);
        }
    }

    private void rethrowIfIsNotNoNodeException(
            ThrowingRunnable<Exception> throwingRunnable, String exceptionMessage) {
        try {
            throwingRunnable.run();
        } catch (KeeperException.NoNodeException e) {
            // ignore
        } catch (Exception e) {
            throw new FlussRuntimeException(exceptionMessage, e);
        }
    }

    private static <T> T uncheck(Callable<T> callable, String errorMsg) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }

    private static void uncheck(RunnableWithException runnable, String errorMsg) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }
}
