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
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.function.RunnableWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.fluss.server.utils.TableDescriptorValidation.validateAlterTableProperties;
import static org.apache.fluss.server.utils.TableDescriptorValidation.validateTableDescriptor;

/** A manager for metadata. */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    private final ZooKeeperClient zookeeperClient;
    private final int maxPartitionNum;
    private final int maxBucketNum;
    private final LakeCatalogDynamicLoader lakeCatalogDynamicLoader;

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
        this.zookeeperClient = zookeeperClient;
        this.maxPartitionNum = conf.get(ConfigOptions.MAX_PARTITION_NUM);
        this.maxBucketNum = conf.get(ConfigOptions.MAX_BUCKET_NUM);
        this.lakeCatalogDynamicLoader = lakeCatalogDynamicLoader;
    }

    public void createDatabase(
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

    public DatabaseInfo getDatabase(String databaseName) throws DatabaseNotExistException {

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

        DatabaseRegistration databaseReg = optionalDB.get();
        return new DatabaseInfo(
                databaseName,
                databaseReg.toDatabaseDescriptor(),
                databaseReg.createdTime,
                databaseReg.modifiedTime);
    }

    public boolean databaseExists(String databaseName) {
        return uncheck(
                () -> zookeeperClient.databaseExists(databaseName),
                "Fail to check database exists or not");
    }

    public List<String> listDatabases() {
        return uncheck(zookeeperClient::listDatabases, "Fail to list database");
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
     * <p>Return a map from partition name to partition id.
     */
    public Map<String, Long> listPartitions(TablePath tablePath)
            throws TableNotExistException, TableNotPartitionedException {
        return listPartitions(tablePath, null);
    }

    /**
     * List the partitions of the given table and partitionSpec.
     *
     * <p>Return a map from partition name to partition id.
     */
    public Map<String, Long> listPartitions(
            TablePath tablePath, ResolvedPartitionSpec partitionFilter)
            throws TableNotExistException, TableNotPartitionedException, InvalidPartitionException {
        TableInfo tableInfo = getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        try {
            if (partitionFilter == null) {
                return zookeeperClient.getPartitionNameAndIds(tablePath);
            } else {

                return zookeeperClient.getPartitionNameAndIds(
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
        return createTable(tablePath, tableToCreate, tableAssignment, ignoreIfExists, false);
    }

    /**
     * Creates the necessary metadata of the given table in zookeeper and return the table id.
     * Returns -1 if the table already exists and ignoreIfExists is true.
     *
     * @param tablePath the table path
     * @param tableToCreate the table descriptor describing the table to create
     * @param tableAssignment the table assignment, will be null when the table is partitioned table
     * @param ignoreIfExists whether to ignore if the table already exists
     * @param isIndexTable whether this is an index table (index tables are allowed to use __offset
     *     system column)
     * @return the table id
     */
    public long createTable(
            TablePath tablePath,
            TableDescriptor tableToCreate,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists,
            boolean isIndexTable)
            throws TableAlreadyExistException, DatabaseNotExistException {
        // validate table properties before creating table
        validateTableDescriptor(tableToCreate, maxBucketNum, isIndexTable);

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
            zookeeperClient.registerSchema(tablePath, tableToCreate.getSchema());
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
                            tablePath, TableRegistration.newTable(tableId, tableToCreate), false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }

    public void alterTableProperties(
            TablePath tablePath,
            List<TableChange> tableChanges,
            TablePropertyChanges tablePropertyChanges,
            boolean ignoreIfNotExists,
            @Nullable LakeCatalog lakeCatalog,
            @Nullable DataLakeFormat dataLakeFormat,
            LakeTableTieringManager lakeTableTieringManager) {
        try {
            // it throws TableNotExistException if the table or database not exists
            TableRegistration tableReg = getTableRegistration(tablePath);
            SchemaInfo schemaInfo = getLatestSchema(tablePath);
            // we can't use MetadataManager#getTable here, because it will add the default
            // lake options to the table properties, which may cause the validation failure
            TableInfo tableInfo = tableReg.toTableInfo(tablePath, schemaInfo);

            // validate the changes
            validateAlterTableProperties(
                    tableInfo,
                    tablePropertyChanges.tableKeysToChange(),
                    tablePropertyChanges.customKeysToChange());

            TableDescriptor tableDescriptor = tableInfo.toTableDescriptor();
            TableDescriptor newDescriptor =
                    getUpdatedTableDescriptor(tableDescriptor, tablePropertyChanges);

            if (newDescriptor != null) {
                // reuse the same validate logic with the createTable() method
                validateTableDescriptor(newDescriptor, maxBucketNum);

                // pre alter table properties, e.g. create lake table in lake storage if it's to
                // enable datalake for the table
                preAlterTableProperties(
                        tablePath,
                        tableDescriptor,
                        newDescriptor,
                        tableChanges,
                        lakeCatalog,
                        dataLakeFormat);
                // update the table to zk
                TableRegistration updatedTableRegistration =
                        tableReg.newProperties(
                                newDescriptor.getProperties(), newDescriptor.getCustomProperties());
                zookeeperClient.updateTable(tablePath, updatedTableRegistration);

                // post alter table properties, e.g. add the table to lake table tiering manager if
                // it's to enable datalake for the table
                postAlterTableProperties(
                        tablePath,
                        schemaInfo,
                        tableDescriptor,
                        updatedTableRegistration,
                        lakeTableTieringManager);
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
                throw new FlussRuntimeException("Failed to alter table: " + tablePath, e);
            }
        }
    }

    private void preAlterTableProperties(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            TableDescriptor newDescriptor,
            List<TableChange> tableChanges,
            LakeCatalog lakeCatalog,
            DataLakeFormat dataLakeFormat) {
        if (isDataLakeEnabled(newDescriptor)) {
            if (lakeCatalog == null) {
                throw new InvalidAlterTableException(
                        "Cannot alter table "
                                + tablePath
                                + " in data lake, because the Fluss cluster doesn't enable datalake tables.");
            }

            boolean isLakeTableNewlyCreated = false;
            // to enable lake table
            if (!isDataLakeEnabled(tableDescriptor)) {
                // before create table in fluss, we may create in lake
                try {
                    lakeCatalog.createTable(tablePath, newDescriptor);
                    // no need to alter lake table if it is newly created
                    isLakeTableNewlyCreated = true;
                } catch (TableAlreadyExistException e) {
                    // TODO: should tolerate if the lake exist but matches our schema. This ensures
                    // eventually consistent by idempotently creating the table multiple times. See
                    // #846
                    throw new LakeTableAlreadyExistException(
                            String.format(
                                    "The table %s already exists in %s catalog, please "
                                            + "first drop the table in %s catalog or use a new table name.",
                                    tablePath, dataLakeFormat, dataLakeFormat));
                }
            }

            // only need to alter lake table if it is not newly created
            if (!isLakeTableNewlyCreated) {
                {
                    try {
                        lakeCatalog.alterTable(tablePath, tableChanges);
                    } catch (TableNotExistException e) {
                        throw new FlussRuntimeException(
                                "Lake table doesn't exists for lake-enabled table "
                                        + tablePath
                                        + ", which shouldn't be happened. Please check if the lake table was deleted manually.",
                                e);
                    }
                }
            }
        }
    }

    private void postAlterTableProperties(
            TablePath tablePath,
            SchemaInfo schemaInfo,
            TableDescriptor oldTableDescriptor,
            TableRegistration newTableRegistration,
            LakeTableTieringManager lakeTableTieringManager) {

        boolean toEnableDataLake =
                !isDataLakeEnabled(oldTableDescriptor)
                        && isDataLakeEnabled(newTableRegistration.properties);
        boolean toDisableDataLake =
                isDataLakeEnabled(oldTableDescriptor)
                        && !isDataLakeEnabled(newTableRegistration.properties);

        if (toEnableDataLake) {
            TableInfo newTableInfo = newTableRegistration.toTableInfo(tablePath, schemaInfo);
            // if the table is lake table, we need to add it to lake table tiering manager
            lakeTableTieringManager.addNewLakeTable(newTableInfo);
        } else if (toDisableDataLake) {
            lakeTableTieringManager.removeLakeTable(newTableRegistration.tableId);
        }
        // more post-alter actions can be added here
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

    private boolean isDataLakeEnabled(Map<String, String> properties) {
        String dataLakeEnabledValue = properties.get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        return Boolean.parseBoolean(dataLakeEnabledValue);
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
        return tableReg.toTableInfo(
                tablePath,
                schemaInfo,
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getDefaultTableLakeOptions());
    }

    public Map<TablePath, TableInfo> getTables(Collection<TablePath> tablePaths)
            throws TableNotExistException {
        Map<TablePath, TableInfo> result = new HashMap<>();
        try {
            Map<TablePath, TableRegistration> tablePath2TableRegistrations =
                    zookeeperClient.getTables(tablePaths);
            // currently, we don't support schema evolution, so all schemas are version 1
            Map<TablePath, SchemaInfo> tablePath2SchemaInfos =
                    zookeeperClient.getV1Schemas(tablePaths);
            for (TablePath tablePath : tablePaths) {
                if (!tablePath2TableRegistrations.containsKey(tablePath)) {
                    throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
                }
                if (!tablePath2SchemaInfos.containsKey(tablePath)) {
                    throw new SchemaNotExistException(
                            "Schema for '" + tablePath + "' with schema_id=1 does not exist.");
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
            boolean ignoreIfExists) {
        String partitionName = partition.getPartitionName();
        Optional<TablePartition> optionalTablePartition =
                getOptionalTablePartition(tablePath, partitionName);
        if (optionalTablePartition.isPresent()) {
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
            // register partition assignments and partition metadata to zk in transaction
            zookeeperClient.registerPartitionAssignmentAndMetadata(
                    partitionId, partitionName, partitionAssignment, tablePath, tableId);
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

    public void dropPartition(
            TablePath tablePath, ResolvedPartitionSpec partition, boolean ignoreIfNotExists) {
        String partitionName = partition.getPartitionName();
        Optional<TablePartition> optionalTablePartition =
                getOptionalTablePartition(tablePath, partitionName);
        if (!optionalTablePartition.isPresent()) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(
                    String.format(
                            "Partition '%s' does not exist for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        try {
            zookeeperClient.deletePartition(tablePath, partitionName);
        } catch (Exception e) {
            LOG.error(
                    "Fail to delete partition '{}' from zookeeper for table {}.",
                    partitionName,
                    tablePath,
                    e);
        }
    }

    private Optional<TablePartition> getOptionalTablePartition(
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
