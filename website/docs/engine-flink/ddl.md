---
sidebar_label: DDL
title: Flink DDL
sidebar_position: 2
---

# Flink DDL

## Create Catalog
Fluss supports creating and managing tables through the Fluss Catalog.
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-server-1:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

The following properties can be set if using the Fluss catalog:

| Option                         | Required | Default   | Description                                                                                                                                                                                                            |
|--------------------------------|----------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                           | required | (none)    | Catalog type, must be 'fluss' here.                                                                                                                                                                                    |
| bootstrap.servers              | required | (none)    | Comma separated list of Fluss servers.                                                                                                                                                                                 |
| default-database               | optional | fluss     | The default database to use when switching to this catalog.                                                                                                                                                            |
| client.security.protocol       | optional | PLAINTEXT | The security protocol used to communicate with brokers. Currently, only `PLAINTEXT` and `SASL` are supported, the configuration value is case insensitive.                                                             |
| `client.security.{protocol}.*` | optional | (none)    | Client-side configuration properties for a specific authentication protocol. E.g., client.security.sasl.jaas.config. More Details in [authentication](../security/authentication.md)                                   |
| `{lake-format}.*`              | optional | (none)    | Extra properties to be passed to the lake catalog. This is useful for configuring sensitive settings, such as the username and password required for lake catalog authentication. E.g., `paimon.jdbc.password = pass`. |

The following statements assume that the current catalog has been switched to the Fluss catalog using the `USE CATALOG <catalog_name>` statement.

## Create Database

By default, FlussCatalog will use the `fluss` database in Flink. You can use the following example to create a separate database to avoid creating tables under the default `fluss` database:

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

## Drop Database

To delete a database, this will drop all the tables in the database as well:

```sql title="Flink SQL"
-- Flink doesn't allow drop current database, switch to Fluss default database
USE fluss;
```

```sql title="Flink SQL"
-- drop the database
DROP DATABASE my_db;
```

## Create Table

### Primary Key Table

The following SQL statement will create a [Primary Key Table](table-design/table-types/pk-table/index.md) with a primary key consisting of shop_id and user_id.
```sql title="Flink SQL"
CREATE TABLE my_pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### Log Table

The following SQL statement creates a [Log Table](table-design/table-types/log-table.md) by not specifying primary key clause.

```sql title="Flink SQL"
CREATE TABLE my_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) WITH (
  'bucket.num' = '8'
);
```

### Partitioned (Primary Key/Log) Table

:::note
1. Currently, Fluss only supports partitioned field with `STRING` type
2. For the Partitioned Primary Key Table, the partitioned field (`dt` in this case) must be a subset of the primary key (`dt, shop_id, user_id` in this case)
:::

The following SQL statement creates a Partitioned Primary Key Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_part_pk_table (
  dt STRING,
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (dt, shop_id, user_id) NOT ENFORCED
) PARTITIONED BY (dt);
```

The following SQL statement creates a Partitioned Log Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt);
```
:::info
Fluss partitioned table supports dynamic partition creation, which means you can write data into a partition without pre-creating it.
You can use the `INSERT INTO` statement to write data into a partitioned table, and Fluss will automatically create the partition if it does not exist.
See the [Dynamic Partitioning](table-design/data-distribution/partitioning.md#dynamic-partitioning) for more details.
But you can still use the [Add Partition](engine-flink/ddl.md#add-partition) statement to manually add partitions if needed.
:::

#### Multi-Fields Partitioned Table

Fluss also supports [Multi-Fields Partitioning](table-design/data-distribution/partitioning.md#multi-field-partitioned-tables), the following SQL statement creates a Multi-Fields Partitioned Log Table in Fluss:

```sql title="Flink SQL"
CREATE TABLE my_multi_fields_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING,
  nation STRING
) PARTITIONED BY (dt, nation);
```

#### Auto Partitioned (Primary Key/Log) Table

Fluss also supports creating Auto Partitioned (Primary Key/Log) Table. The following SQL statement creates an Auto Partitioned Primary Key Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_auto_part_pk_table (
  dt STRING,
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (dt, shop_id, user_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket.num' = '4',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'day'
);
```

The following SQL statement creates an Auto Partitioned Log Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_auto_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt) WITH (
  'bucket.num' = '8',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'hour'
);
```

For more details about Auto Partitioned (Primary Key/Log) Table, refer to [Auto Partitioning](table-design/data-distribution/partitioning.md#auto-partitioning).


### Options

The supported option in `WITH` parameters when creating a table are listed in [Connector Options](engine-flink/options.md) page.

## Create Table Like

To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql title="Flink SQL"
-- there is a temporary datagen table
CREATE TEMPORARY TABLE datagen (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10'
);
```

```sql title="Flink SQL"
-- creates Fluss table which derives the metadata from the temporary table excluding options
CREATE TABLE my_table LIKE datagen (EXCLUDING OPTIONS);
```

For more details, refer to the [Flink CREATE TABLE](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/create/#like) documentation.


## Drop Table

To delete a table, run:

```sql title="Flink SQL"
DROP TABLE my_table;
```

This will entirely remove all the data of the table in the Fluss cluster.

## Alter Table
### SET properties
The SET statement allows users to configure one or more connector options including the [Storage Options](engine-flink/options.md#storage-options) for a specified table. If a particular option is already configured on the table, it will be overridden with the new value.

When using SET to modify [Storage Options](engine-flink/options.md#storage-options), the Fluss cluster will dynamically apply the changes to the table. This can be useful for modifying table behavior without needing to recreate the table.

**Supported Options to modify**
- All [Read Options](engine-flink/options.md#read-options), [Write Options](engine-flink/options.md#write-options), [Lookup Options](engine-flink/options.md#lookup-options) and [Other Options](engine-flink/options.md#other-options) except `bootstrap.servers`.
- The following [Storage Options](engine-flink/options.md#storage-options):
  - `table.datalake.enabled`: Enable or disable lakehouse storage for the table.

```sql title="Flink SQL"
ALTER TABLE my_table SET ('table.datalake.enabled' = 'true');
```

**Limits**
- If lakehouse storage (`table.datalake.enabled`) is already enabled for a table, options with lakehouse format prefixes (e.g., `paimon.*`) cannot be modified again.


### RESET properties
Reset one or more connector option including the [Storage Options](engine-flink/options.md#storage-options) to its default value.

The following example illustrates reset the `table.datalake.enabled` option to its default value `false` on the table.

```sql title="Flink SQL"
ALTER TABLE my_table RESET ('table.datalake.enabled');
```


## Add Partition

Fluss supports manually adding partitions to an existing partitioned table through the Fluss Catalog. If the specified partition 
does not exist, Fluss will create the partition. If the specified partition already exists, Fluss will ignore the request 
or throw an exception.

To add partitions, run:

```sql title="Flink SQL"
-- Add a partition to a single field partitioned table
ALTER TABLE my_part_pk_table ADD PARTITION (dt = '2025-03-05');

-- Add a partition to a multi-field partitioned table
ALTER TABLE my_multi_fields_part_log_table ADD PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink ALTER TABLE(ADD)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#add) documentation.

## Show Partitions

To show all the partitions of a partitioned table, run:
```sql title="Flink SQL"
SHOW PARTITIONS my_part_pk_table;
```

For multi-field partitioned tables, you can use the `SHOW PARTITIONS` command with either **partial** or **full** partition field conditions to list matching partitions.

```sql title="Flink SQL"
-- Show partitions using a partial partition filter
SHOW PARTITIONS my_multi_fields_part_log_table PARTITION (dt = '2025-03-05');

-- Show partitions using a full partition filter
SHOW PARTITIONS my_multi_fields_part_log_table PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink SHOW PARTITIONS](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/show/#show-partitions) documentation.

## Drop Partition

Fluss also supports manually dropping partitions from an existing partitioned table through the Fluss Catalog. If the specified partition 
does not exist, Fluss will ignore the request or throw an exception.


To drop partitions, run:
```sql title="Flink SQL"
-- Drop a partition from a single field partitioned table
ALTER TABLE my_part_pk_table DROP PARTITION (dt = '2025-03-05');

-- Drop a partition from a multi-field partitioned table
ALTER TABLE my_multi_fields_part_log_table DROP PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink ALTER TABLE(DROP)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#drop) documentation.

## Materialized Table
### Overview
Flink Materialized Table is a new table type introduced in Flink SQL that simplifies the development of both batch and streaming data pipelines.
By defining the data freshness and query during creation, Flink automatically derives the table schema and generates a refresh pipeline to maintain the desired freshness level. This provides a unified and consistent development experience for both real-time and batch workloads. For more information, see the [Flink Materialized Table](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/materialized-table/overview) documentation.

Starting from Fluss version 0.8, Flink Materialized Table is now supported, which can significantly reduce the cost of building real-time data pipelines with Apache Flink and Fluss. Materialized tables in Fluss are implemented as regular Fluss tables with special metadata to identify them as materialized tables.

### Create Materialized Table

Materialized tables are created using the `CREATE MATERIALIZED TABLE` statement with a freshness interval and a query definition:

```sql title="Flink SQL"
CREATE MATERIALIZED TABLE shop_summary
FRESHNESS = INTERVAL '5' SECOND
AS SELECT 
  DATE_FORMAT(order_time, 'yyyy-MM-dd') AS order_date,
  shop_id,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders
GROUP BY DATE_FORMAT(order_time, 'yyyy-MM-dd'), shop_id;
```

#### Supported Refresh Modes

Apache Fluss currently supports **CONTINUOUS** refresh mode for materialized table, which means the materialized table is continuously refreshed. The **FULL** refresh mode will be supported in future releases.

#### Schema Definition

The schema of a materialized table is automatically inferred from the query definition. You cannot manually specify column names and types - they are derived from the SELECT statement.

```sql title="Flink SQL"
-- The schema will be automatically inferred as:
-- order_date: STRING
-- shop_id: BIGINT  
-- order_count: BIGINT
-- total_amount: BIGINT
CREATE MATERIALIZED TABLE daily_sales
FRESHNESS = INTERVAL '1' MINUTE
AS SELECT 
  DATE_FORMAT(created_at, 'yyyy-MM-dd') AS order_date,
  shop_id,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM sales_events
GROUP BY DATE_FORMAT(created_at, 'yyyy-MM-dd'), shop_id;
```

### Alter Materialized Table

You can suspend and resume materialized tables to control their refresh behavior:

#### Suspend Materialized Table

```sql title="Flink SQL"
ALTER MATERIALIZED TABLE shop_summary SUSPEND;
```

This stops the automatic refresh of the materialized table and saves the current state.

#### Resume Materialized Table

```sql title="Flink SQL"
ALTER MATERIALIZED TABLE shop_summary RESUME;
```

This resumes the automatic refresh of the materialized table from the last saved state.

### Drop Materialized Table

To delete a materialized table:

```sql title="Flink SQL"
DROP MATERIALIZED TABLE shop_summary;
```

This will drop the materialized table and stop the background refresh job.

### Materialized Table Options

Materialized tables support the same table options as regular Fluss tables, including partitioning and bucketing:

```sql title="Flink SQL"
CREATE MATERIALIZED TABLE partitioned_summary
FRESHNESS = INTERVAL '10' SECOND
AS SELECT 
  dt,
  shop_id,
  COUNT(*) AS order_count
FROM orders
GROUP BY dt, shop_id
PARTITIONED BY (dt)
WITH (
  'bucket.num' = '4'
);
```

### Limitations

- Only continuous refresh mode is supported
- Schema is automatically derived from the query
- Materialized tables are stored as regular Fluss tables with special metadata

## Procedures

Fluss provides stored procedures to perform administrative and management operations through Flink SQL. All procedures are located in the `sys` namespace and can be invoked using the `CALL` statement.

### Available Procedures

You can list all available procedures using:

```sql title="Flink SQL"
SHOW PROCEDURES;
```

### Access Control Procedures

Fluss provides procedures to manage Access Control Lists (ACLs) for security and authorization.

#### add_acl

Add an ACL entry to grant permissions to a principal.

**Syntax:**

```sql
CALL [catalog_name.]sys.add_acl(
  resource => 'STRING',
  permission => 'STRING', 
  principal => 'STRING',
  operation => 'STRING',
  host => 'STRING'  -- optional, defaults to '*'
)
```

**Parameters:**

- `resource` (required): The resource to grant permissions on. Can be `'CLUSTER'` for cluster-level permissions or a specific resource name (e.g., database or table name).
- `permission` (required): The permission type to grant. Valid values are `'ALLOW'` or `'DENY'`.
- `principal` (required): The principal to grant permissions to, in the format `'Type:Name'` (e.g., `'User:Alice'`).
- `operation` (required): The operation type to grant. Valid values include `'READ'`, `'WRITE'`, `'CREATE'`, `'DELETE'`, `'ALTER'`, `'DESCRIBE'`, `'CLUSTER_ACTION'`, `'IDEMPOTENT_WRITE'`.
- `host` (optional): The host from which the principal can access the resource. Defaults to `'*'` (all hosts).

**Example:**

```sql title="Flink SQL"
-- Grant read permission to user Alice from any host
CALL sys.add_acl(
  resource => 'CLUSTER',
  permission => 'ALLOW',
  principal => 'User:Alice',
  operation => 'READ',
  host => '*'
);

-- Grant write permission to user Bob from a specific host
CALL sys.add_acl(
  resource => 'my_database.my_table',
  permission => 'ALLOW',
  principal => 'User:Bob',
  operation => 'WRITE',
  host => '192.168.1.100'
);
```

#### drop_acl

Remove an ACL entry to revoke permissions.

**Syntax:**

```sql
CALL [catalog_name.]sys.drop_acl(
  resource => 'STRING',
  permission => 'STRING',
  principal => 'STRING', 
  operation => 'STRING',
  host => 'STRING'  -- optional, defaults to '*'
)
```

**Parameters:**

All parameters accept the same values as `add_acl`. You can use `'ANY'` as a wildcard value to match multiple entries for batch deletion.

**Example:**

```sql title="Flink SQL"
-- Remove a specific ACL entry
CALL sys.drop_acl(
  resource => 'CLUSTER',
  permission => 'ALLOW',
  principal => 'User:Alice',
  operation => 'READ',
  host => '*'
);

-- Remove all ACL entries for a specific user
CALL sys.drop_acl(
  resource => 'ANY',
  permission => 'ANY',
  principal => 'User:Alice',
  operation => 'ANY',
  host => 'ANY'
);
```

#### list_acl

List ACL entries matching the specified filters.

**Syntax:**

```sql
CALL [catalog_name.]sys.list_acl(
  resource => 'STRING',
  permission => 'STRING',  -- optional, defaults to 'ANY'
  principal => 'STRING',   -- optional, defaults to 'ANY'
  operation => 'STRING',   -- optional, defaults to 'ANY'
  host => 'STRING'         -- optional, defaults to 'ANY'
)
```

**Parameters:**

All parameters accept the same values as `add_acl`. Use `'ANY'` as a wildcard to match all values for that parameter.

**Returns:** An array of strings, each representing an ACL entry in the format: `resource="...";permission="...";principal="...";operation="...";host="..."`

**Example:**

```sql title="Flink SQL"
-- List all ACL entries
CALL sys.list_acl(resource => 'ANY');

-- List all ACL entries for a specific user
CALL sys.list_acl(
  resource => 'ANY',
  principal => 'User:Alice'
);

-- List all read permissions
CALL sys.list_acl(
  resource => 'ANY',
  operation => 'READ'
);
```

### Cluster Configuration Procedures

Fluss provides procedures to dynamically manage cluster configurations without requiring a server restart.

#### get_cluster_config

Retrieve cluster configuration values.

**Syntax:**

```sql
-- Get a specific configuration
CALL [catalog_name.]sys.get_cluster_config(config_key => 'STRING')

-- Get all cluster configurations
CALL [catalog_name.]sys.get_cluster_config()
```

**Parameters:**

- `config_key` (optional): The configuration key to retrieve. If omitted, returns all cluster configurations.

**Returns:** A table with columns:
- `config_key`: The configuration key name
- `config_value`: The current value
- `config_source`: The source of the configuration (e.g., `DYNAMIC_CONFIG`, `STATIC_CONFIG`)

**Example:**

```sql title="Flink SQL"
-- Get a specific configuration
CALL sys.get_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);

-- Get all cluster configurations
CALL sys.get_cluster_config();
```

#### set_cluster_config

Set or delete a cluster configuration dynamically.

**Syntax:**

```sql
-- Set a configuration value
CALL [catalog_name.]sys.set_cluster_config(
  config_key => 'STRING',
  config_value => 'STRING'
)

-- Delete a configuration (reset to default)
CALL [catalog_name.]sys.set_cluster_config(config_key => 'STRING')
```

**Parameters:**

- `config_key` (required): The configuration key to modify.
- `config_value` (optional): The new value to set. If omitted or empty, the configuration is deleted (reset to default).

**Important Notes:**

- Changes are validated before being applied and persisted in ZooKeeper
- Changes are automatically applied to all servers (Coordinator and TabletServers)
- Changes survive server restarts
- Not all configurations support dynamic changes. The server will reject invalid modifications

**Example:**

```sql title="Flink SQL"
-- Set RocksDB rate limiter
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec',
  config_value => '200MB'
);

-- Set datalake format
CALL sys.set_cluster_config(
  config_key => 'datalake.format',
  config_value => 'paimon'
);

-- Delete a configuration (reset to default)
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);
```
