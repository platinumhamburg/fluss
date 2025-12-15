---
title: Updating Configs
sidebar_position: 1
---
# Updating Configs

## Overview

Fluss allows you to update cluster or table configurations dynamically without requiring a cluster restart or table recreation. This section demonstrates how to modify and apply such configurations.

## Updating Cluster Configs

From Fluss version 0.8 onwards, some of the server configs can be updated without restarting the server.

Currently, the supported dynamically updatable server configurations include:
- `datalake.format`: Enable lakehouse storage by specifying the lakehouse format, e.g., `paimon`, `iceberg`.
- Options with prefix `datalake.${datalake.format}`
- `kv.shared-rate-limiter-bytes-per-sec`: Control RocksDB flush and compaction write rate shared across all RocksDB instances on the TabletServer.


You can update the configuration of a cluster with [Java client](apis/java-client.md) or [Flink Stored Procedures](#using-flink-stored-procedures).

### Using Java Client

Here is a code snippet to demonstrate how to update the cluster configurations using the Java Client:

```java
// Enable lakehouse storage with Paimon format
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.SET)));

// Disable lakehouse storage
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig(DATALAKE_FORMAT.key(), "paimon", AlterConfigOpType.DELETE)));

// Set RocksDB shared rate limiter to 200MB/sec
admin.alterClusterConfigs(
        Collections.singletonList(
                new AlterConfig("kv.shared-rate-limiter-bytes-per-sec", "209715200", AlterConfigOpType.SET)));
```

The `AlterConfig` class contains three properties:
* `key`: The configuration key to be modified (e.g., `datalake.format`)
* `value`: The configuration value to be set (e.g., `paimon`)
* `opType`: The operation type, either `AlterConfigOpType.SET` or `AlterConfigOpType.DELETE`

### Using Flink Stored Procedures

For certain configurations, Fluss provides convenient Flink stored procedures that can be called directly from Flink SQL.

#### Managing RocksDB Rate Limiter

You can dynamically adjust the shared RocksDB rate limiter for all TabletServers using Flink stored procedures:

**Set Shared RocksDB Rate Limiter:**
```sql title="Flink SQL"
-- Set rate limiter to 200MB/sec (recommended, use named argument, only supported since Flink 1.19)
CALL fluss_catalog.sys.set_shared_rocksdb_rate_limiter(rate_limit => '200MB');

-- Set rate limiter to 500MB/sec (use indexed argument)
CALL fluss_catalog.sys.set_shared_rocksdb_rate_limiter('500MB');

-- Disable rate limiter
CALL fluss_catalog.sys.set_shared_rocksdb_rate_limiter('0MB');
```

**Get Current Shared RocksDB Rate Limiter:**
```sql title="Flink SQL"
-- Query current rate limiter setting
CALL fluss_catalog.sys.get_shared_rocksdb_rate_limiter();
```

The rate limiter controls the write rate of RocksDB flush and compaction operations across all RocksDB instances on each TabletServer. This helps prevent I/O saturation during heavy write loads.

:::tip
The RocksDB rate limiter is shared across all RocksDB instances on a TabletServer, providing server-level control over disk I/O from RocksDB operations. Adjusting this value can help balance performance and resource utilization during peak loads.
:::


## Updating Table Configs

The connector options on a table including [Storage Options](engine-flink/options.md#storage-options) can be updated dynamically by [ALTER TABLE ... SET](engine-flink/ddl.md#alter-table) statement. See the example below:

```sql
-- Enable lakehouse storage for the given table
ALTER TABLE my_table SET ('table.datalake.enabled' = 'true');
```