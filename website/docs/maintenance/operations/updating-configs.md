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
- `kv.rocksdb.shared-rate-limiter.bytes-per-sec`: Control RocksDB flush and compaction write rate shared across all RocksDB instances on the TabletServer. The rate limiter is always enabled. Set to a lower value (e.g., 100MB) to limit the rate, or a very high value to effectively disable rate limiting.


You can update the configuration of a cluster with [Java client](#using-java-client.md) or [Flink Stored Procedures](#using-flink-stored-procedures).

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
                new AlterConfig(KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "200MB", AlterConfigOpType.SET)));
```

The `AlterConfig` class contains three properties:
* `key`: The configuration key to be modified (e.g., `datalake.format`)
* `value`: The configuration value to be set (e.g., `paimon`)
* `opType`: The operation type, either `AlterConfigOpType.SET` or `AlterConfigOpType.DELETE`

### Using Flink Stored Procedures

For certain configurations, Fluss provides convenient Flink stored procedures that can be called directly from Flink SQL.

#### Managing RocksDB Rate Limiter

You can dynamically adjust the shared RocksDB rate limiter for all TabletServers using Flink stored procedures.

The rate limiter is always enabled. Very high values (e.g., Long.MAX_VALUE, which is the default) effectively disable rate limiting with negligible performance overhead.

**Set Shared RocksDB Rate Limiter:**
```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Set rate limiter to 200MB/sec
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec',
  config_value => '200MB'
);

-- Set rate limiter to a very high value to effectively disable rate limiting
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec',
  config_value => '1TB'
);

-- Reset to default value (Long.MAX_VALUE)
CALL sys.set_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);
```

**Get Current Shared RocksDB Rate Limiter:**
```sql title="Flink SQL"
-- Use the Fluss catalog (replace 'fluss_catalog' with your catalog name if different)
USE fluss_catalog;

-- Query current rate limiter setting
CALL sys.get_cluster_config(
  config_key => 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'
);
```

The rate limiter controls the write rate of RocksDB flush and compaction operations across all RocksDB instances on each TabletServer. This helps prevent I/O saturation during heavy write loads.

:::tip
The RocksDB rate limiter is always enabled and shared across all RocksDB instances on a TabletServer, providing server-level control over disk I/O from RocksDB operations. The default value is Long.MAX_VALUE (effectively unlimited). Adjusting this value to a lower limit (e.g., 100MB/s) can help balance performance and resource utilization during peak loads.
:::


## Updating Table Configs

The connector options on a table including [Storage Options](engine-flink/options.md#storage-options) can be updated dynamically by [ALTER TABLE ... SET](engine-flink/ddl.md#alter-table) statement. See the example below:

```sql
-- Enable lakehouse storage for the given table
ALTER TABLE my_table SET ('table.datalake.enabled' = 'true');
```