---
sidebar_label: Bulk Load
title: Flink Bulk Load
sidebar_position: 10
---

# Flink Bulk Load

In batch execution mode, the Fluss connector for Flink 2.2 can load data into a primary-key table
through the BulkLoad protocol instead of the regular sink writer. The client builds each bucket's
final standard KV Snapshot directly in the table's remote storage, then publishes a small manifest
that describes those files. The Coordinator registers the Snapshots as ordinary Completed Snapshots
in one transaction; TabletServers recover them through their ordinary replica path and initialize
the online Log from each Snapshot's log end offset. This bypasses the per-record write path and is
intended for importing large amounts of data into an empty target.

BulkLoad is supported only by the `fluss-flink-2.2` connector, and only for the following targets:

- a non-partitioned primary-key table that is empty, imported as a whole, or
- a single partition of a partitioned primary-key table that is empty, when the `INSERT INTO`
  statement statically specifies the values of all partition keys.

Both the `FULL` and the `WAL` changelog image of the target table are supported, and an empty input
is legal and commits normally.

:::note
BulkLoad only takes effect in batch execution mode. Planning a BulkLoad statement or running
`EXPLAIN` on it has no side effects on the cluster: the BulkLoad transaction begins when the job
actually runs.
:::

## Options

| Option                          | Type     | Default | Description                                                                                                                                                    |
|---------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sink.bulk-load.enabled          | Boolean  | false   | Whether batch `INSERT INTO` a primary-key table uses the BulkLoad protocol to import into an empty target. Only effective in batch mode.                          |
| sink.bulk-load.build-timeout    | Duration | (None)  | The build deadline passed to BulkLoad Begin. It must cover the whole file-build duration; the server-side default is used when the option is absent. See [Setting the Build Timeout](#setting-the-build-timeout). |
| sink.bulk-load.await-timeout    | Duration | 30 min  | The client-side upper bound for retrying the commit until the transaction reaches `COMMITTED`.                                                                  |

Like other connector options, the `sink.bulk-load.*` options can be set in the `WITH` clause of the
table DDL, or per statement through an `OPTIONS` hint. See [Connector Options](options.md) for the
general mechanism.

## Usage

### Enable Bulk Load in the Table DDL

```sql title="Flink SQL"
CREATE TABLE pk_table (
  id INT NOT NULL,
  name STRING,
  amount BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('sink.bulk-load.enabled' = 'true');
```

```sql title="Flink SQL"
-- BulkLoad requires batch execution mode
SET 'execution.runtime-mode' = 'batch';

INSERT INTO pk_table
SELECT id, name, amount FROM source_table;
```

With the option stored in the table metadata, every eligible batch `INSERT INTO` on this table goes
through the BulkLoad path.

### Enable Bulk Load per Statement

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';

INSERT INTO pk_table /*+ OPTIONS('sink.bulk-load.enabled' = 'true') */
SELECT id, name, amount FROM source_table;
```

The hint channel only routes this statement through BulkLoad. Other statements still use the
regular sink path, but access to the same target is rejected while the BulkLoad transaction holds
its fence.

### Bulk Load a Static Partition

For a partitioned table, the `INSERT INTO` statement must statically specify the values of all
partition keys, and the BulkLoad then covers exactly that single partition:

```sql title="Flink SQL"
CREATE TABLE pk_part (
  id INT NOT NULL,
  name STRING,
  dt STRING,
  PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt);

ALTER TABLE pk_part ADD PARTITION (dt = '2026-08-17');
```

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';

INSERT INTO pk_part /*+ OPTIONS('sink.bulk-load.enabled' = 'true') */
PARTITION (dt = '2026-08-17')
SELECT id, name FROM source_table;
```

Other partitions of the table are not fenced by the BulkLoad of this partition and stay writable
through the regular sink path.

## Transaction Model

Flink users submit one batch `INSERT INTO`; Begin, Commit, Abort, and status are not separate public
Flink operations. The connector orchestrates the load through the BulkLoad client, while the
corresponding `FlussAdmin` methods are internal protocol details. Begin verifies that the target is
empty, fences regular access, and returns the frozen metadata needed to build each bucket. Commit
submits the final manifest, atomically decides the metadata change, lets ordinary replica recovery
load the files, and restores regular access.

The Flink SQL BulkLoad path uses a Begin → Build → Committer topology. Begin establishes the
transaction and frozen build context, parallel Build tasks produce the bucket Snapshots, and one
Committer submits their manifest and waits for the transaction to converge.

`BEGUN` transactions can be aborted. The manifest validation inside Commit is still reversible and
does not create a public Prepare state. The atomic `BEGUN` to `COMMITTING` transition is the only
irreversible decision. Once it succeeds, retries and Coordinator failover continue the same
metadata commit until it reaches `COMMITTED`.

BulkLoad is a trusted administrative import path. Use the Fluss client SDK or the Flink connector to
generate its files. The Coordinator validates file identities, sizes, digests, and standard
metadata, but it deliberately does not download and reprocess the full data set before the
irreversible commit. A custom writer that bypasses the supported SDK can therefore create files
that fail later in the ordinary replica reader.

## Eligibility

When `sink.bulk-load.enabled` is set, the statement is checked while the job is being planned and
fails fast with a validation error if any of the following requirements is not met:

1. The job must run in **batch** execution mode; streaming mode is rejected.
2. The target table must be a **primary-key table**.
3. The target table must use the **default merge engine**; tables with another merge engine (for
   example `aggregation`) are rejected.
4. The target table must not declare an **auto-increment column**.
5. For a partitioned table, the statement must carry a **complete static partition spec**: values
   for all partition keys must be given in the `PARTITION (...)` clause. Dynamic partition inserts
   and partially specified static partitions are rejected. Only partition key columns of type
   `STRING`, `INT` or `BIGINT` are supported.
6. The statement must run on a wired Flink version; currently only the Flink 2.2 connector
   supports BulkLoad.
7. Batch speculative execution must be disabled (`execution.batch.speculative.enabled`, which is
   disabled by default in Flink).

Additional conditions are enforced deterministically by the Fluss server when the transaction
begins at job runtime, and the server-side reason is propagated in the job failure:

- The target table or partition must be **empty**; a non-empty target is rejected.
- The target must reside on the cluster's **default remote data root**. On a cluster configured
  with multiple remote data directories (`remote.data.dirs`), a table or partition whose remote
  directory is not the default one is rejected when the transaction begins.
- Server-side admission checks (such as the cluster API version) must pass.

The shape of the statement itself is also checked at planning time: only full-column batch
`INSERT INTO` statements are supported — `UPDATE` statements and partial-column `INSERT`
statements are rejected with a validation error.

## Deployment Prerequisites

- The TaskManagers upload the final standard files directly to the table's remote storage, so the
  TaskManager classpath must contain the Fluss filesystem plugin matching the scheme of the
  cluster's `remote.data.dir` (for example `fluss-fs-oss` for `oss://`). See
  [File Systems](../maintenance/tiered-storage/filesystems/overview.md) for the available plugins.
- The shaded `fluss-flink-2.2` connector jar already bundles the RocksDB library used to build the
  final KV Snapshots, so no extra RocksDB installation is needed. The builders create one local
  RocksDB instance per assigned bucket in the TaskManager's local temporary directory; make sure
  the TaskManagers have enough local disk for the data volume being imported.
- The identity the job uses to connect to Fluss (the caller of the BulkLoad Begin/Commit
  RPCs) needs the `WRITE` [operation](../security/authorization.md#operation) on the target table.

## Setting the Build Timeout

`sink.bulk-load.build-timeout` is the build deadline of the BulkLoad transaction and must cover
the whole build window: from the moment the transaction begins until all final files have been built
and uploaded and Commit is requested. Estimate the build time from the data volume and the
upload bandwidth between the TaskManagers and the remote storage, and set the timeout generously;
large imports should always set it explicitly instead of relying on the server-side default.

If the deadline expires before the manifest is submitted, the server reclaims the transaction and
lifts the fence. A restart of the same job graph retains its caller token, observes the expired
transaction as `ABORTED`, and fails. Submit a new job graph to use a new token and begin a new
transaction.

Be aware of the fence trade-off: from the moment the transaction begins until it terminates, the
target table or partition is fenced and regular access to it is rejected. This is a deliberate
design cost of the protocol, so keep the window as short as practical, but never shorter than the
expected build time.

## Failure Recovery

The caller token is generated when the Flink job graph is built and remains stable across restarts
of that graph. Recovery therefore addresses the same durable transaction:

- For `BEGUN`, the restarted job reuses the same transaction and frozen metadata. Local attempt
  state is not resumed; every bucket replays its complete input and rebuilds its files.
- For `COMMITTING` or `COMMITTED`, the job repeats the exact Commit. The request joins or confirms
  the existing durable decision, including after Coordinator or TabletServer failover.
- If a Commit result is unknown, the client retries that exact Commit within
  `sink.bulk-load.await-timeout`; it does not switch to status polling.
- For `ABORTED`, including a transaction reclaimed after its build deadline, the same token reports
  the persisted terminal outcome and the job fails. Starting a fresh transaction requires a new
  job graph and therefore a new token.

An independently submitted `INSERT INTO` is a new load, not a retry of the earlier transaction. If
the earlier load committed, the new load is rejected because the target is no longer empty. Within
one load, each bucket writer keeps the last complete row it observes for a primary key. If the input
does not define a stable order, perform any deterministic ordering or deduplication required by the
application before BulkLoad.

## V1 Limitations

The first version of BulkLoad deliberately does not support:

- The Flink 1.18/1.19/1.20 connectors. Only the Flink 2.2 connector is wired; setting
  `sink.bulk-load.enabled` with other versions fails fast.
- Dynamic partition writes, partially specified static partitions, and transaction orchestration
  across multiple partitions.
- `INSERT OVERWRITE` and overwriting non-empty targets; the protocol itself rejects a non-empty
  target.
- BulkLoad in streaming execution mode.
- Client-side replay for non-default merge engines.
- Spark integration and in-place upgrades of lake tables.
- A large-scale production validation report; v1 ships without one, and running such a validation
  is left to the adopters.
