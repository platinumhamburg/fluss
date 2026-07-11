# Offset-Fenced Index Push Design

## Status

This design was re-audited against the current global secondary index v2 code on
2026-07-11. The core choice is accepted with the correctness requirements in this
document. It replaces the uncommitted per-row `__source_offset` and
`__index_deleted` implementation.

The audit found six requirements that are part of the design, not optional follow-up
hardening:

1. source WAL may be released only by an index progress value contained in a committed
   source KV snapshot;
2. target recovery must prove that its KV snapshot, WriterState snapshot, and retained
   WAL jointly cover recovery;
3. a target WAL append whose outcome is uncertain must fail-stop the replica;
4. partition tombstone state must be authoritatively initialized and serialized with
   target apply; and
5. a partitioned physical index key must include `__partition_id` so an old partition
   incarnation cannot modify a recreated incarnation; and
6. under protocol V1, PutKv, target WAL, and WriterState must treat WriterKey as generic
   opaque bits; source-bucket encoding and validation remain owned by the index module.

This protocol is approved and ready to drive the revised implementation plan.
Production readiness still depends on the tests and capacity evidence specified below.

## Context

Global secondary index v2 reads each main-table `TableBucket` WAL, derives physical KV
mutations, and sends them to buckets of an internal Index Table. Delivery is at least
once. A source leader sends at most one window per index at a time, but after source
leader failover a request from the old leader may overlap and race with replay from the
new leader.

Ordinary KV overwrite makes an identical retry harmless. It does not order different
operations. A delayed UPSERT can otherwise arrive after a newer DELETE, or a delayed
DELETE after a newer UPSERT, and become the permanent target state.

The rejected implementation put a source offset and logical-delete bit in every index
row. It prevented some stale row updates, but imposed a read-before-write merger, a
lookup filter, permanent logical tombstones, and per-row storage overhead. It also tried
to compare offsets belonging to different partition incarnations.

The selected design places ordering at the existing target WriterState boundary. One
internal batch represents the complete ordered projection for one source writer, one
index, one target bucket, and one source WAL prefix. Rows remain ordinary physical
UPSERTs and DELETEs.

## Decision Summary

| Concern | Decision |
|---|---|
| Delivery | At least once, eventually consistent |
| Ordering value | `long batchSequence == IndexWindow.windowEndOffset` |
| KV idempotence protocol | Table-bound immutable version: V0 compact exact-sequence or V1 extended fence |
| Writer identity | V0 retains `writerId:int64`; V1 uses opaque 128-bit `WriterKey` canonically encoded by the index module |
| Target state | Reuse WriterState lifecycle with protocol-specific state representation and validation |
| Stale request | Successful no-op after the dominating target WAL offset is committed |
| Index deletion | Physical KV DELETE |
| Per-row ordering columns | None |
| Partition incarnation | `__partition_id` in physical key and value |
| Ordinary KV protocol | Existing V0 wire, in-memory, WAL, snapshot, expiration, and exact-next behavior remain unchanged |
| Log Table protocol | ProduceLog and Log Table idempotence are outside this change |
| Source retention | Released only by committed source snapshot progress, locally and remotely |
| Target recovery | KV snapshot, WriterState snapshot, and WAL must have continuous joint coverage |
| Future main-table rescale | Independent of bucket count; any rescale that renumbers existing source WAL domains requires a new Index Table generation |

## Goals

- Prevent a request from an old source leader from becoming the terminal index state.
- Preserve at-least-once index replication without pretending to provide exactly once.
- Use blind physical UPSERT and DELETE on the normal KV path.
- Remove `__source_offset`, `__index_deleted`, versioned Index Table merge, and logical
  visibility filtering.
- Keep ordering state proportional to source-to-target channels rather than index rows.
- Preserve source failover even when local WAL has moved to remote storage.
- Reuse WriterState durability, target WAL replication, truncation, and delayed HW ACK.
- Keep the ordinary public KV idempotence behavior and wire representation unchanged.
- Add the V1 PutKv WriterKey capability without introducing source-table, partition,
  bucket, or index concepts into the common write path.
- Preserve the compact V0 representation for ordinary KV tables in network, WAL,
  snapshot, and memory.
- Isolate partition incarnations while retaining the existing tombstone visibility and
  compaction defenses.
- Minimize architecture intrusion: no writer field in assignment metadata and no new
  IndexBucket state store.

## Non-Goals

- Exactly-once index replication.
- Atomicity across different target Index Table buckets.
- Implementing main-table rescale or preserving index state across a rescale that
  renumbers existing source WAL domains.
- Reconstructing source index history from lake-compacted data.
- Migrating data created by the rejected, uncommitted row-version format.
- Removing the main-table recheck from secondary-index lookup.
- Turning secondary indexes into user-writable KV tables.
- Adding the V1 fenced protocol to ProduceLog or Log Tables.

## Terminology And Offset Convention

- A source pushed offset is always the next source WAL offset to read.
- `P_i` is the in-memory next-read offset for index `i` on a source `TableBucket`.
- `P_durable` is the minimum all-index next-read offset contained in the latest committed
  source KV snapshot.
- `E` is `IndexWindow.windowEndOffset`, the exclusive end of the source WAL range
  represented by a window. The internal `batchSequence` is exactly `E`.
- `W` is the fixed-width opaque `WriterKey` for one source `TableBucket` incarnation.
  PutKv and WriterState do not interpret its index-specific bit layout.
- `T` is one target bucket of one physical Index Table.
- `L(W,T)` is the greatest accepted internal `batchSequence` for writer `W` at target
  bucket `T`.
- `D(W,T)` is the inclusive target WAL offset of the batch that established `L(W,T)`.
- `K` is the target WAL next offset represented by a committed target KV snapshot.
- `R` is the target WAL next offset represented by a full WriterState snapshot.

All source ranges are half-open. A source window that starts at `P` and ends at `E`
represents `[P,E)`. Remote log segment end offsets are also exclusive.

## Required Invariants

### Source invariants

1. Index replication reads only records below source high watermark.
2. One source leader has at most one unacknowledged window per index.
3. A later window for an index is built only after every target batch of the previous
   window has a committed acknowledgement.
4. `P_i` advances to `E` only after every non-empty target projection in the window is
   acknowledged. An entirely empty projection may advance immediately.
5. A new leader starts every index at or before `P_durable`. Replaying earlier source WAL
   is allowed; starting after `P_durable` is forbidden.
6. Only a committed source KV snapshot may advance the local or remote raw-WAL deletion
   bound. Volatile `P_i` values never release WAL.
7. Records inside one target batch preserve source mutation order.
8. A target batch contains a complete source mutation group; it never ends between
   `UPDATE_BEFORE` and `UPDATE_AFTER`.

The one-window rule is local to a leader. Old and new leaders may overlap. Correctness
must not depend on preventing already-sent requests from the old leader.

### Target invariants

1. `W` identifies one source WAL sequence domain within the lifetime of one Index Table.
2. A target accepts a batch only when its PutKv API version, KV batch magic, and immutable
   table idempotence protocol version agree. An Index Table write guard rejects a V0 or
   non-idempotent writer before KV prewrite.
3. For an active writer, `E > L(W,T)` is fresh and `E <= L(W,T)` is stale.
4. A fresh batch's KV effects and WriterState metadata are recoverable from the same
   target WAL append.
5. A stale batch performs no row decode, KV mutation, or target WAL append.
6. A stale success waits until `D(W,T)` is below target high watermark.
7. V1 WriterState does not expire while its source partition is alive.
8. Target truncation restores WriterState to exactly the retained target WAL prefix.
9. No local or remote target WAL deletion may break joint KV and WriterState recovery.
10. Tombstone validation and WriterState retirement serialize with target KV apply.

## Generic Writer Key And Index Encoding

The common write path represents writer identity as an immutable fixed-width value:

```text
WriterKey = (high:int64, low:int64)
```

Under idempotence protocol V1, PutKv, KvTablet, target WAL, WriterState, snapshots, and
recovery compare and persist these 128 bits exactly. They do not expose or interpret
source table ID, partition ID, bucket ID, or index semantics. Protocol V0 retains the
existing `writerId:int64` representation in the KV batch, target WAL, WriterState map,
and snapshot. V0 and V1 identities never coexist in one table because the protocol
version is immutable and request data cannot select it.

The index module owns the only source-bucket encoding. An Index Table has exactly one
upstream main table and WriterState is scoped by target `TableBucket`, so source table ID
is intentionally omitted. The canonical encoding is:

```text
high                         = partitionId when partitioned, otherwise 0
low bit 63                   = 1 when partitioned, otherwise 0
low bits 0..30               = non-negative sourceBucketId
low bits 31..62              = 0
```

The index codec rejects a negative bucket ID, a negative partition ID, any non-zero
reserved bit, or an unpartitioned key whose `high` value is non-zero. Decoding followed
by encoding must reproduce the exact original WriterKey. This canonicality requirement
prevents one source bucket from acquiring multiple WriterState identities.

The representation is injective over every valid Fluss source bucket. It contains no
hash, multiplication, bucket-count dependency, overflow path, or probabilistic
collision. Java map hash collisions are harmless because key equality compares the key
kind and all stored bits.

The index codec lives in the index module. Common PutKv and WriterState code must not
depend on it. The source bucket count is neither stored in Index Table metadata nor used
to interpret WriterState.

This identity requires no changes to `TableAssignment`, `PartitionAssignment`,
NotifyLeaderAndIsr, or Coordinator writer-ID allocation. After source leader failover,
the new leader derives the same WriterKey from the same source `TableBucket`.

Partition IDs are never reused. A rescale that preserves an existing source bucket's
partition ID and bucket ID preserves its WriterKey, and newly added bucket IDs receive
independent WriterState. A rescale that renumbers or replaces existing source WAL
sequence domains must create a new physical Index Table, build and catch it up, then
atomically switch the index definition. The old table is retired after cutover.

## Protocol-Specific Record Formats

The V1 logical batch-sequence API is 64-bit. Protocol V0 retains its existing 32-bit
field and all existing storage layouts.

### KV request batches

- Idempotence protocol V0 keeps KV magic v0 as
  `writerId:int64 + batchSequence:int32`.
- Idempotence protocol V1 uses a new KV magic with
  `writerKeyHigh:int64 + writerKeyLow:int64 + batchSequence:int64`.
- The V1 format is a generic PutKv idempotence capability. Its common parser and apply
  path treat WriterKey as opaque; IndexSender is its first producer.
- A tablet accepts only the KV magic fixed by its immutable table protocol version. The
  request cannot select, mix, or downgrade that protocol.

### Target WAL batches

- Protocol V0 retains existing WAL magic v0-v2 and its 32-bit ordinary sequence.
- Protocol V1 uses a new target WAL magic that persists WriterKey and a 64-bit sequence.
- Follower append and log recovery verify that the format matches the tablet's trusted
  KV idempotence protocol version.
- Unsupported magic is corruption, not a request that may fall back to non-idempotent
  write behavior.

No source-specific field is added to either common header. `batchSequence` is
`windowEndOffset`; the index module canonically encodes source partition and bucket
identity into the opaque V1 WriterKey.

### WriterState snapshots

Protocol V0 tables continue to read and write the existing snapshot representation. A
protocol V1 snapshot version stores:

```text
kvIdempotenceProtocolVersion = 1
writerKeyHigh:int64
writerKeyLow:int64
lastBatchSequence:int64
dominatingTargetWalOffset:int64
lastTimestamp:int64
```

The V1 form retains only the latest batch metadata for each writer. V0 writers retain
their existing five-batch duplicate history, expiration, exact-next validation, and
`Integer.MAX_VALUE -> 0` rollover.

## Table-Bound KV Idempotence Protocol

The protocol is an immutable property of a primary-key KV table:

```text
table.kv.idempotence-protocol-version = 0 | 1
default = 0
```

It selects one complete, non-composable contract. Code resolves the integer to
`KvIdempotenceProtocol.V0_COMPACT` or `KvIdempotenceProtocol.V1_FENCED`:

| Contract dimension | V0_COMPACT | V1_FENCED |
|---|---|---|
| Writer identity | `writerId:int64` | `WriterKey:128bit` |
| Sequence encoding | `int32` | `int64` |
| Fresh sequence | exact next expected sequence | greater than last accepted fence |
| Duplicate/stale state | existing latest-five history | latest accepted state only |
| Lower or equal sequence | duplicate only when retained, otherwise error | successful stale no-op |
| Automatic expiration | existing writer TTL | none; explicit partition retirement |
| KV/WAL/snapshot representation | existing formats | new protocol-specific formats |

These dimensions are not independent user options. Allowing arbitrary combinations
would create unproved protocols such as a 32-bit expiring sequence fence or a 128-bit
exact-next writer. A protocol version identifies the complete compatibility and
correctness contract.

Table rules are:

- every KV table that does not explicitly set the property resolves to V0, regardless of
  creation time or server version;
- a user-facing DATA_TABLE currently accepts only V0;
- IndexTableDescriptorFactory must explicitly write version 1 for every new Index Table;
  runtime code must not infer V1 from `table.type`;
- a Log Table rejects an explicitly configured `table.kv.idempotence-protocol-version`
  and keeps its existing ProduceLog idempotence behavior;
- the property cannot change after table creation; and
- no request field, API version, or batch magic may change the table's selected protocol.

`WriterStateManager` remains the state lifecycle owner, but it preserves protocol-specific
storage efficiency. V0 continues to use the current `Map<Long, WriterStateEntry>`,
five-entry history, snapshot serde, and expiration path. V1 uses a 128-bit WriterKey map,
one latest entry, V1 snapshot serde, and explicit retirement. Both reuse snapshot
scheduling, target WAL replay, truncation, remote transfer, and delayed-HW metadata.

V1 validation is:

```text
unknown writer and E >= 0  -> fresh
known writer and E > last  -> fresh
known writer and E <= last -> stale
E < 0                      -> invalid
```

Unknown state is valid only after recovery has proved complete coverage. A missing state
after required WAL has been deleted is a recovery failure, never permission to accept a
request as a new writer.

## Common Write Path And Index Boundary

The dependency direction is strict:

```text
index module -> generic PutKv / KvTablet / WriterState
generic write path -X-> index module
```

The common write path owns only:

- protocol V0/V1 batch parsing, identity equality, persistence, and recovery;
- protocol selection from immutable KV table metadata before tablet construction;
- stale/fresh WriterState decisions; and
- a generic write-guard contract executed under `kvLock`.

The index module supplies the WriterKey encoder and an Index Table write guard. The guard
is installed when the Index Table replica is constructed, following the existing pattern
for table-specific row merger, tag extractor, and compaction filter dependencies. The
common path sees only `ACCEPT`, `NO_OP`, or `REJECT`; it never sees an index-specific
return type.

For an Index Table, the guard:

1. decodes and verifies the canonical index WriterKey;
2. verifies partitioned/unpartitioned mode against trusted Index Table metadata;
3. requires the authoritative tombstone baseline and returns `NO_OP` for a tombstoned
   source partition; and
4. on a fresh batch, verifies that every physical index mutation carries the decoded
   partition ID.

The batch-level guard runs before WriterState lookup so a tombstoned partition creates no
state. Per-record validation runs only on the fresh path. The default guard for V0 KV
tables accepts without decoding a WriterKey or adding a per-record branch.

## Target Apply State Machine

The existing per-tablet `kvLock` is the serialization point. This design adds no global
lock and no sender-side lock across source leaders. The lock order remains:

```text
KvTablet.kvLock -> LogTablet lock
```

Tombstone retirement uses the same order. No path may acquire them in reverse order.

### Validation before prewrite

Under `kvLock`, the target:

1. validates schema, PutKv API version, KV magic, and WriterKey against the immutable V1
   table protocol, and validates non-negative `E`;
2. invokes the installed write guard, which validates the canonical source identity,
   requires an authoritative tombstone baseline, and returns tombstone-success for a
   covered partition;
3. reads current WriterState; and
4. takes the stale or fresh path.

Format, identity, and tombstone-readiness failures occur before row decode and prewrite.

### Fresh batch: `E > L(W,T)`

1. Decode records in encoded order.
2. For partitioned records, verify every UPSERT value and every UPSERT/DELETE key carries
   the partition ID decoded by the index write guard.
3. Apply physical operations to KV prewrite with `MergeMode.OVERWRITE`.
4. Build exactly one target WAL batch carrying writer `W` and sequence `E`.
5. Revalidate the V1 sequence under the LogTablet lock.
6. Append target WAL and update WriterState with its inclusive target last offset.
7. Complete the source response only after that target offset is below target HW.

One V1 KV batch must map to one target WAL batch. Splitting the same `(W,E,T)` into
multiple target batches is forbidden because the second batch would correctly be stale.

### Stale batch: `E <= L(W,T)`

1. Do not decode records.
2. Do not touch KV prewrite.
3. Do not append target WAL.
4. Return `D(W,T)` as the required target acknowledgement offset.
5. Complete only when `D(W,T) < targetHighWatermark`.

If leadership changes before `D(W,T)` commits, delayed completion fails and the source
retries. A locally appended but uncommitted newer batch therefore cannot cause false
success for an older batch.

### Tombstoned partition

A tombstoned V1 request is a successful no-op before WriterState lookup and row
decode. It creates no WriterState and no target WAL. The existing response path uses an
already-committed target offset, or an explicit no-append result, so it never waits for a
new HW advance.

### Revalidation result

Although `kvLock` serializes normal target puts, LogTablet revalidates under its own lock
to cover leadership and recovery transitions. If revalidation reports stale, KV prewrite
is truncated and the request follows the stale path using the stored dominating target
offset.

## Correctness Proof For Unaligned Windows

Window boundaries do not need to match across leaders. `E` orders source WAL prefixes,
not byte-identical request payloads.

Assume target `T` has accepted a batch with sequence `E2` and later receives `E1 <= E2`.
The source batch accepted at `E2` was produced from a start offset `S2`.

There are two cases:

1. `S2` is at or before every source mutation represented by the `E1` request. The
   ordered projection ending at `E2` contains that mutation and every later mutation for
   the same physical key, so its final effect dominates `E1`.
2. `S2` is after a mutation represented by `E1`. A leader may start at `S2` only because
   a committed source snapshot proves all index projections below `S2` were already
   acknowledged. Target `T` therefore already contained that earlier effect before the
   `E2` batch was generated.

Within one source leader, a later window cannot be generated before every target batch of
the previous window is committed. Across source leaders, the restored start is never
after the committed snapshot value. Empty target projections create gaps in target
sequences but contain no mutation for that target. Therefore accepting the greatest `E`
and discarding every smaller or equal `E` cannot omit an uncommitted target effect.

Equal end offsets may have different starts. If old leader `[0,100)` and new leader
`[80,100)` race, either the first batch itself covers the prefix, or the committed source
snapshot already proves the `[0,80)` target effects. First acceptance is safe in both
orders.

Repeated operations inside a fresh overlapping batch are also safe: operations are
absolute OVERWRITE/DELETE operations, are applied in source order, and become visible as
one target WAL batch. Their final state is the source projection through `E`.

Cross-target key movement is not atomic. Before all target batches settle, lookup may see
stale candidates or temporarily miss the new candidate, which is the existing eventual
consistency contract. SYNC visibility waits for every target batch in the window, and the
main-table recheck remains mandatory.

## Source Window Construction

### Mutation groups

The indivisible source unit is:

- one INSERT;
- one DELETE; or
- an adjacent `UPDATE_BEFORE`, `UPDATE_AFTER` pair in the same source
  `LogRecordBatch`.

Indexed main tables already require `ChangelogImage.FULL`. `UPDATE_AFTER` without its
matching `UPDATE_BEFORE`, a non-adjacent pair, or a pair split across source batches is
source WAL corruption for index replication. The replicator fails closed and does not
advance `P_i`.

No pending UPDATE row survives across polls. A window never checkpoints after
`UPDATE_BEFORE`. When a read resumes in the middle of a source `LogRecordBatch`, it skips
records whose offsets are below the requested next-read offset and resumes only at a
mutation-group boundary.

### Window and batch boundaries

For each index, one window builds at most one V1 KV batch per target bucket. Before
adding a complete mutation group, the replicator estimates the resulting encoded size of
every affected target batch:

- if a non-empty target batch would cross the preferred index request payload, end the
  current window before that group;
- if the current window is empty, allow one oversized mutation group only when the exact
  resulting single-bucket PutKv request remains below the Netty hard request limit; and
- if one mutation group exceeds the hard limit, fail deterministically with a record-too-
  large error and metric instead of retrying forever.

The sender may consolidate batches for different target buckets, but it never splits an
individual batch. It computes the serialized request size, not only raw batch bytes, when
enforcing the hard transport limit. The description of
`index.replication.max-request-bytes` must describe a preferred aggregate payload bound,
not claim that one encoded batch can be split.

Once the boundary is selected, every non-empty target builder is finalized with the same
opaque WriterKey and `batchSequence = windowEndOffset`. An empty index projection
advances the index next-read offset without sending a request.

## Physical Index Table Schema

New Index Tables use ordinary row overwrite and physical deletion.

For an unpartitioned main table:

```text
primary key = (indexColumns, basePrimaryKey)
value       = base lookup fields
```

For a partitioned main table:

```text
primary key = (indexColumns, basePrimaryKey, __partition_id)
value       = base lookup fields plus __partition_id
```

Main-table partition keys are already required to be part of the main primary key, so
different live logical partitions do not collide. The appended partition ID protects a
different case: drop and recreation of the same logical partition with a new immutable
partition ID. A delayed DELETE from the old incarnation can address only the old physical
key.

`__partition_id` remains in the value because the existing write guard, query filter, and
native compaction filter consume the value tag. It is intentionally present in both key
and value.

The schema contains no `__source_offset` and no `__index_deleted`. Index Tables have no
version-column property, no `DeleteBehavior.IGNORE`, and no index visibility merger or
filter. IndexSender explicitly sends `MergeMode.OVERWRITE`; delete records carry a null
value.

## Partition Tombstone Readiness And Retirement

An empty tombstone set and an unknown tombstone set are different states. The TabletServer
metadata cache records whether it has received an authoritative baseline for each source
main table, even when that baseline is empty. A partitioned Index Table is not write-ready
until that state is initialized.

When a durable tombstone update arrives:

1. publish the new authoritative tombstone state;
2. schedule work on the owning target replica;
3. acquire `kvLock` using the established lock order;
4. re-read tombstone state;
5. remove WriterState entries whose canonically decoded WriterKeys belong to covered
   partitions; and
6. let the next full WriterState snapshot durably omit those entries.

An apply that acquired `kvLock` first may finish, after which the tombstone makes its row
invisible and eligible for compaction. An apply that acquires it later sees the tombstone
and becomes a no-op. There is no race in which a writer is retired and then recreated for
the tombstoned partition.

Partition IDs are never reused, so no time-based grace period is needed. Unpartitioned
WriterState remains until the Index Table is deleted. Dropping an orphaned Index Table is
allowed even when the main table no longer exists.

## Source Snapshot And WAL Retention

In-memory `P_i` may advance as requests are acknowledged and may be used by SYNC write
completion. It is not a durable WAL deletion bound.

When a source KV snapshot is created, it captures:

```text
kvFlushedOffset
allIndexPushedOffset = min(P_i for every current index)
```

Only after that snapshot is durably committed may LogTablet advance its deletion bound to:

```text
min(kvFlushedOffset, allIndexPushedOffset)
```

If snapshot upload or commit fails, the deletion bound does not move. A later volatile
index acknowledgement cannot change it. This rule applies to local segment deletion,
remote TTL deletion, and lake-related raw-log cleanup.

A raw remote segment `[start,end)` may be deleted only when the normal TTL/lake condition
is true and:

```text
end <= committedSnapshotMinRetainOffset
```

This may retain raw WAL beyond `table.log.ttl`; correctness takes precedence over TTL.

### Source remote replay

If the restored index next-read offset is below local log start, IndexReplicator reads raw
remote WAL. Lake-compacted rows are not a valid input because they cannot reproduce
ordered UPDATE/DELETE history.

The index reader verifies a continuous half-open sequence from requested offset to the
local handoff and then to source HW:

- every next segment or record begins at the expected offset;
- overlap is accepted only by skipping records below the expected offset;
- any gap, corrupt segment, or premature remote end fails closed;
- local handoff starts at exactly the next expected offset; and
- records at or above HW are never processed.

Remote download must not block the fixed shared IndexReplicator read workers. A dedicated
asynchronous fetch stage supplies bounded buffers and preserves per-replicator ordering
and backpressure.

## Target WriterState Durability

V1 WriterState is durable target data. It is never filtered by V0 writer TTL, and V1
tables do not use the clean-shutdown optimization that creates empty writer
snapshots.

### Local recovery coverage

A valid target recovery chooses a committed KV snapshot at `K` and a full WriterState
snapshot at `R`, then proves:

1. the WriterState snapshot belongs to the same target log history and `R` is not after
   the recovery end;
2. raw target WAL is continuous from `K` to recovery end for KV replay;
3. raw target WAL is continuous from `R` to recovery end for WriterState replay; and
4. target truncation is applied consistently to both recovered states.

Equivalently, the selected snapshots plus retained WAL must cover both replay ranges. A
WriterState snapshot below local log start is usable only when the missing interval is
available remotely. A snapshot at or above local log start can cover earlier deleted
writer history directly.

Before deleting target Index Table WAL, the target writes and fsyncs a full internal
WriterState snapshot whose offset dominates the deletion boundary. It retains a valid
covering snapshot and does not delete WAL needed by the selected KV snapshot.

If the latest WriterState snapshot is corrupt, recovery may fall back only when another
valid snapshot plus continuous WAL still proves coverage. Otherwise leadership/recovery
fails. It must not delete the corrupt snapshot, manufacture empty state, and continue as
though every writer were unknown.

### Remote tiering and replica recovery

For an Index Table, a remote WAL segment is not published as recoverable and does not
advance `remoteLogEndOffset` unless its required full WriterState snapshot is uploaded
successfully. Remote target WAL expiration is also clamped by the committed target KV
snapshot replay offset.

A new replica restores the compatible WriterState snapshot and continuous target WAL
before serving or becoming leader. Snapshot restoration failure in
`ReplicaFetcherThread` is fatal for an Index Table; logging and continuing is forbidden.

These are strengthened contracts of generic WriterState and log tiering. They do not add
an Index Table-specific database or column family.

## Ambiguous Target WAL Append

The KV path currently materializes prewrite effects before target WAL append. It cannot
safely truncate prewrite and continue when an append exception may have occurred after
bytes reached the WAL: WriterState could recover `E` while KV lacks the corresponding
effect, causing retry `E` to be discarded as stale.

For internal Index Table writes:

- format, schema, identity, tombstone, and sequence validation happen before prewrite;
- once WAL append is invoked, any exception whose append outcome is not explicitly known
  to be `NOT_APPENDED` invokes the replica fatal-storage path;
- the replica stops serving writes and cannot acknowledge the source;
- recovery reconstructs KV and WriterState from the same retained target history; and
- the source retries after target recovery or leader change.

The conservative implementation may fail-stop on every storage exception escaping
internal append. Availability loss is preferable to continuing from an unprovable state.
A normal retry on the same live replica is not allowed after an uncertain append.

## Compatibility And Activation

This change introduces protocol V1 KV/WAL and WriterState snapshot representations. It
does not alter protocol V0 network bytes, target WAL, in-memory map, snapshot bytes,
expiration, or sequence behavior. Existing V0 format and behavior tests are compatibility
gates.

PutKv API version is a transport capability, not a table protocol selector. The target
table protocol is authoritative:

| Target KV protocol | PutKv v0/v1 + V0 batch | PutKv v2 + V0 batch | PutKv v2 + V1 batch |
|---|---|---|---|
| V0_COMPACT | accept under existing API rules | accept | reject |
| V1_FENCED | reject | reject | accept |

PutKv v0/v1 with a V1 batch is always rejected. A new server peeks only the minimum
length and magic needed to establish that mismatch, then fails before interpreting V1
identity fields, reading rows, or entering KV prewrite. PutKv v2 remains able to carry a
V0 batch so a new client or server does not impose V1 overhead on an ordinary table.

The table protocol, batch magic, target WAL magic, and WriterState snapshot version must
agree during append, follower replication, restart, snapshot restore, and remote recovery.
A mismatch is unsupported format or corruption, never a request to convert the table.

The rejected `__source_offset`/`__index_deleted` form has not been committed or released,
so it receives no migration path. Development Index Tables created by that code must be
recreated.

Old TabletServers cannot safely interpret V1 magic. Before sending V1, IndexSender checks
the ApiVersions of the concrete target TabletServer connection and requires PutKv API v2. A
capability result is keyed by server/gateway identity, expires, and is invalidated when
the target leader or gateway changes. An older target receives no V1 payload and the
batch remains retryable. There is no silent fallback to V0 because that would change the
table's identity and ordering contract.

Rolling upgrade proceeds as follows:

1. new servers continue serving all existing V0 KV tables and existing Log Tables;
2. no V1 Index Table is created or activated until every Coordinator and every
   TabletServer that may host it supports the complete V1 KV/WAL/snapshot contract;
3. once V1 exists, a downgraded server must refuse its WAL, snapshot, replica, or leader
   role rather than partially recover it; and
4. ProduceLog, Log Table batches, and Log Table writer state remain on their existing
   protocol and cannot create a V1 WriterKey.

The Index Table records immutable internal metadata:

```text
table.type = INDEX_TABLE
table.index-meta.main-table-id = <source table id>
table.kv.idempotence-protocol-version = 1
```

Every KV table with no explicit protocol property resolves to version 0. There is no
server-version-based or table-type-based implicit promotion to V1. An explicit KV
idempotence protocol property on a Log Table is invalid. Index Table creation remains
unavailable to users and lake-table behavior is unchanged.

## Performance Model

Expected improvements over the rejected row-version design:

- no per-row source offset or logical-delete bit;
- no read-before-write merge for index UPSERT;
- no logical visibility filter on lookup;
- stale replay exits before row decode and RocksDB access;
- physical DELETE permits normal RocksDB reclamation; and
- no new global lock or source-side dispatch lock.

New costs:

- no additional network, WAL, snapshot, or WriterState-map storage for a V0 table;
- twelve additional header bytes in each V1 request and target WAL batch compared with
  the existing `writerId:int64 + batchSequence:int32` representation;
- one latest WriterState entry per touched `(source writer, target IndexBucket)`;
- full V1 WriterState snapshot serialization and fsync;
- eight additional key bytes for partitioned index rows; and
- remote raw-WAL I/O when source recovery starts below local log start.

Worst-case active state cardinality for one index is:

```text
source TableBucket incarnations * target IndexBucket count
```

Only touched pairs allocate state. Partition retirement bounds historical churn, and V1
stores one batch metadata object rather than V0's five-entry history.

Production capacity evidence must report:

- retained heap by source-writer and target-bucket cardinality;
- WriterState snapshot bytes, snapshot duration, and P99 impact;
- restart and remote-recovery duration and peak memory;
- fresh and stale batch throughput;
- mixed UPSERT/DELETE latency;
- source remote-reader throughput and shared-pool isolation; and
- RocksDB Get count, which must remain zero for pure index UPSERT.

## Failure Scenarios

### Old source leader arrives late

Old leader sends `E=100`. New leader restores an earlier durable snapshot and reaches
`E=150`. If 150 arrives first, 100 is stale. If 100 arrives first, 150 applies the ordered
projection through 150. Both converge.

### New leader uses different boundaries

Old leader sends `[0,100)`. New leader sends `[0,40)`, `[40,90)`, and `[90,130)`.
Target order may be arbitrary. A fresh larger prefix includes every mutation after its
safe start, while mutations before that start are covered by committed source progress.
The greatest accepted end offset yields state through that prefix.

### Target response is lost

Target commits `E=100`, but the source loses the response. Retry 100 is a stale no-op and
waits on the stored target WAL offset. No duplicate KV mutation or WAL append occurs.

### Target batch is locally appended but uncommitted

Target has local `E=200` below HW and receives 150. The stale response waits on the WAL
offset for 200. If leadership is lost, it fails rather than claiming 150 is durable.

### Append outcome is uncertain

An exception occurs after prewrite while target WAL append may have landed. The replica
fail-stops. It does not truncate prewrite and serve a stale retry. Recovery establishes a
provable KV/WriterState pair before traffic resumes.

### Source snapshot commit fails

Volatile index progress reaches 500, but the source snapshot containing 500 fails to
commit. Local and remote raw WAL deletion remains at the previous committed bound. A new
source leader can replay safely.

### Target WriterState snapshot is corrupt

Recovery uses an older snapshot only if continuous target WAL bridges it to recovery end.
If required WAL is gone, the target refuses leadership instead of treating delayed old
requests as new.

### Partition drop and recreation

Old pid 10 and new pid 20 use different physical keys and different canonical
WriterKeys. Once pid 10 is tombstoned, its delayed UPSERTs are no-ops and its DELETEs can
only address pid-10 keys. WriterState for pid 10 is retired; compaction removes old rows.

## Test Strategy

### Protocol selection and V0 compatibility

- Round-trip minimum, `Integer.MAX_VALUE`, `Integer.MAX_VALUE + 1`, and `Long.MAX_VALUE`
  V1 sequences.
- Read and write existing V0 KV/WAL batches and WriterState snapshots byte-for-byte.
- Preserve the V0 `Map<Long,...>`, exact-next, duplicate history, expiration, and int
  rollover behavior.
- Exercise every PutKv API/table protocol/batch magic combination in the compatibility
  matrix and assert mismatches fail before row decode and prewrite.
- Prove a missing property on an existing KV table resolves to V0, an explicit property
  on a Log Table is rejected, and the protocol cannot be altered.
- Prove ProduceLog and Log Table WriterState behavior are unchanged.
- Prove the common PutKv, WAL, and WriterState paths can round-trip a V1 WriterKey
  without loading or depending on any index class.

### Writer identity

- Exhaustively test partitioned and unpartitioned canonical encodings at boundary values.
- Reject invalid buckets, negative partition IDs, non-zero reserved bits, and non-canonical
  unpartitioned keys.
- Prove every valid source bucket round-trips through the 128-bit WriterKey exactly.
- Prove a V0 identity and V1 WriterKey cannot coexist in one immutable table protocol.
- Prove two Index Tables may reuse the same WriterKey without sharing target state.
- Prove a stable bucket ID survives source bucket-count growth without reinterpretation.
- Prove a rescale that renumbers source WAL domains requires a new Index Table generation.

### Target state machine

- Accept sparse `100 -> 500 -> Integer.MAX_VALUE + 1` sequences.
- Treat duplicate 500 and delayed 100 as no-op without decode, KV change, or WAL append.
- Hold 500 below HW and prove stale 100 cannot acknowledge early.
- Exercise fresh-to-stale revalidation under the target locks.
- Verify IndexSender uses `MergeMode.OVERWRITE` and required acks `-1`.

### Window construction

- Assert complete, adjacent UPDATE pairs in one source batch.
- Reject missing, non-adjacent, and cross-batch UPDATE halves without progress.
- Resume inside a source batch and assert records below next-read offset are skipped.
- Cut windows at encoded output limits without splitting a mutation group.
- Assert one `(W,E,T)` produces exactly one target KV and WAL batch.
- Fail one hard-limit oversized mutation deterministically without retry spin.

### Source durability and remote replay

- Inject source snapshot upload and commit failures and prove neither local nor remote
  deletion advances from volatile progress.
- Verify remote segments use exclusive end offsets and deletion requires
  `end <= committedSnapshotMinRetainOffset`.
- Recover from remote raw WAL across exact handoff, overlap, gap, corruption, TTL, and CDC
  tiering cases.
- Prove remote reads do not block unrelated shared IndexReplicator workers.

### Target durability and ambiguous failures

- Cover clean and unclean restart, target truncation, local segment deletion, remote
  tiering, snapshot transfer, and new-replica recovery.
- Advance time past V0 writer expiration and prove V1 state remains.
- Corrupt or remove snapshots with and without sufficient WAL; assert fallback only with
  proven coverage.
- Inject failures before prewrite, after prewrite, before append, during append, after
  append, after WriterState update, and before/after HW.
- Assert every uncertain append fail-stops and recovery plus retry converges.

### Partition lifecycle

- Distinguish unknown tombstone baseline from authoritative empty baseline.
- Race tombstone publication and target apply in both lock orders.
- Drop pid 10, recreate as pid 20, then release delayed pid-10 UPSERT and DELETE.
- Reject key/value/WriterKey-derived pid mismatch without changing KV, WAL, or WriterState.
- Verify writer retirement and native compaction cleanup.
- Verify orphaned Index Table drop when the main table is absent.

### Model-based failover

Generate source mutation histories and randomly duplicate, delay, reorder, lose responses,
change window boundaries, fail source leaders, fail target leaders, truncate target WAL,
restart replicas, and drop/recreate partitions. After delivery settles, the target must
equal a reference projection of committed source WAL. The model separately asserts that
no stale request changes target KV or WAL.

### Performance

- Compare physical overwrite, rejected row-version behavior, and offset fencing.
- Measure fresh and stale paths independently.
- Scale to the supported source-writer by target-bucket topology.
- Record snapshot, recovery, heap, network, and remote-read curves.

All asynchronous tests use latches, deterministic fault hooks, or condition waiting.
Fixed sleep is not synchronization.

## Audit Verdict

The design is correct under the stated invariants:

- canonical opaque WriterKey establishes the source offset comparison domain without
  overflow, collision, assignment-layer intrusion, or index semantics in PutKv;
- immutable table protocol selection and the API/magic matrix preserve V0 storage
  efficiency and keep ProduceLog and Log Tables outside V1;
- absolute source WAL end offsets remain valid when old and new leaders choose different
  windows;
- target WriterState prevents terminal stale writes while retaining physical KV updates;
- committed snapshot retention prevents source replay gaps under TTL and tiering;
- joint target recovery prevents stale-drop decisions from outrunning recoverable KV;
- pid-qualified keys and serialized tombstones close drop/recreate races; and
- fail-stop handling closes the ambiguous WAL append hole.

The design is not production-proven until the implementation passes the specified
failover, corruption, retention, model-based, and capacity tests. A failure to implement
any of the six requirements listed in Status invalidates the proof rather than becoming
accepted technical debt.

## Alternatives Rejected

### Per-row source offset and logical delete

Rejected because it adds storage and lookup overhead, requires read-before-write merge,
retains logical tombstones, and mixes partition-incarnation offset domains.

### Source leader epoch

Rejected because a higher epoch may replay from an older committed source snapshot. Epoch
order can therefore accept an older source prefix over a newer one.

### Contiguous generated batch sequence

Rejected because failover window boundaries are intentionally not stable. Persisting a
source-side counter and its per-target relationship would create a second recovery system.
The source WAL exclusive end offset is already the required stable order.

### Protocol V0 alone

Rejected for index push because V0 sequence is 32-bit, requires exact next, expires, and
retains only a small duplicate window. V0 remains unchanged and preferred for ordinary
KV tables.

### Independent idempotence knobs or one behavior-only mode

Rejected because writer identity width, sequence width, validation, duplicate history,
expiration, KV/WAL magic, and snapshot serde form one compatibility contract. Independent
options permit invalid combinations, while names such as CONTIGUOUS or MONOTONIC describe
only one dimension. The table selects a complete versioned protocol instead.

### Source bucket compressed into a long writer ID

Rejected because the valid `(partitionId, bucketId)` domain cannot be injected into a
non-negative signed 64-bit value without either excluding valid identities or allowing
collisions. Exact arithmetic changes silent corruption into deterministic rejection but
does not remove the representation limit.

### Variable-length opaque writer key

Rejected because the current exact identity fits in 128 bits. Variable length would add
length limits, ownership and copying rules, denial-of-service surface, variable snapshot
encoding, and avoidable allocation without improving the current correctness domain.

### Dedicated IndexPush RPC

Rejected because it would duplicate PutKv routing, batching, retry, timeout, delayed-HW
acknowledgement, error mapping, metrics, and version negotiation while target WAL and
WriterState would still require the wider identity. The generic V1 WriterKey adds
the V1 capability once without putting index fields in PutKv.

### ThreadLocal or sender lock

Rejected because it cannot order already-sent requests from different source leaders.
The target must persist the ordering decision.

### Separate IndexBucket apply-offset store

Rejected because WriterState already owns target WAL ordering, replication, truncation,
snapshot, and delayed-HW metadata. A separate store would duplicate those failure modes.
