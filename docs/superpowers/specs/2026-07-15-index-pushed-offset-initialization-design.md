# Index Pushed Offset Initialization and Recovery Design

## Status

The design was accepted in discussion on 2026-07-15 and is awaiting review of this
written form before implementation planning.

## Problem

For a main table that declares secondary indexes, `allIndexPushedOffset` is the next source WAL
offset that every index still needs to read from. A newly created source WAL starts at offset `0`,
so the correct initial value is `0`: no source record has been copied, and offset `0` is the next
record to read.

The current implementation instead initializes the offset to `-1`. This creates the following
failure path:

1. A main-table replica becomes leader and opens its KV tablet.
2. The periodic KV Snapshot manager starts before `ReplicaIndexController` starts or defers the
   `IndexReplicator`.
3. A Snapshot taken in this interval converts the negative `allIndexPushedOffset` to a missing
   `indexPushedOffset` field.
4. Generic KV Snapshot retention interprets a missing field as "no secondary-index retention
   requirement" and uses the KV flushed log offset.
5. Source WAL that has not been copied to the Index Tables may consequently be deleted.
6. When Index Table metadata becomes available, `IndexReplicator` converts its negative initial
   offset to the current source `logStartOffset`, silently skipping the deleted source WAL.

This path is possible during normal metadata propagation. It does not require a corrupted
Snapshot or an exceptional leader transition.

## Goals

- Give every main table that declares secondary indexes explicit, non-negative index replication
  progress from construction onward.
- Prevent a KV Snapshot from releasing source WAL while Index Table metadata is unavailable.
- Restore index replication progress only from an explicit value recorded in the main-table KV
  Snapshot.
- Reject inconsistent Snapshot metadata instead of choosing a later source WAL offset.
- Preserve existing Snapshot behavior for tables that do not declare secondary indexes.
- Make the invalid negative-offset path impossible inside `IndexReplicator`.

## Non-Goals

- Online `ADD INDEX` or `DROP INDEX`.
- Rebuilding an Index Table by scanning current main-table KV rows.
- Querying Index Table WriterState to reconstruct source progress.
- A new leader-readiness state, cross-table coordination mechanism, RPC, configuration, or
  metadata node.
- A new KV Snapshot format version. The existing optional `indexPushedOffset` field is sufficient.
- Changing the normal asynchronous catch-up behavior after a valid index replication offset has
  been restored.

## Required Invariants

### Main Table With Secondary Indexes

1. `allIndexPushedOffset` is always non-negative.
2. Before any source WAL record is copied, `allIndexPushedOffset == 0`.
3. If the table has at least one SYNC index, `syncIndexPushedOffset` is also initialized to `0`.
4. Every newly committed main-table KV Snapshot records a non-negative `indexPushedOffset`.
5. The committed Snapshot may release source WAL only up to:

   ```text
   min(kvFlushedOffset, indexPushedOffset)
   ```

6. `IndexReplicator.initialOffset` is non-negative and is used without substitution.
7. Missing Index Table metadata may defer `IndexReplicator`, but it cannot change or invalidate
   the source offset retained by the main-table KV Snapshot.

### Table Without Secondary Indexes

1. Its KV Snapshot does not need to record `indexPushedOffset`.
2. A missing field continues to mean that WAL retention is governed only by the KV flushed log
   offset.
3. Existing Snapshot metadata remains valid and requires no migration.

## Initialization

`Replica` determines from its immutable `TableInfo` whether the table schema declares secondary
indexes. Schema evolution is already rejected for tables with secondary indexes, so this decision
cannot change during the replica lifetime.

The initial values are:

| Table schema | `allIndexPushedOffset` | `syncIndexPushedOffset` |
| --- | ---: | ---: |
| No secondary index | not applicable | not applicable |
| ASYNC indexes only | `0` | not applicable |
| At least one SYNC index | `0` | `0` |

The existing representation for "not applicable" may remain unchanged for code paths that do
not use index progress. It must not be used for a main table that declares secondary indexes.

`Replica.augmentTabletState()` decides whether to include `indexPushedOffset` from the table
schema, not by testing whether the current numeric value is non-negative. This separates two
different meanings that the current implementation conflates:

- the table has no secondary indexes, so the field is unnecessary;
- the table has secondary indexes whose next source offset is `0`.

## Snapshot Recovery

Recovery follows the table schema and Snapshot contents:

| Table schema | Main-table KV Snapshot | Result |
| --- | --- | --- |
| Has secondary indexes | No Snapshot | Start main KV and index copying from source WAL offset `0` |
| Has secondary indexes | Non-negative `indexPushedOffset` | Restore that exact conservative offset |
| Has secondary indexes | Field missing | Reject leader KV recovery |
| Has secondary indexes | Negative value | Reject leader KV recovery |
| No secondary indexes | Field missing | Preserve existing KV recovery behavior |

Validation occurs before `LogTablet.updateMinRetainOffset()` and before the KV Snapshot manager is
published. A validation failure therefore cannot advance WAL retention, publish a ready KV leader,
or start an `IndexReplicator`.

The failure is reported as an invalid main-table KV Snapshot for a schema that declares secondary
indexes. The existing leader-initialization cleanup must leave the KV tablet, Snapshot manager,
and IndexReplicator unpublished.

### Why Missing Progress Is Rejected

Replaying from source WAL offset `0` is logically safe only if the complete required source WAL is
available. Target WriterState makes duplicate replay and changed window boundaries safe, but it
does not prove that every source WAL offset from `0` up to the required recovery position is still
available.

The current background failure path is insufficient for this special recovery case. If replay
later discovers a source WAL gap, `ReplicaIndexController` enters `FAILED`, while the main table
may remain available. That does not prevent an incomplete Index Table from remaining visible.
Making background replay complete would require additional leader-readiness or failure-propagation
behavior, which is outside the minimal correction.

There is no legitimate released compatibility case that requires guessing here. V2 has not been
released, existing tables could not declare these indexes, and online `ADD INDEX` is unsupported.
Consequently, a main-table schema that declares secondary indexes together with a Snapshot that
does not record `indexPushedOffset` is inconsistent state and must be rejected.

## IndexReplicator Contract

`IndexReplicator` accepts only `initialOffset >= 0`. Its constructor rejects a negative value, and
the polling path removes the fallback that assigns `sourceReader.logStartOffset()`.

This gives the component one simple contract:

```text
next source WAL offset to read = initialOffset or the last acknowledged window end offset
```

`logStartOffset` remains an observed storage boundary used by `IndexSourceReader` when selecting
local or raw remote WAL. It is never a substitute for index replication progress.

If a restored non-negative offset is below the local source WAL start, `IndexSourceReader` follows
the existing raw remote WAL path and requires exact continuity at the remote-to-local handoff. A
proven gap continues to fail rather than moving the offset forward.

## Metadata Propagation

Delayed Index Table metadata keeps the existing `ReplicaIndexController.State.DEFERRED` behavior.
No IndexReplicator is created until the complete Index Table metadata needed by `IndexSpecFactory`
is visible.

While deferred:

- `allIndexPushedOffset` remains `0` or the exact offset restored from a valid Snapshot;
- source writes may advance the KV flushed log offset;
- every main-table KV Snapshot still records the unchanged index replication offset; and
- committed Snapshot retention continues to preserve all source WAL needed by the later
  IndexReplicator.

When metadata becomes available, retry creates `IndexReplicator` with that exact offset. There is
no dependency on whether a Snapshot ran before or after metadata propagation.

## Code Boundaries

### `Replica`

- Record whether the immutable table schema declares secondary indexes.
- Initialize index progress according to the table schema.
- Include `indexPushedOffset` in `TabletState` according to the schema rather than a numeric
  sentinel.
- Validate restored Snapshot progress before changing WAL retention.
- Keep progress advancement monotonic after initialization.

### `IndexReplicator`

- Reject a negative constructor offset.
- Remove the negative-offset fallback in `pollLocked()`.
- Keep normal window extraction, acknowledgement, retry, and progress notification unchanged.

### Snapshot Classes

`TabletState`, `CompletedSnapshot`, and their JSON compatibility behavior do not need a format
change. Their nullable field remains necessary for tables without secondary indexes. Validation
that depends on the table schema belongs in `Replica`, where both the schema and Snapshot are
available.

### FIP

The FIP must state that:

- `allIndexPushedOffset` is an exclusive next-read source WAL offset;
- its initial value is `0` for a main table that declares secondary indexes;
- every KV Snapshot for such a table records a non-negative value; and
- missing progress is not replaced by `logStartOffset`.

## Test Design

### Initialization and Metadata Delay

Use an indexed main-table replica whose derived Index Table metadata is deliberately absent from
the local metadata cache. Promote the main-table replica and assert:

- `ReplicaIndexController` is `DEFERRED`;
- no `IndexReplicator` exists;
- `allIndexPushedOffset == 0`; and
- for a SYNC index, `syncIndexPushedOffset == 0`.

Publish the Index Table metadata, retry initialization, and prove that the new replicator starts
from source WAL offset `0`.

### Snapshot Before IndexReplicator

Use the existing Snapshot-initialization test hook at the point immediately after the periodic KV
Snapshot manager starts and before `onLeaderKvReady()` is invoked. Trigger and complete a real
Snapshot while index replication has not started. Advance the main KV flushed log offset above
zero first, then assert exact values:

- committed `indexPushedOffset == 0`;
- committed `logOffset > 0`;
- committed `getMinRetainLogOffset() == 0`; and
- `LogTablet.getMinRetainOffset() == 0` after commit.

This test directly covers the lifecycle interval that caused the defect. It must use condition
waiting or manually controlled executors, not a fixed sleep.

### Restore a Valid Offset

Create a main-table KV Snapshot with a positive `indexPushedOffset`, recreate or re-promote the
source replica, and assert that both replica progress and the IndexReplicator next-read position
equal that exact value before new source work is processed.

### Reject Missing or Negative Progress

Provide otherwise valid main-table KV Snapshot data whose metadata omits `indexPushedOffset`, and
repeat with a negative value. For each case assert:

- leader recovery returns an error that identifies the invalid index replication progress;
- the replica is not published as leader;
- no KV tablet remains registered;
- no ready Snapshot manager remains; and
- no IndexReplicator is installed.

### Preserve Tables Without Indexes

Restore a table without secondary indexes from existing Snapshot metadata that omits
`indexPushedOffset`. Assert successful KV recovery and that its minimum retained source WAL offset
continues to equal the Snapshot KV log offset.

### Reject the Old IndexReplicator Path

Add a focused constructor test asserting that a negative `initialOffset` is rejected. Existing
poll tests continue to assert exact read offsets for `0` and positive starts. Tests that previously
described `-1` as the valid pre-write state of an indexed main table are changed to assert `0`.
Tests for tables without indexes retain their existing no-index behavior.

## Rejected Alternatives

### Replace Missing Progress With Source `logStartOffset`

Rejected because storage retention may already have moved `logStartOffset` beyond records that
were never copied to the Index Tables. This is the silent data-loss path being removed.

### Replay From Zero in the Background

Rejected for missing Snapshot progress because the existing background terminal-failure state
does not prevent the main table and incomplete Index Tables from remaining available after a
source WAL gap is discovered.

### Start IndexReplicator Before the Snapshot Manager

Rejected because Index Table metadata may still be unavailable. Reordering components does not
define the index replication offset and leaves the same race in a different location.

### Rebuild From Current Main-Table KV Rows

Rejected because scanning current rows can add current index entries but cannot safely remove all
historical stale entries without replacing or versioning the Index Tables. That is a separate
index-build design.

## Completion Criteria

The change is complete when all of the following are true:

- an indexed main-table replica cannot expose a negative `allIndexPushedOffset`;
- a Snapshot taken before IndexReplicator startup retains source WAL from offset `0`;
- missing or negative Snapshot progress prevents leader publication for an indexed main table;
- no production code converts missing index replication progress to `logStartOffset`;
- no-index tables preserve their existing Snapshot behavior;
- the focused unit and integration tests above pass without fixed sleeps; and
- the FIP describes the same initialization and recovery contract as the implementation.
