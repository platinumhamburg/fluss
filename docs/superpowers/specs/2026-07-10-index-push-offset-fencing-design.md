# Offset-Fenced Index Push Design

## Status

This design replaces the uncommitted per-row `__source_offset` and `__index_deleted`
approach. The protocol was approved after two rounds of adversarial review. Approval of
the protocol does not imply that the current implementation already satisfies it.

## Context

Global secondary index v2 derives index mutations from each source KV `TableBucket` WAL
and sends them asynchronously to internal Index Table buckets. Delivery is at least once.
Within one `IndexReplicator`, a target bucket is serialized, but source leader failover can
leave an old request in the network while the new leader replays the same source WAL.

Ordinary KV overwrite semantics make identical retries harmless. They do not make
different operations commutative. For example, a delayed old UPSERT can arrive after a
new DELETE, or a delayed old DELETE can arrive after a new UPSERT. Without an ordering
fence, the delayed request can become the permanent target state.

The rejected implementation addressed this by storing a source offset and a logical
delete marker in every index row. That prevents stale row updates, but it also introduces:

- a `BIGINT` and `BOOLEAN` in every Index Table row;
- logical tombstones that remain until a later compaction policy removes them;
- a read-before-write merge for every UPSERT;
- an extra lookup visibility filter; and
- incorrect offset comparison across partition incarnations, whose source WAL offsets
  belong to different `TableBucket` sequence domains.

The selected design moves ordering from every index row to the existing target WAL
writer-state layer. It reuses the generic writer-state persistence, recovery, and delayed
HW acknowledgement machinery, but introduces a separate internal sequence policy. It
does not pretend that the current public `int + last + 1` policy can be reused unchanged.

## Goals

- Prevent an old source leader request from becoming the terminal index state.
- Preserve at-least-once, eventually consistent index replication; exactly once is not a
  requirement.
- Use ordinary physical KV UPSERT and DELETE operations.
- Remove `__source_offset`, `__index_deleted`, the versioned row merger, and logical index
  deletion filtering.
- Keep ordering state proportional to source-to-target replication channels instead of
  index rows.
- Preserve source WAL retention and failover recovery through the existing
  `indexPushedOffset` contract.
- Isolate partition incarnations without weakening the existing partition tombstone
  write, query, and compaction defenses.
- Keep the public KV idempotence protocol unchanged.

## Non-Goals

- Do not provide exactly-once index replication.
- Do not add an Index Table-specific `perSourceApplyOffset` store or RocksDB column family.
- Do not add a source leader epoch to index ordering.
- Do not make multiple target IndexBucket mutations atomic with each other.
- Do not remove `__partition_id` from the Index Table value.
- Do not support in-place migration from the uncommitted versioned Index Table format.
- Do not relax the main-table lookup recheck that protects query correctness from stale
  index candidates.

## Why Some Target State Is Necessary

Consider two non-commutative operations on the same index key:

```text
old source leader: UPSERT K=A
new source leader: DELETE K
arrival order:      DELETE, then delayed UPSERT
```

With only ordinary PutKv, the target cannot distinguish the delayed UPSERT from a valid
new write. One of the following is therefore necessary:

1. globally serialize both source leaders;
2. persist an ordering fence at the target;
3. make every row operation commutative with per-row versions; or
4. permit corruption and rely on a repair system.

The first option is not crash-safe for already delivered RPCs. The third is the rejected
per-row design. The fourth is outside the v2 contract. A channel-level target ordering
fence is therefore the smallest complete choice.

## Terminology

- `S`: one source main-table `TableBucket` incarnation.
- `I`: one secondary index definition and its physical Index Table.
- `B`: one target bucket of that Index Table.
- `W(S)`: the stable internal writer ID assigned to `S`.
- `F`: the source durable replay floor for `I`.
- `E`: an IndexWindow exclusive source WAL end offset.
- `M(S,I,B,F,E)`: the ordered projection of source WAL mutations in `[F,E)` that target
  `B`.
- `L(W,B)`: the greatest accepted source WAL end offset for writer `W` at target `B`.
- `D(W,B)`: the target WAL offset whose commit dominates `L(W,B)`.

Source WAL offsets are comparable only within one source `TableBucket` incarnation. The
stable writer ID identifies exactly that sequence domain.

## Required Invariants

### Source invariants

1. Index replication reads only source WAL below the source high watermark.
2. Within one source leader, each index has at most one unacknowledged IndexWindow.
3. A later window is produced only after every target batch of the previous window has a
   committed acknowledgement.
4. Source progress advances only after every target batch in the window is committed.
5. A recovered leader starts at or before the durable all-target-acknowledged replay
   floor. It may replay an older prefix but must never start after that floor.
6. Source WAL retention is bounded by the same durable index replay floor.
7. A batch preserves source WAL mutation order, including multiple mutations of the same
   index key.

The one-window invariant is per leader. Old and new leaders may overlap. Correctness does
not depend on globally preventing that overlap.

### Target invariants

1. `W(S)` is globally unique, stable across source leader failover, and never reused.
2. An internal frontier is not expired while its source `TableBucket` is alive.
3. A fresh mutation batch and its frontier metadata are represented by the same target
   WAL recovery boundary.
4. A stale acknowledgement is delayed until the target WAL offset that dominates it has
   reached target HW.
5. Target WAL truncation rolls the frontier back consistently with the data effects.
6. WAL segment deletion cannot remove the last recoverable copy of an internal frontier.
7. One physical index key incarnation is owned by one source writer.
8. The internal sequence policy is accepted only by a compatible internal Index Table.

## Stable Writer Identity

Each source table or partition assignment stores an `indexWriterIdBase` allocated from the
existing global writer ID sequence. The assignment reserves an atomic range:

```text
span = maxBucketId + 1
[base, base + span)
writerId(sourceBucket) = base + sourceBucketId
```

Allocation uses the existing ZK counter's atomic `getAndAdd(span)` capability, not repeated
single-ID allocation. Requirements:

- reject negative bucket IDs and range overflow;
- allow an allocated range to leak if assignment creation fails;
- never reclaim or reuse a leaked or deleted range;
- preserve the base through replica reassignment;
- persist the optional base in versioned `TableAssignment` and `PartitionAssignment`
  serde; and
- do not activate the feature while an old Coordinator that can discard the new field may
  still become leader.

All indexes of the source bucket may share the writer ID because their target Index Tables
have independent writer-state managers.

## Internal Batch Format

Introduce a new KV batch magic for index replication. Its internal metadata contains:

```text
writerId:             int64
sourceEndOffset:      int64
sourcePartitionId:    int64 or explicit NO_PARTITION marker
```

The corresponding target WAL batch format persists the same identity, source sequence,
and partition incarnation. The ordinary client format remains `writerId + int32
batchSequence` and retains exact-next validation.

The internal format is accepted only when all are true:

- the target metadata marks the table as an internal Index Table;
- the Index Table format version declares offset-fenced replication support;
- the request targets the current physical Index Table ID; and
- the cluster feature level confirms that every possible target understands the new KV
  and WAL magic.

All incompatible combinations fail before KV prewrite. They must never silently fall
back to non-idempotent PutKv or ordinary contiguous validation.

## Target State Machine

Each target IndexBucket stores the following in the generic writer-state manager:

```text
InternalWriterState {
    writerId
    lastSourceEndOffset
    dominatingTargetWalOffset
    sourcePartitionId
}
```

The internal state uses a distinct monotonic-source-offset policy. It does not use the
ordinary five-batch duplicate window.

### Fresh batch: `E > L(W,B)`

1. Validate internal table format, writer metadata, and partition identity.
2. Under the serialized KV apply path, recheck the current internal frontier.
3. Apply all encoded mutations in source WAL order to the KV prewrite buffer.
4. Append one target WAL batch containing both mutations and internal writer metadata.
5. Advance the in-memory frontier to `(E, targetWalOffset)`.
6. Return success only after `targetWalOffset` reaches target HW.

The target batch may start before the current frontier because a recovered source leader
can replay an overlapping prefix. Applying the complete ordered projection through the
higher end offset still yields the source state at that higher prefix.

### Stale batch: `E <= L(W,B)`

1. Detect stale state before decoding records or modifying KV prewrite state.
2. Do not mutate KV.
3. Do not append another target WAL batch.
4. Return the existing `D(W,B)` as the required acknowledgement offset.
5. Complete stale-success only after `D(W,B)` reaches target HW.

If the target loses leadership before that offset commits, the request fails and the
source retries against the next target leader.

### Unknown state

An unknown active writer may submit any non-negative first source end offset because
target routing is sparse and empty source windows do not create target batches. Unknown
state is safe only when recovery has proved there is no older retained frontier. Missing
or corrupt state with already-deleted WAL history is a fatal recovery error, not a new
writer.

## Prefix-Dominance Proof

Assume a target accepts a batch ending at `E2`, then later receives `E1 <= E2`.

The accepted `E2` batch was generated in one of two ways:

1. its replay started at a durable floor after all effects below that floor were committed
   to every relevant target; or
2. its replay started before the mutation represented by `E1`, so the ordered projection
   leading to `E2` included that mutation or a later mutation that superseded it.

The source cannot legally generate the later window by skipping an unacknowledged earlier
window: it advances the window only after all target batches receive committed ACKs. A new
leader cannot skip it either because its durable replay floor is conservative. Therefore
discarding `E1` cannot discard an effect that is absent from the durable prefix represented
by `E2`.

Window boundaries need not be deterministic across leaders. `E` identifies a source WAL
prefix, not a byte-identical payload. An old `[0,100)` batch and new `[0,40)`, `[40,90)`,
`[90,130)` batches can coexist safely under the same prefix-dominance invariants.

## Why Source Epoch Is Rejected

An epoch-first order `(leaderEpoch, localSequence)` can accept a higher epoch that replays
from an older snapshot floor. For example:

```text
target committed: epoch 7, source offset 100
new source floor: source offset 0
new request:      epoch 8, replay [0,10)
```

Epoch ordering accepts the older source state and permanently fences epoch 7, even though
epoch 7 represented a later source prefix. It also requires source-side per-target counters,
counter preservation across controller rebuilds, and wrap handling. Source WAL offset is
already the stable total order and remains the sole sequence domain.

## Partition Incarnation Isolation

For partitioned source tables, the physical Index Table primary key is:

```text
(indexColumns, basePrimaryKey, __partition_id)
```

The Index Table value continues to contain `__partition_id` for the existing write,
lookup, and native compaction tombstone filters. The appended key component does not alter
prefix routing because it follows the index columns and base primary key.

Every physical UPSERT and DELETE key includes the source `TableBucket` partition ID. The
target verifies that:

- batch-header partition ID;
- every UPSERT value partition ID; and
- every DELETE key partition ID

agree. A mismatch fails before writer-state or KV mutation.

This guarantees that a delayed DELETE from an old partition incarnation cannot delete a
new incarnation with the same logical partition and primary key.

## Tombstone Readiness And Writer Retirement

Partition tombstone write, query, and compaction filters remain unchanged. They solve
visibility and garbage collection; the writer frontier solves ordering while the partition
is alive.

Before accepting partitioned internal writes, an Index Table leader must have loaded an
authoritative initial tombstone baseline for the source main table. An unknown cache state
returns a retryable not-ready result rather than treating it as an empty tombstone.

When a durable tombstone covers a partition ID:

1. serialize retirement with internal apply on the target IndexBucket;
2. reject all future batches for that partition before writer-state lookup and KV prewrite;
3. return tombstone-success without creating a new writer state;
4. remove writer states associated with that partition; and
5. ensure the next full writer snapshot no longer contains them.

After retirement, a delayed UPSERT is rejected by the tombstone gate. A delayed physical
DELETE can address only the old pid-qualified key. The partition tombstone floor is
monotonic and partition IDs are never reused, so retirement does not need a time-based
network grace period.

Unpartitioned writer state remains until its Index Table is deleted. A failed main-table
cascade does not make the state unsafe; the internal Index Table remains independently
droppable.

## Writer-State Durability

Internal frontiers do not use ordinary writer TTL. Full writer snapshots carry
`lastSourceEndOffset`, `dominatingTargetWalOffset`, and `sourcePartitionId`.

Before deleting any WAL segment that may be required to rebuild an internal frontier, the
target must:

1. write and fsync a full writer-state snapshot at an offset that dominates the segment;
2. verify that the snapshot contains all live internal frontiers;
3. retain at least one valid covering snapshot; and only then
4. delete old WAL segments and older snapshots.

Recovery loads the latest valid snapshot within the retained target log range, replays WAL
to the local log end, and truncates writer state together with target WAL truncation. State
may temporarily include a locally appended but not-yet-committed batch; its stored target
WAL offset prevents a stale request from being acknowledged before that batch reaches HW.

For internal Index Tables:

- disable the clean-shutdown optimization that manufactures empty snapshots when no writer
  snapshot is present;
- fail closed when retained WAL starts after a missing or corrupt required frontier;
- preserve full internal entries when ordinary writers expire; and
- verify remote-tier recovery and snapshot transfer with the same contract.

This strengthens the generic writer-state snapshot contract. It does not create a separate
Index Table state database.

## Ambiguous WAL Append Failures

The current KV path writes the prewrite buffer before target WAL append and has an error
path that may roll back prewrite even when WAL append already occurred. For internal
replication, any exception for which WAL append success is uncertain is fatal to the local
replica:

1. do not continue serving PutKv on that replica;
2. invoke the fatal storage path;
3. recover KV prewrite effects and writer frontier from committed target WAL; and
4. let the source retry after leadership recovery.

The implementation must not convert an uncertain append into either stale-success or a
normal retry on the same live replica.

## Source Processing

`IndexReplicator` continues to read HW-committed WAL and maintain one in-flight window per
index. A window builds at most one encoded KV batch for each target IndexBucket. Mutations
inside that batch preserve source record order.

An empty projection advances the source progress without sending a target batch. The next
batch for that target can therefore have an arbitrary source-offset gap, which is valid for
the internal monotonic policy.

The source snapshot persists the conservative minimum all-index pushed offset. Restoring
every index from that minimum may replay more data but cannot skip data. Snapshot retention
continues to use the minimum of source data progress and index pushed progress.

## Index Table Schema

New internal Index Tables use ordinary row merging and physical deletion:

- non-partitioned key: `(indexColumns, basePrimaryKey)`;
- partitioned key: `(indexColumns, basePrimaryKey, __partition_id)`;
- value: base lookup fields plus `__partition_id` for partitioned tables;
- no `__source_offset`;
- no `__index_deleted`;
- no version column property;
- no `DeleteBehavior.IGNORE`; and
- no logical index visibility filter.

The partition tombstone value tag and its KV format version remain required.

## Compatibility And Rollout

The rejected versioned format exists only in the current uncommitted worktree and has never
appeared in repository history. No supported cluster can therefore contain it. The change
removes those uncommitted artifacts directly; it does not add shadow-table bootstrap or
format migration machinery.

Rolling activation still requires a cluster feature gate:

1. upgrade all Coordinators and TabletServers;
2. confirm support for assignment writer ranges and the new KV/WAL magic;
3. enable offset-fenced Index Table creation and push; and
4. fail closed on any unsupported node or table format.

Development tables created from the rejected worktree are disposable and must be recreated.

## Performance Model

Expected improvements over the rejected per-row version design:

- no per-row source offset or logical delete marker;
- ordinary UPSERT returns to the blind-write fast path;
- stale replay can short-circuit before row decoding and RocksDB access;
- no logical deletion filter on lookup; and
- physical DELETE permits normal RocksDB reclamation.

New costs:

- one writer state per `(source writer, touched target IndexBucket, index)`;
- full writer-state snapshot serialization and fsync;
- one writer-state lookup per target batch; and
- 8 additional key bytes for partitioned Index Tables because pid remains in the value too.

Worst-case state cardinality is:

```text
sum(index) sum(targetBucket) distinctTouchedSourceWriters
```

It approaches `sourceBuckets * sum(indexBucketCounts)` when every source bucket touches
every target bucket. Partition retirement bounds churn-related state, but active-source
cardinality remains workload dependent.

Production readiness requires benchmark evidence at the supported maximum topology. The
benchmark must report retained heap, writer snapshot bytes, snapshot pause/P99 impact,
recovery time and peak memory, fresh/stale throughput, mixed UPSERT/DELETE latency, and
RocksDB Get count. Pure UPSERT must perform zero RocksDB Gets. Production-ready status is
withheld until capacity and latency limits are reviewed and recorded with the benchmark
results.

## Failure Scenarios

### Source leader overlap

Old leader sends `E100`; new leader restores an older floor and sends `E40`, then `E150`.
Whichever target request arrives first, the accepted maximum prefix dominates the final
state. A late `E100` is stale after `E150` and cannot mutate KV.

### Lost target acknowledgement

The target commits `E100`, but the response is lost. Retry `E100` short-circuits, waits for
the stored target WAL offset to be in HW, and returns success without another mutation.

### Uncommitted dominating batch

Target leader locally appends `E200` but HW has not advanced. `E150` arrives and is stale
relative to local state, but its response waits for the target WAL offset of `E200`. If the
target leader fails, `E150` receives a leadership error rather than false success.

### Target recovery after log cleanup

After `E900`, target WAL segments roll and old segments are deleted. A covering full writer
snapshot must retain `E900`. After restart, delayed `E800` is stale-success and cannot
mutate KV. Missing covering state is a recovery failure.

### Partition drop and recreation

Old pid 10 and new pid 20 produce different physical keys. Once pid 10 is tombstoned, old
UPSERTs are rejected and old DELETEs can only remove pid-10 keys. Query filtering and main
table recheck prevent stale candidates from becoming incorrect results; compaction removes
the old records.

## Testing Strategy

### Internal writer policy tests

- Accept `100 -> 500 -> greater than Integer.MAX_VALUE`.
- Treat duplicate `500` and delayed `100` as stale-success with no KV or WAL mutation.
- Keep ordinary client `0 -> 2` out-of-order validation unchanged.
- Reject internal magic on ordinary tables and incompatible Index Tables.

### Commit and recovery tests

- Hold `E200` below target HW, submit `E150`, fail the target leader, and assert no false
  acknowledgement.
- Cover clean restart, unclean restart, writer snapshot reload, WAL truncation, segment
  deletion, and remote-tier recovery.
- Advance the clock beyond ordinary writer expiration and prove the internal frontier
  remains.
- Corrupt or remove the only required snapshot after deleting earlier WAL and assert
  fail-closed recovery.

### Source and target failover tests

- Delay old-source-leader requests while the new source leader uses different window
  boundaries.
- Combine source leader overlap, target leader failover, timeout, response loss, and late
  completion.
- Use strong final-state assertions against a reference projection of the source WAL.

### Mutation tests

- Same-key `UPSERT -> DELETE -> UPSERT` in one and multiple windows.
- Index-key-changing UPDATE with old-key DELETE and new-key UPSERT in the same and different
  target buckets.
- Empty windows, multiple same-key records in one batch, null index columns, and batch
  retries.

### Partition tests

- Drop pid 10, recreate the logical partition as pid 20, then release delayed pid-10 UPSERT
  and DELETE requests.
- Restart the target before tombstone baseline initialization and assert retryable not-ready.
- Reject header/key/value pid mismatches without changing KV, WAL, or writer state.
- Assert old pid state retires and old keys disappear after compaction.

### Identity tests

- Concurrent range allocation produces disjoint ranges.
- Failed assignment creation leaks but never reuses a range.
- Reassignment and serde round trips preserve `indexWriterIdBase`.
- Mixed-version Coordinator activation is rejected.

### Ambiguous failure tests

Inject failures before prewrite, after prewrite, during target WAL append, after WAL append,
after writer-state update, and before/after HW. An uncertain append must make the replica
fail-stop; recovery and retry must converge to the source reference state.

### Model-based test

Run randomized source mutation histories and random duplicate, drop, delay, reorder,
source-failover, target-failover, and restart events. The final target state, after all
deliveries settle, must equal the reference source WAL projection. The model also asserts
that no stale batch changes target KV or WAL.

### Performance tests

- Compare ordinary physical KV, the rejected versioned implementation, and offset fencing.
- Measure fresh and stale batch paths separately.
- Scale source-writer and target-bucket cardinality to the supported maximum topology.
- Assert zero RocksDB Gets for pure UPSERT and record snapshot/recovery resource curves.

All asynchronous tests use explicit latches, failure hooks, or condition waiting. Fixed
sleep is not an acceptable synchronization mechanism.

## Implementation Boundaries

The implementation plan should be decomposed into independently reviewable stages:

1. internal writer identity allocation and assignment serde;
2. KV/WAL internal batch format and writer-state policy;
3. target apply, stale acknowledgement, snapshot durability, and fail-stop semantics;
4. physical Index Table schema and partition-qualified key encoding;
5. source IndexReplicator integration and dead-code removal;
6. tombstone readiness and writer retirement;
7. capability gating and compatibility rejection;
8. high-value correctness tests; and
9. capacity and latency benchmarks.

The old hidden columns, merger configuration, visibility filter, and tests that only prove
the rejected behavior are removed in the same implementation series. Generic versioned row
merge support outside internal secondary indexes is not removed.

## Alternatives Considered

### Per-row source offset and logical delete

Rejected because it adds row and query overhead, requires read-before-write merge, retains
logical tombstones, and compares offsets from unrelated partition incarnations.

### Source epoch plus contiguous sequence

Rejected because a higher epoch can replay an older source prefix and regress target state.
It also adds source-side per-target counters and reset/wrap lifecycle.

### Existing public KV idempotence unchanged

Rejected because public batch sequence is `int`, begins at zero, requires strict `last + 1`,
and retains only a small exact-duplicate window. Sparse source WAL end offsets require a
separate internal policy.

### Separate IndexBucket apply-offset store

Rejected because generic WAL writer-state already supplies the required durability,
truncation, and HW acknowledgement integration once its internal policy and snapshot
contract are strengthened.
