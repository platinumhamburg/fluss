# Secondary Index V2 Production Hardening Design

## Status

The design was accepted in discussion on 2026-07-14 and is awaiting review of this
written form before implementation planning.

This document closes review issues 1-5 from the latest production-readiness audit. Review
issue 6 is explicitly outside this phase. The design keeps the current push architecture,
WriterState fencing, source-offset progress, and sync/async visibility contracts intact.

## Goals

- Keep system-managed Index Tables out of the public client write path without treating an
  RPC API version as caller identity.
- Make a terminal source-bucket replication failure persistently visible to operators.
- Prove that source leader failover can resume index replication from raw remote WAL.
- Bound retained, unacknowledged index batches across an entire TabletServer while preserving
  the existing per-replicator isolation threshold.
- Remove the false-positive path from the index-target failover integration test.
- Achieve these goals with local changes to existing components and no new coordination or
  scheduling subsystem.

## Non-Goals

- Authentication or authorization for raw internal RPC use.
- A new internal RPC, request marker, principal, token, listener service, or Coordinator/ZooKeeper
  health record.
- Exactly-once index replication or atomicity across target index buckets.
- Strict fairness or resource quotas between tables or source buckets when a TabletServer-wide
  memory limit is exhausted.
- Different retry or failover behavior for SYNC and ASYNC indexes. Visibility waiting remains the
  only difference between those modes.
- Changes to lake-table creation, deletion, or rollback behavior.
- A redesign of `IndexReplicatorPool`, `IndexSender`, or the source-WAL window model.

## Current Architecture

For each leader source `TableBucket`, `ReplicaIndexController` owns one `IndexReplicator`.
The replicator reads a bounded source WAL window, derives one window independently for each
secondary index, and encodes one `IndexBatch` per affected target Index Table bucket. A complete
window is staged in the TabletServer-global `IndexAccumulator`. `IndexSender` retries target
PutKv indefinitely for retryable failures and advances the source index-pushed offset only after
all batches in the window are acknowledged.

The source WAL remains the durable backlog. The accumulator is only a dispatch buffer for
derived output that has not yet been acknowledged.

## Decision 1: Public Index Table Writes Are Blocked by Table Metadata in the Client

### Contract

An upgraded Fluss client rejects public writer creation when the target `TableInfo` has
`TableType.INDEX_TABLE`. The normal Table API already has the metadata before creating an
`UpsertWriter`, so no protocol or server change is needed.

The check belongs at the public table-writer boundary, before an RPC or client-side batch can be
created. In the current API shape, `FlussTable.newUpsert()` is the single normal entry point and
will reject an Index Table with a clear error. Index Tables have primary keys, so the existing
append-table validation already prevents `newAppend()` from being used for them.

The product contract is therefore:

```text
TableInfo.isIndexTable() == true
    -> public client refuses newUpsert()
    -> no writer, record batch, metadata routing, or PutKv RPC is created
```

### ApiVersion Is Not an Access-Control Boundary

`ApiVersion` communicates wire capability. It does not identify an internal caller: a server and
an ordinary upgraded client can both negotiate the same version. No new server condition such as
"PutKv v0 may write data tables but v2 may write Index Tables" will be introduced as Index Table
write protection.

The following existing checks remain, but only for protocol correctness:

- PutKv API v2 capability negotiation by `IndexSender` remains necessary to carry the V1 fenced
  KV batch format.
- The server continues to reject a V1 batch received through a PutKv API version that cannot
  decode it.
- A table continues to accept only the immutable KV idempotence protocol selected by its table
  metadata.
- `IndexKvWriteGuard` continues to validate canonical source identity, partition incarnation, and
  tombstone behavior. It is a replication-correctness guard, not caller authentication.

No test or documentation may describe these protocol checks as the public-write security
boundary.

### Trust Boundary and Accepted Risk

This phase trusts the upgraded client. An old client, a modified client, or a hand-crafted raw V1
PutKv request that bypasses the public Table API is outside the supported contract. If such a
request also satisfies the table protocol and Index Table write invariants, the server is not
required to distinguish it from index replication.

This is intentional. Closing that raw-RPC boundary would require authenticated internal caller
identity, which is a separate architecture and is not justified for this change.

### Proof

Add a client integration test that opens a real derived Index Table and asserts:

1. `table.getTableInfo().isIndexTable()` is true;
2. `table.newUpsert()` fails immediately with an Index Table-specific message; and
3. the Index Table KV/WAL state is unchanged.

The test does not iterate over ApiVersions. That would test the rejected design rather than this
contract. A normal main-table writer assertion in the same fixture proves the check is scoped to
Index Tables.

## Decision 2: Terminal Replication Health Is Local, Persistent, and Aggregated

### Failure Semantics

Target Index Table leader loss, leader migration, RPC timeout, stale metadata, and a temporarily
incompatible server during rolling upgrade remain retryable. `IndexSender` retains and retries the
same batches with leader rediscovery and backoff. These conditions do not mark the source bucket
failed and do not use a finite retry budget.

SYNC and ASYNC indexes use the same replication, retry, fencing, failover, and recovery path:

- SYNC only adds PutKv acknowledgement waiting for the relevant pushed offset. A request timeout
  is retriable by the upgraded writer; it is not a reason to fail the Flink job deliberately.
- ASYNC acknowledges the source data path earlier, while the same index pipeline keeps retrying
  and catching up.

A terminal failure is reserved for a condition for which retrying the same source state cannot
make progress, such as proven source WAL corruption or an index batch that cannot be represented
by the transport. The existing `IndexReplicator.terminalFailure` remains the authoritative cause.

### Controller State

`ReplicaIndexController.State` gains `FAILED`. The current replicator reports its one-shot
terminal result directly to its owning controller through a constructor callback. This is a local
lifecycle handoff, not a listener service or a new architecture.

The callback carries the exact `IndexReplicator` instance. The controller changes `RUNNING` to
`FAILED` only when that instance is still its current replicator. This identity check prevents a
late completion from an old leader run from marking a replacement replicator failed.

State transitions become:

```text
NOT_STARTED -> DEFERRED
NOT_STARTED -> RUNNING
DEFERRED    -> RUNNING
RUNNING     -> FAILED
RUNNING     -> NOT_STARTED
FAILED      -> NOT_STARTED
```

There is no `FAILED -> RUNNING` retry loop while the same run remains active. Leader loss or
replica close retires the failed replicator and returns the controller to `NOT_STARTED`; a later
leader incarnation creates a fresh replicator and may run again.

The callback does not close the controller, perform RPC, acquire Coordinator state, or call back
into `IndexReplicator`. It records local state after the replicator has already performed its own
terminal cleanup. Controller lifecycle code never holds its controller monitor while calling
`IndexReplicator.close()`, avoiding lock inversion with a concurrent terminal callback.

The terminal state and cause remain authoritative even if the controller callback itself throws.
Such a callback failure is logged after cleanup and must not restart the replicator, lose the
original cause, or leave owned batches accounted.

### Operator Signal

Add one TabletServer gauge:

```text
indexReplicationFailedSourceBucketCount
```

Its value is the number of current leader source buckets on that TabletServer whose
`ReplicaIndexController` is `FAILED`. `ReplicaManager` computes it from its existing local replica
map at scrape time, matching existing server-level replica gauges. There are no table, partition,
or bucket labels, so metric cardinality is constant per TabletServer.

Operational interpretation is direct:

- server value `0`: no locally led source bucket is known to have stopped index replication;
- server value `> 0`: at least one source bucket requires investigation;
- cluster sum: number of failed source buckets currently led in the cluster.

`delayedWriteCount` remains useful for current user-visible waiting, but it is not a substitute:
it is traffic-dependent, combines ISR and index waits, disappears when requests time out, and
does not represent ASYNC index failure.

No lag gauge or per-bucket time series is added in this phase. Existing request-error, retry,
in-flight-age, remote-read, and record-too-large metrics remain diagnostics after the failed-count
gauge identifies a persistent fault.

### Proof

Focused tests must prove:

- a terminal replicator result moves only its current controller to `FAILED`;
- a late terminal callback from a retired replicator cannot poison a new `RUNNING` instance;
- retryable target leader loss does not enter `FAILED`;
- follower transition and close clear the local failed state;
- the TabletServer gauge counts current failed leader source buckets without bucket labels.

## Decision 3: Prove Remote Source WAL Recovery Across Leader Failover

The production path already gives `IndexReplicator` an `IndexSourceReader` backed by
`RemoteLogManager`. Unit tests cover remote-segment iteration and the handoff from remote to local
WAL. The missing evidence is a production-shaped test that combines tiering, persisted index
progress, source leader failover, and subsequent index catch-up.

Add a server integration test with this causal sequence:

1. Create a replicated main table with a secondary index and raw remote WAL enabled.
2. Write a first prefix and wait until both the Index Table result and the exact source
   index-pushed offset are visible.
3. Persist that exact pushed offset in a completed source KV snapshot.
4. Write a second prefix while index dispatch is deliberately held back, making the second prefix
   committed in source WAL but not reflected in the persisted pushed offset.
5. Roll and upload the required raw source WAL segment, then wait until that range is available
   remotely and no longer readable from the new leader's local WAL path.
6. Fail over the source bucket leader.
7. Assert that the new leader restores the exact snapshot offset, reads the missing range through
   the remote source path, and advances to the exact committed source WAL end.
8. Assert all expected index rows and values, not only row count, and assert no duplicate or stale
   index key survives.
9. Write another row after catch-up and prove normal local-WAL replication continues.

The test must prove the precondition that recovery really used remote WAL. A passing lookup alone
is insufficient. It must assert the relevant local start offset is beyond the restored pushed
offset and that the remote-read byte counter increases during recovery.

All pauses use deterministic hooks or condition waits. Fixed sleeps are prohibited.

## Decision 4: Bound TabletServer-Wide Retained Index Batches

### Exact Meaning of Pending Bytes

For this subsystem, pending bytes are the encoded index-batch buffers admitted to
`IndexAccumulator` but not yet released. They include:

- batches queued for a sender;
- batches in an in-flight PutKv RPC; and
- batches retained during retry backoff or re-enqueued for retry.

They exclude unread source WAL, transient source decoding objects, and batches already
acknowledged or dropped. One `IndexWindow` may fan out to multiple target batches. Individual
batches can be released as they are acknowledged, while the window's pushed offset advances only
after its final batch is acknowledged.

For one owner:

```text
pendingBytes(owner) = sum(retainedBytes(batch))
                      for all admitted, unreleased batches owned by that replicator
```

TabletServer pending bytes are the sum over owners. There is no fixed relationship between source
WAL input bytes and derived pending bytes: multiple indexes, updates, and target-bucket fanout can
expand or redistribute the output.

### Accounting Unit

The current builders allocate 4 KiB heap pages. Logical encoded length undercounts retained heap
for small fanout batches because every non-empty target batch retains at least one page. Capacity
accounting therefore uses page-rounded retained payload bytes, not only
`BytesView.getBytesLength()`:

```text
retainedBytes(batch) = roundUp(encodedLength, 4 KiB)
```

This intentionally accounts the retained payload pages. It does not claim byte-exact accounting
of Java object headers, maps, deques, or RPC framework objects.

The existing `indexPushPendingBytes` gauge keeps its name for compatibility, but its description
changes to this retained-byte definition. Dashboards should compare the per-TabletServer value
with the configured total limit; a cluster sum is not a memory-safety signal.

### Two Bounds With Different Purposes

Keep the existing option:

```text
index.replication.max-pending-bytes = 64 MiB by default
```

It remains a per-replicator threshold. Once an owner has reached the threshold, that owner stops
deriving more windows. A complete window admitted while the owner was below the threshold may take
it slightly above the threshold; this preserves progress for an indivisible valid window.

Add:

```text
index.replication.max-total-pending-bytes = 256 MiB by default
```

This is a TabletServer-wide post-admission bound for retained index batch pages. It is the final
memory-safety boundary; the per-owner threshold remains the normal fault-isolation mechanism.

When the total bound is exhausted, unrelated producers may also pause until admitted batches are
released. This is accepted emergency behavior. Guaranteeing strict table or bucket isolation at
global saturation would require quotas and a fair scheduler, which are outside this minimal
change.

### Whole-Window Admission

`IndexAccumulator` gains one all-or-none window admission operation. It receives all batches of
one `IndexWindow`, computes their retained bytes, and serializes only the capacity reservation.
No WAL read, encoding, leader lookup, RPC, callback, or retry backoff runs in the admission
critical section.

Admission and queue publication linearize against owner failure and close under the existing
`IndexWindow` monitor. The capacity lock is used only inside that window-owned section for the
reservation update. Sender wakeups happen after both locks are released. This preserves the
existing window-to-batch-to-queue lock direction and prevents owner retirement from landing
between reservation and publication.

Admission follows this order:

1. A cheap total-capacity precheck prevents ordinary WAL reads while the server is already full.
2. The replicator reads and encodes one complete window outside the admission lock.
3. The accumulator computes exact page-rounded retained bytes for the complete window.
4. Under the short admission lock, it verifies the owner is active and
   `totalPending + windowBytes <= maxTotalPendingBytes`.
5. On success it reserves accounting once and publishes every batch using the existing per-target
   queues.
6. On rejection it publishes and accounts nothing. The replicator does not register an in-flight
   window or advance its pushed offset; it discards the encoded result and retries the same source
   offset after the existing worker backoff.

Capacity is never acquired batch by batch. Partial capacity admission could publish only some
target mutations and strand the source window. The all-or-none rule concerns local buffer
admission; it does not introduce cross-bucket transactional semantics.

The precheck is only a performance optimization. Concurrent producers may pass it together; the
serialized final admission is the correctness boundary. A rejected producer may re-encode after
backoff, but cannot busy-spin because the existing pool wait remains in effect. Strict waiter
fairness is not promised.

### Oversized Window

If one complete window's retained bytes exceed the total bound by itself, it can never be admitted.
Retrying it forever would create a silent livelock. It therefore enters the existing terminal
replication-failure path with a capacity error and is reflected by
`indexReplicationFailedSourceBucketCount`.

The default total bound is deliberately much larger than the preferred 1 MiB derived window. The
preferred request/window bound still permits an indivisible mutation group to exceed its soft
limit, so the terminal check remains necessary for explicit low configurations or exceptionally
large records.

### Release and Recovery

- A retry reuses the original admission and does not add accounting.
- ACK, terminal window failure, owner retirement, leader loss, and close release each batch at
  most once through the existing one-shot batch state.
- Releasing capacity never waits for new admission and therefore cannot deadlock with producers.
- Existing periodic pool wakeups are sufficient for progress; no new scheduler or waiter queue is
  introduced.
- Source WAL remains the durable backlog. When admission is unavailable, not-yet-derived work stays
  in WAL rather than moving into unbounded JVM heap.

The total bound applies to retained accumulator output. Encoding happens before exact admission,
so transient derivation memory is additionally bounded by the fixed reader-worker count and one
window per active worker, not by `max-total-pending-bytes` alone. The metric and option must not be
documented as a byte-exact bound on the entire JVM process.

### Proof

High-value tests must prove:

- many owners individually below the per-owner threshold cannot collectively admit more than the
  total bound;
- concurrent admissions respect the total bound and a multi-target window is admitted entirely
  or not at all;
- page-rounded accounting catches many tiny fanout batches;
- retry does not double-account, and ACK/failure/owner close release exactly once;
- release allows a previously rejected source offset to make progress;
- an individually oversized window fails once rather than re-encoding forever; and
- with a target unavailable, retained pending bytes stabilize at the bound and drain after target
  recovery while pushed offsets finish at the exact expected value.

## Decision 5: Make the Target Failover Test Causally Valid

`IndexPushFailoverITCase.testIndexTableLeaderFailoverRetries()` currently computes the target
bucket from the test key. If that bucket's leader equals the main-table leader, the test changes
only the server selected for shutdown and leaves the original target bucket unchanged. It can then
stop an unrelated Index Table bucket and pass without exercising target failover.

The corrected test chooses data and topology together:

1. Inspect actual leaders for the Index Table buckets.
2. Select a target bucket whose leader differs from the source main-table leader.
3. Find a deterministic index value hashing to that exact bucket.
4. Build the physical lookup key and `TableBucket` from that same value.
5. Stop the actual leader of that target bucket and assert the main-table leader remains running.
6. Submit the main-table write during the failover interval, wait for the target bucket to acquire
   a different leader, and require the write/index progress to complete.
7. Assert the exact target row and exact source pushed offset.
8. Restart every stopped server in `finally`, even when an assertion fails.

If the fixture cannot provide an independent source/target leader topology, setup fails with a
diagnostic instead of silently testing a different bucket. No fixed sleep is added.

## Error Handling Summary

| Condition | Result |
|---|---|
| Public client opens writer for Index Table | Immediate client-side rejection from table metadata |
| Raw handcrafted PutKv bypasses Table API | Outside trusted-client contract; no ApiVersion-based identity check |
| Target leader unavailable or moving | Infinite retry with rediscovery and backoff |
| SYNC request waits too long | Retriable request timeout; replication continues |
| ASYNC target unavailable | Source write may ack; replication retries and retained memory is bounded |
| Source WAL corruption | Current source controller becomes `FAILED` |
| One window exceeds total retained-byte bound | Current source controller becomes `FAILED` |
| TabletServer total retained-byte bound reached | New windows pause; admitted windows continue draining |
| Source leadership lost | Old replicator and batches retire; new leader reconstructs from persisted progress |

## Documentation Alignment

The FIP and configuration descriptions must state:

- public Index Table write rejection is a trusted-client Table API rule, not an ApiVersion rule;
- V1 protocol negotiation and `IndexKvWriteGuard` are correctness mechanisms;
- target failover is transparent and uses the same retry contract for SYNC and ASYNC indexes;
- `indexPushPendingBytes` includes queued, in-flight, and retry-retained page-rounded batch bytes;
- the per-replicator and TabletServer-total capacity options have distinct purposes; and
- `indexReplicationFailedSourceBucketCount` is the persistent cluster health signal for terminal
  source-bucket replication failure.

## Implementation Boundaries

Expected changes are limited to the existing ownership boundaries:

- `fluss-client`: public Table writer validation and its integration test;
- `ReplicaIndexController` / `IndexReplicator`: one-shot terminal state handoff;
- `ReplicaManager` / `TabletServerMetricGroup` / `MetricNames`: one aggregated failed-source gauge;
- `IndexAccumulator` / `IndexReplicator` / `IndexBatch`: retained-byte accounting and whole-window
  admission;
- existing source remote-WAL and failover integration test fixtures;
- `IndexPushFailoverITCase`; and
- FIP/configuration documentation.

No assignment metadata, Coordinator RPC, ZooKeeper node, public PutKv message field, new scheduler,
or per-bucket metric is introduced.
