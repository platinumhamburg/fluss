# Tombstone Safe-Floor Compression Design

## Context

Global secondary index v2 uses a per-source-table `PartitionTombstone` to let partitioned Index Tables filter stale index entries after source partitions are dropped. The current model is:

```text
partitionId is tombstoned iff partitionId <= floor || explicitSet.contains(partitionId)
```

This model is consumed by three paths:

- Java write apply path via `TombstonedPartitionDiscriminator.isTombstoned(byte[])`
- Java prefix lookup filtering via `ReplicaIndexController.filterTombstonedEntries(...)`
- RocksDB native compaction filtering via `FloorSetCompactionFilter(tagOffset, floor, explicitSet)`

The current `PartitionTombstoneAdvancer` only advances `floor` by absorbing `floor + 1` from `explicitSet`. That is correct when partition ids are dense, but source partition ids are expected to be sparse in practice. In sparse-id workloads, `explicitSet` can grow long-term because consecutive absorption rarely fires.

## Goals

- Compress tombstone state using source-table lifecycle metadata rather than dense partition-id assumptions.
- Keep the existing tombstone membership contract: `pid <= floor || explicitSet.contains(pid)`.
- Avoid changing RPC, ZK binary serde, Java filtering, JNI, or RocksDB native compaction filter in the first phase.
- Preserve fail-open safety under TabletServer stale metadata cache: stale tombstones may retain extra records but must never drop alive partition records.
- Add observability so future bitmap fallback is triggered by evidence, not speculation.

## Non-Goals

- Do not introduce range compression. Sparse partition ids make ranges ineffective and risky unless every id in the range is proven non-alive.
- Do not introduce bitmap or RoaringBitmap in the first phase. Bitmap remains a fallback if high-cardinality random drops keep `explicitSet` above threshold.
- Do not change the v3 Index Table value layout or the fixed-offset partition tag read.
- Do not add user-facing table properties for tombstone compression.

## Key Insight

`floor` should represent a safe alive-partition watermark, not a dense dropped-id prefix.

New invariant:

```text
No alive source partition may have partitionId <= floor.
```

Because partition ids are monotonic and never reused, every Index Table record whose source `partitionId <= floor` can be removed even if many ids in that range were never allocated. The compression signal is the minimum alive partition id, not adjacency inside `explicitSet`.

## Safe-Floor Algorithm

Inputs:

- `before`: current `PartitionTombstone`
- `droppedPartitionId`: partition id being dropped
- `alivePartitionIdsAfterDrop`: authoritative source-table alive partition ids after the drop has been applied

Algorithm:

```text
explicit = copy(before.explicitSet)
floor = before.floor

if droppedPartitionId > floor:
    explicit.add(droppedPartitionId)

if alivePartitionIdsAfterDrop is empty:
    safeFloor = max(floor, droppedPartitionId, max(explicit, default=floor))
else:
    minAlive = min(alivePartitionIdsAfterDrop)
    safeFloor = minAlive - 1

newFloor = max(floor, safeFloor)
explicit.removeAll(pid <= newFloor)
return PartitionTombstone(newFloor, explicit, before.version + 1)
```

Examples:

```text
alive = {1000007, 1000213, 1000450}
before.floor = -1
droppedPartitionId = 1000001
newFloor = 1000006
explicit = {}
```

```text
alive = {1000007, 1000213}
before.floor = 1000006
droppedPartitionId = 1000450
newFloor = 1000006
explicit = {1000450}
```

The first case compresses all historical ids below the earliest live partition. The second case preserves a sparse high-id drop that cannot be folded into floor without dropping live partitions.

## Partition Id Allocation Invariant

The safe-floor algorithm relies on one hard invariant:

```text
Any newly created source partition id must be greater than the table tombstone floor.
```

If a future partition creation path ever attempts to allocate `partitionId <= floor`, Coordinator must reject or fail fast. This protects against partition-id reuse and keeps old Index Table records safely distinguishable from new source partitions.

## Data Flow

On `DROP PARTITION` for a partitioned source table with secondary indexes:

1. Coordinator applies the partition deletion to authoritative metadata.
2. Coordinator reads alive partition ids for the source table after deletion.
3. Coordinator advances tombstone with `dropPartition(before, droppedPartitionId, alivePartitionIdsAfterDrop)`.
4. Coordinator persists the updated tombstone to the existing ZK tombstone znode.
5. Coordinator ships the updated tombstone through the existing `UpdateMetadataRequest` path.
6. TabletServers replace cached tombstone state if the incoming version is not stale.
7. Java apply/query filters and RocksDB compaction filters continue using the unchanged `floor + explicitSet` predicate.

## Error Handling

- If alive partition snapshot is unavailable, the drop path should conservatively persist `explicit += droppedPartitionId` without safe-floor advancement and emit an error metric/log. Safe-floor compression is an optimization and must not make partition drop less reliable.
- If alive partition ids contain any id `<= before.floor`, Coordinator should fail fast because it indicates a broken allocation or metadata invariant.
- Re-dropping an already tombstoned partition remains idempotent in shape and still bumps version, preserving the current observer-change behavior.
- Empty alive set folds all known tombstoned ids into `floor`; a later partition creation must allocate above that floor.

## Observability

Add metrics or logs for:

- Current `floor`
- `explicitSet.size`
- Serialized tombstone payload bytes
- `floor` advancement delta per update
- Count of `explicitSet` entries removed by safe-floor compression

Suggested warning threshold:

```text
explicitSet.size >= 4096 or serialized payload >= 256KB
```

These thresholds are warning-only in the first phase. They identify workloads that may need bitmap fallback.

## Bitmap Fallback Criteria

Bitmap is a later design option, not part of phase one. It becomes worth considering only if all are true:

- `explicitSet` remains above threshold for sustained periods.
- The large set consists mostly of sparse ids above `floor`.
- Query/apply/compaction CPU or metadata payload size becomes measurable operational pressure.

If bitmap is introduced later, it must update Java metadata, RPC, ZK serde, JNI, and native compaction filter together. The public tombstone predicate should remain abstracted behind `PartitionTombstone.isTombstoned(long)`.

## Testing Strategy

Unit tests:

- Sparse low-id drops advance `floor` to `min(alive) - 1`.
- Sparse high-id drops above `min(alive)` remain in `explicitSet`.
- Empty alive set folds all explicit ids into `floor`.
- `alivePartitionIdsAfterDrop` containing `id <= floor` is rejected.
- Re-dropping a partition already under `floor` bumps version without changing shape.

Coordinator tests:

- `DROP PARTITION` computes alive ids after the partition is removed.
- Tombstone persistence and metadata fan-out carry the safe-floor-compressed state.
- Partition creation rejects or fails fast when allocated id is not greater than existing tombstone floor.

Regression tests:

- Existing Java tombstone filtering tests continue passing without changing filter consumers.
- Existing RocksDB `FloorSetCompactionFilter` tests continue passing because native inputs remain `floor + explicitSet`.

## Alternatives Considered

- Keep contiguous absorption only: rejected because sparse partition ids make this mostly a fallback, not compression.
- Range compression: rejected because sparse ids make ranges ineffective unless every id in the range is proven non-alive, which reduces to the safe-floor watermark for the useful lower range.
- Bitmap-first implementation: deferred because it solves the worst-case sparse high-id set but requires cross-language format changes before evidence shows the complexity is needed.
