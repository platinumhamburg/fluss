# Secondary Index Logical-Key Dedup Design

## Problem

A partitioned Index Table appends `__partition_id` to its physical primary key. If a logical
partition is dropped and recreated, an old and a new physical Index Table row can therefore coexist
for the same main-table logical primary key. The server normally suppresses the old row with the
partition tombstone cache, but an Index Bucket whose cache is not initialized or is temporarily
stale must fail open to avoid deleting or hiding live data based on incomplete metadata.

`SecondaryIndexLookuper` currently starts one Hop2 main-table point lookup for every physical Hop1
row. When both partition incarnations pass the fail-open read filter, the same current main-table row
is fetched and emitted twice.

## Decision

Before starting Hop2, `SecondaryIndexLookuper` deduplicates Hop1 candidates by the main table's full
logical primary key. The key includes partition columns and excludes the Index Table's internal
`__partition_id` column. One lookup invocation maintains one local set of seen keys and starts only
the first Hop2 request for each key.

The deduplication is independent of Hop1 result ordering. It does not assume that RocksDB keeps rows
for different partition incarnations adjacent.

The existing Hop2 index-value recheck remains mandatory:

- If the current main row does not exist, Hop2 emits nothing.
- If the current main row no longer matches the requested index value, recheck emits nothing.
- If old and new physical index rows identify the same current main row, deduplication emits it once.

## Component Contracts

- `FlussTable` extracts every logical primary-key field into a fresh `GenericRow`.
- `SecondaryIndexLookuper` accepts `Function<InternalRow, GenericRow>` so hash/equality semantics are
  explicit rather than depending on an arbitrary `InternalRow` implementation.
- `GenericRow` deep equality provides content equality for array-backed key fields.
- The seen-key set is local to `lookup()` processing and is never shared between concurrent calls.
- The existing list of Hop2 futures preserves first-occurrence order among unique candidates.

## Server Responsibilities

The server-side partition tombstone read filter remains an early-pruning optimization. Unknown or
stale tombstone state remains fail-open. Query correctness no longer depends on that state being
initialized before the read.

Write-path partition fencing and compaction filtering are unchanged. They protect persistent state
and storage reclamation and cannot be replaced by client deduplication.

## Compatibility And Scope

The behavior is part of the V2 secondary-index lookup client. Upgraded Java and Flink clients use the
same `FlussTable.getSecondaryIndexLookuper()` implementation. Older clients do not gain the new
deduplication behavior.

Direct reads of an internal Index Table continue to expose its physical-row semantics. This design
does not change the accepted eventual-consistency behavior: index replication lag may still cause a
temporary miss, and independently stale main-table partition metadata follows the same behavior as a
direct main-table lookup.

## Verification

1. A unit test supplies non-adjacent physical candidates `A, B, A` and proves exactly two Hop2 calls
   and one result per logical key.
2. A unit test uses equal-content binary key fields backed by different arrays.
3. A unit test overlaps concurrent calls and proves their seen-key state is isolated.
4. An integration test drops and recreates a logical partition, writes the same logical row in both
   incarnations, fixes each TabletServer cache at stale empty tombstone content, proves two physical
   Index Table rows are visible, and proves the public secondary-index lookup returns the current
   main row exactly once.
