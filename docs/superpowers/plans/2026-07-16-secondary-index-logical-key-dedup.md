# Secondary Index Logical-Key Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make V2 secondary-index reads return each current main-table row at most once when old and new physical Index Table rows coexist across a partition drop and recreation.

**Architecture:** Keep server tombstone filtering fail-open and unchanged. Add order-independent, per-call logical-primary-key deduplication between Hop1 and Hop2, while retaining the existing main-row recheck as the final stale-pointer check.

**Tech Stack:** Java, Fluss client lookup API, JUnit 5, AssertJ, Maven, Fluss in-process integration cluster.

## Global Constraints

- Deduplicate by the complete main-table logical primary key, including partition columns.
- Never include the Index Table's `__partition_id` in the deduplication key.
- Do not depend on Hop1 result ordering or physical-key adjacency.
- Preserve one Hop2 request and one output path for every distinct logical primary key.
- Keep all deduplication state local to one `lookup()` invocation.
- Retain the existing index-value recheck.
- Do not add RPCs, protocol errors, retries, configuration, server state, or metadata propagation paths.
- Keep server read filtering, write fencing, and compaction behavior unchanged.
- Do not add fixed sleeps.
- Run Maven offline with `.cache` under JDK 17 so the normal Checkstyle and Spotless lifecycle checks remain enabled.

---

## File Map

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/SecondaryIndexLookuper.java`
  - Owns per-call logical-primary-key deduplication before Hop2.
- `fluss-client/src/main/java/org/apache/fluss/client/table/FlussTable.java`
  - Makes the extractor's `GenericRow` result contract explicit.
- `fluss-client/src/test/java/org/apache/fluss/client/lookup/SecondaryIndexLookuperTest.java`
  - Proves order independence, structural key equality, and per-call state isolation.
- `fluss-client/src/test/java/org/apache/fluss/client/table/FlussTableSecondaryIndexITCase.java`
  - Proves correctness with two real physical partition incarnations and stale empty tombstone
    content.
- `docs/superpowers/specs/2026-07-16-secondary-index-logical-key-dedup-design.md`
  - Records the approved correctness and compatibility contract.

---

### Task 1: Lock The Client Deduplication Contract With Failing Tests

**Files:**
- Test: `fluss-client/src/test/java/org/apache/fluss/client/lookup/SecondaryIndexLookuperTest.java`

**Interfaces:**
- Consumes: Hop1 `LookupResult` containing physical Index Table rows.
- Produces: assertions that one logical primary key creates one Hop2 lookup within each call.

- [x] **Step 1: Add a non-adjacent duplicate regression test**

Construct candidates for logical keys `A`, `B`, and `A` where the two `A` rows carry different
physical partition IDs. Extract `(id, partitionValue)` and assert that Hop2 receives two keys, not
three, while the result contains the current A and B rows exactly once.

- [x] **Step 2: Add a binary-key equality regression test**

Use two `byte[]` instances with equal contents in extracted `GenericRow` keys. Assert one Hop2 call
and one result, proving that identity equality is not used.

- [x] **Step 3: Add a concurrent-call isolation test**

Start two lookup calls whose Hop1 futures complete independently with duplicate candidates. Assert
one Hop2 request per call and no shared seen-key state between calls.

- [x] **Step 4: Run the three tests and verify RED**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client \
  -Dtest=SecondaryIndexLookuperTest test
```

Expected: the new assertions fail because the current implementation starts Hop2 once per physical
candidate and emits duplicate main rows.

---

### Task 2: Deduplicate Logical Primary Keys Before Hop2

**Files:**
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/lookup/SecondaryIndexLookuper.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/table/FlussTable.java`
- Modify: `fluss-client/src/test/java/org/apache/fluss/client/lookup/SecondaryIndexLookuperTest.java`

**Interfaces:**
- Consumes: `Function<InternalRow, GenericRow> basePkExtractorFromIndexRow`.
- Produces: one `mainTablePointLookuper.lookup(basePk)` call per distinct extracted key.

- [x] **Step 1: Make the extractor result type explicit**

Change the field, constructor parameter, and `FlussTable` local variable from
`Function<InternalRow, InternalRow>` to `Function<InternalRow, GenericRow>`. Adapt test-only identity
extractors with an explicit `GenericRow` cast.

- [x] **Step 2: Add per-call order-independent deduplication**

Create a local `HashSet<GenericRow>` in `doHop2`. For each physical candidate, extract its key and
start Hop2 only when `seenBasePrimaryKeys.add(basePk)` returns true. Keep the existing future list so
unique results retain first-occurrence order.

- [x] **Step 3: Update lookup documentation**

Document that Hop1 candidates are deduplicated by logical main-table primary key before Hop2 and
that this prevents duplicate results across partition incarnations.

- [x] **Step 4: Run the complete lookuper unit suite and verify GREEN**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client \
  -Dtest=SecondaryIndexLookuperTest,FlussTableSecondaryIndexLookuperTest test
```

Expected: all selected tests pass; the existing 1,024-distinct-key test still records exactly 1,024
Hop2 calls.

---

### Task 3: Prove The Real Partition-Recreation Failure Mode

**Files:**
- Test: `fluss-client/src/test/java/org/apache/fluss/client/table/FlussTableSecondaryIndexITCase.java`

**Interfaces:**
- Consumes: the in-process cluster's actual Index Table, partition lifecycle, and TabletServer
  tombstone caches.
- Produces: an end-to-end assertion over the public V2 secondary-index lookuper.

- [x] **Step 1: Add the partition recreation integration test**

Create a one-bucket partitioned main table with one SYNC index. Write a row, drop its partition,
recreate the same partition value, and write the same logical primary key and index value again.
Capture the authoritative tombstone from every TabletServer, temporarily replace it with stale empty
content, and restore it in `finally`.

- [x] **Step 2: Assert the failure precondition and public result**

While the cache contains deliberately stale empty tombstone content, prefix-read the internal Index Table and assert two physical rows with
different `__partition_id` values. Then query through `getSecondaryIndexLookuper()` and assert one
current full main row.

- [x] **Step 3: Run the focused integration test**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client \
  -Dtest=FlussTableSecondaryIndexITCase#testPartitionRecreationWithStaleEmptyTombstoneReturnsOneRow test
```

Expected: PASS without sleeps; every wait is tied to partition metadata or exact index visibility.

---

### Task 4: Final Verification And Review

**Files:**
- Review all files above.

- [x] **Step 1: Run all focused unit and integration tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client \
  -Dtest=SecondaryIndexLookuperTest,FlussTableSecondaryIndexLookuperTest,FlussTableSecondaryIndexITCase test
```

Expected: all selected tests pass.

- [x] **Step 2: Run the complete client module**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client test
```

Expected: all `fluss-client` tests pass.

- [x] **Step 3: Inspect the final patch**

```bash
git diff --check
git diff --stat
git status --short
```

Expected: no whitespace errors; only the planned client files and design/plan documents are changed.

Do not create a commit until explicitly requested.
